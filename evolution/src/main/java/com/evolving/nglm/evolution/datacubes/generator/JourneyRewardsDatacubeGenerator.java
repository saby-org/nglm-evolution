package com.evolving.nglm.evolution.datacubes.generator;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.JourneyMetricDeclaration;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.JourneyStatisticESSinkConnector;
import com.evolving.nglm.evolution.MetricHistory;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.DatacubeManager;
import com.evolving.nglm.evolution.datacubes.DatacubeUtils;
import com.evolving.nglm.evolution.datacubes.DatacubeWriter;
import com.evolving.nglm.evolution.datacubes.SimpleDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.JourneyRewardsMap;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class JourneyRewardsDatacubeGenerator extends SimpleDatacubeGenerator
{
  private static final String DATACUBE_ES_INDEX_SUFFIX = "_datacube_journeyrewards-";
  public static final String DATACUBE_ES_INDEX_PREFIX(int tenantID) { return "t" + tenantID + DATACUBE_ES_INDEX_SUFFIX; }
  private static final String DATA_ES_INDEX_PREFIX = "journeystatistic-";
  private static final String FILTER_STRATUM_PREFIX = "subscriberStratum.";

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private SegmentationDimensionsMap segmentationDimensionList;
  private JourneysMap journeysMap;
  private JourneyRewardsMap journeyRewardsList;
  private JourneyService journeyService;
  
  private String journeyID;
  private Date publishDate;

  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public JourneyRewardsDatacubeGenerator(String datacubeName, ElasticsearchClientAPI elasticsearch, DatacubeWriter datacubeWriter, SegmentationDimensionService segmentationDimensionService, JourneyService journeyService, int tenantID, String timeZone)
  {
    super(datacubeName, elasticsearch, datacubeWriter, tenantID, timeZone);

    this.segmentationDimensionList = new SegmentationDimensionsMap(segmentationDimensionService);
    this.journeysMap = new JourneysMap(journeyService);
    this.journeyService = journeyService;
    this.journeyRewardsList = new JourneyRewardsMap(journeyService, elasticsearch);
  }
  
  public JourneyRewardsDatacubeGenerator(String datacubeName, int tenantID, DatacubeManager datacubeManager) {
    this(datacubeName,
        datacubeManager.getElasticsearchClientAPI(),
        datacubeManager.getDatacubeWriter(),
        datacubeManager.getSegmentationDimensionService(),
        datacubeManager.getJourneyService(),
        tenantID,
        Deployment.getDeployment(tenantID).getTimeZone());
  }

  /*****************************************
  *
  * Elasticsearch indices settings
  *
  *****************************************/
  @Override protected String getDataESIndex() 
  { 
    return DATA_ES_INDEX_PREFIX + JourneyStatisticESSinkConnector.journeyIDFormatterForESIndex(this.journeyID); 
  }
  
  @Override protected String getDatacubeESIndex() 
  { 
    return DATACUBE_ES_INDEX_PREFIX(this.tenantID) + DatacubeUtils.retrieveJourneyEndWeek(this.journeyID, this.journeyService);
  }

  /*****************************************
  *
  * Filters settings
  *
  *****************************************/
  @Override 
  protected List<String> getFilterFields() {
    //
    // Build filter fields
    //
    List<String> filterFields = new ArrayList<String>();

    // getFilterFields is called after runPreGenerationPhase. It safe to assume segmentationDimensionList is up to date.
    // Regarding dimensions, in journeystatistic ES indices, there is already only the "statistics" ones (see JourneyStatisticESSinkConnector.java)
    // Nonetheless, to avoid having plenty of "null" dimensions in the datacube (and pollute the index settings) we also 
    // filter out "non statistics" dimensions here.
    for(String dimensionID: segmentationDimensionList.keySet()) {
      if (segmentationDimensionList.isFlaggedStatistics(dimensionID)) {
        filterFields.add(FILTER_STRATUM_PREFIX + dimensionID);
      }
    }
    
    return filterFields; 
  }
  
  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    if(!isESIndexAvailable(getDataESIndex())) {
      log.info("Elasticsearch index [" + getDataESIndex() + "] does not exist.");
      return false;
    }
    
    this.segmentationDimensionList.update();
    this.journeysMap.update();
    
    //
    // Should we keep pushing datacube for this journey ?
    //
    Journey journey = this.journeysMap.get(this.journeyID);
    if(journey == null) {
      log.error("Error, unable to retrieve info for JourneyID=" + this.journeyID);
      return false;
    }
    // Number of day needed after EndDate for JourneyMetrics to be pushed. - Add 24 hours to be sure (due to truncation, see populateMetricsPost)
    int maximumPostPeriod = Deployment.getJourneyMetricConfiguration().getPostPeriodDays() + 1;
    Date stopDate = RLMDateUtils.addDays(journey.getEffectiveEndDate(), maximumPostPeriod, this.getTimeZone());
    if(publishDate.after(stopDate)) {
      log.info("JourneyID=" + this.journeyID + " has ended more than " + maximumPostPeriod + " days ago. No data will be published anymore.");
      return false;
    }
    
    //
    // Check rewards
    //
    if(this.journeyRewardsList.getRewards(this.journeyID).isEmpty()) {
      log.info("No rewards found in JourneyID=" + this.journeyID + " journey statistics.");
      // It is useless to generate a rewards datacube if there is not any rewards.
      return false;
    }
    
    return true;
  }
  
  @Override 
  protected void embellishFilters(Map<String, Object> filters) 
  {
    //
    // Journey (already in index name, added for Kibana/Grafana)
    //
    filters.put("journey", journeysMap.getDisplay(journeyID, "journey"));
    
    //
    // Special dimension with all, for Grafana 
    //
    filters.put(FILTER_STRATUM_PREFIX + "Global", " ");
    
    //
    // subscriberStratum dimensions
    //
    for(String dimensionID: segmentationDimensionList.keySet()) {
      if (segmentationDimensionList.isFlaggedStatistics(dimensionID)) {
        String fieldName = FILTER_STRATUM_PREFIX + dimensionID;
        String segmentID = (String) filters.remove(fieldName);

        String newFieldName = FILTER_STRATUM_PREFIX + segmentationDimensionList.getDimensionDisplay(dimensionID, fieldName);
        filters.put(newFieldName, segmentationDimensionList.getSegmentDisplay(dimensionID, segmentID, fieldName));
      }
    }
  }
  
  /*****************************************
  *
  * Metrics settings
  *
  *****************************************/
  @Override
  protected List<AggregationBuilder> getMetricAggregations()
  {
    // Those aggregations need to be recomputed with the update of journeyRewardsList
    List<AggregationBuilder> metricAggregations = new ArrayList<AggregationBuilder>();
    
    for(String reward : journeyRewardsList.getRewards(this.journeyID)) 
      {
        metricAggregations.add(AggregationBuilders.sum(reward).field("rewards." + reward));
      }
    
    return metricAggregations;
  }

  @Override
  protected Map<String, Long> extractMetrics(ParsedBucket compositeBucket) throws ClassCastException
  {    
    HashMap<String, Long> metrics = new HashMap<String,Long>();
    
    if (compositeBucket.getAggregations() == null) {
      log.error("Unable to extract metrics, aggregation is missing.");
      return metrics;
    }

    for(String reward : journeyRewardsList.getRewards(this.journeyID)) 
      {
        ParsedSum rewardAggregation = compositeBucket.getAggregations().get(reward);
        if (rewardAggregation == null) {
          log.warn("Unable to extract "+reward+" reward in journeystatistics, aggregation is missing.");
        } else {
          metrics.put("reward." + reward, new Long((int) rewardAggregation.getValue()));
        }
      }
    
    return metrics;
  }

  /*****************************************
  *
  * Initialization -- special
  *
  *****************************************/
  public void init() 
  {
    try 
      {
        this.journeyRewardsList.updateAndPush(datacubeWriter);
      }
    catch(IOException|RuntimeException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Initialization failed: "+stackTraceWriter.toString()+"");
      }
  }
  
  /*****************************************
  *
  * Run
  *
  *****************************************/
  public void definitive(String journeyID, long journeyStartDateTime, Date publishDate)
  {
    this.journeyID = journeyID;
    this.publishDate = publishDate;

    String timestamp = this.printTimestamp(publishDate);
    long targetPeriod = publishDate.getTime() - journeyStartDateTime;
    this.run(timestamp, targetPeriod);
  }
}
