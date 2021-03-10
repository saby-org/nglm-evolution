package com.evolving.nglm.evolution.datacubes.generator;

import java.io.IOException;
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

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.JourneyMetricDeclaration;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.JourneyStatisticESSinkConnector;
import com.evolving.nglm.evolution.MetricHistory;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.DatacubeWriter;
import com.evolving.nglm.evolution.datacubes.SimpleDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.JourneyRewardsMap;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class JourneyRewardsDatacubeGenerator extends SimpleDatacubeGenerator
{
  public static final String DATACUBE_ES_INDEX_PREFIX = "datacube_journeyrewards-";
  private static final String DATA_ES_INDEX_PREFIX = "journeystatistic-";
  private static final String FILTER_STRATUM_PREFIX = "subscriberStratum.";

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private List<String> filterFields;
  private SegmentationDimensionsMap segmentationDimensionList;
  private JourneysMap journeysMap;
  private JourneyRewardsMap journeyRewardsList;
  
  private String journeyID;
  private Date publishDate;

  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public JourneyRewardsDatacubeGenerator(String datacubeName, ElasticsearchClientAPI elasticsearch, DatacubeWriter datacubeWriter, SegmentationDimensionService segmentationDimensionService, JourneyService journeyService)
  {
    super(datacubeName, elasticsearch, datacubeWriter);

    this.segmentationDimensionList = new SegmentationDimensionsMap(segmentationDimensionService);
    this.journeysMap = new JourneysMap(journeyService);
    this.journeyRewardsList = new JourneyRewardsMap(elasticsearch);
    
    //
    // Filter fields
    //
    this.filterFields = new ArrayList<String>();
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
    return DATACUBE_ES_INDEX_PREFIX + JourneyStatisticESSinkConnector.journeyIDFormatterForESIndex(this.journeyID);
  }

  /*****************************************
  *
  * Filters settings
  *
  *****************************************/
  @Override protected List<String> getFilterFields() { return filterFields; }

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
    Date stopDate = RLMDateUtils.addDays(journey.getEffectiveEndDate(), maximumPostPeriod, Deployment.getBaseTimeZone());
    if(publishDate.after(stopDate)) {
      log.info("JourneyID=" + this.journeyID + " has ended more than " + maximumPostPeriod + " days ago. No data will be published anymore.");
      return false;
    }
    
    //
    // Rewards list update
    //
    this.journeyRewardsList.updateAndPush(this.journeyID, this.getDataESIndex(), datacubeWriter);
    if(this.journeyRewardsList.getRewards().isEmpty()) {
      log.info("No rewards found in JourneyID=" + this.journeyID + " journey statistics.");
      // It is useless to generate a rewards datacube if there is not any rewards.
      return false;
    }
    
    this.filterFields = new ArrayList<String>();
    for(String dimensionID: segmentationDimensionList.keySet()) {
      this.filterFields.add(FILTER_STRATUM_PREFIX + dimensionID);
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
    for(String dimensionID: segmentationDimensionList.keySet())
      {
        String fieldName = FILTER_STRATUM_PREFIX + dimensionID;
        String segmentID = (String) filters.remove(fieldName);

        String newFieldName = FILTER_STRATUM_PREFIX + segmentationDimensionList.getDimensionDisplay(dimensionID, fieldName);
        filters.put(newFieldName, segmentationDimensionList.getSegmentDisplay(dimensionID, segmentID, fieldName));
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
    
    for(String reward : journeyRewardsList.getRewards()) 
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

    for(String reward : journeyRewardsList.getRewards()) 
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
  * Run
  *
  *****************************************/
  public void definitive(String journeyID, long journeyStartDateTime, Date publishDate)
  {
    this.journeyID = journeyID;
    this.publishDate = publishDate;

    String timestamp = RLMDateUtils.printTimestamp(publishDate);
    long targetPeriod = publishDate.getTime() - journeyStartDateTime;
    this.run(timestamp, targetPeriod);
  }
}
