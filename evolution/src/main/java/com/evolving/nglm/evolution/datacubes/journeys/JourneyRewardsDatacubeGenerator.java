package com.evolving.nglm.evolution.datacubes.journeys;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.JourneyRewardsMap;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;

public class JourneyRewardsDatacubeGenerator extends DatacubeGenerator
{
  private static final String DATACUBE_ES_INDEX_PREFIX = "datacube_journeyrewards-";
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

  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public JourneyRewardsDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch, SegmentationDimensionService segmentationDimensionService, JourneyService journeyService)
  {
    super(datacubeName, elasticsearch);

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
  // We need to lowerCase the journeyID, cf. JourneyStatisticESSinkConnector (we will follow the same rule)
  @Override protected String getDataESIndex() 
  { 
    return DATA_ES_INDEX_PREFIX + this.journeyID.toLowerCase(); 
  }
  
  @Override protected String getDatacubeESIndex() 
  { 
    return DATACUBE_ES_INDEX_PREFIX + this.journeyID.toLowerCase(); 
  }

  /*****************************************
  *
  * Filters settings
  *
  *****************************************/
  @Override protected List<String> getFilterFields() { return filterFields; }
  @Override protected List<CompositeValuesSourceBuilder<?>> getFilterComplexSources() { return new ArrayList<CompositeValuesSourceBuilder<?>>(); }

  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    if(!isESIndexAvailable(getDataESIndex())) {
      log.info("Elasticsearch index [" + getDataESIndex() + "] does not exist.");
      return false;
    }
    
    this.segmentationDimensionList.update();
    this.journeysMap.update();
    this.journeyRewardsList.update(this.journeyID, this.getDataESIndex());
    
    if(this.journeyRewardsList.getRewards().isEmpty()) {
      log.info("No rewards found in " + this.journeyID + " journey statistics.");
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
    filters.put(FILTER_STRATUM_PREFIX + "Global", "");
    
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
  protected Map<String, Object> extractMetrics(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException
  {    
    HashMap<String, Object> metrics = new HashMap<String,Object>();
    
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
          metrics.put("reward." + reward, (int) rewardAggregation.getValue());
        }
      }
    
    return metrics;
  }

  /*****************************************
  *
  * Run
  *
  *****************************************/
  public void definitive(String journeyID, long journeyStartDateTime)
  {
    this.journeyID = journeyID;

    Date now = SystemTime.getCurrentTime();
    String timestamp = TIMESTAMP_FORMAT.format(now);
    long targetPeriod = now.getTime() - journeyStartDateTime;
    this.run(timestamp, targetPeriod);
  }
}
