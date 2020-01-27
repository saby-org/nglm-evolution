package com.evolving.nglm.evolution.datacubes.journeys;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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

import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.JourneyRewardsMap;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;

public class JourneyRewardsDatacubeGenerator extends DatacubeGenerator
{
  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");
  private static final String DATACUBE_ES_INDEX_PREFIX = "datacube_journeyrewards-";
  private static final String DATA_ES_INDEX_PREFIX = "journeystatistic-";
  private static final String FILTER_STRATUM_PREFIX = "subscriberStratum.";

  private List<String> filterFields;
  private SegmentationDimensionsMap segmentationDimensionList;
  private JourneysMap journeysMap;
  private JourneyRewardsMap journeyRewardsList;
  
  private String journeyID;
  private String generationDate;
  
  public JourneyRewardsDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch, SegmentationDimensionService segmentationDimensionService, JourneyService journeyService)
  {
    super(datacubeName, elasticsearch);

    this.segmentationDimensionList = new SegmentationDimensionsMap(segmentationDimensionService);
    this.journeysMap = new JourneysMap(journeyService);
    this.journeyRewardsList = new JourneyRewardsMap();
    
    //
    // Filter fields
    //
    
    this.filterFields = new ArrayList<String>();
  }

  // We need to lowerCase the journeyID, cf. JourneyStatisticESSinkConnector (we will follow the same rule)
  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX_PREFIX + this.journeyID.toLowerCase(); }
  @Override protected String getDataESIndex() { return DATA_ES_INDEX_PREFIX + this.journeyID.toLowerCase(); }
  @Override protected List<String> getFilterFields() { return filterFields; }
  @Override protected List<CompositeValuesSourceBuilder<?>> getFilterComplexSources() { return new ArrayList<CompositeValuesSourceBuilder<?>>(); }

  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    this.segmentationDimensionList.update();
    this.journeysMap.update();
    this.journeyRewardsList.updateFromElasticsearch(elasticsearch, this.getDataESIndex());
    
    if(this.journeyRewardsList.getRewardIDs().isEmpty())
      {
        log.warn("No rewards found in " + this.journeyID + " journey statistics.");
        // It is useless to generate a rewards datacube if there is not any rewards.
        return false;
      }
    
    this.filterFields = new ArrayList<String>();
    for(String dimensionID: segmentationDimensionList.keySet())
      {
        this.filterFields.add(FILTER_STRATUM_PREFIX + dimensionID);
      }
    
    return true;
  }
  
  @Override 
  protected void embellishFilters(Map<String, Object> filters) 
  {
    //
    // JourneyID (already in index name, added for Kibana)
    //

    filters.put("journey.id", journeyID);
    filters.put("journey.display", journeysMap.getDisplay(journeyID, "journey"));
    
    //
    // subscriberStratum dimensions
    //
    
    for(String dimensionID: segmentationDimensionList.keySet())
      {
        String fieldName = FILTER_STRATUM_PREFIX + dimensionID;
        String segmentID = (String) filters.remove(fieldName);
        
        filters.put(fieldName + ".id", segmentID);
        filters.put(fieldName + ".display", segmentationDimensionList.getSegmentDisplay(dimensionID, segmentID, fieldName));
      }
    
    //
    // rewards
    //
    
  }

  @Override
  protected void addStaticFilters(Map<String, Object> filters)
  {
    filters.put("dataDate", generationDate);
  }
  
  @Override
  protected List<AggregationBuilder> getDataAggregations()
  {
    // Those aggregations need to be recomputed with the update of journeyRewardsList
    List<AggregationBuilder> dataAggregations = new ArrayList<AggregationBuilder>();
    
    for(String rewardID : journeyRewardsList.getRewardIDs()) 
      {
        dataAggregations.add(AggregationBuilders.sum(rewardID).field("rewards." + rewardID));
      }
    
    return dataAggregations;
  }

  @Override
  protected Map<String, Object> extractData(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException
  {    
    HashMap<String, Object> data = new HashMap<String,Object>();
    
    if (compositeBucket.getAggregations() == null) {
      log.error("Unable to extract data, aggregation is missing.");
      return data;
    }

    for(String rewardID : journeyRewardsList.getRewardIDs()) 
      {
        ParsedSum rewardAggregation = compositeBucket.getAggregations().get(rewardID);
        if (rewardAggregation == null) {
          log.warn("Unable to extract "+rewardID+" reward in journeystatistics, aggregation is missing.");
        } else {
          data.put("rewards." + rewardID, (int) rewardAggregation.getValue());
        }
      }
    
    return data;
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/
  
  public void run(String journeyID, Date generationDate)
  {
    this.journeyID = journeyID;
    this.generationDate = DATE_FORMAT.format(generationDate);
    
    this.run();
  }
  
  public void run(String journeyID)
  {
    this.journeyID = journeyID;
    this.generationDate = "last"; // special date for the "current" status that we will override on refresh
    
    this.run();
  }
}
