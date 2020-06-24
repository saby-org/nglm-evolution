package com.evolving.nglm.evolution.datacubes.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;

public class JourneyTrafficDatacubeGenerator extends DatacubeGenerator
{
  private static final String DATACUBE_ES_INDEX_PREFIX = "datacube_journeytraffic-";
  private static final String DATA_ES_INDEX_PREFIX = "journeystatistic-";
  private static final String FILTER_STRATUM_PREFIX = "subscriberStratum.";

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private List<String> filterFields;
  private List<String> basicFilterFields;
  private SegmentationDimensionsMap segmentationDimensionList;
  private JourneysMap journeysMap;
  
  private String journeyID;

  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public JourneyTrafficDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch, SegmentationDimensionService segmentationDimensionService, JourneyService journeyService)  
  {
    super(datacubeName, elasticsearch);

    this.segmentationDimensionList = new SegmentationDimensionsMap(segmentationDimensionService);
    this.journeysMap = new JourneysMap(journeyService);
    
    //
    // Filter fields
    //
    this.basicFilterFields = new ArrayList<String>();
    this.basicFilterFields.add("status");
    this.basicFilterFields.add("nodeID");
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
    
    this.filterFields = new ArrayList<String>(this.basicFilterFields);
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
    // nodeID
    //
    String nodeID = (String) filters.remove("nodeID");
    filters.put("node", journeysMap.getNodeDisplay(journeyID, nodeID, "node"));
    
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
  @Override protected List<AggregationBuilder> getMetricAggregations() { return Collections.emptyList(); }
  @Override protected Map<String, Object> extractMetrics(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException { return Collections.emptyMap(); }

  /*****************************************
  *
  * Run
  *
  *****************************************/
  public void definitive(String journeyID, long journeyStartDateTime, Date publishDate)
  {
    this.journeyID = journeyID;
    
    String timestamp = TIMESTAMP_FORMAT.format(publishDate);
    long targetPeriod = publishDate.getTime() - journeyStartDateTime;
    this.run(timestamp, targetPeriod);
  }
}
