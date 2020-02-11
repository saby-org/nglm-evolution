package com.evolving.nglm.evolution.datacubes.journeys;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;

public class JourneyTrafficDatacubeGenerator extends DatacubeGenerator
{
  private static final DateFormat DATE_FORMAT;
  static
  {
    DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
  }
  private static final String DATACUBE_ES_INDEX_PREFIX = "datacube_journeytraffic-";
  private static final String DATA_ES_INDEX_PREFIX = "journeystatistic-";
  private static final String FILTER_STRATUM_PREFIX = "subscriberStratum.";

  private List<String> filterFields;
  private List<String> basicFilterFields;
  private SegmentationDimensionsMap segmentationDimensionList;
  private JourneysMap journeysMap;
  
  private String journeyID;
  private String generationDate;
  
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

  // We need to lowerCase the journeyID, cf. JourneyStatisticESSinkConnector (we will follow the same rule)
  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX_PREFIX + this.journeyID.toLowerCase(); }
  @Override protected String getDataESIndex() { return DATA_ES_INDEX_PREFIX + this.journeyID.toLowerCase(); }
  @Override protected List<String> getFilterFields() { return filterFields; }
  @Override protected List<CompositeValuesSourceBuilder<?>> getFilterComplexSources() { return new ArrayList<CompositeValuesSourceBuilder<?>>(); }
  @Override protected List<AggregationBuilder> getDataAggregations() { return Collections.emptyList(); }
  @Override protected Map<String, Object> extractData(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException { return Collections.emptyMap(); }

  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    this.segmentationDimensionList.update();
    this.journeysMap.update();
    
    this.filterFields = new ArrayList<String>(this.basicFilterFields);
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
    // nodeID
    //

    String nodeID = (String) filters.remove("nodeID");
    filters.put("node.id", nodeID);
    filters.put("node.display", journeysMap.getNodeDisplay(journeyID, nodeID, "node"));
    
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
  }

  @Override
  protected void addStaticFilters(Map<String, Object> filters)
  {
    filters.put("dataDate", generationDate);
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
