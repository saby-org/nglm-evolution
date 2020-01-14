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
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;

import com.evolving.nglm.evolution.Segment;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.GUIManagerClient;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;

public class JourneyTrafficDatacubeGenerator extends DatacubeGenerator
{
  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");
  private static final String DATACUBE_ES_INDEX_PREFIX = "datacube_journeytraffic-";
  private static final String DATA_ES_INDEX_PREFIX = "journeystatistic-";
  private static final String FILTER_STRATUM_PREFIX = "subscriberStratum.";

  private GUIManagerClient guiClient;
  private List<String> filterFields;
  private List<String> basicFilterFields;
  private SegmentationDimensionsMap segmentationDimensionList;
  private JourneysMap journeysMap;
  
  private String journeyID;
  private String generationDate;
  
  public JourneyTrafficDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch, GUIManagerClient guiClient)  
  {
    super(datacubeName, elasticsearch);

    this.guiClient = guiClient;
    this.segmentationDimensionList = new SegmentationDimensionsMap();
    this.journeysMap = new JourneysMap();
    
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

  @Override
  protected void runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    this.segmentationDimensionList.updateFromGUIManager(guiClient);
    this.journeysMap.updateFromGUIManager(guiClient);
    
    this.filterFields = new ArrayList<String>(this.basicFilterFields);
    for(String dimensionID: segmentationDimensionList.keySet())
      {
        this.filterFields.add(FILTER_STRATUM_PREFIX + dimensionID);
      }
  }
  
  @Override 
  protected void embellishFilters(Map<String, Object> filters) 
  {
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
    
    //
    // rewards
    //
    
    // TODO : implements rewards 
  }

  @Override
  protected void addStaticFilters(Map<String, Object> filters)
  {
    filters.put("dataDate", generationDate);
  }
  
  @Override
  protected List<AggregationBuilder> getDataAggregations()
  {
    // Those aggregations need to be recomputed with the requested date !
    List<AggregationBuilder> dataAggregations = new ArrayList<AggregationBuilder>();
    
    return dataAggregations;
  }

  @Override
  protected Map<String, Object> extractData(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException
  {    
    HashMap<String, Object> data = new HashMap<String,Object>();

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
