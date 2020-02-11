package com.evolving.nglm.evolution.datacubes.subscriber;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.SegmentationDimensionsMap;

public class SubscriberProfileDatacubeGenerator extends DatacubeGenerator
{
  private static final DateFormat DATE_FORMAT;
  static
  {
    DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
  }
  private static final String DATACUBE_ES_INDEX = "datacube_subscriberprofile";
  private static final String DATA_ES_INDEX = "subscriberprofile";
  private static final String DATA_ES_INDEX_SNAPSHOT_PREFIX = "subscriberprofile_snapshot-";
  private static final String FILTER_STRATUM_PREFIX = "stratum.";

  private List<String> filterFields;
  private SegmentationDimensionsMap segmentationDimensionList;
  
  private String generationDate;
  private boolean onToday; // needed to target current subscriberprofile index
  
  public SubscriberProfileDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch, SegmentationDimensionService segmentationDimensionService)
  {
    super(datacubeName, elasticsearch);

    this.segmentationDimensionList = new SegmentationDimensionsMap(segmentationDimensionService);
    
    //
    // Filter fields
    //
    
    this.filterFields = new ArrayList<String>();
  }

  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX; }
  @Override protected List<String> getFilterFields() { return filterFields; }
  @Override protected List<CompositeValuesSourceBuilder<?>> getFilterComplexSources() { return new ArrayList<CompositeValuesSourceBuilder<?>>(); }

  @Override protected String getDataESIndex() { 
    if (onToday) {
      return DATA_ES_INDEX;
    } else {
      return DATA_ES_INDEX_SNAPSHOT_PREFIX + generationDate; 
    }
  }
  
  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    this.segmentationDimensionList.update();
    
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
  
  @Override
  protected List<AggregationBuilder> getDataAggregations()
  {
    List<AggregationBuilder> dataAggregations = new ArrayList<AggregationBuilder>();
    
    //
    // TODO
    //
    
    return dataAggregations;
  }

  @Override
  protected Map<String, Object> extractData(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException
  {    
    HashMap<String, Object> data = new HashMap<String,Object>();
    
    //
    // TODO
    //
    
    return data;
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/
  
  public void run(Date generationDate, boolean today)
  {
    this.generationDate = DATE_FORMAT.format(generationDate);
    this.onToday = today;

    this.run();
  }
}
