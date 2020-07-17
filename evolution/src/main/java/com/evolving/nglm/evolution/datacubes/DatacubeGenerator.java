package com.evolving.nglm.evolution.datacubes;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.HashMap;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Deployment;

public abstract class DatacubeGenerator
{
  /*****************************************
  *
  * Static
  *
  *****************************************/
  protected static final Logger log = LoggerFactory.getLogger(DatacubeGenerator.class);
  
  /** 
   * The maximum number of buckets allowed in a single response is limited by a dynamic cluster
   * setting named search.max_buckets. It defaults to 10,000, requests that try to return more
   * than the limit will fail with an exception. */
  protected static final int BUCKETS_MAX_NBR = 10000; // TODO: factorize in ES client later (with some generic ES calls)
  private static final String UNDEFINED_BUCKET_VALUE = "undefined";

  //
  // Date formats
  //
  public static final DateFormat TIMESTAMP_FORMAT;
  static
  {
    TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
    TIMESTAMP_FORMAT.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
  }
  
  public static final DateFormat DAY_FORMAT;
  static
  {
    DAY_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    DAY_FORMAT.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
  }
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  protected RestHighLevelClient elasticsearch;
  private String datacubeName;
  protected String compositeAggregationName = "DATACUBE";
  protected ByteBuffer tmpBuffer = null;

  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public DatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch) 
  {
    this.datacubeName = datacubeName;
    this.elasticsearch = elasticsearch;
  }
  
  /*****************************************
  *
  *  abstract functions
  *
  *****************************************/
  //
  // Elasticsearch indices settings
  //
  protected abstract String getDataESIndex();
  protected abstract String getDatacubeESIndex();
  
  //
  // Filters settings
  //
  protected abstract List<String> getFilterFields();
  protected abstract List<CompositeValuesSourceBuilder<?>> getFilterComplexSources();
  protected abstract boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException; // return false if generation must stop here.
  protected abstract void embellishFilters(Map<String, Object> filters);
  
  //
  // Metrics settings
  //
  protected abstract List<AggregationBuilder> getMetricAggregations();
  protected abstract Map<String, Object> extractMetrics(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException;

  /*****************************************
  *
  *  overridable functions
  *
  *****************************************/
  protected String getDatacubeName() {
    return this.datacubeName;
  }
  //
  // DocumentID settings
  //
  protected void addStaticFilters(Map<String, Object> filters) { 
    return; 
  }
  
  protected String getDocumentID(Map<String,Object> filters, String timestamp) {
    return this.extractDocumentIDFromFilter(filters, timestamp);
  }
  
  /*****************************************
  *
  *  extractDocumentIDFromFilter
  *  
  *  The purpose of this function is to extract a unique ID from the filter object.
  *  This function will return a string created from the concatenation of a hash code 
  *  of each filter 'value'. This concatenation is, then, encoded in base64.
  *  
  *  We use **URLEncoder** for base64 (with '-' and '_') to be consistent with IDs
  *  in Elasticsearch.
  *  
  *  rl: There is a potential bug with dynamic filters. Two rows could have different
  *  filters with same value and be considered equal (with the same ID).
  *
  *****************************************/
  protected String extractDocumentIDFromFilter(Map<String,Object> filters, String timestamp) 
  {
    List<String> sortedkeys = new ArrayList<String>(filters.keySet());
    Collections.sort(sortedkeys);
    
    if(tmpBuffer == null) {
      tmpBuffer = ByteBuffer.allocate(Integer.BYTES * (sortedkeys.size() + 1));
    } else {
      tmpBuffer.rewind();
    }
    
    tmpBuffer.putInt(timestamp.hashCode());
    for(String key: sortedkeys) {
      tmpBuffer.putInt(filters.get(key).toString().hashCode());
    }
    
    return Base64.getUrlEncoder().encodeToString(tmpBuffer.array());
  }
  
  /*****************************************
  *
  * isESIndexAvailable
  *  
  * @return false if the ES index does not exist
  *****************************************/
  protected boolean isESIndexAvailable(String index)
  {
    SearchSourceBuilder request = new SearchSourceBuilder()
        .sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
        .query(QueryBuilders.matchAllQuery())
        .size(1);
    
    try 
      {
        elasticsearch.search(new SearchRequest(index).source(request), RequestOptions.DEFAULT);
      } 
    catch(ElasticsearchException e) 
      {
        if (e.status() == RestStatus.NOT_FOUND) {
          // ES index does not exist.
          return false;
        }
      }
    catch(IOException e)
      {
      }
    
    return true;
  }
  
  /*****************************************
  *
  *  getElasticsearchRequest
  *
  *****************************************/
  private SearchRequest getElasticsearchRequest() 
  {
    String ESIndex = getDataESIndex();
    
    List<String> datacubeFilterFields = getFilterFields();
    List<CompositeValuesSourceBuilder<?>> datacubeFilterComplexSources = getFilterComplexSources();
    List<AggregationBuilder> datacubeMetricAggregations = getMetricAggregations();
    
    //
    // Composite sources are created from datacube filters and complex sources (scripts)
    //
    List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
    for(String datacubeFilter: datacubeFilterFields) {
      TermsValuesSourceBuilder sourceTerms = new TermsValuesSourceBuilder(datacubeFilter).field(datacubeFilter).missingBucket(true);
      sources.add(sourceTerms);
    }
    for(CompositeValuesSourceBuilder<?> complexSources: datacubeFilterComplexSources) {
      sources.add(complexSources);
    }
    
    CompositeAggregationBuilder compositeAggregation = AggregationBuilders.composite(compositeAggregationName, sources)
        .size(BUCKETS_MAX_NBR);

    //
    // Metric aggregations
    //
    for(AggregationBuilder subaggregation : datacubeMetricAggregations) {
      compositeAggregation = compositeAggregation.subAggregation(subaggregation);
    }
    
    //
    // Datacube request
    //
    SearchSourceBuilder datacubeRequest = new SearchSourceBuilder()
        .sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
        .query(QueryBuilders.matchAllQuery())
        .aggregation(compositeAggregation)
        .size(0);
    
    return new SearchRequest(ESIndex).source(datacubeRequest);
  }

  /*****************************************
  *
  *  executeESSearchRequest
  *
  *****************************************/
  protected SearchResponse executeESSearchRequest(SearchRequest request) throws ElasticsearchException, IOException 
  {
    try 
      {
        SearchResponse searchResponse = this.elasticsearch.search(request, RequestOptions.DEFAULT);
        return searchResponse;
      } 
    catch(ElasticsearchException e)
      {
        if (e.status() == RestStatus.NOT_FOUND) {
          log.warn("[{}]: elasticsearch index {} does not exist", getDatacubeName(), request.indices());
          return null;
        } else {
          throw e;
        }
      }
  }

  /*****************************************
  *
  *  extractDatacubeRows
  *
  *****************************************/
  private List<Map<String,Object>> extractDatacubeRows(SearchResponse response, String timestamp, long period) throws ClassCastException 
  {
    List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
    
    if (response.isTimedOut()
        || response.getFailedShards() > 0
        || response.getSkippedShards() > 0
        || response.status() != RestStatus.OK) {
      log.error("Elasticsearch search response return with bad status in {} generation.", getDatacubeName());
      return result;
    }
    
    if(response.getAggregations() == null) {
      log.error("Main aggregation is missing in {} search response.", getDatacubeName());
      return result;
    }
    
    ParsedComposite compositeBuckets = response.getAggregations().get(compositeAggregationName);
    if(compositeBuckets == null) {
      log.error("Composite buckets are missing in {} search response.", getDatacubeName());
      return result;
    }
    
    for(ParsedBucket bucket: compositeBuckets.getBuckets()) {
      long docCount = bucket.getDocCount();

      //
      // Extract filter, replace null
      //
      Map<String, Object> filters = bucket.getKey();
      for(String key: filters.keySet()) {
        if(filters.get(key) == null) {
          filters.replace(key, UNDEFINED_BUCKET_VALUE);
        }
      }
      
      //
      // Add static filters that will be used for ID generation (such as date for instance)
      //
      addStaticFilters(filters);
      
      //
      // Extract documentID from filters & timestamp (before calling embellish)
      //
      String documentID = getDocumentID(filters, timestamp);
      
      //
      // Embellish filters for Kibana/Grafana (display names), they won't be taken into account for documentID
      //
      embellishFilters(filters);
      
      //
      // Extract metrics (@rl may also modify filters, see loyalty hack)
      //
      Map<String, Object> metrics = extractMetrics(bucket, filters);
      
      //
      // Result
      //
      HashMap<String,Object> datacubeRow = new HashMap<String,Object>();
      datacubeRow.put("_id",  documentID); // _id field is a temporary variable and will be removed before insertion
      datacubeRow.put("timestamp", timestamp);
      datacubeRow.put("period", period);
      datacubeRow.put("count", docCount);
      
      //
      // Flat filter fields for Kibana/Grafana
      //
      for(String filter: filters.keySet()) {
        datacubeRow.put("filter." + filter, filters.get(filter));
      }
      
      //
      // Flat metric fields for Kibana/Grafana
      //
      for(String metric : metrics.keySet()) {
        datacubeRow.put("metric." + metric, metrics.get(metric));
      }
      
      result.add(datacubeRow);
    }
    
    
    return result;
  }

  /*****************************************
  *
  *  pushDatacubeRows
  *
  *****************************************/
  private void pushDatacubeRows(List<Map<String,Object>> datacubeRows) throws ElasticsearchException, IOException 
  {
    for(Map<String,Object> datacubeRow: datacubeRows) {
      String documentID = (String) datacubeRow.remove("_id");
      
      UpdateRequest request = new UpdateRequest(getDatacubeESIndex(), documentID);
      request.doc(datacubeRow);
      request.docAsUpsert(true);
      request.retryOnConflict(4);
      
      this.elasticsearch.update(request, RequestOptions.DEFAULT);
    }
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/
  protected void run(String timestamp, long period) 
  {  
    try 
      {
        //
        // Pre-generation phase (for retrieving some mapping infos)
        //
        log.debug("[{}]: running pre-generation phase.", getDatacubeName());
        boolean success = runPreGenerationPhase();
        if(!success) {
          log.info("[{}]: stopped in pre-generation phase.", getDatacubeName());
          return;
        }
        
        //
        // Generate Elasticsearch request
        //
        SearchRequest request = getElasticsearchRequest();
        log.info("[{}]: executing ES request: {}", getDatacubeName(), request);
        
        //
        // Execute Elasticsearch request
        //
        SearchResponse response = executeESSearchRequest(request);
        if(response == null) {
          log.warn("[{}]: cannot retrieve any ES response, datacube generation stop here.", getDatacubeName());
          return;
        }
        log.debug("[{}]: retrieving ES response: {}", getDatacubeName(), response);
        
        //
        // Extract datacube rows from JSON response
        //
        log.debug("[{}]: extracting data from ES response.", getDatacubeName());
        List<Map<String,Object>> datacubeRows = extractDatacubeRows(response, timestamp, period);
        
        //
        // Push datacube rows in Elasticsearch
        //
        log.info("[{}]: pushing {} datacube row(s) in ES index [{}].", getDatacubeName(), datacubeRows.size(), this.getDatacubeESIndex());
        pushDatacubeRows(datacubeRows);
      } 
    catch(IOException|RuntimeException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("[{}]: generation failed: {}", getDatacubeName(), stackTraceWriter.toString());
      }
  }
}
