package com.evolving.nglm.evolution.datacubes;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Date;
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
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DatacubeGenerator
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(DatacubeGenerator.class);
  
  //
  // Other
  //
  
  /** The maximum number of buckets allowed in a single response is limited by a dynamic cluster
   *  setting named search.max_buckets. It defaults to 10,000, requests that try to return more
   *  than the limit will fail with an exception. */
  protected static final int BUCKETS_MAX_NBR = 10000;
  protected static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

  /*****************************************
  *
  *  Data
  *
  *****************************************/
  
  protected String compositeAggregationName = "DATACUBE";
  protected final String datacubeName;
  protected ByteBuffer tmpBuffer = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public DatacubeGenerator(String datacubeName) 
  {
    this.datacubeName = datacubeName;
  }
  
  /*****************************************
  *
  *  getter
  *
  *****************************************/
  
  public String getDatacubeName() { return this.datacubeName; }
  
  /*****************************************
  *
  *  Abstract functions
  *
  *****************************************/
  
  protected abstract String getDataESIndex(String date);
  protected abstract String getDatacubeESIndex();
  protected abstract List<String> getFilterFields();
  protected abstract List<AggregationBuilder> getDataAggregations();
  protected abstract JSONObject extractData(ParsedBucket compositeBucket) throws ClassCastException;

  /*****************************************
  *
  *  extractDateStringFromDate
  *
  *****************************************/
  
  protected String extractDateStringFromDate(Date date) 
  {
    return DATE_FORMAT.format(date);
  }
  
  /*****************************************
  *
  *  extractDocumentIDFromFilter
  *  
  *  The purpose of this function is to extract a unique ID from the filter object.
  *  This function will return a string created from the concatenation of a hash code 
  *  of each filter 'value'. This concatenation is, then, encoded in base64.
  *
  *****************************************/
  
  protected String extractDocumentIDFromFilter(JSONObject filter) 
  {
    List<String> keySet = getFilterFields();
    
    if(tmpBuffer == null) {
      tmpBuffer = ByteBuffer.allocate(Integer.BYTES * (keySet.size() + 1)); // Adding 'dataDate' field
    } else {
      tmpBuffer.rewind();
    }
    
    tmpBuffer.putInt(filter.get("dataDate").toString().hashCode());
    for(String key: keySet) {
      tmpBuffer.putInt(filter.get(key).toString().hashCode());
    }
    
    return Base64.getEncoder().encodeToString(tmpBuffer.array());
  }
  
  /*****************************************
  *
  *  getElasticsearchRequest
  *
  *****************************************/

  /**
   * @param date: YYYY-MM-dd format
   * @return
   */
  private SearchRequest getElasticsearchRequest(String date) 
  {
    String ESIndex = getDataESIndex(date);
    
    // TODO : This could be done in constructor, it does not change with the date argument
    List<String> datacubeFilters = getFilterFields();
    List<AggregationBuilder> datacubeDataAggregations = getDataAggregations();
    
    //
    // Composite sources are created from datacube filters
    //
    
    List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
    for(String datacubeFilter: datacubeFilters) {
      TermsValuesSourceBuilder sourceTerms = new TermsValuesSourceBuilder(datacubeFilter).field(datacubeFilter);
      sources.add(sourceTerms);
    }
    
    //
    // Composite aggregation
    //
    
    CompositeAggregationBuilder compositeAggregation = AggregationBuilders.composite(compositeAggregationName, sources)
        .size(BUCKETS_MAX_NBR);
    
    for(AggregationBuilder subaggregation : datacubeDataAggregations) {
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
  *  executeRequest
  *
  *****************************************/
  
  private SearchResponse executeESRequest(SearchRequest request, RestHighLevelClient elasticsearch) throws ElasticsearchException, IOException 
  {
    try 
      {
        SearchResponse searchResponse = elasticsearch.search(request, RequestOptions.DEFAULT);
        return searchResponse;
      } 
    catch(ElasticsearchException e)
      {
        if (e.status() == RestStatus.NOT_FOUND) {
          log.warn("[{}]: elasticsearch index {} is empty", this.datacubeName, request.indices());
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

  /**
   * 
   * @param response
   * @param date: YYYY-MM-dd format
   * @return
   */
  private List<Map<String,Object>> extractDatacubeRows(SearchResponse response, String date) throws ClassCastException 
  {
    List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
    
    if (response.isTimedOut()
        || response.getFailedShards() > 0
        || response.getSkippedShards() > 0
        || response.status() != RestStatus.OK) {
      log.error("Elasticsearch search response return with bad status in {} generation.", this.datacubeName);
      return result;
    }
    
    if(response.getAggregations() == null) {
      log.error("Main aggregation is missing in {} search response.", this.datacubeName);
      return result;
    }
    
    ParsedComposite compositeBuckets = response.getAggregations().get(compositeAggregationName);
    if(compositeBuckets == null) {
      log.error("Composite buckets are missing in {} search response.", this.datacubeName);
      return result;
    }
    
    for(ParsedBucket bucket: compositeBuckets.getBuckets()) {
      long computationDate = System.currentTimeMillis();
      long docCount = bucket.getDocCount();
      Map<String, Object> keyFilterMap = bucket.getKey();
      
      JSONObject keyFilter = new JSONObject();
      for(String filter : keyFilterMap.keySet()) {
        keyFilter.put(filter, keyFilterMap.get(filter));
      }
      keyFilter.put("dataDate", date);
      
      //
      // extract data 
      //
      
      JSONObject data = extractData(bucket);
      
      //
      // result
      //

      HashMap<String,Object> datacubeRow = new HashMap<String,Object>();
      datacubeRow.put("computationDate", computationDate);
      datacubeRow.put("filter", keyFilter);
      datacubeRow.put("count", docCount);
      datacubeRow.put("data", data);
      
      result.add(datacubeRow);
    }
    
    
    return result;
  }

  /*****************************************
  *
  *  extractDatacubeRows
  *
  *****************************************/
  
  private void pushDatacubeRows(List<Map<String,Object>> datacubeRows, RestHighLevelClient elasticsearch) throws ElasticsearchException, IOException 
  {
    for(Map<String,Object> datacubeRow: datacubeRows) {
      String documentID = extractDocumentIDFromFilter((JSONObject) datacubeRow.get("filter"));
      
      UpdateRequest request = new UpdateRequest("datacube_odr", documentID);
      request.doc(datacubeRow);
      request.docAsUpsert(true);
      request.retryOnConflict(4);
      
      elasticsearch.update(request, RequestOptions.DEFAULT);
    }
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/
  
  public void run(Date date, RestHighLevelClient elasticsearch) 
  {
    String requestedDate = extractDateStringFromDate(date);
    
    //
    // Generate Elasticsearch request
    //
    
    SearchRequest request = getElasticsearchRequest(requestedDate);
    log.info("[{}]: executing ES request: {}", this.datacubeName, request);
  
    try 
      {
        //
        // Execute Elasticsearch request
        //

        SearchResponse response = executeESRequest(request, elasticsearch);
        if(response == null) {
          log.warn("[{}]: cannot retrieve any ES response, datacube generation stop here.", this.datacubeName);
          return;
        }
        log.debug("[{}]: retrieving ES response: {}", this.datacubeName, response);
        
        //
        // Extract datacube rows from JSON response
        //

        log.info("[{}]: extracting data from ES response.", this.datacubeName);
        List<Map<String,Object>> datacubeRows = extractDatacubeRows(response, requestedDate);
        
        //
        // push datacube rows in Elasticsearch
        //

        log.info("[{}]: pushing {} datacube rows in ES.", this.datacubeName, datacubeRows.size());
        pushDatacubeRows(datacubeRows, elasticsearch);
        
      } 
    catch(IOException|ElasticsearchException|ClassCastException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("[{}]: generation failed: {}", this.datacubeName, stackTraceWriter.toString());
      }
  }
}
