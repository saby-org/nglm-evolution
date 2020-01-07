package com.evolving.nglm.evolution.datacubes;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

public abstract class DatacubeGenerator
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/
  
  protected static final Logger log = LoggerFactory.getLogger(DatacubeGenerator.class);
  
  /** The maximum number of buckets allowed in a single response is limited by a dynamic cluster
   *  setting named search.max_buckets. It defaults to 10,000, requests that try to return more
   *  than the limit will fail with an exception. */
  protected static final int BUCKETS_MAX_NBR = 10000; // TODO: factorize in ES client later (with some generic ES calls)

  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  protected RestHighLevelClient elasticsearch;
  protected String datacubeName;
  protected String compositeAggregationName = "DATACUBE";
  protected ByteBuffer tmpBuffer = null;

  /*****************************************
  *
  *  constructor
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
  
  protected abstract String getDataESIndex();
  protected abstract String getDatacubeESIndex();
  protected abstract List<String> getFilterFields();
  protected abstract List<CompositeValuesSourceBuilder<?>> getFilterComplexSources();
  protected abstract List<AggregationBuilder> getDataAggregations();
  protected abstract void runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException;
  protected abstract void addStaticFilters(Map<String, Object> filters);
  protected abstract void embellishFilters(Map<String, Object> filters);
  protected abstract Map<String, Object> extractData(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException;
  
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
  *****************************************/
  
  protected String extractDocumentIDFromFilter(Map<String,Object> filter) 
  {
    Set<String> keySet = filter.keySet();
    
    if(tmpBuffer == null) {
      tmpBuffer = ByteBuffer.allocate(Integer.BYTES * keySet.size());
    } else {
      tmpBuffer.rewind();
    }
    
    for(String key: keySet) {
      tmpBuffer.putInt(filter.get(key).toString().hashCode());
    }
    
    return Base64.getUrlEncoder().encodeToString(tmpBuffer.array());
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
    List<AggregationBuilder> datacubeDataAggregations = getDataAggregations();
    
    //
    // Composite sources are created from datacube filters and complex sources (scripts)
    //
    
    List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
    for(String datacubeFilter: datacubeFilterFields) {
      TermsValuesSourceBuilder sourceTerms = new TermsValuesSourceBuilder(datacubeFilter).field(datacubeFilter);
      sources.add(sourceTerms);
    }
    for(CompositeValuesSourceBuilder<?> complexSources: datacubeFilterComplexSources) {
      sources.add(complexSources);
    }
    
    CompositeAggregationBuilder compositeAggregation = AggregationBuilders.composite(compositeAggregationName, sources)
        .size(BUCKETS_MAX_NBR);

    //
    // Data aggregations
    //
    
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
  
  private List<Map<String,Object>> extractDatacubeRows(SearchResponse response) throws ClassCastException 
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

      //
      // Extract filter 
      //
      
      Map<String, Object> filters = bucket.getKey();
      
      //
      // Add static filters that will be used for ID generation (such as date for instance)
      //
      
      addStaticFilters(filters);
      
      //
      // Extract documentID
      //
      
      String documentID = extractDocumentIDFromFilter(filters);
      
      //
      // Embellish filters for Kibana (display names)
      //
      
      embellishFilters(filters);
      
      //
      // Extract data 
      //
      
      Map<String, Object> data = extractData(bucket, filters);
      
      //
      // Result
      //

      HashMap<String,Object> datacubeRow = new HashMap<String,Object>();
      datacubeRow.put("_id",  documentID); // _id field is a temporary variable and will be removed before insertion
      datacubeRow.put("computationDate", computationDate);
      datacubeRow.put("count", docCount);
      
      //
      // Flat filter fields for Kibana
      //
      
      for(String filter: filters.keySet()) {
        datacubeRow.put("filter." + filter, filters.get(filter));
      }
      
      //
      // Flat data fields for Kibana
      //
      
      for(String dataKey : data.keySet()) {
        datacubeRow.put("data." + dataKey, data.get(dataKey));
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
  
  protected void run() 
  {  
    try 
      {
        //
        // Pre-generation phase (for retrieving some mapping infos)
        //
        
        runPreGenerationPhase();
        
        //
        // Generate Elasticsearch request
        //
        
        SearchRequest request = getElasticsearchRequest();
        log.info("[{}]: executing ES request: {}", this.datacubeName, request);
        
        //
        // Execute Elasticsearch request
        //

        SearchResponse response = executeESSearchRequest(request);
        if(response == null) {
          log.warn("[{}]: cannot retrieve any ES response, datacube generation stop here.", this.datacubeName);
          return;
        }
        log.debug("[{}]: retrieving ES response: {}", this.datacubeName, response);
        
        //
        // Extract datacube rows from JSON response
        //

        log.info("[{}]: extracting data from ES response.", this.datacubeName);
        List<Map<String,Object>> datacubeRows = extractDatacubeRows(response);
        
        //
        // Push datacube rows in Elasticsearch
        //

        log.info("[{}]: pushing {} datacube rows in ES.", this.datacubeName, datacubeRows.size());
        pushDatacubeRows(datacubeRows);
      } 
    catch(IOException|RuntimeException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("[{}]: generation failed: {}", this.datacubeName, stackTraceWriter.toString());
      }
  }
}
