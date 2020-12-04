package com.evolving.nglm.evolution.datacubes;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public abstract class DatacubeGenerator
{
  /*****************************************
  *
  * Static
  *
  *****************************************/
  protected static final Logger log = LoggerFactory.getLogger(DatacubeGenerator.class);
  
  protected static final String UNDEFINED_BUCKET_VALUE = "undefined";
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  protected ElasticsearchClientAPI elasticsearch;
  protected String datacubeName;
  protected ByteBuffer tmpBuffer = null;

  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public DatacubeGenerator(String datacubeName, ElasticsearchClientAPI elasticsearch) 
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
  // Datacube generation phases
  // 
  protected abstract boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException; // return false if generation must stop here.
  protected abstract SearchRequest getElasticsearchRequest();
  protected abstract List<Map<String,Object>> extractDatacubeRows(SearchResponse response, String timestamp, long period) throws ClassCastException;
  
  //
  // Row extraction
  //
  protected abstract void embellishFilters(Map<String, Object> filters);
  

  /*****************************************
  *
  *  overridable functions
  *
  *****************************************/
  protected String getDatacubeName() 
  {
    return this.datacubeName;
  }
  
  //
  // DocumentID settings
  //
  protected void addStaticFilters(Map<String, Object> filters) 
  { 
    return; 
  }
  
  protected String getDocumentID(Map<String,Object> filters, String timestamp) 
  {
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
    
    int bufferCapacity = Integer.BYTES * (sortedkeys.size() + 1);
    if(tmpBuffer == null || tmpBuffer.capacity() != bufferCapacity) {
      tmpBuffer = ByteBuffer.allocate(bufferCapacity);
    } else {
      tmpBuffer.rewind(); // re-use the buffer to optimize allocate calls.
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
  *  extractRow
  *
  *****************************************/
  protected Map<String,Object> extractRow(Map<String, Object> filters, long count, String timestamp, long period, Map<String, Object> metrics) 
  {
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
    // Build row
    //
    HashMap<String,Object> datacubeRow = new HashMap<String,Object>();
    datacubeRow.put("_id",  documentID); // _id field is a temporary variable and will be removed before insertion
    datacubeRow.put("timestamp", timestamp);
    datacubeRow.put("period", period);
    datacubeRow.put("count", count);
    
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
    
    return datacubeRow;
  }
  
  /*****************************************
  *
  *  executeESSearchRequest
  *
  *****************************************/
  private SearchResponse executeESSearchRequest(SearchRequest request) throws ElasticsearchException, IOException 
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
