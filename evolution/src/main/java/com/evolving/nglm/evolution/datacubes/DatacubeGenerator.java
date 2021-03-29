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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.HashMap;
import java.util.LinkedList;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
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

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.RLMDateUtils.DatePattern;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

/**
 * DatacubeGenerator is NOT thread-safe ! - Should only be used in mono-thread.
 */
public abstract class DatacubeGenerator
{
  /*****************************************
  *
  * Static
  *
  *****************************************/
  private static final Logger logger = LoggerFactory.getLogger(DatacubeGenerator.class);
  
  protected static final String UNDEFINED_BUCKET_VALUE = "undefined";
  
  /*****************************************
  *
  * Logger
  *
  *****************************************/
  public class DatacubeLogger 
  {
    DatacubeGenerator dg;
    
    public DatacubeLogger(DatacubeGenerator dg) {
      this.dg = dg;
    }
    
    public void error(String s) { logger.error("["+dg.getDatacubeName()+"]: " + s); }
    public void warn(String s) { logger.warn("["+dg.getDatacubeName()+"]: " + s); }
    public void info(String s) { logger.info("["+dg.getDatacubeName()+"]: " + s); }
    public void debug(String s) { logger.debug("["+dg.getDatacubeName()+"]: " + s); }
    public void trace(String s) { logger.trace("["+dg.getDatacubeName()+"]: " + s); }
  }
  protected DatacubeLogger log;
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  protected DatacubeWriter datacubeWriter;
  protected ElasticsearchClientAPI elasticsearch;
  protected String datacubeName;
  protected ByteBuffer tmpBuffer = null;
  
  protected int tenantID;
  protected String timeZone;
  protected SimpleDateFormat timestampFormat;
  protected SimpleDateFormat dayFormat;

  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public DatacubeGenerator(String datacubeName, ElasticsearchClientAPI elasticsearch, DatacubeWriter datacubeWriter, int tenantID, String timeZone) 
  {
    this.log = new DatacubeLogger(this);
    
    this.datacubeName = datacubeName;
    this.elasticsearch = elasticsearch;
    this.datacubeWriter = datacubeWriter;
    
    this.tenantID = tenantID;
    this.timeZone = timeZone;
    this.timestampFormat = RLMDateUtils.createLocalDateFormat(DatePattern.ELASTICSEARCH_UNIVERSAL_TIMESTAMP, timeZone);
    this.dayFormat = RLMDateUtils.createLocalDateFormat(DatePattern.LOCAL_DAY, timeZone);
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
  
  protected String getDocumentID(Map<String,Object> filters, String timestamp) 
  {
    // @param mode can be used to distinguish several type of publishing mode (hourly, daily)
    return this.extractDocumentIDFromFilter(filters, timestamp, "default");
  }

  /*****************************************
  *
  * Tenant & Date Utils
  *
  *****************************************/
  protected int getTenantID() 
  {
    return this.tenantID;
  }
  
  protected String getTimeZone() 
  { 
    return this.timeZone; 
  }
  
  protected String printTimestamp(Date date) 
  {
    return this.timestampFormat.format(date);
  }
  
  protected String printDay(Date date)
  {
    return this.dayFormat.format(date);
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
  *  rl: Also, hash could create collisions resulting in a bug hard to understand.
  *
  *****************************************/
  protected String extractDocumentIDFromFilter(Map<String,Object> filters, String timestamp, String mode) 
  {
    List<String> sortedkeys = new ArrayList<String>(filters.keySet());
    Collections.sort(sortedkeys);
    
    int bufferCapacity = Integer.BYTES * (sortedkeys.size() + 2);
    if(tmpBuffer == null || tmpBuffer.capacity() != bufferCapacity) {
      tmpBuffer = ByteBuffer.allocate(bufferCapacity);
    } else {
      tmpBuffer.rewind(); // re-use the buffer to optimize allocate calls.
    }
    
    tmpBuffer.putInt(timestamp.hashCode());
    tmpBuffer.putInt(mode.hashCode());
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
  protected Map<String,Object> extractRow(Map<String, Object> filters, long count, String timestamp, long period, Map<String, Long> metrics) 
  {
    //
    // Extract documentID from filters, timestamp
    //
    String documentID = getDocumentID(filters, timestamp);
    
    //
    // Embellish filters for Kibana/Grafana (display names), they won't be taken into account for documentID
    //
    embellishFilters(filters);
    
    //
    // Extract special filters (timestamp and period)
    //
    Object timestampOverride = filters.remove("timestamp");
    Object periodOverride = filters.remove("period");
    if(timestampOverride != null) {
      timestamp = (String) timestampOverride;
    }
    if(periodOverride != null) {
      period = (Long) period;
    }
    
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
  *  pushDatacubeRows
  *
  *****************************************/
  private void pushDatacubeRows(List<Map<String,Object>> datacubeRows) throws ElasticsearchException, IOException 
  {
    List<UpdateRequest> list = new LinkedList<UpdateRequest>();
    
    for(Map<String,Object> datacubeRow: datacubeRows) {
      String documentID = (String) datacubeRow.remove("_id");
      
      UpdateRequest request = new UpdateRequest(getDatacubeESIndex(), documentID);
      request.doc(datacubeRow);
      request.docAsUpsert(true);
      request.retryOnConflict(4);
      
      list.add(request);
    }
    
    datacubeWriter.getDatacubeBulkProcessor().add(list);
  }

  /*****************************************
  *
  * Generate datacube
  *
  *****************************************/
  private void generate(String timestamp, long period) 
  {  
    try 
      {
        //
        // Pre-generation phase (for retrieving some mapping infos)
        //
        log.debug("Running pre-generation phase.");
        boolean success = runPreGenerationPhase();
        if(!success) {
          log.info("Stopped in pre-generation phase.");
          return;
        }
        
        //
        // Generate Elasticsearch request
        //
        SearchRequest request = getElasticsearchRequest();
        log.info("Executing Elasticsearch request: "+request+"");
        
        //
        // Send Elasticsearch request
        //
        SearchResponse response;
        try 
          {
            response = this.elasticsearch.search(request, RequestOptions.DEFAULT);
          } 
        catch(ElasticsearchException e)
          {
            if (e.status() == RestStatus.NOT_FOUND) {
              log.warn("Elasticsearch index ["+request.indices()[0] +"] does not exist. Datacube generation stop here.");
              return;
            } 
            else {
              throw e;
            }
          }
        log.debug("Retrieving Elasticsearch response: "+response+"");
        
        //
        // Extract datacube rows from JSON response
        //
        log.debug("Extracting data from ES response.");
        List<Map<String,Object>> datacubeRows = extractDatacubeRows(response, timestamp, period);
        
        //
        // Push datacube rows in Elasticsearch
        //
        log.info("Data were successfully aggregated in "+datacubeRows.size()+" row(s). They will be pushed in ["+this.getDatacubeESIndex()+"] index later.");
        pushDatacubeRows(datacubeRows);
      } 
    catch(IOException|RuntimeException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Generation failed: "+stackTraceWriter.toString()+"");
      }
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/
  protected void run(String timestamp, long period) 
  {
    // 
    // Prevent writing during the datacube generation
    //
    datacubeWriter.pause();
    
    //
    // Generate aggregated data
    //
    generate(timestamp, period);
    
    //
    // Restart writing if allowed
    //
    datacubeWriter.restart();
  }
}
