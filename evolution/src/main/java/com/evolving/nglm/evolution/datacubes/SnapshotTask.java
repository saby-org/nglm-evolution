package com.evolving.nglm.evolution.datacubes;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;

public class SnapshotTask
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(SnapshotTask.class);

  //
  //  others
  //
  
  protected static final DateFormat DATE_FORMAT;
  static
  {
    DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
  }
  
  /*****************************************
  *
  *  Data
  *
  *****************************************/
  
  private RestHighLevelClient elasticsearch;
  private String snapshotName;
  private String srcIndex;
  private String destIndexPrefix;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public SnapshotTask(String snapshotName, String srcIndex, String destIndexPrefix, RestHighLevelClient elasticsearch) 
  {
    this.elasticsearch = elasticsearch;
    this.snapshotName = snapshotName;
    this.srcIndex = srcIndex;
    this.destIndexPrefix = destIndexPrefix;
  }

  /*****************************************
  *
  *  get
  *
  *****************************************/
  private String getDestinationIndex(String date) {
    return this.destIndexPrefix + "-" + date;
  }
  
  /*****************************************
  *
  *  run
  *
  *****************************************/
  
  public void run(Date snapshotDate) 
  {
    String requestedDate = DATE_FORMAT.format(snapshotDate);
    
    try
      {
        String destIndex = getDestinationIndex(requestedDate);
        
        ReindexRequest request = new ReindexRequest().setSourceIndices(srcIndex).setDestIndex(destIndex);
        BulkByScrollResponse response = elasticsearch.reindex(request, RequestOptions.DEFAULT);
        
        if(response.isTimedOut()) {
          log.error("[{}]: snapshot task timed-out.", this.snapshotName);
          return;
        } else if (!response.getBulkFailures().isEmpty()) {
          String failureMessages = "";
          for(Failure failure : response.getBulkFailures())
            {
              failureMessages += "- " + failure.getMessage() + " -";
            }
          log.error("[{}]: snapshot task failed ({} reasons: {}).", this.snapshotName, response.getBulkFailures().size(), failureMessages);
          return;
        } else {
          log.info("[{}]: successful snapshot of {} index in {}. Copied {} documents in {} seconds.", this.snapshotName, srcIndex, destIndex, response.getTotal(), response.getTook().getSeconds());
        }
        
      }
    catch(IOException|ElasticsearchException|ClassCastException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("[{}]: snapshot task failed: {}", this.snapshotName, stackTraceWriter.toString());
      }
  }
}
