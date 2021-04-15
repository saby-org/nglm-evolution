package com.evolving.nglm.evolution.datacubes;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

/**
 * This class must be thread-safe because it will be called by dedicated threads of every datacubes at the same time.
 * This class will push datacube rows in Elasticsearch only when no datacube generation are running.
 */
public class DatacubeWriter
{
  /*****************************************
  *
  * Static
  *
  *****************************************/
  private static final Logger log = LoggerFactory.getLogger(DatacubeWriter.class);
  
  private static final int BULK_REQUEST_NUMBER_LIMIT = 1000;
  private static final long BULK_REQUEST_SIZE_LIMIT = new ByteSizeValue(5, ByteSizeUnit.MB).getBytes(); // 5MB
  private static final long WRITE_PRIORITY_THRESHOLD = 100000; // Number of actions waiting to be written threshold. If reached, it will trigger the priority to the write and stop any reading task (datacube generation)
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private ElasticsearchClientAPI elasticsearch;
  private DatacubeBulkProcessor bulkProcessor;
  private int activeReader;
  private Object activeReaderSetterLock;

  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public DatacubeWriter(ElasticsearchClientAPI elasticsearch) 
  {
    this.elasticsearch = elasticsearch;
    this.bulkProcessor = new DatacubeBulkProcessor(BULK_REQUEST_NUMBER_LIMIT, BULK_REQUEST_SIZE_LIMIT);
    this.activeReader = 0;
    this.activeReaderSetterLock = new Object();
  }
  
  /*****************************************
  *
  * Write
  *
  *****************************************/
  private void write(BulkRequest request) {
    if(request.numberOfActions() == 0) {
      return;
    }
    
    try {
      log.info("[DatacubeWriter]: Pushing {} document(s) in Elasticsearch.", request.numberOfActions());
      BulkResponse bulkResponse = elasticsearch.bulk(request, RequestOptions.DEFAULT);
    }
    catch(IOException|RuntimeException e) {
      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.error("[DatacubeWriter]: Failed to push a bulk request. {}", stackTraceWriter.toString());
    }
  }
  
  /*
   * @return the number of documents remaining.
   */
  private long writeNext() {
    BulkRequest bulk = bulkProcessor.pop();
    this.write(bulk);
    
    long n = bulkProcessor.getTotalNumber();
    log.info("[DatacubeWriter]: There is {} document(s) waiting to be pushed in Elasticsearch.", n);
    return n;
  }
  
  // Only used when overloaded. Will stop every reader (datacube generator) !
  // Should not be used in nominal case
  private void forceWrite() {
    log.warn("[DatacubeWriter]: Waiting queue is OVERLOADED. Start of RECOVER phase. Priority has been switched to writing task. Every datacube tasks will pause and wait.");
    // This lock is taken and will stop every datacube generation trying to take the resource (by calling pause() ).
    synchronized(activeReaderSetterLock) {
      long remaining;
      do {
        remaining = writeNext();
      } while(remaining != 0);
    }
    log.warn("[DatacubeWriter]: End of RECOVER phase. Priority has been switched back to reading tasks.");
  }
  
  /*****************************************
  *
  * Getter
  *
  *****************************************/
  public DatacubeBulkProcessor getDatacubeBulkProcessor() {
    return this.bulkProcessor;
  }
  
  /*****************************************
  *
  * External API
  *
  *****************************************/
  /* 
   * Called by every datacube generation at the beginning.
   * The datacube generation prevent any other writing while it is aggregation data (reader priority).
   */
  public void pause() {
    synchronized(activeReaderSetterLock) {
      activeReader++;
      log.info("[DatacubeWriter]: Pause() - activeReader={}", activeReader);
    }
  }
  
  /*
   * Called by every datacube generation at the end.
   * The datacube generation, take care of pushing waiting rows in Elasticsearch if writing is available (e.g. no one is reading)
   * Therefore, the push in Elasticsearch will be executing in a dedicated thread of one datacube generator, but not necessarily the one that generated those rows.
   */
  public void restart() {
    boolean writingAllowed = false;    
    synchronized(activeReaderSetterLock) {
      activeReader--;
      log.info("[DatacubeWriter]: Restart() - activeReader={}", activeReader);
      writingAllowed = (activeReader == 0);
    }
    
    long remaining = bulkProcessor.getTotalNumber();
    if(remaining >= WRITE_PRIORITY_THRESHOLD) {
      forceWrite();
      return;
    }
    
    while(writingAllowed && remaining != 0) {
      remaining = writeNext();

      synchronized(activeReaderSetterLock) {
        writingAllowed = (activeReader == 0);
      }
    }
  }
  
}
