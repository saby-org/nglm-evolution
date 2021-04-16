/****************************************************************************
*
*  class SimpleESSinkTask
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class SimpleESSinkTask extends SinkTask
{
  /*****************************************
  *
  *  config
  *
  *****************************************/

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(SimpleESSinkTask.class);
  
  //
  //  configuration
  //

  private String connectorName = null;
  private String indexName = null;
  private String pipelineName = null;    
  private int batchRecordCount;
  private long batchSize;
  private long closeTimeout;
  private int retries;
  private int taskNumber;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public int getTaskNumber() { return taskNumber; }
  protected String getDefaultIndexName() { return indexName; }
  protected String getPipelineName() { return pipelineName; }
  
  /****************************************
  *
  *  attributes
  *
  ****************************************/

  private ElasticsearchClientAPI client = null;
  private BulkProcessor bulkProcessor = null;
  private Throwable bulkFailure = null;
  private int consecutiveRetriableExceptionCount = 0;

  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  public abstract List<DocWriteRequest> getRequests(SinkRecord sinkRecord);

  /*****************************************
  *
  *  version
  *
  *****************************************/

  @Override public String version()
  {
    return SimpleESSinkConnector.SimpleESSinkVersion;
  }

  /*****************************************
  *
  *  start
  *
  *****************************************/

  @Override public void start(Map<String, String> taskConfig)
  {
    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("{} -- Task.start()", connectorName);

    /*****************************************
    *
    *  config
    *
    *****************************************/

    connectorName = taskConfig.get("connectorName");
    indexName = taskConfig.get("indexName");
    pipelineName = taskConfig.get("pipelineName");    
    batchRecordCount = SimpleESSinkConnector.parseIntegerConfig(taskConfig.get("batchRecordCount"));
    batchSize = SimpleESSinkConnector.parseLongConfig(taskConfig.get("batchSize"));
    closeTimeout = SimpleESSinkConnector.parseLongConfig(taskConfig.get("closeTimeout"));
    retries = SimpleESSinkConnector.parseIntegerConfig(taskConfig.get("retries"));
    taskNumber = SimpleESSinkConnector.parseIntegerConfig(taskConfig.get("taskNumber"));
    log.info("{} -- indexName: {}, pipelineName: {}, batchRecordCount: {}, batchSize: {}, closeTimeout: {}",
            connectorName, indexName, pipelineName, batchRecordCount, batchSize, closeTimeout);

    /*****************************************
    *
    *  initialize client, bulkProcessor
    *
    *****************************************/

    try
      {

        client = new ElasticsearchClientAPI(connectorName);

        //
        //  bulkProcessor
        //
        
        BulkProcessorListener listener = new BulkProcessorListener();
        BulkProcessor.Builder builder = BulkProcessor.builder((request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener);
        builder.setBulkActions(batchRecordCount);
        ByteSizeValue byteLimit = (batchSize == -1L) ? new ByteSizeValue(-1) : new ByteSizeValue(batchSize, ByteSizeUnit.MB);
        builder.setBulkSize(byteLimit);
        builder.setConcurrentRequests(1);
        int initialWait = Deployment.getConnectTaskInitialWait(connectorName);
        int retries = Deployment.getConnectTaskRetries(connectorName);
        builder.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(initialWait), retries));
        log.info("{} -- initialWait: {}, retries: {}", connectorName, initialWait, retries);        
        bulkProcessor = builder.build();
      }
    catch (ElasticsearchException e)
      {
        log.error("Error starting: {}", connectorName);
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error(stackTraceWriter.toString());
        throw new ConnectException(e);
      }
  }

  /*****************************************
  *
  *  put
  *
  *****************************************/

  @Override public void put(Collection<SinkRecord> sinkRecords)
  {
    /****************************************
    *
    *  handle any failures
    *
    ****************************************/

    handleBulkFailure();

    /****************************************
    *
    *  insert all records
    *
    ****************************************/

    if (sinkRecords.isEmpty())
      log.trace("{} -- Task.put() - 0 records.", connectorName);
    else
      log.info("{} -- Task.put() - {} records.", connectorName, sinkRecords.size());
    
    for (SinkRecord sinkRecord : sinkRecords)
      {
        for (DocWriteRequest request : getRequests(sinkRecord))
          {
            bulkProcessor.add(request);
          }
      }

    /****************************************
    *
    *  update statistics
    *
    ****************************************/

    updatePutCount(connectorName, taskNumber, 1);
  }

  /*****************************************
  *
  *  flush
  *
  *****************************************/

  @Override public void flush(Map<TopicPartition, OffsetAndMetadata> map)
  {
    log.trace("{} -- Task.flush()", connectorName);

    //
    //  flush
    //
    
    bulkProcessor.flush();
    
    //
    //  handle any failures
    //

    handleBulkFailure();
  }

  /*****************************************
  *
  *  processBulkFailure
  *
  *****************************************/

  private void handleBulkFailure()
  {
    if (bulkFailure != null)
      {
        log.error("{} -- ConnectException - {}", connectorName, bulkFailure.getMessage());

        //
        //  determine whether to retry
        // 

        boolean retry = false;
        retry = retry || bulkFailure instanceof IOException;

        //
        //  create appropriate exception -- retriable or standard
        //

        ConnectException connectException;
        if (retry && consecutiveRetriableExceptionCount <= retries)
          {
            connectException = new RetriableException(connectorName + " -- bulk request failure "+consecutiveRetriableExceptionCount, bulkFailure);
            consecutiveRetriableExceptionCount += 1;
          }
        else
          {
            connectException = new ConnectException(connectorName + " -- bulk request failure", bulkFailure);
          }

        //
        //  throw
        //
        
        bulkFailure = null;
        throw connectException;
      }
    else
      {
        consecutiveRetriableExceptionCount = 0;
      }
  }

  /*****************************************
  *
  *  stop
  *
  *****************************************/

  @Override public void stop()
  {
    log.info("{} -- Task.stop()", connectorName);

    /*****************************************
    *
    *  close the bulkProcessor
    *
    *****************************************/

    try
      {
        if (bulkProcessor != null) bulkProcessor.awaitClose(closeTimeout, TimeUnit.SECONDS);
      }
    catch (InterruptedException e)
      {
        log.error("{} -- Error stopping bulkProcessor", connectorName);
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error(stackTraceWriter.toString());
      }
    bulkProcessor = null;

    /*****************************************
    *
    *  close the client
    *
    *****************************************/
            
    try
      {
        if (client != null) client.closeCleanly();
      }
    catch (IOException e)
      {
        log.error("{} -- Error stopping client", connectorName);
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error(stackTraceWriter.toString());
      }
    client = null;
    
    /*****************************************
    *
    *  clear failure
    *
    *****************************************/
            
    bulkFailure = null;

    /*****************************************
    *
    *  unregister statistics
    *
    *****************************************/
    
    stopStatisticsCollection();
  }

  /****************************************************************************
  *
  *  BulkProcessorListener
  *
  ****************************************************************************/

  private class BulkProcessorListener implements BulkProcessor.Listener
  {
    /****************************************
    *
    *  beforeBulk
    *
    ****************************************/  

    @Override public void beforeBulk(long executionId, BulkRequest request)
    {
      if (request.numberOfActions() > 0)
        log.info("{} -- BulkProcessorListener.beforeBulk() - executing bulkRequest [{}] with {} records.", connectorName, executionId, request.numberOfActions());
      else
        log.trace("{} -- BulkProcessorListener.beforeBulk() - executing bulkRequest [{}] with {} records.", connectorName, executionId, request.numberOfActions());
      updateBatchCount(connectorName, taskNumber, 1);
      updateRecordCount(connectorName, taskNumber, request.numberOfActions());
    }

    /****************************************
    *
    *  afterBulk
    *  -- request succeeded
    *
    ****************************************/  

    @Override public void afterBulk(long executionId, BulkRequest request, BulkResponse response)
    {      
      /****************************************
      *
      *  log
      *
      ****************************************/

      if (! response.hasFailures())
        log.info("{} -- BulkProcessorListener.afterBulk() - bulkRequest [{}] completed in {} ms.", connectorName, executionId, response.getTook().getMillis());
      else
        log.error("{} -- BulkProcessorListener.afterBulk() - bulkRequest [{}] executed with failures.", connectorName, executionId);

      /****************************************
      *
      *  check each item response for failures
      *
      ****************************************/

      for (BulkItemResponse bulkItemResponse : response)
        {
          if (bulkItemResponse.isFailed())
            {
              log.error("{} -- BulkProcessorListener.afterBulk() - request {}/{}/{} [{}] failed [{}]: {}", connectorName, bulkItemResponse.getIndex(), bulkItemResponse.getType(), bulkItemResponse.getId(), bulkItemResponse.getVersion(), bulkItemResponse.status(), bulkItemResponse.getFailureMessage());
            }
          else
            {
              DocWriteResponse docWriteResponse = (DocWriteResponse) bulkItemResponse.getResponse();
              if (docWriteResponse.getShardInfo().getFailed() > 0)
                {
                  for (ReplicationResponse.ShardInfo.Failure failure : docWriteResponse.getShardInfo().getFailures())
                    {
                      log.error("{} -- BulkProcessorListener.afterBulk() - request {}/{}/{} [{}] had a shard failure: {}", connectorName, bulkItemResponse.getIndex(), bulkItemResponse.getType(), bulkItemResponse.getId(), bulkItemResponse.getVersion(), failure.reason());
                    }
                }
            }
        }      
    }

    /****************************************
    *
    *  afterBulk
    *  -- entire request failed
    *
    ****************************************/  

    @Override public void afterBulk(long executionId, BulkRequest request, Throwable failure)
    {
      log.error("{} -- BulkProcessorListener.afterBulk() - failed to execute bulkRequest [{}]: {}", connectorName, executionId, failure.toString());
      StringWriter stackTraceWriter = new StringWriter();
      failure.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.error(stackTraceWriter.toString());
      if (bulkFailure == null) bulkFailure = failure;
    }
  }  

  /****************************************************************************
  *
  *  statistics
  *    - updateBatchCount
  *    - updatePutCount
  *    - updateRecordCount
  *    - stopStatisticsCollection
  *
  ****************************************************************************/

  /*****************************************
  *
  *  statistics data (transient)
  *
  *****************************************/

  private String statisticsKey(String connector, int taskNumber) { return connector + "-" + taskNumber; }
  private static Map<String,SinkTaskStatistics> allTaskStatistics = new HashMap<String, SinkTaskStatistics>();

  /*****************************************
  *
  *  getStatistics
  *
  *****************************************/

  private SinkTaskStatistics getStatistics(String connectorName, int taskNumber)
  {
    synchronized (allTaskStatistics)
      {
        SinkTaskStatistics taskStatistics = allTaskStatistics.get(statisticsKey(connectorName, taskNumber));
        if (taskStatistics == null)
          {
            try
              {
                taskStatistics = new SinkTaskStatistics(connectorName, "Elasticsearch", taskNumber);
              }
            catch (ServerException se)
              {
                throw new ServerRuntimeException("Could not create statistics object", se);
              }
            allTaskStatistics.put(statisticsKey(connectorName, taskNumber), taskStatistics);
          }
        return taskStatistics;
      }
  }    

  /*****************************************
  *
  *  updateBatchCount
  *
  *****************************************/

  private void updateBatchCount(String connectorName, int taskNumber, int amount)
  {
    synchronized (allTaskStatistics)
      {
        SinkTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
        taskStatistics.updateBatchCount(amount);
      }
  }

  /*****************************************
  *
  *  updatePutCount
  *
  *****************************************/

  private void updatePutCount(String connectorName, int taskNumber, int amount)
  {
    synchronized (allTaskStatistics)
      {
        SinkTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
        taskStatistics.updatePutCount(amount);
      }
  }

  /*****************************************
  *
  *  updateRecordCount
  *
  *****************************************/

  private void updateRecordCount(String connectorName, int taskNumber, int amount)
  {
    synchronized (allTaskStatistics)
      {
        SinkTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
        taskStatistics.updateRecordCount(amount);
      }
  }

  /*****************************************
  *
  *  stopStatisticsCollection
  *
  *****************************************/

  private void stopStatisticsCollection()
  {
    synchronized (allTaskStatistics)
      {
        for (SinkTaskStatistics taskStatistics : allTaskStatistics.values())
          {
            taskStatistics.unregister();
          }
        allTaskStatistics.clear();
      }
  }
}
