/****************************************************************************
*
*  CleanupSubscriberESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CleanupSubscriberESSinkConnector extends SinkConnector
{
  /*****************************************
  *
  *  config
  *
  *****************************************/

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(CleanupSubscriberESSinkConnector.class);
  
  //
  //  configuration
  //

  private String connectorName = null;
  private String connectionHost = null;
  private String connectionPort = null;
  private String connectionUserName = null;
  private String connectionUserPassword = null;
  private String maxSubscribersPerRequest = null;
  private String connectTimeout = null;
  private String queryTimeout = null;
  private String closeTimeout = null;

  //
  //  version
  //

  final static String CleanupSubscriberESSinkVersion = "0.1";

  //
  //  default values
  //

  private static final String DEFAULT_MAXSUBSCRIBERSPERREQUEST = "500";
  private static final String DEFAULT_CONNECTTIMEOUT = "5";
  private static final String DEFAULT_QUERYTIMEOUT = "60";
  private static final String DEFAULT_CLOSETIMEOUT = "30";
  
  /****************************************
  *
  *  version
  *
  ****************************************/

  @Override public String version()
  {
    return CleanupSubscriberESSinkVersion;
  }

  /****************************************
  *
  *  start
  *
  ****************************************/

  @Override public void start(Map<String, String> properties)
  {
    /*****************************************
    *
    *  log
    *
    *****************************************/

    //
    //  configuration -- connectorName
    //

    connectorName = properties.get("name");

    //
    //  log -- start
    //

    log.info("{} -- Connector.start() START", connectorName);

    /*****************************************
    *
    *  configuration
    *
    *****************************************/

    //
    //  configuration -- connectionHost
    //

    connectionHost = properties.get("connectionHost");
    if (connectionHost == null || connectionHost.trim().length() == 0) throw new ConnectException("CleanupSubscriberESSinkConnector configuration must specify 'connectionHost'");

    //
    //  configuration -- connectionPort
    //

    connectionPort = properties.get("connectionPort");
    if (! validIntegerConfig(connectionPort)) throw new ConnectException("CleanupSubscriberESSinkConnector configuration field 'connectionPort' is a required integer");    
    
    //
    //  configuration -- connectionUserName
    //

    connectionUserName = properties.get("connectionUserName");
    if (connectionUserName == null || connectionUserName.trim().length() == 0) throw new ConnectException("SimpleESSinkConnector configuration must specify 'connectionUserName'");
    
    //
    //  configuration -- connectionUserPassword
    //

    connectionUserPassword = properties.get("connectionUserPassword");
    if (connectionUserPassword == null || connectionUserPassword.trim().length() == 0) throw new ConnectException("SimpleESSinkConnector configuration must specify 'connectionUserPassword'");

    //
    //  configuration -- maxSubscribersPerRequest
    //

    String maxSubscribersPerRequestString = properties.get("maxSubscribersPerRequest");
    maxSubscribersPerRequest = (maxSubscribersPerRequestString != null && maxSubscribersPerRequestString.trim().length() > 0) ? maxSubscribersPerRequestString : DEFAULT_MAXSUBSCRIBERSPERREQUEST;
    if (! validIntegerConfig(maxSubscribersPerRequest)) throw new ConnectException("CleanupSubscriberESSinkConnector configuration field 'maxSubscribersPerRequest' must be an integer");
    
    //
    //  configuration -- connectTimeout
    //

    String connectTimeoutString = properties.get("connectTimeout");
    connectTimeout = (connectTimeoutString != null && connectTimeoutString.trim().length() > 0) ? connectTimeoutString : DEFAULT_CONNECTTIMEOUT;
    if (! validIntegerConfig(connectTimeout)) throw new ConnectException("CleanupSubscriberESSinkConnector configuration field 'connectTimeout' must be an integer");

    //
    //  configuration -- queryTimeout
    //

    String queryTimeoutString = properties.get("queryTimeout");
    queryTimeout = (queryTimeoutString != null && queryTimeoutString.trim().length() > 0) ? queryTimeoutString : DEFAULT_QUERYTIMEOUT;
    if (! validIntegerConfig(queryTimeout)) throw new ConnectException("CleanupSubscriberESSinkConnector configuration field 'queryTimeout' must be an integer");

    //
    //  configuration -- closeTimeout
    //

    String closeTimeoutString = properties.get("closeTimeout");
    closeTimeout = (closeTimeoutString != null && closeTimeoutString.trim().length() > 0) ? closeTimeoutString : DEFAULT_CLOSETIMEOUT;
    if (! validLongConfig(closeTimeout)) throw new ConnectException("CleanupSubscriberESSinkConnector configuration field 'closeTimeout' must be a long");
    
    /*****************************************
    *
    *  log
    *
    *****************************************/
    
    log.info("{} -- Connector.start() END", connectorName);
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return CleanupSubscriberESSinkTask.class;
  }

  /****************************************
  *
  *  taskConfigs
  *
  ****************************************/

  @Override public List<Map<String, String>> taskConfigs(int maxTasks)
  {
    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("{} -- taskConfigs() START", connectorName);
    log.info("{} -- taskConfigs() {} maxTasks", connectorName, maxTasks);

    /*****************************************
    *
    *  create N task configs
    *
    *****************************************/
    
    List<Map<String, String>> result = new ArrayList<Map<String,String>>();
    for (int i = 0; i < maxTasks; i++)
      {
        Map<String, String> taskConfig = new HashMap<>();
        taskConfig.put("connectorName", connectorName);
        taskConfig.put("connectionHost", connectionHost);
        taskConfig.put("connectionPort", connectionPort);
        taskConfig.put("connectionUserName", connectionUserName);
        taskConfig.put("connectionUserPassword", connectionUserPassword);
        taskConfig.put("maxSubscribersPerRequest", maxSubscribersPerRequest);
        taskConfig.put("connectTimeout", connectTimeout);
        taskConfig.put("queryTimeout", queryTimeout);
        taskConfig.put("closeTimeout", closeTimeout);
        taskConfig.put("taskNumber", Integer.toString(i));
        result.add(taskConfig);
      }

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("{} -- taskConfigs() END", connectorName);

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return result;
  }

  /****************************************
  *
  *  stop
  *
  ****************************************/

  @Override public void stop()
  {
    log.info("{} -- Connector.stop()", connectorName);
  }

  /****************************************
  *
  *  config
  *
  ****************************************/

  @Override public ConfigDef config()
  {
    ConfigDef result = new ConfigDef();
    result.define("connectionHost", Type.STRING, Importance.HIGH, "elastic search hostname");
    result.define("connectionPort", Type.STRING, Importance.HIGH, "elastic search http port");
    result.define("connectionUserName", Type.STRING, Importance.HIGH, "elastic search username for authentication");
    result.define("connectionUserPassword", Type.STRING, Importance.HIGH, "elastic search password for authentication");
    result.define("maxSubscribersPerRequest", Type.STRING, DEFAULT_MAXSUBSCRIBERSPERREQUEST, Importance.LOW, "number of subscriber ids to submit in a single request");
    result.define("connectTimeout", Type.STRING, DEFAULT_CONNECTTIMEOUT, Importance.MEDIUM, "timeout to wait for connection, in seconds");
    result.define("queryTimeout", Type.STRING, DEFAULT_QUERYTIMEOUT, Importance.MEDIUM, "timeout to wait for query, in seconds");
    result.define("closeTimeout", Type.STRING, DEFAULT_CLOSETIMEOUT, Importance.MEDIUM, "timeout to wait for records to finish when stopping, in seconds");
    return result;
  }

  /****************************************************************************
  *
  *  support
  *
  ****************************************************************************/

  /*****************************************
  *
  *  validIntegerConfig
  *
  *****************************************/

  private static boolean validIntegerConfig(String attribute)
  {
    boolean valid = true;
    if (attribute == null || attribute.trim().length() == 0)
      {
        valid = false;
      }
    else
      {
        try
          {
            int result = Integer.parseInt(attribute);
            if (result <= 0) valid = false;
          }
        catch (NumberFormatException e)
          {
            valid = false;
          }
      }
    return valid;
  }
  
  /*****************************************
  *
  *  validLongConfig
  *
  *****************************************/

  private static boolean validLongConfig(String attribute)
  {
    boolean valid = true;
    if (attribute == null || attribute.trim().length() == 0)
      {
        valid = false;
      }
    else
      {
        try
          {
            long result = Long.parseLong(attribute);
            if (result <= 0) valid = false;
          }
        catch (NumberFormatException e)
          {
            valid = false;
          }
      }
    return valid;
  }

  /****************************************************************************
  *
  *  utilities
  *
  ****************************************************************************/
  
  /*****************************************
  *
  *  parseIntegerConfig
  *
  *****************************************/

  protected static Integer parseIntegerConfig(String attribute)
  {
    try
      {
        return (attribute != null && attribute.trim().length() != 0) ? Integer.parseInt(attribute) : null;
      }
    catch (NumberFormatException e)
      {
        throw new ServerRuntimeException("invalid config", e);
      }
  }

  /*****************************************
  *
  *  parseLongConfig
  *
  *****************************************/

  protected static Long parseLongConfig(String attribute)
  {
    try
      {
        return (attribute != null && attribute.trim().length() != 0) ? Long.parseLong(attribute) : null;
      }
    catch (NumberFormatException e)
      {
        throw new ServerRuntimeException("invalid config", e);
      }
  }

  /****************************************************************************
  *
  *  class CleanupSubscriberESSinkTask
  *
  ****************************************************************************/

  public static class CleanupSubscriberESSinkTask extends SinkTask
  {
    /*****************************************
    *
    *  config
    *
    *****************************************/

    //
    //  logger
    //

    protected static final Logger log = LoggerFactory.getLogger(CleanupSubscriberESSinkTask.class);

    //
    //  configuration
    //

    private String connectorName = null;
    private String connectionHost = null;
    private int connectionPort;
    private String connectionUserName = null;
    private String connectionUserPassword = null;
    private int maxSubscribersPerRequest;
    private int connectTimeout;
    private int queryTimeout;
    private long closeTimeout;
    private int taskNumber;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public int getTaskNumber() { return taskNumber; }

    /****************************************
    *
    *  attributes
    *
    ****************************************/

    private ElasticsearchClientAPI client = null;

    /*****************************************
    *
    *  version
    *
    *****************************************/

    @Override public String version()
    {
      return CleanupSubscriberESSinkConnector.CleanupSubscriberESSinkVersion;
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
      connectionHost = taskConfig.get("connectionHost");
      connectionUserName = taskConfig.get("connectionUserName");
      connectionUserPassword = taskConfig.get("connectionUserPassword");
      connectionPort = CleanupSubscriberESSinkConnector.parseIntegerConfig(taskConfig.get("connectionPort"));
      maxSubscribersPerRequest = CleanupSubscriberESSinkConnector.parseIntegerConfig(taskConfig.get("maxSubscribersPerRequest"));
      connectTimeout = CleanupSubscriberESSinkConnector.parseIntegerConfig(taskConfig.get("connectTimeout"));
      queryTimeout = CleanupSubscriberESSinkConnector.parseIntegerConfig(taskConfig.get("queryTimeout"));
      closeTimeout = CleanupSubscriberESSinkConnector.parseLongConfig(taskConfig.get("closeTimeout"));
      taskNumber = CleanupSubscriberESSinkConnector.parseIntegerConfig(taskConfig.get("taskNumber"));
      log.info("{} -- connectionHost: {}, connectionPort: {}, maxSubscribersPerRequest: {}, connectTimeout: {}, queryTimeout {}, closeTimeout: {}", connectorName, connectionHost, connectionPort, maxSubscribersPerRequest, connectTimeout, queryTimeout, closeTimeout);

      /*****************************************
      *
      *  initialize client, bulkProcessor
      *
      *****************************************/

      try
        {
          client = new ElasticsearchClientAPI(connectionHost, connectionPort, connectTimeout*1000, queryTimeout*1000, connectionUserName, connectionUserPassword);
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
      *  identify subscriberIDs
      *
      ****************************************/

      if (sinkRecords.size() > 0)
        log.info("{} -- Task.put() - {} records.", connectorName, sinkRecords.size());
      else
        log.trace("{} -- Task.put() - {} records.", connectorName, sinkRecords.size());
      List<String> subscriberIDs = new ArrayList<String>();
      for (SinkRecord sinkRecord : sinkRecords)
        {
          Object cleanupSubscriberValue = sinkRecord.value();
          Schema cleanupSubscriberValueSchema = sinkRecord.valueSchema();
          CleanupSubscriber cleanupSubscriber = CleanupSubscriber.unpack(new SchemaAndValue(cleanupSubscriberValueSchema, cleanupSubscriberValue));
          subscriberIDs.add(cleanupSubscriber.getSubscriberID());
        }

      /****************************************
      *
      *  delete from elasticsearch (if indexes are defined)
      *
      ****************************************/

      if (Deployment.getSubscriberESIndexes().size() > 0)
        {
          int processed = 0;
          while (processed < subscriberIDs.size())
            {
              //
              //  subscriberIDs
              //

              int start = processed;
              int end = Math.min(processed + maxSubscribersPerRequest, subscriberIDs.size());
              String[] currentSubscriberIDs = subscriberIDs.subList(start, end).toArray(new String[0]);
              processed = end;

              //
              //  delete - retrying on failures
              //

              try
                {
                  //
                  //  delete
                  //

                  DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(Deployment.getSubscriberESIndexes().toArray(new String[0]));
                  deleteByQueryRequest.setQuery(QueryBuilders.termsQuery("subscriberID", currentSubscriberIDs));
                  deleteByQueryRequest.setConflicts("proceed");
                  deleteByQueryRequest.setSlices(DeleteByQueryRequest.AUTO_SLICES);
                  deleteByQueryRequest.setScroll(TimeValue.timeValueMinutes(5));
                  BulkByScrollResponse response = client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);

                  //
                  //  abort (and retry) on failures
                  //

                  if (response.getBulkFailures().size() > 0)
                    {
                      log.error("{} -- failed to delete subscribers from indices", connectorName);
                      Exception e = null;
                      for (BulkItemResponse.Failure failure: response.getBulkFailures())
                        {
                          log.error("{} -- delete from index {} failed with message '{}' and cause '{}'", connectorName, failure.getIndex(), failure.getMessage(), failure.getCause());
                          e = failure.getCause();
                        }
                      throw new RetriableException(e);
                    }
                }
              catch (IOException e)
                {
                  log.error("{} -- failed to delete subscribers from indices: {}", connectorName, e.getMessage());
                  throw new RetriableException(e);
                }
            }
        }
      
      /****************************************
      *
      *  update statistics
      *
      ****************************************/

      updatePutCount(connectorName, taskNumber, 1);
      updateRecordCount(connectorName, taskNumber, subscriberIDs.size());
    }

    /*****************************************
    *
    *  flush
    *
    *****************************************/

    @Override public void flush(Map<TopicPartition, OffsetAndMetadata> map)
    {
      log.trace("{} -- Task.flush()", connectorName);
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
      *  close the client
      *
      *****************************************/

      try
        {
          if (client != null) client.close();
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
      *  unregister statistics
      *
      *****************************************/

      stopStatisticsCollection();
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
}
