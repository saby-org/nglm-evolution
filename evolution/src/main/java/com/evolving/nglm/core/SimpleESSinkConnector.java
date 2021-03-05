/****************************************************************************
*
*  SimpleESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class SimpleESSinkConnector extends SinkConnector
{
  /*****************************************
  *
  *  config
  *
  *****************************************/

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(SimpleESSinkConnector.class);
  
  //
  //  configuration
  //

  private String connectorName = null;
  private String indexName = null;
  private String pipelineName = null;  
  private String batchRecordCount = null;
  private String batchSize = null;
  private String closeTimeout = null;
  private String retries = null;

  //
  //  version
  //

  final static String SimpleESSinkVersion = "0.1";

  //
  //  default values
  //

  private static final String DEFAULT_BATCHRECORDCOUNT = "-1";
  private static final String DEFAULT_BATCHSIZE = "5";
  private static final String DEFAULT_CLOSETIMEOUT = "30";
  private static final String DEFAULT_PIPELINENAME = "";
  private static final String DEFAULT_RETRIES = Integer.MAX_VALUE+"";
  
  /****************************************
  *
  *  version
  *
  ****************************************/

  @Override public String version()
  {
    return SimpleESSinkVersion;
  }

  /****************************************
  *
  *  start
  *
  ****************************************/

  @Override public void start(Map<String, String> properties)
  {

    connectorName = properties.get("name");
    log.info("{} -- Connector.start() START", connectorName);

    indexName = properties.get("indexName");
    if (indexName == null || indexName.trim().length() == 0) throw new ConnectException("SimpleESSinkConnector configuration must specify 'indexName'");

    //
    //  configuration -- pipelineName(optional)
    //

    pipelineName = properties.get("pipelineName") != null ? properties.get("pipelineName") : "";

    //
    //  configuration -- batchRecordCount
    //

    String batchRecordCountString = properties.get("batchRecordCount");
    batchRecordCount = (batchRecordCountString != null && batchRecordCountString.trim().length() > 0) ? batchRecordCountString : DEFAULT_BATCHRECORDCOUNT;
    if (! validIntegerConfig(batchRecordCount, true)) throw new ConnectException("SimpleESSinkConnector configuration field 'batchRecordCount' must be an integer");

    //
    //  configuration -- batchSize
    //

    String batchSizeString = properties.get("batchSize");
    batchSize = (batchSizeString != null && batchSizeString.trim().length() > 0) ? batchSizeString : DEFAULT_BATCHSIZE;
    if (! validLongConfig(batchSize, true)) throw new ConnectException("SimpleESSinkConnector configuration field 'batchSize' must be a long");

    //
    //  configuration -- closeTimeout
    //

    String closeTimeoutString = properties.get("closeTimeout");
    closeTimeout = (closeTimeoutString != null && closeTimeoutString.trim().length() > 0) ? closeTimeoutString : DEFAULT_CLOSETIMEOUT;
    if (! validLongConfig(closeTimeout, true)) throw new ConnectException("SimpleESSinkConnector configuration field 'closeTimeout' must be a long");

    //
    //  configuration -- retries
    //

    String retriesString = properties.get("retries");
    retries = (retriesString != null && retriesString.trim().length() > 0) ? retriesString : DEFAULT_RETRIES;
    if (! validIntegerConfig(retries, true)) throw new ConnectException("SimpleESSinkConnector configuration field 'retries' must be a integer");
    
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
  
  @Override public abstract Class<? extends Task> taskClass();

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
        taskConfig.put("indexName", indexName);
        taskConfig.put("pipelineName", pipelineName);
        taskConfig.put("batchRecordCount", batchRecordCount);
        taskConfig.put("batchSize", batchSize);
        taskConfig.put("closeTimeout", closeTimeout);
        taskConfig.put("retries", retries);
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
    result.define("indexName", Type.STRING, Importance.HIGH, "index name");
    result.define("pipelineName", Type.STRING, DEFAULT_PIPELINENAME, Importance.MEDIUM, "pipeline name");        
    result.define("batchRecordCount", Type.STRING, DEFAULT_BATCHRECORDCOUNT, Importance.MEDIUM, "number of records to trigger a batch");
    result.define("batchSize", Type.STRING, DEFAULT_BATCHSIZE, Importance.MEDIUM, "size of records to trigger a batch, in MB");
    result.define("closeTimeout", Type.STRING, DEFAULT_CLOSETIMEOUT, Importance.MEDIUM, "timeout to wait for records to finish when stopping, in seconds");
	result.define("retries", Type.STRING, DEFAULT_RETRIES, Importance.MEDIUM, "number of retries on transient failures");
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

  private static boolean validIntegerConfig(String attribute, boolean required)
  {
    boolean valid = true;
    if (attribute == null || attribute.trim().length() == 0)
      {
        if (required) valid = false;
      }
    else
      {
        try
          {
            int result = Integer.parseInt(attribute);
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

  private static boolean validLongConfig(String attribute, boolean required)
  {
    boolean valid = true;
    if (attribute == null || attribute.trim().length() == 0)
      {
        if (required) valid = false;
      }
    else
      {
        try
          {
            long result = Long.parseLong(attribute);
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
}
