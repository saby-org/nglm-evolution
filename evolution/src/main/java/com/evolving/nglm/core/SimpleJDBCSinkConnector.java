/****************************************************************************
*
*  SimpleJDBCSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SimpleJDBCSinkConnector extends SinkConnector
{
  /*****************************************
  *
  *  config
  *
  *****************************************/

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(SimpleJDBCSinkConnector.class);
  
  //
  //  configuration
  //

  private String connectorName = null;
  private String databaseProduct;
  private String databaseConnectionURL;
  private String databaseConnectionProperties;
  private String insertSQL;
  private String batchSize;
  private String useStoredProcedureConfig;

  //
  //  version
  //

  final static String SimpleJDBCSinkVersion = "0.1";

  /****************************************
  *
  *  version
  *
  ****************************************/

  @Override public String version()
  {
    return SimpleJDBCSinkVersion;
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
    *  configuration -- connectorName
    *
    *****************************************/

    connectorName = properties.get("name");

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("{} -- Connector.start() START", connectorName);

    /*****************************************
    *
    *  configuration -- databaseProduct    
    *
    *****************************************/

    databaseProduct = properties.get("databaseProduct");
    if (databaseProduct == null || databaseProduct.trim().length() == 0) throw new ConnectException("SimpleJDBCSinkConnector configuration must specify 'databaseProduct'");

    /*****************************************
    *
    *  configuration -- databaseConnectionURL
    *
    *****************************************/

    databaseConnectionURL = properties.get("databaseConnectionURL");
    if (databaseConnectionURL == null || databaseConnectionURL.trim().length() == 0) throw new ConnectException("SimpleJDBCSinkConnector configuration must specify 'databaseConnectionURL'");

    /*****************************************
    *
    *  configuration -- databaseConnectionProperties
    *
    *****************************************/

    databaseConnectionProperties = properties.get("databaseConnectionProperties");

    /*****************************************
    *
    *  configuration -- useStoredProcedure
    *
    *****************************************/

    useStoredProcedureConfig = properties.get("useStoredProcedure");
    if (useStoredProcedureConfig == null || useStoredProcedureConfig.trim().length() == 0) throw new ConnectException("SimpleJDBCSinkConnector configuration must specify 'useStoredProcedure'");

    /*****************************************
    *
    *  configuration -- insertSQL
    *
    *****************************************/

    insertSQL = properties.get("insertSQL");
    if (insertSQL == null || insertSQL.trim().length() == 0) throw new ConnectException("SimpleJDBCSinkConnector configuration must specify 'insertSQL'");
    if (useStoredProcedureConfig.equalsIgnoreCase("true")  && (insertSQL.trim().toLowerCase().indexOf("call ") < 0)) throw new ConnectException("SimpleJDBCSinkConnector configuration 'insertSQL' must use 'call' syntax for stored procedures");

    /*****************************************
    *
    *  configuration -- batchSize (with default)
    *
    *****************************************/

    String batchSizeString = properties.get("batchSize");
    batchSize = (batchSizeString != null && batchSizeString.trim().length() > 0) ? batchSizeString : "1000";
    
    /*****************************************
    *
    *  log
    *
    *****************************************/
    
    log.info("{} -- Connector.start() END", connectorName);
  }

  /****************************************
  *
  *  abstract
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

    log.info("taskConfigs() START");
    log.info("taskConfigs() {} maxTasks", maxTasks);

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
        taskConfig.put("databaseProduct", databaseProduct);
        taskConfig.put("databaseConnectionURL", databaseConnectionURL);
        taskConfig.put("databaseConnectionProperties", (databaseConnectionProperties != null ? databaseConnectionProperties : "(no properties)"));
        taskConfig.put("useStoredProcedure", useStoredProcedureConfig);
        taskConfig.put("insertSQL", insertSQL);
        taskConfig.put("batchSize", batchSize);
        taskConfig.put("taskNumber", Integer.toString(i));
        result.add(taskConfig);
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    //
    //  log
    //

    log.info("taskConfigs() END");

    //
    //  return
    //

    return result;
  }

  /****************************************
  *
  *  stop
  *
  ****************************************/

  @Override public void stop()
  {
    /****************************************
    *
    *  log
    *
    ****************************************/
    
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
    result.define("databaseProduct", Type.STRING, Importance.HIGH, "supported database product (i.e., MySQL, MSSQLServer)");
    result.define("databaseConnectionURL", Type.STRING, Importance.HIGH, "database connection URL");
    result.define("databaseConnectionProperties", Type.STRING, "(no properties)", Importance.MEDIUM, "database connection properties");
    result.define("insertSQL", Type.STRING, Importance.HIGH, "insert statement");
    result.define("useStoredProcedure", Type.STRING, Importance.HIGH, "true/false use stored procedure");
    return result;
  }

  /*****************************************
  *
  *  parseIntegerConfig
  *
  *****************************************/

  protected static Integer parseIntegerConfig(String attribute)
  {
    try
      {
        return (attribute != null) ? Integer.parseInt(attribute) : null;
      }
    catch (NumberFormatException e)
      {
        return null;
      }
  }
}
