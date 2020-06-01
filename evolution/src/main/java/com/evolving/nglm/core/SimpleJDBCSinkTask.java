/****************************************************************************
*
*  class SimpleJDBCSinkTask
*
****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.core.utilities.ConnectionPool;
import com.evolving.nglm.core.utilities.UtilitiesException;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class SimpleJDBCSinkTask extends SinkTask
{
  /*****************************************
  *
  *  config
  *
  *****************************************/

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(SimpleJDBCSinkTask.class);
  
  //
  //  configuration
  //

  private String connectorName = null;
  private String databaseProduct = null;
  private String databaseConnectionURL = null;
  private String databaseConnectionProperties = null;
  private boolean useStoredProcedure = false;
  private String insertSQL = null;
  private int batchSize;
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

  private ConnectionPool connectionPool = null;
  int currentBatchSize = 0;

  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  public abstract void prepareStatement(PreparedStatement statement, SinkRecord sinkRecord);

  /*****************************************
  *
  *  version
  *
  *****************************************/

  @Override public String version()
  {
    return SimpleJDBCSinkConnector.SimpleJDBCSinkVersion;
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
    databaseProduct = taskConfig.get("databaseProduct");
    databaseConnectionURL = taskConfig.get("databaseConnectionURL");
    databaseConnectionProperties = taskConfig.get("databaseConnectionProperties");
    useStoredProcedure = new Boolean(taskConfig.get("useStoredProcedure"));
    insertSQL = taskConfig.get("insertSQL");
    batchSize = SimpleJDBCSinkConnector.parseIntegerConfig(taskConfig.get("batchSize"));
    taskNumber = SimpleJDBCSinkConnector.parseIntegerConfig(taskConfig.get("taskNumber"));

    log.info("insertSQL: {}", insertSQL);

    /*****************************************
    *
    *  initialize database connection, statement
    *
    *****************************************/

    try
      {
        //
        //  parse database connection properties
        //

        Properties properties = new Properties();
        String[] propertiesArray = databaseConnectionProperties.trim().split(",");
        for (String property : propertiesArray)
          {
            String[] propertyParts = property.trim().split("=");
            if (propertyParts.length == 2)
              {
                properties.put(propertyParts[0],propertyParts[1]);
              }
          }

        //
        //  instantiate connection pool
        //

        connectionPool = new ConnectionPool(databaseProduct, databaseConnectionURL, properties, 1);
      }
    catch (UtilitiesException e)
      {
        log.error("Error starting: {}", connectorName);
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error(stackTraceWriter.toString());
        throw new ConnectException(e);
      }

    /*****************************************
    *
    *  initialize currentBatchSize
    *
    *****************************************/

    currentBatchSize = 0;
  }

  /*****************************************
  *
  *  put
  *
  *****************************************/

  @Override public void put(Collection<SinkRecord> sinkRecords)
  {
    if (sinkRecords.isEmpty())
      log.trace("{} -- Task.put() - 0 records.", connectorName);
    else
      log.info("{} -- Task.put() - {} records.", connectorName, sinkRecords.size());
    
    if (sinkRecords.size() == 0) return;

    /****************************************
    *
    *  commit
    *
    ****************************************/
    
    boolean successfulCommit = false;
    while (!successfulCommit)
      {
        Connection connection = null;
        try
          {
            /****************************************
            *
            *  get connection
            *
            ****************************************/

            connection = connectionPool.getConnection();

            /****************************************
            *
            *  insert all records
            *
            ****************************************/

            PreparedStatement statement = null;
            for (SinkRecord sinkRecord : sinkRecords)
              {
                //
                //  get cached statement
                //

                statement = connectionPool.statement(connection, insertSQL, useStoredProcedure);

                //
                //  insert record
                //

                prepareStatement(statement, sinkRecord);

                //
                //  execute batch (if necessary)
                //

                currentBatchSize += 1;
                if (currentBatchSize >= batchSize)
                  {
                    executeBatch(statement);
                  }
              }

            //
            //  execute final batch (if necessary)
            //
            
            if (currentBatchSize > 0)
              {
                executeBatch(statement);
              }

            /****************************************
            *
            *  commit and release connection
            *
            ****************************************/

            connection.commit();
            connectionPool.releaseConnection(connection);
            successfulCommit = true;
          }
        catch (SQLException e)
          {
            try
              {
                connectionPool.handleException(connection, e);
              }
            catch (SQLException|UtilitiesException e1)
              {
                if (connection != null)
                  {
                    try
                      {
                        connection.rollback(); 
                        connectionPool.releaseConnection(connection);
                      }
                    catch (SQLException|UtilitiesException e2)
                      {
                        // ignore
                      }
                  }
                throw new ConnectException(e);
              }
          }
        catch (Exception e)
          {
            if (connection != null)
              {
                try
                  {
                    connection.rollback(); 
                    connectionPool.releaseConnection(connection);
                  }
                catch (SQLException|UtilitiesException e2)
                  {
                    // ignore
                  }
              }
            throw new ConnectException(e);
          }
      }

    //
    //  statistics
    //

    updatePutCount(connectorName, taskNumber, 1);
  }

  /*****************************************
  *
  *  flush
  *
  *****************************************/

  @Override public void flush(Map<TopicPartition, OffsetAndMetadata> map)
  {
    log.info("{} -- Task.flush()", connectorName);
  }

  /*****************************************
  *
  *  executeBatch
  *
  *****************************************/

  private void executeBatch(PreparedStatement statement)
  {
    try
      {
        if (currentBatchSize > 0)
          {
            statement.executeBatch();

            //
            //  statistics
            //

            updateBatchCount(connectorName, taskNumber, 1);
            updateRecordCount(connectorName, taskNumber, currentBatchSize);

            //
            // reset currentBatchSize
            //
            
            currentBatchSize = 0;
          }
      }
    catch (SQLException e)
      {
        log.error("Error executing batch: " + e.getMessage());
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error(stackTraceWriter.toString());
        throw new ConnectException(e);
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

    //
    //  attributes
    //
    
    if (connectionPool != null) connectionPool.close();
    currentBatchSize = 0;

    //
    //  stopStatisticsCollection (unregister)
    //
    
    stopStatisticsCollection();
  }

  /*****************************************
  *
  *  statistics
  *    - updateBatchCount
  *    - updatePutCount
  *    - updateRecordCount
  *    - stopStatisticsCollection
  *
  *****************************************/

  //
  //  statistics data (transient)
  //
  
  private String statisticsKey(String connector, int taskNumber) { return connector + "-" + taskNumber; }
  private static Map<String,SinkTaskStatistics> allTaskStatistics = new HashMap<String, SinkTaskStatistics>();

  //
  //  getStatistics
  //

  private SinkTaskStatistics getStatistics(String connectorName, int taskNumber)
  {
    synchronized (allTaskStatistics)
      {
        SinkTaskStatistics taskStatistics = allTaskStatistics.get(statisticsKey(connectorName, taskNumber));
        if (taskStatistics == null)
          {
            try
              {
                taskStatistics = new SinkTaskStatistics(connectorName, "JDBC", taskNumber);
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

  //
  //  updateBatchCount
  //

  private void updateBatchCount(String connectorName, int taskNumber, int amount)
  {
    synchronized (allTaskStatistics)
      {
        SinkTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
        taskStatistics.updateBatchCount(amount);
      }
  }

  //
  //  updatePutCount
  //

  private void updatePutCount(String connectorName, int taskNumber, int amount)
  {
    synchronized (allTaskStatistics)
      {
        SinkTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
        taskStatistics.updatePutCount(amount);
      }
  }

  //
  //  updateRecordCount
  //

  private void updateRecordCount(String connectorName, int taskNumber, int amount)
  {
    synchronized (allTaskStatistics)
      {
        SinkTaskStatistics taskStatistics = getStatistics(connectorName, taskNumber);
        taskStatistics.updateRecordCount(amount);
      }
  }

  //
  //  stopStatisticsCollection
  //

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
