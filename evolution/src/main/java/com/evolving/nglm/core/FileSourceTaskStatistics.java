/****************************************************************************
*
*  FileSourceTaskStatistics.java
*
****************************************************************************/

package com.evolving.nglm.core;

import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSourceTaskStatistics implements FileSourceTaskStatisticsMBean, NGLMMonitoringObject
{
  //
  //  Base JMX object name
  //

  public static String BaseJMXObjectName = "com.evolving.nglm.core:type=FileSourceTask";

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(FileSourceTaskStatistics.class);
  

  //
  //  attributes
  //

  String connectorName;
  String objectNameForManagement;
  int filesProcessed;
  int totalRecordCount;
  int errorRecordCount;
  int taskNumber;

  //
  //  record type tracking
  //

  Map<String,FileSourceTaskRecordStatistics> allRecordStatistics;
  
  //
  // Interface: FileSourceTaskStatisticsMBean
  //

  public String getConnectorName() { return connectorName; }
  public int getFilesProcessedCount() { return filesProcessed; }
  public int getTotalRecordCount() { return totalRecordCount; }
  public int getErrorRecordCount() { return errorRecordCount; }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public FileSourceTaskStatistics(String connectorName, int taskNumber) throws ServerException
  {
    this.connectorName = connectorName;
    this.filesProcessed = 0;
    this.totalRecordCount = 0;
    this.errorRecordCount = 0;
    this.taskNumber = taskNumber;
    this.allRecordStatistics = new HashMap<String,FileSourceTaskRecordStatistics>();

    this.objectNameForManagement = BaseJMXObjectName + ",connectorName=" + connectorName + ",taskNumber=" + taskNumber;

    //
    // register
    //

    NGLMRuntime.registerMonitoringObject(this, true);
  }

  //
  //  ensureRecordTypeTracked
  //

  private FileSourceTaskRecordStatistics ensureRecordTypeTracked(String recordType)
  {
    FileSourceTaskRecordStatistics probe = null;
    try
      {
        probe = allRecordStatistics.get(recordType);
        if (probe == null)
          {
            probe = new FileSourceTaskRecordStatistics(connectorName, taskNumber, recordType);
            allRecordStatistics.put(recordType, probe);
          }
      }
    catch (ServerException se)
      {
        throw new ServerRuntimeException("Could not register file source task record statistics MBean");
      }
    return probe;
  }

  /*****************************************
  *
  *  updateFilesProcessed
  *
  *****************************************/
      
  synchronized void updateFilesProcessed(int amount)
  {
    filesProcessed = filesProcessed + amount;
  }

  /*****************************************
  *
  *  updateErrorRecords
  *
  *****************************************/
      
  synchronized void updateErrorRecords(int amount)
  {
    errorRecordCount = errorRecordCount + amount;
  }

  /*****************************************
  *
  *  updateRecordStatistics
  *
  *****************************************/
      
  synchronized void updateRecordStatistics(String recordType, int amount)
  {
    //
    // overall total
    //
    
    totalRecordCount = totalRecordCount + amount;

    //
    // record type statistics
    //
    
    FileSourceTaskRecordStatistics recordStatistics = ensureRecordTypeTracked(recordType);
    recordStatistics.updateRecordCount(amount);
  }

  /*****************************************
  *
  *  unregister
  *
  *****************************************/

  public void unregister()
  {
    for (FileSourceTaskRecordStatistics recordStats : allRecordStatistics.values())
      {
        recordStats.unregister();
      }
    NGLMRuntime.unregisterMonitoringObject(this);
  }
}
