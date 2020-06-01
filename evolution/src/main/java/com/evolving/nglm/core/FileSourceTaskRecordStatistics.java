/****************************************************************************
*
*  FileSourceTaskRecordStatistics.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSourceTaskRecordStatistics implements FileSourceTaskRecordStatisticsMBean, NGLMMonitoringObject
{
  //
  //  Base JMX object name
  //

  public static String BaseJMXObjectName = "com.evolving.nglm.core:type=FileSourceTaskRecord";

  //
  //  attributes
  //

  String recordType;
  int recordCount;
  String objectNameForManagement;

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(FileSourceTaskRecordStatistics.class);

  //
  // Interface: FileSourceTaskRecordStatisticsMBean
  //

  public String getRecordType() { return recordType; }
  public int getRecordCount() { return recordCount; }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public FileSourceTaskRecordStatistics(String connectorName, int taskNumber, String recordType) throws ServerException
  {
    this.recordType = recordType;
    this.recordCount = 0;
    this.objectNameForManagement = BaseJMXObjectName + ",connectorName=" + connectorName + ",taskNumber=" + taskNumber + ",recordType=" + recordType;

    //
    // register
    //

    log.info("Registering MBEAN {}", this.objectNameForManagement);
    NGLMRuntime.registerMonitoringObject(this, true);
  }

  /*****************************************
  *
  *  updateRecordCount
  *
  *****************************************/
      
  synchronized void updateRecordCount(int amount)
  {
    recordCount = recordCount + amount;
  }

  /*****************************************
  *
  *  unregister
  *
  *****************************************/

  public void unregister()
  {
    NGLMRuntime.unregisterMonitoringObject(this);
  }
}