/****************************************************************************
*
*  SinkTaskStatistics.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkTaskStatistics implements SinkTaskStatisticsMBean, NGLMMonitoringObject
{
  //
  //  Base JMX object name
  //

  public static String BaseJMXObjectName = "com.evolving.nglm.core:type=SinkTask";

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SinkTaskStatistics.class);
  

  //
  //  attributes
  //

  String connectorName;
  String sinkType;
  String objectNameForManagement;
  int batchCount;
  int putCount;
  int recordCount;
  int taskNumber;

  //
  // Interface: SinkTaskStatisticsMBean
  //

  public String getConnectorName() { return connectorName; }
  public int getBatchCount() { return batchCount; }
  public int getPutCount() { return putCount; }
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
  
  public SinkTaskStatistics(String connectorName, String sinkType, int taskNumber) throws ServerException
  {
    this.connectorName = connectorName;
    this.sinkType = sinkType;
    this.batchCount = 0;
    this.putCount = 0;
    this.recordCount = 0;
    this.taskNumber = taskNumber;

    this.objectNameForManagement = BaseJMXObjectName + ",connectorName=" + connectorName + ",sinkType=" + sinkType + ",taskNumber=" + taskNumber;

    //
    // register
    //

    NGLMRuntime.registerMonitoringObject(this, true);
  }

  /*****************************************
  *
  *  updateBatchCount
  *
  *****************************************/
      
  synchronized void updateBatchCount(int amount)
  {
    batchCount = batchCount + amount;
  }

  /*****************************************
  *
  *  updatePutCount
  *
  *****************************************/
      
  synchronized void updatePutCount(int amount)
  {
    putCount = putCount + amount;
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
