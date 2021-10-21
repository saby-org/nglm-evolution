/****************************************************************************
*
*  ReportManagerStatistics.java
*
****************************************************************************/

package com.evolving.nglm.evolution.backup.kafka;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;

public class BackupManagerStatistics implements BackupManagerStatisticsMBean, NGLMMonitoringObject
{
  //
  //  JMXBaseObjectName
  //

  public static final String BaseJMXObjectName = "com.evolving.nglm.evolution:type=BackupManager";

  //
  //  attributes
  //

  String name;
  String objectNameForManagement;
  int backupCount;
  int failureCount;

  //
  // Interface: ReportManagerStatisticsMBean
  //

  public int getBackupCount() { return backupCount; }
  public int getFailureCount() { return failureCount; }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public BackupManagerStatistics(String name)
  {
    //
    //  initialize
    //

    this.name = name;
    this.backupCount = 0;
    this.failureCount = 0;
    this.objectNameForManagement = BaseJMXObjectName + ",name=" + name;

    //
    //  register
    //

    NGLMRuntime.registerMonitoringObject(this);
  }

  /*****************************************
  *
  *  setters
  *
  *****************************************/

  synchronized void incrementBackupCount() { backupCount += 1; }
  synchronized void incrementFailureCount() { failureCount += 1; }

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
