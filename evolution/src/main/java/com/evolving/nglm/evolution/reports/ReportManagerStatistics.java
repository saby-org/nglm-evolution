/****************************************************************************
*
*  ReportManagerStatistics.java
*
****************************************************************************/

package com.evolving.nglm.evolution.reports;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;

public class ReportManagerStatistics implements ReportManagerStatisticsMBean, NGLMMonitoringObject
{
  //
  //  JMXBaseObjectName
  //

  public static final String BaseJMXObjectName = "com.evolving.nglm.evolution:type=ReportManager";

  //
  //  attributes
  //

  String name;
  String objectNameForManagement;
  int reportCount;
  int failureCount;

  //
  // Interface: ReportManagerStatisticsMBean
  //

  public int getReportCount() { return reportCount; }
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
  
  public ReportManagerStatistics(String name)
  {
    //
    //  initialize
    //

    this.name = name;
    this.reportCount = 0;
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

  synchronized void incrementReportCount() { reportCount += 1; }
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
