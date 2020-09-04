/****************************************************************************
 *
 *  ReportManagerStatistics.java
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.extracts;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;

public class ExtractManagerStatistics implements ExtractManagerStatisticsMBean, NGLMMonitoringObject
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

  /*****************************************
   *
   *  constructor
   *
   *****************************************/

  public ExtractManagerStatistics(String name)
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

  public int getExtractCount()
  {
    return reportCount;
  }

  //
  // Interface: NGLMMonitoringObject
  //

  public int getFailureCount()
  {
    return failureCount;
  }

  public String getObjectNameForManagement()
  {
    return objectNameForManagement;
  }

  /*****************************************
   *
   *  setters
   *
   *****************************************/

  synchronized void incrementExtractCount()
  {
    reportCount += 1;
  }

  synchronized void incrementFailureCount()
  {
    failureCount += 1;
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
