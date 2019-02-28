/****************************************************************************
*
*  UCGEngineStatisticsMBean.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SubscriberStreamEvent;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class UCGEngineStatistics implements UCGEngineStatisticsMBean, NGLMMonitoringObject
{
  //
  //  JMXBaseObjectName
  //

  public static final String BaseJMXObjectName = "com.evolving.nglm.evolution:type=UCGEngine";

  //
  //  attributes
  //

  String name;
  String objectNameForManagement;
  int refreshCount;
  int queryCount;

  //
  // Interface: UCGEngineStatisticsMBean
  //

  public int getRefreshCount() { return refreshCount; }
  public int getQueryCount() { return queryCount; }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public UCGEngineStatistics(String name)
  {
    //
    //  initialize
    //

    this.name = name;
    this.refreshCount = 0;
    this.queryCount = 0;
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

  synchronized void incrementRefreshCount() { refreshCount += 1; }
  synchronized void incrementQueryCount() { queryCount += 1; }

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
