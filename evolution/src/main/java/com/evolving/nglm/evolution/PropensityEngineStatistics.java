/****************************************************************************
*
*  PropensityEngineStatisticsMBean.java
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

public class PropensityEngineStatistics implements PropensityEngineStatisticsMBean, NGLMMonitoringObject
{
  //
  //  JMXBaseObjectName
  //

  public static final String BaseJMXObjectName = "com.evolving.nglm.evolution:type=PropensityEngine";

  //
  //  attributes
  //

  String name;
  String objectNameForManagement;
  int presentationCount;
  int acceptanceCount;

  //
  // Interface: PropensityEngineStatisticsMBean
  //

  public int getPresentationCount() { return presentationCount; }
  public int getAcceptanceCount() { return acceptanceCount; }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public PropensityEngineStatistics(String name)
  {
    //
    //  initialize
    //

    this.name = name;
    this.presentationCount = 0;
    this.acceptanceCount = 0;
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

  synchronized void incrementPresentationCount() { presentationCount += 1; }
  synchronized void incrementAcceptanceCount() { acceptanceCount += 1; }

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
