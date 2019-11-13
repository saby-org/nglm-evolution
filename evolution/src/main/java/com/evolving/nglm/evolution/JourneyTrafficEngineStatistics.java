/****************************************************************************
*
*  JourneyTrafficEngineStatistics.java
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

public class JourneyTrafficEngineStatistics implements JourneyTrafficEngineStatisticsMBean, NGLMMonitoringObject
{
  //
  //  JMXBaseObjectName
  //

  public static final String BaseJMXObjectName = "com.evolving.nglm.evolution:type=JourneyTrafficEngine";

  //
  //  attributes
  //

  String name;
  String objectNameForManagement;
  int eventProcessedCount;
  int completedJourneys;

  //
  // Interface: JourneyTrafficEngineStatisticsMBean
  //

  public int getEventProcessedCount() { return eventProcessedCount; }
  public int getCompletedJourneys() { return completedJourneys; }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public JourneyTrafficEngineStatistics(String name)
  {
    //
    //  initialize
    //

    this.name = name;
    this.eventProcessedCount = 0;
    this.completedJourneys = 0;
    this.objectNameForManagement = BaseJMXObjectName + ",name=" + name;

    //
    //  register
    //

    NGLMRuntime.registerMonitoringObject(this);
  }

  /*****************************************
  *
  *  incrementEventProcessedCount
  *
  *****************************************/

  synchronized void incrementEventProcessedCount()
  {
    eventProcessedCount = eventProcessedCount + 1;
  }

  /*****************************************
  *
  *  incrementCompletedJourneys
  *
  *****************************************/

  synchronized void incrementCompletedJourneys()
  {
    completedJourneys = completedJourneys + 1;
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
