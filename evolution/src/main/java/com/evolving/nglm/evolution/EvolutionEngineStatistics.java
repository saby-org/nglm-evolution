/****************************************************************************
*
*  EvolutionEngineStatisticsMBean.java
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

public class EvolutionEngineStatistics implements EvolutionEngineStatisticsMBean, NGLMMonitoringObject
{
  //
  //  JMXBaseObjectName
  //

  public static final String BaseJMXObjectName = "com.evolving.nglm.evolution:type=EvolutionEngine";

  //
  //  attributes
  //

  String name;
  String objectNameForManagement;
  int eventProcessedCount;

  //
  //  profile event counts, profile counts
  //

  Map<String,EvolutionEventStatistics> evolutionEventCounts;

  //
  // Interface: EvolutionEngineStatisticsMBean
  //

  public int getEventProcessedCount() { return eventProcessedCount; }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public EvolutionEngineStatistics(String name)
  {
    //
    //  initialize
    //

    this.name = name;
    this.eventProcessedCount = 0;
    this.evolutionEventCounts = new HashMap<String,EvolutionEventStatistics>();
    this.objectNameForManagement = BaseJMXObjectName + ",name=" + name;

    //
    //  register
    //

    NGLMRuntime.registerMonitoringObject(this);
  }

  /*****************************************
  *
  *  updateEventProcessedCount
  *
  *****************************************/

  synchronized void updateEventProcessedCount(int amount)
  {
    eventProcessedCount = eventProcessedCount + amount;
  }

  /*****************************************
  *
  *  updateEventCount
  *
  *****************************************/

  synchronized void updateEventCount(SubscriberStreamEvent event, int amount)
  {
    String simpleName = event.getClass().getSimpleName();
    EvolutionEventStatistics stats = evolutionEventCounts.get(simpleName);
    if (stats == null)
      {
        stats = new EvolutionEventStatistics(this.name, simpleName);
        evolutionEventCounts.put(simpleName, stats);
      }
    stats.updateEvolutionEventCount(1);
  }

  /*****************************************
  *
  *  unregister
  *
  *****************************************/

  public void unregister()
  {
    for (EvolutionEventStatistics stats : evolutionEventCounts.values())
      {
        stats.unregister();
      }
    NGLMRuntime.unregisterMonitoringObject(this);
  }
}
