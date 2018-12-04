/****************************************************************************
*
*  EvolutionEventStatistics.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;

import java.util.Map;
import java.util.HashMap;

public class EvolutionEventStatistics implements EvolutionEventStatisticsMBean, NGLMMonitoringObject
{
  //
  //  attributes
  //

  String namePrefix;
  String objectNameForManagement;
  String evolutionEventName;
  int evolutionEventCount;

  //
  // Interface: EvolutionEngineStatisticsMBean
  //

  public String getEvolutionEventName() { return evolutionEventName; }
  public int getEvolutionEventCount() { return evolutionEventCount; }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public EvolutionEventStatistics(String namePrefix, String evolutionEventName)
  {
    //
    // initialize
    //

    this.namePrefix = namePrefix;
    this.evolutionEventName = evolutionEventName;
    this.evolutionEventCount = 0;
    this.objectNameForManagement = EvolutionEngineStatistics.BaseJMXObjectName + ",name=" + namePrefix + ",evolutionEvent=" + evolutionEventName;

    //
    // register
    //

    NGLMRuntime.registerMonitoringObject(this);    
  }

  //
  // update
  //
      
  synchronized void updateEvolutionEventCount(int amount) { evolutionEventCount = evolutionEventCount + amount; }

  //
  // unregister
  //
  
  public void unregister()
  {
    NGLMRuntime.unregisterMonitoringObject(this);
  }
}