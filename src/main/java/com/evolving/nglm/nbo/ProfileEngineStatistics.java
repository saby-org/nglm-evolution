/****************************************************************************
*
*  ProfileEngineStatisticsMBean.java
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

public class ProfileEngineStatistics implements ProfileEngineStatisticsMBean, NGLMMonitoringObject
{
  //
  //  JMXBaseObjectName
  //

  public static final String BaseJMXObjectName = "com.evolving.nglm.evolution:type=ProfileEngine";

  //
  //  attributes
  //

  String name;
  String objectNameForManagement;
  int eventProcessedCount;

  //
  //  profile event counts, profile counts
  //

  Map<String,ProfileEventStatistics> profileEventCounts;

  //
  // Interface: ProfileEngineStatisticsMBean
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
  
  public ProfileEngineStatistics(String name) throws ServerException
  {
    //
    //  initialize
    //

    this.name = name;
    this.eventProcessedCount = 0;
    this.profileEventCounts = new HashMap<String,ProfileEventStatistics>();
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
    try
      {
        String simpleName = event.getClass().getSimpleName();
        ProfileEventStatistics stats = profileEventCounts.get(simpleName);
        if (stats == null)
          {
            stats = new ProfileEventStatistics(this.name, simpleName);
            profileEventCounts.put(simpleName, stats);
          }
        stats.updateProfileEventCount(1);
      }
    catch (ServerException se)
      {
        throw new ServerRuntimeException("Could not register profile event MBean");
      }
  }

  /*****************************************
  *
  *  unregister
  *
  *****************************************/

  public void unregister()
  {
    for (ProfileEventStatistics stats : profileEventCounts.values())
      {
        stats.unregister();
      }
    NGLMRuntime.unregisterMonitoringObject(this);
  }
}
