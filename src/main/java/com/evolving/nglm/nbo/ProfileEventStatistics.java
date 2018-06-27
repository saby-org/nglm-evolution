/****************************************************************************
*
*  ProfileEventStatistics.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;

import java.util.Map;
import java.util.HashMap;

public class ProfileEventStatistics implements ProfileEventStatisticsMBean, NGLMMonitoringObject
{
  //
  //  attributes
  //

  String namePrefix;
  String objectNameForManagement;
  String profileEventName;
  int profileEventCount;

  //
  // Interface: ProfileEngineStatisticsMBean
  //

  public String getProfileEventName() { return profileEventName; }
  public int getProfileEventCount() { return profileEventCount; }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public ProfileEventStatistics(String namePrefix, String profileEventName) throws ServerException
  {
    //
    // initialize
    //

    this.namePrefix = namePrefix;
    this.profileEventName = profileEventName;
    this.profileEventCount = 0;
    this.objectNameForManagement = ProfileEngineStatistics.BaseJMXObjectName + ",name=" + namePrefix + ",profileEvent=" + profileEventName;

    //
    // register
    //

    NGLMRuntime.registerMonitoringObject(this);    
  }

  //
  // update
  //
      
  synchronized void updateProfileEventCount(int amount) { profileEventCount = profileEventCount + amount; }

  //
  // unregister
  //
  
  public void unregister()
  {
    NGLMRuntime.unregisterMonitoringObject(this);
  }
}