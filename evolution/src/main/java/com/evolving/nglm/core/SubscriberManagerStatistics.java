/****************************************************************************
*
*  SubscriberManagerStatisticsMBean.java
*
****************************************************************************/

package com.evolving.nglm.core;

public class SubscriberManagerStatistics implements SubscriberManagerStatisticsMBean, NGLMMonitoringObject
{
  //
  //  JMXBaseObjectName
  //

  public static final String BaseJMXObjectName = "com.evolving.nglm.core:type=SubscriberManager";

  //
  //  attributes
  //

  String name;
  String objectNameForManagement;
  int autoProvisionCount;
  int assignSubscriberIDCount;
  int updateSubscriberIDCount;

  //
  // Interface: SubscriberManagerStatisticsMBean
  //

  public int getAutoProvisionCount() { return autoProvisionCount; }
  public int getAssignSubscriberIDCount() { return assignSubscriberIDCount; }
  public int getUpdateSubscriberIDCount() { return updateSubscriberIDCount; }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public SubscriberManagerStatistics(String name) throws com.evolving.nglm.core.ServerException
  {
    //
    //  initialize
    //

    this.name = name;
    this.autoProvisionCount = 0;
    this.assignSubscriberIDCount = 0;
    this.updateSubscriberIDCount = 0;
    this.objectNameForManagement = BaseJMXObjectName + ",name=" + name;

    //
    //  register
    //

    NGLMRuntime.registerMonitoringObject(this);
  }

  /*****************************************
  *
  *  updateAutoProvisionCount
  *
  *****************************************/

  synchronized void updateAutoProvisionCount(int amount)
  {
    autoProvisionCount = autoProvisionCount + amount;
  }

  /*****************************************
  *
  *  updateAssignSubscriberIDCount
  *
  *****************************************/

  synchronized void updateAssignSubscriberIDCount(int amount)
  {
    assignSubscriberIDCount = assignSubscriberIDCount + amount;
  }

  /*****************************************
  *
  *  updateUpdateSubscriberIDCount
  *
  *****************************************/

  synchronized void updateUpdateSubscriberIDCount(int amount)
  {
    updateSubscriberIDCount = updateSubscriberIDCount + amount;
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
