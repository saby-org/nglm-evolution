/*****************************************************************************
*
*  AutoProvisionEvent.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.json.simple.JSONObject;

public class AutoProvisionEvent extends DeploymentManagedObject
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String eventClass;
  private String eventTopic;
  private String autoProvisionTopic;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getEventClass() { return eventClass; }
  public String getEventTopic() { return eventTopic; }
  public String getAutoProvisionTopic() { return autoProvisionTopic; }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public AutoProvisionEvent(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
    this.eventClass = JSONUtilities.decodeString(jsonRoot, "eventClass", true);
    this.eventTopic = JSONUtilities.decodeString(jsonRoot, "eventTopic", true);
    this.autoProvisionTopic = JSONUtilities.decodeString(jsonRoot, "autoProvisionTopic", true);
  }
}
