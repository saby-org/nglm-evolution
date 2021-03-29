/*****************************************************************************
*
*  AutoProvisionEvent.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.json.simple.JSONObject;

/* IF COPY/PASTE FROM ANOTHER DeploymentManagedObject OBJECT
 * DO NOT FORGET TO ADD IT TO THE DeploymentManagedObject FACTORY
 * SEE DeploymentManagedObject.create */
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
   * @throws ClassNotFoundException 
  *
  *****************************************/

  public AutoProvisionEvent(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException, ClassNotFoundException
  {
    super(jsonRoot);
    this.eventClass = JSONUtilities.decodeString(jsonRoot, "eventClass", true);
    this.eventTopic = JSONUtilities.decodeString(jsonRoot, "eventTopic", true);
    this.autoProvisionTopic = JSONUtilities.decodeString(jsonRoot, "autoProvisionTopic", true);
    
    // ensure the eventClass implements the AutoProvisionSubscriberStreamEvent
    Class<? extends AutoProvisionSubscriberStreamEvent> eventClass = (Class<? extends AutoProvisionSubscriberStreamEvent>) Class.forName(getEventClass());
    if(!AutoProvisionSubscriberStreamEvent.class.isAssignableFrom(eventClass))
      {
        throw new RuntimeException("Class " + getEventClass() + " does not implement interface " + AutoProvisionSubscriberStreamEvent.class.getName());
      }
  }
}
