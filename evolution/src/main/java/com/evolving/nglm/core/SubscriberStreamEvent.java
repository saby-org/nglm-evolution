/****************************************************************************
*
*  SubscriberStreamEvent.java
*
****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.core.NGLMRuntime.NGLMPriority;

import org.apache.kafka.connect.data.Schema;

import java.util.Date;
import java.util.UUID;

public interface SubscriberStreamEvent 
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/
  
  public enum SubscriberAction
  {
    Standard("standard"),
    Delete("delete"),
    Cleanup("cleanup"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private SubscriberAction(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static SubscriberAction fromExternalRepresentation(String externalRepresentation) { for (SubscriberAction enumeratedValue : SubscriberAction.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return null; }
  }

  /*****************************************
  *
  *  interface
  *
  *****************************************/

  public String getSubscriberID();
  public Date getEventDate();
  public Schema subscriberStreamEventSchema();
  public Object subscriberStreamEventPack(Object value);
  default public SubscriberAction getSubscriberAction() { return SubscriberAction.Standard; }
  default public NGLMPriority getNGLMPriority() { return NGLMPriority.NormalPriority; }
  default public UUID getTrackingID() { return null; }
}
