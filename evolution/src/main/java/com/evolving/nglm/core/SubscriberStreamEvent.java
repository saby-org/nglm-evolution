package com.evolving.nglm.core;

import org.apache.kafka.connect.data.Schema;

import java.util.Date;
import java.util.UUID;

public interface SubscriberStreamEvent extends SubscriberStreamPriority
{

  enum SubscriberAction
  {
    Standard("standard"),
    Delete("delete"), // this is for subscriber manager, evolution engine ignores it
    DeleteImmediate("deleteImediate"), // this is for subscriber manager, evolution engine ignores it
    Cleanup("cleanup"), // this is for evolution engine, subscriber manager ignores it and only generates a CleanupSubscriber event 
    CleanupImmediate("cleanupImmediate"), // this is for evolution engine, subscriber manager ignores it and only generates a CleanupSubscriber event
    Unknown("(unknown)");
    private String externalRepresentation;
    private SubscriberAction(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static SubscriberAction fromExternalRepresentation(String externalRepresentation) { for (SubscriberAction enumeratedValue : SubscriberAction.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return null; }
  }

  String getSubscriberID();
  Date getEventDate();
  Schema subscriberStreamEventSchema();
  Object subscriberStreamEventPack(Object value);
  default String getEventID() { return null; }
  default SubscriberAction getSubscriberAction() { return SubscriberAction.Standard; }
  default UUID getTrackingID() { return null; }
}
