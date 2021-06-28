/****************************************************************************
*
*  CleanupSubscriber.java 
*
****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.evolution.DeliveryRequest;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;

public class CleanupSubscriber implements com.evolving.nglm.core.SubscriberStreamEvent
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("cleanup_subscriber");
    schemaBuilder.version(com.evolving.nglm.core.SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("subscriberAction", SchemaBuilder.string().defaultValue("standard").schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CleanupSubscriber> serde = new ConnectSerde<CleanupSubscriber>(schema, false, CleanupSubscriber.class, CleanupSubscriber::pack, CleanupSubscriber::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CleanupSubscriber> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String subscriberID;
  private Date eventDate;
  private SubscriberAction subscriberAction;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  @Override public String getSubscriberID() { return subscriberID; }
  @Override public Date getEventDate() { return eventDate; }
  @Override public SubscriberAction getSubscriberAction() { return subscriberAction; }
  @Override public DeliveryRequest.DeliveryPriority getDeliveryPriority(){return DeliveryRequest.DeliveryPriority.Low; }

  /*****************************************
  *
  *  constructor (simple/unpack)
  *
  *****************************************/

  public CleanupSubscriber(String subscriberID, Date eventDate, SubscriberAction subscriberAction)
  {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.subscriberAction = subscriberAction;
  }

  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  public CleanupSubscriber(CleanupSubscriber cleanupSubscriber)
  {
    this.subscriberID = cleanupSubscriber.getSubscriberID();
    this.eventDate = cleanupSubscriber.getEventDate();
    this.subscriberAction = cleanupSubscriber.getSubscriberAction();
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CleanupSubscriber cleanupSubscriber = (CleanupSubscriber) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", cleanupSubscriber.getSubscriberID());
    struct.put("eventDate", cleanupSubscriber.getEventDate());
    struct.put("subscriberAction", cleanupSubscriber.getSubscriberAction().getExternalRepresentation());
    return struct;
  }

  //
  //  subscriberStreamEventPack
  //

  public Object subscriberStreamEventPack(Object value) { return pack(value); }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CleanupSubscriber unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? com.evolving.nglm.core.SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    
    SubscriberAction subscriberAction = schema.field("subscriberAction") != null ? SubscriberAction.fromExternalRepresentation(valueStruct.getString("subscriberAction")) : SubscriberAction.Cleanup;

    //
    //  return
    //

    return new CleanupSubscriber(subscriberID, eventDate, subscriberAction);
  }
}
