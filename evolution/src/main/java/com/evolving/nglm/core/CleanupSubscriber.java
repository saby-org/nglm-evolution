/****************************************************************************
*
*  CleanupSubscriber.java 
*
****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.evolution.DeliveryRequest;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;

public class CleanupSubscriber extends SubscriberStreamOutput implements com.evolving.nglm.core.SubscriberStreamEvent
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(), 4));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
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
  private SubscriberAction subscriberAction;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  @Override public String getSubscriberID() { return subscriberID; }
  @Override public SubscriberAction getSubscriberAction() { return subscriberAction; }
  @Override public DeliveryRequest.DeliveryPriority getDeliveryPriority(){return DeliveryRequest.DeliveryPriority.Low; }

  /*****************************************
  *
  *  constructor (simple)
  *
  *****************************************/

  public CleanupSubscriber(String subscriberID, SubscriberAction subscriberAction)
  {
    this.subscriberID = subscriberID;
    this.subscriberAction = subscriberAction;
  }

  public CleanupSubscriber(String subscriberID, AssignSubscriberIDs assignSubscriberIDs, SubscriberAction subscriberAction)
  {
    super(assignSubscriberIDs);
    this.subscriberID = subscriberID;
    this.subscriberAction = subscriberAction;
  }

  /*****************************************
  *
  *  constructor (simple/unpack)
  *
  *****************************************/

  public CleanupSubscriber(SchemaAndValue schemaAndValue, String subscriberID, SubscriberAction subscriberAction)
  {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
    this.subscriberAction = subscriberAction;
  }

  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  public CleanupSubscriber(CleanupSubscriber cleanupSubscriber)
  {
    super(cleanupSubscriber);
    this.subscriberID = cleanupSubscriber.getSubscriberID();
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
    packSubscriberStreamOutput(struct, cleanupSubscriber);
    struct.put("subscriberID", cleanupSubscriber.getSubscriberID());
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    SubscriberAction subscriberAction = schema.field("subscriberAction") != null ? SubscriberAction.fromExternalRepresentation(valueStruct.getString("subscriberAction")) : SubscriberAction.Cleanup;

    //
    //  return
    //

    return new CleanupSubscriber(schemaAndValue, subscriberID, subscriberAction);
  }
}
