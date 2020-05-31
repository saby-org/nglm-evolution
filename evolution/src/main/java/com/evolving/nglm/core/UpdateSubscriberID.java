/****************************************************************************
*
*  UpdateSubscriberID.java 
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;

public class UpdateSubscriberID implements SubscriberStreamEvent
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
    schemaBuilder.name("update_subscriberid");
    schemaBuilder.version(com.evolving.nglm.core.SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("idField", Schema.STRING_SCHEMA);
    schemaBuilder.field("alternateID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("subscriberAction", SchemaBuilder.string().defaultValue("standard").schema());
    schemaBuilder.field("backChannel", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("cleanupTableEntry", SchemaBuilder.bool().defaultValue(false).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<UpdateSubscriberID> serde = new ConnectSerde<UpdateSubscriberID>(schema, false, UpdateSubscriberID.class, UpdateSubscriberID::pack, UpdateSubscriberID::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<UpdateSubscriberID> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String subscriberID;
  private String idField;
  private String alternateID;
  private Date eventDate;
  private SubscriberAction subscriberAction;
  private boolean backChannel;
  private boolean cleanupTableEntry;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public String getIDField() { return idField; }
  public String getAlternateID() { return alternateID; }
  public SubscriberAction getSubscriberAction() { return subscriberAction; }
  public boolean getBackChannel() { return backChannel; }
  public boolean getCleanupTableEntry() { return cleanupTableEntry; }
  public Date getEventDate() { return eventDate; }

  /*****************************************
  *
  *  constructor (simple/unpack)
  *
  *****************************************/

  public UpdateSubscriberID(String subscriberID, String idField, String alternateID, Date eventDate, SubscriberAction subscriberAction, boolean backChannel, boolean cleanupTableEntry)
  {
    this.subscriberID = subscriberID;
    this.idField = idField;
    this.alternateID = alternateID;
    this.eventDate = eventDate;
    this.subscriberAction = subscriberAction;
    this.backChannel = backChannel;
    this.cleanupTableEntry = cleanupTableEntry;
  }

  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  public UpdateSubscriberID(UpdateSubscriberID updateSubscriberID)
  {
    this.subscriberID = updateSubscriberID.getSubscriberID();
    this.idField = updateSubscriberID.getIDField();
    this.alternateID = updateSubscriberID.getAlternateID();
    this.eventDate = updateSubscriberID.getEventDate();
    this.subscriberAction = updateSubscriberID.getSubscriberAction();
    this.backChannel = updateSubscriberID.getBackChannel();
    this.cleanupTableEntry = updateSubscriberID.getCleanupTableEntry();
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    UpdateSubscriberID updateSubscriberID = (UpdateSubscriberID) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", updateSubscriberID.getSubscriberID());
    struct.put("idField", updateSubscriberID.getIDField());
    struct.put("alternateID", updateSubscriberID.getAlternateID());
    struct.put("eventDate", updateSubscriberID.getEventDate());
    struct.put("subscriberAction", updateSubscriberID.getSubscriberAction().getExternalRepresentation());
    struct.put("backChannel", updateSubscriberID.getBackChannel());
    struct.put("cleanupTableEntry", updateSubscriberID.getCleanupTableEntry());
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

  public static UpdateSubscriberID unpack(SchemaAndValue schemaAndValue)
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
    String idField = valueStruct.getString("idField");
    String alternateID = valueStruct.getString("alternateID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    SubscriberAction subscriberAction = (schemaVersion >= 2) ? SubscriberAction.fromExternalRepresentation(valueStruct.getString("subscriberAction")) : SubscriberAction.Standard;
    boolean backChannel = (Boolean) valueStruct.get("backChannel");
    boolean cleanupTableEntry = (schemaVersion >= 2) ? (Boolean) valueStruct.get("cleanupTableEntry") : Boolean.FALSE;

    //
    //  return
    //

    return new UpdateSubscriberID(subscriberID, idField, alternateID, eventDate, subscriberAction, backChannel, cleanupTableEntry);
  }
}
