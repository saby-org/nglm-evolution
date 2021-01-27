/****************************************************************************
*
*  RecordSubscriberID.java 
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

public class RecordSubscriberID implements com.evolving.nglm.core.SubscriberStreamEvent
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
    schemaBuilder.name("record_subscriberid");
    schemaBuilder.version(com.evolving.nglm.core.SchemaUtilities.packSchemaVersion(3));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("idField", Schema.STRING_SCHEMA);
    schemaBuilder.field("alternateID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("subscriberAction", SchemaBuilder.string().defaultValue("standard").schema());
    schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<RecordSubscriberID> serde = new ConnectSerde<RecordSubscriberID>(schema, false, RecordSubscriberID.class, RecordSubscriberID::pack, RecordSubscriberID::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<RecordSubscriberID> serde() { return serde; }
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
  private int tenantID;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public String getIDField() { return idField; }
  public String getAlternateID() { return alternateID; }
  public Date getEventDate() { return eventDate; }
  @Override public SubscriberAction getSubscriberAction() { return subscriberAction; }
  @Override public DeliveryRequest.DeliveryPriority getDeliveryPriority(){return DeliveryRequest.DeliveryPriority.High; }
  public int getTenantID() { return tenantID; }

  /*****************************************
  *
  *  constructor (simple/unpack)
  *
  *****************************************/

  public RecordSubscriberID(String subscriberID, String idField, String alternateID, Date eventDate, SubscriberAction subscriberAction, int tenantID)
  {
    this.subscriberID = subscriberID;
    this.idField = idField;
    this.alternateID = alternateID;
    this.eventDate = eventDate;
    this.subscriberAction = subscriberAction;
    this.tenantID = tenantID;
  }

  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  public RecordSubscriberID(RecordSubscriberID recordSubscriberID)
  {
    this.subscriberID = recordSubscriberID.getSubscriberID();
    this.idField = recordSubscriberID.getIDField();
    this.alternateID = recordSubscriberID.getAlternateID();
    this.eventDate = recordSubscriberID.getEventDate();
    this.subscriberAction = recordSubscriberID.getSubscriberAction();
    this.tenantID = recordSubscriberID.getTenantID();
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    RecordSubscriberID recordSubscriberID = (RecordSubscriberID) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", recordSubscriberID.getSubscriberID());
    struct.put("idField", recordSubscriberID.getIDField());
    struct.put("alternateID", recordSubscriberID.getAlternateID());
    struct.put("eventDate", recordSubscriberID.getEventDate());
    struct.put("subscriberAction", recordSubscriberID.getSubscriberAction().getExternalRepresentation());
    struct.put("tenantID", (short)recordSubscriberID.getTenantID());
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

  public static RecordSubscriberID unpack(SchemaAndValue schemaAndValue)
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
    int tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : 1; // by default tenantID = 1
    //
    //  return
    //

    return new RecordSubscriberID(subscriberID, idField, alternateID, eventDate, subscriberAction, tenantID);
  }
}
