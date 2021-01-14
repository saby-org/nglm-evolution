/*****************************************************************************
*
*  SubscriberTraceControl.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.evolution.DeliveryRequest;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Date;

public class SubscriberTraceControl implements AutoProvisionSubscriberStreamEvent
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
    schemaBuilder.name("subscribertracecontrol");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberTraceEnabled", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  @Override public DeliveryRequest.DeliveryPriority getDeliveryPriority(){return DeliveryRequest.DeliveryPriority.High; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String subscriberID;
  private boolean subscriberTraceEnabled;
  private int tenantID;

  //
  //  transient
  //

  private Date eventDate;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SubscriberTraceControl(String subscriberID, boolean subscriberTraceEnabled, int tenantID)
  {
    this.subscriberID = subscriberID;
    this.subscriberTraceEnabled = subscriberTraceEnabled;
    this.eventDate = SystemTime.getCurrentTime();
    this.tenantID = tenantID;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberID() { return subscriberID; }
  public boolean getSubscriberTraceEnabled() { return subscriberTraceEnabled; }
  public Date getEventDate() { return eventDate; }
  public int getTenantID() { return tenantID; }

  /****************************************
  *
  *  setters
  *
  ****************************************/

  @Override public void rebindSubscriberID(String subscriberID) { this.subscriberID = subscriberID; }
  
  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<SubscriberTraceControl> serde()
  {
    return new ConnectSerde<SubscriberTraceControl>(schema, false, SubscriberTraceControl.class, SubscriberTraceControl::pack, SubscriberTraceControl::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SubscriberTraceControl subscriberTraceControl = (SubscriberTraceControl) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", subscriberTraceControl.getSubscriberID());
    struct.put("subscriberTraceEnabled", subscriberTraceControl.getSubscriberTraceEnabled());
    struct.put("tenantID", subscriberTraceControl.getTenantID());
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

  public static SubscriberTraceControl unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    boolean subscriberTraceEnabled = valueStruct.getBoolean("subscriberTraceEnabled");
    int tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : 1;

    //
    //  return
    //

    return new SubscriberTraceControl(subscriberID, subscriberTraceEnabled, tenantID);
  }
}
