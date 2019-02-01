/*****************************************************************************
*
*  SubscriberGroup.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.AutoProvisionSubscriberStreamEvent;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;

public class SubscriberGroup implements AutoProvisionSubscriberStreamEvent
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
    schemaBuilder.name("subscriber_group");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("dimensionID", Schema.STRING_SCHEMA);
    schemaBuilder.field("segmentID", Schema.STRING_SCHEMA);
    schemaBuilder.field("epoch", Schema.INT32_SCHEMA);
    schemaBuilder.field("addSubscriber", Schema.BOOLEAN_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SubscriberGroup> serde = new ConnectSerde<SubscriberGroup>(schema, false, SubscriberGroup.class, SubscriberGroup::pack, SubscriberGroup::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberGroup> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String subscriberID;
  private Date eventDate;
  private String dimensionID;
  private String segmentID;
  private int epoch;
  private boolean addSubscriber;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public String getDimensionID() { return dimensionID; }
  public String getSegmentID() { return segmentID; }
  public int getEpoch() { return epoch; }
  public boolean getAddSubscriber() { return addSubscriber; }
  
  /****************************************
  *
  *  setters
  *
  ****************************************/

  @Override public void rebindSubscriberID(String subscriberID) { this.subscriberID = subscriberID; }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SubscriberGroup(String subscriberID, Date eventDate, String dimensionID, String segmentID, int epoch, boolean addSubscriber)
  {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.dimensionID = dimensionID;
    this.segmentID = segmentID;
    this.epoch = epoch;
    this.addSubscriber = addSubscriber;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SubscriberGroup subscriberGroup = (SubscriberGroup) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", subscriberGroup.getSubscriberID());
    struct.put("eventDate", subscriberGroup.getEventDate());
    struct.put("dimensionID", subscriberGroup.getDimensionID());
    struct.put("segmentID", subscriberGroup.getSegmentID());
    struct.put("epoch", subscriberGroup.getEpoch());
    struct.put("addSubscriber", subscriberGroup.getAddSubscriber());
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

  public static SubscriberGroup unpack(SchemaAndValue schemaAndValue)
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
    Date eventDate = (Date) valueStruct.get("eventDate");
    String dimensionID = valueStruct.getString("dimensionID");
    String segmentID = valueStruct.getString("segmentID");
    int epoch = valueStruct.getInt32("epoch");
    boolean addSubscriber = valueStruct.getBoolean("addSubscriber");
    
    //
    //  return
    //

    return new SubscriberGroup(subscriberID, eventDate, dimensionID, segmentID, epoch, addSubscriber);
  }
}