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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SubscriberGroup implements AutoProvisionSubscriberStreamEvent
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum SubscriberGroupType
  {
    SegmentationDimension("segmentationDimension"),
    Target("target"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private SubscriberGroupType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static SubscriberGroupType fromExternalRepresentation(String externalRepresentation) { for (SubscriberGroupType enumeratedValue : SubscriberGroupType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("subscriberGroupType", SchemaBuilder.string().defaultValue("").schema());
    schemaBuilder.field("subscriberGroupIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(new ArrayList<String>()).schema());
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
  private SubscriberGroupType subscriberGroupType;
  private List<String> subscriberGroupIDs;
  private int epoch;
  private boolean addSubscriber;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public SubscriberGroupType getSubscriberGroupType() { return subscriberGroupType; }
  public List<String> getSubscriberGroupIDs() { return  subscriberGroupIDs; }
  public String getSubscriberGroupPrimaryID() { return subscriberGroupIDs.get(0); }
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

  public SubscriberGroup(String subscriberID, Date eventDate, SubscriberGroupType subscriberGroupType, List<String> subscriberGroupIDs, int epoch, boolean addSubscriber)
  {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.subscriberGroupType = subscriberGroupType;
    this.subscriberGroupIDs = subscriberGroupIDs;
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
    struct.put("subscriberGroupType", subscriberGroup.getSubscriberGroupType().getExternalRepresentation());
    struct.put("subscriberGroupIDs", subscriberGroup.getSubscriberGroupIDs());
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
    SubscriberGroupType subscriberGroupType = (schemaVersion >= 2) ? SubscriberGroupType.fromExternalRepresentation(valueStruct.getString("subscriberGroupType")) : SubscriberGroupType.SegmentationDimension;
    List<String> subscriberGroupIDs = (schemaVersion >= 2) ? (List<String>) valueStruct.get("subscriberGroupIDs") : new ArrayList<String>();
    int epoch = valueStruct.getInt32("epoch");
    boolean addSubscriber = valueStruct.getBoolean("addSubscriber");
    
    //
    //  version 1
    //

    if (schemaVersion < 2)
      {
        subscriberGroupIDs.add(valueStruct.getString("dimensionID"));
        subscriberGroupIDs.add(valueStruct.getString("segmentID"));
      }
    
    //
    //  return
    //

    return new SubscriberGroup(subscriberID, eventDate, subscriberGroupType, subscriberGroupIDs, epoch, addSubscriber);
  }
}
