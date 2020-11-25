/****************************************************************************
*
*  UpdateAlternateID.java 
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

public class UpdateAlternateID implements com.evolving.nglm.core.SubscriberStreamEvent
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/
  
  //
  //  AssignmentType
  //
  
  enum AssignmentType
  {
    AssignSubscriberID("standard"),
    ReassignSubscriberID("reassign"),
    UnassignSubscriberID("unassign");
    private String externalRepresentation;
    private String getExternalRepresentation() { return externalRepresentation; }
    private AssignmentType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public static AssignmentType fromExternalRepresentation(String externalRepresentation) { for (AssignmentType enumeratedValue : AssignmentType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return null; }
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
    schemaBuilder.name("update_alternateid");
    schemaBuilder.version(com.evolving.nglm.core.SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("idField", Schema.STRING_SCHEMA);
    schemaBuilder.field("alternateID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("assignmentType", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("subscriberAction", SchemaBuilder.string().defaultValue("standard").schema());
    schemaBuilder.field("backChannel", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("cleanupTableEntry", SchemaBuilder.bool().defaultValue(false).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<UpdateAlternateID> serde = new ConnectSerde<UpdateAlternateID>(schema, false, UpdateAlternateID.class, UpdateAlternateID::pack, UpdateAlternateID::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<UpdateAlternateID> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String idField;
  private String alternateID;
  private String subscriberID;
  private AssignmentType assignmentType;
  private Date eventDate;
  private SubscriberAction subscriberAction;
  private boolean backChannel;
  private boolean cleanupTableEntry;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getIDField() { return idField; }
  public String getAlternateID() { return alternateID; }
  public String getSubscriberID() { return subscriberID; }
  public AssignmentType getAssignmentType() { return assignmentType; }
  public Date getEventDate() { return eventDate; }
  public SubscriberAction getSubscriberAction() { return subscriberAction; }
  public boolean getBackChannel() { return backChannel; }
  public boolean getCleanupTableEntry() { return cleanupTableEntry; }

  @Override public DeliveryRequest.DeliveryPriority getDeliveryPriority(){return DeliveryRequest.DeliveryPriority.High; }

  /*****************************************
  *
  *  constructor (simple/unpack)
  *
  *****************************************/

  public UpdateAlternateID(String idField, String alternateID, String subscriberID, AssignmentType assignmentType, Date eventDate, SubscriberAction subscriberAction, boolean backChannel, boolean cleanupTableEntry)
  {
    this.idField = idField;
    this.alternateID = alternateID;
    this.subscriberID = subscriberID;
    this.assignmentType = assignmentType;
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

  public UpdateAlternateID(UpdateAlternateID updateAlternateID)
  {
    this.idField = updateAlternateID.getIDField();
    this.alternateID = updateAlternateID.getAlternateID();
    this.subscriberID = updateAlternateID.getSubscriberID();
    this.assignmentType = updateAlternateID.getAssignmentType();
    this.eventDate = updateAlternateID.getEventDate();
    this.subscriberAction = updateAlternateID.getSubscriberAction();
    this.backChannel = updateAlternateID.getBackChannel();
    this.cleanupTableEntry = updateAlternateID.getCleanupTableEntry();
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    UpdateAlternateID updateAlternateID = (UpdateAlternateID) value;
    Struct struct = new Struct(schema);
    struct.put("idField", updateAlternateID.getIDField());
    struct.put("alternateID", updateAlternateID.getAlternateID());
    struct.put("subscriberID", updateAlternateID.getSubscriberID());
    struct.put("assignmentType", updateAlternateID.getAssignmentType().getExternalRepresentation());
    struct.put("eventDate", updateAlternateID.getEventDate());
    struct.put("subscriberAction", updateAlternateID.getSubscriberAction().getExternalRepresentation());
    struct.put("backChannel", updateAlternateID.getBackChannel());
    struct.put("cleanupTableEntry", updateAlternateID.getCleanupTableEntry());
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

  public static UpdateAlternateID unpack(SchemaAndValue schemaAndValue)
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
    String idField = valueStruct.getString("idField");
    String alternateID = valueStruct.getString("alternateID");
    String subscriberID = valueStruct.getString("subscriberID");
    AssignmentType assignmentType = AssignmentType.fromExternalRepresentation(valueStruct.getString("assignmentType"));
    Date eventDate = (Date) valueStruct.get("eventDate");
    SubscriberAction subscriberAction = (schemaVersion >= 2) ? SubscriberAction.fromExternalRepresentation(valueStruct.getString("subscriberAction")) : SubscriberAction.Standard;
    boolean backChannel = (Boolean) valueStruct.get("backChannel");
    boolean cleanupTableEntry = (schemaVersion >= 2) ? (Boolean) valueStruct.get("cleanupTableEntry") : Boolean.FALSE;

    //
    //  return
    //

    return new UpdateAlternateID(idField, alternateID, subscriberID, assignmentType, eventDate, subscriberAction, backChannel, cleanupTableEntry);
  }
}
