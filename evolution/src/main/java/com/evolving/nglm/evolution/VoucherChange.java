package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;
import org.apache.kafka.connect.data.*;

import java.util.Date;

// main purpose is to trigger request to modify voucher in evolutionEngine, might be used as an event
public class VoucherChange extends SubscriberStreamOutput implements EvolutionEngineEvent {

  //action
  public enum VoucherChangeAction {
    Redeem("redeem"),
    Resend("resend"),
    Extend("extend"),
    Expire("expire"),
    Deliver("deliver"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private VoucherChangeAction(String externalRepresentation) { this.externalRepresentation = externalRepresentation;}
    public String getExternalRepresentation() { return externalRepresentation; }
    public static VoucherChangeAction fromExternalRepresentation(String externalRepresentation) { for (VoucherChangeAction enumeratedValue : VoucherChangeAction.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  private static Schema schema = null;
  static {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("voucher_change");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),8));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.builder().schema());
    schemaBuilder.field("newVoucherExpiryDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("eventID", Schema.STRING_SCHEMA);
    schemaBuilder.field("action", Schema.STRING_SCHEMA);
    schemaBuilder.field("voucherCode", Schema.STRING_SCHEMA);
    schemaBuilder.field("voucherID", Schema.STRING_SCHEMA);
    schemaBuilder.field("fileID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("moduleID", Schema.STRING_SCHEMA);
    schemaBuilder.field("featureID", Schema.STRING_SCHEMA);
    schemaBuilder.field("origin", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("returnStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  }

  private static ConnectSerde<VoucherChange> serde = new ConnectSerde<VoucherChange>(schema, false, VoucherChange.class, VoucherChange::pack, VoucherChange::unpack);

  public static Schema schema() { return schema; }
  public static ConnectSerde<VoucherChange> serde() { return serde; }

  public static Object pack(Object value) {
    VoucherChange voucherChange = (VoucherChange) value;
    Struct struct = new Struct(schema);
    packSubscriberStreamOutput(struct,voucherChange);
    struct.put("subscriberID",voucherChange.getSubscriberID());
    struct.put("eventDate",voucherChange.getEventDate());
    struct.put("newVoucherExpiryDate",voucherChange.getNewVoucherExpiryDate());
    struct.put("eventID", voucherChange.getEventID());
    struct.put("action", voucherChange.getAction().getExternalRepresentation());
    struct.put("voucherCode", voucherChange.getVoucherCode());
    struct.put("voucherID", voucherChange.getVoucherID());
    struct.put("fileID", voucherChange.getFileID());
    struct.put("moduleID", voucherChange.getModuleID());
    struct.put("featureID", voucherChange.getFeatureID());
    struct.put("origin", voucherChange.getOrigin());
    struct.put("returnStatus", voucherChange.getReturnStatus().getGenericResponseMessage());
    return struct;
  }

  public static VoucherChange unpack(SchemaAndValue schemaAndValue) {
    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;
    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDateTime = (Date) valueStruct.get("eventDate");
    Date newVoucherExpiryDate = (Date) valueStruct.get("newVoucherExpiryDate");
    String eventID = valueStruct.getString("eventID");
    VoucherChangeAction action = VoucherChangeAction.fromExternalRepresentation(valueStruct.getString("action"));
    String voucherCode = valueStruct.getString("voucherCode");
    String voucherID = valueStruct.getString("voucherID");
    String fileID = valueStruct.getString("fileID");
    String moduleID = valueStruct.getString("moduleID");
    String featureID = valueStruct.getString("featureID");
    String origin = valueStruct.getString("origin");
    RESTAPIGenericReturnCodes returnStatus = RESTAPIGenericReturnCodes.fromGenericResponseMessage(valueStruct.getString("returnStatus"));
    return new VoucherChange(schemaAndValue,subscriberID,eventDateTime,newVoucherExpiryDate,eventID,action,voucherCode,voucherID,fileID,moduleID,featureID,origin,returnStatus);
  }

  private String subscriberID;
  private Date eventDate;
  private Date newVoucherExpiryDate;
  private String eventID;
  private VoucherChangeAction action;
  private String voucherCode;
  private String voucherID;
  private String fileID;
  private String moduleID;
  private String featureID;
  private String origin;
  private RESTAPIGenericReturnCodes returnStatus;

  @Override
  public String getSubscriberID() { return subscriberID; }
  @Override
  public Date getEventDate() { return eventDate; }
  public Date getNewVoucherExpiryDate() { return newVoucherExpiryDate; }
  public String getEventID() { return eventID; }
  public VoucherChangeAction getAction() { return action; }
  public String getVoucherCode() { return voucherCode; }
  public String getVoucherID() { return voucherID; }
  public String getFileID() { return fileID; }
  public String getModuleID() { return moduleID; }
  public String getFeatureID() { return featureID; }
  public String getOrigin() { return origin; }
  public RESTAPIGenericReturnCodes getReturnStatus() { return returnStatus; }

  @Override
  public Schema subscriberStreamEventSchema()
  {
    return schema;
  }
  @Override
  public Object subscriberStreamEventPack(Object value)
  {
    return pack(value);
  }
  @Override
  public String getEventName()
  {
    return "voucher change";
  }

  public void setReturnStatus(RESTAPIGenericReturnCodes returnStatus) { this.returnStatus = returnStatus; }

  public VoucherChange(String subscriberID, Date eventDate, Date newVoucherExpiryDate, String eventID, VoucherChangeAction action, String voucherCode, String voucherID, String fileID, String moduleID, String featureID, String origin, RESTAPIGenericReturnCodes returnStatus) {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.newVoucherExpiryDate = newVoucherExpiryDate;
    this.eventID = eventID;
    this.action = action;
    this.voucherCode = voucherCode;
    this.voucherID = voucherID;
    this.fileID = fileID;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.origin = origin;
    this.returnStatus = returnStatus;
  }

  public VoucherChange(SchemaAndValue schemaAndValue, String subscriberID, Date eventDate, Date newVoucherExpiryDate, String eventID, VoucherChangeAction action, String voucherCode, String voucherID, String fileID, String moduleID, String featureID, String origin, RESTAPIGenericReturnCodes returnStatus) {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.newVoucherExpiryDate = newVoucherExpiryDate;
    this.eventID = eventID;
    this.action = action;
    this.voucherCode = voucherCode;
    this.voucherID = voucherID;
    this.fileID = fileID;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.origin = origin;
    this.returnStatus = returnStatus;
  }

  @Override
  public String toString() {
    return "VoucherChange{" +
            "subscriberID='" + subscriberID + '\'' +
            ", eventDate=" + eventDate +
            ", newVoucherExpiryDate=" + newVoucherExpiryDate +
            ", eventID='" + eventID + '\'' +
            ", action=" + action.getExternalRepresentation() +
            ", voucherCode='" + voucherCode + '\'' +
            ", voucherID='" + voucherID + '\'' +
            ", fileID='" + fileID + '\'' +
            ", moduleID='" + moduleID + '\'' +
            ", featureID='" + featureID + '\'' +
            ", origin='" + origin + '\'' +
            ", returnStatus=" + returnStatus.getGenericResponseMessage() +
            '}';
  }

}
