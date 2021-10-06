package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;

import org.apache.kafka.connect.data.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// main purpose is to trigger request to modify voucher in evolutionEngine, might be used as an event
public class VoucherChange extends SubscriberStreamOutput implements EvolutionEngineEvent{

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
  private static Schema groupIDSchema = null;
  static {
    //
    //  groupID schema
    //

    SchemaBuilder groupIDSchemaBuilder = SchemaBuilder.struct();
    groupIDSchemaBuilder.name("subscribergroup_groupid");
    groupIDSchemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    groupIDSchemaBuilder.field("subscriberGroupIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(new ArrayList<String>()).schema());
    groupIDSchema = groupIDSchemaBuilder.build();

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("voucher_change");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),11));
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
    schemaBuilder.field("segments", SchemaBuilder.map(groupIDSchema, Schema.INT32_SCHEMA).name("voucherchange_segments").schema());
    schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
    schemaBuilder.field("offerID", Schema.OPTIONAL_STRING_SCHEMA);
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
    struct.put("segments",packSegments(voucherChange.getSegments()));
    struct.put("tenantID", (short) voucherChange.getTenantID());
    struct.put("offerID",voucherChange.getOfferID());
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
    Map<Pair<String,String>, Integer> segments = (schemaVersion >= 10) ? unpackSegments(valueStruct.get("segments")) : unpackSegmentsV1(valueStruct.get("subscriberGroups"));
    int tenantID = (schemaVersion >= 9)? valueStruct.getInt16("tenantID") : 1; // for old system, default to tenant 1
    String offerID = (schemaVersion >= 10)? valueStruct.getString("offerID") : "";
    return new VoucherChange(schemaAndValue,subscriberID,eventDateTime,newVoucherExpiryDate,eventID,action,voucherCode,voucherID,fileID,moduleID,featureID,origin,returnStatus,segments, tenantID, offerID);
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
  private Map<Pair<String,String>,Integer> segments;
  private int tenantID;
  private String offerID;

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
  public Map<Pair<String, String>, Integer> getSegments(){return segments;}
  public int getTenantID() { return tenantID; }
  public String getOfferID() { return offerID; }


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

  public VoucherChange(SubscriberProfile subscriberProfile, Date eventDate, Date newVoucherExpiryDate, String eventID, VoucherChangeAction action, String voucherCode, String voucherID, String fileID, String moduleID, String featureID, String origin, RESTAPIGenericReturnCodes returnStatus, int tenantID, String offerID) {
    this(subscriberProfile.getSubscriberID(), eventDate, newVoucherExpiryDate, eventID, action, voucherCode, voucherID, fileID, moduleID, featureID, origin, returnStatus, subscriberProfile.getSegments(), tenantID, offerID);
  }
  public VoucherChange(String subscriberID, Date eventDate, Date newVoucherExpiryDate, String eventID, VoucherChangeAction action, String voucherCode, String voucherID, String fileID, String moduleID, String featureID, String origin, RESTAPIGenericReturnCodes returnStatus, Map<Pair<String,String>,Integer> segments, int tenantID, String offerID) {
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
    this.segments = segments;
    this.tenantID = tenantID;
    this.offerID = offerID;
  }
  
  public VoucherChange(VoucherChange voucherChange) {
    this.subscriberID = voucherChange.getSubscriberID();
    this.eventDate = voucherChange.getEventDate();
    this.newVoucherExpiryDate = voucherChange.getNewVoucherExpiryDate();
    this.eventID = voucherChange.getEventID();
    this.action = voucherChange.getAction();
    this.voucherCode = voucherChange.getVoucherCode();
    this.voucherID = voucherChange.getVoucherID();
    this.fileID = voucherChange.getFileID();
    this.moduleID = voucherChange.getModuleID();
    this.featureID = voucherChange.getFeatureID();
    this.origin = voucherChange.getOrigin();
    this.returnStatus = voucherChange.getReturnStatus();
    this.segments = voucherChange.getSegments();
    this.tenantID = voucherChange.getTenantID();
    this.offerID = voucherChange.getOfferID();
  }

  public VoucherChange(SchemaAndValue schemaAndValue, String subscriberID, Date eventDate, Date newVoucherExpiryDate, String eventID, VoucherChangeAction action, String voucherCode, String voucherID, String fileID, String moduleID, String featureID, String origin, RESTAPIGenericReturnCodes returnStatus, Map<Pair<String,String>,Integer> segments, int tenantID, String offerID) {
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
    this.segments = segments;
    this.tenantID = tenantID;
    this.offerID = offerID;
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
            ", returnStatus=" + returnStatus.getGenericResponseMessage() + '\'' +
            ", tenantID='" + tenantID + 
            ", offerID='" + offerID +
            '}';
  }
  

  /****************************************
  *
  *  packSegments
  *
  ****************************************/

  private static Object packSegments(Map<Pair<String,String>, Integer> segments)
  {
    Map<Object, Object> result = new HashMap<Object, Object>();
    for (Pair<String,String> groupID : segments.keySet())
      {
        String dimensionID = groupID.getFirstElement();
        String segmentID = groupID.getSecondElement();
        Integer epoch = segments.get(groupID);
        Struct packedGroupID = new Struct(groupIDSchema);
        packedGroupID.put("subscriberGroupIDs", Arrays.asList(dimensionID, segmentID));
        result.put(packedGroupID, epoch);
      }
    return result;
  }
  
  /*****************************************
  *
  *  unpackSegments
  *
  *****************************************/

  private static Map<Pair<String,String>, Integer> unpackSegments(Object value)
  {
    Map<Pair<String,String>, Integer> result = new HashMap<Pair<String,String>, Integer>();
    if (value != null)
      {
        Map<Object, Integer> valueMap = (Map<Object, Integer>) value;
        for (Object packedGroupID : valueMap.keySet())
          {
            List<String> subscriberGroupIDs = (List<String>) ((Struct) packedGroupID).get("subscriberGroupIDs");
            Pair<String,String> groupID = new Pair<String,String>(subscriberGroupIDs.get(0), subscriberGroupIDs.get(1));
            Integer epoch = valueMap.get(packedGroupID);
            result.put(groupID, epoch);
          }
      }
    return result;
  }

  /*****************************************
  *
  *  unpackSegmentsV1
  *
  *****************************************/

  private static Map<Pair<String,String>, Integer> unpackSegmentsV1(Object value)
  {
    Map<Pair<String,String>, Integer> result = new HashMap<Pair<String,String>, Integer>();
    if (value != null)
      {
        Map<Object, Integer> valueMap = (Map<Object, Integer>) value;
        for (Object packedGroupID : valueMap.keySet())
          {
            Pair<String,String> groupID = new Pair<String,String>(((Struct) packedGroupID).getString("dimensionID"), ((Struct) packedGroupID).getString("segmentID"));
            Integer epoch = valueMap.get(packedGroupID);
            result.put(groupID, epoch);
          }
      }
    return result;
  }


  public Map<String, String> getStatisticsSegmentsMap(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, SegmentationDimensionService segmentationDimensionService)
  {
    Map<String, String> result = new HashMap<String, String>();
    if (segments != null) {
      for (Pair<String, String> groupID : segments.keySet()) {
        String dimensionID = groupID.getFirstElement();
        boolean statistics = false;
        if (dimensionID != null) {
          GUIManagedObject segmentationDimensionObject = segmentationDimensionService
              .getStoredSegmentationDimension(dimensionID);
          if (segmentationDimensionObject != null && segmentationDimensionObject instanceof SegmentationDimension) {
            SegmentationDimension segmenationDimension = (SegmentationDimension) segmentationDimensionObject;
            statistics = segmenationDimension.getStatistics();
          }
        }
        if (statistics) {
          int epoch = segments.get(groupID);
          if (epoch == (subscriberGroupEpochReader.get(dimensionID) != null
              ? subscriberGroupEpochReader.get(dimensionID).getEpoch()
                  : 0)) {
                    result.put(dimensionID, groupID.getSecondElement());
            }
        }
      }
    }
    return result;
  }

  
  
}
