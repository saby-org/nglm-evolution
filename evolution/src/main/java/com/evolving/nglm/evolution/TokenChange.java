/****************************************************************************
*
*  TokenChange.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.*;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import java.util.ArrayList;

public class TokenChange extends SubscriberStreamOutput implements EvolutionEngineEvent, Action
{
  
  // static
  
  public static final String CREATE   = "Create";
  public static final String ALLOCATE = "Allocate";
  public static final String REDEEM   = "Redeem";
  public static final String RESEND   = "Resend";
  public static final String REFUSE   = "Refuse";
  public static final String EXTEND   = "Extend";

  //public static final String OK = "OK";
  //public static final String BAD_TOKEN_TYPE = "invalid token type";
  //public static final String ALREADY_REDEEMED = "already redeemed";
  //public static final String NO_TOKEN = "no token";
  
  /*****************************************
  *
  * schema
  *
  *****************************************/

  //
  // schema
  //

  private static Schema schema = null;
  private static Schema groupIDSchema = null;

  static
  {
    //
    //  groupID schema
    //

    SchemaBuilder groupIDSchemaBuilder = SchemaBuilder.struct();
    groupIDSchemaBuilder.name("subscribergroup_groupid");
    groupIDSchemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    groupIDSchemaBuilder.field("subscriberGroupIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(new ArrayList<String>()).schema());
    groupIDSchema = groupIDSchemaBuilder.build();

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("token_change");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),11));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDateTime", Timestamp.builder().schema());
    schemaBuilder.field("eventID", Schema.STRING_SCHEMA);
    schemaBuilder.field("tokenCode", Schema.STRING_SCHEMA);
    schemaBuilder.field("action", Schema.STRING_SCHEMA);
    schemaBuilder.field("returnStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("origin", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("moduleID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("featureID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("callUniqueIdentifier", Schema.OPTIONAL_STRING_SCHEMA);//map to an AcceptanceLog
    schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
    schemaBuilder.field("acceptedOfferID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("presentedOfferIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("segments", SchemaBuilder.map(groupIDSchema, Schema.INT32_SCHEMA).name("tokenchange_segments").schema());
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<TokenChange> serde = new ConnectSerde<TokenChange>(schema, false, TokenChange.class, TokenChange::pack, TokenChange::unpack);

  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<TokenChange> serde() { return serde; }

  /****************************************
  *
  * data
  *
  ****************************************/

  private String subscriberID;
  private Date eventDateTime;
  private String eventID;
  private String tokenCode;
  private String action;
  private String returnStatus;
  private String origin;
  private String moduleID;
  private String featureID;
  private String callUniqueIdentifier;
  private Map<Pair<String,String>,Integer> segments;
  private int tenantID;
  private String acceptedOfferID;
  private List<String> presentedOfferIDs;
  
  /****************************************
  *
  * accessors - basic
  *
  ****************************************/

  //
  // accessor
  //

  @Override
  public String getSubscriberID() { return subscriberID; }
  public Date geteventDateTime() { return eventDateTime; }
  public String getEventID() { return eventID; }
  public String getTokenCode() { return tokenCode; }
  public String getAction() { return action; }
  public String getReturnStatus() { return returnStatus; }
  public String getOrigin() { return origin; }
  public String getModuleID() { return moduleID; }
  public String getFeatureID() { return featureID; }
  public ActionType getActionType() { return ActionType.TokenChange; }
  public String getCallUniqueIdentifier() { return callUniqueIdentifier; }
  public Map<Pair<String, String>, Integer> getSegments(){return segments;}
  public int getTenantID() { return tenantID; }
  public String getAcceptedOfferID() { return acceptedOfferID;}
  public List<String> getPresentedOffersIDs() {return presentedOfferIDs;}

  //
  //  setters
  //

  public void setSubscriberID(String subscriberID) { this.subscriberID = subscriberID; }
  public void seteventDateTime(Date eventDateTime) { this.eventDateTime = eventDateTime; }
  public void setEventID(String eventID) { this.eventID = eventID; }
  public void setTokenCode(String tokenCode) { this.tokenCode = tokenCode; }
  public void setAction(String action) { this.action = action; }
  public void setReturnStatus(String returnStatus) { this.returnStatus = returnStatus; }
  public void setOrigin(String origin) { this.origin = origin; }
  public void setModuleID(String moduleID) { this.moduleID = moduleID; }
  public void setFeatureID(String featureID) { this.featureID = featureID; }
  public void setCallUniqueIdentifier(String callUniqueIdentifier) { this.callUniqueIdentifier = callUniqueIdentifier; }
  public void setAcceptedOfferID(String acceptedOfferID){this.acceptedOfferID=acceptedOfferID;}
  public void setPresentedOffersIDs(List<String> presentedOfferIDs) {this.presentedOfferIDs=presentedOfferIDs; }
  /*****************************************
  *
  * constructor (simple)
  *
  *****************************************/
  public TokenChange(SubscriberProfile subscriberProfile, Date eventDateTime, String eventID, String tokenCode, String action, String returnStatus, String origin, Module module, String featureID, int tenantID)
  {
    this(subscriberProfile.getSubscriberID(), eventDateTime, eventID, tokenCode, action, returnStatus, origin, module.getExternalRepresentation(), featureID, null, subscriberProfile.getSegments(), tenantID);
  }
  public TokenChange(String subscriberID, Date eventDateTime, String eventID, String tokenCode, String action, String returnStatus, String origin, Module module, String featureID, int tenantID,String acceptedOfferID,List<String> presentedOfferIDs)
  {
    this(subscriberID, eventDateTime, eventID, tokenCode, action, returnStatus, origin, module.getExternalRepresentation(), featureID, null, tenantID,acceptedOfferID,presentedOfferIDs);
  }
  
  /*****************************************
  *
  * constructor (simple)
  *
  *****************************************/

  public TokenChange(String subscriberID, Date eventDateTime, String eventID, String tokenCode, String action, String returnStatus, String origin, String moduleID, String featureID, String callUniqueIdentifier, Map<Pair<String, String>, Integer> segments, int tenantID)
  {
    this.subscriberID = subscriberID;
    this.eventDateTime = eventDateTime;
    this.eventID = eventID;
    this.tokenCode = tokenCode;
    this.action = action;
    this.returnStatus = returnStatus;
    this.origin = origin;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.callUniqueIdentifier = callUniqueIdentifier;
    this.segments = segments;
    this.tenantID = tenantID;
  }
   public TokenChange(String subscriberID, Date eventDateTime, String eventID, String tokenCode, String action, String returnStatus, String origin, String moduleID, String featureID, String callUniqueIdentifier, String acceptedOfferID, List<String> presentedOfferIDs, Map<Pair<String, String>, Integer> segments, int tenantID)
  {
    this.subscriberID = subscriberID;
    this.eventDateTime = eventDateTime;
    this.eventID = eventID;
    this.tokenCode = tokenCode;
    this.action = action;
    this.returnStatus = returnStatus;
    this.origin = origin;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.callUniqueIdentifier = callUniqueIdentifier;
    this.acceptedOfferID=acceptedOfferID;
    this.presentedOfferIDs = presentedOfferIDs;
    this.segments = segments;

  }  

  /*****************************************
   *
   * constructor unpack
   *
   *****************************************/
  public TokenChange(SchemaAndValue schemaAndValue, String subscriberID, Date eventDateTime, String eventID, String tokenCode, String action, String returnStatus, String origin, String moduleID, String featureID, String callUniqueIdentifier, Map<Pair<String, String>, Integer> segments, int tenantID)
  {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
    this.eventDateTime = eventDateTime;
    this.eventID = eventID;
    this.tokenCode = tokenCode;
    this.action = action;
    this.returnStatus = returnStatus;
    this.origin = origin;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.tenantID = tenantID;
    this.segments = segments;
    this.callUniqueIdentifier = callUniqueIdentifier;
  }

  public TokenChange(SchemaAndValue schemaAndValue, String subscriberID, Date eventDateTime, String eventID, String tokenCode, String action, String returnStatus, String origin, String moduleID, String featureID, String callUniqueIdentifier, int tenantID, String acceptedOfferID,List<String> presentedOfferIDs)
  {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
    this.eventDateTime = eventDateTime;
    this.eventID = eventID;
    this.tokenCode = tokenCode;
    this.action = action;
    this.returnStatus = returnStatus;
    this.origin = origin;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.tenantID = tenantID;
    this.callUniqueIdentifier = callUniqueIdentifier;
    this.acceptedOfferID = acceptedOfferID;
    this.presentedOfferIDs = presentedOfferIDs;
  }

  /*****************************************
  *
  * pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    TokenChange tokenChange = (TokenChange) value;
    Struct struct = new Struct(schema);
    packSubscriberStreamOutput(struct,tokenChange);
    struct.put("subscriberID",tokenChange.getSubscriberID());
    struct.put("eventDateTime",tokenChange.geteventDateTime());
    struct.put("eventID", tokenChange.getEventID());
    struct.put("tokenCode", tokenChange.getTokenCode());
    struct.put("action", tokenChange.getAction());
    struct.put("returnStatus", tokenChange.getReturnStatus());
    struct.put("origin", tokenChange.getOrigin());
    struct.put("moduleID", tokenChange.getModuleID());
    struct.put("featureID", tokenChange.getFeatureID());
    if(tokenChange.getCallUniqueIdentifier()!=null) struct.put("callUniqueIdentifier", tokenChange.getCallUniqueIdentifier());
    struct.put("segments",packSegments(tokenChange.getSegments()));
    struct.put("tenantID", (short) tokenChange.getTenantID());
    struct.put("acceptedOfferID", tokenChange.getAcceptedOfferID()); 
   
    if(tokenChange.getPresentedOffersIDs()==null)
    struct.put("presentedOfferIDs", new ArrayList<String>()); 
    else
    struct.put("presentedOfferIDs", tokenChange.getPresentedOffersIDs());
    return struct;
  }

  /*****************************************
  *
  * unpack
  *
  *****************************************/

  public static TokenChange unpack(SchemaAndValue schemaAndValue)
  {
    //
    // data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    // unpack
    //

    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDateTime = (Date) valueStruct.get("eventDateTime");
    String eventID = valueStruct.getString("eventID");
    String tokenCode = valueStruct.getString("tokenCode");
    String action = valueStruct.getString("action");
    String returnStatus = valueStruct.getString("returnStatus");
    String origin = valueStruct.getString("origin");
    String moduleID = (schemaVersion >=2 ) ? valueStruct.getString("moduleID") : "";
    String featureID = (schemaVersion >=3 ) ? valueStruct.getString("featureID") : "";
    String callUniqueIdentifier = (schemaVersion >=10 && schema.field("callUniqueIdentifier")!=null) ? valueStruct.getString("callUniqueIdentifier") : null;
    Map<Pair<String,String>, Integer> segments = (schemaVersion >= 11) ? unpackSegments(valueStruct.get("segments")) : unpackSegmentsV1(valueStruct.get("subscriberGroups"));

    int tenantID = (schemaVersion >= 9)? valueStruct.getInt16("tenantID") : 1; // for old system, default to tenant 1
    String acceptedOfferID =  (schemaVersion >=11 && schema.field("acceptedOfferID")!=null) ? valueStruct.getString("acceptedOfferID") : null;
    List<String> presentedOfferIDs = (schemaVersion >=11 && schema.field("presentedOfferIDs")!=null) ? (List<String>) valueStruct.get("presentedOfferIDs") : new ArrayList<>();

    //
    // return
    //

    return new TokenChange(schemaAndValue, subscriberID, eventDateTime, eventID, tokenCode, action, returnStatus, origin, moduleID, featureID, callUniqueIdentifier, acceptedOfferID,presentedOfferIDs, segments, tenantID);
  }
  
  
  @Override
  public Date getEventDate()
  {
    return geteventDateTime();
  }
  
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
    return "token change";
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
