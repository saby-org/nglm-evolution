/*****************************************
*
*  DeliveryRequest.java
*
*****************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;

public abstract class DeliveryRequest implements SubscriberStreamEvent, SubscriberStreamOutput, Action, Comparable
{
  
  /*****************************************
  *
  *  presentation-keys
  *
  *****************************************/
  
  //
  // this
  //
  
  public static final String DELIVERYREQUESTID = "deliveryRequestID";
  public static final String EVENTDATETIME = "eventDatetime";
  public static final String DELIVERYSTATUS = "deliveryStatus";
  public static final String EVENTID = "eventID";
  public static final String ACTIVITYTYPE = "activityType";
  
  //
  // child generic
  //
  
  public static final String CUSTOMERID = "customerId";
  public static final String RETURNCODE = "returnCode";
  public static final String RETURNCODEDETAILS = "returnCodeDetails";
  
  //
  // BDRs
  //
  
  public static final String PROVIDERID = "providerId";
  public static final String DELIVERABLEID = "deliverableId";
  public static final String DELIVERABLEQTY = "deliverableQty";
  public static final String OPERATION = "operation";
  public static final String MODULEID = "moduleId";
  public static final String MODULENAME = "moduleName";
  public static final String FEATUREID = "featureId";
  public static final String FEATURENAME = "featureName";
  public static final String ORIGIN = "origin";
  
  //
  // ODRs
  //
  
  public static final String PURCHASEID = "purchaseId";
  public static final String OFFERID = "offerId";
  public static final String OFFERNAME = "offerName";
  public static final String OFFERQTY = "offerQty";
  public static final String SALESCHANNELID = "salesChannelId";
  public static final String SALESCHANNEL = "salesChannel";
  public static final String SALESCHANNELS = "salesChannels";
  public static final String OFFERPRICE = "offerPrice";
  public static final String OFFERSTOCK = "offerStock";
  public static final String OFFERCONTENT = "offerContent";
  public static final String VOUCHERCODE = "voucherCode";
  public static final String VOUCHERPARTNERID = "voucherPartnerId";
  
  //
  // Messages
  //
  
  public static final String NOTIFICATION_CHANNEL = "notificationChannel";
  public static final String NOTIFICATION_SUBJECT = "subject";
  public static final String NOTIFICATION_TEXT_BODY = "textBody";
  public static final String NOTIFICATION_HTML_BODY = "htmlBody";
  public static final String NOTIFICATION_RECIPIENT = "recipient";
  
  /*****************************************
  *
  *  enum - module
  *
  *****************************************/
  
  public enum Module
  {
    Campaign_Manager(1),
    Journey_Manager(2),
    Offer_Catalog(3),
    Delivery_Manager(4),
    Customer_Care(5),
    REST_API(6),
    Unknown(999);
    private Integer externalRepresentation;
    private Module(Integer externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public Integer getExternalRepresentation() { return externalRepresentation; }
    public static Module fromExternalRepresentation(String externalRepresentation) { for (Module enumeratedValue : Module.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
  /*****************************************
  *
  *  enum - ActivityType
  *
  *****************************************/
  
  public enum ActivityType
  {
    BDR(1),
    ODR(2),
    Messages(3),
    Unknown(-1);
    private Integer externalRepresentation;
    private ActivityType(Integer externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public Integer getExternalRepresentation() { return externalRepresentation; }
    public static ActivityType fromActivityTypeExternalRepresentation(Integer externalRepresentation) { for (ActivityType enumeratedValue : ActivityType.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
  /*****************************************
  *
  *  schema/serde
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema commonSchema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("delivery_request");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("deliveryRequestID", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryRequestSource", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventID", Schema.STRING_SCHEMA);
    schemaBuilder.field("moduleID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("featureID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("deliveryPartition", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("retries", Schema.INT32_SCHEMA);
    schemaBuilder.field("timeout", Timestamp.builder().optional().schema());
    schemaBuilder.field("correlator", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("control", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("deliveryType", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryStatus", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("diplomaticBriefcase", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).name("deliveryrequest_diplomaticBriefcase").schema());
    commonSchema = schemaBuilder.build();
  };

  //
  //  commonSerde
  //

  private static ConnectSerde<DeliveryRequest> commonSerde = null;
  private static void initializeCommonSerde()
  {
    //
    //  get serdes from registered delivery classes
    //

    List<ConnectSerde<DeliveryRequest>> deliveryRequestSerdes = new ArrayList<ConnectSerde<DeliveryRequest>>();
    for (DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values())
      {
        deliveryRequestSerdes.add((ConnectSerde<DeliveryRequest>) deliveryManager.getRequestSerde());
      }

    //
    //  return
    //

    commonSerde = new ConnectSerde<DeliveryRequest>("deliveryrequest", false, deliveryRequestSerdes.toArray(new ConnectSerde[0]));
  };

  //
  //  accessor
  //

  public static Schema commonSchema() { return commonSchema; }
  public static ConnectSerde<DeliveryRequest> commonSerde() { if (commonSerde == null) initializeCommonSerde(); return commonSerde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String deliveryRequestID;
  private String deliveryRequestSource;
  private String subscriberID;
  private String eventID;
  private String moduleID;
  private String featureID;
  private Integer deliveryPartition;
  private int retries;
  private Date timeout;
  private String correlator;
  private boolean control;
  private String deliveryType;
  private DeliveryStatus deliveryStatus;
  private Date deliveryDate;
  private Map<String, String> diplomaticBriefcase;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDeliveryRequestID() { return deliveryRequestID; }
  public String getDeliveryRequestSource() { return deliveryRequestSource; }
  public String getSubscriberID() { return subscriberID; }
  public String getEventID() { return eventID; }
  public String getModuleID() { return moduleID; }
  public String getFeatureID() { return featureID; }
  public Integer getDeliveryPartition() { return deliveryPartition; }
  public int getRetries() { return retries; }
  public Date getTimeout() { return timeout; }
  public String getCorrelator() { return correlator; }
  public boolean getControl() { return control; }
  public String getDeliveryType() { return deliveryType; }
  public DeliveryStatus getDeliveryStatus() { return deliveryStatus; }
  public Date getDeliveryDate() { return deliveryDate; }
  public Date getEventDate() { return deliveryDate; }
  public Map<String, String> getDiplomaticBriefcase() { return diplomaticBriefcase; }
  public ActionType getActionType() { return ActionType.DeliveryRequest; }

  //
  //  setters
  //

  public void setControl(boolean control) { this.control = control; }
  public void setDeliveryPartition(int deliveryPartition) { this.deliveryPartition = deliveryPartition; }
  public void setRetries(int retries) { this.retries = retries; }
  public void setTimeout(Date timeout) { this.timeout = timeout; }
  public void setCorrelator(String correlator) { this.correlator = correlator; }
  public void setDeliveryStatus(DeliveryStatus deliveryStatus) { this.deliveryStatus = deliveryStatus; }
  public void setDeliveryDate(Date deliveryDate) { this.deliveryDate = deliveryDate; }
  public void setEventID(String eventID) { this.eventID = eventID; }
  public void setFeatureID(String featureID) { this.featureID = featureID; }
  public void setModuleID(String moduleID) { this.moduleID = moduleID; }
  public void setDiplomaticBriefcase(Map<String, String> diplomaticBriefcase) { this.diplomaticBriefcase = (diplomaticBriefcase != null) ? diplomaticBriefcase : new HashMap<String,String>(); }
  
  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  public abstract DeliveryRequest copy();
  public abstract Schema subscriberStreamEventSchema();
  public abstract Object subscriberStreamEventPack(Object value);
  public abstract void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap);
  public abstract void addFieldsForThirdPartyPresentation(HashMap<String, Object> guiPresentationMap);
  public abstract Integer getActivityType();

  /*****************************************
  *
  *  constructor -- evolution engine
  *
  *****************************************/

  protected DeliveryRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource)
  {
    /*****************************************
    *
    *  simple fields
    *
    *****************************************/

    this.deliveryRequestID = context.getUniqueKey();
    this.deliveryRequestSource = deliveryRequestSource;
    this.subscriberID = context.getSubscriberState().getSubscriberID();
    this.eventID = this.deliveryRequestID;
    this.moduleID = null;
    this.featureID = null;
    this.deliveryPartition = null;
    this.retries = 0;
    this.timeout = null;
    this.correlator = null;
    this.control = context.getSubscriberState().getSubscriberProfile().getUniversalControlGroup(context.getSubscriberGroupEpochReader());
    this.deliveryType = deliveryType;
    this.deliveryStatus = DeliveryStatus.Pending;
    this.deliveryDate = null;
    this.diplomaticBriefcase = new HashMap<String, String>();
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  protected DeliveryRequest(DeliveryRequest deliveryRequest)
  {
    this.deliveryRequestID = deliveryRequest.getDeliveryRequestID();
    this.deliveryRequestSource = deliveryRequest.getDeliveryRequestSource();
    this.subscriberID = deliveryRequest.getSubscriberID();
    this.eventID = deliveryRequest.getEventID();
    this.moduleID = deliveryRequest.getModuleID();
    this.featureID = deliveryRequest.getFeatureID();
    this.deliveryPartition = deliveryRequest.getDeliveryPartition();
    this.retries = deliveryRequest.getRetries();
    this.timeout = deliveryRequest.getTimeout();
    this.correlator = deliveryRequest.getCorrelator();
    this.control = deliveryRequest.getControl();
    this.deliveryType = deliveryRequest.getDeliveryType();
    this.deliveryStatus = deliveryRequest.getDeliveryStatus();
    this.deliveryDate = deliveryRequest.getDeliveryDate();
    this.diplomaticBriefcase = deliveryRequest.getDiplomaticBriefcase();
  }

  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  protected DeliveryRequest(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  simple fields
    *
    *****************************************/

    this.deliveryRequestID = JSONUtilities.decodeString(jsonRoot, "deliveryRequestID", true);
    this.deliveryRequestSource = "external";
    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    this.eventID = JSONUtilities.decodeString(jsonRoot, "eventID", true);
    this.moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", true);
    this.featureID = JSONUtilities.decodeString(jsonRoot, "featureID", true);
    this.deliveryPartition = null;
    this.retries = 0;
    this.timeout = null;
    this.correlator = null;
    this.control = JSONUtilities.decodeBoolean(jsonRoot, "control", Boolean.FALSE);
    this.deliveryType = JSONUtilities.decodeString(jsonRoot, "deliveryType", true);
    this.deliveryStatus = DeliveryStatus.Pending;
    this.deliveryDate = null;
    this.diplomaticBriefcase = (Map<String, String>) jsonRoot.get("diplomaticBriefcase");
  }

//private HashMap<String, Object> decodeDiplomaticBriefcase(JSONObject jsonRoot){
//HashMap<String, Object> result = new HashMap<String, Object>();
//for (Object keyObject : jsonRoot.keySet())
//  {
//    String key = (String)keyObject;
//    Object value = (String)jsonRoot.get(key);
//    result.put(key, value);
//  }
//return result;
//}

  /*****************************************
  *
  *  packCommon
  *
  *****************************************/

  protected static void packCommon(Struct struct, DeliveryRequest deliveryRequest)
  {
    struct.put("deliveryRequestID", deliveryRequest.getDeliveryRequestID());
    struct.put("deliveryRequestSource", deliveryRequest.getDeliveryRequestSource());
    struct.put("subscriberID", deliveryRequest.getSubscriberID());
    struct.put("eventID", deliveryRequest.getEventID());
    struct.put("moduleID", deliveryRequest.getModuleID());
    struct.put("featureID", deliveryRequest.getFeatureID());
    struct.put("deliveryPartition", deliveryRequest.getDeliveryPartition()); 
    struct.put("retries", deliveryRequest.getRetries()); 
    struct.put("timeout", deliveryRequest.getTimeout()); 
    struct.put("correlator", deliveryRequest.getCorrelator()); 
    struct.put("control", deliveryRequest.getControl());
    struct.put("deliveryType", deliveryRequest.getDeliveryType());
    struct.put("deliveryStatus", deliveryRequest.getDeliveryStatus().getExternalRepresentation());
    struct.put("deliveryDate", deliveryRequest.getDeliveryDate());
    struct.put("diplomaticBriefcase", (deliveryRequest.getDiplomaticBriefcase() == null ? new HashMap<String, String>() : deliveryRequest.getDiplomaticBriefcase()));
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  protected DeliveryRequest(SchemaAndValue schemaAndValue)
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
    String deliveryRequestID = valueStruct.getString("deliveryRequestID");
    String deliveryRequestSource = valueStruct.getString("deliveryRequestSource");
    String subscriberID = valueStruct.getString("subscriberID");
    String eventID = valueStruct.getString("eventID");
    String moduleID = valueStruct.getString("moduleID");
    String featureID = valueStruct.getString("featureID");
    Integer deliveryPartition = valueStruct.getInt32("deliveryPartition");
    int retries = valueStruct.getInt32("retries");
    Date timeout = (Date) valueStruct.get("timeout");
    String correlator = valueStruct.getString("correlator");
    boolean control = valueStruct.getBoolean("control");
    String deliveryType = valueStruct.getString("deliveryType");
    DeliveryStatus deliveryStatus = DeliveryStatus.fromExternalRepresentation(valueStruct.getString("deliveryStatus"));
    Date deliveryDate = (Date) valueStruct.get("deliveryDate");
    Map<String, String> diplomaticBriefcase = (Map<String, String>) valueStruct.get("diplomaticBriefcase");;

    //
    //  return
    //

    this.deliveryRequestID = deliveryRequestID;
    this.deliveryRequestSource = deliveryRequestSource;
    this.subscriberID = subscriberID;
    this.eventID = eventID;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.deliveryPartition = deliveryPartition;
    this.retries = retries;
    this.timeout = timeout;
    this.correlator = correlator;
    this.control = control;
    this.deliveryType = deliveryType;
    this.deliveryStatus = deliveryStatus;
    this.deliveryDate = deliveryDate;
    this.diplomaticBriefcase = diplomaticBriefcase;
  }

//  /*****************************************
//  *
//  *  toJSONObject
//  *
//  *****************************************/
//  
//  public JSONObject getJSONRepresentation(){
//    Map<String, Object> data = new HashMap<String, Object>();
//    
//    //DeliveryRequest fields
//    data.put("deliveryRequestID", this.getDeliveryRequestID());
//    data.put("deliveryRequestSource", this.getDeliveryRequestSource());
//    data.put("subscriberID", this.getSubscriberID());
//    data.put("deliveryPartition", this.getDeliveryPartition());
//    data.put("retries", this.getRetries());
//    data.put("timeout", this.getTimeout());
//    data.put("correlator", this.getCorrelator());
//    data.put("control", this.getControl());
//    data.put("deliveryType", this.getDeliveryType());
//    data.put("deliveryStatus", this.getDeliveryStatus().toString());
//    data.put("deliveryDate", this.getDeliveryDate());
//    data.put("diplomaticBriefcase", this.getDiplomaticBriefcase());
//    
//    return JSONUtilities.encodeObject(data);
//  }
  
  /****************************************
  *
  *  presentation utilities
  *
  ****************************************/
  
  public Map<String, Object> getGUIPresentationMap()
  {
    HashMap<String, Object> guiPresentationMap = new HashMap<String,Object>();
    guiPresentationMap.put(DELIVERYREQUESTID, getDeliveryRequestID());
    guiPresentationMap.put(EVENTID, getEventID());
    guiPresentationMap.put(EVENTDATETIME, getEventDate());
    guiPresentationMap.put(DELIVERYSTATUS, getDeliveryStatus().getExternalRepresentation());
    guiPresentationMap.put(ACTIVITYTYPE, ActivityType.fromActivityTypeExternalRepresentation(getActivityType()).toString());
    addFieldsForGUIPresentation(guiPresentationMap);
    return guiPresentationMap;
  }
  
  public Map<String, Object> getThirdPartyPresentationMap()
  {
    HashMap<String, Object> thirdPartyPresentationMap = new HashMap<String,Object>();
    thirdPartyPresentationMap.put(DELIVERYREQUESTID, getDeliveryRequestID());
    thirdPartyPresentationMap.put(EVENTID, getEventID());
    thirdPartyPresentationMap.put(EVENTDATETIME, getEventDate());
    thirdPartyPresentationMap.put(DELIVERYSTATUS, getDeliveryStatus().getExternalRepresentation());
    thirdPartyPresentationMap.put(ACTIVITYTYPE, ActivityType.fromActivityTypeExternalRepresentation(getActivityType()).toString());
    addFieldsForGUIPresentation(thirdPartyPresentationMap);
    return thirdPartyPresentationMap;
  }

  /*****************************************
  *
  *  compareTo
  *
  *****************************************/

  public int compareTo(Object obj)
  {
    int result = -1;
    if (obj instanceof DeliveryRequest)
      {
        DeliveryRequest entry = (DeliveryRequest) obj;
        result = (deliveryDate != null && entry.getDeliveryDate() != null) ? deliveryDate.compareTo(entry.getDeliveryDate()) : 0;
        if (result == 0) result = deliveryRequestID.compareTo(entry.getDeliveryRequestID());
      }
    return result;
  }

  /*****************************************
  *
  *  toStringFields
  *
  *****************************************/

  protected String toStringFields()
  {
    StringBuilder b = new StringBuilder();
    b.append(deliveryRequestID);
    b.append("," + deliveryRequestSource);
    b.append("," + subscriberID);
    b.append("," + eventID);
    b.append("," + moduleID);
    b.append("," + featureID);
    b.append("," + deliveryPartition);
    b.append("," + retries);
    b.append("," + timeout);
    b.append("," + correlator);
    b.append("," + control);
    b.append("," + deliveryType);
    b.append("," + deliveryStatus);
    b.append("," + deliveryDate);
    b.append("," + diplomaticBriefcase);
    return b.toString();
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  public String toString()
  {
    StringBuilder b = new StringBuilder();
    b.append("DeliveryRequest:{");
    b.append(toStringFields());
    b.append("}");
    return b.toString();
  }
}
