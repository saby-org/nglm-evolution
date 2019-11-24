/*****************************************
*
*  DeliveryRequest.java
*
*****************************************/

package com.evolving.nglm.evolution;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;

public abstract class DeliveryRequest implements EvolutionEngineEvent, SubscriberStreamOutput, Action, Comparable
{
  /*****************************************
  *
  *  presentation-keys
  *
  *****************************************/
	
  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DeliveryRequest.class);
  
  //
  // this
  //
  
  public static final String DELIVERYREQUESTID = "deliveryRequestID";
  public static final String ORIGINATINGDELIVERYREQUESTID = "originatingDeliveryRequestID";
  public static final String DELIVERYSTATUS = "deliveryStatus";
  public static final String EVENTID = "eventID";
  public static final String EVENTDATE = "eventDate";
  public static final String ACTIVITYTYPE = "activityType";
  public static final String DELIVERYDATE = "deliveryDate";
  public static final String CREATIONDATE = "creationDate";
  
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
  public static final String PROVIDERNAME = "providerName";
  public static final String DELIVERABLEID = "deliverableId";
  public static final String DELIVERABLENAME = "deliverableName";
  public static final String DELIVERABLEDISPLAY = "deliverableDisplay";
  public static final String DELIVERABLEQTY = "deliverableQty";
  public static final String OPERATION = "operation";
  public static final String VALIDITYPERIODTYPE = "validityPeriodType";
  public static final String VALIDITYPERIODQUANTITY = "validityPeriodQuantity";
  public static final String DELIVERABLEEXPIRATIONDATE = "deliverableExpirationDate";
  public static final String MODULEID = "moduleId";
  public static final String MODULENAME = "moduleName";
  public static final String FEATUREID = "featureId";
  public static final String FEATURENAME = "featureName";
  public static final String FEATUREDISPLAY = "featureDisplay";
  public static final String ORIGIN = "origin";
  
  //
  // ODRs
  //
  
  public static final String PURCHASEID = "purchaseId";
  public static final String OFFERID = "offerId";
  public static final String OFFERNAME = "offerName";
  public static final String OFFERDISPLAY = "offerDisplay";
  public static final String OFFERQTY = "offerQty";
  public static final String SALESCHANNELID = "salesChannelId";
  public static final String SALESCHANNEL = "salesChannel";
  public static final String SALESCHANNELS = "salesChannels";
  public static final String OFFERPRICE = "offerPrice";
  public static final String OFFERSTOCK = "offerStock";
  public static final String OFFERCONTENT = "offerContent";
  public static final String MEANOFPAYMENT = "meanOfPayment";
  public static final String VOUCHERCODE = "voucherCode";
  public static final String VOUCHERPARTNERID = "voucherPartnerId";
  
  //
  // Messages
  //
  
  public static final String MESSAGE_ID = "messageID";
  public static final String SOURCE = "source";
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
    Journey_Manager("1"),
    Loyalty_Program("2"),
    Offer_Catalog("3"),
    Delivery_Manager("4"),
    Customer_Care("5"),
    REST_API("6"),
    Unknown("999");
    private String externalRepresentation;
    private Module(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
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
    Other(0),
    Unknown(-1);
    private Integer externalRepresentation;
    private ActivityType(Integer externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public Integer getExternalRepresentation() { return externalRepresentation; }
    public static ActivityType fromExternalRepresentation(Integer externalRepresentation) { for (ActivityType enumeratedValue : ActivityType.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  enum - DeliveryPriority
  *
  ****************************************/

  public enum DeliveryPriority
  {
    Urgent("urgent", 2),
    High("high", 1),
    Standard("standard", 0),
    Unknown("(unknown)", -1);
    private String externalRepresentation;
    private int topicIndex;
    private DeliveryPriority(String externalRepresentation, int topicIndex) { this.externalRepresentation = externalRepresentation; this.topicIndex = topicIndex; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public int getTopicIndex() { return topicIndex; }
    public static DeliveryPriority fromExternalRepresentation(String externalRepresentation) { for (DeliveryPriority enumeratedValue : DeliveryPriority.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(3));
    schemaBuilder.field("deliveryRequestID", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryRequestSource", Schema.STRING_SCHEMA);
    schemaBuilder.field("originatingDeliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("creationDate", Schema.INT64_SCHEMA);
    schemaBuilder.field("originatingRequest", SchemaBuilder.bool().defaultValue(true).schema());
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryPriority", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventID", Schema.STRING_SCHEMA);
    schemaBuilder.field("moduleID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("featureID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("deliveryPartition", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("retries", Schema.INT32_SCHEMA);
    schemaBuilder.field("timeout", Schema.OPTIONAL_INT64_SCHEMA);
    schemaBuilder.field("correlator", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("control", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("segmentContactPolicyID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("deliveryType", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryStatus", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryDate", Schema.OPTIONAL_INT64_SCHEMA);
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
  private String originatingDeliveryRequestID;
  private boolean originatingRequest;
  private Date creationDate;
  private String subscriberID;
  private DeliveryPriority deliveryPriority;
  private String eventID;
  private String moduleID;
  private String featureID;
  private Integer deliveryPartition;
  private int retries;
  private Date timeout;
  private String correlator;
  private boolean control;
  private String segmentContactPolicyID;
  private String deliveryType;
  private DeliveryStatus deliveryStatus;
  private Date deliveryDate;
  private Map<String, String> diplomaticBriefcase;
  private Date rescheduledDate;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDeliveryRequestID() { return deliveryRequestID; }
  public String getDeliveryRequestSource() { return deliveryRequestSource; }
  public String getOriginatingDeliveryRequestID() { return originatingDeliveryRequestID; }
  public boolean getOriginatingRequest() { return originatingRequest; }
  public Date getCreationDate() { return creationDate; }
  public String getSubscriberID() { return subscriberID; }
  public DeliveryPriority getDeliveryPriority() { return deliveryPriority; }
  public String getEventID() { return eventID; }
  public String getModuleID() { return moduleID; }
  public String getFeatureID() { return featureID; }
  public Integer getDeliveryPartition() { return deliveryPartition; }
  public int getRetries() { return retries; }
  public Date getTimeout() { return timeout; }
  public String getCorrelator() { return correlator; }
  public boolean getControl() { return control; }
  public String getSegmentContactPolicyID() { return segmentContactPolicyID; }
  public String getDeliveryType() { return deliveryType; }
  public DeliveryStatus getDeliveryStatus() { return deliveryStatus; }
  public Date getDeliveryDate() { return deliveryDate; }
  public Date getEventDate() { return (deliveryDate != null) ? deliveryDate : creationDate; }
  public Map<String, String> getDiplomaticBriefcase() { return diplomaticBriefcase; }
  public ActionType getActionType() { return ActionType.DeliveryRequest; }
  public boolean isPending() { return deliveryStatus == DeliveryStatus.Pending; }
  public Date getRescheduledDate() { return rescheduledDate; }

  //
  //  setters
  //

  public void setOriginatingDeliveryRequestID(String originatingDeliveryRequestID) { this.originatingDeliveryRequestID = originatingDeliveryRequestID; }
  public void setControl(boolean control) { this.control = control; }
  public void setDeliveryPartition(int deliveryPartition) { this.deliveryPartition = deliveryPartition; }
  public void setRetries(int retries) { this.retries = retries; }
  public void setTimeout(Date timeout) { this.timeout = timeout; }
  public void setCorrelator(String correlator) { this.correlator = correlator; }
  public void setDeliveryStatus(DeliveryStatus deliveryStatus) { this.deliveryStatus = deliveryStatus; }
  public void setDeliveryDate(Date deliveryDate) { this.deliveryDate = deliveryDate; }
  public void setDeliveryPriority(DeliveryPriority deliveryPriority) { this.deliveryPriority = deliveryPriority; }
  public void setEventID(String eventID) { this.eventID = eventID; }
  public void setFeatureID(String featureID) { this.featureID = featureID; }
  public void setModuleID(String moduleID) { this.moduleID = moduleID; }
  public void setDiplomaticBriefcase(Map<String, String> diplomaticBriefcase) { this.diplomaticBriefcase = (diplomaticBriefcase != null) ? diplomaticBriefcase : new HashMap<String,String>(); }
  public void setRescheduledDate(Date rescheduledDate) { this.rescheduledDate = rescheduledDate; }
  
  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  public abstract DeliveryRequest copy();
  public abstract Schema subscriberStreamEventSchema();
  public abstract Object subscriberStreamEventPack(Object value);
  public abstract void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService);
  public abstract void addFieldsForThirdPartyPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService);
  public ActivityType getActivityType() { return ActivityType.Other; }

  /*****************************************
  *
  *  getEventName
  *
  *****************************************/

  public String getEventName()
  {
    switch (getActivityType())
      {
        case BDR:
          return "bonusDelivery";

        case ODR:
          return "offerDelivery";

        case Messages:
          return "messageDelivery";

        default:
          return null;
      }
  }

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
    this.originatingDeliveryRequestID = null;
    this.originatingRequest = true;
    this.creationDate = context.now();
    this.subscriberID = context.getSubscriberState().getSubscriberID();
    this.deliveryPriority = DeliveryPriority.Standard;
    this.eventID = this.deliveryRequestID;
    this.moduleID = null;
    this.featureID = null;
    this.deliveryPartition = null;
    this.retries = 0;
    this.timeout = null;
    this.correlator = null;
    this.control = context.getSubscriberState().getSubscriberProfile().getUniversalControlGroup();
    this.segmentContactPolicyID = context.getSubscriberState().getSubscriberProfile().getSegmentContactPolicyID(context.getSegmentContactPolicyService(), context.getSegmentationDimensionService(), context.getSubscriberGroupEpochReader());
    this.deliveryType = deliveryType;
    this.deliveryStatus = DeliveryStatus.Pending;
    this.deliveryDate = null;
    this.diplomaticBriefcase = new HashMap<String, String>();
    this.rescheduledDate = null;
  }
  
  /*******************************************
  *
  *  constructor -- guimanager (enterCampaign)
  *
  *******************************************/

  protected DeliveryRequest(String uniqueKey, String subscriberID, String deliveryType, String deliveryRequestSource, boolean universalControlGroup)
  {
    /*****************************************
    *
    *  simple fields
    *
    *****************************************/

    this.deliveryRequestID = uniqueKey;
    this.deliveryRequestSource = deliveryRequestSource;
    this.originatingDeliveryRequestID = null;
    this.originatingRequest = true;
    this.creationDate = SystemTime.getCurrentTime();
    this.subscriberID = subscriberID;
    this.deliveryPriority = DeliveryPriority.Standard;
    this.eventID = this.deliveryRequestID;
    this.moduleID = null;
    this.featureID = null;
    this.deliveryPartition = null;
    this.retries = 0;
    this.timeout = null;
    this.correlator = null;
    this.control = universalControlGroup;
    this.segmentContactPolicyID = null;
    this.deliveryType = deliveryType;
    this.deliveryStatus = DeliveryStatus.Pending;
    this.deliveryDate = null;
    this.diplomaticBriefcase = new HashMap<String, String>();
    this.rescheduledDate = null;
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
    this.originatingDeliveryRequestID = deliveryRequest.getOriginatingDeliveryRequestID();
    this.originatingRequest = deliveryRequest.getOriginatingRequest();
    this.creationDate = deliveryRequest.getCreationDate();
    this.subscriberID = deliveryRequest.getSubscriberID();
    this.deliveryPriority = deliveryRequest.getDeliveryPriority();
    this.eventID = deliveryRequest.getEventID();
    this.moduleID = deliveryRequest.getModuleID();
    this.featureID = deliveryRequest.getFeatureID();
    this.deliveryPartition = deliveryRequest.getDeliveryPartition();
    this.retries = deliveryRequest.getRetries();
    this.timeout = deliveryRequest.getTimeout();
    this.correlator = deliveryRequest.getCorrelator();
    this.control = deliveryRequest.getControl();
    this.segmentContactPolicyID = deliveryRequest.getSegmentContactPolicyID();
    this.deliveryType = deliveryRequest.getDeliveryType();
    this.deliveryStatus = deliveryRequest.getDeliveryStatus();
    this.deliveryDate = deliveryRequest.getDeliveryDate();
    this.diplomaticBriefcase = deliveryRequest.getDiplomaticBriefcase();
    this.rescheduledDate = deliveryRequest.getRescheduledDate();
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
    this.originatingDeliveryRequestID = JSONUtilities.decodeString(jsonRoot, "originatingDeliveryRequestID", false);
    this.originatingRequest = JSONUtilities.decodeBoolean(jsonRoot, "originatingRequest", Boolean.TRUE);
    this.creationDate = SystemTime.getCurrentTime();
    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    this.deliveryPriority = DeliveryPriority.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "deliveryPriority", "standard"));
    this.eventID = JSONUtilities.decodeString(jsonRoot, "eventID", true);
    this.moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", true);
    this.featureID = JSONUtilities.decodeString(jsonRoot, "featureID", true);
    this.deliveryPartition = null;
    this.retries = 0;
    this.timeout = null;
    this.correlator = null;
    this.control = JSONUtilities.decodeBoolean(jsonRoot, "control", Boolean.FALSE);
    this.segmentContactPolicyID = null;
    this.deliveryType = JSONUtilities.decodeString(jsonRoot, "deliveryType", true);
    this.deliveryStatus = DeliveryStatus.Pending;
    this.deliveryDate = null;
    this.diplomaticBriefcase = (Map<String, String>) jsonRoot.get("diplomaticBriefcase");
    this.rescheduledDate = JSONUtilities.decodeDate(jsonRoot, "rescheduledDate", false);
  }

  /*****************************************
  *
  *  packCommon
  *
  *****************************************/

  protected static void packCommon(Struct struct, DeliveryRequest deliveryRequest)
  {
    struct.put("deliveryRequestID", deliveryRequest.getDeliveryRequestID());
    struct.put("deliveryRequestSource", deliveryRequest.getDeliveryRequestSource());
    struct.put("originatingDeliveryRequestID", deliveryRequest.getOriginatingDeliveryRequestID());
    struct.put("originatingRequest", deliveryRequest.getOriginatingRequest());
    struct.put("creationDate", deliveryRequest.getCreationDate().getTime());
    struct.put("subscriberID", deliveryRequest.getSubscriberID());
    struct.put("deliveryPriority", deliveryRequest.getDeliveryPriority().getExternalRepresentation());
    struct.put("eventID", deliveryRequest.getEventID());
    struct.put("moduleID", deliveryRequest.getModuleID());
    struct.put("featureID", deliveryRequest.getFeatureID());
    struct.put("deliveryPartition", deliveryRequest.getDeliveryPartition()); 
    struct.put("retries", deliveryRequest.getRetries()); 
    struct.put("timeout", deliveryRequest.getTimeout() != null ? deliveryRequest.getTimeout().getTime() : null); 
    struct.put("correlator", deliveryRequest.getCorrelator()); 
    struct.put("control", deliveryRequest.getControl());
    struct.put("segmentContactPolicyID", deliveryRequest.getSegmentContactPolicyID());
    struct.put("deliveryType", deliveryRequest.getDeliveryType());
    struct.put("deliveryStatus", deliveryRequest.getDeliveryStatus().getExternalRepresentation());
    struct.put("deliveryDate", deliveryRequest.getDeliveryDate() != null ? deliveryRequest.getDeliveryDate().getTime() : null);
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String deliveryRequestID = valueStruct.getString("deliveryRequestID");
    String deliveryRequestSource = valueStruct.getString("deliveryRequestSource");
    String originatingDeliveryRequestID = (schemaVersion >= 3) ? valueStruct.getString("originatingDeliveryRequestID") : null;
    boolean originatingRequest = (schemaVersion >= 2) ? valueStruct.getBoolean("originatingRequest") : true;
    Date creationDate = (schemaVersion >= 3) ? new Date(valueStruct.getInt64("creationDate")) : ((schemaVersion >= 2) ? (Date) valueStruct.get("creationDate") : SystemTime.getCurrentTime());
    String subscriberID = valueStruct.getString("subscriberID");
    DeliveryPriority deliveryPriority = DeliveryPriority.fromExternalRepresentation(valueStruct.getString("deliveryPriority"));
    String eventID = valueStruct.getString("eventID");
    String moduleID = valueStruct.getString("moduleID");
    String featureID = valueStruct.getString("featureID");
    Integer deliveryPartition = valueStruct.getInt32("deliveryPartition");
    int retries = valueStruct.getInt32("retries");
    Date timeout = (schemaVersion >= 3) ? (valueStruct.get("timeout") != null ? new Date(valueStruct.getInt64("timeout")) : null) : (Date) valueStruct.get("timeout");
    String correlator = valueStruct.getString("correlator");
    boolean control = valueStruct.getBoolean("control");
    String segmentContactPolicyID = valueStruct.getString("segmentContactPolicyID");
    String deliveryType = valueStruct.getString("deliveryType");
    DeliveryStatus deliveryStatus = DeliveryStatus.fromExternalRepresentation(valueStruct.getString("deliveryStatus"));
    Date deliveryDate = (schemaVersion >= 3) ? (valueStruct.get("deliveryDate") != null ? new Date(valueStruct.getInt64("deliveryDate")) : null) : (Date) valueStruct.get("deliveryDate");
    Map<String, String> diplomaticBriefcase = (Map<String, String>) valueStruct.get("diplomaticBriefcase");

    //
    //  return
    //

    this.deliveryRequestID = deliveryRequestID;
    this.deliveryRequestSource = deliveryRequestSource;
    this.originatingDeliveryRequestID = originatingDeliveryRequestID;
    this.originatingRequest = originatingRequest;
    this.creationDate = creationDate;
    this.subscriberID = subscriberID;
    this.deliveryPriority = deliveryPriority;
    this.eventID = eventID;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.deliveryPartition = deliveryPartition;
    this.retries = retries;
    this.timeout = timeout;
    this.correlator = correlator;
    this.control = control;
    this.segmentContactPolicyID = segmentContactPolicyID;
    this.deliveryType = deliveryType;
    this.deliveryStatus = deliveryStatus;
    this.deliveryDate = deliveryDate;
    this.diplomaticBriefcase = diplomaticBriefcase;
    this.rescheduledDate = null;
  }

  /****************************************
  *
  *  presentation utilities
  *
  ****************************************/
  
  //
  //  getGUIPresentationMap
  //

  public Map<String, Object> getGUIPresentationMap(SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
  {
    if (! originatingRequest) throw new ServerRuntimeException("presentationMap for non-originating request");
    HashMap<String, Object> guiPresentationMap = new HashMap<String,Object>();
    guiPresentationMap.put(DELIVERYREQUESTID, getDeliveryRequestID());
    guiPresentationMap.put(ORIGINATINGDELIVERYREQUESTID, getOriginatingDeliveryRequestID());
    guiPresentationMap.put(EVENTDATE, getDateString(getEventDate()));
    guiPresentationMap.put(EVENTID, getEventID());    
    guiPresentationMap.put(DELIVERYSTATUS, getDeliveryStatus().getExternalRepresentation()); 
    guiPresentationMap.put(CREATIONDATE, getDateString(getCreationDate()));
    guiPresentationMap.put(DELIVERYDATE, getDateString(getDeliveryDate()));
    guiPresentationMap.put(ACTIVITYTYPE, getActivityType().toString());
    addFieldsForGUIPresentation(guiPresentationMap, subscriberMessageTemplateService, salesChannelService, journeyService, offerService, loyaltyProgramService, productService, deliverableService, paymentMeanService);
    return guiPresentationMap;
  }
  
  //
  //  getThirdPartyPresentationMap
  //

  public Map<String, Object> getThirdPartyPresentationMap(SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
  {
    if (! originatingRequest) throw new ServerRuntimeException("presentationMap for non-originating request");
    HashMap<String, Object> thirdPartyPresentationMap = new HashMap<String,Object>();
    thirdPartyPresentationMap.put(DELIVERYREQUESTID, getDeliveryRequestID());
    thirdPartyPresentationMap.put(ORIGINATINGDELIVERYREQUESTID, getOriginatingDeliveryRequestID());
    thirdPartyPresentationMap.put(EVENTDATE, getDateString(getEventDate()));
    thirdPartyPresentationMap.put(EVENTID, getEventID()); 
    thirdPartyPresentationMap.put(DELIVERYSTATUS, getDeliveryStatus().getExternalRepresentation()); 
    thirdPartyPresentationMap.put(CREATIONDATE, getDateString(getCreationDate()));
    thirdPartyPresentationMap.put(DELIVERYDATE, getDateString(getDeliveryDate()));
    thirdPartyPresentationMap.put(ACTIVITYTYPE, getActivityType().toString());
    addFieldsForThirdPartyPresentation(thirdPartyPresentationMap, subscriberMessageTemplateService, salesChannelService, journeyService, offerService, loyaltyProgramService, productService, deliverableService, paymentMeanService);
    return thirdPartyPresentationMap;
  }
  
  //
  //  getFeatureName
  //

  public static String getFeatureName(Module module, String featureId, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService)
  {
    String featureName = null;

    switch (module)
      {
        case Journey_Manager:
          GUIManagedObject journey = journeyService.getStoredJourney(featureId);
          journey = (journey != null && (journey.getGUIManagedObjectType() == GUIManagedObjectType.Journey || journey.getGUIManagedObjectType() == GUIManagedObjectType.Campaign || journey.getGUIManagedObjectType() == GUIManagedObjectType.BulkCampaign)) ? journey : null;
          featureName = journey == null ? null : journey.getGUIManagedObjectName();
          break;

        case Loyalty_Program:
          GUIManagedObject loyaltyProgram = loyaltyProgramService.getStoredLoyaltyProgram(featureId);
          featureName = loyaltyProgram == null ? null : loyaltyProgram.getGUIManagedObjectName();
          break;

        case Offer_Catalog:
          featureName = offerService.getStoredOffer(featureId).getGUIManagedObjectName();
          break;

        case Delivery_Manager:
          featureName = "Delivery_Manager-its temp"; //To DO
          break;

        case REST_API:
          featureName = "REST_API-its temp"; //To DO
          break;
          
        case Customer_Care:
          featureName = "Customer_Care-its temp"; //To DO
          break;

        case Unknown:
          featureName = "Unknown";
          break;
      }
    return featureName;
  }

  //
  //  getFeatureDisplay
  //

  public static String getFeatureDisplay(Module module, String featureId, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService)
  {
    String featureDisplay = null;

    switch (module)
      {
        case Journey_Manager:
          GUIManagedObject journey = journeyService.getStoredJourney(featureId);
          journey = (journey != null && (journey.getGUIManagedObjectType() == GUIManagedObjectType.Journey || journey.getGUIManagedObjectType() == GUIManagedObjectType.Campaign || journey.getGUIManagedObjectType() == GUIManagedObjectType.BulkCampaign)) ? journey : null;
          featureDisplay = journey == null ? null : journey.getGUIManagedObjectDisplay();
          break;

        case Loyalty_Program:
          GUIManagedObject loyaltyProgram = loyaltyProgramService.getStoredLoyaltyProgram(featureId);
          featureDisplay = loyaltyProgram == null ? null : loyaltyProgram.getGUIManagedObjectDisplay();
          break;

        case Offer_Catalog:
          featureDisplay = offerService.getStoredOffer(featureId).getGUIManagedObjectDisplay();
          break;

        case Delivery_Manager:
          featureDisplay = "Delivery_Manager-its temp"; //To DO
          break;

        case REST_API:
          featureDisplay = "REST_API-its temp"; //To DO
          break;
          
        case Customer_Care:
          featureDisplay = "Customer_Care-its temp"; //To DO
          break;

        case Unknown:
          featureDisplay = "Unknown";
          break;
      }
    return featureDisplay;
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
        if (result == 0) result = creationDate.compareTo(entry.getCreationDate());
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
    b.append("," + originatingDeliveryRequestID);
    b.append("," + originatingRequest);
    b.append("," + creationDate);
    b.append("," + subscriberID);
    b.append("," + deliveryPriority);
    b.append("," + eventID);
    b.append("," + moduleID);
    b.append("," + featureID);
    b.append("," + deliveryPartition);
    b.append("," + retries);
    b.append("," + timeout);
    b.append("," + correlator);
    b.append("," + control);
    b.append("," + segmentContactPolicyID);
    b.append("," + deliveryType);
    b.append("," + deliveryStatus);
    b.append("," + deliveryDate);
    b.append("," + diplomaticBriefcase);
    b.append("," + rescheduledDate);
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
  
  /*****************************************
  *
  *  getDateString
  *
  *****************************************/
  
  public String getDateString(Date date)

  {
    String result = null;
    if (null == date) return result;
    try
      {
        SimpleDateFormat dateFormat = new SimpleDateFormat(Deployment.getAPIresponseDateFormat());
        dateFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
        result = dateFormat.format(date);
      }
    catch (Exception e)
      {
    	log.warn(e.getMessage());
      }
    return result;
  }
}
