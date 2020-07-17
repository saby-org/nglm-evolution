/*****************************************
*
*  JourneyRequest.java
*
*****************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;

import com.evolving.nglm.core.*;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;

import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;

public class JourneyRequest extends DeliveryRequest implements SubscriberStreamEvent, Action, BonusDelivery
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
    schemaBuilder.name("journey_request");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),8));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("journeyRequestID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("callingJourneyInstanceID", SchemaBuilder.string().optional().defaultValue(null).schema());
    schemaBuilder.field("callingJourneyNodeID", SchemaBuilder.string().optional().defaultValue(null).schema());
    schemaBuilder.field("waitForCompletion", SchemaBuilder.bool().optional().defaultValue(false).schema());
    schemaBuilder.field("boundParameters", SimpleParameterMap.serde().optionalSchema());
    schemaBuilder.field("journeyStatus", SchemaBuilder.string().optional().defaultValue(null).schema());
    schemaBuilder.field("journeyResults", SimpleParameterMap.serde().optionalSchema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyRequest> serde = new ConnectSerde<JourneyRequest>(schema, false, JourneyRequest.class, JourneyRequest::pack, JourneyRequest::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyRequest> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  *****************************************/

  private String journeyRequestID;
  private Date eventDate;
  private String journeyID;
  private String callingJourneyInstanceID;
  private String callingJourneyNodeID;
  private boolean waitForCompletion;
  private SimpleParameterMap boundParameters;
  private SubscriberJourneyStatus journeyStatus;
  private SimpleParameterMap journeyResults;

  //
  //  transient
  //

  private boolean eligible;
      
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getJourneyRequestID() { return journeyRequestID; }
  public Date getEventDate() { return eventDate; }
  public String getJourneyID() { return journeyID; }
  public String getCallingJourneyInstanceID() { return callingJourneyInstanceID; }
  public String getCallingJourneyNodeID() { return callingJourneyNodeID; }
  public boolean getWaitForCompletion() { return waitForCompletion; }
  public SimpleParameterMap getBoundParameters() { return boundParameters; }
  public SubscriberJourneyStatus getJourneyStatus() { return journeyStatus; }
  public SimpleParameterMap getJourneyResults() { return journeyResults; }
  public boolean getEligible() { return eligible; }
  public ActionType getActionType() { return ActionType.JourneyRequest; }

  //
  //  setters
  //

  public void setEligible(boolean eligible) { this.eligible = eligible; }
  public void setJourneyStatus(SubscriberJourneyStatus journeyStatus) { this.journeyStatus = journeyStatus; }
  public void setJourneyResults(SimpleParameterMap journeyResults) { this.journeyResults = journeyResults; }

  //
  //  structure
  //

  @Override public ActivityType getActivityType() { return ActivityType.BDR; }
  @Override public String getEventName() { return isPending() ? "journeyRequest" : "journeyComplete"; }

  //
  //  bonus delivery accessors
  //

  public int getBonusDeliveryReturnCode() { return 0; }
  public String getBonusDeliveryReturnCodeDetails() { return null; }
  public String getBonusDeliveryOrigin() { return null; }
  public String getBonusDeliveryProviderId() { return null; }
  public String getBonusDeliveryDeliverableId() { return null; }
  public String getBonusDeliveryDeliverableName() { return null; }
  public int getBonusDeliveryDeliverableQty() { return 0; }
  public String getBonusDeliveryOperation() { return null; }
  
  /*****************************************
  *
  *  constructor -- journey
  *
  *****************************************/

  public JourneyRequest(EvolutionEventContext context, SubscriberEvaluationRequest subscriberEvaluationRequest, String deliveryRequestSource, String journeyID)
  {
    super(context, "journeyFulfillment", deliveryRequestSource);
    this.journeyRequestID = context.getUniqueKey();
    this.eventDate = context.now();
    this.journeyID = journeyID;
    this.callingJourneyInstanceID = subscriberEvaluationRequest.getJourneyState().getJourneyInstanceID();
    this.callingJourneyNodeID = subscriberEvaluationRequest.getJourneyState().getJourneyNodeID();
    this.waitForCompletion = false;
    this.boundParameters = new SimpleParameterMap();
    this.journeyStatus = null;
    this.journeyResults = null;
    this.eligible = false;
  }
  
  /*****************************************
  *
  *  constructor -- journey (workflow)
  *
  *****************************************/

  public JourneyRequest(EvolutionEventContext context, SubscriberEvaluationRequest subscriberEvaluationRequest, String deliveryRequestSource, String workflowID, SimpleParameterMap boundParameters)
  {
    super(context, "journeyFulfillment", deliveryRequestSource);
    this.journeyRequestID = context.getUniqueKey();
    this.eventDate = context.now();
    this.journeyID = workflowID;
    this.callingJourneyInstanceID = subscriberEvaluationRequest.getJourneyState().getJourneyInstanceID();
    this.callingJourneyNodeID = subscriberEvaluationRequest.getJourneyState().getJourneyNodeID();
    this.waitForCompletion = true;
    this.boundParameters = boundParameters;
    this.journeyStatus = null;
    this.journeyResults = null;
    this.eligible = false;
  }

  /*****************************************
  *
  *  constructor -- enterCampaign
  *
  *****************************************/

  public JourneyRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, String uniqueKey, String subscriberID, String deliveryRequestSource, boolean universalControlGroup)
  {
    super(subscriberProfile,subscriberGroupEpochReader,uniqueKey, subscriberID, "journeyFulfillment", deliveryRequestSource, universalControlGroup);
    this.journeyRequestID = uniqueKey;
    this.eventDate = SystemTime.getCurrentTime();
    this.journeyID = deliveryRequestSource;
    this.callingJourneyInstanceID = null;
    this.callingJourneyNodeID = null;
    this.waitForCompletion = false;
    this.boundParameters = new SimpleParameterMap();
    this.journeyStatus = null;
    this.journeyResults = null;
    this.eligible = false;
  }

  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  public JourneyRequest(DeliveryRequest originatingRequest, JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
  {
    super(originatingRequest,jsonRoot);
    this.journeyRequestID = JSONUtilities.decodeString(jsonRoot, "journeyRequestID", true);
    this.eventDate = JSONUtilities.decodeDate(jsonRoot, "eventDate", true);
    this.journeyID = JSONUtilities.decodeString(jsonRoot, "journeyID", true);
    this.callingJourneyInstanceID = null;
    this.callingJourneyNodeID = null;
    this.waitForCompletion = false;
    this.boundParameters = new SimpleParameterMap();
    this.journeyStatus = null;
    this.journeyResults = null;
    this.eligible = false;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public JourneyRequest(SchemaAndValue schemaAndValue, String journeyRequestID, Date eventDate, String journeyID, String callingJourneyInstanceID, String callingJourneyNodeID, boolean waitForCompletion, SimpleParameterMap boundParameters, SubscriberJourneyStatus journeyStatus, SimpleParameterMap journeyResults)
  {
    super(schemaAndValue);
    this.journeyRequestID = journeyRequestID;
    this.eventDate = eventDate;
    this.journeyID = journeyID;
    this.callingJourneyInstanceID = callingJourneyInstanceID;
    this.callingJourneyNodeID = callingJourneyNodeID;
    this.waitForCompletion = waitForCompletion;
    this.boundParameters = boundParameters;
    this.journeyStatus = journeyStatus;
    this.journeyResults = journeyResults;
    this.eligible = false;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private JourneyRequest(JourneyRequest journeyRequest)
  {
    super(journeyRequest);
    this.journeyRequestID = journeyRequest.getJourneyRequestID();
    this.eventDate = journeyRequest.getEventDate();
    this.journeyID = journeyRequest.getJourneyID();
    this.callingJourneyInstanceID = journeyRequest.getCallingJourneyInstanceID();
    this.callingJourneyNodeID = journeyRequest.getCallingJourneyNodeID();
    this.waitForCompletion = journeyRequest.getWaitForCompletion();
    this.boundParameters = new SimpleParameterMap(journeyRequest.getBoundParameters());
    this.journeyStatus = journeyRequest.getJourneyStatus();
    this.journeyResults = (journeyResults != null) ? new SimpleParameterMap(journeyRequest.getJourneyResults()) : null;
    this.eligible = journeyRequest.getEligible();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public JourneyRequest copy()
  {
    return new JourneyRequest(this);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyRequest journeyRequest = (JourneyRequest) value;
    Struct struct = new Struct(schema);
    packCommon(struct, journeyRequest);
    struct.put("journeyRequestID", journeyRequest.getJourneyRequestID());
    struct.put("eventDate", journeyRequest.getEventDate());
    struct.put("journeyID", journeyRequest.getJourneyID());
    struct.put("callingJourneyInstanceID", journeyRequest.getCallingJourneyInstanceID());
    struct.put("callingJourneyNodeID", journeyRequest.getCallingJourneyNodeID());
    struct.put("waitForCompletion", journeyRequest.getWaitForCompletion());
    struct.put("boundParameters", SimpleParameterMap.serde().packOptional(journeyRequest.getBoundParameters()));
    struct.put("journeyStatus", (journeyRequest.getJourneyStatus() != null) ? journeyRequest.getJourneyStatus().getExternalRepresentation() : null);
    struct.put("journeyResults", SimpleParameterMap.serde().packOptional(journeyRequest.getJourneyResults()));
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

  public static JourneyRequest unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion2(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String journeyRequestID = valueStruct.getString("journeyRequestID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    String journeyID = valueStruct.getString("journeyID");
    String callingJourneyInstanceID = (schemaVersion >= 2) ? valueStruct.getString("callingJourneyInstanceID") : null;
    String callingJourneyNodeID = (schemaVersion >= 2) ? valueStruct.getString("callingJourneyNodeID") : null;
    boolean waitForCompletion = (schemaVersion >= 2) ? valueStruct.getBoolean("waitForCompletion") : false;
    SimpleParameterMap boundParameters = (schemaVersion >= 2) ? SimpleParameterMap.serde().unpackOptional(new SchemaAndValue(schema.field("boundParameters").schema(), valueStruct.get("boundParameters"))) : new SimpleParameterMap();
    SubscriberJourneyStatus journeyStatus = (schemaVersion >= 2) ? (valueStruct.get("journeyStatus") != null ? SubscriberJourneyStatus.fromExternalRepresentation(valueStruct.getString("journeyStatus")) : null) : null;
    SimpleParameterMap journeyResults = (schemaVersion >= 2) ? SimpleParameterMap.serde().unpackOptional(new SchemaAndValue(schema.field("journeyResults").schema(), valueStruct.get("journeyResults"))) : null;
    
    //
    //  return
    //

    return new JourneyRequest(schemaAndValue, journeyRequestID, eventDate, journeyID, callingJourneyInstanceID, callingJourneyNodeID, waitForCompletion, boundParameters, journeyStatus,journeyResults);
  }
  
  /****************************************
  *
  *  presentation utilities
  *
  ****************************************/
  
  @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService)
  {
    Module module = Module.fromExternalRepresentation(getModuleID());
    guiPresentationMap.put(CUSTOMERID, getSubscriberID());
    guiPresentationMap.put(DELIVERABLEID, getJourneyID());
    guiPresentationMap.put(DELIVERABLEQTY, 1);
    guiPresentationMap.put(OPERATION, CommodityDeliveryOperation.Credit.toString());
    guiPresentationMap.put(MODULEID, getModuleID());
    guiPresentationMap.put(MODULENAME, module.toString());
    guiPresentationMap.put(FEATUREID, getFeatureID());
    guiPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
    guiPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
    guiPresentationMap.put(ORIGIN, "");
  }
  
  @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService)
  {
    Module module = Module.fromExternalRepresentation(getModuleID());
    thirdPartyPresentationMap.put(DELIVERABLEID, getJourneyID());
    thirdPartyPresentationMap.put(DELIVERABLEQTY, 1);
    thirdPartyPresentationMap.put(OPERATION, CommodityDeliveryOperation.Credit.toString());
    thirdPartyPresentationMap.put(MODULEID, getModuleID());
    thirdPartyPresentationMap.put(MODULENAME, module.toString());
    thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
    thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
    thirdPartyPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
    thirdPartyPresentationMap.put(ORIGIN, "");
  }
  @Override
  public void resetDeliveryRequestAfterReSchedule()
  {
    // 
    // JourneyRequest never rescheduled, let return unchanged
    //  
    
  }
}
