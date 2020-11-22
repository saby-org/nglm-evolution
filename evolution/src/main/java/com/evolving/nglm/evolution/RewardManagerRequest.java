/****************************************************************************
*
*  RewardManagerRequest.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.*;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityType;
import com.evolving.nglm.evolution.DeliveryRequest.Module;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class RewardManagerRequest extends DeliveryRequest implements BonusDelivery
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/
  
  private static final Logger log = LoggerFactory.getLogger(RewardManagerRequest.class);

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("service_rewardmanager_request");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),8));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("msisdn", Schema.STRING_SCHEMA);
    schemaBuilder.field("providerID", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliverableID", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliverableName", Schema.STRING_SCHEMA);
    schemaBuilder.field("amount", Schema.FLOAT64_SCHEMA);
    schemaBuilder.field("periodQuantity", Schema.INT32_SCHEMA);
    schemaBuilder.field("periodType", Schema.STRING_SCHEMA);
    schemaBuilder.field("returnCode", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("returnCodeDetails", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("returnStatus", Schema.OPTIONAL_INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<RewardManagerRequest> serde = new ConnectSerde<RewardManagerRequest>(schema, false, RewardManagerRequest.class, RewardManagerRequest::pack, RewardManagerRequest::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<RewardManagerRequest> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String msisdn;
  private String providerID;
  private String deliverableID;
  private String deliverableName;
  private double amount;
  private int periodQuantity;
  private TimeUnit periodType;
  private Integer returnCode;
  private String returnCodeDetails;
  private Integer returnStatus;

  //
  //  accessors
  //

  public String getMSISDN() { return msisdn; }
  public String getProviderID() { return providerID; }
  public String getDeliverableID() { return deliverableID; }
  public String getDeliverableName() { return deliverableName; }
  public double getAmount() { return amount; }
  public int getPeriodQuantity() { return periodQuantity; }
  public TimeUnit getPeriodType() { return periodType; }
  public Integer getReturnCode() { return returnCode; }
  public String getReturnCodeDetails() { return returnCodeDetails; }
  public Integer getReturnStatus() { return returnStatus; }

  //
  //  setters
  //

  public void setReturnCode(Integer returnCode) { this.returnCode = returnCode; }
  public void setReturnCodeDetails(String returnCodeDetails) { this.returnCodeDetails = returnCodeDetails; }
  public void setReturnStatus(Integer returnStatus) { this.returnStatus = returnStatus; }

  //
  //  abstract
  //

  @Override public ActivityType getActivityType() { return ActivityType.BDR; }

  //
  //  bonus delivery accessors
  //

  public int getBonusDeliveryReturnCode() { return getReturnCode() != null? getReturnCode() : 0; }
  public String getBonusDeliveryReturnCodeDetails() { return getReturnCodeDetails(); }
  public String getBonusDeliveryOrigin() { return null; }
  public String getBonusDeliveryProviderId() { return getProviderID(); }
  public String getBonusDeliveryDeliverableId() { return getDeliverableID(); }
  public String getBonusDeliveryDeliverableName() { return getDeliverableName(); }
  public int getBonusDeliveryDeliverableQty() { return (int) getAmount(); }
  public String getBonusDeliveryOperation() { return null; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public RewardManagerRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource, String msisdn, String providerID, String deliverableID, String deliverableName, double amount, int periodQuantity, TimeUnit periodType)
  {
    super(context, deliveryType, deliveryRequestSource);
    this.msisdn = msisdn;
    this.providerID = providerID;
    this.deliverableID = deliverableID;
    this.deliverableName = deliverableName;
    this.amount = amount;
    this.periodQuantity = periodQuantity;
    this.periodType = periodType;
    this.returnCode = null;
    this.returnCodeDetails = null;
    this.returnStatus = null;
  }

  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  public RewardManagerRequest(DeliveryRequest originatingRequest, JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
  {
    super(originatingRequest,jsonRoot);
    this.msisdn = JSONUtilities.decodeString(jsonRoot, "msisdn", true);
    this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", true);
    this.deliverableID = JSONUtilities.decodeString(jsonRoot, "deliverableID", true);
    this.deliverableName = JSONUtilities.decodeString(jsonRoot, "deliverableName", true);
    this.amount = JSONUtilities.decodeDouble(jsonRoot, "amount", true);
    this.periodQuantity = JSONUtilities.decodeInteger(jsonRoot, "periodQuantity", true);
    this.periodType = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "periodType", true));
    this.returnCode = null;
    this.returnCodeDetails = null;
    this.returnStatus = null;
  }

  /*****************************************
   *
   *  constructor -- external
   *
   *****************************************/

  public RewardManagerRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader, JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
  {
    super(subscriberProfile,subscriberGroupEpochReader,jsonRoot);
    this.msisdn = JSONUtilities.decodeString(jsonRoot, "msisdn", true);
    this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", true);
    this.deliverableID = JSONUtilities.decodeString(jsonRoot, "deliverableID", true);
    this.deliverableName = JSONUtilities.decodeString(jsonRoot, "deliverableName", true);
    this.amount = JSONUtilities.decodeDouble(jsonRoot, "amount", true);
    this.periodQuantity = JSONUtilities.decodeInteger(jsonRoot, "periodQuantity", true);
    this.periodType = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "periodType", true));
    this.returnCode = null;
    this.returnCodeDetails = null;
    this.returnStatus = null;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private RewardManagerRequest(SchemaAndValue schemaAndValue, String msisdn, String providerID, String deliverableID, String deliverableName, double amount, int periodQuantity, TimeUnit periodType, Integer returnCode, String returnCodeDetails, Integer returnStatus)
  {
    super(schemaAndValue);
    this.msisdn = msisdn;
    this.providerID = providerID;
    this.deliverableID = deliverableID;
    this.deliverableName = deliverableName;
    this.amount = amount;
    this.periodQuantity = periodQuantity;
    this.periodType = periodType;
    this.returnCode = returnCode;
    this.returnCodeDetails = returnCodeDetails;
    this.returnStatus = returnStatus;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private RewardManagerRequest(RewardManagerRequest rewardManagerRequest)
  {
    super(rewardManagerRequest);
    this.msisdn = rewardManagerRequest.getMSISDN();
    this.providerID = rewardManagerRequest.getProviderID();
    this.deliverableID = rewardManagerRequest.getDeliverableID();
    this.deliverableName = rewardManagerRequest.getDeliverableName();
    this.amount = rewardManagerRequest.getAmount();
    this.periodQuantity = rewardManagerRequest.getPeriodQuantity();
    this.periodType = rewardManagerRequest.getPeriodType();
    this.returnCode = rewardManagerRequest.getReturnCode();
    this.returnCodeDetails = rewardManagerRequest.getReturnCodeDetails();
    this.returnStatus = rewardManagerRequest.getReturnStatus();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public RewardManagerRequest copy()
  {
    return new RewardManagerRequest(this);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    RewardManagerRequest rewardManagerRequest = (RewardManagerRequest) value;
    Struct struct = new Struct(schema);
    packCommon(struct, rewardManagerRequest);
    struct.put("msisdn", rewardManagerRequest.getMSISDN());
    struct.put("providerID", rewardManagerRequest.getProviderID());
    struct.put("deliverableID", rewardManagerRequest.getDeliverableID());
    struct.put("deliverableName", rewardManagerRequest.getDeliverableName());
    struct.put("amount", rewardManagerRequest.getAmount());
    struct.put("periodQuantity", rewardManagerRequest.getPeriodQuantity());
    struct.put("periodType", rewardManagerRequest.getPeriodType().getExternalRepresentation());
    struct.put("returnCode", rewardManagerRequest.getReturnCode());
    struct.put("returnCodeDetails", rewardManagerRequest.getReturnCodeDetails());
    struct.put("returnStatus", rewardManagerRequest.getReturnStatus());
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

  public static RewardManagerRequest unpack(SchemaAndValue schemaAndValue)
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
    String msisdn = valueStruct.getString("msisdn");
    String providerID = valueStruct.getString("providerID");
    String deliverableID = valueStruct.getString("deliverableID");
    String deliverableName = valueStruct.getString("deliverableName");
    double amount = valueStruct.getFloat64("amount");
    int periodQuantity = valueStruct.getInt32("periodQuantity");
    TimeUnit periodType = TimeUnit.fromExternalRepresentation(valueStruct.getString("periodType"));
    Integer returnCode = valueStruct.getInt32("returnCode");
    String returnCodeDetails = valueStruct.getString("returnCodeDetails");
    Integer returnStatus = valueStruct.getInt32("returnStatus");

    //
    //  return
    //

    return new RewardManagerRequest(schemaAndValue, msisdn, providerID, deliverableID, deliverableName, amount, periodQuantity, periodType, returnCode, returnCodeDetails, returnStatus);
  }

  /****************************************
  *
  *  presentation utilities
  *
  ****************************************/

  //
  //  addFieldsForGUIPresentation
  //

  @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService)
  {
    guiPresentationMap.put(CUSTOMERID, getSubscriberID());
    guiPresentationMap.put(PROVIDERID, getProviderID());
    guiPresentationMap.put(PROVIDERNAME, Deployment.getFulfillmentProviders().get(getProviderID()).getProviderName());
    guiPresentationMap.put(DELIVERABLEID, getDeliverableID());
    guiPresentationMap.put(DELIVERABLENAME, deliverableService.getActiveDeliverable(getDeliverableID(), SystemTime.getCurrentTime()).getDeliverableName());
    guiPresentationMap.put(DELIVERABLEQTY, getAmount());
    guiPresentationMap.put(OPERATION, "credit");
    guiPresentationMap.put(VALIDITYPERIODTYPE, getPeriodType().getExternalRepresentation());
    guiPresentationMap.put(VALIDITYPERIODQUANTITY, getPeriodQuantity());
    guiPresentationMap.put(MODULEID, getModuleID());
    guiPresentationMap.put(MODULENAME, getModule().toString());
    guiPresentationMap.put(FEATUREID, getFeatureID());
    guiPresentationMap.put(FEATURENAME, getFeatureName(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
    guiPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
    guiPresentationMap.put(ORIGIN, "");
    guiPresentationMap.put(RETURNCODE, getReturnCode());
    guiPresentationMap.put(RETURNCODEDETAILS, getReturnStatus());
  }

  //
  //  addFieldsForThirdPartyPresentation
  //

  @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService)
  {
    thirdPartyPresentationMap.put(CUSTOMERID, getSubscriberID());
    thirdPartyPresentationMap.put(PROVIDERID, getProviderID());
    thirdPartyPresentationMap.put(PROVIDERNAME, Deployment.getFulfillmentProviders().get(getProviderID()).getProviderName());
    thirdPartyPresentationMap.put(DELIVERABLEID, getDeliverableID());
    thirdPartyPresentationMap.put(DELIVERABLENAME, deliverableService.getActiveDeliverable(getDeliverableID(), SystemTime.getCurrentTime()).getDeliverableName());
    thirdPartyPresentationMap.put(DELIVERABLEQTY, getAmount());
    thirdPartyPresentationMap.put(OPERATION, "credit");
    thirdPartyPresentationMap.put(VALIDITYPERIODTYPE, getPeriodType().getExternalRepresentation());
    thirdPartyPresentationMap.put(VALIDITYPERIODQUANTITY, getPeriodQuantity());
    thirdPartyPresentationMap.put(MODULEID, getModuleID());
    thirdPartyPresentationMap.put(MODULENAME, getModule().toString());
    thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
    thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
    thirdPartyPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
    thirdPartyPresentationMap.put(ORIGIN, "");
    thirdPartyPresentationMap.put(RETURNCODE, getReturnCode());
    thirdPartyPresentationMap.put(RETURNCODEDESCRIPTION, RESTAPIGenericReturnCodes.fromGenericResponseCode(getReturnCode()).getGenericResponseMessage());
    thirdPartyPresentationMap.put(RETURNCODEDETAILS, getReturnCodeDetails());
  }
  @Override
  public void resetDeliveryRequestAfterReSchedule()
  {
    // 
    // RewardManagerRequest never rescheduled, let return unchanged
    //      
  }
  
  
  /*****************************************
  *
  *  class ActionManager
  *
  *****************************************/

  public static class RewardActionManager extends com.evolving.nglm.evolution.ActionManager
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String moduleID;
    private String deliveryType;
    private String providerID;
    private CommodityDeliveryOperation operation;

    
    /*****************************************
    * 
    *  constructor
    *
    *****************************************/

    public RewardActionManager(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
      this.moduleID = JSONUtilities.decodeString(configuration, "moduleID", true);
      this.deliveryType = JSONUtilities.decodeString(configuration, "deliveryType", true);
      this.operation = CommodityDeliveryOperation.fromExternalRepresentation(JSONUtilities.decodeString(configuration, "operation", true));
      this.providerID = Deployment.getDeliveryManagers().get(this.deliveryType).getProviderID();
    }

    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      /*****************************************
      *
      *  parameters
      *
      *****************************************/

      String commodityID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.commodityid");

      // Set amount default to 1 for Activate / Deactivate
      int amount = 1;
      if ( operation != CommodityDeliveryOperation.Activate && operation != CommodityDeliveryOperation.Deactivate ){
          amount = ((Number) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.amount")).intValue();
      }
      TimeUnit validityPeriodType = (CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.validity.period.type") != null) ? TimeUnit.fromExternalRepresentation((String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.validity.period.type")) : null;
      Integer validityPeriodQuantity = (Integer) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.validity.period.quantity");
      
      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String journeyID = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      Journey journey = evolutionEventContext.getJourneyService().getActiveJourney(journeyID, evolutionEventContext.now());
      String newModuleID = moduleID;
      if (journey != null && journey.getGUIManagedObjectType() == GUIManagedObjectType.LoyaltyWorkflow)
        {
          newModuleID = Module.Loyalty_Program.getExternalRepresentation();
        }

      // retrieve the featureID that is the origin of this delivery request:
      // - If the Journey related to JourneyState is not a Workflow, then featureID = JourneyState.getID
      // - if the Journey related to JourneyState is a Workflown then we must extract the original featureID from the origial delivery Request that created the workflow instance
      String deliveryRequestSource = extractWorkflowFeatureID(evolutionEventContext, subscriberEvaluationRequest, journeyID);

      /*****************************************
      *
      *  Commodity to Provider and Deliverable
      *
      *****************************************/
      String deliveryType = null;
      PaymentMean paymentMean = null;
      Deliverable deliverable = null;
      switch (operation)
        {
        case Debit:
          //
          // Debit => check in paymentMean list
          //
          
          paymentMean = evolutionEventContext.getPaymentMeanService().getActivePaymentMean(commodityID, SystemTime.getCurrentTime());
          if(paymentMean == null){
            log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : paymentMean not found ");
            return new ArrayList<ActionManager.Action>();
          }else{
            DeliveryManagerDeclaration provider = Deployment.getFulfillmentProviders().get(paymentMean.getFulfillmentProviderID());
            if(provider == null){
              log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : paymentMean not found ");
              return new ArrayList<ActionManager.Action>();
            }else{
              deliveryType = provider.getDeliveryType();
            }
          }     

          break;
        case Credit:
        case Set:
        case Expire:
        case Activate:
        case Deactivate:
                  
          //
          // Other than Debit => check in paymentMean list
          //
          
          deliverable = evolutionEventContext.getDeliverableService().getActiveDeliverable(commodityID, SystemTime.getCurrentTime());
          if(deliverable == null){
            log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : deliverable not found ");
            return new ArrayList<ActionManager.Action>();
          }else{
            DeliveryManagerDeclaration provider = Deployment.getFulfillmentProviders().get(deliverable.getFulfillmentProviderID());
            if(provider == null){
              log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : deliverable not found ");
              return new ArrayList<ActionManager.Action>();
            }else{
              deliveryType = provider.getDeliveryType();
            }
          }     

          break;

        default:
          log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : deliverable not found ");
          break;
        }

      /*****************************************
      *
      *  request
      *
      *****************************************/
      //
      //  request (JSON)
      //

      HashMap<String,Object> rewardRequestData = new HashMap<String,Object>();
      rewardRequestData.put("deliveryRequestID", evolutionEventContext.getUniqueKey());
      rewardRequestData.put("originatingRequest", true);
      rewardRequestData.put("subscriberID", evolutionEventContext.getSubscriberState().getSubscriberID());
      rewardRequestData.put("eventID", rewardRequestData.get("deliveryRequestID")); // sure of that ??
      rewardRequestData.put("moduleID", newModuleID);
      rewardRequestData.put("featureID", deliveryRequestSource);
      rewardRequestData.put("deliveryType", deliveryType);
      rewardRequestData.put("diplomaticBriefcase", new HashMap<String, String>());
      rewardRequestData.put("msisdn", evolutionEventContext.getSubscriberState().getSubscriberID());
      rewardRequestData.put("providerID", providerID);
      rewardRequestData.put("deliverableID", deliverable.getDeliverableID());
      rewardRequestData.put("deliverableName", deliverable.getDeliverableName());
      rewardRequestData.put("operation", operation.getExternalRepresentation());
      rewardRequestData.put("amount", amount);
      rewardRequestData.put("periodQuantity", (validityPeriodQuantity == null ? 1 : validityPeriodQuantity)); //mandatory in RewardManagerRequest => set default value if nul
      rewardRequestData.put("periodType", (validityPeriodType == null ? TimeUnit.Day.getExternalRepresentation() : validityPeriodType.getExternalRepresentation())); //mandatory in RewardManagerRequest => set default value if nul

      //
      //  send
      //
      
      if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : generating "+CommodityType.REWARD+" request DONE");

      RewardManagerRequest rewardRequest = new RewardManagerRequest(evolutionEventContext.getSubscriberState().getSubscriberProfile(),evolutionEventContext.getSubscriberGroupEpochReader(),JSONUtilities.encodeObject(rewardRequestData), Deployment.getDeliveryManagers().get(deliveryType));
      rewardRequest.setModuleID(newModuleID);
      rewardRequest.setFeatureID(deliveryRequestSource);

      // XL: manual hack, the only purpose of this is campaign request not going into same topic as purchase request
      // purchase going into "standard" aka, first topic in : requestTopics of delivery manager declaration, this one going in the second one of the list
      rewardRequest.setDeliveryPriority(DeliveryPriority.High);

      /*****************************************
      *
      *  return
      *
      *****************************************/

      return Collections.<Action>singletonList(rewardRequest);
    }
  }

}
