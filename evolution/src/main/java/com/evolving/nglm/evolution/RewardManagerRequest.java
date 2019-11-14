/****************************************************************************
*
*  RewardManagerRequest.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.DeliveryRequest;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryManagerDeclaration;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.PaymentMeanService;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.SubscriberMessageTemplateService;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.ProductService;
import com.evolving.nglm.evolution.DeliverableService;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class RewardManagerRequest extends DeliveryRequest
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
    schemaBuilder.name("service_rewardmanager_request");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
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

  @Override public Integer getActivityType() { return ActivityType.BDR.getExternalRepresentation(); }

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

  public RewardManagerRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
  {
    super(jsonRoot);
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

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

  @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
  {
    Module module = Module.fromExternalRepresentation(getModuleID());
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
    guiPresentationMap.put(MODULENAME, module.toString());
    guiPresentationMap.put(FEATUREID, getFeatureID());
    guiPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
    guiPresentationMap.put(ORIGIN, "");
    guiPresentationMap.put(RETURNCODE, getReturnCode());
    guiPresentationMap.put(RETURNCODEDETAILS, getReturnStatus());
  }

  //
  //  addFieldsForThirdPartyPresentation
  //

  @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
  {
    Module module = Module.fromExternalRepresentation(getModuleID());
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
    thirdPartyPresentationMap.put(MODULENAME, module.toString());
    thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
    thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
    thirdPartyPresentationMap.put(ORIGIN, "");
    thirdPartyPresentationMap.put(RETURNCODE, getReturnCode());
    thirdPartyPresentationMap.put(RETURNCODEDETAILS, getReturnStatus());
  }
}
