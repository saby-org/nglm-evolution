package com.evolving.nglm.evolution;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.evolving.nglm.core.*;
import com.evolving.nglm.evolution.commoditydelivery.CommodityDeliveryException;
import com.evolving.nglm.evolution.commoditydelivery.CommodityDeliveryManagerRemovalUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.EmptyFulfillmentManager.EmptyFulfillmentRequest;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;


public class CommodityDeliveryManager
{

  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  CommodityOperation
  //
  final static String POINT_PREFIX="point-";
  final static String JOURNEY_PREFIX="journey-";
  public enum CommodityDeliveryOperation
  {
    Credit("credit"),
    Debit("debit"),
    Set("set"),
    Expire("expire"),
    Activate("activate"),
    Deactivate("deactivate"),
    Check("check"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private CommodityDeliveryOperation(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CommodityDeliveryOperation fromExternalRepresentation(String externalRepresentation) { for (CommodityDeliveryOperation enumeratedValue : CommodityDeliveryOperation.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  CommoditySelection
  //

  public enum CommodityType {
    JOURNEY(JourneyRequest.class.getName()),
    IN(INFulfillmentRequest.class.getName()),
    POINT(PointFulfillmentRequest.class.getName()),
    EMPTY(EmptyFulfillmentRequest.class.getName()),
    REWARD(RewardManagerRequest.class.getName());

    private String externalRepresentation;
    private CommodityType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CommodityType fromExternalRepresentation(String externalRepresentation) {
      for (CommodityType enumeratedValue : CommodityType.values()) 
        {
          if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) 
            {
              return enumeratedValue;
            }
        }
      return null;
    }
  }

  public enum CommodityDeliveryStatus
  {
    SUCCESS(0),
    MISSING_PARAMETERS(4),
    BAD_FIELD_VALUE(5),
    PENDING(708),
    CUSTOMER_NOT_FOUND(20),
    SYSTEM_ERROR(21),
    TIMEOUT(22),
    THIRD_PARTY_ERROR(24),
    BONUS_NOT_FOUND(100),
    INSUFFICIENT_BALANCE(405),
    CHECK_BALANCE_LT(300),
    CHECK_BALANCE_GT(301),
    CHECK_BALANCE_ET(302),
    UNKNOWN(-1);
    private Integer externalRepresentation;
    private CommodityDeliveryStatus(Integer externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public Integer getReturnCode() { return externalRepresentation; }
    public static CommodityDeliveryStatus fromReturnCode(Integer externalRepresentation) { for (CommodityDeliveryStatus enumeratedValue : CommodityDeliveryStatus.values()) { if (enumeratedValue.getReturnCode().equals(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
  }

  public DeliveryManager.DeliveryStatus getDeliveryStatus (CommodityDeliveryStatus status)
  {
    switch(status)
      {
      case SUCCESS:
        return DeliveryManager.DeliveryStatus.Delivered;
      case PENDING:
        return DeliveryManager.DeliveryStatus.Pending;
      case CUSTOMER_NOT_FOUND:
        return DeliveryManager.DeliveryStatus.Failed;
      case BONUS_NOT_FOUND:
        return DeliveryManager.DeliveryStatus.BonusNotFound;
      case INSUFFICIENT_BALANCE:
        return DeliveryManager.DeliveryStatus.InsufficientBalance;
      case CHECK_BALANCE_LT:
        return DeliveryManager.DeliveryStatus.CheckBalanceLowerThan;
      case CHECK_BALANCE_GT:
        return DeliveryManager.DeliveryStatus.CheckBalanceGreaterThan;
      case CHECK_BALANCE_ET:
        return DeliveryManager.DeliveryStatus.CheckBalanceEqualsTo;
      default:
        return DeliveryManager.DeliveryStatus.Failed;
      }
  }

  private static final Logger log = LoggerFactory.getLogger(CommodityDeliveryManager.class);

  public static final String COMMODITY_DELIVERY_TYPE = "commodityDelivery";
  public static final String COMMODITY_DELIVERY_BRIEFCASE = "commodity_delivery_briefcase";
  public static final String APPLICATION_ID = "application_id";
  public static final String APPLICATION_BRIEFCASE = "application_briefcase";

  /*****************************************
  *
  *  class CommodityDeliveryRequest
  *
  *****************************************/

  public static class CommodityDeliveryRequest extends DeliveryRequest
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
      schemaBuilder.name("service_commodityDelivery_request");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),10));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("providerID", Schema.STRING_SCHEMA);
      schemaBuilder.field("commodityID", Schema.STRING_SCHEMA);
      schemaBuilder.field("commodityName", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("operation", Schema.STRING_SCHEMA);
      schemaBuilder.field("amount", Schema.INT32_SCHEMA);
      schemaBuilder.field("validityPeriodType", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("validityPeriodQuantity", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("deliverableExpirationDate", Timestamp.builder().optional().schema());
      schemaBuilder.field("commodityDeliveryStatusCode", Schema.INT32_SCHEMA);
      schemaBuilder.field("statusMessage", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("origin", Schema.OPTIONAL_STRING_SCHEMA);
      schema = schemaBuilder.build();
    }

    //
    //  serde
    //

    private static ConnectSerde<CommodityDeliveryRequest> serde = new ConnectSerde<CommodityDeliveryRequest>(schema, false, CommodityDeliveryRequest.class, CommodityDeliveryRequest::pack, CommodityDeliveryRequest::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<CommodityDeliveryRequest> serde() { return serde; }
    public Schema subscriberStreamEventSchema() { return schema(); }

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String providerID;
    private String commodityID;
    private String commodityName;
    private CommodityDeliveryOperation operation;
    private int amount;
    private TimeUnit validityPeriodType;
    private Integer validityPeriodQuantity;
    private Date deliverableExpirationDate;
    private CommodityDeliveryStatus commodityDeliveryStatus;
    private String statusMessage;
    private String origin;
    
    //
    //  accessors
    //

    public String getProviderID() { return providerID; }
    public String getCommodityID() { return commodityID; }
    public String getCommodityName() { return commodityName; }
    public CommodityDeliveryOperation getOperation() { return operation; }
    public int getAmount() { return amount; }
    public TimeUnit getValidityPeriodType() { return validityPeriodType; }
    public Integer getValidityPeriodQuantity() { return validityPeriodQuantity; }
    public Date getDeliverableExpirationDate() {
      // so far only internal point returns this
      if(deliverableExpirationDate!=null) return deliverableExpirationDate;
      // but if empty we will compute it (so might be not a real one), for all the others cases, based on delivery date
      if(getDeliveryDate()!= null && validityPeriodType!=null && validityPeriodType!=TimeUnit.Unknown && validityPeriodQuantity!=null){
        return EvolutionUtilities.addTime(getDeliveryDate(), validityPeriodQuantity, validityPeriodType, Deployment.getDeployment(this.getTenantID()).getBaseTimeZone(), EvolutionUtilities.RoundingSelection.NoRound);
      }
      // should be null here
      return deliverableExpirationDate;
    }
    public CommodityDeliveryStatus getCommodityDeliveryStatus() { return commodityDeliveryStatus; }
    public String getStatusMessage() { return statusMessage; }
    public String getOrigin() { return origin; }

    //
    //  setters
    //

    public void setCommodityDeliveryStatus(CommodityDeliveryStatus status) { this.commodityDeliveryStatus = status; }
    public void setStatusMessage(String statusMessage) { this.statusMessage = statusMessage; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public CommodityDeliveryRequest(EvolutionEventContext context, String deliveryRequestSource, Map<String, String> diplomaticBriefcase, String providerID, String commodityID, CommodityDeliveryOperation operation, int amount, TimeUnit validityPeriodType, Integer validityPeriodQuantity, Date deliverableExpirationDate, String origin, int tenantID)
    {
      super(context, COMMODITY_DELIVERY_TYPE, deliveryRequestSource, tenantID);
      setDiplomaticBriefcase(diplomaticBriefcase);
      this.providerID = providerID;
      this.commodityID = commodityID;
      this.operation = operation;
      this.amount = amount;
      this.validityPeriodType = validityPeriodType;
      this.validityPeriodQuantity = validityPeriodQuantity;
      this.deliverableExpirationDate = deliverableExpirationDate;
      this.commodityDeliveryStatus = CommodityDeliveryStatus.PENDING;
      this.statusMessage = "";
      this.origin = origin;
    }

    /*****************************************
    *
    *  constructor -- external
    *
    *****************************************/

    public CommodityDeliveryRequest(DeliveryRequest originatingRequet,JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager, int tenantID)
    {
      super(originatingRequet,jsonRoot, tenantID);
      this.setCorrelator(JSONUtilities.decodeString(jsonRoot, "correlator", false));
      this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", true);
      this.commodityID = JSONUtilities.decodeString(jsonRoot, "commodityID", true);
      this.commodityName = JSONUtilities.decodeString(jsonRoot, "commodityName", false);
      this.operation = CommodityDeliveryOperation.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "operation", true));
      this.amount = JSONUtilities.decodeInteger(jsonRoot, "amount", true);
      this.validityPeriodType = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "validityPeriodType", false));
      this.validityPeriodQuantity = JSONUtilities.decodeInteger(jsonRoot, "validityPeriodQuantity", false);
      this.deliverableExpirationDate = JSONUtilities.decodeDate(jsonRoot, "deliverableExpirationDate", false);
      this.commodityDeliveryStatus = CommodityDeliveryStatus.fromReturnCode(JSONUtilities.decodeInteger(jsonRoot, "commodityDeliveryStatusCode", true));
      this.statusMessage = JSONUtilities.decodeString(jsonRoot, "statusMessage", false);
      this.origin = JSONUtilities.decodeString(jsonRoot, "origin", false);
    }

    /*****************************************
     *
     *  constructor -- external
     *
     *****************************************/

    public CommodityDeliveryRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader,JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager, int tenantID)
    {
      super(subscriberProfile,subscriberGroupEpochReader,jsonRoot, tenantID);
      this.setCorrelator(JSONUtilities.decodeString(jsonRoot, "correlator", false));
      this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", true);
      this.commodityID = JSONUtilities.decodeString(jsonRoot, "commodityID", true);
      this.commodityName = JSONUtilities.decodeString(jsonRoot, "commodityName", false);
      this.operation = CommodityDeliveryOperation.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "operation", true));
      this.amount = JSONUtilities.decodeInteger(jsonRoot, "amount", true);
      this.validityPeriodType = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "validityPeriodType", false));
      this.validityPeriodQuantity = JSONUtilities.decodeInteger(jsonRoot, "validityPeriodQuantity", false);
      this.deliverableExpirationDate = JSONUtilities.decodeDate(jsonRoot, "deliverableExpirationDate", false);
      this.commodityDeliveryStatus = CommodityDeliveryStatus.fromReturnCode(JSONUtilities.decodeInteger(jsonRoot, "commodityDeliveryStatusCode", true));
      this.statusMessage = JSONUtilities.decodeString(jsonRoot, "statusMessage", false);
      this.origin = JSONUtilities.decodeString(jsonRoot, "origin", false);
    }

    /*****************************************
    *  
    *  to JSONObject
    *
    *****************************************/

    public JSONObject getJSONRepresentation(int tenantID){
      Map<String, Object> data = new HashMap<String, Object>();
      
      data.put("deliveryRequestID", this.getDeliveryRequestID());
      data.put("deliveryRequestSource", this.getDeliveryRequestSource());
      data.put("originatingRequest", this.getOriginatingRequest());
      data.put("deliveryType", this.getDeliveryType());

      data.put("correlator", this.getCorrelator());

      data.put("control", this.getControl());
      data.put("diplomaticBriefcase", this.getDiplomaticBriefcase());
      
      data.put("correlator", this.getCorrelator());
      
      data.put("eventID", this.getEventID());
      data.put("moduleID", this.getModuleID());
      data.put("featureID", this.getFeatureID());

      data.put("subscriberID", this.getSubscriberID());
      data.put("providerID", this.getProviderID());
      data.put("commodityID", this.getCommodityID());
      data.put("commodityName", this.getCommodityName());
      data.put("operation", this.getOperation().getExternalRepresentation());
      data.put("amount", this.getAmount());
      data.put("validityPeriodType", (this.getValidityPeriodType() != null ? this.getValidityPeriodType().getExternalRepresentation() : null));
      data.put("validityPeriodQuantity", this.getValidityPeriodQuantity());
      
      data.put("deliverableExpirationDate", this.getDeliverableExpirationDate());
      
      data.put("commodityDeliveryStatusCode", this.getCommodityDeliveryStatus().getReturnCode());
      data.put("statusMessage", this.getStatusMessage());
      data.put("origin", this.getOrigin());

      return JSONUtilities.encodeObject(data);
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private CommodityDeliveryRequest(SchemaAndValue schemaAndValue, String providerID, String commodityID, String commodityName, CommodityDeliveryOperation operation, int amount, TimeUnit validityPeriodType, Integer validityPeriodQuantity, Date deliverableExpirationDate, CommodityDeliveryStatus status, String statusMessage, String origin)
    {
      super(schemaAndValue);
      this.providerID = providerID;
      this.commodityID = commodityID;
      this.commodityName = commodityName;
      this.operation = operation;
      this.amount = amount;
      this.validityPeriodType = validityPeriodType;
      this.validityPeriodQuantity = validityPeriodQuantity;
      this.deliverableExpirationDate = deliverableExpirationDate;
      this.commodityDeliveryStatus = status;
      this.statusMessage = statusMessage;
      this.origin = origin;
    }

    /*****************************************
    *
    *  constructor -- copy
    *
    *****************************************/

    private CommodityDeliveryRequest(CommodityDeliveryRequest commodityDeliveryRequest)
    {
      super(commodityDeliveryRequest);
      this.providerID = commodityDeliveryRequest.getProviderID();
      this.commodityID = commodityDeliveryRequest.getCommodityID();
      this.operation = commodityDeliveryRequest.getOperation();
      this.amount = commodityDeliveryRequest.getAmount();
      this.validityPeriodType = commodityDeliveryRequest.getValidityPeriodType();
      this.validityPeriodQuantity = commodityDeliveryRequest.getValidityPeriodQuantity();
      this.deliverableExpirationDate = commodityDeliveryRequest.getDeliverableExpirationDate();
      this.commodityDeliveryStatus = commodityDeliveryRequest.getCommodityDeliveryStatus();
      this.statusMessage = commodityDeliveryRequest.getStatusMessage();
      this.origin = commodityDeliveryRequest.getOrigin();
    }

    /*****************************************
    *
    *  constructor -- esFields
    *
    *****************************************/
    
    public CommodityDeliveryRequest(Map<String, Object> esFields)
    {
      //
      //  super
      //
      
      super(esFields);
      setCreationDate(getDateFromESString(esDateFormat, (String) esFields.get("creationDate")));
      setDeliveryDate(getDateFromESString(esDateFormat, (String) esFields.get("eventDatetime")));
      
      //
      //  this
      //
      
      this.providerID = (String) esFields.get("providerID");
      this.commodityID = (String) esFields.get("deliverableID");
      String esOperation = (String) esFields.get("operation");
      this.operation = CommodityDeliveryOperation.fromExternalRepresentation(esOperation != null ? esOperation.toLowerCase() : esOperation);
      this.amount = (int) esFields.get("deliverableQty");
      this.validityPeriodType = TimeUnit.Year;
      this.validityPeriodQuantity = 1;
      this.deliverableExpirationDate = getDateFromESString(esDefaultDateFormat, (String) esFields.get("deliverableExpirationDate"));
      CommodityDeliveryStatus commodityDeliveryStatus = CommodityDeliveryStatus.fromReturnCode((Integer) esFields.get("returnCode"));
      this.commodityDeliveryStatus = commodityDeliveryStatus;
      this.statusMessage = (String) esFields.get("returnCodeDetails");
    }
    /*****************************************
    *
    *  copy
    *
    *****************************************/

    public CommodityDeliveryRequest copy()
    {
      return new CommodityDeliveryRequest(this);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      CommodityDeliveryRequest commodityDeliveryRequest = (CommodityDeliveryRequest) value;
      Struct struct = new Struct(schema);
      packCommon(struct, commodityDeliveryRequest);
      struct.put("providerID", commodityDeliveryRequest.getProviderID());
      struct.put("commodityID", commodityDeliveryRequest.getCommodityID());
      struct.put("commodityName", commodityDeliveryRequest.getCommodityName());
      struct.put("operation", commodityDeliveryRequest.getOperation().getExternalRepresentation());
      struct.put("amount", commodityDeliveryRequest.getAmount());
      struct.put("validityPeriodType", (commodityDeliveryRequest.getValidityPeriodType() != null ? commodityDeliveryRequest.getValidityPeriodType().getExternalRepresentation() : null));
      struct.put("validityPeriodQuantity", commodityDeliveryRequest.getValidityPeriodQuantity());
      struct.put("deliverableExpirationDate", commodityDeliveryRequest.getDeliverableExpirationDate());
      struct.put("commodityDeliveryStatusCode", commodityDeliveryRequest.getCommodityDeliveryStatus().getReturnCode());
      struct.put("statusMessage", commodityDeliveryRequest.getStatusMessage());
      struct.put("origin", commodityDeliveryRequest.getOrigin());
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

    public static CommodityDeliveryRequest unpack(SchemaAndValue schemaAndValue)
    {
      //
      //  data
      //

      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion2(schema.version()) : null;

      //  unpack
      //

      Struct valueStruct = (Struct) value;
      String providerID = valueStruct.getString("providerID");
      String commodityID = valueStruct.getString("commodityID");
      String commodityName = (schemaVersion >= 2) ? valueStruct.getString("commodityName") : "";
      CommodityDeliveryOperation operation = CommodityDeliveryOperation.fromExternalRepresentation(valueStruct.getString("operation"));
      int amount = valueStruct.getInt32("amount");
      TimeUnit validityPeriodType = TimeUnit.fromExternalRepresentation(valueStruct.getString("validityPeriodType"));
      Integer validityPeriodQuantity = valueStruct.getInt32("validityPeriodQuantity");
      Date deliverableExpirationDate = (Date) valueStruct.get("deliverableExpirationDate");
      int commodityDeliveryStatusCode = valueStruct.getInt32("commodityDeliveryStatusCode");
      CommodityDeliveryStatus status = CommodityDeliveryStatus.fromReturnCode(commodityDeliveryStatusCode);
      String statusMessage = valueStruct.getString("statusMessage");
      String origin = (schema.field("origin") != null) ? valueStruct.getString("origin") : null;
      

      //
      //  return
      //

      return new CommodityDeliveryRequest(schemaAndValue, providerID, commodityID, commodityName, operation, amount, validityPeriodType, validityPeriodQuantity, deliverableExpirationDate, status, statusMessage, origin);
    }

    /*****************************************
    *  
    *  toString
    *
    *****************************************/

    public String toString()
    {
      StringBuilder b = new StringBuilder();
      b.append("CommodityDeliveryRequest:{");
      b.append(super.toStringFields());
      b.append("," + getSubscriberID());
      b.append("," + providerID);
      b.append("," + commodityID);
      b.append("," + commodityName);
      b.append("," + operation);
      b.append("," + amount);
      b.append("," + validityPeriodType);
      b.append("," + validityPeriodQuantity);
      b.append("," + deliverableExpirationDate);
      b.append("," + commodityDeliveryStatus);
      b.append("," + statusMessage);
      b.append("," + origin);
      b.append("}");
      return b.toString();
    }
    
    @Override public ActivityType getActivityType() { return ActivityType.BDR; }
    
    /****************************************
    *
    *  presentation utilities
    *
    ****************************************/
    
    @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService, int tenantID)
    {
      Date now = SystemTime.getCurrentTime();
      guiPresentationMap.put(CUSTOMERID, getSubscriberID());
      guiPresentationMap.put(PROVIDERID, getProviderID());
      guiPresentationMap.put(PROVIDERNAME, Deployment.getFulfillmentProviders().get(getProviderID()).getProviderName());
      guiPresentationMap.put(DELIVERABLEID, getCommodityID());
      guiPresentationMap.put(DELIVERABLENAME, (deliverableService.getActiveDeliverable(getCommodityID(), now) != null ? deliverableService.getActiveDeliverable(getCommodityID(), now).getDeliverableName() : getCommodityID()));
      guiPresentationMap.put(DELIVERABLEDISPLAY, (deliverableService.getActiveDeliverable(getCommodityID(), now) != null ? deliverableService.getActiveDeliverable(getCommodityID(), now).getGUIManagedObjectDisplay() : getCommodityID()));
      guiPresentationMap.put(DELIVERABLEQTY, getAmount());
      guiPresentationMap.put(OPERATION, getOperation().getExternalRepresentation());
      guiPresentationMap.put(VALIDITYPERIODTYPE, getValidityPeriodType().getExternalRepresentation());
      guiPresentationMap.put(VALIDITYPERIODQUANTITY, getValidityPeriodQuantity());
      guiPresentationMap.put(DELIVERABLEEXPIRATIONDATE, getDateString(getDeliverableExpirationDate()));
      guiPresentationMap.put(MODULEID, getModuleID());
      guiPresentationMap.put(MODULENAME, getModule().toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(FEATURENAME, getFeatureName(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
      guiPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
      guiPresentationMap.put(ORIGIN, getOrigin());
      guiPresentationMap.put(RETURNCODE, getCommodityDeliveryStatus().getReturnCode());
      guiPresentationMap.put(RETURNCODEDETAILS, getCommodityDeliveryStatus().toString());
    }
    
    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService, int tenantID)
    {
      Date now = SystemTime.getCurrentTime();
      thirdPartyPresentationMap.put(PROVIDERID, getProviderID());
      thirdPartyPresentationMap.put(PROVIDERNAME, Deployment.getFulfillmentProviders().get(getProviderID()).getProviderName());
      thirdPartyPresentationMap.put(DELIVERABLEID, getCommodityID());
      thirdPartyPresentationMap.put(DELIVERABLENAME, (deliverableService.getActiveDeliverable(getCommodityID(), now) != null ? deliverableService.getActiveDeliverable(getCommodityID(), now).getDeliverableName() : getCommodityID()));
      thirdPartyPresentationMap.put(DELIVERABLEDISPLAY, (deliverableService.getActiveDeliverable(getCommodityID(), now) != null ? deliverableService.getActiveDeliverable(getCommodityID(), now).getGUIManagedObjectDisplay() : getCommodityID()));
      thirdPartyPresentationMap.put(DELIVERABLEQTY, getAmount());
      thirdPartyPresentationMap.put(OPERATION, getOperation().getExternalRepresentation());
      thirdPartyPresentationMap.put(DELIVERABLEEXPIRATIONDATE, getDateString(getDeliverableExpirationDate()));
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, getModule().toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(ORIGIN, getOrigin());
      thirdPartyPresentationMap.put(RETURNCODE, getCommodityDeliveryStatus().getReturnCode());
      thirdPartyPresentationMap.put(RETURNCODEDESCRIPTION, RESTAPIGenericReturnCodes.fromGenericResponseCode(getCommodityDeliveryStatus().getReturnCode()).getGenericResponseMessage());
      thirdPartyPresentationMap.put(RETURNCODEDETAILS, getCommodityDeliveryStatus().toString());
    }

    @Override
    public void resetDeliveryRequestAfterReSchedule()
    {
      // 
      // CommodityDeliveryRequest never rescheduled, let return unchanged
      //            
    }
  }

  /*****************************************
  *
  *  class ActionManager
  *
  *****************************************/

  public static class ActionManager extends com.evolving.nglm.evolution.ActionManager
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

    public ActionManager(JSONObject configuration) throws GUIManagerException
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
      String origin = null;
      if (subscriberEvaluationRequest.getJourneyNode() != null)
        {
          origin = subscriberEvaluationRequest.getJourneyNode().getNodeName();
        }
      Journey journey = evolutionEventContext.getJourneyService().getActiveJourney(journeyID, evolutionEventContext.now());
      String newModuleID = moduleID;
      if (journey != null && journey.getGUIManagedObjectType() == GUIManagedObjectType.LoyaltyWorkflow)
        {
          newModuleID = Module.Loyalty_Program.getExternalRepresentation();
          if (subscriberEvaluationRequest.getJourneyState() != null && subscriberEvaluationRequest.getJourneyState().getsourceOrigin() != null)
            {
              origin = subscriberEvaluationRequest.getJourneyState().getsourceOrigin();
            }
        }
      
      // retrieve the featureID that is the origin of this delivery request:
      // - If the Journey related to JourneyState is not a Workflow, then featureID = JourneyState.getID
      // - if the Journey related to JourneyState is a Workflown then we must extract the original featureID from the original delivery Request that created the workflow instance
      String deliveryRequestSource = extractWorkflowFeatureID(evolutionEventContext, subscriberEvaluationRequest, journeyID);

      /*****************************************
      *
      *  request
      *
      *****************************************/

      CommodityDeliveryRequest commodityDeliveryRequest = new CommodityDeliveryRequest(evolutionEventContext, deliveryRequestSource, null, providerID, commodityID, operation, amount, validityPeriodType, validityPeriodQuantity, null, origin, subscriberEvaluationRequest.getTenantID());
      commodityDeliveryRequest.setModuleID(newModuleID);
      commodityDeliveryRequest.setFeatureID(deliveryRequestSource);
      try {
        return Collections.singletonList(CommodityDeliveryManagerRemovalUtils.createDeliveryRequest(commodityDeliveryRequest,evolutionEventContext.getPaymentMeanService(),evolutionEventContext.getDeliverableService()));
      } catch (CommodityDeliveryException e) {
        log.info("could not create delivery request "+e.getError().getGenericResponseMessage());
        return Collections.emptyList();
      }
    }

    @Override public Map<String, String> getGUIDependencies(JourneyNode journeyNode, int tenantID)
    {
      Map<String, String> result = new HashMap<String, String>();
      String pointID = (String) journeyNode.getNodeParameters().get("node.parameter.commodityid");
      if (pointID != null) result.put("point", pointID.startsWith(POINT_PREFIX)?pointID.replace(POINT_PREFIX, ""):"");
      return result;
    }

  }
}
