/*****************************************************************************
*
*  INFulfillmentManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.evolving.nglm.evolution.statistics.CounterStat;
import com.evolving.nglm.evolution.statistics.DurationStat;
import com.evolving.nglm.evolution.statistics.StatBuilder;
import com.evolving.nglm.evolution.statistics.StatsBuilders;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.Expression.ConstantExpression;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
  
public class INFulfillmentManager extends DeliveryManager implements Runnable
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  INFulfillmentStatus
  //

  public enum INFulfillmentStatus
  {
    SUCCESS(0),
    PENDING(10),
    CUSTOMER_NOT_FOUND(20),
    SYSTEM_ERROR(21),
    TIMEOUT(22),
    THROTTLING(23),
    THIRD_PARTY_ERROR(24),
    BONUS_NOT_FOUND(100),
    INSUFFICIENT_BALANCE(405),
    CHECK_BALANCE_LT(300),
    CHECK_BALANCE_GT(301),
    CHECK_BALANCE_ET(302),
    UNKNOWN(999);
    private Integer externalRepresentation;
    private INFulfillmentStatus(Integer externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public Integer getExternalRepresentation() { return externalRepresentation; }
    public static INFulfillmentStatus fromReturnCode(Integer externalRepresentation) { for (INFulfillmentStatus enumeratedValue : INFulfillmentStatus.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
  }

  /*****************************************
  *
  *  conversion method
  *
  *****************************************/

  public DeliveryStatus getINFulfillmentStatus (INFulfillmentStatus status)
  {
    switch(status)
      {
        case PENDING:
          return DeliveryStatus.Pending;
        case SUCCESS:
          return DeliveryStatus.Delivered;
        case SYSTEM_ERROR:
        case THIRD_PARTY_ERROR:
        	return DeliveryStatus.Failed;
        case CUSTOMER_NOT_FOUND:
        case BONUS_NOT_FOUND:
          return DeliveryStatus.BonusNotFound;
        case INSUFFICIENT_BALANCE:
          return DeliveryStatus.InsufficientBalance;
        case TIMEOUT:
        case THROTTLING:
          return DeliveryStatus.FailedRetry;
        case CHECK_BALANCE_LT:
          return DeliveryStatus.CheckBalanceLowerThan;
        case CHECK_BALANCE_GT:
          return DeliveryStatus.CheckBalanceGreaterThan;
        case CHECK_BALANCE_ET:
          return DeliveryStatus.CheckBalanceEqualsTo;
        default:
          return DeliveryStatus.Failed;
      }
  }

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(INFulfillmentManager.class);

  //
  //  number of threads
  //
  
  private static final int threadNumber = 50;   //TODO : make this configurable (would even be better if it is used)
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private INPluginInterface[] inPluginArray;
  private ArrayList<Thread> threads = new ArrayList<Thread>();
  private StatBuilder<DurationStat> statsDuration = null;
  
  private EvaluationCriterion[] criterionArray;
  private boolean useCriteriaMode;
  private boolean connectionFailover = false;
  private int[] deliveryRatioArray;
  private double[] thruputMin, thruputMax; // defines interval for each connection, length proportional to connection thruput
  //     0................................................1
  //   min[0]   max[0]
  //            min[1]   max[1]
  //                     min[2]   max[2] ...
  //                                        min[n-1]   max[n-1]

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public INFulfillmentManager(String deliveryManagerKey, String pluginName, String pluginConfiguration)
  {
    //
    //  superclass
    //
    
    super("deliverymanager-infulfillment", deliveryManagerKey, Deployment.getBrokerServers(), INFulfillmentRequest.serde(), Deployment.getDeliveryManagers().get(pluginName), threadNumber);

    //
    //  plugin instanciation
    //
    
    String inPluginClassName = JSONUtilities.decodeString(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "inPluginClass", true);
    log.info("INFufillmentManager: plugin instanciation : inPluginClassName = "+inPluginClassName);

    JSONArray inPluginConfigurations = JSONUtilities.decodeJSONArray(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "inPluginConfiguration", true);
    inPluginArray = new INPluginInterface[inPluginConfigurations.size()];
    criterionArray = new EvaluationCriterion[inPluginConfigurations.size()];
    deliveryRatioArray = new int[inPluginConfigurations.size()];
    thruputMin = new double[inPluginConfigurations.size()];
    thruputMax = new double[inPluginConfigurations.size()];
    int thruputSum = 0;

    for (int i=0; i<inPluginConfigurations.size(); i++)
      {
        JSONObject inPluginConfiguration = (JSONObject) inPluginConfigurations.get(i);
        log.info("INFufillmentManager: plugin instanciation : inPluginConfiguration = "+inPluginConfiguration);

        try
        {
          inPluginArray[i] = (INPluginInterface) (Class.forName(inPluginClassName).newInstance());
          inPluginArray[i].init(inPluginConfiguration, pluginConfiguration);
        }
        catch (InstantiationException | IllegalAccessException | IllegalArgumentException e)
        {
          log.error("INFufillmentManager: could not create new instance of class " + inPluginClassName, e);
          throw new RuntimeException("INFufillmentManager: could not create new instance of class " + inPluginClassName, e);
        }
        catch (ClassNotFoundException e)
        {
          log.error("INFufillmentManager: could not find class " + inPluginClassName, e);
          throw new RuntimeException("INFufillmentManager: could not find class " + inPluginClassName, e);
        }
        
        JSONObject evaluationCriterionJSON = JSONUtilities.decodeJSONObject(inPluginConfiguration, "criteria", false);
        if (evaluationCriterionJSON == null) {
          criterionArray[i] = null;
        } else {
          try
          {
            EvaluationCriterion evaluationCriterion = new EvaluationCriterion(evaluationCriterionJSON, CriterionContext.Profile(0), 0);
            criterionArray[i] = evaluationCriterion;
            useCriteriaMode = true; // we have at least one valid criteria => criteria mode
          }
          catch (GUIManagerException e)
          {
            log.info("error in notificationPluginConfiguration criteria for " + JSONUtilities.decodeString(inPluginConfiguration, "inServers") + " connection : " + e.getLocalizedMessage());
          }
        }
        deliveryRatioArray[i] = JSONUtilities.decodeInteger(inPluginConfiguration, "deliveryRatio", Integer.MAX_VALUE);
        thruputSum += deliveryRatioArray[i];
      }
    // compute percentage per connection, for load balancing
    double currentMin = 0.0;
    for (int i=0; i<inPluginConfigurations.size(); i++) {
      thruputMin[i] = currentMin;
      currentMin += (double) deliveryRatioArray[i] / (double) thruputSum;
      thruputMax[i] = currentMin; 
    }
    
    //
    // statistics
    //

    statsDuration = StatsBuilders.getEvolutionDurationStatisticsBuilder("infulfillmentdelivery",pluginName+"-"+deliveryManagerKey);

    //
    //  threads
    //
    try 
    {
      for(int i = 0; i < threadNumber; i++)
        {
          Thread t = new Thread(this, "INFufillmentManagerThread_"+i);
          threads.add(t);
          t.start();
        }
    }
    catch(Exception e)
    {
      log.warn("Could not start threads ", e);
    }
    
    
    //
    //  startDelivery
    //
    
    startDelivery();

  }

  /*****************************************
  *
  *  class INFulfillmentRequest
  *
  *****************************************/

  public static class INFulfillmentRequest extends BonusDelivery
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
      schemaBuilder.name("service_infulfillment_request");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),8));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("providerID", Schema.STRING_SCHEMA);
      schemaBuilder.field("commodityID", Schema.STRING_SCHEMA);
      schemaBuilder.field("commodityName", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("externalAccountID", Schema.STRING_SCHEMA);
      schemaBuilder.field("operation", Schema.STRING_SCHEMA);
      schemaBuilder.field("amount", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("validityPeriodType", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("validityPeriodQuantity", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("return_code", Schema.INT32_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //
        
    private static ConnectSerde<INFulfillmentRequest> serde = new ConnectSerde<INFulfillmentRequest>(schema, false, INFulfillmentRequest.class, INFulfillmentRequest::pack, INFulfillmentRequest::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<INFulfillmentRequest> serde() { return serde; }
    public Schema subscriberStreamEventSchema() { return schema(); }
        
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String providerID;
    private String commodityID;
    private String commodityName;
    private String externalAccountID;
    private CommodityDeliveryOperation operation;
    private int amount;
    private TimeUnit validityPeriodType;
    private Integer validityPeriodQuantity;
    private int returnCode;
    private INFulfillmentStatus status;
    private String returnCodeDetails;

    //
    //  accessors
    //

    public String getProviderID() { return providerID; }
    public String getCommodityID() { return commodityID; }
    public String getCommodityName() { return commodityName; }
    public String getExternalAccountID() { return externalAccountID; }
    public CommodityDeliveryOperation getOperation() { return operation; }
    public int getAmount() { return amount; }
    public TimeUnit getValidityPeriodType() { return validityPeriodType; }
    public Integer getValidityPeriodQuantity() { return validityPeriodQuantity; }
    public Integer getReturnCode() { return returnCode; }
    public INFulfillmentStatus getStatus() { return status; }
    public String getReturnCodeDetails() { return returnCodeDetails; }

    //
    //  setters
    //  

    public void setReturnCode(Integer returnCode) { this.returnCode = returnCode; }
    public void setStatus(INFulfillmentStatus status) { this.status = status; }
    public void setReturnCodeDetails(String returnCodeDetails) { this.returnCodeDetails = returnCodeDetails; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public INFulfillmentRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource, String providerID, String commodityID, String commodityName, String externalAccountID, CommodityDeliveryOperation operation, int amount, TimeUnit validityPeriodType, Integer validityPeriodQuantity, int tenantID)
    {
      super(context, deliveryType, deliveryRequestSource, tenantID);
      this.providerID = providerID;
      this.commodityID = commodityID;
      this.commodityName = commodityName;
      this.externalAccountID = externalAccountID;
      this.operation = operation;
      this.amount = amount;
      this.validityPeriodType = validityPeriodType;
      this.validityPeriodQuantity = validityPeriodQuantity;
      this.status = INFulfillmentStatus.PENDING;
      this.returnCode = INFulfillmentStatus.PENDING.getExternalRepresentation();
      this.returnCodeDetails = "";
    }

    /*****************************************
    *
    *  constructor -- external
    *
    *****************************************/

    public INFulfillmentRequest(DeliveryRequest initialDeliveryRequest, JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager, int tenantID)
    {
      super(initialDeliveryRequest,jsonRoot, tenantID);
      this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", true);
      this.commodityID = JSONUtilities.decodeString(jsonRoot, "commodityID", true);
      this.commodityName = JSONUtilities.decodeString(jsonRoot, "commodityName", false);
      this.externalAccountID = JSONUtilities.decodeString(jsonRoot, "externalAccountID", true);
      this.operation = CommodityDeliveryOperation.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "operation", true));
      this.amount = JSONUtilities.decodeInteger(jsonRoot, "amount", false);
      this.validityPeriodType = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "validityPeriodType", false));
      this.validityPeriodQuantity = JSONUtilities.decodeInteger(jsonRoot, "validityPeriodQuantity", false);
      this.status = INFulfillmentStatus.PENDING;
      this.returnCode = INFulfillmentStatus.PENDING.getExternalRepresentation();
      this.returnCodeDetails = "";
    }

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private INFulfillmentRequest(SchemaAndValue schemaAndValue, String providerID, String commodityID, String commodityName, String externalAccountID, CommodityDeliveryOperation operation, int amount, TimeUnit validityPeriodType, Integer validityPeriodQuantity, INFulfillmentStatus status)
    {
      super(schemaAndValue);
      this.providerID = providerID;
      this.commodityID = commodityID;
      this.commodityName = commodityName;
      this.externalAccountID = externalAccountID;
      this.operation = operation;
      this.amount = amount;
      this.validityPeriodType = validityPeriodType;
      this.validityPeriodQuantity = validityPeriodQuantity;
      this.status = status;
      this.returnCode = status.getExternalRepresentation();
    }

    /*****************************************
    *
    *  constructor -- copy
    *
    *****************************************/

    private INFulfillmentRequest(INFulfillmentRequest inFulfillmentRequest)
    {
      super(inFulfillmentRequest);
      this.providerID = inFulfillmentRequest.getProviderID();
      this.commodityID = inFulfillmentRequest.getCommodityID();
      this.commodityName = inFulfillmentRequest.getCommodityName();
      this.externalAccountID = inFulfillmentRequest.getExternalAccountID();
      this.operation = inFulfillmentRequest.getOperation();
      this.amount = inFulfillmentRequest.getAmount();
      this.validityPeriodType = inFulfillmentRequest.getValidityPeriodType();
      this.validityPeriodQuantity = inFulfillmentRequest.getValidityPeriodQuantity();
      this.status = inFulfillmentRequest.getStatus();
      this.returnCode = inFulfillmentRequest.getReturnCode();
      this.returnCodeDetails = inFulfillmentRequest.getReturnCodeDetails();
    }

    /*****************************************
    *
    *  copy
    *
    *****************************************/

    public INFulfillmentRequest copy()
    {
      return new INFulfillmentRequest(this);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      INFulfillmentRequest inFulfillmentRequest = (INFulfillmentRequest) value;
      Struct struct = new Struct(schema);
      packCommon(struct, inFulfillmentRequest);
      struct.put("providerID", inFulfillmentRequest.getProviderID());
      struct.put("commodityID", inFulfillmentRequest.getCommodityID());
      struct.put("commodityName", inFulfillmentRequest.getCommodityName());
      struct.put("externalAccountID", inFulfillmentRequest.getExternalAccountID());
      struct.put("operation", inFulfillmentRequest.getOperation().getExternalRepresentation());
      struct.put("amount", inFulfillmentRequest.getAmount());
      struct.put("validityPeriodType", (inFulfillmentRequest.getValidityPeriodType() != null ? inFulfillmentRequest.getValidityPeriodType().getExternalRepresentation() : null));
      struct.put("validityPeriodQuantity", inFulfillmentRequest.getValidityPeriodQuantity());
      struct.put("return_code", inFulfillmentRequest.getReturnCode());
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

    public static INFulfillmentRequest unpack(SchemaAndValue schemaAndValue)
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
      String externalAccountID = valueStruct.getString("externalAccountID");
      CommodityDeliveryOperation operation = CommodityDeliveryOperation.fromExternalRepresentation(valueStruct.getString("operation"));
      int amount = valueStruct.getInt32("amount");
      TimeUnit validityPeriodType = TimeUnit.fromExternalRepresentation(valueStruct.getString("validityPeriodType"));
      Integer validityPeriodQuantity = valueStruct.getInt32("validityPeriodQuantity");
      Integer returnCode = valueStruct.getInt32("return_code");
      INFulfillmentStatus status = INFulfillmentStatus.fromReturnCode(returnCode);

      //
      //  return
      //

      return new INFulfillmentRequest(schemaAndValue, providerID, commodityID, commodityName, externalAccountID, operation, amount, validityPeriodType, validityPeriodQuantity, status);
    }

    /*****************************************
    *  
    *  toString
    *
    *****************************************/

    public String toString()
    {
      StringBuilder b = new StringBuilder();
      b.append("INFulfillmentRequest:{");
      b.append(super.toStringFields());
      b.append("," + getSubscriberID());
      b.append("," + providerID);
      b.append("," + commodityID);
      b.append("," + commodityName);
      b.append("," + externalAccountID);
      b.append("," + operation);
      b.append("," + amount);
      b.append("," + validityPeriodType);
      b.append("," + validityPeriodQuantity);
      b.append("," + returnCode);
      b.append("," + status.toString());
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
      guiPresentationMap.put(CUSTOMERID, getSubscriberID());
      guiPresentationMap.put(PROVIDERID, getProviderID());
      guiPresentationMap.put(PROVIDERNAME, Deployment.getFulfillmentProviders().get(getProviderID()).getProviderName());
      guiPresentationMap.put(DELIVERABLEID, getCommodityID());
      guiPresentationMap.put(DELIVERABLENAME, deliverableService.getActiveDeliverable(getCommodityID(), SystemTime.getCurrentTime()).getDeliverableName());
      guiPresentationMap.put(DELIVERABLEQTY, getAmount());
      guiPresentationMap.put(OPERATION, getOperation().getExternalRepresentation());
      guiPresentationMap.put(VALIDITYPERIODTYPE, getValidityPeriodType().getExternalRepresentation());
      guiPresentationMap.put(VALIDITYPERIODQUANTITY, getValidityPeriodQuantity());
      guiPresentationMap.put(MODULEID, getModuleID());
      guiPresentationMap.put(MODULENAME, getModule().toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(FEATURENAME, getFeatureName(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
      guiPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
      guiPresentationMap.put(ORIGIN, "");
      guiPresentationMap.put(RETURNCODE, getReturnCode());
      guiPresentationMap.put(RETURNCODEDETAILS, getReturnCodeDetails());
    }
    
    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService, int tenantID)
    {
      thirdPartyPresentationMap.put(PROVIDERID, getProviderID());
      thirdPartyPresentationMap.put(PROVIDERNAME, Deployment.getFulfillmentProviders().get(getProviderID()).getProviderName());
      thirdPartyPresentationMap.put(DELIVERABLEID, getCommodityID());
      thirdPartyPresentationMap.put(DELIVERABLENAME, deliverableService.getActiveDeliverable(getCommodityID(), SystemTime.getCurrentTime()).getDeliverableName());
      thirdPartyPresentationMap.put(DELIVERABLEQTY, getAmount());
      thirdPartyPresentationMap.put(OPERATION, getOperation().getExternalRepresentation());
      thirdPartyPresentationMap.put(VALIDITYPERIODTYPE, getValidityPeriodType().getExternalRepresentation());
      thirdPartyPresentationMap.put(VALIDITYPERIODQUANTITY, getValidityPeriodQuantity());
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
      // INFulfillmentRequest never rescheduled, let return unchanged
      //        
    }
  }

  /*****************************************
  *
  *  interface INPluginInterface
  *
  *****************************************/

  public interface INPluginInterface
  {
    public void init(JSONObject inPluginSharedConfiguration, String inPluginSpecificConfiguration);
    public INFulfillmentStatus credit(INFulfillmentRequest inFulfillmentRequest);
    public INFulfillmentStatus debit(INFulfillmentRequest inFulfillmentRequest);
    public INFulfillmentStatus activate(INFulfillmentRequest inFulfillmentRequest);
    public INFulfillmentStatus deactivate(INFulfillmentRequest inFulfillmentRequest);
    public INFulfillmentStatus set(INFulfillmentRequest inFulfillmentRequest);
    public INFulfillmentStatus check(INFulfillmentRequest inFulfillmentRequest);
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/

  @Override
  public void run()
  {
    while (true)
      {
        /*****************************************
        *
        *  nextRequest
        *
        *****************************************/
        
        DeliveryRequest deliveryRequest = nextRequest();

        /*****************************************
        *
        *  call INPlugin
        *
        *****************************************/

        long startTime = DurationStat.startTime();
        INFulfillmentStatus status = null;
        CommodityDeliveryOperation operation = ((INFulfillmentRequest)deliveryRequest).getOperation();
        
        // select an IN, based on throughput or criteria
        
        INPluginInterface inPlugin = null;
        Map<String, String> subscriberFields = deliveryRequest.getSubscriberFields(); // normalized to lower case
        if (useCriteriaMode) {
          log.info("Criteria mode");
          for (int i = 0; i<inPluginArray.length; i++) {
            INPluginInterface currentIN = inPluginArray[i]; 
            EvaluationCriterion criteria = criterionArray[i];
            if (criteria == null) { // no more criteria for this connection => take current sender
              inPlugin = currentIN;
            } else {
              String criterionField = criteria.getCriterionField().getID(); // "subscriber.city"
              String profileValue = subscriberFields.get(criterionField);
              if (profileValue != null) {
                Expression exp = criteria.getArgument();
                if (exp instanceof ConstantExpression) {
                  String value = ((ConstantExpression) exp).evaluateConstant() + "";  // "Las Vegas"
                  log.info("Evaluating " + profileValue + " " + criteria.getCriterionOperator() + " " + value);
                  switch (criteria.getCriterionOperator()) {
                    case EqualOperator:
                      if (profileValue.equalsIgnoreCase(value)) inPlugin = currentIN;
                      break;
                    case NotEqualOperator:
                      if (!profileValue.equalsIgnoreCase(value)) inPlugin = currentIN;
                      break;
                    default:
                      log.info("unsupported criteria operator : " + criteria.getCriterionOperator());
                  }
                } else {
                  log.info("only constants are supported for criteria : " + criteria.getArgument().getClass());
                }
              } else {
                log.info("profile value for " + criterionField + " is not available");
              }
            }
            if (inPlugin != null) break;
          }
          // if no sender matches criteria, or if there are errors, take the first IN to send 
          if ((inPlugin == null) && (inPluginArray.length > 0)) {
            inPlugin = inPluginArray[0];
          }
        } else { // load-balancing mode, select connection based on thruput repartition
          log.info("Load-balancing mode");
          double rnd = Math.random();
          for (int i = 0; i<inPluginArray.length; i++) {
            if (thruputMin[i] <= rnd && rnd < thruputMax[i]) {
              inPlugin = inPluginArray[i];
              log.info("selects IN # " + i);
              break;
            }
          }
          if (inPlugin == null) {
            inPlugin = inPluginArray[0]; // defaults to 1st one
            StringBuffer sb = new StringBuffer("Min : ");
            Arrays.stream(thruputMin).forEach(tm -> sb.append(tm).append(" "));
            sb.append("Max : ");
            Arrays.stream(thruputMax).forEach(tm -> sb.append(tm).append(" "));
            log.info("Unable to load-balance, rnd = " + rnd + " intervals are : " + sb);
          }
        }
        
        switch (operation) {
        case Credit:
          status = inPlugin.credit((INFulfillmentRequest)deliveryRequest);
          break;
        case Debit:
          status = inPlugin.debit((INFulfillmentRequest)deliveryRequest);
          break;
        case Activate:
          status = inPlugin.activate((INFulfillmentRequest)deliveryRequest);
          break;
        case Deactivate:
          status = inPlugin.deactivate((INFulfillmentRequest)deliveryRequest);
          break;
        case Set:
          status = inPlugin.set((INFulfillmentRequest)deliveryRequest);
          break;
        case Check:
          status = inPlugin.check((INFulfillmentRequest)deliveryRequest);
          break;
        default:
          break;
        }
        
        /*****************************************
        *
        *  update and return response
        *
        *****************************************/

        ((INFulfillmentRequest)deliveryRequest).setStatus(status);
        ((INFulfillmentRequest)deliveryRequest).setReturnCode(status.getExternalRepresentation());
        deliveryRequest.setDeliveryStatus(getINFulfillmentStatus(status));
        deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
        completeRequest(deliveryRequest);

        statsDuration.withLabel(StatsBuilders.LABEL.status.name(),((INFulfillmentRequest) deliveryRequest).getStatus().name())
                     .withLabel(StatsBuilders.LABEL.operation.name(),((INFulfillmentRequest) deliveryRequest).getOperation().name())
                     .withLabel(StatsBuilders.LABEL.module.name(), deliveryRequest.getModule().name())
                     .withLabel(StatsBuilders.LABEL.tenant.name(), String.valueOf(deliveryRequest.getTenantID()))
                     .getStats().add(startTime);

      }
  }

  /*****************************************
  *
  *  processCorrelatorUpdate
  *
  *****************************************/

  @Override protected void processCorrelatorUpdate(DeliveryRequest deliveryRequest, JSONObject correlatorUpdate)
  {
    int result = JSONUtilities.decodeInteger(correlatorUpdate, "result", true);
    switch (result)
      {
        case 0:
        case 1:
          log.info("INFufillmentManager:  processCorrelatorUpdate success for {}", deliveryRequest.getDeliveryRequestID());
          deliveryRequest.setDeliveryStatus(DeliveryStatus.Delivered);
          deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
          completeRequest(deliveryRequest);
          break;
          
        case 2:
          log.info("INFufillmentManager:  processCorrelatorUpdate failure for {}", deliveryRequest.getDeliveryRequestID());
          deliveryRequest.setDeliveryStatus(DeliveryStatus.Failed);
          deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
          completeRequest(deliveryRequest);
          break;          
      }
  }

  
  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  @Override protected void shutdown()
  {
    log.info("INFufillmentManager:  shutdown");
  }
  
  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    log.info("INFufillmentManager: recieved " + args.length + " args");
    for(String arg : args){
      log.info("INFufillmentManager: arg " + arg);
    }
    
    //
    //  configuration
    //

    String deliveryManagerKey = args[0];
    String pluginName = args[1];
    String pluginConfiguration = args[2];

    //
    //  instance  
    //
    
    log.info("Configuration " + Deployment.getDeliveryManagers());

    
    int nbInstances = JSONUtilities.decodeInteger(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "nbInstancePerProcess", 1);
    log.warn("Number of instances " + nbInstances);
    
    for(int i=0; i < nbInstances; i++)
      {        
        new INFulfillmentManager(deliveryManagerKey, pluginName, pluginConfiguration);        
      }
  }
}
