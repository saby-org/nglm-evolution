/*****************************************************************************
*
*  CommodityDeliveryManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EmptyFulfillmentManager.EmptyFulfillmentRequest;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.INFulfillmentManager.Account;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;
import com.evolving.nglm.evolution.PointTypeFulfillmentManager.PointTypeFulfillmentRequest;
import com.evolving.nglm.evolution.PointTypeService.PointTypeListener;
import com.evolving.nglm.evolution.SubscriberProfileService.RedisSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;

public class CommodityDeliveryManager extends DeliveryManager implements Runnable, PointTypeListener
{

  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  CommodityOperation
  //

  public enum CommodityOperation
  {
    Credit("credit"),
    Debit("debit"),
    Set("set"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private CommodityOperation(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CommodityOperation fromExternalRepresentation(String externalRepresentation) { for (CommodityOperation enumeratedValue : CommodityOperation.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  CommoditySelection
  //

  public enum CommodityType {
    IN(INFulfillmentRequest.class.getName()),
    POINT(PointTypeFulfillmentRequest.class.getName()),
    EMPTY(EmptyFulfillmentRequest.class.getName());

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

  //
  //  CommodityStatus
  //

  public enum CommodityStatus
  {
    PENDING(10),
    SYSTEM_ERROR(21),
    THIRD_PARTY_ERROR(24),
    COMMODITY_NOT_FOUND(999999999),
    INSUFFICIENT_BALANCE(405),
    UNKNOWN(999);
    private Integer externalRepresentation;
    private CommodityStatus(Integer externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public Integer getReturnCode() { return externalRepresentation; }
    public static CommodityStatus fromReturnCode(Integer externalRepresentation) { for (CommodityStatus enumeratedValue : CommodityStatus.values()) { if (enumeratedValue.getReturnCode().equals(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
  }
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CommodityDeliveryManager.class);

  //
  //  variables
  //
  
  private int threadNumber = 5;   //TODO : make this configurable
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private ArrayList<Thread> threads = new ArrayList<Thread>();
  
  private SubscriberProfileService subscriberProfileService;
  private PointTypeService pointTypeService;
  private BDRStatistics bdrStats = null;
  
  private Map<String, FulfillmentProvider> providers = new HashMap<String, FulfillmentProvider>();
  
  private String pointTypeProviderID = null;
  private String pointTypeDeliveryType = null;
  
  private Map<String, Map<String, String>> paymentMeans /*debit only*/ = new HashMap<String/*providerID*/, Map<String/*paymentMeanID*/, String/*commodityType+"-"+deliveryType*/>>();
  private Map<String, Map<String, String>> commodities /*credit only*/ = new HashMap<String/*providerID*/, Map<String/*commodityID*/, String/*commodityType+"-"+deliveryType*/>>();
  
  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public Map<String, FulfillmentProvider> getProviders() { return providers; }
  public Map<String, Map<String, String>> getProviderName() { return paymentMeans; }
  public Map<String, Map<String, String>> getProviderType() { return commodities; }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CommodityDeliveryManager(String deliveryManagerKey)
  {
    //
    //  superclass
    //
    
    super("deliverymanager-commodityDelivery", deliveryManagerKey, Deployment.getBrokerServers(), CommodityRequest.serde(), Deployment.getDeliveryManagers().get("commodityDelivery"));

    //
    //  plugin instanciation
    //
    
    subscriberProfileService = new RedisSubscriberProfileService(Deployment.getRedisSentinels());
    subscriberProfileService.start();
    
    pointTypeService = new PointTypeService(Deployment.getBrokerServers(), "CommodityMgr-pointtypeservice-"+deliveryManagerKey, Deployment.getPointTypeTopic(), false, this);
    pointTypeService.start();
    
    //
    // get list of paymentMeans and list of commodities
    //
    
    getProviderAndCommodityAndPaymentMeanFromDM();
    
    //
    // statistics
    //
    
    try{
      bdrStats = new BDRStatistics("deliverymanager-commodityDelivery");
    }catch(Exception e){
      log.error("CommodityDeliveryManager : could not load statistics ", e);
      throw new RuntimeException("CommodityDeliveryManager : could not load statistics  ", e);
    }
    
    //
    //  threads
    //
    
    for(int i = 0; i < threadNumber; i++)
      {
        threads.add(new Thread(this, "CommodityDeliveryManagerThread_"+i));
      }
    
    //
    //  startDelivery
    //
    
    startDelivery();

  }

  private void getProviderAndCommodityAndPaymentMeanFromDM() {
    for(DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values()){
      CommodityType commodityType = CommodityType.fromExternalRepresentation(deliveryManager.getRequestClassName());
      
      // -------------------------------
      // handle IN managers
      // -------------------------------
      
      if((commodityType != null) && (commodityType.equals(CommodityType.IN))){
        log.debug("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : get information from deliveryManager "+deliveryManager);

        //
        // get information from DeliveryManager
        //
        
        JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
        String deliveryType = (String) deliveryManagerJSON.get("deliveryType");
        String providerID = (String) deliveryManagerJSON.get("providerID");
        String providerName = (String) deliveryManagerJSON.get("providerName");
        String providerURL = (String) deliveryManagerJSON.get("url");
        JSONArray availableAccountsArray = (JSONArray) deliveryManagerJSON.get("availableAccounts");

        //
        // get paymentMeans and commodities from availableAccounts
        //
        
        Map<String, String> paymentMeanIDs = new HashMap<String/*paymentMeanID*/, String/*deliveryType*/>();
        Map<String, String> commodityIDs = new HashMap<String/*commodityID*/, String/*deliveryType*/>();
        for (int i=0; i<availableAccountsArray.size(); i++) {
          Account newAccount = new Account((JSONObject) availableAccountsArray.get(i));
          if(newAccount.getDebitable()){
            paymentMeanIDs.put(newAccount.getAccountID(), commodityType+"-"+deliveryType);
          }
          if(newAccount.getCreditable()){
            commodityIDs.put(newAccount.getAccountID(), commodityType+"-"+deliveryType);
          }
        }

        //
        // update provider list
        //
        
        Map<String, String> providerJSON = new HashMap<String, String>();
        providerJSON.put("id", providerID);
        providerJSON.put("name", providerName);
        providerJSON.put("providerType", commodityType.toString());
        providerJSON.put("url", providerURL);
        FulfillmentProvider provider = new FulfillmentProvider(JSONUtilities.encodeObject(providerJSON));
        providers.put(providerID, provider);
        
        //
        // update paymentMean list
        //
        
        if(!paymentMeanIDs.isEmpty()){
          if(!paymentMeans.keySet().contains(providerID)){
            paymentMeans.put(providerID, paymentMeanIDs);
          }else{
            paymentMeans.get(providerID).putAll(paymentMeanIDs);
          }
        }

        //
        // update commodity list
        //
        
        if(!commodityIDs.isEmpty()){
          if(!commodities.keySet().contains(providerID)){
            commodities.put(providerID, commodityIDs);
          }else{
            commodities.get(providerID).putAll(commodityIDs);
          }
        }

        log.debug("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : get information from deliveryManager "+deliveryManager+" DONE");

      } 
      
      // -------------------------------
      // handle internal points manager
      // -------------------------------

      else if((commodityType != null) && (commodityType.equals(CommodityType.POINT))){
        log.debug("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : get information from deliveryManager "+deliveryManager);

        //
        // get information from DeliveryManager
        //
        
        JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
        pointTypeDeliveryType = (String) deliveryManagerJSON.get("deliveryType");
        pointTypeProviderID = (String) deliveryManagerJSON.get("providerID");
        String providerName = (String) deliveryManagerJSON.get("providerName");
        String providerURL = (String) deliveryManagerJSON.get("url");

        //
        // get paymentMeans and commodities from pointTypes
        //
        
        Map<String, String> paymentMeanIDs = new HashMap<String/*paymentMeanID*/, String/*deliveryType*/>();
        Map<String, String> commodityIDs = new HashMap<String/*commodityID*/, String/*deliveryType*/>();
        for (PointType pointType : pointTypeService.getActivePointTypes(SystemTime.getCurrentTime())) {
          if(pointType.getDebitable()){
            paymentMeanIDs.put(pointType.getPointTypeID(), commodityType+"-"+pointTypeDeliveryType);
          }
          if(pointType.getCreditable()){
            commodityIDs.put(pointType.getPointTypeID(), commodityType+"-"+pointTypeDeliveryType);
          }
        }

        //
        // update provider list
        //
        
        Map<String, String> providerJSON = new HashMap<String, String>();
        providerJSON.put("id", pointTypeProviderID);
        providerJSON.put("name", providerName);
        providerJSON.put("providerType", commodityType.toString());
        providerJSON.put("url", providerURL);
        FulfillmentProvider provider = new FulfillmentProvider(JSONUtilities.encodeObject(providerJSON));
        providers.put(pointTypeProviderID, provider);
        
        //
        // update paymentMean list
        //
        
        if(!paymentMeanIDs.isEmpty()){
          if(!paymentMeans.keySet().contains(pointTypeProviderID)){
            paymentMeans.put(pointTypeProviderID, paymentMeanIDs);
          }else{
            paymentMeans.get(pointTypeProviderID).putAll(paymentMeanIDs);
          }
        }

        //
        // update commodity list
        //
        
        if(!commodityIDs.isEmpty()){
          if(!commodities.keySet().contains(pointTypeProviderID)){
            commodities.put(pointTypeProviderID, commodityIDs);
          }else{
            commodities.get(pointTypeProviderID).putAll(commodityIDs);
          }
        }

        log.debug("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : get information from deliveryManager "+deliveryManager+" DONE");

      } 
      
      // -------------------------------
      // handle empty manager
      // -------------------------------
      
      else if((commodityType != null) && (commodityType.equals(CommodityType.POINT))){
        log.debug("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : get information from deliveryManager "+deliveryManager);

        //
        // get information from DeliveryManager
        //
        
        JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
        String providerID = (String) deliveryManagerJSON.get("providerID");
        String providerName = (String) deliveryManagerJSON.get("providerName");
        String providerURL = (String) deliveryManagerJSON.get("url");

        //
        // update provider list
        //
        
        Map<String, String> providerJSON = new HashMap<String, String>();
        providerJSON.put("id", providerID);
        providerJSON.put("name", providerName);
        providerJSON.put("providerType", commodityType.toString());
        providerJSON.put("url", providerURL);
        FulfillmentProvider provider = new FulfillmentProvider(JSONUtilities.encodeObject(providerJSON));
        providers.put(providerID, provider);
        
      }
      
      // -------------------------------
      // skip all other managers
      // -------------------------------
      
      else{

        log.debug("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : skip deliveryManager "+deliveryManager);

      }
    }
  }

  /*****************************************
  *
  *  class CommodityRequest
  *
  *****************************************/

  public static class CommodityRequest extends DeliveryRequest
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
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("providerID", Schema.STRING_SCHEMA);
      schemaBuilder.field("commodityID", Schema.STRING_SCHEMA);
      schemaBuilder.field("operation", Schema.STRING_SCHEMA);
      schemaBuilder.field("amount", Schema.INT32_SCHEMA);
      schemaBuilder.field("return_code", Schema.INT32_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //
        
    private static ConnectSerde<CommodityRequest> serde = new ConnectSerde<CommodityRequest>(schema, false, CommodityRequest.class, CommodityRequest::pack, CommodityRequest::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<CommodityRequest> serde() { return serde; }
    public Schema subscriberStreamEventSchema() { return schema(); }
        
    /*****************************************
    *
    *  data
    *
    *****************************************/

//  private String deliveryRequestID;
//  private String deliveryRequestSource;
//  private String deliveryType;
//  private String eventID;
//  private String moduleID;
//  private String featureID;
//  private String subscriberID;
//  private boolean control;

    private String providerID;
    private String commodityID;
    private CommodityOperation operation;
    private int amount;
    
//    private Date startValidityDate; //TODO SCH : schema, accessors, ..., and everything !!!
//    private Date endValidityDate; //TODO SCH : schema, accessors, ..., and everything !!!
    
    private CommodityStatus status;
    private int returnCode;
    private String returnCodeDetails;
    
    //
    //  accessors
    //

    public String getProviderID() { return providerID; }
    public String getCommodityID() { return commodityID; }
    public CommodityOperation getOperation() { return operation; }
    public int getAmount() { return amount; }
    public CommodityStatus getStatus() { return status; }
    public int getReturnCode() { return returnCode; }
    public String getReturnCodeDetails() { return returnCodeDetails; }

    //
    //  setters
    //

    public void setStatus(CommodityStatus status) { this.status = status; }
    public void setReturnCode(Integer returnCode) { this.returnCode = returnCode; }
    public void setReturnCodeDetails(String returnCodeDetails) { this.returnCodeDetails = returnCodeDetails; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public CommodityRequest(EvolutionEventContext context, String deliveryRequestSource, String providerID, String commodityID, CommodityOperation operation, int amount)
    {
      super(context, "commodityDelivery", deliveryRequestSource);
      this.providerID = providerID;
      this.commodityID = commodityID;
      this.operation = operation;
      this.amount = amount;
      this.status = CommodityStatus.PENDING;
      this.returnCode = CommodityStatus.PENDING.getReturnCode();
      this.returnCodeDetails = "";
    }

    /*****************************************
    *
    *  constructor -- external
    *
    *****************************************/

    public CommodityRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
    {
      super(jsonRoot);
      this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", true);
      this.commodityID = JSONUtilities.decodeString(jsonRoot, "commodityID", true);
      this.operation = CommodityOperation.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "operation", true));
      this.amount = JSONUtilities.decodeInteger(jsonRoot, "amount", true);
      this.status = CommodityStatus.PENDING;
      this.returnCode = CommodityStatus.PENDING.getReturnCode();
      this.returnCodeDetails = "";
    }

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private CommodityRequest(SchemaAndValue schemaAndValue, String providerID, String commodityID, CommodityOperation operation, int amount, CommodityStatus status)
    {
      super(schemaAndValue);
      this.providerID = providerID;
      this.commodityID = commodityID;
      this.operation = operation;
      this.amount = amount;
      this.status = status;
      this.returnCode = status.getReturnCode();
      this.returnCodeDetails = "";
    }

    /*****************************************
    *
    *  constructor -- copy
    *
    *****************************************/

    private CommodityRequest(CommodityRequest commodityRequest)
    {
      super(commodityRequest);
      this.providerID = commodityRequest.getProviderID();
      this.commodityID = commodityRequest.getCommodityID();
      this.operation = commodityRequest.getOperation();
      this.amount = commodityRequest.getAmount();
      this.status = commodityRequest.getStatus();
      this.returnCode = commodityRequest.getReturnCode();
      this.returnCodeDetails = commodityRequest.getReturnCodeDetails();
    }

    /*****************************************
    *
    *  copy
    *
    *****************************************/

    public CommodityRequest copy()
    {
      return new CommodityRequest(this);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      CommodityRequest commodityRequest = (CommodityRequest) value;
      Struct struct = new Struct(schema);
      packCommon(struct, commodityRequest);
      struct.put("providerID", commodityRequest.getProviderID());
      struct.put("commodityID", commodityRequest.getCommodityID());
      struct.put("operation", commodityRequest.getOperation().getExternalRepresentation());
      struct.put("amount", commodityRequest.getAmount());
      struct.put("return_code", commodityRequest.getReturnCode());
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

    public static CommodityRequest unpack(SchemaAndValue schemaAndValue)
    {
      //
      //  data
      //

      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

      //  unpack
      //

      Struct valueStruct = (Struct) value;
      String providerID = valueStruct.getString("providerID");
      String commodityID = valueStruct.getString("commodityID");
      CommodityOperation operation = CommodityOperation.fromExternalRepresentation(valueStruct.getString("operation"));
      int amount = valueStruct.getInt32("amount");
      Integer returnCode = valueStruct.getInt32("return_code");
      CommodityStatus status = CommodityStatus.fromReturnCode(returnCode);

      //
      //  return
      //

      return new CommodityRequest(schemaAndValue, providerID, commodityID, operation, amount, status);
    }

    /*****************************************
    *  
    *  toString
    *
    *****************************************/

    public String toString()
    {
      StringBuilder b = new StringBuilder();
      b.append("CommodityRequest:{");
      b.append(super.toStringFields());
      b.append("," + getSubscriberID());
      b.append("," + providerID);
      b.append("," + commodityID);
      b.append("," + operation);
      b.append("," + amount);
      b.append("," + returnCode);
      b.append("," + returnCodeDetails);
      b.append("}");
      return b.toString();
    }
    
    @Override public Integer getActivityType() { return ActivityType.BDR.getExternalRepresentation(); }
    
    /****************************************
    *
    *  presentation utilities
    *
    ****************************************/
    
    @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SalesChannelService salesChannelService)
    {
      guiPresentationMap.put(CUSTOMERID, getSubscriberID());
      guiPresentationMap.put(PROVIDERID, getProviderID());
      guiPresentationMap.put(DELIVERABLEID, getCommodityID());
      guiPresentationMap.put(DELIVERABLEQTY, getAmount());
      guiPresentationMap.put(OPERATION, getOperation().getExternalRepresentation());
      guiPresentationMap.put(MODULEID, getModuleID());
      guiPresentationMap.put(MODULENAME, Module.fromExternalRepresentation(getModuleID()).toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(ORIGIN, "");
      guiPresentationMap.put(RETURNCODE, getReturnCode());
      guiPresentationMap.put(RETURNCODEDETAILS, getReturnCodeDetails());
    }
    
    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SalesChannelService salesChannelService)
    {
      thirdPartyPresentationMap.put(CUSTOMERID, getSubscriberID());
      thirdPartyPresentationMap.put(PROVIDERID, getProviderID());
      thirdPartyPresentationMap.put(DELIVERABLEID, getCommodityID());
      thirdPartyPresentationMap.put(DELIVERABLEQTY, getAmount());
      thirdPartyPresentationMap.put(OPERATION, getOperation().getExternalRepresentation());
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, Module.fromExternalRepresentation(getModuleID()).toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(ORIGIN, "");
      thirdPartyPresentationMap.put(RETURNCODE, getReturnCode());
      thirdPartyPresentationMap.put(RETURNCODEDETAILS, getReturnCodeDetails());
    }
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/

  @Override
  public void run()
  {
    while (isProcessing())
      {
        /*****************************************
        *
        *  nextRequest
        *
        *****************************************/
        
        DeliveryRequest deliveryRequest = nextRequest();
        CommodityRequest commodityRequest = ((CommodityRequest)deliveryRequest);

        /*****************************************
        *
        *  respond with correlator
        *
        *****************************************/
        
        String correlator = deliveryRequest.getDeliveryRequestID();
        deliveryRequest.setCorrelator(correlator);
        updateRequest(deliveryRequest);
        
        /*****************************************
        *
        *  get offer, customer, ...
        *
        *****************************************/
        
        String subscriberID = commodityRequest.getSubscriberID();
        String providerID = commodityRequest.getProviderID();
        String commodityID = commodityRequest.getCommodityID();
        CommodityOperation operation = commodityRequest.getOperation();
        int amount = commodityRequest.getAmount();
        CommodityRequestStatus commodityRequestStatus = new CommodityRequestStatus(correlator, commodityRequest.getEventID(), commodityRequest.getModuleID(), commodityRequest.getFeatureID(), subscriberID, providerID, commodityID, operation.getExternalRepresentation(), amount);
        
        //
        // Get amount
        //
        
        if(amount < 1){
          log.info(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : bad field value for amount");
          submitCorrelatorUpdate(commodityRequestStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "bad field value for amount"/* TODO : use right message here */);
          return;
        }
        
        //
        // Get customer
        //
        
        if(subscriberID == null){
          log.info(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : bad field value for subscriberID");
          submitCorrelatorUpdate(commodityRequestStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "bad field value for subscriberID"/* TODO : use right message here */);
          return;
        }
        SubscriberProfile subscriberProfile = null;
        try{
          subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID);
          if(subscriberProfile == null){
            log.info(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : subscriber " + subscriberID + " not found");
            submitCorrelatorUpdate(commodityRequestStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "subscriber " + subscriberID + " not found"/* TODO : use right message here */);
            return;
          }else{
            log.info(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : subscriber " + subscriberID + " found ("+subscriberProfile+")");
          }
        }catch (SubscriberProfileServiceException e) {
          log.info(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : subscriberService not available");
          submitCorrelatorUpdate(commodityRequestStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "subscriberService not available"/* TODO : use right message here */);
          return;
        }

        //
        // Get commodity
        //
        
        //TODO SCH : A IMPLEMENTER ??? !!!

        /*****************************************
        *
        *  Proceed with the commodity action
        *
        *****************************************/

        proceedCommodityRequest(commodityRequestStatus);
        
      }
  }

  /*****************************************
  *
  *  CorrelatorUpdate
  *
  *****************************************/

  private void submitCorrelatorUpdate(CommodityRequestStatus commodityRequestStatus, DeliveryStatus deliveryStatus, int statusCode, String statusMessage){
    commodityRequestStatus.setDeliveryStatus(deliveryStatus);
    commodityRequestStatus.setDeliveryStatusCode(statusCode);
    commodityRequestStatus.setDeliveryStatusMessage(statusMessage);
    submitCorrelatorUpdate(commodityRequestStatus);
  }
  
  private void submitCorrelatorUpdate(CommodityRequestStatus commodityRequestStatus){
    submitCorrelatorUpdate(commodityRequestStatus.getCorrelator(), commodityRequestStatus.getJSONRepresentation());
  }

  @Override protected void processCorrelatorUpdate(DeliveryRequest deliveryRequest, JSONObject correlatorUpdate)
  {
    log.info("CommodityDeliveryManager.processCorrelatorUpdate("+deliveryRequest.getDeliveryRequestID()+", "+correlatorUpdate+") : called ...");

    CommodityRequestStatus commodityRequestStatus = new CommodityRequestStatus(correlatorUpdate);
    deliveryRequest.setDeliveryStatus(commodityRequestStatus.getDeliveryStatus());
    deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
    completeRequest(deliveryRequest);
    bdrStats.updateBDREventCount(1, deliveryRequest.getDeliveryStatus());

    log.debug("CommodityDeliveryManager.processCorrelatorUpdate("+deliveryRequest.getDeliveryRequestID()+", "+correlatorUpdate+") : DONE");

  }

  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  @Override protected void shutdown()
  {
    log.info("CommodityDeliveryManager: shutdown called");
    log.info("CommodityDeliveryManager: shutdown DONE");
  }
  
  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    log.info("CommodityDeliveryManager: recieved " + args.length + " args :");
    for(int index = 0; index < args.length; index++){
      log.info("       args["+index+"] " + args[index]);
    }
    
    //
    //  configuration
    //

    String deliveryManagerKey = args[0];

    //
    //  instance  
    //
    
    CommodityDeliveryManager manager = new CommodityDeliveryManager(deliveryManagerKey);

    //
    //  run
    //

    manager.run();
  }
  
  /*****************************************
  *
  *  proceed with commodity request
  *
  *****************************************/

  private void proceedCommodityRequest(CommodityRequestStatus commodityRequestStatus){
    
    //
    // execute commodity request
    //

    
//  //TODO SCH : A IMPLEMENTER !!! !!! !!! !!! !!! 
//  commodityActionManager.makePayment(commodityRequestStatus.getJSONRepresentation(), commodityRequestStatus.getCorrelator(), commodityRequestStatus.getEventID(), commodityRequestStatus.getModuleID(), commodityRequestStatus.getFeatureID(), commodityRequestStatus.getSubscriberID(), offerPrice.getProviderID(), offerPrice.getPaymentMeanID(), offerPrice.getAmount() * commodityRequestStatus.getAmount(), this);
//  commodityActionManager.creditCommodity(commodityRequestStatus.getJSONRepresentation(), commodityRequestStatus.getCorrelator(), commodityRequestStatus.getEventID(), commodityRequestStatus.getModuleID(), commodityRequestStatus.getFeatureID(), commodityRequestStatus.getSubscriberID(), offerPrice.getProviderID(), offerPrice.getPaymentMeanID(), offerPrice.getAmount() * commodityRequestStatus.getAmount(), this);
    
    log.info(Thread.currentThread().getId()+"CommodityDeliveryManager.proceedCommodityRequest(...) called !!! !!! !!! !!! !!! !!! !!! !!! ");
    
    printVariableContent();
    
    //
    // everything is OK => update and return response (succeed)
    //
    
    submitCorrelatorUpdate(commodityRequestStatus, DeliveryStatus.Delivered, 0/* TODO : use right code here */, "Success"/* TODO : use right message here */);
    
  }

  //=========================================================================================
  //TODO :   | TODO : REMOVE THIS   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!
  //        \|/
  //=========================================================================================
  private void printVariableContent(){
    log.info("====================================================================================");
    log.info("        PAYMENT MEANS");
    log.info("====================================================================================");
    for(String providerIdentifier : paymentMeans.keySet()){
      log.info("PROVIDER : "+providerIdentifier);
      Map<String, String> providerPaymentMeans = paymentMeans.get(providerIdentifier);
      for(String paymentID : providerPaymentMeans.keySet()){
        log.info("      PaymentMean "+paymentID+"  ->  "+providerPaymentMeans.get(paymentID));
      }
    }
    log.info("====================================================================================");
    log.info("        COMMODITIES");
    log.info("====================================================================================");
    for(String providerIdentifier : commodities.keySet()){
      log.info("PROVIDER : "+providerIdentifier);
      Map<String, String> providerPaymentMeans = commodities.get(providerIdentifier);
      for(String commodityID : providerPaymentMeans.keySet()){
        log.info("      commodity "+commodityID+"  ->  "+providerPaymentMeans.get(commodityID));
      }
    }
    log.info("====================================================================================");
  }
  //=========================================================================================
  //        /|\
  //TODO :   | TODO : REMOVE THIS   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!
  //=========================================================================================
  
  /*****************************************
  *
  *  class CommodityRequestStatus
  *
  *****************************************/

  public static class CommodityRequestStatus
  {

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String correlator = null;
    private String eventID = null;
    private String subscriberID = null;
    private String moduleID = null;
    private String featureID = null;
    private String providerID = null;
    private String commodityID = null;
    private String operation = null;
    private int amount = -1;
    
    private DeliveryStatus deliveryStatus = DeliveryStatus.Unknown;
    private int deliveryStatusCode = -1;
    private String deliveryStatusMessage = null;
    
    /*****************************************
    *
    *  getters
    *
    *****************************************/

    public String getCorrelator(){return correlator;}
    public String getEventID(){return eventID;}
    public String getSubscriberID(){return subscriberID;}
    public String getModuleID(){return moduleID;}
    public String getFeatureID(){return featureID;}
    public String getProviderID(){return providerID;}
    public String getCommodityID(){return commodityID;}
    public String getOperation(){return operation;}
    public int getAmount(){return amount;}

    public DeliveryStatus getDeliveryStatus(){return deliveryStatus;}
    public int getDeliveryStatusCode(){return deliveryStatusCode;}
    public String getDeliveryStatusMessage(){return deliveryStatusMessage;}

    /*****************************************
    *
    *  setters
    *
    *****************************************/

    public void setDeliveryStatus(DeliveryStatus deliveryStatus){this.deliveryStatus = deliveryStatus;}
    public void setDeliveryStatusCode(int deliveryStatusCode){this.deliveryStatusCode = deliveryStatusCode;}
    public void setDeliveryStatusMessage(String deliveryStatusMessage){this.deliveryStatusMessage = deliveryStatusMessage;}

    /*****************************************
    *
    *  Constructors
    *
    *****************************************/

    public CommodityRequestStatus(String correlator, String eventID, String moduleID, String featureID, String subscriberID, String providerID, String commodityID, String operation, int amount){
      this.correlator = correlator;
      this.eventID = eventID;
      this.subscriberID = subscriberID;
      this.moduleID = moduleID;
      this.featureID = featureID;
      this.providerID = providerID;
      this.commodityID = commodityID;
      this.operation = operation;
      this.amount = amount;
    }

    /*****************************************
    *
    *  constructor -- JSON
    *
    *****************************************/

    public CommodityRequestStatus(JSONObject jsonRoot)
    {  
      this.correlator = JSONUtilities.decodeString(jsonRoot, "correlator", true);
      this.eventID = JSONUtilities.decodeString(jsonRoot, "eventID", true);
      this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
      this.moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", true);
      this.featureID = JSONUtilities.decodeString(jsonRoot, "featureID", true);
      this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", true);
      this.commodityID = JSONUtilities.decodeString(jsonRoot, "commodityID", true);
      this.operation = JSONUtilities.decodeString(jsonRoot, "operation", true);
      this.amount = JSONUtilities.decodeInteger(jsonRoot, "amount", true);
      
      this.deliveryStatus = DeliveryStatus.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "deliveryStatus", false));
      this.deliveryStatusCode = JSONUtilities.decodeInteger(jsonRoot, "deliveryStatusCode", false);
      this.deliveryStatusMessage = JSONUtilities.decodeString(jsonRoot, "deliveryStatusMessage", false);
    }
    
    /*****************************************
    *  
    *  to JSONObject
    *
    *****************************************/
    
    public JSONObject getJSONRepresentation(){
      Map<String, Object> data = new HashMap<String, Object>();
      
      data.put("correlator", this.getCorrelator());
      data.put("eventID", this.getEventID());
      data.put("subscriberID", this.getSubscriberID());
      data.put("moduleID", this.getModuleID());
      data.put("featureID", this.getFeatureID());
      data.put("providerID", this.getProviderID());
      data.put("commodityID", this.getCommodityID());
      data.put("operation", this.getOperation());
      data.put("amount", this.getAmount());
      
      data.put("deliveryStatus", this.getDeliveryStatus().getExternalRepresentation());
      data.put("deliveryStatusCode", this.getDeliveryStatusCode());
      data.put("deliveryStatusMessage", this.getDeliveryStatusMessage());

      return JSONUtilities.encodeObject(data);
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

    private CommodityOperation operation;
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ActionManager(JSONObject configuration)
    {
      super(configuration);
      this.operation = CommodityOperation.fromExternalRepresentation(JSONUtilities.decodeString(configuration, "operation", true));
    }

    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public DeliveryRequest executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      /*****************************************
      *
      *  parameters
      *
      *****************************************/

      String providerID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.providerid");
      String commodityID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.pointid");
      int amount = ((Number) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.amount")).intValue();
      
      /*****************************************
      *
      *  TEMP DEW HACK
      *
      *****************************************/

      providerID = (providerID != null) ? providerID : "0"; //TODO : maybe set default here (ie internal provider)
      commodityID = (commodityID != null) ? commodityID : "0";

      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      CommodityOperation operation = this.operation;

      /*****************************************
      *
      *  request
      *
      *****************************************/

      CommodityRequest request = new CommodityRequest(evolutionEventContext, deliveryRequestSource, providerID, commodityID, operation, amount);

      /*****************************************
      *
      *  return
      *
      *****************************************/

      return request;
    }
  }

  
  /*****************************************
  *
  *  Implements interface PointTypeListener
  *
  *****************************************/

  @Override
  public void pointTypeActivated(PointType pointType)
  {
    log.info("CommodityDeliveryManager.pointTypeActivated() called ...");
    
    //
    // get paymentMeans and commodities from pointType
    //

    Map<String, String> paymentMeanIDs = new HashMap<String/*paymentMeanID*/, String/*commodityType+"-"+deliveryType*/>();
    Map<String, String> commodityIDs = new HashMap<String/*commodityID*/, String/*commodityType+"-"+deliveryType*/>();
    if(pointType.getDebitable()){
      paymentMeanIDs.put(pointType.getPointTypeID(), CommodityType.POINT+"-"+pointTypeDeliveryType);
    }
    if(pointType.getCreditable()){
      commodityIDs.put(pointType.getPointTypeID(), CommodityType.POINT+"-"+pointTypeDeliveryType);
    }

    //
    // update paymentMean list
    //
    
    if(!paymentMeanIDs.isEmpty()){
      if(!paymentMeans.keySet().contains(pointTypeProviderID)){
        paymentMeans.put(pointTypeProviderID, paymentMeanIDs);
      }else{
        paymentMeans.get(pointTypeProviderID).putAll(paymentMeanIDs);
      }
    }

    //
    // update commodity list
    //
    
    if(!commodityIDs.isEmpty()){
      if(!commodities.keySet().contains(pointTypeProviderID)){
        commodities.put(pointTypeProviderID, commodityIDs);
      }else{
        commodities.get(pointTypeProviderID).putAll(commodityIDs);
      }
    }
    
    log.info("CommodityDeliveryManager.pointTypeActivated() DONE");
  }
  

  @Override
  public void pointTypeDeactivated(String pointTypeID)
  {
    log.info("CommodityDeliveryManager.pointTypeDeactivated() called ...");

    //
    // remove from paymentMean list
    //

    if(paymentMeans.containsKey(pointTypeProviderID)){
      paymentMeans.get(pointTypeProviderID).remove(pointTypeID);
    }
    
    //
    // remove from commodity list
    //
    
    if(commodities.containsKey(pointTypeProviderID)){
      commodities.get(pointTypeProviderID).remove(pointTypeID);
    }
    
    log.info("CommodityDeliveryManager.pointTypeDeactivated() DONE");
  }
  
}

