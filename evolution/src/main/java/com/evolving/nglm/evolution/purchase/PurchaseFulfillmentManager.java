/*****************************************************************************
*
*  PurchaseFulfillmentManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.purchase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryManager;
import com.evolving.nglm.evolution.DeliveryManagerDeclaration;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferPrice;
import com.evolving.nglm.evolution.OfferProduct;
import com.evolving.nglm.evolution.OfferSalesChannelsAndPrice;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.Product;
import com.evolving.nglm.evolution.ProductService;
import com.evolving.nglm.evolution.SalesChannel;
import com.evolving.nglm.evolution.SubscriberEvaluationRequest;
import com.evolving.nglm.evolution.SubscriberGroupEpoch;
import com.evolving.nglm.evolution.SubscriberProfile;
import com.evolving.nglm.evolution.SubscriberProfileService;
import com.evolving.nglm.evolution.DeliveryRequest.ActivityType;
import com.evolving.nglm.evolution.SubscriberProfileService.RedisSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;

public class PurchaseFulfillmentManager extends DeliveryManager implements Runnable, IDRCallback
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum PurchaseFulfillmentStatus
  {
    PENDING(10),
    PURCHASED(0),
    SYSTEM_ERROR(21),
    THIRD_PARTY_ERROR(24),
    BONUS_NOT_FOUND(101),
    OFFER_NOT_FOUND(400),
    PRODUCT_NOT_FOUND(401),
    INVALID_PRODUCT(402),
    OFFER_NOT_APPLICABLE(403),
    INSUFFICIENT_STOCK(404),
    INSUFFICIENT_BALANCE(405),
    BAD_OFFER_STATUS(406),
    PRICE_NOT_APPLICABLE(407),
    CHANNEL_DEACTIVATED(409),
    BAD_OFFER_DATES(411),
    CUSTOMER_OFFER_LIMIT_REACHED(410),
    NO_VOUCHER_CODE_AVAILABLE(408),
    UNKNOWN(999);
    private Integer externalRepresentation;
    private PurchaseFulfillmentStatus(Integer externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public Integer getReturnCode() { return externalRepresentation; }
    public static PurchaseFulfillmentStatus fromReturnCode(Integer externalRepresentation) { for (PurchaseFulfillmentStatus enumeratedValue : PurchaseFulfillmentStatus.values()) { if (enumeratedValue.getReturnCode().equals(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
  }

  /*****************************************
  *
  *  conversion method
  *
  *****************************************/

  public DeliveryStatus getPurchaseFulfillmentStatus (PurchaseFulfillmentStatus status)
  {

    switch(status)
      {
        case PENDING:
          return DeliveryStatus.Pending;
        case PURCHASED:
          return DeliveryStatus.Delivered;
        case SYSTEM_ERROR:
        case THIRD_PARTY_ERROR:
        case BONUS_NOT_FOUND:
        case OFFER_NOT_FOUND:
        case PRODUCT_NOT_FOUND:
        case INVALID_PRODUCT:
        case OFFER_NOT_APPLICABLE:
        case INSUFFICIENT_STOCK:
        case INSUFFICIENT_BALANCE:
        case BAD_OFFER_STATUS:
        case PRICE_NOT_APPLICABLE:
        case CHANNEL_DEACTIVATED:
        case BAD_OFFER_DATES:
        case CUSTOMER_OFFER_LIMIT_REACHED:
        case NO_VOUCHER_CODE_AVAILABLE:
          return DeliveryStatus.Failed;
        default:
          return DeliveryStatus.Unknown;
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

  private static final Logger log = LoggerFactory.getLogger(PurchaseFulfillmentManager.class);

  //
  //  variables
  //
  
  private int threadNumber = 5;   //TODO : make this configurable
  private final String purchase_status = "purchase_status";
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private ArrayList<Thread> threads = new ArrayList<Thread>();
  
  private SubscriberProfileService subscriberProfileService;
  private OfferService offerService;
  private ProductService productService;
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private CommodityActionManager commodityActionManager;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PurchaseFulfillmentManager(String deliveryManagerKey)
  {
    //
    //  superclass
    //
    
    super("deliverymanager-purchasefulfillment", deliveryManagerKey, Deployment.getBrokerServers(), PurchaseFulfillmentRequest.serde(), Deployment.getDeliveryManagers().get("purchaseFulfillment"));

    //
    //  plugin instanciation
    //
    
    subscriberProfileService = new RedisSubscriberProfileService(Deployment.getBrokerServers(), "example-subscriberprofileservice-245", Deployment.getSubscriberUpdateTopic(), Deployment.getRedisSentinels(), null);
    subscriberProfileService.start();
    
    offerService = new OfferService(Deployment.getBrokerServers(), "example-offerservice-673", Deployment.getOfferTopic(), false);
    offerService.start();

    productService = new ProductService(Deployment.getBrokerServers(), "example-productservice-001", Deployment.getProductTopic(), false);
    productService.start();
    
    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("example", "example-subscriberGroupReader-001", Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);

    commodityActionManager = new CommodityActionManager(this);

    //
    //  threads
    //
    
    for(int i = 0; i < threadNumber; i++)
      {
        threads.add(new Thread(this, "PurchaseFulfillmentManagerThread_"+i));
      }
    
    //
    //  startDelivery
    //
    
    startDelivery();

  }

  /*****************************************
  *
  *  class PurchaseFulfillmentRequest
  *
  *****************************************/

  public static class PurchaseFulfillmentRequest extends DeliveryRequest
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
      schemaBuilder.name("service_purchasefulfillment_request");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("offerID", Schema.STRING_SCHEMA);
      schemaBuilder.field("quantity", Schema.INT32_SCHEMA);
      schemaBuilder.field("salesChannelID", Schema.STRING_SCHEMA);
      schemaBuilder.field("return_code", Schema.INT32_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //
        
    private static ConnectSerde<PurchaseFulfillmentRequest> serde = new ConnectSerde<PurchaseFulfillmentRequest>(schema, false, PurchaseFulfillmentRequest.class, PurchaseFulfillmentRequest::pack, PurchaseFulfillmentRequest::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<PurchaseFulfillmentRequest> serde() { return serde; }
    public Schema subscriberStreamEventSchema() { return schema(); }
        
    /*****************************************
    *
    *  data
    *
    *****************************************/

    //private String deliveryRequestID;
    //private String deliveryRequestSource;
    //private String subscriberID;
    //private String deliveryType;
    //private boolean control;
  
    private String offerID;
    private int quantity;
    private String salesChannelID;
    private PurchaseFulfillmentStatus status;
    private int returnCode;
    private String returnCodeDetails;
    
    //
    //  accessors
    //

    public String getOfferID() { return offerID; }
    public int getQuantity() { return quantity; }
    public String getSalesChannelID() { return salesChannelID; }
    public PurchaseFulfillmentStatus getStatus() { return status; }
    public int getReturnCode() { return returnCode; }
    public String getReturnCodeDetails() { return returnCodeDetails; }

    //
    //  setters
    //

    public void setStatus(PurchaseFulfillmentStatus status) { this.status = status; }
    public void setReturnCode(Integer returnCode) { this.returnCode = returnCode; }
    public void setReturnCodeDetails(String returnCodeDetails) { this.returnCodeDetails = returnCodeDetails; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public PurchaseFulfillmentRequest(EvolutionEventContext context, String deliveryRequestSource, String offerID, int quantity, String salesChannelID)
    {
      super(context, "purchaseFulfillment", deliveryRequestSource);
      this.offerID = offerID;
      this.quantity = quantity;
      this.salesChannelID = salesChannelID;
      this.status = PurchaseFulfillmentStatus.PENDING;
      this.returnCode = PurchaseFulfillmentStatus.PENDING.getReturnCode();
      this.returnCodeDetails = "";
    }

    /*****************************************
    *
    *  constructor -- external
    *
    *****************************************/

    public PurchaseFulfillmentRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
    {
      super(jsonRoot);
      this.offerID = JSONUtilities.decodeString(jsonRoot, "offerID", true);
      this.quantity = JSONUtilities.decodeInteger(jsonRoot, "quantity", true);
      this.salesChannelID = JSONUtilities.decodeString(jsonRoot, "salesChannelID", true);
      this.status = PurchaseFulfillmentStatus.PENDING;
      this.returnCode = PurchaseFulfillmentStatus.PENDING.getReturnCode();
      this.returnCodeDetails = "";
    }

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private PurchaseFulfillmentRequest(SchemaAndValue schemaAndValue, String offerID, int quantity, String salesChannelID, PurchaseFulfillmentStatus status)
    {
      super(schemaAndValue);
      this.offerID = offerID;
      this.quantity = quantity;
      this.salesChannelID = salesChannelID;
      this.status = status;
      this.returnCode = status.getReturnCode();
    }

    /*****************************************
    *
    *  constructor -- copy
    *
    *****************************************/

    private PurchaseFulfillmentRequest(PurchaseFulfillmentRequest purchaseFulfillmentRequest)
    {
      super(purchaseFulfillmentRequest);
      this.offerID = purchaseFulfillmentRequest.getOfferID();
      this.quantity = purchaseFulfillmentRequest.getQuantity();
      this.salesChannelID = purchaseFulfillmentRequest.getSalesChannelID();
      this.returnCode = purchaseFulfillmentRequest.getReturnCode();
      this.status = purchaseFulfillmentRequest.getStatus();
      this.returnCodeDetails = purchaseFulfillmentRequest.getReturnCodeDetails();
    }

    /*****************************************
    *
    *  copy
    *
    *****************************************/

    public PurchaseFulfillmentRequest copy()
    {
      return new PurchaseFulfillmentRequest(this);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      PurchaseFulfillmentRequest purchaseFulfillmentRequest = (PurchaseFulfillmentRequest) value;
      Struct struct = new Struct(schema);
      packCommon(struct, purchaseFulfillmentRequest);
      struct.put("offerID", purchaseFulfillmentRequest.getOfferID());
      struct.put("quantity", purchaseFulfillmentRequest.getQuantity());
      struct.put("salesChannelID", purchaseFulfillmentRequest.getSalesChannelID());
      struct.put("return_code", purchaseFulfillmentRequest.getReturnCode());
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

    public static PurchaseFulfillmentRequest unpack(SchemaAndValue schemaAndValue)
    {
      //
      //  data
      //

      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

      //  unpack
      //

      Struct valueStruct = (Struct) value;
      String offerID = valueStruct.getString("offerID");
      int quantity = valueStruct.getInt32("quantity");
      String salesChannelID = valueStruct.getString("salesChannelID");
      Integer returnCode = valueStruct.getInt32("return_code");
      PurchaseFulfillmentStatus status = PurchaseFulfillmentStatus.fromReturnCode(returnCode);

      //
      //  return
      //

      return new PurchaseFulfillmentRequest(schemaAndValue, offerID, quantity, salesChannelID, status);
    }

//    /*****************************************
//    *  
//    *  to/from JSONObject
//    *
//    *****************************************/
//
//    @Override
//    public JSONObject getJSONRepresentation(){
//      Map<String, Object> data = new HashMap<String, Object>();
//      
//      //DeliveryRequest fields
//      data.put("deliveryRequestID", this.getDeliveryRequestID());
//      data.put("deliveryRequestSource", this.getDeliveryRequestSource());
//      data.put("subscriberID", this.getSubscriberID());
//      data.put("deliveryPartition", this.getDeliveryPartition());
//      data.put("retries", this.getRetries());
//      data.put("timeout", this.getTimeout());
//      data.put("correlator", this.getCorrelator());
//      data.put("control", this.getControl());
//      data.put("deliveryType", this.getDeliveryType());
//      data.put("deliveryStatus", this.getDeliveryStatus().toString());
//      data.put("deliveryDate", this.getDeliveryDate());
//      data.put("diplomaticBriefcase", this.getDiplomaticBriefcase());
//      
//      //PurchaseFulfillmentRequest fields
//      data.put("offerID", this.getOfferID());
//      data.put("quantity",  this.getQuantity());
//      data.put("salesChannelID", this.getSalesChannelID());
//      
//      return JSONUtilities.encodeObject(data);
//    }
    
    /*****************************************
    *  
    *  toString
    *
    *****************************************/

    public String toString()
    {
      StringBuilder b = new StringBuilder();
      b.append("PurchaseFulfillmentRequest:{");
      b.append(super.toStringFields());
      b.append("," + getSubscriberID());
      b.append("," + offerID);
      b.append("," + quantity);
      b.append("," + salesChannelID);
      b.append("," + returnCode);
      b.append("," + returnCodeDetails);
      b.append("}");
      return b.toString();
    }
    
    @Override public Integer getActivityType() { return ActivityType.ODR.getExternalRepresentation(); }
    
    /****************************************
    *
    *  presentation utilities
    *
    ****************************************/
    
    @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap)
    {
      guiPresentationMap.put(CUSTOMERID, getSubscriberID());
      guiPresentationMap.put(PURCHASEID, getEventID());
      guiPresentationMap.put(OFFERID, getOfferID());
      guiPresentationMap.put(OFFERQTY, getQuantity());
      guiPresentationMap.put(SALESCHANNELID, getSalesChannelID());
      guiPresentationMap.put(SALESCHANNEL, Deployment.getSalesChannels().get(getSalesChannelID()).getName());
      guiPresentationMap.put(MODULEID, getModuleID());
      guiPresentationMap.put(MODULENAME, Module.fromModuleId(getModuleID()).toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(ORIGIN, getDeliveryRequestSource());
      guiPresentationMap.put(RETURNCODE, "TO DO:");
      guiPresentationMap.put(RETURNCODEDETAILS, "TO DO:");
      guiPresentationMap.put(VOUCHERCODE, "");
      guiPresentationMap.put(VOUCHERPARTNERID, "");
    }
    
    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap) 
    {
      thirdPartyPresentationMap.put(CUSTOMERID, getSubscriberID());
      thirdPartyPresentationMap.put(PURCHASEID, getEventID());
      thirdPartyPresentationMap.put(OFFERID, getOfferID());
      thirdPartyPresentationMap.put(OFFERQTY, getQuantity());
      thirdPartyPresentationMap.put(SALESCHANNELID, getSalesChannelID());
      thirdPartyPresentationMap.put(SALESCHANNEL, Deployment.getSalesChannels().get(getSalesChannelID()).getName());
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, Module.fromModuleId(getModuleID()).toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(ORIGIN, getDeliveryRequestSource());
      thirdPartyPresentationMap.put(RETURNCODE, "TO DO:");
      thirdPartyPresentationMap.put(RETURNCODEDETAILS, "TO DO:");
      thirdPartyPresentationMap.put(VOUCHERCODE, "");
      thirdPartyPresentationMap.put(VOUCHERPARTNERID, "");
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
        PurchaseFulfillmentRequest purchaseRequest = ((PurchaseFulfillmentRequest)deliveryRequest);

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
        
        Date now = SystemTime.getCurrentTime();
        String offerID = purchaseRequest.getOfferID();
        int quantity = purchaseRequest.getQuantity();
        String subscriberID = purchaseRequest.getSubscriberID();
        String salesChannelID = purchaseRequest.getSalesChannelID();
        PurchaseRequestStatus purchaseStatus = new PurchaseRequestStatus(correlator, offerID, subscriberID, quantity, salesChannelID);
        
        //
        // Get quantity
        //
        
        if(quantity < 1){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : bad field value for quantity");
          submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "bad field value for quantity"/* TODO : use right message here */);
          return;
        }
        
        //
        // Get customer
        //
        
        if(subscriberID == null){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : bad field value for subscriberID");
          submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "bad field value for subscriberID"/* TODO : use right message here */);
          return;
        }
        SubscriberProfile subscriberProfile = null;
        try{
          subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID);
          if(subscriberProfile == null){
            log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : subscriber " + subscriberID + " not found");
            submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "subscriber " + subscriberID + " not found"/* TODO : use right message here */);
            return;
          }else{
            log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : subscriber " + subscriberID + " found ("+subscriberProfile+")");
          }
        }catch (SubscriberProfileServiceException e) {
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : subscriberService not available");
          submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "subscriberService not available"/* TODO : use right message here */);
          return;
        }

        //
        // Get offer
        //
        
        if(offerID == null){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : bad field value for offerID");
          submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "bad field value for offerID"/* TODO : use right message here */);
          return;
        }
        Offer offer = offerService.getActiveOffer(offerID, now);
        if(offer == null){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : offer " + offerID + " not found");
          submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "offer " + offerID + " not found"/* TODO : use right message here */);
          return;
        }else{
          log.debug(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : offer " + offerID + " found ("+offer+")");
        }

        //
        // Get sales channel
        //

        SalesChannel salesChannel = Deployment.getSalesChannels().get(salesChannelID);
        if(salesChannel == null){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : salesChannel " + salesChannelID + " not found");
          submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "salesChannel " + salesChannelID + " not found"/* TODO : use right message here */);
          return;
        }else{
          log.debug(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : salesChannel " + salesChannelID + " found ("+salesChannel+")");
        }

        //
        // Get offer price
        //
        
        OfferPrice offerPrice = null;
        Boolean priceFound = false;
        for(OfferSalesChannelsAndPrice offerSalesChannelsAndPrice : offer.getOfferSalesChannelsAndPrices()){
          if(offerSalesChannelsAndPrice.getSalesChannelIDs() != null && offerSalesChannelsAndPrice.getSalesChannelIDs().contains(salesChannel.getID())){
            offerPrice = offerSalesChannelsAndPrice.getPrice();
            priceFound = true;
            log.debug(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.checkOffer (offer, subscriberProfile) : offer price for sales channel "+salesChannel.getID()+" found ("+offerPrice.getAmount()+" "+offerPrice.getPaymentMeanID()+")");
            break;
          }
        }
        if(!priceFound){ //need this boolean since price can be null (if offer is free)
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : offer price for sales channel " + salesChannelID + " not found");
          submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "offer price for sales channel " + salesChannelID + " not found"/* TODO : use right message here */);
          return;
        }
        purchaseStatus.addPaymentToBeDebited(offerPrice);

        /*****************************************
        *
        *  Check offer, subscriber, ...
        *
        *****************************************/
        
        //
        // check offer is active (should be since we used 'getActiveOffer' ...)
        //

        if(!offerService.isActiveOffer(offer, now)){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.checkOffer (offer, subscriberProfile) : offer " + offer.getOfferID() + " not active (date = "+now+")");
          submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "offer " + offer.getOfferID() + " not active (date = "+now+")"/* TODO : use right message here */);
          return;
        }
        purchaseStatus.addOfferStockToBeDebited(offer.getOfferID());

        //
        // check offer content
        //
        
        for(OfferProduct offerProduct : offer.getOfferProducts()){
          Product product = productService.getActiveProduct(offerProduct.getProductID(), now);
          if(product == null){
            log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.checkOffer (offer, subscriberProfile) : product with ID " + offerProduct.getProductID() + " not found or not active (date = "+now+")");
            submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "product with ID " + offerProduct.getProductID() + " not found or not active (date = "+now+")"/* TODO : use right message here */);
            return;
          }else{
            purchaseStatus.addProductStockToBeDebited(offerProduct.getProductID());
          }
        }
        
        //
        // check offer criteria (for the specific subscriber)
        //

        SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, now);
        if(!offer.evaluateProfileCriteria(evaluationRequest)){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.checkOffer (offer, subscriberProfile) : criteria of offer "+offer.getOfferID()+" not valid for subscriber "+subscriberProfile.getSubscriberID()+" (date = "+now+")");
          submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "criteria of offer "+offer.getOfferID()+" not valid for subscriber "+subscriberProfile.getSubscriberID()+" (date = "+now+")"/* TODO : use right message here */);
          return;
        }
        
        //TODO : still to be done :
        //    - sales channel validity
        //    - checkSubscriberLimit (decrement subscriber offer remaining counter)

        /*****************************************
        *
        *  Proceed with the purchase
        *
        *****************************************/

        proceedPurchase(purchaseStatus);
        
      }
  }

  /*****************************************
  *
  *  CorrelatorUpdate
  *
  *****************************************/

  private void submitCorrelatorUpdate(PurchaseRequestStatus purchaseStatus, DeliveryStatus deliveryStatus, int statusCode, String statusMessage){
    purchaseStatus.setDeliveryStatus(deliveryStatus);
    purchaseStatus.setDeliveryStatusCode(statusCode);
    purchaseStatus.setDeliveryStatusMessage(statusMessage);
    submitCorrelatorUpdate(purchaseStatus);
  }
  
  private void submitCorrelatorUpdate(PurchaseRequestStatus purchaseStatus){
    HashMap<String,Object> correlatorUpdateRecord = new HashMap<String,Object>();
    correlatorUpdateRecord.put(purchase_status, PurchaseRequestStatus.toJSON(purchaseStatus));
    JSONObject correlatorUpdate = JSONUtilities.encodeObject(correlatorUpdateRecord);
    submitCorrelatorUpdate(purchaseStatus.getCorrelator(), correlatorUpdate);
  }

  @Override protected void processCorrelatorUpdate(DeliveryRequest deliveryRequest, JSONObject correlatorUpdate)
  {
    log.info("PurchaseFulfillmentManager.processCorrelatorUpdate("+deliveryRequest.getDeliveryRequestID()+", "+correlatorUpdate+") : called ...");
    
    String purchaseStatusAsString = JSONUtilities.decodeString(correlatorUpdate, purchase_status);
    if(purchaseStatusAsString != null){
      PurchaseRequestStatus purchaseStatus = PurchaseRequestStatus.fromJSON(purchaseStatusAsString);
      deliveryRequest.setDeliveryStatus(purchaseStatus.getDeliveryStatus());
      //purchaseStatus.getDeliveryStatusCode();
      //purchaseStatus.getDeliveryStatusMessage();
      deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
      completeRequest(deliveryRequest);
    }else{
      log.warn("PurchaseFulfillmentManager.processCorrelatorUpdate() : ERROR : missing purchase status data");
    }

    log.debug("PurchaseFulfillmentManager.processCorrelatorUpdate("+deliveryRequest.getDeliveryRequestID()+", "+correlatorUpdate+") : DONE");

  }

  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  @Override protected void shutdown()
  {
    log.info("PurchaseFulfillmentManager: shutdown");
  }
  
  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    log.info("PurchaseFulfillmentManager: recieved " + args.length + " args :");
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
    
    log.info("PurchaseFulfillmentManager: Configuration " + Deployment.getDeliveryManagers());

    
    PurchaseFulfillmentManager manager = new PurchaseFulfillmentManager(deliveryManagerKey);

    //
    //  run
    //

    manager.run();
  }
  
  /*****************************************
  *
  *  proceed with purchase
  *
  *****************************************/

  private void proceedPurchase(PurchaseRequestStatus purchaseStatus){
    //Change to return PurchaseManagerStatus? 
    //
    // debit all product stocks
    //
    
    if(purchaseStatus.getProductStockToBeDebited() != null && !purchaseStatus.getProductStockToBeDebited().isEmpty()){
      String productID = purchaseStatus.getProductStockToBeDebited().remove(0);
      purchaseStatus.setProductStockBeingDebited(productID);
      debitProductStock(purchaseStatus);
      return;
    }

    //
    // debit all offer stocks
    //
    
    if(purchaseStatus.getOfferStockToBeDebited() != null && !purchaseStatus.getOfferStockToBeDebited().isEmpty()){
      String offerID = purchaseStatus.getOfferStockToBeDebited().remove(0);
      purchaseStatus.setOfferStockBeingDebited(offerID);
      debitOfferStock(purchaseStatus);
      return;
    }

    //
    // make all payments
    //
    
    if(purchaseStatus.getPaymentToBeDebited() != null && !purchaseStatus.getPaymentToBeDebited().isEmpty()){
      OfferPrice offerPrice = purchaseStatus.getPaymentToBeDebited().remove(0);
      if(offerPrice == null){// => offer is free
        purchaseStatus.addPaymentDebited(offerPrice);
      }else{
        purchaseStatus.setPaymentBeingDebited(offerPrice);
        if(!makePayment(purchaseStatus)){
          log.warn("PurchaseFulfillmentManager.proceedPurchase(offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") : could not make payment of price "+offerPrice.getPaymentMeanID()+" => initiate ROLLBACK");
          purchaseStatus.setPaymentDebitFailed(offerPrice);
          purchaseStatus.setPaymentBeingDebited(null);
          proceedRollback(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "proceedPurchase : could not make payment of price "+offerPrice.getPaymentMeanID()/* TODO : use right message here */);
        }
        return;
      }
    }

    //TODO SCH : uncomment when CreditCommodities will be implemented ...
    //if(purchaseStatus.getCommoditiesToBeCredited() != null && !purchaseStatus.getCommoditiesToBeCredited().isEmpty()){
    //  String commodityID = purchaseStatus.getCommoditiesToBeCredited().remove(0);
    //  purchaseStatus.setCommodityBeingCredited(commodityID);
    //  creditCommodities(purchaseStatus);
    //  return;
    //}

    //TODO SCH : uncomment when NotifyProductProvider will be implemented ...
    //if(purchaseStatus.getProviderToBeNotifyed() != null && !purchaseStatus.getProviderToBeNotifyed().isEmpty()){
    //  String providerID = purchaseStatus.getProviderToBeNotifyed().remove(0);
    //  purchaseStatus.setProviderBeingNotifyed(providerID);
    //  notifyProductProvider(purchaseStatus);
    //  return;
    //}

    // update and return response (succeed)
    submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Delivered, 0/* TODO : use right code here */, "Success"/* TODO : use right message here */);
    
  }

  /*****************************************
  *
  *  steps of the purchase
  *
  *****************************************/

  private void debitProductStock(PurchaseRequestStatus purchaseStatus){
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitProductStock (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
    //==================================================================
    //TODO SCH : make real call to debit product stock
    //    / \     
    //   / | \     ATTENTION : purchaseRequest.getProductID() x purchaseRequest.getQuantity()
    //  /_____\
    //==================================================================
    purchaseStatus.addProductStockDebited(purchaseStatus.getProductStockBeingDebited());
    purchaseStatus.setProductStockBeingDebited(null);
    proceedPurchase(purchaseStatus);
    //==================================================================
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitProductStock (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") DONE");
  }

  private void debitOfferStock(PurchaseRequestStatus purchaseStatus){
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitOfferStock (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
    //==================================================================
    // TODO SCH : make real call to debit offer stock
    //    / \     
    //   / | \     ATTENTION : purchaseRequest.getOfferID() x purchaseRequest.getQuantity()
    //  /_____\
    //==================================================================
    purchaseStatus.addOfferStockDebited(purchaseStatus.getOfferStockBeingDebited());
    purchaseStatus.setOfferStockBeingDebited(null);
    proceedPurchase(purchaseStatus);
    //==================================================================
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitOfferStock (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") DONE");
  }

  private boolean makePayment(PurchaseRequestStatus purchaseStatus){
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.makePayment (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
    OfferPrice offerPrice = purchaseStatus.getPaymentBeingDebited();
    boolean paymentDone =  commodityActionManager.makePayment(PurchaseRequestStatus.toJSON(purchaseStatus), purchaseStatus.getCorrelator(), purchaseStatus.getSubscriberID(), offerPrice.getProviderID(), offerPrice.getPaymentMeanID(), offerPrice.getAmount() * purchaseStatus.getQuantity(), this);
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.makePayment (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") DONE");
    return paymentDone;
  }
  
  private void creditCommodities(PurchaseRequestStatus purchaseStatus){
    log.debug(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.creditCommodities (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
    Date now = SystemTime.getCurrentTime();
    
    //GET OFFER
    Offer offer = offerService.getActiveOffer(purchaseStatus.getOfferID(), now);
    if(offer == null){ //should not happen
      log.warn(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.creditCommodities (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") : could not get offer content => initiate ROLLBACK");
      proceedRollback(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "could not get offer with ID "+purchaseStatus.getOfferID() /* TODO : use right message here */);
      return;
    }

    //GET PRODUCTS
    List<Product> products = new ArrayList<Product>();
    for(OfferProduct offerProduct : offer.getOfferProducts()){
      Product product = productService.getActiveProduct(offerProduct.getProductID(), now);
      if(product != null){
        products.add(product);
      }else{ //should not happen
        log.warn(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.creditCommodities (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") : could not get product with ID '"+offerProduct.getProductID()+"' => initiate ROLLBACK");
        proceedRollback(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "could not get product with ID "+offerProduct.getProductID() /* TODO : use right message here */);
        return;
      }
    }
    
    for(Product product : products){
      String supplierID = product.getSupplierID();
      String productID = product.getProductID();
      //==================================================================
      // TODO SCH : make real call to credit commodities
      //    / \     
      //   / | \     ATTENTION : x purchaseRequest.getQuantity()
      //  /_____\
      //==================================================================
    }
    
    log.debug(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.creditCommodities (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") DONE");
  }

  private void notifyProductProvider(PurchaseRequestStatus purchaseStatus){
    log.debug(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.notifyProductProvider (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
    //TODO SCH : make real call to notify providers
    log.debug(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.notifyProductProvider (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") DONE");
  }
  
  /*****************************************
  *
  *  proceed with rollback
  *
  *****************************************/

  private void proceedRollback(PurchaseRequestStatus purchaseStatus, DeliveryStatus deliveryStatus, Integer statusCode, String statusMessage){

    //
    // update purchaseStatus
    //
    
    purchaseStatus.setRollbackInProgress(true);
    if(deliveryStatus != null){purchaseStatus.setDeliveryStatus(deliveryStatus);}
    if(statusCode != null){purchaseStatus.setDeliveryStatusCode(statusCode);}
    if(statusMessage != null){purchaseStatus.setDeliveryStatusMessage(statusMessage);}

    //
    // debit all product stocks
    //
    
    if(purchaseStatus.getProductStockDebited() != null && !purchaseStatus.getProductStockDebited().isEmpty()){
      String productID = purchaseStatus.getProductStockDebited().remove(0);
      purchaseStatus.setProductStockBeingRollbacked(productID);
      creditProductStock(purchaseStatus);
      return;
    }

    //
    // debit all offer stocks
    //
    
    if(purchaseStatus.getOfferStockDebited() != null && !purchaseStatus.getOfferStockDebited().isEmpty()){
      String offerID = purchaseStatus.getOfferStockDebited().remove(0);
      purchaseStatus.setOfferStockBeingRollbacked(offerID);
      creditOfferStock(purchaseStatus);
      return;
    }

    //
    // make all payments
    //
    
    if(purchaseStatus.getPaymentDebited() != null && !purchaseStatus.getPaymentDebited().isEmpty()){
      OfferPrice offerPrice = purchaseStatus.getPaymentDebited().remove(0);
      if(offerPrice == null){// => offer is free
        purchaseStatus.addPaymentRollbacked(offerPrice);
      }else{
        purchaseStatus.setPaymentBeingRollbacked(offerPrice);
        if(!rollbackPayment(purchaseStatus)){
          log.warn("PurchaseFulfillmentManager.proceedRollback(offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") : could not rollback payment of price "+offerPrice.getPaymentMeanID());
          purchaseStatus.addPaymentRollbackFailed(offerPrice);
          purchaseStatus.setPaymentBeingRollbacked(null);
        }else{
          return;
        }
      }
      proceedRollback(purchaseStatus, null, null, null);
      return;
    }

    //TODO SCH : uncomment when RollbackCommodities will be implemented ...
    //if(purchaseStatus.getCommoditiesToRollback() != null && !purchaseStatus.getCommoditiesToRollback().isEmpty()){
    //  String commodityID = purchaseStatus.getCommoditiesToRollback().remove(0);
    //  purchaseStatus.setCommodityBeingRollbacked(commodityID);
    //  rollbackCommodities(purchaseStatus);
    //  return;
    //}

    //TODO SCH : uncomment when RollbackNotifyProductProvider will be implemented ...
    //if(purchaseStatus.getProviderToBeCancelled() != null && !purchaseStatus.getProviderToBeCancelled().isEmpty()){
    //  String providerID = purchaseStatus.getProviderToBeCancelled().remove(0);
    //  purchaseStatus.setProviderBeingCancelled(providerID);
    //  cancelNotifyProductProvider(purchaseStatus);
    //  return;
    //}

    // update and return response (succeed)
    submitCorrelatorUpdate(purchaseStatus);
  }

  /*****************************************
  *
  *  steps of the rollback
  *
  *****************************************/

  private void creditProductStock(PurchaseRequestStatus purchaseStatus){
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.creditProductStock (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
    //==================================================================
    //TODO SCH : make real call to credit product stock
    //    / \     
    //   / | \     ATTENTION : purchaseRequest.getProductID() x purchaseRequest.getQuantity()
    //  /_____\
    //==================================================================
    purchaseStatus.addProductStockRollbacked(purchaseStatus.getProductStockBeingDebited());
    purchaseStatus.setProductStockBeingRollbacked(null);
    proceedRollback(purchaseStatus, null, null, null);
    //==================================================================
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.creditProductStock (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") DONE");
  }

  private void creditOfferStock(PurchaseRequestStatus purchaseStatus){
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.creditOfferStock (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
    //==================================================================
    // TODO SCH : make real call to debit offer stock
    //    / \     
    //   / | \     ATTENTION : purchaseRequest.getOfferID() x purchaseRequest.getQuantity()
    //  /_____\
    //==================================================================
    purchaseStatus.addOfferStockRollbacked(purchaseStatus.getOfferStockBeingRollbacked());
    purchaseStatus.setOfferStockBeingRollbacked(null);
    proceedRollback(purchaseStatus, null, null, null);
    //==================================================================
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.creditOfferStock (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") DONE");
  }

  private boolean rollbackPayment(PurchaseRequestStatus purchaseStatus){
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.rollbackPayment (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
    OfferPrice offerPrice = purchaseStatus.getPaymentBeingRollbacked();
    boolean paymentDone =  commodityActionManager.creditCommodity(PurchaseRequestStatus.toJSON(purchaseStatus), purchaseStatus.getCorrelator(), purchaseStatus.getSubscriberID(), offerPrice.getProviderID(), offerPrice.getPaymentMeanID(), offerPrice.getAmount() * purchaseStatus.getQuantity(), this);
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.rollbackPayment (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") DONE");
    return paymentDone;
  }
  
//  - rollbackBonus (rolling back offer content)
//  - rollbackProductProviderAPI
//  - rollbackVoucher ( OUT OF SCOPE !!! !!! !!! )
//  - rollbackPurchase (decrement purchase counters & stats)
  
  /*****************************************
  *
  *  IDRCallback.onDRResponse(...)
  *
  *****************************************/

  @Override
  public void onDRResponse(DeliveryRequest response)
  {
    
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.onDRResponse(...)");

    // ------------------------------------
    // Getting initial request status
    // ------------------------------------
    
    log.debug(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.onDRResponse(...) : getting purchase status ");
    if(response.getDiplomaticBriefcase() == null || response.getDiplomaticBriefcase().get(originalRequest) == null || response.getDiplomaticBriefcase().get(originalRequest).isEmpty()){
      log.warn(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.onDRResponse(response) : can not get purchase status => ignore this response");
      return;
    }
    PurchaseRequestStatus purchaseStatus = PurchaseRequestStatus.fromJSON(response.getDiplomaticBriefcase().get(originalRequest));
    log.debug(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.onDRResponse(...) : getting purchase status DONE : "+purchaseStatus);
    
    // ------------------------------------
    // Handling response
    // ------------------------------------

    DeliveryStatus responseDeliveryStatus = response.getDeliveryStatus();
    boolean isRollbackInProgress = purchaseStatus.getRollbackInProgress();
    if(isRollbackInProgress){

      //
      // processing rollback
      //
      
      //  ---  check product stocks  ---
      if(purchaseStatus.getProductStockBeingRollbacked() != null && !purchaseStatus.getProductStockBeingRollbacked().isEmpty()){
        String productStockBeingRollbacked = purchaseStatus.getProductStockBeingRollbacked();
        if(responseDeliveryStatus.equals(DeliveryStatus.Delivered)){
          purchaseStatus.addProductStockRollbacked(productStockBeingRollbacked);
          purchaseStatus.setProductStockBeingRollbacked(null);
          proceedRollback(purchaseStatus, null, null, null);
          return;
        }else{
          //responseDeliveryStatus is one of those : Pending, FailedRetry, Delivered, Indeterminate, Failed, FailedTimeout, Unknown
          purchaseStatus.addProductStockRollbackFailed(productStockBeingRollbacked);
          purchaseStatus.setProductStockBeingRollbacked(null);
          proceedRollback(purchaseStatus, null, null, null);
          return;
        }
      }

      //  ---  check offer stock  ---
      if(purchaseStatus.getOfferStockBeingRollbacked() != null && !purchaseStatus.getOfferStockBeingRollbacked().isEmpty()){
        String offerStockBeingRollbacked = purchaseStatus.getOfferStockBeingRollbacked();
        if(responseDeliveryStatus.equals(DeliveryStatus.Delivered)){
          purchaseStatus.addOfferStockRollbacked(offerStockBeingRollbacked);
          purchaseStatus.setOfferStockBeingRollbacked(null);
          proceedRollback(purchaseStatus, null, null, null);
          return;
        }else{
          //responseDeliveryStatus is one of those : Pending, FailedRetry, Delivered, Indeterminate, Failed, FailedTimeout, Unknown
          purchaseStatus.addOfferStockRollbackFailed(offerStockBeingRollbacked);
          purchaseStatus.setOfferStockBeingRollbacked(null);
          proceedRollback(purchaseStatus, null, null, null);
          return;
        }
      }

      //  ---  check payment  ---
      if(purchaseStatus.getPaymentBeingRollbacked() != null){
        OfferPrice offerPrice = purchaseStatus.getPaymentBeingRollbacked();
        if(responseDeliveryStatus.equals(DeliveryStatus.Delivered)){
          purchaseStatus.addPaymentRollbacked(offerPrice);
          purchaseStatus.setPaymentBeingRollbacked(null);
          proceedRollback(purchaseStatus, null, null, null);
          return;
        }else{
          //responseDeliveryStatus is one of those : Pending, FailedRetry, Delivered, Indeterminate, Failed, FailedTimeout, Unknown
          purchaseStatus.addPaymentRollbackFailed(offerPrice);
          purchaseStatus.setPaymentBeingRollbacked(null);
          proceedRollback(purchaseStatus, null, null, null);
          return;
        }
      }
      
      //  ---  check credit offer content  ---
      //TODO : complete when CreditCommodities will be implemented ...

      //  ---  check provider notifications  ---
      //TODO : complete when NotifyProductProvider will be implemented ...

      // ROLLBACK IS COMPLETE => update and return failed response
      log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.onDRResponse(...) : ROLLBACK is complete => calling submitCorrelatorUpdate("+purchaseStatus.getCorrelator()+", "+purchaseStatus.getDeliveryStatus()+", "+purchaseStatus.getDeliveryStatusCode()+", "+purchaseStatus.getDeliveryStatusMessage()+") ...");
      submitCorrelatorUpdate(purchaseStatus);
      log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.onDRResponse(...) : ROLLBACK is complete => calling submitCorrelatorUpdate("+purchaseStatus.getCorrelator()+", "+purchaseStatus.getDeliveryStatus()+", "+purchaseStatus.getDeliveryStatusCode()+", "+purchaseStatus.getDeliveryStatusMessage()+") DONE");

    }else{
      
      //
      // processing purchase
      //
      
      //  ---  check product stocks  ---
      if(purchaseStatus.getProductStockBeingDebited() != null && !purchaseStatus.getProductStockBeingDebited().isEmpty()){
        String productStockBeingDebited = purchaseStatus.getProductStockBeingDebited();
        if(responseDeliveryStatus.equals(DeliveryStatus.Delivered)){
          purchaseStatus.addProductStockDebited(productStockBeingDebited);
          purchaseStatus.setProductStockBeingDebited(null);
          proceedPurchase(purchaseStatus);
          return;
        }else{
          //responseDeliveryStatus is one of those : Pending, FailedRetry, Delivered, Indeterminate, Failed, FailedTimeout, Unknown
          purchaseStatus.setProductStockDebitFailed(productStockBeingDebited);
          purchaseStatus.setProductStockBeingDebited(null);
          proceedRollback(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "could not debit stock of product "+productStockBeingDebited /* TODO : use right message here */);
          return;
        }
      }

      //  ---  check offer stock  ---
      if(purchaseStatus.getOfferStockBeingDebited() != null && !purchaseStatus.getOfferStockBeingDebited().isEmpty()){
        String offerStockBeingDebited = purchaseStatus.getOfferStockBeingDebited();
        if(responseDeliveryStatus.equals(DeliveryStatus.Delivered)){
          purchaseStatus.addOfferStockDebited(offerStockBeingDebited);
          purchaseStatus.setOfferStockBeingDebited(null);
          proceedPurchase(purchaseStatus);
          return;
        }else{
          //responseDeliveryStatus is one of those : Pending, FailedRetry, Delivered, Indeterminate, Failed, FailedTimeout, Unknown
          purchaseStatus.setOfferStockDebitFailed(offerStockBeingDebited);
          purchaseStatus.setOfferStockBeingDebited(null);
          proceedRollback(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "could not debit stock of offer "+offerStockBeingDebited /* TODO : use right message here */);
          return;
        }
      }

      //  ---  check payment  ---
      if(purchaseStatus.getPaymentBeingDebited() != null){
        OfferPrice offerPrice = purchaseStatus.getPaymentBeingDebited();
        if(responseDeliveryStatus.equals(DeliveryStatus.Delivered)){
          purchaseStatus.addPaymentDebited(offerPrice);
          purchaseStatus.setPaymentBeingDebited(null);
          
          
          
          //=========================================================================================
          //TODO :   | TODO : REMOVE THIS   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!
          //        \|/
          //=========================================================================================
          log.error("SCH : ADDING FAKE PRICE TO TEST ROLLBACK PROCESS");
          OfferPrice newOfferPrice = new OfferPrice(null, 15, "currency1", "provider1", "paymentMean1");
          purchaseStatus.addPaymentToBeDebited(newOfferPrice);
          //=========================================================================================
          //        /|\
          //TODO :   | TODO : REMOVE THIS   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!
          //=========================================================================================

          
          
          proceedPurchase(purchaseStatus);
          return;
        }else{
          //responseDeliveryStatus is one of those : Pending, FailedRetry, Delivered, Indeterminate, Failed, FailedTimeout, Unknown
          purchaseStatus.setPaymentDebitFailed(offerPrice);
          purchaseStatus.setPaymentBeingDebited(null);
          proceedRollback(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "onDRResponse : could not make payment of price "+offerPrice /* TODO : use right message here */);
          return;
        }
      }
      
      //  ---  check credit offer content  ---
      //TODO SCH : uncomment and complete when CreditCommodities will be implemented ...
      //if(purchaseStatus.getCommoditiesToBeCredited() != null && !purchaseStatus.getCommoditiesToBeCredited().isEmpty()){
      //  String commodityID = purchaseStatus.getCommoditiesToBeCredited().remove(0);
      //  purchaseStatus.setCommodityBeingCredited(commodityID);
      //  creditCommodities(purchaseStatus);
      //  return;
      //}

      //  ---  check provider notifications  ---
      //TODO SCH : uncomment and complete when NotifyProductProvider will be implemented ...
      //if(purchaseStatus.getProviderToBeNotifyed() != null && !purchaseStatus.getProviderToBeNotifyed().isEmpty()){
      //  String providerID = purchaseStatus.getProviderToBeNotifyed().remove(0);
      //  purchaseStatus.setProviderBeingNotifyed(providerID);
      //  notifyProductProvider(purchaseStatus);
      //  return;
      //}

      // EVERYTHING IS OK => update and return successful response (DeliveryStatus.Delivered)
      log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.onDRResponse(...) : purchase is complete => calling submitCorrelatorUpdate("+purchaseStatus.getCorrelator()+", DeliveryStatus.Delivered, 0, Success) ...");
      submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Delivered, 0/* TODO : use right code here */, "Success" /* TODO : use right message here */);
      log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.onDRResponse(...) : purchase is complete => calling submitCorrelatorUpdate("+purchaseStatus.getCorrelator()+", DeliveryStatus.Delivered, 0, Success) DONE");
      
    }

    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.onDRResponse(...) DONE");
  }
  
  /*****************************************
  *
  *  IDRCallback.getIdentifier()
  *
  *****************************************/

  @Override
  public String getIdentifier(){
    return "PurchaseFulfillmentManager";
  }

  /*****************************************
  *
  *  class PurchaseRequestStatus
  *
  *****************************************/

  public static class PurchaseRequestStatus
  {

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String correlator = null;
    private String offerID = null;
    private String subscriberID = null;
    private int quantity = -1;
    private String salesChannelID = null;
    
    private boolean rollbackInProgress = false;
    
    private DeliveryStatus deliveryStatus = DeliveryStatus.Unknown;
    private int deliveryStatusCode = -1;
    private String deliveryStatusMessage = null;
    
    private List<String> productStockToBeDebited = null;
    private String productStockBeingDebited = null;
    private List<String> productStockDebited = null;
    private String productStockDebitFailed = null;
    private String productStockBeingRollbacked = null;
    private List<String> productStockRollbacked = null;
    private List<String> productStockRollbackFailed = null;
    
    private List<String> offerStockToBeDebited = null;
    private String offerStockBeingDebited = null;
    private List<String> offerStockDebited = null;
    private String offerStockDebitFailed = null;
    private String offerStockBeingRollbacked = null;
    private List<String> offerStockRollbacked = null;
    private List<String> offerStockRollbackFailed = null;
    
    private List<OfferPrice> paymentToBeDebited = null;
    private OfferPrice paymentBeingDebited = null;
    private List<OfferPrice> paymentDebited = null;
    private OfferPrice paymentDebitFailed = null;
    private OfferPrice paymentBeingRollbacked = null;
    private List<OfferPrice> paymentRollbacked = null;
    private List<OfferPrice> paymentRollbackFailed = null;
    
    private List<String> commoditiesToBeCredited = null;
    private List<String> commoditiesCredited = null;
    
    private List<String> providerToBeNotifyed = null;
    private List<String> providerNotifyed = null;
    
//    private Boolean sendNotificationDone = false;
//    private Boolean recordPaymentDone = false;                  // generate BDR
//    private Boolean recordPurchaseDone = false;                 // generate ODR and PODR
//    private Boolean incrementPurchaseCountersDone = false;      //check subscriber limits... and record stats
//    private Boolean incrementPurchaseStatsDone = false;         //check subscriber limits... and record stats
    
    /*****************************************
    *
    *  getters
    *
    *****************************************/

    public String getCorrelator(){return correlator;}
    public String getOfferID(){return offerID;}
    public String getSubscriberID(){return subscriberID;}
    public int getQuantity(){return quantity;}
    public String getSalesChannelID(){return salesChannelID;}

    public boolean getRollbackInProgress(){return rollbackInProgress;}
    
    public DeliveryStatus getDeliveryStatus(){return deliveryStatus;}
    public int getDeliveryStatusCode(){return deliveryStatusCode;}
    public String getDeliveryStatusMessage(){return deliveryStatusMessage;}

    public List<String> getProductStockToBeDebited(){return productStockToBeDebited;}
    public String getProductStockBeingDebited(){return productStockBeingDebited;}
    public List<String> getProductStockDebited(){return productStockDebited;}
    public String getProductStockDebitFailed(){return productStockDebitFailed;}
    public String getProductStockBeingRollbacked(){return productStockBeingRollbacked;}
    public List<String> getProductStockRollbacked(){return productStockRollbacked;}
    public List<String> getProductStockRollbackFailed(){return productStockRollbackFailed;}

    public List<String> getOfferStockToBeDebited(){return offerStockToBeDebited;}
    public String getOfferStockBeingDebited(){return offerStockBeingDebited;}
    public List<String> getOfferStockDebited(){return offerStockDebited;}
    public String getOfferStockDebitFailed(){return offerStockDebitFailed;}
    public String getOfferStockBeingRollbacked(){return offerStockBeingRollbacked;}
    public List<String> getOfferStockRollbacked(){return offerStockRollbacked;}
    public List<String> getOfferStockRollbackFailed(){return offerStockRollbackFailed;}
    
    public List<OfferPrice> getPaymentToBeDebited(){return paymentToBeDebited;}
    public OfferPrice getPaymentBeingDebited(){return paymentBeingDebited;}
    public List<OfferPrice> getPaymentDebited(){return paymentDebited;}
    public OfferPrice getPaymentDebitFailed(){return paymentDebitFailed;}
    public OfferPrice getPaymentBeingRollbacked(){return paymentBeingRollbacked;}
    public List<OfferPrice> getPaymentRollbacked(){return paymentRollbacked;}
    public List<OfferPrice> getPaymentRollbackFailed(){return paymentRollbackFailed;}
    
    public List<String> getCommoditiesToBeCredited(){return commoditiesToBeCredited;}
    public List<String> getCommoditiesCredited(){return commoditiesCredited;}
    
    public List<String> getProviderToBeNotifyed(){return providerToBeNotifyed;}
    public List<String> getProviderNotifyed(){return providerNotifyed;}
 
    /*****************************************
    *
    *  setters
    *
    *****************************************/

    public void setRollbackInProgress(boolean rollbackInProgress){this.rollbackInProgress = rollbackInProgress;}

    public void setDeliveryStatus(DeliveryStatus deliveryStatus){this.deliveryStatus = deliveryStatus;}
    public void setDeliveryStatusCode(int deliveryStatusCode){this.deliveryStatusCode = deliveryStatusCode;}
    public void setDeliveryStatusMessage(String deliveryStatusMessage){this.deliveryStatusMessage = deliveryStatusMessage;}

    public void addProductStockToBeDebited(String productID){if(productStockToBeDebited == null){productStockToBeDebited = new ArrayList<String>(); productStockToBeDebited.add(productID);}}
    public void setProductStockBeingDebited(String productID){this.productStockBeingDebited = productID;}
    public void addProductStockDebited(String productID){if(productStockDebited == null){productStockDebited = new ArrayList<String>(); productStockDebited.add(productID);}}
    public void setProductStockDebitFailed(String productID){this.productStockDebitFailed = productID;}
    public void setProductStockBeingRollbacked(String productID){this.productStockBeingRollbacked = productID;}
    public void addProductStockRollbacked(String productID){if(productStockRollbacked == null){productStockRollbacked = new ArrayList<String>(); productStockRollbacked.add(productID);}}
    public void addProductStockRollbackFailed(String productID){if(productStockRollbackFailed == null){productStockRollbackFailed = new ArrayList<String>(); productStockRollbackFailed.add(productID);}}
    
    public void addOfferStockToBeDebited(String offerID){if(offerStockToBeDebited == null){offerStockToBeDebited = new ArrayList<String>(); offerStockToBeDebited.add(offerID);}}
    public void setOfferStockBeingDebited(String offerID){this.offerStockBeingDebited = offerID;}
    public void addOfferStockDebited(String offerID){if(offerStockDebited == null){offerStockDebited = new ArrayList<String>(); offerStockDebited.add(offerID);}}
    public void setOfferStockDebitFailed(String offerID){this.offerStockDebitFailed = offerID;}
    public void setOfferStockBeingRollbacked(String offerID){this.offerStockBeingRollbacked = offerID;}
    public void addOfferStockRollbacked(String offerID){if(offerStockRollbacked == null){offerStockRollbacked = new ArrayList<String>(); offerStockRollbacked.add(offerID);}}
    public void addOfferStockRollbackFailed(String offerID){if(offerStockRollbackFailed == null){offerStockRollbackFailed = new ArrayList<String>(); offerStockRollbackFailed.add(offerID);}}

    public void addPaymentToBeDebited(OfferPrice offerPrice){if(paymentToBeDebited == null){paymentToBeDebited = new ArrayList<OfferPrice>(); paymentToBeDebited.add(offerPrice);}}
    public void setPaymentBeingDebited(OfferPrice offerPrice){this.paymentBeingDebited = offerPrice;}
    public void addPaymentDebited(OfferPrice offerPrice){if(paymentDebited == null){paymentDebited = new ArrayList<OfferPrice>(); paymentDebited.add(offerPrice);}}
    public void setPaymentDebitFailed(OfferPrice offerPrice){this.paymentDebitFailed = offerPrice;}
    public void setPaymentBeingRollbacked(OfferPrice offerPrice){this.paymentBeingRollbacked = offerPrice;}
    public void addPaymentRollbacked(OfferPrice offerPrice){if(paymentRollbacked == null){paymentRollbacked = new ArrayList<OfferPrice>(); paymentRollbacked.add(offerPrice);}}
    public void addPaymentRollbackFailed(OfferPrice offerPrice){if(paymentRollbackFailed == null){paymentRollbackFailed = new ArrayList<OfferPrice>(); paymentRollbackFailed.add(offerPrice);}}
    
    /*****************************************
    *
    *  Constructors
    *
    *****************************************/

    public PurchaseRequestStatus(){
    }
    
    public PurchaseRequestStatus(String correlator, String offerID, String subscriberID, int quantity, String salesChannelID){
      this.correlator = correlator;
      this.offerID = offerID;
      this.subscriberID = subscriberID;
      this.quantity = quantity;
      this.salesChannelID = salesChannelID;
    }

    /*****************************************
    *
    *  Object to/from json
    *
    *****************************************/

    public static String toJSON(PurchaseRequestStatus purchaseRequestStatus){
      ObjectMapper mapper = new ObjectMapper();
      String jsonInString = null;
      try {
        jsonInString = mapper.writeValueAsString(purchaseRequestStatus);
      } catch (IOException e)
      {
        e.printStackTrace();
      }
      return jsonInString;
    }
    
    public static PurchaseRequestStatus fromJSON(String jsonInString){
      ObjectMapper mapper = new ObjectMapper();
      PurchaseRequestStatus obj = null;
      try {
        obj = mapper.readValue(jsonInString, PurchaseRequestStatus.class);
      } catch (IOException e)
      {
        e.printStackTrace();
      }
      return obj;
    }

  }
  
}

