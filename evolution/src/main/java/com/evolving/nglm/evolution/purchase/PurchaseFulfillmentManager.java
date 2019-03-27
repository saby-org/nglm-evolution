/*****************************************************************************
*
*  PurchaseFulfillmentManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.purchase;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryManager;
import com.evolving.nglm.evolution.DeliveryManagerDeclaration;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.ODRStatistics;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferPrice;
import com.evolving.nglm.evolution.OfferProduct;
import com.evolving.nglm.evolution.OfferSalesChannelsAndPrice;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.Product;
import com.evolving.nglm.evolution.ProductService;
import com.evolving.nglm.evolution.SalesChannel;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.StockMonitor;
import com.evolving.nglm.evolution.SubscriberEvaluationRequest;
import com.evolving.nglm.evolution.SubscriberGroupEpoch;
import com.evolving.nglm.evolution.SubscriberProfile;
import com.evolving.nglm.evolution.SubscriberProfileService;
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
  private SalesChannelService salesChannelService;
  private StockMonitor stockService;
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private CommodityActionManager commodityActionManager;
  private ODRStatistics odrStats = null;
  
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
    
    subscriberProfileService = new RedisSubscriberProfileService(Deployment.getRedisSentinels());
    subscriberProfileService.start();
    
    offerService = new OfferService(Deployment.getBrokerServers(), "PurchaseMgr-offerservice-"+deliveryManagerKey, Deployment.getOfferTopic(), false);
    offerService.start();

    productService = new ProductService(Deployment.getBrokerServers(), "PurchaseMgr-productservice-"+deliveryManagerKey, Deployment.getProductTopic(), false);
    productService.start();
    
    salesChannelService = new SalesChannelService(Deployment.getBrokerServers(), "PurchaseMgr-salesChannelservice-"+deliveryManagerKey, Deployment.getSalesChannelTopic(), false);
    salesChannelService.start();

    stockService = new StockMonitor("PurchaseMgr-stockService-"+deliveryManagerKey, offerService, productService);
    stockService.start();

    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("example", "PurchaseMgr-subscriberGroupReader-"+deliveryManagerKey, Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);

    commodityActionManager = new CommodityActionManager(this);

    //
    // statistics
    //
    
    try{
      odrStats = new ODRStatistics("deliverymanager-purchasefulfillment");
    }catch(Exception e){
      log.error("PurchaseFulfillmentManager: could not load statistics ", e);
      throw new RuntimeException("PurchaseFulfillmentManager: could not load statistics  ", e);
    }
    
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
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

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
    
    @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SalesChannelService salesChannelService)
    {
      //
      //  salesChannel
      //

      SalesChannel salesChannel = salesChannelService.getActiveSalesChannel(getSalesChannelID(), SystemTime.getCurrentTime());

      //
      //  presentation
      //

      guiPresentationMap.put(CUSTOMERID, getSubscriberID());
      guiPresentationMap.put(PURCHASEID, getEventID());
      guiPresentationMap.put(OFFERID, getOfferID());
      guiPresentationMap.put(OFFERQTY, getQuantity());
      guiPresentationMap.put(SALESCHANNELID, getSalesChannelID());
      guiPresentationMap.put(SALESCHANNEL, (salesChannel != null) ? salesChannel.getSalesChannelName() : null);
      guiPresentationMap.put(MODULEID, getModuleID());
      guiPresentationMap.put(MODULENAME, Module.fromExternalRepresentation(getModuleID()).toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(ORIGIN, getDeliveryRequestSource());
      guiPresentationMap.put(RETURNCODE, getReturnCode());
      guiPresentationMap.put(RETURNCODEDETAILS, getReturnCodeDetails());
      guiPresentationMap.put(VOUCHERCODE, "");
      guiPresentationMap.put(VOUCHERPARTNERID, "");
    }
    
    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SalesChannelService salesChannelService) 
    {
      //
      //  salesChannel
      //

      SalesChannel salesChannel = salesChannelService.getActiveSalesChannel(getSalesChannelID(), SystemTime.getCurrentTime());

      //
      //  presentation
      //

      thirdPartyPresentationMap.put(CUSTOMERID, getSubscriberID());
      thirdPartyPresentationMap.put(PURCHASEID, getEventID());
      thirdPartyPresentationMap.put(OFFERID, getOfferID());
      thirdPartyPresentationMap.put(OFFERQTY, getQuantity());
      thirdPartyPresentationMap.put(SALESCHANNELID, getSalesChannelID());
      thirdPartyPresentationMap.put(SALESCHANNEL, (salesChannel != null) ? salesChannel.getSalesChannelName() : null);
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, Module.fromExternalRepresentation(getModuleID()).toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(ORIGIN, getDeliveryRequestSource());
      thirdPartyPresentationMap.put(RETURNCODE, getReturnCode());
      thirdPartyPresentationMap.put(RETURNCODEDETAILS, getReturnCodeDetails());
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
        PurchaseRequestStatus purchaseStatus = new PurchaseRequestStatus(correlator, purchaseRequest.getEventID(), purchaseRequest.getModuleID(), purchaseRequest.getFeatureID(), offerID, subscriberID, quantity, salesChannelID);
        
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

        SalesChannel salesChannel = salesChannelService.getActiveSalesChannel(salesChannelID, now);
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
          if(offerSalesChannelsAndPrice.getSalesChannelIDs() != null && offerSalesChannelsAndPrice.getSalesChannelIDs().contains(salesChannel.getSalesChannelID())){
            offerPrice = offerSalesChannelsAndPrice.getPrice();
            priceFound = true;
            log.debug(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.checkOffer (offer, subscriberProfile) : offer price for sales channel "+salesChannel.getSalesChannelID()+" found ("+offerPrice.getAmount()+" "+offerPrice.getPaymentMeanID()+")");
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
            purchaseStatus.addProductStockToBeDebited(offerProduct);
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
    submitCorrelatorUpdate(purchaseStatus.getCorrelator(), purchaseStatus.getJSONRepresentation());
  }

  @Override protected void processCorrelatorUpdate(DeliveryRequest deliveryRequest, JSONObject correlatorUpdate)
  {
    log.info("PurchaseFulfillmentManager.processCorrelatorUpdate("+deliveryRequest.getDeliveryRequestID()+", "+correlatorUpdate+") : called ...");

    PurchaseRequestStatus purchaseStatus = new PurchaseRequestStatus(correlatorUpdate);
    deliveryRequest.setDeliveryStatus(purchaseStatus.getDeliveryStatus());
    //purchaseStatus.getDeliveryStatusCode();
    //purchaseStatus.getDeliveryStatusMessage();
    deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
    completeRequest(deliveryRequest);
    odrStats.updatePurchasesCount(1, deliveryRequest.getDeliveryStatus());

    log.debug("PurchaseFulfillmentManager.processCorrelatorUpdate("+deliveryRequest.getDeliveryRequestID()+", "+correlatorUpdate+") : DONE");

  }

  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  @Override protected void shutdown()
  {
    log.info("PurchaseFulfillmentManager: shutdown called");
    stockService.close();
    log.info("PurchaseFulfillmentManager: shutdown DONE");
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
      boolean debitOK = debitProductStock(purchaseStatus);
      if(!debitOK){
        proceedRollback(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "proceedPurchase : could not debit stock of product "+purchaseStatus.getProductStockDebitFailed().getProductID()/* TODO : use right message here */);
        return;
      }
    }

    //
    // debit all offer stocks
    //
    
    if(purchaseStatus.getOfferStockToBeDebited() != null && !purchaseStatus.getOfferStockToBeDebited().isEmpty()){
      boolean debitOK = debitOfferStock(purchaseStatus);
      if(!debitOK){
        proceedRollback(purchaseStatus, DeliveryStatus.Failed, -1/* TODO : use right code here */, "proceedPurchase : could not debit stock of offer "+purchaseStatus.getOfferStockDebitFailed()/* TODO : use right message here */);
        return;
      }
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

    //
    // everything is OK => confirm products and offers reservations
    //
    
    if(purchaseStatus.getProductStockDebited() != null && !purchaseStatus.getProductStockDebited().isEmpty()){
      for(OfferProduct offerProduct : purchaseStatus.getProductStockDebited()){
        Product product = productService.getActiveProduct(offerProduct.getProductID(), SystemTime.getCurrentTime());
        if(product == null){
          //TODO SCH : log warn
        }else{
          int quantity = offerProduct.getQuantity() * purchaseStatus.getQuantity();
          stockService.confirmReservation(product, quantity);
        }
      }
    }
    if(purchaseStatus.getOfferStockDebited() != null && !purchaseStatus.getOfferStockDebited().isEmpty()){
      for(String offerID : purchaseStatus.getOfferStockDebited()){
        Offer offer = offerService.getActiveOffer(offerID, SystemTime.getCurrentTime());
        if(offer == null){
          //TODO SCH : log warn
        }else{
          int quantity = purchaseStatus.getQuantity();
          stockService.confirmReservation(offer, quantity);
        }
      }
    }
    
    //
    // everything is OK => update and return response (succeed)
    //
    
    submitCorrelatorUpdate(purchaseStatus, DeliveryStatus.Delivered, 0/* TODO : use right code here */, "Success"/* TODO : use right message here */);
    
  }

  /*****************************************
  *
  *  steps of the purchase
  *
  *****************************************/

  private boolean debitProductStock(PurchaseRequestStatus purchaseStatus){
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitProductStock (offerID "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
    boolean allGood = true;
    if(purchaseStatus.getProductStockToBeDebited() != null && !purchaseStatus.getProductStockToBeDebited().isEmpty()){
      while(!purchaseStatus.getProductStockToBeDebited().isEmpty() && allGood){
        OfferProduct offerProduct = purchaseStatus.getProductStockToBeDebited().remove(0);
        purchaseStatus.setProductStockBeingDebited(offerProduct);
        Product product = productService.getActiveProduct(offerProduct.getProductID(), SystemTime.getCurrentTime());
        if(product == null){
          purchaseStatus.setProductStockDebitFailed(purchaseStatus.getProductStockBeingDebited());
          purchaseStatus.setProductStockBeingDebited(null);
          allGood = false;
        }else{
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitProductStock (offerID "+purchaseStatus.getOfferID()+", productID "+product.getProductID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
          int quantity = offerProduct.getQuantity() * purchaseStatus.getQuantity();
          boolean approved = stockService.reserve(product, quantity);
          if(approved){
            purchaseStatus.addProductStockDebited(purchaseStatus.getProductStockBeingDebited());
            purchaseStatus.setProductStockBeingDebited(null);
            log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitProductStock : " + product.getProductID() + " reserved");
          }else{
            purchaseStatus.setProductStockDebitFailed(purchaseStatus.getProductStockBeingDebited());
            purchaseStatus.setProductStockBeingDebited(null);
            allGood = false;
            log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitProductStock : " + product.getProductID() + " reservation FAILED");
          }
        }
      }
    }
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitProductStock (offerID "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") DONE");
    return allGood;
  }

  private boolean debitOfferStock(PurchaseRequestStatus purchaseStatus){
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitOfferStock (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
    boolean allGood = true;
    if(purchaseStatus.getOfferStockToBeDebited() != null && !purchaseStatus.getOfferStockToBeDebited().isEmpty()){
      while(!purchaseStatus.getOfferStockToBeDebited().isEmpty() && allGood){
        String offerID = purchaseStatus.getOfferStockToBeDebited().remove(0);
        purchaseStatus.setOfferStockBeingDebited(offerID);
        Offer offer = offerService.getActiveOffer(offerID, SystemTime.getCurrentTime());
        if(offer == null){
          purchaseStatus.setOfferStockDebitFailed(purchaseStatus.getOfferStockBeingDebited());
          purchaseStatus.setOfferStockBeingDebited(null);
          allGood = false;
        }else{
          int quantity = purchaseStatus.getQuantity();
          boolean approved = stockService.reserve(offer, quantity);
          if(approved){
            purchaseStatus.addOfferStockDebited(purchaseStatus.getOfferStockBeingDebited());
            purchaseStatus.setOfferStockBeingDebited(null);
          }else{
            purchaseStatus.setOfferStockDebitFailed(purchaseStatus.getOfferStockBeingDebited());
            purchaseStatus.setOfferStockBeingDebited(null);
            allGood = false;
          }
        }
      }
    }
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitOfferStock (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") DONE");
    return allGood;
  }

  private boolean makePayment(PurchaseRequestStatus purchaseStatus){
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.makePayment (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
    OfferPrice offerPrice = purchaseStatus.getPaymentBeingDebited();
    boolean paymentDone =  commodityActionManager.makePayment(purchaseStatus.getJSONRepresentation(), purchaseStatus.getCorrelator(), purchaseStatus.getEventID(), purchaseStatus.getModuleID(), purchaseStatus.getFeatureID(), purchaseStatus.getSubscriberID(), offerPrice.getProviderID(), offerPrice.getPaymentMeanID(), offerPrice.getAmount() * purchaseStatus.getQuantity(), this);
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
    // cancel all product stocks
    //
    
    if(purchaseStatus.getProductStockDebited() != null && !purchaseStatus.getProductStockDebited().isEmpty()){
      for(OfferProduct offerProduct : purchaseStatus.getProductStockDebited()){
        Product product = productService.getActiveProduct(offerProduct.getProductID(), SystemTime.getCurrentTime());
        if(product == null){
          //TODO SCH : log warn
        }else{
          int quantity = offerProduct.getQuantity() * purchaseStatus.getQuantity();
          stockService.voidReservation(product, quantity);
        }
      }
    }

    //
    // cancel all offer stocks
    //
    
    if(purchaseStatus.getOfferStockDebited() != null && !purchaseStatus.getOfferStockDebited().isEmpty()){
      for(String offerID : purchaseStatus.getOfferStockDebited()){
        Offer offer = offerService.getActiveOffer(offerID, SystemTime.getCurrentTime());
        if(offer == null){
          //TODO SCH : log warn
        }else{
          int quantity = purchaseStatus.getQuantity();
          stockService.voidReservation(offer, quantity);
        }
      }
    }

    //
    // cancel all payments
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

    //
    // rollback completed => update and return response (failed)
    //
    
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
    purchaseStatus.addProductStockRollbacked(purchaseStatus.getProductStockBeingRollbacked());
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
    boolean paymentDone =  commodityActionManager.creditCommodity(purchaseStatus.getJSONRepresentation(), purchaseStatus.getCorrelator(), purchaseStatus.getEventID(), purchaseStatus.getModuleID(), purchaseStatus.getFeatureID(), purchaseStatus.getSubscriberID(), offerPrice.getProviderID(), offerPrice.getPaymentMeanID(), offerPrice.getAmount() * purchaseStatus.getQuantity(), this);
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
    JSONParser parser = new JSONParser();
    PurchaseRequestStatus purchaseStatus = null;
    try
      {
        JSONObject requestStatusJSON = (JSONObject) parser.parse(response.getDiplomaticBriefcase().get(originalRequest));
        purchaseStatus = new PurchaseRequestStatus(requestStatusJSON);
      } catch (ParseException e)
      {
        log.error(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.onDRResponse(...) : ERROR whilme getting request status from '"+response.getDiplomaticBriefcase().get(originalRequest)+"' => IGNORED");
        return;
      }
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
      if(purchaseStatus.getProductStockBeingRollbacked() != null){
        OfferProduct productStockBeingRollbacked = purchaseStatus.getProductStockBeingRollbacked();
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
      if(purchaseStatus.getProductStockBeingDebited() != null){
        OfferProduct productStockBeingDebited = purchaseStatus.getProductStockBeingDebited();
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
    private String  eventID = null;
    private String  moduleID = null;
    private String  featureID = null;
    private String offerID = null;
    private String subscriberID = null;
    private int quantity = -1;
    private String salesChannelID = null;
    
    private boolean rollbackInProgress = false;
    
    private DeliveryStatus deliveryStatus = DeliveryStatus.Unknown;
    private int deliveryStatusCode = -1;
    private String deliveryStatusMessage = null;
    
    private List<OfferProduct> productStockToBeDebited = null;
    private OfferProduct productStockBeingDebited = null;
    private List<OfferProduct> productStockDebited = null;
    private OfferProduct productStockDebitFailed = null;
    private OfferProduct productStockBeingRollbacked = null;
    private List<OfferProduct> productStockRollbacked = null;
    private List<OfferProduct> productStockRollbackFailed = null;
    
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
    public String getEventID(){return eventID;}
    public String getModuleID(){return moduleID;}
    public String getFeatureID(){return featureID;}
    public String getOfferID(){return offerID;}
    public String getSubscriberID(){return subscriberID;}
    public int getQuantity(){return quantity;}
    public String getSalesChannelID(){return salesChannelID;}

    public boolean getRollbackInProgress(){return rollbackInProgress;}
    
    public DeliveryStatus getDeliveryStatus(){return deliveryStatus;}
    public int getDeliveryStatusCode(){return deliveryStatusCode;}
    public String getDeliveryStatusMessage(){return deliveryStatusMessage;}

    public List<OfferProduct> getProductStockToBeDebited(){return productStockToBeDebited;}
    public OfferProduct getProductStockBeingDebited(){return productStockBeingDebited;}
    public List<OfferProduct> getProductStockDebited(){return productStockDebited;}
    public OfferProduct getProductStockDebitFailed(){return productStockDebitFailed;}
    public OfferProduct getProductStockBeingRollbacked(){return productStockBeingRollbacked;}
    public List<OfferProduct> getProductStockRollbacked(){return productStockRollbacked;}
    public List<OfferProduct> getProductStockRollbackFailed(){return productStockRollbackFailed;}

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

    public void addProductStockToBeDebited(OfferProduct product){if(productStockToBeDebited == null){productStockToBeDebited = new ArrayList<OfferProduct>();} productStockToBeDebited.add(product);}
    public void setProductStockBeingDebited(OfferProduct product){this.productStockBeingDebited = product;}
    public void addProductStockDebited(OfferProduct product){if(productStockDebited == null){productStockDebited = new ArrayList<OfferProduct>();} productStockDebited.add(product);}
    public void setProductStockDebitFailed(OfferProduct product){this.productStockDebitFailed = product;}
    public void setProductStockBeingRollbacked(OfferProduct product){this.productStockBeingRollbacked = product;}
    public void addProductStockRollbacked(OfferProduct product){if(productStockRollbacked == null){productStockRollbacked = new ArrayList<OfferProduct>();} productStockRollbacked.add(product);}
    public void addProductStockRollbackFailed(OfferProduct product){if(productStockRollbackFailed == null){productStockRollbackFailed = new ArrayList<OfferProduct>();} productStockRollbackFailed.add(product);}
    
    public void addOfferStockToBeDebited(String offerID){if(offerStockToBeDebited == null){offerStockToBeDebited = new ArrayList<String>();} offerStockToBeDebited.add(offerID);}
    public void setOfferStockBeingDebited(String offerID){this.offerStockBeingDebited = offerID;}
    public void addOfferStockDebited(String offerID){if(offerStockDebited == null){offerStockDebited = new ArrayList<String>();} offerStockDebited.add(offerID);}
    public void setOfferStockDebitFailed(String offerID){this.offerStockDebitFailed = offerID;}
    public void setOfferStockBeingRollbacked(String offerID){this.offerStockBeingRollbacked = offerID;}
    public void addOfferStockRollbacked(String offerID){if(offerStockRollbacked == null){offerStockRollbacked = new ArrayList<String>();} offerStockRollbacked.add(offerID);}
    public void addOfferStockRollbackFailed(String offerID){if(offerStockRollbackFailed == null){offerStockRollbackFailed = new ArrayList<String>();} offerStockRollbackFailed.add(offerID);}

    public void addPaymentToBeDebited(OfferPrice offerPrice){if(paymentToBeDebited == null){paymentToBeDebited = new ArrayList<OfferPrice>();} paymentToBeDebited.add(offerPrice);}
    public void setPaymentBeingDebited(OfferPrice offerPrice){this.paymentBeingDebited = offerPrice;}
    public void addPaymentDebited(OfferPrice offerPrice){if(paymentDebited == null){paymentDebited = new ArrayList<OfferPrice>();} paymentDebited.add(offerPrice);}
    public void setPaymentDebitFailed(OfferPrice offerPrice){this.paymentDebitFailed = offerPrice;}
    public void setPaymentBeingRollbacked(OfferPrice offerPrice){this.paymentBeingRollbacked = offerPrice;}
    public void addPaymentRollbacked(OfferPrice offerPrice){if(paymentRollbacked == null){paymentRollbacked = new ArrayList<OfferPrice>();} paymentRollbacked.add(offerPrice);}
    public void addPaymentRollbackFailed(OfferPrice offerPrice){if(paymentRollbackFailed == null){paymentRollbackFailed = new ArrayList<OfferPrice>();} paymentRollbackFailed.add(offerPrice);}
    
    /*****************************************
    *
    *  Constructors
    *
    *****************************************/

    public PurchaseRequestStatus(String correlator, String eventID, String moduleID, String featureID, String offerID, String subscriberID, int quantity, String salesChannelID){
      this.correlator = correlator;
      this.eventID = eventID;
      this.moduleID = moduleID;
      this.featureID = featureID;
      this.offerID = offerID;
      this.subscriberID = subscriberID;
      this.quantity = quantity;
      this.salesChannelID = salesChannelID;
    }

    /*****************************************
    *
    *  constructor -- JSON
    *
    *****************************************/

    public PurchaseRequestStatus(JSONObject jsonRoot)
    {  
      this.correlator = JSONUtilities.decodeString(jsonRoot, "correlator", true);
      this.eventID = JSONUtilities.decodeString(jsonRoot, "eventID", true);
      this.moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", true);
      this.featureID = JSONUtilities.decodeString(jsonRoot, "featureID", true);
      this.offerID = JSONUtilities.decodeString(jsonRoot, "offerID", true);
      this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
      this.quantity = JSONUtilities.decodeInteger(jsonRoot, "quantity", true);
      this.salesChannelID = JSONUtilities.decodeString(jsonRoot, "salesChannelID", true);
      
      this.rollbackInProgress = JSONUtilities.decodeBoolean(jsonRoot, "rollbackInProgress", false);
      
      this.deliveryStatus = DeliveryStatus.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "deliveryStatus", false));
      this.deliveryStatusCode = JSONUtilities.decodeInteger(jsonRoot, "deliveryStatusCode", false);
      this.deliveryStatusMessage = JSONUtilities.decodeString(jsonRoot, "deliveryStatusMessage", false);
      
      if(JSONUtilities.decodeJSONObject(jsonRoot, "productStockBeingDebited", false) != null){
        this.productStockBeingDebited = new OfferProduct(JSONUtilities.decodeJSONObject(jsonRoot, "productStockBeingDebited", false));
      }
      if(JSONUtilities.decodeJSONObject(jsonRoot, "productStockDebitFailed", false) != null){
        this.productStockDebitFailed = new OfferProduct(JSONUtilities.decodeJSONObject(jsonRoot, "productStockDebitFailed", false));
      }
      if(JSONUtilities.decodeJSONObject(jsonRoot, "productStockBeingRollbacked", false) != null){
        this.productStockBeingRollbacked = new OfferProduct(JSONUtilities.decodeJSONObject(jsonRoot, "productStockBeingRollbacked", false));
      }
      JSONArray productStockToBeDebitedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "productStockToBeDebited", false);
      if (productStockToBeDebitedJSON != null){
        List<OfferProduct> productStockToBeDebitedList = new ArrayList<OfferProduct>();
        for (int i=0; i<productStockToBeDebitedJSON.size(); i++){
          productStockToBeDebitedList.add(new OfferProduct((JSONObject) productStockToBeDebitedJSON.get(i)));
        }
        this.productStockToBeDebited = productStockToBeDebitedList;
      }
      JSONArray productStockDebitedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "productStockDebited", false);
      if (productStockDebitedJSON != null){
        List<OfferProduct> productStockDebitedList = new ArrayList<OfferProduct>();
        for (int i=0; i<productStockDebitedJSON.size(); i++){
          productStockDebitedList.add(new OfferProduct((JSONObject) productStockDebitedJSON.get(i)));
        }
        this.productStockDebited = productStockDebitedList;
      }
      JSONArray productStockRollbackedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "productStockRollbacked", false);
      if (productStockRollbackedJSON != null){
        List<OfferProduct> productStockRollbackedList = new ArrayList<OfferProduct>();
        for (int i=0; i<productStockRollbackedJSON.size(); i++){
          productStockRollbackedList.add(new OfferProduct((JSONObject) productStockRollbackedJSON.get(i)));
        }
        this.productStockRollbacked = productStockRollbackedList;
      }
      JSONArray productStockRollbackFailedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "productStockRollbackFailed", false);
      if (productStockRollbackFailedJSON != null){
        List<OfferProduct> productStockRollbackFailedList = new ArrayList<OfferProduct>();
        for (int i=0; i<productStockRollbackFailedJSON.size(); i++){
          productStockRollbackFailedList.add(new OfferProduct((JSONObject) productStockRollbackFailedJSON.get(i)));
        }
        this.productStockRollbackFailed = productStockRollbackFailedList;
      }

      JSONArray offerStockToBeDebitedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "offerStockToBeDebited", false);
      if (offerStockToBeDebitedJSON != null){
        List<String> offerStockToBeDebitedList = new ArrayList<String>();
        for (int i=0; i<offerStockToBeDebitedJSON.size(); i++){
          offerStockToBeDebitedList.add((String) offerStockToBeDebitedJSON.get(i));
        }
        this.offerStockToBeDebited = offerStockToBeDebitedList;
      }
      this.offerStockBeingDebited = JSONUtilities.decodeString(jsonRoot, "offerStockBeingDebited", false);
      JSONArray offerStockDebitedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "offerStockDebited", false);
      if (offerStockDebitedJSON != null){
        List<String> offerStockDebitedList = new ArrayList<String>();
        for (int i=0; i<offerStockDebitedJSON.size(); i++){
          offerStockDebitedList.add((String) offerStockDebitedJSON.get(i));
        }
        this.offerStockDebited = offerStockDebitedList;
      }
      this.offerStockDebitFailed = JSONUtilities.decodeString(jsonRoot, "offerStockDebitFailed", false);
      this.offerStockBeingRollbacked = JSONUtilities.decodeString(jsonRoot, "offerStockBeingRollbacked", false);
      JSONArray offerStockRollbackedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "offerStockRollbacked", false);
      if (offerStockRollbackedJSON != null){
        List<String> offerStockRollbackedList = new ArrayList<String>();
        for (int i=0; i<offerStockRollbackedJSON.size(); i++){
          offerStockRollbackedList.add((String) offerStockRollbackedJSON.get(i));
        }
        this.offerStockRollbacked = offerStockRollbackedList;
      }
      JSONArray offerStockRollbackFailedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "offerStockRollbackFailed", false);
      if (offerStockRollbackFailedJSON != null){
        List<String> offerStockRollbackFailedList = new ArrayList<String>();
        for (int i=0; i<offerStockRollbackFailedJSON.size(); i++){
          offerStockRollbackFailedList.add((String) offerStockRollbackFailedJSON.get(i));
        }
        this.offerStockRollbackFailed = offerStockRollbackFailedList;
      }
      
      JSONArray paymentToBeDebitedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "paymentToBeDebited", false);
      if (paymentToBeDebitedJSON != null){
        List<OfferPrice> paymentToBeDebitedList = new ArrayList<OfferPrice>();
        for (int i=0; i<paymentToBeDebitedJSON.size(); i++){
            paymentToBeDebitedList.add(new OfferPrice((JSONObject) paymentToBeDebitedJSON.get(i)));
        }
        this.paymentToBeDebited = paymentToBeDebitedList;
      }
      if(JSONUtilities.decodeJSONObject(jsonRoot, "paymentBeingDebited", false) != null){
        this.paymentBeingDebited = new OfferPrice(JSONUtilities.decodeJSONObject(jsonRoot, "paymentBeingDebited", false));
      }
      JSONArray paymentDebitedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "paymentDebited", false);
      if (paymentDebitedJSON != null){
        List<OfferPrice> paymentDebitedList = new ArrayList<OfferPrice>();
        for (int i=0; i<paymentDebitedJSON.size(); i++){
          paymentDebitedList.add(new OfferPrice((JSONObject) paymentDebitedJSON.get(i)));
        }
        this.paymentDebited = paymentDebitedList;
      }
      if(JSONUtilities.decodeJSONObject(jsonRoot, "paymentDebitFailed", false) != null){
        this.paymentDebitFailed = new OfferPrice(JSONUtilities.decodeJSONObject(jsonRoot, "paymentDebitFailed", false));
      }
      if(JSONUtilities.decodeJSONObject(jsonRoot, "paymentBeingRollbacked", false) != null){
        this.paymentBeingRollbacked = new OfferPrice(JSONUtilities.decodeJSONObject(jsonRoot, "paymentBeingRollbacked", false));
      }
      JSONArray paymentRollbackedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "paymentRollbacked", false);
      if (paymentRollbackedJSON != null){
        List<OfferPrice> paymentRollbackedList = new ArrayList<OfferPrice>();
        for (int i=0; i<paymentRollbackedJSON.size(); i++){
          paymentRollbackedList.add(new OfferPrice((JSONObject) paymentRollbackedJSON.get(i)));
        }
        this.paymentRollbacked = paymentRollbackedList;
      }
      JSONArray paymentRollbackFailedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "paymentRollbackFailed", false);
      if (paymentRollbackFailedJSON != null){
        List<OfferPrice> paymentRollbackFailedList = new ArrayList<OfferPrice>();
        for (int i=0; i<paymentRollbackFailedJSON.size(); i++){
          paymentRollbackFailedList.add(new OfferPrice((JSONObject) paymentRollbackFailedJSON.get(i)));
        }
        this.paymentRollbackFailed = paymentRollbackFailedList;
      }
      
    }
    
    /*****************************************
    *  
    *  to JSONObject
    *
    *****************************************/
//    private List<String> commoditiesToBeCredited = null;
//    private List<String> commoditiesCredited = null;
//    
//    private List<String> providerToBeNotifyed = null;
//    private List<String> providerNotifyed = null;
    
    public JSONObject getJSONRepresentation(){
      Map<String, Object> data = new HashMap<String, Object>();
      
      data.put("correlator", this.getCorrelator());
      data.put("eventID", this.getEventID());
      data.put("moduleID", this.getModuleID());
      data.put("featureID", this.getFeatureID());
      data.put("offerID", this.getOfferID());
      data.put("subscriberID", this.getSubscriberID());
      data.put("quantity", this.getQuantity());
      data.put("salesChannelID", this.getSalesChannelID());
      
      data.put("rollbackInProgress", this.getRollbackInProgress());
      
      data.put("deliveryStatus", this.getDeliveryStatus().getExternalRepresentation());
      data.put("deliveryStatusCode", this.getDeliveryStatusCode());
      data.put("deliveryStatusMessage", this.getDeliveryStatusMessage());
      
      if(this.getProductStockBeingDebited() != null){
        data.put("productStockBeingDebited", this.getProductStockBeingDebited().getJSONRepresentation());
      }
      if(this.getProductStockDebitFailed() != null){
        data.put("productStockDebitFailed", this.getProductStockDebitFailed().getJSONRepresentation());
      }
      if(this.getProductStockBeingRollbacked() != null){
        data.put("productStockBeingRollbacked", this.getProductStockBeingRollbacked().getJSONRepresentation());
      }
      if(this.getProductStockToBeDebited() != null){
        List<JSONObject> productStockToBeDebitedList = new ArrayList<JSONObject>();
        for(OfferProduct product : this.getProductStockToBeDebited()){
          productStockToBeDebitedList.add(product.getJSONRepresentation());
        }
        data.put("productStockToBeDebited", productStockToBeDebitedList);
      }
      if(this.getProductStockDebited() != null){
        List<JSONObject> productStockDebitedList = new ArrayList<JSONObject>();
        for(OfferProduct product : this.getProductStockDebited()){
          productStockDebitedList.add(product.getJSONRepresentation());
        }
        data.put("productStockDebited", productStockDebitedList);
      }
      if(this.getProductStockRollbacked() != null){
        List<JSONObject> productStockRollbackedList = new ArrayList<JSONObject>();
        for(OfferProduct product : this.getProductStockRollbacked()){
          productStockRollbackedList.add(product.getJSONRepresentation());
        }
        data.put("productStockRollbacked", productStockRollbackedList);
      }
      if(this.getProductStockRollbackFailed() != null){
        List<JSONObject> productStockRollbackFailedList = new ArrayList<JSONObject>();
        for(OfferProduct product : this.getProductStockRollbackFailed()){
          productStockRollbackFailedList.add(product.getJSONRepresentation());
        }
        data.put("productStockRollbackFailed", productStockRollbackFailedList);
      }

      data.put("offerStockToBeDebited", this.getOfferStockToBeDebited());
      data.put("offerStockBeingDebited", this.getOfferStockBeingDebited());
      data.put("offerStockDebited", this.getOfferStockDebited());
      data.put("offerStockDebitFailed", this.getOfferStockDebitFailed());
      data.put("offerStockBeingRollbacked", this.getOfferStockBeingRollbacked());
      data.put("offerStockRollbacked", this.getOfferStockRollbacked());
      data.put("offerStockRollbackFailed", this.getOfferStockRollbackFailed());

      if(this.getPaymentBeingDebited() != null){
        data.put("paymentBeingDebited", this.getPaymentBeingDebited().getJSONRepresentation());
      }
      if(this.getPaymentDebitFailed() != null){
        data.put("paymentDebitFailed", this.getPaymentDebitFailed().getJSONRepresentation());
      }
      if(this.getPaymentBeingRollbacked() != null){
        data.put("paymentBeingRollbacked", this.getPaymentBeingRollbacked().getJSONRepresentation());
      }
      if(this.getPaymentToBeDebited() != null){
        List<JSONObject> paymentToBeDebitedList = new ArrayList<JSONObject>();
        for(OfferPrice price : this.getPaymentToBeDebited()){
          paymentToBeDebitedList.add(price.getJSONRepresentation());
        }
        data.put("paymentToBeDebited", paymentToBeDebitedList);
      }
      if(this.getPaymentDebited() != null){
        List<JSONObject> paymentDebitedList = new ArrayList<JSONObject>();
        for(OfferPrice price : this.getPaymentDebited()){
          paymentDebitedList.add(price.getJSONRepresentation());
        }
        data.put("paymentDebited", paymentDebitedList);
      }
      if(this.getPaymentRollbacked() != null){
        List<JSONObject> paymentRollbackedList = new ArrayList<JSONObject>();
        for(OfferPrice price : this.getPaymentRollbacked()){
          paymentRollbackedList.add(price.getJSONRepresentation());
        }
        data.put("paymentRollbacked", paymentRollbackedList);
      }
      if(this.getPaymentRollbackFailed() != null){
        List<JSONObject> paymentRollbackFailedList = new ArrayList<JSONObject>();
        for(OfferPrice price : this.getPaymentRollbackFailed()){
          paymentRollbackFailedList.add(price.getJSONRepresentation());
        }
        data.put("paymentRollbackFailed", paymentRollbackFailedList);
      }

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

    private String salesChannelID;
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ActionManager(JSONObject configuration)
    {
      super(configuration);
      this.salesChannelID = JSONUtilities.decodeString(configuration, "salesChannel", true);
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

      String offerID = (String) subscriberEvaluationRequest.getJourneyNode().getNodeParameters().get("node.parameter.offerid");
      int quantity = (Integer) subscriberEvaluationRequest.getJourneyNode().getNodeParameters().get("node.parameter.quantity");
      
      /*****************************************
      *
      *  TEMP DEW HACK
      *
      *****************************************/

      offerID = (offerID != null) ? offerID : "0";

      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();

      /*****************************************
      *
      *  request
      *
      *****************************************/

      PurchaseFulfillmentRequest request = new PurchaseFulfillmentRequest(evolutionEventContext, deliveryRequestSource, offerID, quantity, salesChannelID);

      /*****************************************
      *
      *  return
      *
      *****************************************/

      return request;
    }
  }
}

