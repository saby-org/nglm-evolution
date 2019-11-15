/*****************************************************************************
*
*  PurchaseFulfillmentManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
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
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.SubscriberProfileService.RedisSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;

public class PurchaseFulfillmentManager extends DeliveryManager implements Runnable, CommodityDeliveryResponseHandler
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum PurchaseFulfillmentStatus
  {
    PURCHASED(0),
    MISSING_PARAMETERS(4),
    BAD_FIELD_VALUE(5),
    PENDING(10),
    CUSTOMER_NOT_FOUND(20),
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
    NO_VOUCHER_CODE_AVAILABLE(408),
    CHANNEL_DEACTIVATED(409),
    CUSTOMER_OFFER_LIMIT_REACHED(410),
    BAD_OFFER_DATES(411),
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
        case MISSING_PARAMETERS:
        case BAD_FIELD_VALUE:
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
        case NO_VOUCHER_CODE_AVAILABLE:
        case CHANNEL_DEACTIVATED:
        case CUSTOMER_OFFER_LIMIT_REACHED:
        case BAD_OFFER_DATES:
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
  private DeliverableService deliverableService;
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private ODRStatistics odrStats = null;
  
  private String application_ID;
  
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
    // variables
    //
    
    application_ID = "application-deliverymanager-purchasefulfillment-" + deliveryManagerKey;
    
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

    deliverableService = new DeliverableService(Deployment.getBrokerServers(), "PurchaseMgr-deliverableservice-"+deliveryManagerKey, Deployment.getDeliverableTopic(), false);
    deliverableService.start();

    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("example", "PurchaseMgr-subscriberGroupReader-"+deliveryManagerKey, Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);

    //
    // define as commodityDelivery response consumer
    //
    
    CommodityDeliveryManager.addCommodityDeliveryResponseConsumer(application_ID, this);
    
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
    
    @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
    {
      Module module = Module.fromExternalRepresentation(getModuleID());
      //
      //  salesChannel
      //

      SalesChannel salesChannel = salesChannelService.getActiveSalesChannel(getSalesChannelID(), SystemTime.getCurrentTime());
      
      //
      //  offer
      //

      Offer offer = offerService.getActiveOffer(getOfferID(), SystemTime.getCurrentTime());

      //
      //  presentation
      //
      
      if(offer != null)
        {
          guiPresentationMap.put(CUSTOMERID, getSubscriberID());
          guiPresentationMap.put(OFFERID, getOfferID());
          guiPresentationMap.put(OFFERNAME, offer.getJSONRepresentation().get("name"));
          guiPresentationMap.put(OFFERQTY, getQuantity());
          guiPresentationMap.put(OFFERSTOCK, offer.getStock());
          if(offer.getOfferSalesChannelsAndPrices() != null){
            for(OfferSalesChannelsAndPrice channel : offer.getOfferSalesChannelsAndPrices()){
              if(channel.getSalesChannelIDs() != null) {
                for(String salesChannelID : channel.getSalesChannelIDs()) {
                  if(salesChannelID.equals(getSalesChannelID())) {
                    if(channel.getPrice() != null) {
                      PaymentMean paymentMean = (PaymentMean) paymentMeanService.getStoredPaymentMean(channel.getPrice().getPaymentMeanID());
                      if(paymentMean != null) {
                        guiPresentationMap.put(OFFERPRICE, channel.getPrice().getAmount());
                        guiPresentationMap.put(MEANOFPAYMENT, paymentMean.getDisplay());
                      }
                    }
                  }
                }
              }
            }
          }

          StringBuilder sb = new StringBuilder();
          if(offer.getOfferProducts() != null) {
            for(OfferProduct offerProduct : offer.getOfferProducts()) {
              Product product = (Product) productService.getStoredProduct(offerProduct.getProductID());
              sb.append(offerProduct.getQuantity()+" ").append(product!=null?product.getDisplay():offerProduct.getProductID()).append(",");
            }
          }
          String offerContent = null;
          if(sb.length() >0){
            offerContent = sb.toString().substring(0, sb.toString().length()-1);
          }
          guiPresentationMap.put(OFFERCONTENT, offerContent);

          guiPresentationMap.put(SALESCHANNELID, getSalesChannelID());
          guiPresentationMap.put(SALESCHANNEL, (salesChannel != null) ? salesChannel.getSalesChannelName() : null);
          guiPresentationMap.put(MODULEID, getModuleID());
          guiPresentationMap.put(MODULENAME, module.toString());
          guiPresentationMap.put(FEATUREID, getFeatureID());
          guiPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
          guiPresentationMap.put(ORIGIN, getDeliveryRequestSource());
          guiPresentationMap.put(RETURNCODE, getReturnCode());
          guiPresentationMap.put(RETURNCODEDETAILS, PurchaseFulfillmentStatus.fromReturnCode(getReturnCode()).toString());
          guiPresentationMap.put(VOUCHERCODE, "");
          guiPresentationMap.put(VOUCHERPARTNERID, "");
        }
    }
    
    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
    {
      
      Module module = Module.fromExternalRepresentation(getModuleID());
      
      //
      //  salesChannel
      //

      SalesChannel salesChannel = salesChannelService.getActiveSalesChannel(getSalesChannelID(), SystemTime.getCurrentTime());
      
      //
      //  offer
      //

      Offer offer = offerService.getActiveOffer(getOfferID(), SystemTime.getCurrentTime());

      //
      //  presentation
      //

      thirdPartyPresentationMap.put(OFFERID, getOfferID());
      thirdPartyPresentationMap.put(OFFERNAME, offer.getJSONRepresentation().get("name"));
      thirdPartyPresentationMap.put(OFFERQTY, getQuantity());
      thirdPartyPresentationMap.put(OFFERSTOCK, offer.getStock());
      if(offer.getOfferSalesChannelsAndPrices() != null){
        for(OfferSalesChannelsAndPrice channel : offer.getOfferSalesChannelsAndPrices()){
          if(channel.getSalesChannelIDs() != null) {
            for(String salesChannelID : channel.getSalesChannelIDs()) {
              if(salesChannelID.equals(getSalesChannelID())) {
                if(channel.getPrice() != null) {
                  PaymentMean paymentMean = (PaymentMean) paymentMeanService.getStoredPaymentMean(channel.getPrice().getPaymentMeanID());
                  if(paymentMean != null) {
                    thirdPartyPresentationMap.put(OFFERPRICE, channel.getPrice().getAmount());
                    thirdPartyPresentationMap.put(MEANOFPAYMENT, paymentMean.getDisplay());
                  }
                }
              }
            }
          }
        }
      }
      
      StringBuilder sb = new StringBuilder();
      if(offer.getOfferProducts() != null) {
        for(OfferProduct offerProduct : offer.getOfferProducts()) {
          Product product = (Product) productService.getStoredProduct(offerProduct.getProductID());
          sb.append(product!=null?product.getDisplay():offerProduct.getProductID()).append(";").append(offerProduct.getQuantity()).append(",");
        }
      }
      String offerContent = sb.toString().substring(0, sb.toString().length()-1);
      thirdPartyPresentationMap.put(OFFERCONTENT, offerContent);
      
      thirdPartyPresentationMap.put(SALESCHANNELID, getSalesChannelID());
      thirdPartyPresentationMap.put(SALESCHANNEL, (salesChannel != null) ? salesChannel.getSalesChannelName() : null);
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, module.toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(ORIGIN, getDeliveryRequestSource());
      thirdPartyPresentationMap.put(RETURNCODE, getReturnCode());
      thirdPartyPresentationMap.put(RETURNCODEDETAILS, PurchaseFulfillmentStatus.fromReturnCode(getReturnCode()).toString());
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
          submitCorrelatorUpdate(purchaseStatus, PurchaseFulfillmentStatus.BAD_FIELD_VALUE, "bad field value for quantity");
          return;
        }
        
        //
        // Get customer
        //
        
        if(subscriberID == null){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : bad field value for subscriberID");
          submitCorrelatorUpdate(purchaseStatus, PurchaseFulfillmentStatus.MISSING_PARAMETERS, "missing mandatory field (subscriberID)");
          return;
        }
        SubscriberProfile subscriberProfile = null;
        try{
          subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID);
          if(subscriberProfile == null){
            log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : subscriber " + subscriberID + " not found");
            submitCorrelatorUpdate(purchaseStatus, PurchaseFulfillmentStatus.CUSTOMER_NOT_FOUND, "customer " + subscriberID + " not found");
            return;
          }else{
            log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : subscriber " + subscriberID + " found ("+subscriberProfile+")");
          }
        }catch (SubscriberProfileServiceException e) {
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : subscriberService not available");
          submitCorrelatorUpdate(purchaseStatus, PurchaseFulfillmentStatus.SYSTEM_ERROR, "subscriberService not available");
          return;
        }

        //
        // Get offer
        //
        
        if(offerID == null){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : bad field value for offerID");
          submitCorrelatorUpdate(purchaseStatus, PurchaseFulfillmentStatus.MISSING_PARAMETERS, "missing mandatory field (offerID)");
          return;
        }
        Offer offer = offerService.getActiveOffer(offerID, now);
        if(offer == null){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager (offer "+offerID+", subscriberID "+subscriberID+") : offer " + offerID + " not found");
          submitCorrelatorUpdate(purchaseStatus, PurchaseFulfillmentStatus.OFFER_NOT_FOUND, "offer " + offerID + " not found or not active (date = "+now+")");
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
          submitCorrelatorUpdate(purchaseStatus, PurchaseFulfillmentStatus.CHANNEL_DEACTIVATED, "salesChannel " + salesChannelID + " not activated");
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
          submitCorrelatorUpdate(purchaseStatus, PurchaseFulfillmentStatus.PRICE_NOT_APPLICABLE, "offer price for sales channel " + salesChannelID + " not found");
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
          submitCorrelatorUpdate(purchaseStatus, PurchaseFulfillmentStatus.BAD_OFFER_STATUS, "offer " + offer.getOfferID() + " not active (date = "+now+")");
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
            submitCorrelatorUpdate(purchaseStatus, PurchaseFulfillmentStatus.PRODUCT_NOT_FOUND, "product with ID " + offerProduct.getProductID() + " not found or not active (date = "+now+")");
            return;
          }else{
            purchaseStatus.addProductStockToBeDebited(offerProduct);
            purchaseStatus.addProductToBeCredited(offerProduct);
          }
        }
        
        //
        // check offer criteria (for the specific subscriber)
        //

        SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, now);
        if(!offer.evaluateProfileCriteria(evaluationRequest)){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.checkOffer (offer, subscriberProfile) : criteria of offer "+offer.getOfferID()+" not valid for subscriber "+subscriberProfile.getSubscriberID()+" (date = "+now+")");
          submitCorrelatorUpdate(purchaseStatus, PurchaseFulfillmentStatus.OFFER_NOT_APPLICABLE, "criteria of offer "+offer.getOfferID()+" not valid for subscriber "+subscriberProfile.getSubscriberID()+" (date = "+now+")");
          return;
        }
        
        //TODO : still to be done :
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

  private void submitCorrelatorUpdate(PurchaseRequestStatus purchaseStatus, PurchaseFulfillmentStatus status, String statusMessage){
    purchaseStatus.setPurchaseFulfillmentStatus(status);
    purchaseStatus.setDeliveryStatus(getPurchaseFulfillmentStatus(status));
    purchaseStatus.setDeliveryStatusCode(status.getReturnCode());
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
    ((PurchaseFulfillmentRequest)deliveryRequest).setReturnCode(purchaseStatus.getDeliveryStatusCode());
    ((PurchaseFulfillmentRequest)deliveryRequest).setStatus(purchaseStatus.getPurchaseFulfillmentStatus());
    deliveryRequest.setDeliveryStatus(purchaseStatus.getDeliveryStatus());
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
    // reserve all products (manage stock)
    //
    
    if(purchaseStatus.getProductStockToBeDebited() != null && !purchaseStatus.getProductStockToBeDebited().isEmpty()){
      boolean debitOK = debitProductStock(purchaseStatus);
      if(!debitOK){
        proceedRollback(purchaseStatus, PurchaseFulfillmentStatus.INSUFFICIENT_STOCK, "proceedPurchase : could not debit stock of product "+purchaseStatus.getProductStockDebitFailed().getProductID());
        return;
      }
    }

    //
    // reserve offer (manage stock)
    //
    
    if(purchaseStatus.getOfferStockToBeDebited() != null && !purchaseStatus.getOfferStockToBeDebited().isEmpty()){
      boolean debitOK = debitOfferStock(purchaseStatus);
      if(!debitOK){
        proceedRollback(purchaseStatus, PurchaseFulfillmentStatus.INSUFFICIENT_STOCK, "proceedPurchase : could not debit stock of offer "+purchaseStatus.getOfferStockDebitFailed());
        return;
      }
    }

    //
    // make payments
    //
    
    if(purchaseStatus.getPaymentToBeDebited() != null && !purchaseStatus.getPaymentToBeDebited().isEmpty()){
      OfferPrice offerPrice = purchaseStatus.getPaymentToBeDebited().remove(0);
      if(offerPrice == null){// => offer is free
        purchaseStatus.addPaymentDebited(offerPrice);
      }else{
        purchaseStatus.setPaymentBeingDebited(offerPrice);
        requestCommodityDelivery(purchaseStatus);
        return;
      }
    }

    //
    // credit products
    //

    if(purchaseStatus.getProductToBeCredited() != null && !purchaseStatus.getProductToBeCredited().isEmpty()){
      OfferProduct productToBeCredited = purchaseStatus.getProductToBeCredited().remove(0);
      purchaseStatus.setProductBeingCredited(productToBeCredited);
      requestCommodityDelivery(purchaseStatus);
      return;
    }

    //
    // confirm products and offers reservations
    //
    
    if(purchaseStatus.getProductStockDebited() != null && !purchaseStatus.getProductStockDebited().isEmpty()){
      for(OfferProduct offerProduct : purchaseStatus.getProductStockDebited()){
        Product product = productService.getActiveProduct(offerProduct.getProductID(), SystemTime.getCurrentTime());
        if(product == null){
          log.warn("PurchaseFulfillmentManager.proceedPurchase(offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") : could not confirm reservation of product "+offerProduct.getProductID());
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
          log.warn("PurchaseFulfillmentManager.proceedPurchase(offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") : could not confirm reservation of offer "+offerID);
        }else{
          int quantity = purchaseStatus.getQuantity();
          stockService.confirmReservation(offer, quantity);
        }
      }
    }
    
    //TODO : still to be done :
    //    - subscriber stats and/or limits (?) 

    //
    // everything is OK => update and return response (succeed)
    //
    
    submitCorrelatorUpdate(purchaseStatus, PurchaseFulfillmentStatus.PURCHASED, "Success");
    
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
            log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitProductStock : product with ID " + product.getProductID() + " reserved");
          }else{
            purchaseStatus.setProductStockDebitFailed(purchaseStatus.getProductStockBeingDebited());
            purchaseStatus.setProductStockBeingDebited(null);
            allGood = false;
            log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.debitProductStock : product with ID " + product.getProductID() + " reservation FAILED");
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

  /*****************************************
  *
  *  proceed with rollback
  *
  *****************************************/

  private void proceedRollback(PurchaseRequestStatus purchaseStatus, PurchaseFulfillmentStatus deliveryStatus, String statusMessage){

    //
    // update purchaseStatus
    //
    
    purchaseStatus.setRollbackInProgress(true);
    if(deliveryStatus != null){
      purchaseStatus.setDeliveryStatus(getPurchaseFulfillmentStatus(deliveryStatus));
      purchaseStatus.setDeliveryStatusCode(deliveryStatus.getReturnCode());
    }
    if(statusMessage != null){purchaseStatus.setDeliveryStatusMessage(statusMessage);}

    //
    // cancel all product stocks
    //
    
    if(purchaseStatus.getProductStockDebited() != null && !purchaseStatus.getProductStockDebited().isEmpty()){
      while(purchaseStatus.getProductStockDebited() != null && !purchaseStatus.getProductStockDebited().isEmpty()){
        OfferProduct offerProduct = purchaseStatus.getProductStockDebited().remove(0);
        Product product = productService.getActiveProduct(offerProduct.getProductID(), SystemTime.getCurrentTime());
        if(product == null){
          log.warn(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.proceedRollback (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") : could not cancel reservation of product "+offerProduct.getProductID());
          purchaseStatus.addProductStockRollbackFailed(offerProduct);
        }else{
          int quantity = offerProduct.getQuantity() * purchaseStatus.getQuantity();
          stockService.voidReservation(product, quantity);
          purchaseStatus.addProductStockRollbacked(offerProduct);
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.proceedRollback : reservation product " + product.getProductID() + " canceled");
        }
      }
    }

    //
    // cancel all offer stocks
    //
    
    if(purchaseStatus.getOfferStockDebited() != null && !purchaseStatus.getOfferStockDebited().isEmpty()){
      while(purchaseStatus.getOfferStockDebited() != null && !purchaseStatus.getOfferStockDebited().isEmpty()){
        String offerID = purchaseStatus.getOfferStockDebited().remove(0);
        Offer offer = offerService.getActiveOffer(offerID, SystemTime.getCurrentTime());
        if(offer == null){
          purchaseStatus.addOfferStockRollbackFailed(offerID);
          log.warn(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.proceedRollback (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") : could not cancel reservation of offer "+offerID);
        }else{
          int quantity = purchaseStatus.getQuantity();
          stockService.voidReservation(offer, quantity);
          purchaseStatus.addOfferStockRollbacked(offerID);
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.proceedRollback : reservation offer " + offer.getOfferID() + " canceled");
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
        requestCommodityDelivery(purchaseStatus);
        return;
      }
      proceedRollback(purchaseStatus, null, null);
      return;
    }

    //
    // cancel all product deliveries
    //

    if(purchaseStatus.getProductCredited() != null && !purchaseStatus.getProductCredited().isEmpty()){
      OfferProduct offerProduct = purchaseStatus.getProductCredited().remove(0);
      if(offerProduct != null){
        purchaseStatus.setProductBeingRollbacked(offerProduct);
        requestCommodityDelivery(purchaseStatus);
        return;
      }else{
        proceedRollback(purchaseStatus, null, null);
        return;
      }
    }

    //
    // rollback completed => update and return response (failed)
    //
    
    submitCorrelatorUpdate(purchaseStatus);
    
  }

  /*****************************************
  *
  *  requestCommodityDelivery (paymentMean or product)
  *
  *****************************************/
  
  private void requestCommodityDelivery(PurchaseRequestStatus purchaseStatus){
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") called ...");
    Date now = SystemTime.getCurrentTime();
    
    //
    // debit price
    //
    
    OfferPrice offerPrice = purchaseStatus.getPaymentBeingDebited();
    if(offerPrice != null){
      log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") debiting offer price ...");
      purchaseStatus.incrementNewRequestCounter();
      ZookeeperUniqueKeyServer zuks = new ZookeeperUniqueKeyServer("guimanager");
      String deliveryRequestID = zuks.getStringKey();
      CommodityDeliveryManager.sendCommodityDeliveryRequest(purchaseStatus.getJSONRepresentation(), application_ID, deliveryRequestID, purchaseStatus.getCorrelator(), false, purchaseStatus.getEventID(), purchaseStatus.getModuleID(), purchaseStatus.getFeatureID(), purchaseStatus.getSubscriberID(), offerPrice.getProviderID(), offerPrice.getPaymentMeanID(), CommodityDeliveryOperation.Debit, offerPrice.getAmount() * purchaseStatus.getQuantity(), null, 0);
      log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (deliveryReqID "+deliveryRequestID+", originatingDeliveryRequestID "+purchaseStatus.getCorrelator()+", offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") debiting offer price DONE");
    }
    
    //
    // deliver product
    //
    
    OfferProduct offerProduct = purchaseStatus.getProductBeingCredited();
    if(offerProduct != null){
      Product product = productService.getActiveProduct(offerProduct.getProductID(), now);
      if(product != null){
        Deliverable deliverable = deliverableService.getActiveDeliverable(product.getDeliverableID(), now);
        if(deliverable != null){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") delivering product ("+offerProduct.getProductID()+") ...");
          purchaseStatus.incrementNewRequestCounter();
          ZookeeperUniqueKeyServer zuks = new ZookeeperUniqueKeyServer("guimanager");
          String deliveryRequestID = zuks.getStringKey();
          CommodityDeliveryManager.sendCommodityDeliveryRequest(purchaseStatus.getJSONRepresentation(), application_ID, deliveryRequestID, purchaseStatus.getCorrelator(), false, purchaseStatus.getEventID(), purchaseStatus.getModuleID(), purchaseStatus.getFeatureID(), purchaseStatus.getSubscriberID(), deliverable.getFulfillmentProviderID(), deliverable.getDeliverableID(), CommodityDeliveryOperation.Credit, offerProduct.getQuantity() * purchaseStatus.getQuantity(), null, 0);
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (deliveryReqID "+deliveryRequestID+", originatingDeliveryRequestID "+purchaseStatus.getCorrelator()+", offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") delivering product ("+offerProduct.getProductID()+") DONE");
        }else{
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") delivering deliverable ("+offerProduct.getProductID()+") FAILED => rollback");
          purchaseStatus.setProductCreditFailed(offerProduct);
          purchaseStatus.setProductBeingCredited(null);
          proceedRollback(purchaseStatus, PurchaseFulfillmentStatus.INVALID_PRODUCT, "could not credit deliverable "+product.getDeliverableID());
        }
      }else{
        log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") delivering product ("+offerProduct.getProductID()+") FAILED => rollback");
        purchaseStatus.setProductCreditFailed(offerProduct);
        purchaseStatus.setProductBeingCredited(null);
        proceedRollback(purchaseStatus, PurchaseFulfillmentStatus.PRODUCT_NOT_FOUND, "could not credit product "+offerProduct.getProductID());
      }
    }
    
    //
    // rollback debited price
    //
    
    OfferPrice offerPriceRollback = purchaseStatus.getPaymentBeingRollbacked();
    if(offerPriceRollback != null){
      log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") rollbacking offer price ...");
      purchaseStatus.incrementNewRequestCounter();
      ZookeeperUniqueKeyServer zuks = new ZookeeperUniqueKeyServer("guimanager");
      String deliveryRequestID = zuks.getStringKey();
      CommodityDeliveryManager.sendCommodityDeliveryRequest(purchaseStatus.getJSONRepresentation(), application_ID, deliveryRequestID, purchaseStatus.getCorrelator(), false, purchaseStatus.getEventID(), purchaseStatus.getModuleID(), purchaseStatus.getFeatureID(), purchaseStatus.getSubscriberID(), offerPriceRollback.getProviderID(), offerPriceRollback.getPaymentMeanID(), CommodityDeliveryOperation.Credit, offerPriceRollback.getAmount() * purchaseStatus.getQuantity(), null, 0);
      log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (deliveryReqID "+deliveryRequestID+", originatingDeliveryRequestID "+purchaseStatus.getCorrelator()+", offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") rollbacking offer price DONE");
    }
    
    //
    // rollback product delivery
    //
    
    OfferProduct offerProductRollback = purchaseStatus.getProductBeingRollbacked();
    if(offerProductRollback != null){
      Product product = productService.getActiveProduct(offerProductRollback.getProductID(), now);
      if(product != null){
        Deliverable deliverable = deliverableService.getActiveDeliverable(product.getDeliverableID(), now);
        if(deliverable != null){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") rollbacking product delivery ("+offerProductRollback.getProductID()+") ...");
          purchaseStatus.incrementNewRequestCounter();
          ZookeeperUniqueKeyServer zuks = new ZookeeperUniqueKeyServer("guimanager");
          String deliveryRequestID = zuks.getStringKey();
          CommodityDeliveryManager.sendCommodityDeliveryRequest(purchaseStatus.getJSONRepresentation(), application_ID, deliveryRequestID, purchaseStatus.getCorrelator(), false, purchaseStatus.getEventID(), purchaseStatus.getModuleID(), purchaseStatus.getFeatureID(), purchaseStatus.getSubscriberID(), deliverable.getFulfillmentProviderID(), deliverable.getDeliverableID(), CommodityDeliveryOperation.Debit, offerProduct.getQuantity() * purchaseStatus.getQuantity(), null, 0);
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (deliveryReqID "+deliveryRequestID+", originatingDeliveryRequestID "+purchaseStatus.getCorrelator()+", offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") rollbacking product delivery ("+offerProductRollback.getProductID()+") DONE");
        }else{
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") rollbacking deliverable delivery failed (product id "+offerProductRollback.getProductID()+")");
          purchaseStatus.addProductRollbackFailed(offerProductRollback);
          purchaseStatus.setProductBeingRollbacked(null);
          proceedRollback(purchaseStatus, null, null);
        }
      }else{
        log.warn(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") rollbacking product delivery failed (product id "+offerProductRollback.getProductID()+")");
        purchaseStatus.addProductRollbackFailed(offerProductRollback);
        purchaseStatus.setProductBeingRollbacked(null);
        proceedRollback(purchaseStatus, null, null);
      }
      
    }

    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.requestCommodityDelivery (offer "+purchaseStatus.getOfferID()+", subscriberID "+purchaseStatus.getSubscriberID()+") DONE");
  }
    
  /*****************************************
  *
  *  CommodityDeliveryResponseHandler.handleCommodityDeliveryResponse(...)
  *
  *****************************************/

  @Override
  public void handleCommodityDeliveryResponse(DeliveryRequest response)
  {
    
    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse(...) called ...");

    // ------------------------------------
    // Getting initial request status
    // ------------------------------------
    
    log.debug(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse(...) : getting purchase status ");
    if(response.getDiplomaticBriefcase() == null || response.getDiplomaticBriefcase().get(CommodityDeliveryManager.APPLICATION_BRIEFCASE) == null || response.getDiplomaticBriefcase().get(CommodityDeliveryManager.APPLICATION_BRIEFCASE).isEmpty()){
      log.warn(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse(response) : can not get purchase status => ignore this response");
      return;
    }
    JSONParser parser = new JSONParser();
    PurchaseRequestStatus purchaseStatus = null;
    try
      {
        JSONObject requestStatusJSON = (JSONObject) parser.parse(response.getDiplomaticBriefcase().get(CommodityDeliveryManager.APPLICATION_BRIEFCASE));
        purchaseStatus = new PurchaseRequestStatus(requestStatusJSON);
      } catch (ParseException e)
      {
        log.error(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse(...) : ERROR while getting purchase status from '"+response.getDiplomaticBriefcase().get(CommodityDeliveryManager.APPLICATION_BRIEFCASE)+"' => IGNORED");
        return;
      }
    log.debug(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse(...) : getting purchase status DONE : "+purchaseStatus);
    
    // ------------------------------------
    // Handling response
    // ------------------------------------

    DeliveryStatus responseDeliveryStatus = response.getDeliveryStatus();
    boolean isRollbackInProgress = purchaseStatus.getRollbackInProgress();
    if(isRollbackInProgress){

      //
      // processing rollback
      //

      //  ---  check payment  ---
      if(purchaseStatus.getPaymentBeingRollbacked() != null){
        OfferPrice offerPrice = purchaseStatus.getPaymentBeingRollbacked();
        if(responseDeliveryStatus.equals(DeliveryStatus.Delivered)){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse("+purchaseStatus.getOfferID()+", "+purchaseStatus.getSubscriberID()+") : price rollbacked");
          purchaseStatus.addPaymentRollbacked(offerPrice);
          purchaseStatus.setPaymentBeingRollbacked(null);
        }else{
          //responseDeliveryStatus is one of those : Pending, FailedRetry, Delivered, Indeterminate, Failed, FailedTimeout, Unknown
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse("+purchaseStatus.getOfferID()+", "+purchaseStatus.getSubscriberID()+") : price rollback failed");
          purchaseStatus.addPaymentRollbackFailed(offerPrice);
          purchaseStatus.setPaymentBeingRollbacked(null);
        }
      }

      //  ---  check products  ---
      if(purchaseStatus.getProductBeingRollbacked() != null){
        OfferProduct offerProduct = purchaseStatus.getProductBeingRollbacked();
        if(responseDeliveryStatus.equals(DeliveryStatus.Delivered)){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse("+purchaseStatus.getOfferID()+", "+purchaseStatus.getSubscriberID()+") : product delivery rollbacked (product id "+offerProduct.getProductID()+")");
          purchaseStatus.addProductRollbacked(offerProduct);
          purchaseStatus.setProductBeingRollbacked(null);
        }else{
          //responseDeliveryStatus is one of those : Pending, FailedRetry, Delivered, Indeterminate, Failed, FailedTimeout, Unknown
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse("+purchaseStatus.getOfferID()+", "+purchaseStatus.getSubscriberID()+") : product delivery rollback failed (product id "+offerProduct.getProductID()+")");
          purchaseStatus.addProductRollbackFailed(offerProduct);
          purchaseStatus.setProductBeingRollbacked(null);
        }
      }

      // continue rollback process
      log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse("+purchaseStatus.getOfferID()+", "+purchaseStatus.getSubscriberID()+") : continue rollback process ...");
      proceedRollback(purchaseStatus, null, null);

    }else{
      
      //
      // processing purchase
      //
      
      //  ---  check payment  ---
      if(purchaseStatus.getPaymentBeingDebited() != null){
        OfferPrice offerPrice = purchaseStatus.getPaymentBeingDebited();
        if(responseDeliveryStatus.equals(DeliveryStatus.Delivered)){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse("+purchaseStatus.getOfferID()+", "+purchaseStatus.getSubscriberID()+") : price debited");
          purchaseStatus.addPaymentDebited(offerPrice);
          purchaseStatus.setPaymentBeingDebited(null);
        }else{
          //responseDeliveryStatus is one of those : Pending, FailedRetry, Delivered, Indeterminate, Failed, FailedTimeout, Unknown
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse("+purchaseStatus.getOfferID()+", "+purchaseStatus.getSubscriberID()+") : price debit failed => initiate rollback ...");
          purchaseStatus.setPaymentDebitFailed(offerPrice);
          purchaseStatus.setPaymentBeingDebited(null);
          proceedRollback(purchaseStatus, PurchaseFulfillmentStatus.INSUFFICIENT_BALANCE, "handleCommodityDeliveryResponse : could not make payment of price "+offerPrice);
          return;
        }
      }
      
      //  ---  check products  ---
      if(purchaseStatus.getProductBeingCredited() != null){
        OfferProduct product = purchaseStatus.getProductBeingCredited();
        if(responseDeliveryStatus.equals(DeliveryStatus.Delivered)){
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse("+purchaseStatus.getOfferID()+", "+purchaseStatus.getSubscriberID()+") : product credited (product id "+product.getProductID()+")");
          purchaseStatus.addProductCredited(product);
          purchaseStatus.setProductBeingCredited(null);
        }else{
          //responseDeliveryStatus is one of those : Pending, FailedRetry, Delivered, Indeterminate, Failed, FailedTimeout, Unknown
          log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse("+purchaseStatus.getOfferID()+", "+purchaseStatus.getSubscriberID()+") : product credit failed (product id "+product.getProductID()+") => initiate rollback ...");
          purchaseStatus.setProductCreditFailed(product);
          purchaseStatus.setProductBeingCredited(null);
          proceedRollback(purchaseStatus, PurchaseFulfillmentStatus.THIRD_PARTY_ERROR, "handleCommodityDeliveryResponse : could not credit product "+product.getProductID());
          return;
        }
      }

      // continue purchase process
      log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse("+purchaseStatus.getOfferID()+", "+purchaseStatus.getSubscriberID()+") : continue purchase process ...");
      proceedPurchase(purchaseStatus);
      
    }

    log.info(Thread.currentThread().getId()+" - PurchaseFulfillmentManager.handleCommodityDeliveryResponse(...) DONE");

  }
  
  /*****************************************
  *
  *  class PurchaseRequestStatus
  *
  *****************************************/

  private static class PurchaseRequestStatus
  {

    /*****************************************
    *
    *  data
    *
    *****************************************/
    
    private int newRequestCounter = 0;
    
    private String correlator = null;
    private String  eventID = null;
    private String  moduleID = null;
    private String  featureID = null;
    private String offerID = null;
    private String subscriberID = null;
    private int quantity = -1;
    private String salesChannelID = null;
    
    private boolean rollbackInProgress = false;
    
    private PurchaseFulfillmentStatus purchaseFulfillmentStatus = PurchaseFulfillmentStatus.PENDING;
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
    
    private List<OfferProduct> productToBeCredited = null;
    private OfferProduct productBeingCredited = null;
    private List<OfferProduct> productCredited = null;
    private OfferProduct productCreditFailed = null;
    private OfferProduct productBeingRollbacked = null;
    private List<OfferProduct> productRollbacked = null;
    private List<OfferProduct> productRollbackFailed = null;
        
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

    public int getNewRequestCounter(){return newRequestCounter;}
    
    public String getCorrelator(){return correlator;}
    public String getEventID(){return eventID;}
    public String getModuleID(){return moduleID;}
    public String getFeatureID(){return featureID;}
    public String getOfferID(){return offerID;}
    public String getSubscriberID(){return subscriberID;}
    public int getQuantity(){return quantity;}
    public String getSalesChannelID(){return salesChannelID;}

    public boolean getRollbackInProgress(){return rollbackInProgress;}
    
    public PurchaseFulfillmentStatus getPurchaseFulfillmentStatus(){return purchaseFulfillmentStatus;}
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
    
    public List<OfferProduct> getProductToBeCredited(){return productToBeCredited;}
    public OfferProduct getProductBeingCredited(){return productBeingCredited;}
    public List<OfferProduct> getProductCredited(){return productCredited;}
    public OfferProduct getProductCreditFailed(){return productCreditFailed;}
    public OfferProduct getProductBeingRollbacked(){return productBeingRollbacked;}
    public List<OfferProduct> getProductRollbacked(){return productRollbacked;}
    public List<OfferProduct> getProductRollbackFailed(){return productRollbackFailed;}
    
    public List<String> getProviderToBeNotifyed(){return providerToBeNotifyed;}
    public List<String> getProviderNotifyed(){return providerNotifyed;}
 
    /*****************************************
    *
    *  setters
    *
    *****************************************/
    
    private void incrementNewRequestCounter(){this.newRequestCounter++;}

    public void setRollbackInProgress(boolean rollbackInProgress){this.rollbackInProgress = rollbackInProgress;}

    public void setPurchaseFulfillmentStatus(PurchaseFulfillmentStatus purchaseFulfillmentStatus){this.purchaseFulfillmentStatus = purchaseFulfillmentStatus;}
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

    public void addProductToBeCredited(OfferProduct product){if(productToBeCredited == null){productToBeCredited = new ArrayList<OfferProduct>();} productToBeCredited.add(product);}
    public void setProductBeingCredited(OfferProduct product){this.productBeingCredited = product;}
    public void addProductCredited(OfferProduct product){if(productCredited == null){productCredited = new ArrayList<OfferProduct>();} productCredited.add(product);}
    public void setProductCreditFailed(OfferProduct product){this.productCreditFailed = product;}
    public void setProductBeingRollbacked(OfferProduct product){this.productBeingRollbacked = product;}
    public void addProductRollbacked(OfferProduct product){if(productRollbacked == null){productRollbacked = new ArrayList<OfferProduct>();} productRollbacked.add(product);}
    public void addProductRollbackFailed(OfferProduct product){if(productRollbackFailed == null){productRollbackFailed = new ArrayList<OfferProduct>();} productRollbackFailed.add(product);}
    
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
      this.newRequestCounter = JSONUtilities.decodeInteger(jsonRoot, "newRequestCounter", true);
      
      this.correlator = JSONUtilities.decodeString(jsonRoot, "correlator", true);
      this.eventID = JSONUtilities.decodeString(jsonRoot, "eventID", true);
      this.moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", true);
      this.featureID = JSONUtilities.decodeString(jsonRoot, "featureID", true);
      this.offerID = JSONUtilities.decodeString(jsonRoot, "offerID", true);
      this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
      this.quantity = JSONUtilities.decodeInteger(jsonRoot, "quantity", true);
      this.salesChannelID = JSONUtilities.decodeString(jsonRoot, "salesChannelID", true);
      
      this.rollbackInProgress = JSONUtilities.decodeBoolean(jsonRoot, "rollbackInProgress", false);
      
      this.purchaseFulfillmentStatus = PurchaseFulfillmentStatus.fromReturnCode(JSONUtilities.decodeInteger(jsonRoot, "purchaseFulfillmentStatus", false));
      this.deliveryStatus = DeliveryStatus.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "deliveryStatus", false));
      this.deliveryStatusCode = JSONUtilities.decodeInteger(jsonRoot, "deliveryStatusCode", false);
      this.deliveryStatusMessage = JSONUtilities.decodeString(jsonRoot, "deliveryStatusMessage", false);
      
      //
      // product stock
      //
      
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

      //
      // offer stock
      //
      
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
      
      //
      // paymentMeans
      //
      
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
      
      //
      // product delivery
      //
      
      if(JSONUtilities.decodeJSONObject(jsonRoot, "productBeingCredited", false) != null){
        this.productBeingCredited = new OfferProduct(JSONUtilities.decodeJSONObject(jsonRoot, "productBeingCredited", false));
      }
      if(JSONUtilities.decodeJSONObject(jsonRoot, "productCreditFailed", false) != null){
        this.productCreditFailed = new OfferProduct(JSONUtilities.decodeJSONObject(jsonRoot, "productCreditFailed", false));
      }
      if(JSONUtilities.decodeJSONObject(jsonRoot, "productBeingRollbacked", false) != null){
        this.productBeingRollbacked = new OfferProduct(JSONUtilities.decodeJSONObject(jsonRoot, "productBeingRollbacked", false));
      }
      JSONArray productToBeCreditedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "productToBeCredited", false);
      if (productToBeCreditedJSON != null){
        List<OfferProduct> productToBeCreditedList = new ArrayList<OfferProduct>();
        for (int i=0; i<productToBeCreditedJSON.size(); i++){
          productToBeCreditedList.add(new OfferProduct((JSONObject) productToBeCreditedJSON.get(i)));
        }
        this.productToBeCredited = productToBeCreditedList;
      }
      JSONArray productCreditedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "productCredited", false);
      if (productCreditedJSON != null){
        List<OfferProduct> productCreditedList = new ArrayList<OfferProduct>();
        for (int i=0; i<productCreditedJSON.size(); i++){
          productCreditedList.add(new OfferProduct((JSONObject) productCreditedJSON.get(i)));
        }
        this.productCredited = productCreditedList;
      }
      JSONArray productRollbackedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "productRollbacked", false);
      if (productRollbackedJSON != null){
        List<OfferProduct> productRollbackedList = new ArrayList<OfferProduct>();
        for (int i=0; i<productRollbackedJSON.size(); i++){
          productRollbackedList.add(new OfferProduct((JSONObject) productRollbackedJSON.get(i)));
        }
        this.productRollbacked = productRollbackedList;
      }
      JSONArray productRollbackFailedJSON = JSONUtilities.decodeJSONArray(jsonRoot, "productRollbackFailed", false);
      if (productRollbackFailedJSON != null){
        List<OfferProduct> productRollbackFailedList = new ArrayList<OfferProduct>();
        for (int i=0; i<productRollbackFailedJSON.size(); i++){
          productRollbackFailedList.add(new OfferProduct((JSONObject) productRollbackFailedJSON.get(i)));
        }
        this.productRollbackFailed = productRollbackFailedList;
      }

    }
    
    /*****************************************
    *  
    *  to JSONObject
    *
    *****************************************/
    
//    private List<String> providerToBeNotifyed = null;
//    private List<String> providerNotifyed = null;
    
    public JSONObject getJSONRepresentation(){
      Map<String, Object> data = new HashMap<String, Object>();
      
      data.put("newRequestCounter", this.getNewRequestCounter());

      data.put("correlator", this.getCorrelator());
      data.put("eventID", this.getEventID());
      data.put("moduleID", this.getModuleID());
      data.put("featureID", this.getFeatureID());
      data.put("offerID", this.getOfferID());
      data.put("subscriberID", this.getSubscriberID());
      data.put("quantity", this.getQuantity());
      data.put("salesChannelID", this.getSalesChannelID());
      
      data.put("rollbackInProgress", this.getRollbackInProgress());
      
      data.put("purchaseFulfillmentStatus", this.getPurchaseFulfillmentStatus().getReturnCode());
      data.put("deliveryStatus", this.getDeliveryStatus().getExternalRepresentation());
      data.put("deliveryStatusCode", this.getDeliveryStatusCode());
      data.put("deliveryStatusMessage", this.getDeliveryStatusMessage());
      
      //
      // product stock
      //
      
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

      //
      // offer stock
      //
      
      data.put("offerStockToBeDebited", this.getOfferStockToBeDebited());
      data.put("offerStockBeingDebited", this.getOfferStockBeingDebited());
      data.put("offerStockDebited", this.getOfferStockDebited());
      data.put("offerStockDebitFailed", this.getOfferStockDebitFailed());
      data.put("offerStockBeingRollbacked", this.getOfferStockBeingRollbacked());
      data.put("offerStockRollbacked", this.getOfferStockRollbacked());
      data.put("offerStockRollbackFailed", this.getOfferStockRollbackFailed());

      //
      // paymentMeans
      //

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

      //
      // product delivery
      //

      if(this.getProductBeingCredited() != null){
        data.put("productBeingCredited", this.getProductBeingCredited().getJSONRepresentation());
      }
      if(this.getProductCreditFailed() != null){
        data.put("productCreditFailed", this.getProductCreditFailed().getJSONRepresentation());
      }
      if(this.getProductBeingRollbacked() != null){
        data.put("productBeingRollbacked", this.getProductBeingRollbacked().getJSONRepresentation());
      }
      if(this.getProductToBeCredited() != null){
        List<JSONObject> productToBeCreditedList = new ArrayList<JSONObject>();
        for(OfferProduct product : this.getProductToBeCredited()){
          productToBeCreditedList.add(product.getJSONRepresentation());
        }
        data.put("productToBeCredited", productToBeCreditedList);
      }
      if(this.getProductCredited() != null){
        List<JSONObject> productCreditedList = new ArrayList<JSONObject>();
        for(OfferProduct product : this.getProductCredited()){
          productCreditedList.add(product.getJSONRepresentation());
        }
        data.put("productCredited", productCreditedList);
      }
      if(this.getProductRollbacked() != null){
        List<JSONObject> productRollbackedList = new ArrayList<JSONObject>();
        for(OfferProduct product : this.getProductRollbacked()){
          productRollbackedList.add(product.getJSONRepresentation());
        }
        data.put("productRollbacked", productRollbackedList);
      }
      if(this.getProductRollbackFailed() != null){
        List<JSONObject> productRollbackFailedList = new ArrayList<JSONObject>();
        for(OfferProduct product : this.getProductRollbackFailed()){
          productRollbackFailedList.add(product.getJSONRepresentation());
        }
        data.put("productRollbackFailed", productRollbackFailedList);
      }
      
      //
      // return 
      //

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

    private String moduleID;
    private String salesChannelID;
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ActionManager(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
      this.moduleID = JSONUtilities.decodeString(configuration, "moduleID", true);
      this.salesChannelID = JSONUtilities.decodeString(configuration, "salesChannel", true);
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
      request.setModuleID(moduleID);
      request.setFeatureID(deliveryRequestSource);

      /*****************************************
      *
      *  return
      *
      *****************************************/

      return Collections.<Action>singletonList(request);
    }
  }
}

