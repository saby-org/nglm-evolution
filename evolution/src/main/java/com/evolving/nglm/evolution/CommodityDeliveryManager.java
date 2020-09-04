/*****************************************************************************
*
*  CommodityDeliveryManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.management.relation.RelationService;

import com.evolving.nglm.core.*;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.EmptyFulfillmentManager.EmptyFulfillmentRequest;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;


public class CommodityDeliveryManager extends DeliveryManager implements Runnable
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

  //
  //  CommodityStatus
  //
  
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
    UNKNOWN(-1);
    private Integer externalRepresentation;
    private CommodityDeliveryStatus(Integer externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public Integer getReturnCode() { return externalRepresentation; }
    public static CommodityDeliveryStatus fromReturnCode(Integer externalRepresentation) { for (CommodityDeliveryStatus enumeratedValue : CommodityDeliveryStatus.values()) { if (enumeratedValue.getReturnCode().equals(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
  }
  
  /*****************************************
  *
  *  conversion method
  *
  *****************************************/

  public DeliveryStatus getDeliveryStatus (CommodityDeliveryStatus status)
  {
    switch(status)
      {
      case SUCCESS:
        return DeliveryStatus.Delivered;
      case PENDING:
        return DeliveryStatus.Pending;
      case CUSTOMER_NOT_FOUND:
      case BONUS_NOT_FOUND:
      case INSUFFICIENT_BALANCE:
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

  private static final Logger log = LoggerFactory.getLogger(CommodityDeliveryManager.class);

  public static final String COMMODITY_DELIVERY_MANAGER_NAME = "commodityDelivery";
  public static final String APPLICATION_ID = "application_id";
  public static final String APPLICATION_BRIEFCASE = "application_briefcase";
  private static final String COMMODITY_DELIVERY_ID = "commodity_delivery_id";
  private static final String COMMODITY_DELIVERY_BRIEFCASE = "commodity_delivery_briefcase";
  private static final String COMMODITY_DELIVERY_ID_VALUE = "deliverymanager-commodityDelivery";

  //
  // a kafka producer for static util sendCommodityDeliveryRequest
  //

  private static KafkaProducer commodityDeliveryRequestProducer = null;
  static{
    Properties kafkaProducerProperties = new Properties();
    kafkaProducerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
    kafkaProducerProperties.put("acks", "all");
    kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    commodityDeliveryRequestProducer = new KafkaProducer<byte[], byte[]>(kafkaProducerProperties);
  }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private static PaymentMeanService paymentMeanService;
  private static DeliverableService deliverableService;
  private static BDRStatistics bdrStats;
  private static ZookeeperUniqueKeyServer zookeeperUniqueKeyServer;

  private Map<String, KafkaProducer> providerRequestProducers = new HashMap<String/*providerID*/, KafkaProducer>();

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CommodityDeliveryManager(String deliveryManagerInstanceKey)
  {
    //
    //  superclass
    //
    
    super(COMMODITY_DELIVERY_ID_VALUE, deliveryManagerInstanceKey, Deployment.getBrokerServers(), CommodityDeliveryRequest.serde(), Deployment.getDeliveryManagers().get(COMMODITY_DELIVERY_MANAGER_NAME));
    
    //
    // set up all the providers conf and flows
    //
    
    getProviderAndCommodityAndPaymentMeanFromDM(deliveryManagerInstanceKey);

    //
    // starting main thread job
    //

    new Thread(this,COMMODITY_DELIVERY_ID_VALUE+"-"+deliveryManagerInstanceKey).start();
    
  }

  private void getProviderAndCommodityAndPaymentMeanFromDM(String deliveryManagerInstanceKey) {
    for(DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values()){
      CommodityType commodityType = CommodityType.fromExternalRepresentation(deliveryManager.getRequestClassName());
      
      if(commodityType != null){
        
        switch (commodityType) {
        case JOURNEY:
        case IN:
        case POINT:
        case REWARD:
        case EMPTY:

          log.info("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : get information from deliveryManager "+deliveryManager);

          //
          // get information from DeliveryManager
          //
          
          JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
          String providerID = (String) deliveryManagerJSON.get("providerID");
          String providerName = (String) deliveryManagerJSON.get("providerName");

          //
          // update list (kafka) request producers
          //

          Properties kafkaProducerProperties = new Properties();
          kafkaProducerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
          kafkaProducerProperties.put("acks", "all");
          kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
          kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
          KafkaProducer neWProducer = new KafkaProducer<byte[], byte[]>(kafkaProducerProperties);
          providerRequestProducers.put(providerID, neWProducer);
          log.info("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : added kafka producer for provider "+providerName+" (ID "+providerID+")");
          
          //
          // update list of (kafka) response consumers
          //

          String responseTopic = deliveryManager.getResponseTopic();
          String prefix = commodityType.toString()+"_"+providerID+"_"+responseTopic;
          Thread consumerThread = new Thread(new Runnable(){
            @Override
            public void run()
            {
              Properties consumerProperties = new Properties();
              consumerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
              consumerProperties.put("group.id", prefix+"_"+"requestReader");
              consumerProperties.put("auto.offset.reset", "earliest");
              consumerProperties.put("enable.auto.commit", "false");
              consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
              consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
              consumerProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Deployment.getMaxPollIntervalMs());
              KafkaConsumer consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties);
              consumer.subscribe(Arrays.asList(responseTopic));
              log.info("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : added kafka consumer for provider "+providerName+" (ID "+providerID+")");

              while(isProcessing()){

                // poll

                long lastPollTime=System.currentTimeMillis();// just to log if exception happened later, can be because of this, but most likely because of rebalance because of new consumer created
                ConsumerRecords<byte[], byte[]> fileRecords = consumer.poll(Duration.ofMillis(5000));

                //  process records

                try{
                  for (ConsumerRecord<byte[], byte[]> fileRecord : fileRecords) {
                    //  parse
                    DeliveryRequest response = deliveryManager.getRequestSerde().deserializer().deserialize(responseTopic, fileRecord.value());
                    if(response.getDiplomaticBriefcase() != null && response.getDiplomaticBriefcase().get(COMMODITY_DELIVERY_ID) != null && response.getDiplomaticBriefcase().get(COMMODITY_DELIVERY_ID).equals(COMMODITY_DELIVERY_ID_VALUE)){
                      if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" CommodityDeliveryManager : reading response from "+commodityType+" "+responseTopic+" topic ...");
                      handleThirdPartyResponse(response);
                      if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" CommodityDeliveryManager : reading response from "+commodityType+" "+responseTopic+" topic DONE");
                    }
                  }
                  consumer.commitSync();

                }catch (CommitFailedException ex){
                  long lastPoll_ms=System.currentTimeMillis()-lastPollTime;
                  log.info(Thread.currentThread().getId()+" CommodityDeliveryManager : CommitFailedException catched, can be normal rebalancing or poll time interval too long, last was "+lastPoll_ms+"ms ago");
                }

              }

              // thread leaving the main loop !
              consumer.close();
			  log.warn(Thread.currentThread().getId()+" CommodityDeliveryManager : STOPPING reading response from "+commodityType+" "+responseTopic);
              }
          }, "consumer_"+prefix+"_"+deliveryManagerInstanceKey);
          consumerThread.start();

          log.info("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : get information from deliveryManager "+deliveryManager+" DONE");
        
          break;

        default:
          log.info("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : skip deliveryManager "+deliveryManager);
          break;
        }
      }
      
      // -------------------------------
      // skip all other managers
      // -------------------------------
      
      else{
        log.info("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : skip deliveryManager "+deliveryManager);
      }
    }
  }

  /*****************************************
  *
  *  class CommodityDeliveryRequest
  *
  *****************************************/

  public static class CommodityDeliveryRequest extends DeliveryRequest implements BonusDelivery
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
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),8));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("externalSubscriberID", Schema.OPTIONAL_STRING_SCHEMA);
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

    private String externalSubscriberID;
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
    
    //
    //  accessors
    //

    public String getExternalSubscriberID() { return externalSubscriberID; }
    public String getProviderID() { return providerID; }
    public String getProviderName() { return Deployment.getFulfillmentProviders().get(getProviderID()).getProviderName(); }
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
        return EvolutionUtilities.addTime(getDeliveryDate(), validityPeriodQuantity, validityPeriodType, Deployment.getBaseTimeZone(), EvolutionUtilities.RoundingSelection.NoRound);
      }
      // should be null here
      return deliverableExpirationDate;
    }    
    public CommodityDeliveryStatus getCommodityDeliveryStatus() { return commodityDeliveryStatus; }
    public String getStatusMessage() { return statusMessage; }

    //
    //  setters
    //

    public void setCommodityDeliveryStatus(CommodityDeliveryStatus status) { this.commodityDeliveryStatus = status; }
    public void setStatusMessage(String statusMessage) { this.statusMessage = statusMessage; }
    public void setDeliverableExpirationDate(Date deliverableExpirationDate) { this.deliverableExpirationDate = deliverableExpirationDate; }

    //
    //  bonus delivery accessors
    //

    public int getBonusDeliveryReturnCode() { return getCommodityDeliveryStatus().getReturnCode(); }
    public String getBonusDeliveryReturnCodeDetails() { return getStatusMessage(); }
    public String getBonusDeliveryOrigin() { return ""; }
    public String getBonusDeliveryProviderId() { return getProviderID(); }
    public String getBonusDeliveryDeliverableId() { return getCommodityID(); }
    public String getBonusDeliveryDeliverableName() { return getCommodityName(); }
    public int getBonusDeliveryDeliverableQty() { return getAmount(); }
    public String getBonusDeliveryOperation() { return getOperation().getExternalRepresentation(); }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public CommodityDeliveryRequest(EvolutionEventContext context, String externalSubscriberID, String deliveryRequestSource, Map<String, String> diplomaticBriefcase, String providerID, String commodityID, CommodityDeliveryOperation operation, int amount, TimeUnit validityPeriodType, Integer validityPeriodQuantity, Date deliverableExpirationDate)
    {
      super(context, "commodityDelivery", deliveryRequestSource);
      this.externalSubscriberID = externalSubscriberID;
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
    }

    /*****************************************
    *
    *  constructor -- external
    *
    *****************************************/

    public CommodityDeliveryRequest(DeliveryRequest originatingRequet,JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
    {
      super(originatingRequet,jsonRoot);
      this.setCorrelator(JSONUtilities.decodeString(jsonRoot, "correlator", false));
      this.externalSubscriberID = JSONUtilities.decodeString(jsonRoot, "externalSubscriberID", false);
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
    }

    /*****************************************
     *
     *  constructor -- external
     *
     *****************************************/

    public CommodityDeliveryRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader,JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
    {
      super(subscriberProfile,subscriberGroupEpochReader,jsonRoot);
      this.setCorrelator(JSONUtilities.decodeString(jsonRoot, "correlator", false));
      this.externalSubscriberID = JSONUtilities.decodeString(jsonRoot, "externalSubscriberID", false);
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
    }

    /*****************************************
    *  
    *  to JSONObject
    *
    *****************************************/

    public JSONObject getJSONRepresentation(){
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
      data.put("externalSubscriberID", this.getExternalSubscriberID());
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

      return JSONUtilities.encodeObject(data);
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private CommodityDeliveryRequest(SchemaAndValue schemaAndValue, String externalSubscriberID, String providerID, String commodityID, String commodityName, CommodityDeliveryOperation operation, int amount, TimeUnit validityPeriodType, Integer validityPeriodQuantity, Date deliverableExpirationDate, CommodityDeliveryStatus status, String statusMessage)
    {
      super(schemaAndValue);
      this.externalSubscriberID = externalSubscriberID;
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
    }

    /*****************************************
    *
    *  constructor -- copy
    *
    *****************************************/

    private CommodityDeliveryRequest(CommodityDeliveryRequest commodityDeliveryRequest)
    {
      super(commodityDeliveryRequest);
      this.externalSubscriberID = commodityDeliveryRequest.externalSubscriberID;
      this.providerID = commodityDeliveryRequest.getProviderID();
      this.commodityID = commodityDeliveryRequest.getCommodityID();
      this.operation = commodityDeliveryRequest.getOperation();
      this.amount = commodityDeliveryRequest.getAmount();
      this.validityPeriodType = commodityDeliveryRequest.getValidityPeriodType();
      this.validityPeriodQuantity = commodityDeliveryRequest.getValidityPeriodQuantity();
      this.deliverableExpirationDate = commodityDeliveryRequest.getDeliverableExpirationDate();
      this.commodityDeliveryStatus = commodityDeliveryRequest.getCommodityDeliveryStatus();
      this.statusMessage = commodityDeliveryRequest.getStatusMessage();
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
      struct.put("externalSubscriberID", commodityDeliveryRequest.getExternalSubscriberID());
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
      String externalSubscriberID = (schemaVersion >= 3) ? valueStruct.getString("externalSubscriberID") : "";
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

      //
      //  return
      //

      return new CommodityDeliveryRequest(schemaAndValue, externalSubscriberID, providerID, commodityID, commodityName, operation, amount, validityPeriodType, validityPeriodQuantity, deliverableExpirationDate, status, statusMessage);
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
      b.append("}");
      return b.toString();
    }
    
    @Override public ActivityType getActivityType() { return ActivityType.BDR; }
    
    /****************************************
    *
    *  presentation utilities
    *
    ****************************************/
    
    @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService)
    {
      Module module = Module.fromExternalRepresentation(getModuleID());
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
      guiPresentationMap.put(MODULENAME, module.toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      guiPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      guiPresentationMap.put(ORIGIN, "");
      guiPresentationMap.put(RETURNCODE, getCommodityDeliveryStatus().getReturnCode());
      guiPresentationMap.put(RETURNCODEDETAILS, getCommodityDeliveryStatus().toString());
    }
    
    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService)
    {
      Module module = Module.fromExternalRepresentation(getModuleID());
      Date now = SystemTime.getCurrentTime();
      thirdPartyPresentationMap.put(PROVIDERID, getProviderID());
      thirdPartyPresentationMap.put(PROVIDERNAME, Deployment.getFulfillmentProviders().get(getProviderID()).getProviderName());
      thirdPartyPresentationMap.put(DELIVERABLEID, getCommodityID());
      thirdPartyPresentationMap.put(DELIVERABLENAME, (deliverableService.getActiveDeliverable(getCommodityID(), now) != null ? deliverableService.getActiveDeliverable(getCommodityID(), now).getDeliverableName() : getCommodityID()));
      thirdPartyPresentationMap.put(DELIVERABLEDISPLAY, (deliverableService.getActiveDeliverable(getCommodityID(), now) != null ? deliverableService.getActiveDeliverable(getCommodityID(), now).getGUIManagedObjectDisplay() : getCommodityID()));
      thirdPartyPresentationMap.put(DELIVERABLEQTY, getAmount());
      thirdPartyPresentationMap.put(OPERATION, getOperation().getExternalRepresentation());
      thirdPartyPresentationMap.put(VALIDITYPERIODTYPE, getValidityPeriodType().getExternalRepresentation());
      thirdPartyPresentationMap.put(VALIDITYPERIODQUANTITY, getValidityPeriodQuantity());
      thirdPartyPresentationMap.put(DELIVERABLEEXPIRATIONDATE, getDateString(getDeliverableExpirationDate()));
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, module.toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(ORIGIN, "");
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
  *  sendCommodityDeliveryRequest
  *
  *****************************************/

  public static void sendCommodityDeliveryRequest(DeliveryRequest originatingDeliveryRequest, JSONObject briefcase, String applicationID, String deliveryRequestID, String originatingDeliveryRequestID, boolean originatingRequest, String eventID, String moduleID, String featureID, String subscriberID, String providerID, String commodityID, CommodityDeliveryOperation operation, long amount, TimeUnit validityPeriodType, Integer validityPeriodQuantity){

    log.info("CommodityDeliveryManager.sendCommodityDeliveryRequest(..., "+subscriberID+", "+providerID+", "+commodityID+", "+operation+", "+amount+", "+validityPeriodType+", "+validityPeriodQuantity+", ...) : method called ...");

    // ---------------------------------
    //
    // generate the request
    //
    // ---------------------------------
    
    HashMap<String,Object> requestData = new HashMap<String,Object>();

    requestData.put("deliveryRequestID", deliveryRequestID);
    requestData.put("originatingRequest", originatingRequest);
    requestData.put("originatingDeliveryRequestID", originatingDeliveryRequestID);
    requestData.put("deliveryType", "commodityDelivery");

    requestData.put("eventID", eventID);
    requestData.put("moduleID", moduleID);
    requestData.put("featureID", featureID);

    requestData.put("subscriberID", subscriberID);
    //TODO:fix that hack
    requestData.put("externalSubscriberID", subscriberID);
    requestData.put("providerID", providerID);
    requestData.put("commodityID", commodityID);
    requestData.put("operation", operation.getExternalRepresentation());
    requestData.put("amount", amount);
    requestData.put("validityPeriodType", (validityPeriodType != null ? validityPeriodType.getExternalRepresentation() : null));
    requestData.put("validityPeriodQuantity", validityPeriodQuantity);

    requestData.put("commodityDeliveryStatusCode", CommodityDeliveryStatus.PENDING.getReturnCode());

    Map<String,String> diplomaticBriefcase = new HashMap<String,String>();
    if(applicationID != null){diplomaticBriefcase.put(APPLICATION_ID, applicationID);}
    if(briefcase != null){diplomaticBriefcase.put(APPLICATION_BRIEFCASE, briefcase.toJSONString());}
    requestData.put("diplomaticBriefcase", diplomaticBriefcase);
    
    CommodityDeliveryRequest commodityDeliveryRequest = new CommodityDeliveryRequest(originatingDeliveryRequest,JSONUtilities.encodeObject(requestData), Deployment.getDeliveryManagers().get("commodityDelivery"));
    
    // ---------------------------------
    //
    // send the request
    //
    // ---------------------------------
    
    // get kafka producer

    DeliveryManagerDeclaration deliveryManagerDeclaration = Deployment.getDeliveryManagers().get("commodityDelivery");
    String requestTopic = deliveryManagerDeclaration.getDefaultRequestTopic();

    // send the request
    
    commodityDeliveryRequestProducer.send(new ProducerRecord<byte[], byte[]>(requestTopic, StringKey.serde().serializer().serialize(requestTopic, new StringKey(commodityDeliveryRequest.getDeliveryRequestID())), ((ConnectSerde<DeliveryRequest>)deliveryManagerDeclaration.getRequestSerde()).serializer().serialize(requestTopic, commodityDeliveryRequest))); 

    log.info("CommodityDeliveryManager.sendCommodityDeliveryRequest(..., "+subscriberID+", "+providerID+", "+commodityID+", "+operation+", "+amount+", "+validityPeriodType+", "+validityPeriodQuantity+", ...) : DONE");
  }

  public static void sendCommodityDeliveryRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, JSONObject briefcase, String applicationID, String deliveryRequestID, String originatingDeliveryRequestID, boolean originatingRequest, String eventID, String moduleID, String featureID, String subscriberID, String providerID, String commodityID, CommodityDeliveryOperation operation, long amount, TimeUnit validityPeriodType, Integer validityPeriodQuantity){

    log.info("CommodityDeliveryManager.sendCommodityDeliveryRequest(..., "+subscriberID+", "+providerID+", "+commodityID+", "+operation+", "+amount+", "+validityPeriodType+", "+validityPeriodQuantity+", ...) : method called ...");

    // ---------------------------------
    //
    // generate the request
    //
    // ---------------------------------

    HashMap<String,Object> requestData = new HashMap<String,Object>();

    requestData.put("deliveryRequestID", deliveryRequestID);
    requestData.put("originatingRequest", originatingRequest);
    requestData.put("originatingDeliveryRequestID", originatingDeliveryRequestID);
    requestData.put("deliveryType", "commodityDelivery");

    requestData.put("eventID", eventID);
    requestData.put("moduleID", moduleID);
    requestData.put("featureID", featureID);

    requestData.put("subscriberID", subscriberID);
    //TODO:fix that hack
    requestData.put("externalSubscriberID", subscriberID);
    requestData.put("providerID", providerID);
    requestData.put("commodityID", commodityID);
    requestData.put("operation", operation.getExternalRepresentation());
    requestData.put("amount", amount);
    requestData.put("validityPeriodType", (validityPeriodType != null ? validityPeriodType.getExternalRepresentation() : null));
    requestData.put("validityPeriodQuantity", validityPeriodQuantity);

    requestData.put("commodityDeliveryStatusCode", CommodityDeliveryStatus.PENDING.getReturnCode());

    Map<String,String> diplomaticBriefcase = new HashMap<String,String>();
    if(applicationID != null){diplomaticBriefcase.put(APPLICATION_ID, applicationID);}
    if(briefcase != null){diplomaticBriefcase.put(APPLICATION_BRIEFCASE, briefcase.toJSONString());}
    requestData.put("diplomaticBriefcase", diplomaticBriefcase);

    CommodityDeliveryRequest commodityDeliveryRequest = new CommodityDeliveryRequest(subscriberProfile, subscriberGroupEpochReader, JSONUtilities.encodeObject(requestData), Deployment.getDeliveryManagers().get("commodityDelivery"));

    // ---------------------------------
    //
    // send the request
    //
    // ---------------------------------

    // get kafka producer

    DeliveryManagerDeclaration deliveryManagerDeclaration = Deployment.getDeliveryManagers().get("commodityDelivery");
    String requestTopic = deliveryManagerDeclaration.getDefaultRequestTopic();

    // send the request

    commodityDeliveryRequestProducer.send(new ProducerRecord<byte[], byte[]>(requestTopic, StringKey.serde().serializer().serialize(requestTopic, new StringKey(commodityDeliveryRequest.getDeliveryRequestID())), ((ConnectSerde<DeliveryRequest>)deliveryManagerDeclaration.getRequestSerde()).serializer().serialize(requestTopic, commodityDeliveryRequest)));

    log.info("CommodityDeliveryManager.sendCommodityDeliveryRequest(..., "+subscriberID+", "+providerID+", "+commodityID+", "+operation+", "+amount+", "+validityPeriodType+", "+validityPeriodQuantity+", ...) : DONE");
  }
  
  /*****************************************
  *
  *  addCommodityDeliveryResponseConsumer
  *
  *****************************************/

  public static void addCommodityDeliveryResponseConsumer(String applicationID, CommodityDeliveryResponseHandler commodityDeliveryConsumer){
    DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("commodityDelivery");
    String responseTopic = deliveryManager.getResponseTopic();
    String prefix = "CommodityDeliveryResponseConsumer_"+applicationID;
    Thread consumerThread = new Thread(new Runnable(){
      private volatile boolean stopping=false;
      @Override
      public void run()
      {
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
        consumerProperties.put("group.id", prefix+"_"+"requestReader");
        consumerProperties.put("auto.offset.reset", "earliest");
        consumerProperties.put("enable.auto.commit", "false");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties);
        consumer.subscribe(Arrays.asList(responseTopic));
        NGLMRuntime.addShutdownHook(normalShutdown -> stopping=true);
        log.info("CommodityDeliveryManager.addCommodityDeliveryResponseConsumer(...) : added kafka consumer for application "+applicationID);

        while(!stopping){

          // poll

          long lastPollTime=System.currentTimeMillis();// just to log if exception happened later, can be because of this, but most likely because of rebalance because of new consumer created
          ConsumerRecords<byte[], byte[]> fileRecords = consumer.poll(5000);

          //  process records

          try{
            for (ConsumerRecord<byte[], byte[]> fileRecord : fileRecords)
            {
              //  parse
              DeliveryRequest response = deliveryManager.getRequestSerde().deserializer().deserialize(responseTopic, fileRecord.value());
              if(response.getDiplomaticBriefcase() != null && response.getDiplomaticBriefcase().get(APPLICATION_ID) != null && response.getDiplomaticBriefcase().get(APPLICATION_ID).equals(applicationID)){
                commodityDeliveryConsumer.handleCommodityDeliveryResponse(response);
              }
            }

            //
            //  commit offsets
            //

            consumer.commitSync();
          }catch (CommitFailedException ex){
            long lastPoll_ms=System.currentTimeMillis()-lastPollTime;
            log.info(Thread.currentThread().getId()+" CommodityDeliveryManager : CommitFailedException catched, can be normal rebalancing or poll time interval too long, last was "+lastPoll_ms+"ms ago");
          }

        }
        consumer.close();
        log.warn(Thread.currentThread().getId()+" CommodityDeliveryManager.addCommodityDeliveryResponseConsumer : STOPPING reading response from "+responseTopic);
      }
    }, "consumer_"+prefix);
    consumerThread.start();

  }
  
  /*****************************************
  *
  *  handleThirdPartyResponse
  *
  *****************************************/

  private void handleThirdPartyResponse(DeliveryRequest response){

    //
    // Getting initial request
    //

    if(response.getDiplomaticBriefcase() == null || response.getDiplomaticBriefcase().get(COMMODITY_DELIVERY_BRIEFCASE) == null || response.getDiplomaticBriefcase().get(COMMODITY_DELIVERY_BRIEFCASE).isEmpty()){
      log.warn(Thread.currentThread().getId()+" - CommodityDeliveryManager.handleThirdPartirResponse(response) : can not get purchase status => ignore this response");
      return;
    }
    JSONParser parser = new JSONParser();
    CommodityDeliveryRequest commodityDeliveryRequest = null;
    try
      {
        JSONObject requestStatusJSON = (JSONObject) parser.parse(response.getDiplomaticBriefcase().get(COMMODITY_DELIVERY_BRIEFCASE));
        commodityDeliveryRequest = new CommodityDeliveryRequest(response, requestStatusJSON, Deployment.getDeliveryManagers().get("commodityDelivery"));
      } catch (ParseException e)
      {
        log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager.handleThirdPartirResponse(...) : ERROR while getting request status from '"+response.getDiplomaticBriefcase().get(COMMODITY_DELIVERY_BRIEFCASE)+"' => IGNORED");
        return;
      }
    if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - CommodityDeliveryManager.handleThirdPartirResponse(...) : getting commodity status DONE : "+commodityDeliveryRequest);

    //
    // extract validityPeriod from response
    //

    if(response instanceof PointFulfillmentRequest) {
      commodityDeliveryRequest.setDeliverableExpirationDate(((PointFulfillmentRequest)response).getDeliverableExpirationDate());
    }
    
    //
    // Handle response
    //

    DeliveryStatus responseDeliveryStatus = response.getDeliveryStatus();
    switch (responseDeliveryStatus) {
    case Delivered:
      submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.SUCCESS, "Success", commodityDeliveryRequest.getDeliverableExpirationDate());
      break;

    case FailedRetry:
    case Indeterminate:
    case Failed:
    case FailedTimeout:
      submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.THIRD_PARTY_ERROR, "Commodity delivery request failed", commodityDeliveryRequest.getDeliverableExpirationDate());
      break;
    case Pending:
    case Unknown:
    default:
      submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.THIRD_PARTY_ERROR, "Commodity delivery request failure", commodityDeliveryRequest.getDeliverableExpirationDate());
      break;
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
        CommodityDeliveryRequest commodityDeliveryRequest = ((CommodityDeliveryRequest)deliveryRequest);
        if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - CommodityDeliveryManager : recieved new CommodityDeliveryRequest : "+commodityDeliveryRequest);

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
        *  get delivery details (providerID, commodityID, customerID, ...)
        *
        *****************************************/
        
        String subscriberID = commodityDeliveryRequest.getSubscriberID();
        String providerID = commodityDeliveryRequest.getProviderID();
        String commodityID = commodityDeliveryRequest.getCommodityID();
        CommodityDeliveryOperation operation = commodityDeliveryRequest.getOperation();
        int amount = commodityDeliveryRequest.getAmount();
        
        //
        // Get amount
        //
        
        if(amount < 1){
          log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : bad field value for amount");
          submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.BAD_FIELD_VALUE, "bad field value for amount (must be greater than 0, but recieved "+amount+")", null);
          continue;
        }
        
        //
        // Get customer
        //
        
        if(subscriberID == null){
          log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : bad field value for subscriberID");
          submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.MISSING_PARAMETERS, "missing mandatoryfield (subscriberID)", null);
          continue;
        }

        //
        // Check commodity exists
        //
        
        String externalAccountID = null;
        CommodityType commodityType = null;
        String deliveryType = null;
        PaymentMean paymentMean = null;
        Deliverable deliverable = null;
        if(operation.equals(CommodityDeliveryOperation.Debit)){
          
          //
          // Debit => check in paymentMean list
          //
          
          paymentMean = paymentMeanService.getActivePaymentMean(commodityID, SystemTime.getCurrentTime());
          if(paymentMean == null){
            log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : paymentMean not found ");
            submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.BONUS_NOT_FOUND, "payment mean not found (providerID "+providerID+" - commodityID "+commodityID+")", null);
            continue;
          }else{
            externalAccountID = paymentMean.getExternalAccountID();
            DeliveryManagerDeclaration provider = Deployment.getFulfillmentProviders().get(paymentMean.getFulfillmentProviderID());
            if(provider == null){
              log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : paymentMean not found ");
              submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.BONUS_NOT_FOUND, "provider of payment mean not found (providerID "+providerID+" - commodityID "+commodityID+")", null);
              continue;
            }else{
              commodityType = provider.getProviderType();
              deliveryType = provider.getDeliveryType();
            }
          }
          
        }else if(operation.equals(CommodityDeliveryOperation.Credit)){
          
          //
          // Credit => check in commodity list
          //
          
          deliverable = deliverableService.getActiveDeliverable(commodityID, SystemTime.getCurrentTime());
          if(deliverable == null){
            log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : commodity not found ");
            submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.BONUS_NOT_FOUND, "commodity not found (providerID "+providerID+" - commodityID "+commodityID+")", null);
            continue;
          }else{
            externalAccountID = deliverable.getExternalAccountID();
            DeliveryManagerDeclaration provider = Deployment.getFulfillmentProviders().get(deliverable.getFulfillmentProviderID());
            if(provider == null){
              log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : paymentMean not found ");
              submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.BONUS_NOT_FOUND, "provider of deliverable not found (providerID "+providerID+" - commodityID "+commodityID+")", null);
              continue;
            }else{
              commodityType = provider.getProviderType();
              deliveryType = provider.getDeliveryType();
            }
          }
          
        }else if(operation.equals(CommodityDeliveryOperation.Set)){
            
            //
            // Set => check in commodity list
            //
            
            deliverable = deliverableService.getActiveDeliverable(commodityID, SystemTime.getCurrentTime());
            if(deliverable == null){
              log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : commodity not found ");
              submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.BONUS_NOT_FOUND, "commodity not found (providerID "+providerID+" - commodityID "+commodityID+")", null);
              continue;
            }else{
              externalAccountID = deliverable.getExternalAccountID();
              DeliveryManagerDeclaration provider = Deployment.getFulfillmentProviders().get(deliverable.getFulfillmentProviderID());
              if(provider == null){
                log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : paymentMean not found ");
                submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.BONUS_NOT_FOUND, "provider of deliverable not found (providerID "+providerID+" - commodityID "+commodityID+")", null);
                continue;
              }else{
                commodityType = provider.getProviderType();
                deliveryType = provider.getDeliveryType();
              }
            }
            
        }else if(operation.equals(CommodityDeliveryOperation.Activate)){
            
            //
            // Activate => check in commodity list
            //
            
            deliverable = deliverableService.getActiveDeliverable(commodityID, SystemTime.getCurrentTime());
            if(deliverable == null){
              log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : commodity not found ");
              submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.BONUS_NOT_FOUND, "commodity not found (providerID "+providerID+" - commodityID "+commodityID+")", null);
              continue;
            }else{
              externalAccountID = deliverable.getExternalAccountID();
              DeliveryManagerDeclaration provider = Deployment.getFulfillmentProviders().get(deliverable.getFulfillmentProviderID());
              if(provider == null){
                log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : paymentMean not found ");
                submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.BONUS_NOT_FOUND, "provider of deliverable not found (providerID "+providerID+" - commodityID "+commodityID+")", null);
                continue;
              }else{
                commodityType = provider.getProviderType();
                deliveryType = provider.getDeliveryType();
              }
            }
            
        }else if(operation.equals(CommodityDeliveryOperation.Check)){
          
          //
          // Check => check in commodity list
          //
          
          deliverable = deliverableService.getActiveDeliverable(commodityID, SystemTime.getCurrentTime());
          if(deliverable == null){
            log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : commodity not found ");
            submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.BONUS_NOT_FOUND, "commodity not found (providerID "+providerID+" - commodityID "+commodityID+")", null);
            continue;
          }else{
            externalAccountID = deliverable.getExternalAccountID();
            DeliveryManagerDeclaration provider = Deployment.getFulfillmentProviders().get(deliverable.getFulfillmentProviderID());
            if(provider == null){
              log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : paymentMean not found ");
              submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.BONUS_NOT_FOUND, "provider of deliverable not found (providerID "+providerID+" - commodityID "+commodityID+")", null);
              continue;
            }else{
              commodityType = provider.getProviderType();
              deliveryType = provider.getDeliveryType();
            }
          }
          
        }else{

          //
          // unknown operation => return an error
          //
          
          log.error(Thread.currentThread().getId()+" - CommodityDeliveryManager (provider "+providerID+", commodity "+commodityID+", operation "+operation.getExternalRepresentation()+", amount "+amount+") : unknown operation");
          submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.SYSTEM_ERROR, "unknown operation", null);
          continue;
        }

        /*****************************************
        *
        *  Proceed with the commodity action
        *
        *****************************************/

        proceedCommodityDeliveryRequest(commodityDeliveryRequest, commodityType, deliveryType, externalAccountID, deliverable);
      }
  }

  /*****************************************
  *
  *  CorrelatorUpdate
  *
  *****************************************/

  private void submitCorrelatorUpdate(String correlator, CommodityDeliveryStatus commodityDeliveryStatus, String statusMessage, Date deliverableExpirationDate){
    Map<String, Object> correlatorUpdate = new HashMap<String, Object>();
    correlatorUpdate.put("resultCode", commodityDeliveryStatus.externalRepresentation);
    correlatorUpdate.put("statusMessage", statusMessage);
    correlatorUpdate.put("deliverableExpirationDate", deliverableExpirationDate);
    submitCorrelatorUpdate(correlator, JSONUtilities.encodeObject(correlatorUpdate));
  }

  @Override protected void processCorrelatorUpdate(DeliveryRequest deliveryRequest, JSONObject correlatorUpdate)
  {
    if(log.isDebugEnabled()) log.debug("CommodityDeliveryManager.processCorrelatorUpdate("+deliveryRequest.getDeliveryRequestID()+", "+correlatorUpdate+") : called ...");

    CommodityDeliveryRequest commodityDeliveryRequest = (CommodityDeliveryRequest) deliveryRequest;
    if (commodityDeliveryRequest != null)
      {
        int result = JSONUtilities.decodeInteger(correlatorUpdate, "resultCode", true);
        Date deliverableExpirationDate = JSONUtilities.decodeDate(correlatorUpdate, "deliverableExpirationDate");
        CommodityDeliveryStatus commodityDeliveryStatus = CommodityDeliveryStatus.fromReturnCode(result);
        String statusMessage = JSONUtilities.decodeString(correlatorUpdate, "statusMessage", false);
        commodityDeliveryRequest.setCommodityDeliveryStatus(commodityDeliveryStatus);
        commodityDeliveryRequest.setDeliveryStatus(getDeliveryStatus(commodityDeliveryStatus));
        commodityDeliveryRequest.setStatusMessage(statusMessage);
        commodityDeliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
        commodityDeliveryRequest.setDeliverableExpirationDate(deliverableExpirationDate);
        completeRequest(commodityDeliveryRequest);
      }

    bdrStats.updateBDREventCount(1, deliveryRequest.getDeliveryStatus());

    if(log.isDebugEnabled()) log.debug("CommodityDeliveryManager.processCorrelatorUpdate("+deliveryRequest.getDeliveryRequestID()+", "+correlatorUpdate+") : DONE");

  }

  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  @Override protected void shutdown()
  {
    log.info(Thread.currentThread().getId()+" CommodityDeliveryManager: shutdown called");

    synchronized (CommodityDeliveryManager.class){
      if(paymentMeanService!=null){
        log.info(Thread.currentThread().getId()+" CommodityDeliveryManager: stopping paymentMeanService");
        paymentMeanService.stop();
        paymentMeanService=null;
      }
      if(deliverableService!=null){
        log.info(Thread.currentThread().getId()+" CommodityDeliveryManager: stopping deliverableService");
        deliverableService.stop();
        deliverableService=null;
      }
      if(bdrStats!=null){
        log.info(Thread.currentThread().getId()+" CommodityDeliveryManager: stopping bdrStats");
        bdrStats.unregister();
        bdrStats=null;
      }
      if(zookeeperUniqueKeyServer!=null){
        log.info(Thread.currentThread().getId()+" CommodityDeliveryManager: stopping zookeeperUniqueKeyServer");
        zookeeperUniqueKeyServer.close();
        zookeeperUniqueKeyServer=null;
      }
    }

    if(providerRequestProducers!=null && !providerRequestProducers.isEmpty()){
      log.info(Thread.currentThread().getId()+" CommodityDeliveryManager: stopping all providerRequestProducers");
      providerRequestProducers.forEach((providerName,kafkaProducer)->{
        log.info(Thread.currentThread().getId()+" CommodityDeliveryManager: stopping producer for "+providerName);
        kafkaProducer.close();
      });
    }

    log.info("CommodityDeliveryManager: shutdown DONE");
  }
  
  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    new LoggerInitialization().initLogger();
    log.info("CommodityDeliveryManager: recieved " + args.length + " args :");
    for(int index = 0; index < args.length; index++){
      log.info("       args["+index+"] " + args[index]);
    }
    
    //
    //  configuration
    //

    String deliveryManagerKey = args[0];
    int instancesNumber=1;//default, but should come from args conf
    try{
      instancesNumber = Integer.parseInt(args[1]);
    }catch(NumberFormatException|IndexOutOfBoundsException ex){
      log.error("CommodityDeliveryManager: could not get instances number configuration ! will start with default "+instancesNumber);
    }

    synchronized (CommodityDeliveryManager.class){

      //
      // configuration services
      //

      paymentMeanService = new PaymentMeanService(Deployment.getBrokerServers(), "CommodityMgr-paymentmeanservice-"+deliveryManagerKey, Deployment.getPaymentMeanTopic(), false);
      paymentMeanService.start();

      deliverableService = new DeliverableService(Deployment.getBrokerServers(), "CommodityMgr-deliverableservice-"+deliveryManagerKey, Deployment.getDeliverableTopic(), false);
      deliverableService.start();

      try{
        bdrStats = new BDRStatistics(COMMODITY_DELIVERY_ID_VALUE+"-"+deliveryManagerKey);
      }catch(Exception e){
        log.error("CommodityDeliveryManager : could not load statistics ", e);
        throw new RuntimeException("CommodityDeliveryManager : could not load statistics  ", e);
      }

      //
      //  unique key server
      //

      zookeeperUniqueKeyServer = new ZookeeperUniqueKeyServer("commoditydelivery");
    }

    //
    //  instances
    //
    
    List<CommodityDeliveryManager> managers=new ArrayList<>();
    for(int i=0;i<instancesNumber;i++){
      String commodityDeliveryManagerInstanceKey = deliveryManagerKey+"-"+i;
      managers.add(new CommodityDeliveryManager(commodityDeliveryManagerInstanceKey));
    }

    //
    //  starting delivery
    //
    log.info("CommodityDeliveryManager: starting managers delivery");
    managers.stream().forEach(manager->manager.startDelivery());


    //
    // logging periodic few kpi
    //

    while(true){
      log.info("CommodityDeliveryManager: "+managers.stream().filter(manager->manager.isProcessing()).count()+" running managers over "+managers.size()+" ("+instancesNumber+" starting conf)");
      log.info("CommodityDeliveryManager: JVM approximate free memory "+FileUtils.byteCountToDisplaySize(Runtime.getRuntime().freeMemory())+", over "+FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory()));
      try{
        Thread.sleep(300*1000L);
      }catch (InterruptedException e){
        log.warn("CommodityDeliveryManager: ending main on exception",e);
        break;
      }
    }

    log.warn("CommodityDeliveryManager: main thread ending");

  }
  
  /*****************************************
  *
  *  proceed with commodity request
  *
  *****************************************/

  private void proceedCommodityDeliveryRequest(CommodityDeliveryRequest commodityDeliveryRequest, CommodityType commodityType, String deliveryType, String externalAccountID, Deliverable deliverable){

    if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+"CommodityDeliveryManager.proceedCommodityDeliveryRequest(..., "+commodityType+", "+deliveryType+") : method called ...");

    //
    // add identifier in briefcase (used later to filter responses) 
    //
    
    Map<String, String> diplomaticBriefcase = commodityDeliveryRequest.getDiplomaticBriefcase();
    if(diplomaticBriefcase == null){
      diplomaticBriefcase = new HashMap<String, String>();
    }
    diplomaticBriefcase.put(COMMODITY_DELIVERY_ID, COMMODITY_DELIVERY_ID_VALUE);
    diplomaticBriefcase.put(COMMODITY_DELIVERY_BRIEFCASE, commodityDeliveryRequest.getJSONRepresentation().toJSONString());

    //
    // execute commodity request
    //

    String validityPeriodType = commodityDeliveryRequest.getValidityPeriodType() != null ? commodityDeliveryRequest.getValidityPeriodType().getExternalRepresentation() : null;
    Integer validityPeriodQuantity = commodityDeliveryRequest.getValidityPeriodQuantity();
    String newDeliveryRequestID = zookeeperUniqueKeyServer.getStringKey();
    switch (commodityType) {
    case IN:
      
      if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : generating "+CommodityType.IN+" request ...");
      
      DeliveryManagerDeclaration inManagerDeclaration = Deployment.getDeliveryManagers().get(deliveryType);
      String inRequestTopic = inManagerDeclaration.getDefaultRequestTopic();

      HashMap<String,Object> inRequestData = new HashMap<String,Object>();
      
      inRequestData.put("deliveryRequestID", newDeliveryRequestID);
      inRequestData.put("originatingRequest", false);
      inRequestData.put("deliveryType", deliveryType);

      inRequestData.put("eventID", commodityDeliveryRequest.getEventID());
      inRequestData.put("moduleID", commodityDeliveryRequest.getModuleID());
      inRequestData.put("featureID", commodityDeliveryRequest.getFeatureID());

      inRequestData.put("subscriberID", commodityDeliveryRequest.getSubscriberID());
      inRequestData.put("providerID", commodityDeliveryRequest.getProviderID());
      inRequestData.put("commodityID", commodityDeliveryRequest.getCommodityID());
	  inRequestData.put("externalAccountID", externalAccountID);

      inRequestData.put("operation", CommodityDeliveryOperation.fromExternalRepresentation(commodityDeliveryRequest.getOperation().getExternalRepresentation()).getExternalRepresentation());
      inRequestData.put("amount", commodityDeliveryRequest.getAmount());
      inRequestData.put("validityPeriodType", validityPeriodType);
      inRequestData.put("validityPeriodQuantity", validityPeriodQuantity);
      inRequestData.put("diplomaticBriefcase", diplomaticBriefcase);
      inRequestData.put("dateFormat", "yyyy-MM-dd'T'HH:mm:ss:XX");

      if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : generating "+CommodityType.IN+" request DONE");
      
      INFulfillmentRequest inRequest = new INFulfillmentRequest(commodityDeliveryRequest,JSONUtilities.encodeObject(inRequestData), Deployment.getDeliveryManagers().get(deliveryType));
      KafkaProducer kafkaProducer = providerRequestProducers.get(commodityDeliveryRequest.getProviderID());
      if(kafkaProducer != null){
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(inRequestTopic, StringKey.serde().serializer().serialize(inRequestTopic, new StringKey(inRequest.getDeliveryRequestID())), ((ConnectSerde<DeliveryRequest>)inManagerDeclaration.getRequestSerde()).serializer().serialize(inRequestTopic, inRequest))); 
      }else{
        submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.SYSTEM_ERROR, "Could not send delivery request to provider (providerID = "+commodityDeliveryRequest.getProviderID()+")", null);
      }
      
      if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : sending "+CommodityType.IN+" request DONE");

      break;

    case POINT:

      DeliveryManagerDeclaration pointManagerDeclaration = Deployment.getDeliveryManagers().get(deliveryType);
      String pointRequestTopic = pointManagerDeclaration.getDefaultRequestTopic();

      HashMap<String,Object> pointRequestData = new HashMap<String,Object>();
      
      pointRequestData.put("deliveryRequestID", newDeliveryRequestID);
      pointRequestData.put("originatingRequest", false);
      pointRequestData.put("deliveryType", deliveryType);

      pointRequestData.put("eventID", commodityDeliveryRequest.getEventID());
      pointRequestData.put("moduleID", commodityDeliveryRequest.getModuleID());
      pointRequestData.put("featureID", commodityDeliveryRequest.getFeatureID());

      pointRequestData.put("subscriberID", commodityDeliveryRequest.getSubscriberID());
      pointRequestData.put("pointID", externalAccountID);
      
      pointRequestData.put("operation", commodityDeliveryRequest.getOperation().getExternalRepresentation());
      pointRequestData.put("amount", commodityDeliveryRequest.getAmount());
      pointRequestData.put("validityPeriodType", validityPeriodType);
      pointRequestData.put("validityPeriodQuantity", validityPeriodQuantity);
      pointRequestData.put("diplomaticBriefcase", diplomaticBriefcase);
      
      if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : generating "+CommodityType.POINT+" request DONE");

      PointFulfillmentRequest pointRequest = new PointFulfillmentRequest(commodityDeliveryRequest, JSONUtilities.encodeObject(pointRequestData), Deployment.getDeliveryManagers().get(deliveryType));
      KafkaProducer pointProducer = providerRequestProducers.get(commodityDeliveryRequest.getProviderID());
      if(pointProducer != null){
        pointProducer.send(new ProducerRecord<byte[], byte[]>(pointRequestTopic, StringKey.serde().serializer().serialize(pointRequestTopic, new StringKey(pointRequest.getSubscriberID())), ((ConnectSerde<DeliveryRequest>)pointManagerDeclaration.getRequestSerde()).serializer().serialize(pointRequestTopic, pointRequest))); 
      }else{
        submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.SYSTEM_ERROR, "Could not send delivery request to provider (providerID = "+commodityDeliveryRequest.getProviderID()+")", null);
      }
      
      if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : sending "+CommodityType.POINT+" request DONE");

      break;

    case REWARD:

      DeliveryManagerDeclaration rewardManagerDeclaration = Deployment.getDeliveryManagers().get(deliveryType);
      String rewardRequestTopic = rewardManagerDeclaration.getDefaultRequestTopic();

      //
      //  request (JSON)
      //

      HashMap<String,Object> rewardRequestData = new HashMap<String,Object>();
      rewardRequestData.put("deliveryRequestID", newDeliveryRequestID);
      rewardRequestData.put("originatingRequest", false);
      rewardRequestData.put("subscriberID", commodityDeliveryRequest.getSubscriberID());
      rewardRequestData.put("eventID", commodityDeliveryRequest.getEventID());
      rewardRequestData.put("moduleID", commodityDeliveryRequest.getModuleID());
      rewardRequestData.put("featureID", commodityDeliveryRequest.getFeatureID());
      rewardRequestData.put("deliveryType", deliveryType);
      rewardRequestData.put("diplomaticBriefcase", diplomaticBriefcase);
      rewardRequestData.put("msisdn", commodityDeliveryRequest.getExternalSubscriberID());
      rewardRequestData.put("providerID", commodityDeliveryRequest.getProviderID());
      rewardRequestData.put("deliverableID", deliverable.getDeliverableID());
      rewardRequestData.put("deliverableName", deliverable.getDeliverableName());
      rewardRequestData.put("operation", commodityDeliveryRequest.getOperation().getExternalRepresentation());
      rewardRequestData.put("amount", commodityDeliveryRequest.getAmount());
      rewardRequestData.put("periodQuantity", (validityPeriodQuantity == null ? 1 : validityPeriodQuantity)); //mandatory in RewardManagerRequest => set default value if nul
      rewardRequestData.put("periodType", (validityPeriodType == null ? TimeUnit.Day.getExternalRepresentation() : validityPeriodType)); //mandatory in RewardManagerRequest => set default value if nul

      //
      //  send
      //
      
      if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : generating "+CommodityType.REWARD+" request DONE");

      RewardManagerRequest rewardRequest = new RewardManagerRequest(commodityDeliveryRequest,JSONUtilities.encodeObject(rewardRequestData), Deployment.getDeliveryManagers().get(deliveryType));
      KafkaProducer rewardProducer = providerRequestProducers.get(commodityDeliveryRequest.getProviderID());
      if(rewardProducer != null){
        rewardProducer.send(new ProducerRecord<byte[], byte[]>(rewardRequestTopic, StringKey.serde().serializer().serialize(rewardRequestTopic, new StringKey(rewardRequest.getDeliveryRequestID())), ((ConnectSerde<DeliveryRequest>)rewardManagerDeclaration.getRequestSerde()).serializer().serialize(rewardRequestTopic, rewardRequest))); 
      }else{
        submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.SYSTEM_ERROR, "Could not send delivery request to provider (providerID = "+commodityDeliveryRequest.getProviderID()+")", null);
      }
      
      if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : sending "+CommodityType.REWARD+" request DONE");

      break;

    case JOURNEY:

      DeliveryManagerDeclaration journeyManagerDeclaration = Deployment.getDeliveryManagers().get(deliveryType);
      String journeyRequestTopic = journeyManagerDeclaration.getDefaultRequestTopic();

      HashMap<String,Object> journeyRequestData = new HashMap<String,Object>();
      
      journeyRequestData.put("deliveryRequestID", newDeliveryRequestID);
      journeyRequestData.put("originatingRequest", false);
      journeyRequestData.put("deliveryType", deliveryType);

      journeyRequestData.put("eventID", commodityDeliveryRequest.getEventID());
      journeyRequestData.put("moduleID", commodityDeliveryRequest.getModuleID());
      journeyRequestData.put("featureID", commodityDeliveryRequest.getFeatureID());
      journeyRequestData.put("journeyRequestID", commodityDeliveryRequest.getDeliveryRequestID());
      
      journeyRequestData.put("subscriberID", commodityDeliveryRequest.getSubscriberID());
      journeyRequestData.put("eventDate", SystemTime.getCurrentTime());
      journeyRequestData.put("journeyID", externalAccountID);
      
      journeyRequestData.put("diplomaticBriefcase", diplomaticBriefcase);
      
      if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : generating "+CommodityType.JOURNEY+" request DONE");

      JourneyRequest journeyRequest = new JourneyRequest(commodityDeliveryRequest, JSONUtilities.encodeObject(journeyRequestData), Deployment.getDeliveryManagers().get(deliveryType));
      KafkaProducer journeyProducer = providerRequestProducers.get(commodityDeliveryRequest.getProviderID());
      if(journeyProducer != null){
        journeyProducer.send(new ProducerRecord<byte[], byte[]>(journeyRequestTopic, StringKey.serde().serializer().serialize(journeyRequestTopic, new StringKey(journeyRequest.getDeliveryRequestID())), ((ConnectSerde<DeliveryRequest>)journeyManagerDeclaration.getRequestSerde()).serializer().serialize(journeyRequestTopic, journeyRequest))); 
      }else{
        submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.SYSTEM_ERROR, "Could not send delivery request to provider (providerID = "+commodityDeliveryRequest.getProviderID()+")", null);
      }

      if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+" - CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : sending "+CommodityType.JOURNEY+" request DONE");
      
      break;

    default:
      submitCorrelatorUpdate(commodityDeliveryRequest.getCorrelator(), CommodityDeliveryStatus.SUCCESS, "Success", null);
      log.info(Thread.currentThread().getId()+"CommodityDeliveryManager.proceedCommodityDeliveryRequest(...) : "+commodityType+" (default statement) DONE");
      break;
    }

    if(log.isDebugEnabled()) log.debug(Thread.currentThread().getId()+"CommodityDeliveryManager.proceedCommodityDeliveryRequest(..., "+commodityType+", "+deliveryType+") : method DONE");
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

      // retrieve the featureID that is the origin of this delivery request:
      // - If the Journey related to JourneyState is not a Workflow, then featureID = JourneyState.getID
      // - if the Journey related to JourneyState is a Workflown then we must extract the original featureID from the origial delivery Request that created the workflow instance
      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      deliveryRequestSource = extractWorkflowFeatureID(evolutionEventContext, subscriberEvaluationRequest, deliveryRequestSource);

      // if external accountID needed (really for veon rewardManager here so far, but might worth having something generic for IN)
      String externalSubscriberID = null;
      String profileExternalSubscriberIDField = Deployment.getDeliveryManagers().get(this.deliveryType).getProfileExternalSubscriberIDField();
      if ( profileExternalSubscriberIDField!=null ){
        CriterionField criterionField = Deployment.getProfileCriterionFields().get(profileExternalSubscriberIDField);
        externalSubscriberID = (String) criterionField.retrieveNormalized(subscriberEvaluationRequest);
      }

      /*****************************************
      *
      *  request
      *
      *****************************************/

      CommodityDeliveryRequest request = new CommodityDeliveryRequest(evolutionEventContext, externalSubscriberID, deliveryRequestSource, null, providerID, commodityID, operation, amount, validityPeriodType, validityPeriodQuantity, null);
      request.setModuleID(moduleID);
      request.setFeatureID(deliveryRequestSource);

      /*****************************************
      *
      *  return
      *
      *****************************************/

      return Collections.<Action>singletonList(request);
    }
    @Override public Map<String, String> getGUIDependencies(JourneyNode journeyNode)
    {
      Map<String, String> result = new HashMap<String, String>();
      String pointID = (String) journeyNode.getNodeParameters().get("node.parameter.commodityid");
      if (pointID != null) result.put("point", pointID.startsWith(POINT_PREFIX)?pointID.replace(POINT_PREFIX, ""):"");
      return result;
    }

  }
}
