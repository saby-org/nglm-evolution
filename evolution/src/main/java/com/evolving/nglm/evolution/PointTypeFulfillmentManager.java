/*****************************************************************************
*
*  PointTypeFulfillmentManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvaluationCriterion.TimeUnit;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
  
public class PointTypeFulfillmentManager extends DeliveryManager implements Runnable
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  PointTypeOperation
  //

  public enum PointTypeOperation
  {
    Credit("credit"),
    Debit("debit"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private PointTypeOperation(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static PointTypeOperation fromExternalRepresentation(String externalRepresentation) { for (PointTypeOperation enumeratedValue : PointTypeOperation.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  PointTypeFulfillmentStatus
  //

  public enum PointTypeFulfillmentStatus
  {
    PENDING(10),
    SUCCESS(0),
    SYSTEM_ERROR(21),
    TIMEOUT(22),
    THROTTLING(23),
    CUSTOMER_NOT_FOUND(20),
    BONUS_NOT_FOUND(101),
    UNKNOWN(999);
    private Integer externalRepresentation;
    private PointTypeFulfillmentStatus(Integer externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public Integer getExternalRepresentation() { return externalRepresentation; }
    public static PointTypeFulfillmentStatus fromReturnCode(Integer externalRepresentation) { for (PointTypeFulfillmentStatus enumeratedValue : PointTypeFulfillmentStatus.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
  }

  /*****************************************
  *
  *  conversion method
  *
  *****************************************/

  public DeliveryStatus getPointTypeFulfillmentStatus (PointTypeFulfillmentStatus status)
  {
    switch(status)
      {
        case PENDING:
          return DeliveryStatus.Pending;
        case SUCCESS:
          return DeliveryStatus.Delivered;
        case SYSTEM_ERROR:
        case CUSTOMER_NOT_FOUND:
        case BONUS_NOT_FOUND:
          return DeliveryStatus.Failed;
        case TIMEOUT:
        case THROTTLING:
          return DeliveryStatus.FailedRetry;
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

  private static final Logger log = LoggerFactory.getLogger(PointTypeFulfillmentManager.class);

  //
  //  number of threads
  //
  
  private int threadNumber = 5;   //TODO : make this configurable
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private ArrayList<Thread> threads = new ArrayList<Thread>();
  private BDRStatistics bdrStats = null;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PointTypeFulfillmentManager(String deliveryManagerKey)
  {
    //
    //  superclass
    //
    
    super("deliverymanager-pointtypefulfillment", deliveryManagerKey, Deployment.getBrokerServers(), PointTypeFulfillmentRequest.serde(), Deployment.getDeliveryManagers().get("pointTypeFulfillment"));

    //
    // statistics
    //
    try{
      bdrStats = new BDRStatistics("pointTypeFulfillment");
    }catch(Exception e){
      log.error("PointTypeFulfillmentManager: could not load statistics ", e);
      throw new RuntimeException("PointTypeFulfillmentManager: could not load statistics  ", e);
    }
    
    //
    //  threads
    //
    
    for(int i = 0; i < threadNumber; i++)
      {
        threads.add(new Thread(this, "PointTypeFulfillmentManagerThread_"+i));
      }
    
    //
    //  startDelivery
    //
    
    startDelivery();

  }

//  /*****************************************
//  *
//  *  class Account
//  *
//  *****************************************/
//  
//  public static class Account 
//  {
//    /*****************************************
//    *
//    *  data
//    *
//    *****************************************/
//
//    private String accountID;
//    private String name;
//    private boolean creditable;
//    private boolean debitable;
//    
//    //
//    //  accessors
//    //
//
//    public String getAccountID() { return accountID; }
//    public String getName() { return name; }
//    public boolean getCreditable() { return creditable; }
//    public boolean getDebitable() { return debitable; }
//    
//    /*****************************************
//    *
//    *  constructor
//    *
//    *****************************************/
//
//    public Account(String accountID, String name, boolean creditable, boolean debitable)
//    {
//      this.accountID = accountID;
//      this.name = name;
//      this.creditable = creditable;
//      this.debitable = debitable;
//    }
//
//    /*****************************************
//    *
//    *  constructor -- external
//    *
//    *****************************************/
//
//    public Account(JSONObject jsonRoot)
//    {
//      this.accountID = JSONUtilities.decodeString(jsonRoot, "accountID", true);
//      this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
//      this.creditable = JSONUtilities.decodeBoolean(jsonRoot, "creditable", true);
//      this.debitable = JSONUtilities.decodeBoolean(jsonRoot, "debitable", true);
//    }
//    
//  }
  
  /*****************************************
  *
  *  class PointTypeFulfillmentRequest
  *
  *****************************************/

  public static class PointTypeFulfillmentRequest extends DeliveryRequest
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
      schemaBuilder.name("service_pointtypefulfillment_request");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
//      schemaBuilder.field("providerID", Schema.STRING_SCHEMA);
      schemaBuilder.field("pointTypeID", Schema.STRING_SCHEMA);
      schemaBuilder.field("operation", Schema.STRING_SCHEMA);
      schemaBuilder.field("amount", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("startValidityDate", Timestamp.builder().optional().schema());
      schemaBuilder.field("endValidityDate", Timestamp.builder().optional().schema());
      schemaBuilder.field("return_code", Schema.INT32_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //
        
    private static ConnectSerde<PointTypeFulfillmentRequest> serde = new ConnectSerde<PointTypeFulfillmentRequest>(schema, false, PointTypeFulfillmentRequest.class, PointTypeFulfillmentRequest::pack, PointTypeFulfillmentRequest::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<PointTypeFulfillmentRequest> serde() { return serde; }
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

//  public String providerID;
    private String pointTypeID;
    private PointTypeOperation operation;
    private int amount;
    private Date startValidityDate;
    private Date endValidityDate;
    private int returnCode;
    private PointTypeFulfillmentStatus status;
    private String returnCodeDetails;

    //
    //  accessors
    //

//  public String getProviderID() { return providerID; }
    public String getPointTypeID() { return pointTypeID; }
    public PointTypeOperation getOperation() { return operation; }
    public int getAmount() { return amount; }
    public Date getStartValidityDate() { return startValidityDate; }
    public Date getEndValidityDate() { return endValidityDate; }
    public Integer getReturnCode() { return returnCode; }
    public PointTypeFulfillmentStatus getStatus() { return status; }
    public String getReturnCodeDetails() { return returnCodeDetails; }

    //
    //  setters
    //  

    public void setReturnCode(Integer returnCode) { this.returnCode = returnCode; }
    public void setStatus(PointTypeFulfillmentStatus status) { this.status = status; }
    public void setReturnCodeDetails(String returnCodeDetails) { this.returnCodeDetails = returnCodeDetails; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public PointTypeFulfillmentRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource, /*String providerID,*/ String pointTypeID, PointTypeOperation operation, int amount, Date startValidityDate, Date endValidityDate)
    {
      super(context, deliveryType, deliveryRequestSource);
//      this.providerID = providerID;
      this.pointTypeID = pointTypeID;
      this.operation = operation;
      this.amount = amount;
      this.startValidityDate = startValidityDate;
      this.endValidityDate = endValidityDate;
      this.status = PointTypeFulfillmentStatus.PENDING;
      this.returnCode = PointTypeFulfillmentStatus.PENDING.getExternalRepresentation();
      this.returnCodeDetails = "";
    }

    /*****************************************
    *
    *  constructor -- external
    *
    *****************************************/

    public PointTypeFulfillmentRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
    {
      super(jsonRoot);
      String dateFormat = JSONUtilities.decodeString(deliveryManager.getJSONRepresentation(), "dateFormat", true);
//      this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", true);
      this.pointTypeID = JSONUtilities.decodeString(jsonRoot, "pointTypeID", true);
      this.operation = PointTypeOperation.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "operation", true));
      this.amount = JSONUtilities.decodeInteger(jsonRoot, "amount", false);
      this.startValidityDate = RLMDateUtils.parseDate(JSONUtilities.decodeString(jsonRoot, "startValidityDate", false), dateFormat, Deployment.getBaseTimeZone());
      this.endValidityDate = RLMDateUtils.parseDate(JSONUtilities.decodeString(jsonRoot, "endValidityDate", false), dateFormat, Deployment.getBaseTimeZone());
      this.status = PointTypeFulfillmentStatus.PENDING;
      this.returnCode = PointTypeFulfillmentStatus.PENDING.getExternalRepresentation();
      this.returnCodeDetails = "";
    }

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private PointTypeFulfillmentRequest(SchemaAndValue schemaAndValue, /*String providerID,*/ String pointTypeID, PointTypeOperation operation, int amount, Date startValidityDate, Date endValidityDate, PointTypeFulfillmentStatus status)
    {
      super(schemaAndValue);
//      this.providerID = providerID;
      this.pointTypeID = pointTypeID;
      this.operation = operation;
      this.amount = amount;
      this.startValidityDate = startValidityDate;
      this.endValidityDate = endValidityDate;
      this.status = status;
      this.returnCode = status.getExternalRepresentation();
    }

    /*****************************************
    *
    *  constructor -- copy
    *
    *****************************************/

    private PointTypeFulfillmentRequest(PointTypeFulfillmentRequest pointTypeFulfillmentRequest)
    {
      super(pointTypeFulfillmentRequest);
//      this.providerID = pointTypeFulfillmentRequest.getProviderID();
      this.pointTypeID = pointTypeFulfillmentRequest.getPointTypeID();
      this.operation = pointTypeFulfillmentRequest.getOperation();
      this.amount = pointTypeFulfillmentRequest.getAmount();
      this.startValidityDate = pointTypeFulfillmentRequest.getStartValidityDate();
      this.endValidityDate = pointTypeFulfillmentRequest.getEndValidityDate();
      this.status = pointTypeFulfillmentRequest.getStatus();
      this.returnCode = pointTypeFulfillmentRequest.getReturnCode();
      this.returnCodeDetails = pointTypeFulfillmentRequest.getReturnCodeDetails();
    }

    /*****************************************
    *
    *  copy
    *
    *****************************************/

    public PointTypeFulfillmentRequest copy()
    {
      return new PointTypeFulfillmentRequest(this);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      PointTypeFulfillmentRequest pointTypeFulfillmentRequest = (PointTypeFulfillmentRequest) value;
      Struct struct = new Struct(schema);
      packCommon(struct, pointTypeFulfillmentRequest);
//      struct.put("providerID", pointTypeFulfillmentRequest.getProviderID());
      struct.put("pointTypeID", pointTypeFulfillmentRequest.getPointTypeID());
      struct.put("operation", pointTypeFulfillmentRequest.getOperation().getExternalRepresentation());
      struct.put("amount", pointTypeFulfillmentRequest.getAmount());
      struct.put("startValidityDate", pointTypeFulfillmentRequest.getStartValidityDate());
      struct.put("endValidityDate", pointTypeFulfillmentRequest.getEndValidityDate());
      struct.put("return_code", pointTypeFulfillmentRequest.getReturnCode());
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

    public static PointTypeFulfillmentRequest unpack(SchemaAndValue schemaAndValue)
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
//      String providerID = valueStruct.getString("providerID");
      String pointTypeID = valueStruct.getString("pointTypeID");
      PointTypeOperation operation = PointTypeOperation.fromExternalRepresentation(valueStruct.getString("operation"));
      int amount = valueStruct.getInt32("amount");
      Date startValidityDate = (Date) valueStruct.get("startValidityDate");
      Date endValidityDate = (Date) valueStruct.get("endValidityDate");
      Integer returnCode = valueStruct.getInt32("return_code");
      PointTypeFulfillmentStatus status = PointTypeFulfillmentStatus.fromReturnCode(returnCode);

      //
      //  return
      //

      return new PointTypeFulfillmentRequest(schemaAndValue, /*providerID,*/ pointTypeID, operation, amount, startValidityDate, endValidityDate, status);
    }

    /*****************************************
    *  
    *  toString
    *
    *****************************************/

    public String toString()
    {
      StringBuilder b = new StringBuilder();
      b.append("PointTypeFulfillmentRequest:{");
      b.append(super.toStringFields());
      b.append("," + getSubscriberID());
//      b.append("," + providerID);
      b.append("," + pointTypeID);
      b.append("," + operation);
      b.append("," + amount);
      b.append("," + startValidityDate);
      b.append("," + endValidityDate);
      b.append("," + returnCode);
      b.append("," + status.toString());
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
//      guiPresentationMap.put(PROVIDERID, getProviderID());
      guiPresentationMap.put(DELIVERABLEID, getPointTypeID());
      guiPresentationMap.put(DELIVERABLEQTY, getAmount());
      guiPresentationMap.put(OPERATION, getOperation().toString());
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
//      thirdPartyPresentationMap.put(PROVIDERID, getProviderID());
      thirdPartyPresentationMap.put(DELIVERABLEID, getPointTypeID());
      thirdPartyPresentationMap.put(DELIVERABLEQTY, getAmount());
      thirdPartyPresentationMap.put(OPERATION, getOperation().toString());
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

    private String deliveryType;
//    private String providerID;
    private PointTypeOperation operation;
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ActionManager(JSONObject configuration)
    {
      super(configuration);
      this.deliveryType = JSONUtilities.decodeString(configuration, "deliveryType", true);
//      this.providerID = JSONUtilities.decodeString(Deployment.getDeliveryManagers().get(this.deliveryType).getJSONRepresentation(), "providerID", true);
      this.operation = PointTypeOperation.fromExternalRepresentation(JSONUtilities.decodeString(configuration, "operation", true));
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

      String pointTypeID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.pointTypeid");
      int amount = ((Number) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.amount")).intValue();
      Number endValidityDuration = (Number) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.endvalidityduration");
      String endValidityUnits = (String) subscriberEvaluationRequest.getJourneyNode().getNodeParameters().get("node.parameter.endvalidityunits");
      
      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      PointTypeOperation operation = this.operation;
      Date startValidityDate = subscriberEvaluationRequest.getEvaluationDate();
      Date endValidityDate = (endValidityDuration != null && endValidityUnits != null) ? EvolutionUtilities.addTime(startValidityDate, endValidityDuration.intValue(), TimeUnit.fromExternalRepresentation(endValidityUnits), Deployment.getBaseTimeZone(), false) : null;

      /*****************************************
      *
      *  request
      *
      *****************************************/

      PointTypeFulfillmentRequest request = new PointTypeFulfillmentRequest(evolutionEventContext, deliveryType, deliveryRequestSource, /*providerID,*/ pointTypeID, operation, amount, startValidityDate, endValidityDate);

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

        /*****************************************
        *
        *  call INPlugin
        *
        *****************************************/
        
        PointTypeFulfillmentStatus status = null;
        PointTypeOperation operation = ((PointTypeFulfillmentRequest)deliveryRequest).getOperation();
        switch (operation) {
        case Credit:
          status = credit((PointTypeFulfillmentRequest)deliveryRequest);
          break;
        case Debit:
          status = debit((PointTypeFulfillmentRequest)deliveryRequest);
          break;
        default:
          break;
        }
        
        /*****************************************
        *
        *  update and return response
        *
        *****************************************/

        ((PointTypeFulfillmentRequest)deliveryRequest).setStatus(status);
        ((PointTypeFulfillmentRequest)deliveryRequest).setReturnCode(status.getExternalRepresentation());
        deliveryRequest.setDeliveryStatus(getPointTypeFulfillmentStatus(status));
        deliveryRequest.setDeliveryDate(SystemTime.getActualCurrentTime());
        completeRequest(deliveryRequest);
        bdrStats.updateBDREventCount(1, getPointTypeFulfillmentStatus(status));

      }
  }

  /*****************************************
  *
  *  credit
  *
  *****************************************/

  private PointTypeFulfillmentStatus credit(PointTypeFulfillmentRequest pointTypeFulfillmentRequest)
  {
    //TODO SCH : TBD
    log.info("PointTypeFulfillmentManager.credit("+pointTypeFulfillmentRequest+") : called ...");
    return PointTypeFulfillmentStatus.SUCCESS;
  }
  
  /*****************************************
  *
  *  debit
  *
  *****************************************/

  private PointTypeFulfillmentStatus debit(PointTypeFulfillmentRequest pointTypeFulfillmentRequest)
  {
    //TODO SCH : TBD
    log.info("PointTypeFulfillmentManager.debit("+pointTypeFulfillmentRequest+") : called ...");
    return PointTypeFulfillmentStatus.SUCCESS;
  }
  
  /*****************************************
  *
  *  processCorrelatorUpdate
  *
  *****************************************/

  @Override protected void processCorrelatorUpdate(DeliveryRequest deliveryRequest, JSONObject correlatorUpdate)
  {
    log.info("PointTypeFulfillmentManager.processCorrelatorUpdate("+deliveryRequest.getDeliveryRequestID()+", "+correlatorUpdate+") : called ...");

    int result = JSONUtilities.decodeInteger(correlatorUpdate, "result", true);
    PointTypeFulfillmentRequest pointTypeFulfillmentRequest = (PointTypeFulfillmentRequest) deliveryRequest;
    if (pointTypeFulfillmentRequest != null)
      {
        pointTypeFulfillmentRequest.setStatus(PointTypeFulfillmentStatus.fromReturnCode(result));
        pointTypeFulfillmentRequest.setDeliveryStatus(getPointTypeFulfillmentStatus(pointTypeFulfillmentRequest.getStatus()));
        pointTypeFulfillmentRequest.setDeliveryDate(SystemTime.getCurrentTime());
        completeRequest(pointTypeFulfillmentRequest);
      }
  
    log.debug("PointTypeFulfillmentManager.processCorrelatorUpdate("+deliveryRequest.getDeliveryRequestID()+", "+correlatorUpdate+") : DONE");
  }

  
  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  @Override protected void shutdown()
  {
    log.info("PointTypeFulfillmentManager:  shutdown");
  }
  
  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    log.info("PointTypeFulfillmentManager: recieved " + args.length + " args");
    for(String arg : args){
      log.info("PointTypeFulfillmentManager: arg " + arg);
    }
    
    //
    //  configuration
    //

    String deliveryManagerKey = args[0];

    //
    //  instance  
    //
    
    log.info("Configuration " + Deployment.getDeliveryManagers());

    
    PointTypeFulfillmentManager manager = new PointTypeFulfillmentManager(deliveryManagerKey);

    //
    //  run
    //

    manager.run();
  }
}
