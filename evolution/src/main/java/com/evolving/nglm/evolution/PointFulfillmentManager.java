/*****************************************************************************
*
*  PointFulfillmentManager.java
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
  
public class PointFulfillmentManager extends DeliveryManager implements Runnable
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  PointOperation
  //

  public enum PointOperation
  {
    Credit("credit"),
    Debit("debit"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private PointOperation(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static PointOperation fromExternalRepresentation(String externalRepresentation) { for (PointOperation enumeratedValue : PointOperation.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  PointFulfillmentStatus
  //

  public enum PointFulfillmentStatus
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
    private PointFulfillmentStatus(Integer externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public Integer getExternalRepresentation() { return externalRepresentation; }
    public static PointFulfillmentStatus fromReturnCode(Integer externalRepresentation) { for (PointFulfillmentStatus enumeratedValue : PointFulfillmentStatus.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
  }

  /*****************************************
  *
  *  conversion method
  *
  *****************************************/

  public DeliveryStatus getPointFulfillmentStatus (PointFulfillmentStatus status)
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

  private static final Logger log = LoggerFactory.getLogger(PointFulfillmentManager.class);

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

  public PointFulfillmentManager(String deliveryManagerKey)
  {
    //
    //  superclass
    //
    
    super("deliverymanager-pointfulfillment", deliveryManagerKey, Deployment.getBrokerServers(), PointFulfillmentRequest.serde(), Deployment.getDeliveryManagers().get("pointFulfillment"));

    //
    // statistics
    //
    try{
      bdrStats = new BDRStatistics("pointFulfillment");
    }catch(Exception e){
      log.error("PointFulfillmentManager: could not load statistics ", e);
      throw new RuntimeException("PointFulfillmentManager: could not load statistics  ", e);
    }
    
    //
    //  threads
    //
    
    for(int i = 0; i < threadNumber; i++)
      {
        threads.add(new Thread(this, "PointFulfillmentManagerThread_"+i));
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
  *  class PointFulfillmentRequest
  *
  *****************************************/

  public static class PointFulfillmentRequest extends DeliveryRequest
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
      schemaBuilder.name("service_pointfulfillment_request");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
//      schemaBuilder.field("providerID", Schema.STRING_SCHEMA);
      schemaBuilder.field("pointID", Schema.STRING_SCHEMA);
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
        
    private static ConnectSerde<PointFulfillmentRequest> serde = new ConnectSerde<PointFulfillmentRequest>(schema, false, PointFulfillmentRequest.class, PointFulfillmentRequest::pack, PointFulfillmentRequest::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<PointFulfillmentRequest> serde() { return serde; }
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
    private String pointID;
    private PointOperation operation;
    private int amount;
    private Date startValidityDate;
    private Date endValidityDate;
    private int returnCode;
    private PointFulfillmentStatus status;

    //
    //  accessors
    //

//  public String getProviderID() { return providerID; }
    public String getPointID() { return pointID; }
    public PointOperation getOperation() { return operation; }
    public int getAmount() { return amount; }
    public Date getStartValidityDate() { return startValidityDate; }
    public Date getEndValidityDate() { return endValidityDate; }
    public Integer getReturnCode() { return returnCode; }
    public PointFulfillmentStatus getStatus() { return status; }

    //
    //  setters
    //  

    public void setReturnCode(Integer returnCode) { this.returnCode = returnCode; }
    public void setStatus(PointFulfillmentStatus status) { this.status = status; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public PointFulfillmentRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource, /*String providerID,*/ String pointID, PointOperation operation, int amount, Date startValidityDate, Date endValidityDate)
    {
      super(context, deliveryType, deliveryRequestSource);
//      this.providerID = providerID;
      this.pointID = pointID;
      this.operation = operation;
      this.amount = amount;
      this.startValidityDate = startValidityDate;
      this.endValidityDate = endValidityDate;
      this.status = PointFulfillmentStatus.PENDING;
      this.returnCode = PointFulfillmentStatus.PENDING.getExternalRepresentation();
    }

    /*****************************************
    *
    *  constructor -- external
    *
    *****************************************/

    public PointFulfillmentRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
    {
      super(jsonRoot);
      String dateFormat = JSONUtilities.decodeString(deliveryManager.getJSONRepresentation(), "dateFormat", true);
//      this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", true);
      this.pointID = JSONUtilities.decodeString(jsonRoot, "pointID", true);
      this.operation = PointOperation.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "operation", true));
      this.amount = JSONUtilities.decodeInteger(jsonRoot, "amount", false);
      this.startValidityDate = RLMDateUtils.parseDate(JSONUtilities.decodeString(jsonRoot, "startValidityDate", false), dateFormat, Deployment.getBaseTimeZone());
      this.endValidityDate = RLMDateUtils.parseDate(JSONUtilities.decodeString(jsonRoot, "endValidityDate", false), dateFormat, Deployment.getBaseTimeZone());
      this.status = PointFulfillmentStatus.PENDING;
      this.returnCode = PointFulfillmentStatus.PENDING.getExternalRepresentation();
    }

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private PointFulfillmentRequest(SchemaAndValue schemaAndValue, /*String providerID,*/ String pointID, PointOperation operation, int amount, Date startValidityDate, Date endValidityDate, PointFulfillmentStatus status)
    {
      super(schemaAndValue);
//      this.providerID = providerID;
      this.pointID = pointID;
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

    private PointFulfillmentRequest(PointFulfillmentRequest pointFulfillmentRequest)
    {
      super(pointFulfillmentRequest);
//      this.providerID = pointFulfillmentRequest.getProviderID();
      this.pointID = pointFulfillmentRequest.getPointID();
      this.operation = pointFulfillmentRequest.getOperation();
      this.amount = pointFulfillmentRequest.getAmount();
      this.startValidityDate = pointFulfillmentRequest.getStartValidityDate();
      this.endValidityDate = pointFulfillmentRequest.getEndValidityDate();
      this.status = pointFulfillmentRequest.getStatus();
      this.returnCode = pointFulfillmentRequest.getReturnCode();
    }

    /*****************************************
    *
    *  copy
    *
    *****************************************/

    public PointFulfillmentRequest copy()
    {
      return new PointFulfillmentRequest(this);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      PointFulfillmentRequest pointFulfillmentRequest = (PointFulfillmentRequest) value;
      Struct struct = new Struct(schema);
      packCommon(struct, pointFulfillmentRequest);
//      struct.put("providerID", pointFulfillmentRequest.getProviderID());
      struct.put("pointID", pointFulfillmentRequest.getPointID());
      struct.put("operation", pointFulfillmentRequest.getOperation().getExternalRepresentation());
      struct.put("amount", pointFulfillmentRequest.getAmount());
      struct.put("startValidityDate", pointFulfillmentRequest.getStartValidityDate());
      struct.put("endValidityDate", pointFulfillmentRequest.getEndValidityDate());
      struct.put("return_code", pointFulfillmentRequest.getReturnCode());
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

    public static PointFulfillmentRequest unpack(SchemaAndValue schemaAndValue)
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
      String pointID = valueStruct.getString("pointID");
      PointOperation operation = PointOperation.fromExternalRepresentation(valueStruct.getString("operation"));
      int amount = valueStruct.getInt32("amount");
      Date startValidityDate = (Date) valueStruct.get("startValidityDate");
      Date endValidityDate = (Date) valueStruct.get("endValidityDate");
      Integer returnCode = valueStruct.getInt32("return_code");
      PointFulfillmentStatus status = PointFulfillmentStatus.fromReturnCode(returnCode);

      //
      //  return
      //

      return new PointFulfillmentRequest(schemaAndValue, /*providerID,*/ pointID, operation, amount, startValidityDate, endValidityDate, status);
    }

    /*****************************************
    *  
    *  toString
    *
    *****************************************/

    public String toString()
    {
      StringBuilder b = new StringBuilder();
      b.append("PointFulfillmentRequest:{");
      b.append(super.toStringFields());
      b.append("," + getSubscriberID());
//      b.append("," + providerID);
      b.append("," + pointID);
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
      guiPresentationMap.put(DELIVERABLEID, getPointID());
      guiPresentationMap.put(DELIVERABLEQTY, getAmount());
      guiPresentationMap.put(OPERATION, getOperation().toString());
      guiPresentationMap.put(MODULEID, getModuleID());
      guiPresentationMap.put(MODULENAME, Module.fromExternalRepresentation(getModuleID()).toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(ORIGIN, "");
      guiPresentationMap.put(RETURNCODE, getReturnCode());
    }
    
    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SalesChannelService salesChannelService)
    {
      thirdPartyPresentationMap.put(CUSTOMERID, getSubscriberID());
//      thirdPartyPresentationMap.put(PROVIDERID, getProviderID());
      thirdPartyPresentationMap.put(DELIVERABLEID, getPointID());
      thirdPartyPresentationMap.put(DELIVERABLEQTY, getAmount());
      thirdPartyPresentationMap.put(OPERATION, getOperation().toString());
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, Module.fromExternalRepresentation(getModuleID()).toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(ORIGIN, "");
      thirdPartyPresentationMap.put(RETURNCODE, getReturnCode());
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
    private PointOperation operation;
    
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
      this.operation = PointOperation.fromExternalRepresentation(JSONUtilities.decodeString(configuration, "operation", true));
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

      String pointID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.pointid");
      int amount = ((Number) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.amount")).intValue();
      Number endValidityDuration = (Number) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.endvalidityduration");
      String endValidityUnits = (String) subscriberEvaluationRequest.getJourneyNode().getNodeParameters().get("node.parameter.endvalidityunits");
      
      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      PointOperation operation = this.operation;
      Date startValidityDate = subscriberEvaluationRequest.getEvaluationDate();
      Date endValidityDate = (endValidityDuration != null && endValidityUnits != null) ? EvolutionUtilities.addTime(startValidityDate, endValidityDuration.intValue(), TimeUnit.fromExternalRepresentation(endValidityUnits), Deployment.getBaseTimeZone(), false) : null;

      /*****************************************
      *
      *  request
      *
      *****************************************/

      PointFulfillmentRequest request = new PointFulfillmentRequest(evolutionEventContext, deliveryType, deliveryRequestSource, /*providerID,*/ pointID, operation, amount, startValidityDate, endValidityDate);

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
        
        PointFulfillmentStatus status = null;
        PointOperation operation = ((PointFulfillmentRequest)deliveryRequest).getOperation();
        switch (operation) {
        case Credit:
          status = credit((PointFulfillmentRequest)deliveryRequest);
          break;
        case Debit:
          status = debit((PointFulfillmentRequest)deliveryRequest);
          break;
        default:
          break;
        }
        
        /*****************************************
        *
        *  update and return response
        *
        *****************************************/

        ((PointFulfillmentRequest)deliveryRequest).setStatus(status);
        ((PointFulfillmentRequest)deliveryRequest).setReturnCode(status.getExternalRepresentation());
        deliveryRequest.setDeliveryStatus(getPointFulfillmentStatus(status));
        deliveryRequest.setDeliveryDate(SystemTime.getActualCurrentTime());
        completeRequest(deliveryRequest);
        bdrStats.updateBDREventCount(1, getPointFulfillmentStatus(status));

      }
  }

  /*****************************************
  *
  *  credit
  *
  *****************************************/

  private PointFulfillmentStatus credit(PointFulfillmentRequest pointFulfillmentRequest)
  {
    //TODO SCH : TBD
    log.info("PointFulfillmentManager.credit("+pointFulfillmentRequest+") : called ...");
    return PointFulfillmentStatus.SUCCESS;
  }
  
  /*****************************************
  *
  *  debit
  *
  *****************************************/

  private PointFulfillmentStatus debit(PointFulfillmentRequest pointFulfillmentRequest)
  {
    //TODO SCH : TBD
    log.info("PointFulfillmentManager.debit("+pointFulfillmentRequest+") : called ...");
    return PointFulfillmentStatus.SUCCESS;
  }
  
  /*****************************************
  *
  *  processCorrelatorUpdate
  *
  *****************************************/

  @Override protected void processCorrelatorUpdate(DeliveryRequest deliveryRequest, JSONObject correlatorUpdate)
  {
    log.info("PointFulfillmentManager.processCorrelatorUpdate("+deliveryRequest.getDeliveryRequestID()+", "+correlatorUpdate+") : called ...");

    int result = JSONUtilities.decodeInteger(correlatorUpdate, "result", true);
    PointFulfillmentRequest pointFulfillmentRequest = (PointFulfillmentRequest) deliveryRequest;
    if (pointFulfillmentRequest != null)
      {
        pointFulfillmentRequest.setStatus(PointFulfillmentStatus.fromReturnCode(result));
        pointFulfillmentRequest.setDeliveryStatus(getPointFulfillmentStatus(pointFulfillmentRequest.getStatus()));
        pointFulfillmentRequest.setDeliveryDate(SystemTime.getCurrentTime());
        completeRequest(pointFulfillmentRequest);
      }
  
    log.debug("PointFulfillmentManager.processCorrelatorUpdate("+deliveryRequest.getDeliveryRequestID()+", "+correlatorUpdate+") : DONE");
  }

  
  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  @Override protected void shutdown()
  {
    log.info("PointFulfillmentManager:  shutdown");
  }
  
  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    log.info("PointFulfillmentManager: recieved " + args.length + " args");
    for(String arg : args){
      log.info("PointFulfillmentManager: arg " + arg);
    }
    
    //
    //  configuration
    //

    String deliveryManagerKey = args[0];

    //
    //  instance  
    //
    
    log.info("Configuration " + Deployment.getDeliveryManagers());

    
    PointFulfillmentManager manager = new PointFulfillmentManager(deliveryManagerKey);

    //
    //  run
    //

    manager.run();
  }
}
