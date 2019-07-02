/*****************************************************************************
*
*  SMSNotificationManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.DeliveryManager;
import com.evolving.nglm.evolution.DeliveryManagerDeclaration;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.OutgoingConnectionPoint;
import com.evolving.nglm.evolution.SMSMessage;
import com.evolving.nglm.evolution.SubscriberEvaluationRequest;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;

public class SMSNotificationManager extends DeliveryManager implements Runnable
{
  /*****************************************
  *
  *  enum - status
  *
  *****************************************/

  public enum SMSMessageStatus
  {
    PENDING(10),
    SENT(1),
    NO_CUSTOMER_LANGUAGE(701),
    NO_CUSTOMER_CHANNEL(702),
    DELIVERED(0),
    EXPIRED(707),
    ERROR(706),
    UNDELIVERABLE(703),
    INVALID(704),
    QUEUE_FULL(705),
    THROTTLING(23),
    UNKNOWN(999);
    private Integer returnCode;
    private SMSMessageStatus(Integer externalRepresentation) { this.returnCode = externalRepresentation; }
    public Integer getReturnCode() { return returnCode; }
    public static SMSMessageStatus fromReturnCode(Integer externalRepresentation) { for (SMSMessageStatus enumeratedValue : SMSMessageStatus.values()) { if (enumeratedValue.getReturnCode().equals(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
    public static SMSMessageStatus fromExternalRepresentation(String value) { for (SMSMessageStatus enumeratedValue : SMSMessageStatus.values()) { if (enumeratedValue.toString().equalsIgnoreCase(value)) return enumeratedValue; } return UNKNOWN; }
  }

  /*****************************************
  *
  *  conversion method
  *
  *****************************************/

  public DeliveryStatus getMessageStatus(SMSMessageStatus status)
  {
    switch(status)
      {
        case PENDING:
          return DeliveryStatus.Pending;
        case SENT:
        case DELIVERED:
          return DeliveryStatus.Delivered;
        case NO_CUSTOMER_LANGUAGE:
        case NO_CUSTOMER_CHANNEL:
        case ERROR:
        case UNDELIVERABLE:
        case INVALID:
        case QUEUE_FULL:
        default:
          return DeliveryStatus.Failed;
      }
  }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private int threadNumber = 5;   //TODO : make this configurable
  private SMSNotificationInterface smsNotification;
  private ArrayList<Thread> threads = new ArrayList<Thread>();
  private NotificationStatistics stats = null;
  private static String applicationID = "deliverymanager-notificationmanagersms";
  public String pluginName;
  
  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SMSNotificationManager.class);
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public SMSNotificationManager(String deliveryManagerKey, String pluginName, String pluginConfiguration)
  {
    //
    //  superclass
    //
    
    super(applicationID, deliveryManagerKey, Deployment.getBrokerServers(), SMSNotificationManagerRequest.serde(), Deployment.getDeliveryManagers().get(pluginName));
    
    //
    //  manager
    //
    
    this.pluginName = pluginName;
    
    String smsPluginClassName = JSONUtilities.decodeString(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "notificationPluginClass", true);
    log.info("SMSNotificationManager: plugin instanciation : smsPluginClassName = "+smsPluginClassName);

    JSONObject smsPluginConfiguration = JSONUtilities.decodeJSONObject(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "notificationPluginConfiguration", true);
    log.info("SMSNotificationManager: plugin instanciation : smsPluginConfiguration = "+smsPluginConfiguration);

    try
      {
        smsNotification = (SMSNotificationInterface) (Class.forName(smsPluginClassName).newInstance());
        smsNotification.init(this, smsPluginConfiguration, pluginConfiguration, pluginName);
      }
    catch (InstantiationException | IllegalAccessException | IllegalArgumentException e)
      {
        log.error("SMSNotificationManager: could not create new instance of class " + smsPluginClassName, e);
        throw new RuntimeException("SMSNotificationManager: could not create new instance of class " + smsPluginClassName, e);
      }
    catch (ClassNotFoundException e)
      {
        log.error("SMSNotificationManager: could not find class " + smsPluginClassName, e);
        throw new RuntimeException("SMSNotificationManager: could not find class " + smsPluginClassName, e);
      }
    
    //
    // statistics
    //
    
    try{
      stats = new NotificationStatistics(applicationID, pluginName);
    }catch(Exception e){
      log.error("SMSNotificationManager: could not load statistics ", e);
      throw new RuntimeException("SMSNotificationManager: could not load statistics  ", e);
    }
      
    //
    //  threads
    //
    
    for(int i = 0; i < threadNumber; i++)
      {
        threads.add(new Thread(this, "SMSNotificationManagerThread_"+i));
      }
    
    //
    //  startDelivery
    //
    
    startDelivery();
  }
  
  /*****************************************
  *
  *  class NotificationManagerRequest
  *
  *****************************************/
  
  public static class SMSNotificationManagerRequest extends DeliveryRequest
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
      schemaBuilder.name("service_smsnotification_request");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("destination", Schema.STRING_SCHEMA);
      schemaBuilder.field("source", Schema.STRING_SCHEMA);
      schemaBuilder.field("text", Schema.STRING_SCHEMA);
      schemaBuilder.field("return_code", Schema.INT32_SCHEMA);
      schemaBuilder.field("confirmation_expected", Schema.BOOLEAN_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //
        
    private static ConnectSerde<SMSNotificationManagerRequest> serde = new ConnectSerde<SMSNotificationManagerRequest>(schema, false, SMSNotificationManagerRequest.class, SMSNotificationManagerRequest::pack, SMSNotificationManagerRequest::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<SMSNotificationManagerRequest> serde() { return serde; }
    public Schema subscriberStreamEventSchema() { return schema(); }
    
    /*****************************************
    *
    *  data
    *
    *****************************************/

    public String destination;
    public String source;
    public String text;
    private SMSMessageStatus status;
    private int returnCode;
    private String returnCodeDetails;
    private boolean confirmationExpected;
    
    //
    //  accessors
    //
    
    public String getDestination() { return destination; }
    public String getSource() { return source; }
    public String getText() { return text; }
    public SMSMessageStatus getMessageStatus() { return status; }
    public int getReturnCode() { return returnCode; }
    public String getReturnCodeDetails() { return returnCodeDetails; }
    public boolean isConfirmationExpected() { return confirmationExpected; }

    //
    //   setters
    // 

    public void setMessageStatus(SMSMessageStatus status) { this.status = status; }
    public void setReturnCode(Integer returnCode) { this.returnCode = returnCode; }
    public void setReturnCodeDetails(String returnCodeDetails) { this.returnCodeDetails = returnCodeDetails; }
    public void setIsConfirmationExpected(boolean expected) { this.confirmationExpected = expected; }
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/
    
    public SMSNotificationManagerRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource, String destination, String source, String text)
    {
      super(context, deliveryType, deliveryRequestSource);
      this.destination = destination;
      this.source = source;
      this.text = text;
      this.status = SMSMessageStatus.PENDING;
      this.returnCode = status.getReturnCode();
      this.returnCodeDetails = "";
    }
    
    /*****************************************
    *
    *  constructor -- external
    *
    *****************************************/
    
    public SMSNotificationManagerRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
    {
      super(jsonRoot);
      this.destination = JSONUtilities.decodeString(jsonRoot, "destination", true);
      this.source = JSONUtilities.decodeString(jsonRoot, "source", true);
      this.text = JSONUtilities.decodeString(jsonRoot, "text", true);
      this.status = SMSMessageStatus.PENDING;
      this.returnCode = SMSMessageStatus.PENDING.getReturnCode();
      this.returnCodeDetails = "";
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private SMSNotificationManagerRequest(SchemaAndValue schemaAndValue, String destination, String source, String text, SMSMessageStatus status, boolean confirmationExpected)
    {
      super(schemaAndValue);
      this.destination = destination;
      this.source = source;
      this.text = text;
      this.status = status;
      this.returnCode = status.getReturnCode();
      this.confirmationExpected = confirmationExpected;
    }
    
    /*****************************************
    *
    *  constructor -- copy
    *
    *****************************************/

    private SMSNotificationManagerRequest(SMSNotificationManagerRequest smsNotificationManagerRequest)
    {
      super(smsNotificationManagerRequest);
      this.destination = smsNotificationManagerRequest.getDestination();
      this.source = smsNotificationManagerRequest.getSource();
      this.text = smsNotificationManagerRequest.getText();
      this.status = smsNotificationManagerRequest.getMessageStatus();
      this.returnCode = smsNotificationManagerRequest.getReturnCode();
      this.returnCodeDetails = smsNotificationManagerRequest.getReturnCodeDetails();
      this.confirmationExpected = smsNotificationManagerRequest.isConfirmationExpected();
    }

    /*****************************************
    *
    *  copy
    *
    *****************************************/

    public SMSNotificationManagerRequest copy()
    {
      return new SMSNotificationManagerRequest(this);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      SMSNotificationManagerRequest notificationRequest = (SMSNotificationManagerRequest) value;
      Struct struct = new Struct(schema);
      packCommon(struct, notificationRequest);
      struct.put("destination", notificationRequest.getDestination());
      struct.put("source", notificationRequest.getSource());
      struct.put("text", notificationRequest.getText());
      struct.put("return_code", notificationRequest.getReturnCode());
      struct.put("confirmation_expected", notificationRequest.isConfirmationExpected());
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

    public static SMSNotificationManagerRequest unpack(SchemaAndValue schemaAndValue)
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
      String destination = valueStruct.getString("destination");
      String source = valueStruct.getString("source");
      String text = valueStruct.getString("text");
      Integer returnCode = valueStruct.getInt32("return_code");
      boolean confirmationExpected = valueStruct.getBoolean("confirmation_expected");
      SMSMessageStatus status = SMSMessageStatus.fromReturnCode(returnCode);
      
      //
      //  log
      //

      log.debug("SMSNotificationManagerRequest:unpack destination;"+destination + " source;"+source + " text;"+text);

      //
      //  return
      //

      
      return new SMSNotificationManagerRequest(schemaAndValue, destination, source, text, status, confirmationExpected);
    }
    
    @Override public Integer getActivityType() { return ActivityType.Messages.getExternalRepresentation(); }
    
    /****************************************
    *
    *  presentation utilities
    *
    ****************************************/
    
    @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SalesChannelService salesChannelService)
    {
      guiPresentationMap.put(CUSTOMERID, getSubscriberID());
      guiPresentationMap.put(MODULEID, getModuleID());
      guiPresentationMap.put(MODULENAME, Module.fromExternalRepresentation(getModuleID()).toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(ORIGIN, getSource());
      guiPresentationMap.put(RETURNCODE, getReturnCode());
      guiPresentationMap.put(RETURNCODEDETAILS, getReturnCodeDetails());
      guiPresentationMap.put(NOTIFICATION_SUBJECT, null);
      guiPresentationMap.put(NOTIFICATION_TEXT_BODY, getText());
      guiPresentationMap.put(NOTIFICATION_HTML_BODY, null);
      guiPresentationMap.put(NOTIFICATION_CHANNEL, "SMS");
      guiPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
    }
    
    @Override
    public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SalesChannelService salesChannelService)
    {
      thirdPartyPresentationMap.put(CUSTOMERID, getSubscriberID());
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, Module.fromExternalRepresentation(getModuleID()).toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(ORIGIN, getSource());
      thirdPartyPresentationMap.put(RETURNCODE, getReturnCode());
      thirdPartyPresentationMap.put(RETURNCODEDETAILS, getReturnCodeDetails());
      thirdPartyPresentationMap.put(NOTIFICATION_SUBJECT, null);
      thirdPartyPresentationMap.put(NOTIFICATION_TEXT_BODY, getText());
      thirdPartyPresentationMap.put(NOTIFICATION_HTML_BODY, null);
      thirdPartyPresentationMap.put(NOTIFICATION_CHANNEL, "SMS");
      thirdPartyPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
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
    private String moduleID;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ActionManager(JSONObject configuration)
    {
      super(configuration);
      this.moduleID = JSONUtilities.decodeString(configuration, "moduleID", true);
      this.deliveryType = JSONUtilities.decodeString(configuration, "deliveryType", true);
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

      SMSMessage smsMessage = (SMSMessage) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.message");
      String source = (CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.source") != null) ? (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.source") : "TBD";
      boolean confirmationExpected = (Boolean) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.confirmationexpected");

      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      String msisdn = ((SubscriberProfile) subscriberEvaluationRequest.getSubscriberProfile()).getMSISDN();
      String text = smsMessage.resolve(subscriberEvaluationRequest);

      /*****************************************
      *
      *  request
      *
      *****************************************/

      SMSNotificationManagerRequest request = null;
      if (msisdn != null)
        {
          request = new SMSNotificationManagerRequest(evolutionEventContext, deliveryType, deliveryRequestSource, msisdn, source, text);
          request.setModuleID(moduleID);
          request.setFeatureID(deliveryRequestSource);
          request.setIsConfirmationExpected(confirmationExpected);
        }
      else
        {
          log.info("SMSNotificationManager unknown MSISDN for subscriberID {}" + subscriberEvaluationRequest.getSubscriberProfile().getSubscriberID());
        }

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

        log.debug("SMSNotificationManagerRequest run deliveryRequest;" + deliveryRequest);
        
        smsNotification.send((SMSNotificationManagerRequest)deliveryRequest);

      }
  }

  /*****************************************
  *
  *  updateDeliveryRequest
  *
  *****************************************/

  public void updateDeliveryRequest(DeliveryRequest deliveryRequest)
  {
    log.debug("SMSNotificationManager.updateDeliveryRequest(deliveryRequest="+deliveryRequest+")");
    updateRequest(deliveryRequest);
  }
  
  /*****************************************
  *
  *  completeDeliveryRequest
  *
  *****************************************/

  public void completeDeliveryRequest(DeliveryRequest deliveryRequest)
  {
    log.debug("SMSNotificationManager.updateDeliveryRequest(deliveryRequest="+deliveryRequest+")");
    completeRequest(deliveryRequest);
    stats.updateMessageCount(pluginName, 1, deliveryRequest.getDeliveryStatus());
  }

  /*****************************************
  *
  *  submitCorrelatorUpdateDeliveryRequest
  *
  *****************************************/

  public void submitCorrelatorUpdateDeliveryRequest(String correlator, JSONObject correlatorUpdate)
  {
    log.debug("SMSNotificationManager.submitCorrelatorUpdateDeliveryRequest(correlator="+correlator+", correlatorUpdate="+correlatorUpdate.toJSONString()+")");
    submitCorrelatorUpdate(correlator, correlatorUpdate);
  }
  
  /*****************************************
  *
  *  processCorrelatorUpdate
  *
  *****************************************/

  @Override protected void processCorrelatorUpdate(DeliveryRequest deliveryRequest, JSONObject correlatorUpdate)
  {
    int result = JSONUtilities.decodeInteger(correlatorUpdate, "result", true);
    SMSNotificationManagerRequest smsRequest = (SMSNotificationManagerRequest) deliveryRequest;
    if (smsRequest != null)
      {
        log.debug("SMSNotificationManager.processCorrelatorUpdate(deliveryRequest="+deliveryRequest.toString()+", correlatorUpdate="+correlatorUpdate.toJSONString()+")");
        smsRequest.setMessageStatus(SMSMessageStatus.fromReturnCode(result));
        smsRequest.setDeliveryStatus(getMessageStatus(smsRequest.getMessageStatus()));
        smsRequest.setDeliveryDate(SystemTime.getCurrentTime());
        completeRequest(smsRequest);
      }
  }

  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  @Override protected void shutdown()
  {
    log.info("SMSNotificationManager:  shutdown");
  }
  
  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    log.info("SMSNotificationManager: recieved " + args.length + " args");
    for(String arg : args)
      {
        log.info("SMSNotificationManager: arg " + arg);
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
    
    log.info("SMSNotificationManager: configuration " + Deployment.getDeliveryManagers());

    SMSNotificationManager manager = new SMSNotificationManager(deliveryManagerKey, pluginName, pluginConfiguration);

    //
    //  run
    //

    manager.run();
  }
}
