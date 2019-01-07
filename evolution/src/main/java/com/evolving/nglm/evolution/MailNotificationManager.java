/*****************************************************************************
*
*  MailNotificationManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.DeliveryManager;
import com.evolving.nglm.evolution.DeliveryManagerDeclaration;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;

public class MailNotificationManager extends DeliveryManager implements Runnable
{
  /*****************************************
  *
  *  enum - status
  *
  *****************************************/

  public enum MAILMessageStatus
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
    UNKNOWN(999);
    private Integer returncode;
    private MAILMessageStatus(Integer returncode) { this.returncode = returncode; }
    public Integer getReturnCode() { return returncode; }
    public static MAILMessageStatus fromReturnCode(Integer externalRepresentation) { for (MAILMessageStatus enumeratedValue : MAILMessageStatus.values()) { if (enumeratedValue.getReturnCode().equals(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
  }

  /*****************************************
  *
  *  conversion method
  *
  *****************************************/

  public DeliveryStatus getMessageStatus (MAILMessageStatus status)
  {
    switch(status)
      {
        case PENDING:
          return DeliveryStatus.Pending;
        case SENT:
          return DeliveryStatus.Delivered;
        case NO_CUSTOMER_LANGUAGE:
        case NO_CUSTOMER_CHANNEL:
        case ERROR:
        case UNDELIVERABLE:
        case INVALID:
        case QUEUE_FULL:
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

  private int threadNumber = 5;   //TODO : make this configurable
  private ArrayList<Thread> threads = new ArrayList<Thread>();
  private MailNotificationInterface mailNotification;

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(MailNotificationManager.class);

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public MailNotificationManager(String deliveryManagerKey, String pluginName, String pluginConfiguration)
  {
    //
    //  superclass
    //

    super("deliverymanager-notificationmanagermail", deliveryManagerKey, Deployment.getBrokerServers(), MailNotificationManagerRequest.serde, Deployment.getDeliveryManagers().get(pluginName));

    //
    //  manager
    //

    String mailPluginClassName = JSONUtilities.decodeString(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "notificationPluginClass", true);
    log.info("MailNotificationManager: plugin instanciation : mailPluginClassName = "+mailPluginClassName);

    JSONObject mailPluginConfiguration = JSONUtilities.decodeJSONObject(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "notificationPluginConfiguration", true);
    log.info("MailNotificationManager: plugin instanciation : mailPluginConfiguration = "+mailPluginConfiguration);

    try
      {
        mailNotification = (MailNotificationInterface) (Class.forName(mailPluginClassName).newInstance());
        mailNotification.init(this, mailPluginConfiguration, pluginConfiguration, pluginName);
      }
    catch (InstantiationException | IllegalAccessException | IllegalArgumentException e)
      {
        log.error("MailNotificationManager: could not create new instance of class " + mailPluginClassName, e);
        throw new RuntimeException("MailNotificationManager: could not create new instance of class " + mailPluginClassName, e);
      }
    catch (ClassNotFoundException e)
      {
        log.error("MailNotificationManager: could not find class " + mailPluginClassName, e);
        throw new RuntimeException("MailNotificationManager: could not find class " + mailPluginClassName, e);
      }

    //
    //  threads
    //

    for(int i = 0; i < threadNumber; i++)
      {
        threads.add(new Thread(this, "MailNotificationManagerThread_"+i));
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

  public static class MailNotificationManagerRequest extends DeliveryRequest
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
      schemaBuilder.name("service_mailnotification_request");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("destination", Schema.STRING_SCHEMA);
      schemaBuilder.field("source", Schema.STRING_SCHEMA);
      schemaBuilder.field("subject", Schema.STRING_SCHEMA);
      schemaBuilder.field("htmlBody", Schema.STRING_SCHEMA);
      schemaBuilder.field("textBody", Schema.STRING_SCHEMA);
      schemaBuilder.field("return_code", Schema.INT32_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<MailNotificationManagerRequest> serde = new ConnectSerde<MailNotificationManagerRequest>(schema, false, MailNotificationManagerRequest.class, MailNotificationManagerRequest::pack, MailNotificationManagerRequest::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<MailNotificationManagerRequest> serde() { return serde; }
    public Schema subscriberStreamEventSchema() { return schema(); }

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String destination;
    private String source;
    private String subject;
    private String htmlBody;
    private String textBody;
    private MAILMessageStatus status;
    private int returnCode;
    private String returnCodeDetails;

    //
    //  accessors
    //

    public String getDestination() { return destination; }
    public String getSource() { return source; }
    public String getSubject() { return subject; }
    public String getHtmlBody() { return htmlBody; }
    public String getTextBody() { return textBody; }
    public MAILMessageStatus getMessageStatus() { return status; }
    public int getReturnCode() { return returnCode; }
    public String getReturnCodeDetails() { return returnCodeDetails; }

    //
    //  setters
    //

    public void setMessageStatus(MAILMessageStatus status) { this.status = status; }
    public void setReturnCode(int returnCode) { this.returnCode = returnCode; }
    public void setReturnCodeDetails(String returnCodeDetails) { this.returnCodeDetails = returnCodeDetails; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public MailNotificationManagerRequest(EvolutionEventContext context, String deliveryRequestSource, String destination, String source, String subject, String htmlBody, String textBody)
    {
      super(context, "notification", deliveryRequestSource);
      this.destination = destination;
      this.source = source;
      this.subject = subject;
      this.htmlBody = htmlBody;
      this.textBody = textBody;
      this.status = MAILMessageStatus.PENDING;
      this.returnCode = MAILMessageStatus.PENDING.getReturnCode();
      this.returnCodeDetails = "";
    }

    /*****************************************
    *
    *  constructor -- external
    *
    *****************************************/

    public MailNotificationManagerRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
    {
      super(jsonRoot);
      this.destination = JSONUtilities.decodeString(jsonRoot, "destination", true);
      this.source = JSONUtilities.decodeString(jsonRoot, "source", true);
      this.subject = JSONUtilities.decodeString(jsonRoot, "subject", true);
      this.htmlBody = JSONUtilities.decodeString(jsonRoot, "htmlBody", true);
      this.textBody = JSONUtilities.decodeString(jsonRoot, "textBody", true);
      this.status = MAILMessageStatus.PENDING;
      this.returnCode = MAILMessageStatus.PENDING.getReturnCode();
      this.returnCodeDetails = "";
    }

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private MailNotificationManagerRequest(SchemaAndValue schemaAndValue, String destination, String source, String subject, String htmlBody, String textBody, MAILMessageStatus status)
    {
      super(schemaAndValue);
      this.destination = destination;
      this.source = source;
      this.subject = subject;
      this.htmlBody = htmlBody;
      this.textBody = textBody;
      this.status = status;
      this.returnCode = status.getReturnCode();
    }

    /*****************************************
    *
    *  constructor -- copy
    *
    *****************************************/

    private MailNotificationManagerRequest(MailNotificationManagerRequest mailNotificationManagerRequest)
    {
      super(mailNotificationManagerRequest);
      this.destination = mailNotificationManagerRequest.getDestination();
      this.source = mailNotificationManagerRequest.getSource();
      this.subject = mailNotificationManagerRequest.getSubject();
      this.htmlBody = mailNotificationManagerRequest.getHtmlBody();
      this.textBody = mailNotificationManagerRequest.getTextBody();
      this.status = mailNotificationManagerRequest.getMessageStatus();
      this.returnCode = mailNotificationManagerRequest.getReturnCode();
      this.returnCodeDetails = mailNotificationManagerRequest.getReturnCodeDetails();
    }

    /*****************************************
    *
    *  copy
    *
    *****************************************/

    public MailNotificationManagerRequest copy()
    {
      return new MailNotificationManagerRequest(this);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      MailNotificationManagerRequest notificationRequest = (MailNotificationManagerRequest) value;
      Struct struct = new Struct(schema);
      packCommon(struct, notificationRequest);
      struct.put("destination", notificationRequest.getDestination());
      struct.put("source", notificationRequest.getSource());
      struct.put("subject", notificationRequest.getSubject());
      struct.put("htmlBody", notificationRequest.getHtmlBody());
      struct.put("textBody", notificationRequest.getTextBody());
      struct.put("return_code", notificationRequest.getReturnCode());
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

    public static MailNotificationManagerRequest unpack(SchemaAndValue schemaAndValue)
    {
      //
      //  data
      //

      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

      //
      //  unpack
      //

      Struct valueStruct = (Struct) value;
      String destination = valueStruct.getString("destination");
      String source = valueStruct.getString("source");
      String subject = valueStruct.getString("subject");
      String htmlBody = valueStruct.getString("htmlBody");
      String textBody = valueStruct.getString("textBody");
      Integer returnCode = valueStruct.getInt32("return_code");
      MAILMessageStatus status = MAILMessageStatus.fromReturnCode(returnCode);

      //
      //  return
      //

      return new MailNotificationManagerRequest(schemaAndValue, destination, source, subject, htmlBody, textBody, status);
    }
    
    @Override public Integer getActivityType() { return ActivityType.Messages.getExternalRepresentation(); }
    
    /****************************************
    *
    *  presentation utilities
    *
    ****************************************/
    
    @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap)
    {
      guiPresentationMap.put(CUSTOMERID, getSubscriberID());
      guiPresentationMap.put(MODULEID, getModuleID());
      guiPresentationMap.put(MODULENAME, Module.fromModuleId(getModuleID()).toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(ORIGIN, "");
      guiPresentationMap.put(RETURNCODE, "TO DO:");
      guiPresentationMap.put(RETURNCODEDETAILS, "TO DO:");
      guiPresentationMap.put(NOTIFICATION_SUBJECT, getSubject());
      guiPresentationMap.put(NOTIFICATION_TEXT_BODY, getTextBody());
      guiPresentationMap.put(NOTIFICATION_HTML_BODY, getHtmlBody());
      guiPresentationMap.put(NOTIFICATION_CHANNEL, "EMAIL");
      guiPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
    }
    
    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap)
    {
      thirdPartyPresentationMap.put(CUSTOMERID, getSubscriberID());
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, Module.fromModuleId(getModuleID()).toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(ORIGIN, "");
      thirdPartyPresentationMap.put(RETURNCODE, "TO DO:");
      thirdPartyPresentationMap.put(RETURNCODEDETAILS, "TO DO:");
      thirdPartyPresentationMap.put(NOTIFICATION_SUBJECT, getSubject());
      thirdPartyPresentationMap.put(NOTIFICATION_TEXT_BODY, getTextBody());
      thirdPartyPresentationMap.put(NOTIFICATION_HTML_BODY, getHtmlBody());
      thirdPartyPresentationMap.put(NOTIFICATION_CHANNEL, "EMAIL");
      thirdPartyPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
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

        log.info("SMSNotificationManagerRequest run deliveryRequest;" + deliveryRequest);

        mailNotification.send((MailNotificationManagerRequest)deliveryRequest);

      }
  }

  /*****************************************
  *
  *  updateDeliveryRequest
  *
  *****************************************/

  public void updateDeliveryRequest(DeliveryRequest deliveryRequest)
  {
    log.info("MailNotificationManager.updateDeliveryRequest(deliveryRequest="+deliveryRequest+")");
    updateRequest(deliveryRequest);
  }

  /*****************************************
  *
  *  completeDeliveryRequest
  *
  *****************************************/

  public void completeDeliveryRequest(DeliveryRequest deliveryRequest)
  {
    log.info("MailNotificationManager.updateDeliveryRequest(deliveryRequest="+deliveryRequest+")");
    completeRequest(deliveryRequest);
  }

  /*****************************************
  *
  *  submitCorrelatorUpdateDeliveryRequest
  *
  *****************************************/

  public void submitCorrelatorUpdateDeliveryRequest(String correlator, JSONObject correlatorUpdate)
  {
    log.info("MailNotificationManager.submitCorrelatorUpdateDeliveryRequest(correlator="+correlator+", correlatorUpdate="+correlatorUpdate.toJSONString()+")");
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
    MailNotificationManagerRequest mailRequest = (MailNotificationManagerRequest) deliveryRequest;
    if (mailRequest != null)
      {
        mailRequest.setMessageStatus(MAILMessageStatus.fromReturnCode(result));
        mailRequest.setDeliveryStatus(getMessageStatus(mailRequest.getMessageStatus()));
        mailRequest.setDeliveryDate(SystemTime.getCurrentTime());
        completeRequest(mailRequest);
      }
  }

  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  @Override protected void shutdown()
  {
    log.info("MailNotificationManager:  shutdown");
  }

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    log.info("MailNotificationManager: recieved " + args.length + " args");
    for(String arg : args)
      {
        log.info("MailNotificationManager: arg " + arg);
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

    log.info("MailNotificationManager: configuration " + Deployment.getDeliveryManagers());

    MailNotificationManager manager = new MailNotificationManager(deliveryManagerKey, pluginName, pluginConfiguration);

    //
    //  run
    //

    manager.run();
  }
}
