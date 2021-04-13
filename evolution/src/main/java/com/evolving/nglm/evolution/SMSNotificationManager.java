/*****************************************************************************
*
*  SMSNotificationManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.ContactPolicyCommunicationChannels.ContactType;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;

public class SMSNotificationManager extends DeliveryManagerForNotifications implements Runnable
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private static final int threadNumber = 1;   //TODO : make this configurable (not even used)
  private SMSNotificationInterface smsNotification;
  private ArrayList<Thread> threads = new ArrayList<Thread>();
  private static String applicationID = "deliverymanager-notificationmanagersms";
  public String pluginName;

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SMSNotificationManager.class);
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/


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
    
    super(applicationID, deliveryManagerKey, Deployment.getBrokerServers(), SMSNotificationManagerRequest.serde(), Deployment.getDeliveryManagers().get(pluginName), threadNumber);
    
    //
    //  plugin class
    //

    String smsPluginClassName = JSONUtilities.decodeString(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "notificationPluginClass", true);
    JSONObject smsPluginConfiguration = JSONUtilities.decodeJSONObject(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "notificationPluginConfiguration", true);
    log.info("SMSNotificationManager: plugin instanciation : smsPluginClassName = "+smsPluginClassName);
    log.info("SMSNotificationManager: plugin instanciation : smsPluginConfiguration = "+smsPluginConfiguration);

    //
    //  manager
    //
    
    this.pluginName = pluginName;
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
  
  public static class SMSNotificationManagerRequest extends DeliveryRequest implements MessageDelivery, INotificationRequest
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
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(), 9));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("destination", Schema.STRING_SCHEMA);
      schemaBuilder.field("source", Schema.STRING_SCHEMA);
      schemaBuilder.field("language", Schema.STRING_SCHEMA);
      schemaBuilder.field("templateID", Schema.STRING_SCHEMA);
      schemaBuilder.field("messageTags", SchemaBuilder.array(Schema.STRING_SCHEMA));
      schemaBuilder.field("confirmationExpected", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("restricted", SchemaBuilder.bool().defaultValue(false).schema());
      schemaBuilder.field("flashSMS", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("returnCode", Schema.INT32_SCHEMA);
      schemaBuilder.field("returnCodeDetails", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("contactType", SchemaBuilder.string().defaultValue("unknown").schema());
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

    private String destination;
    private String source;
    private String language;
    private String templateID;
    private List<String> messageTags;
    private boolean confirmationExpected;
    private boolean restricted;
    private boolean flashSMS;
    private MessageStatus status;
    private int returnCode;
    private String returnCodeDetails;
    private String contactType;

    //
    //  accessors
    //

    public String getDestination() { return destination; }
    public String getSource() { return source; }
    public String getLanguage() { return language; }
    public String getTemplateID() { return templateID; }
    public List<String> getMessageTags() { return messageTags; }
    public boolean getConfirmationExpected() { return confirmationExpected; }
    public boolean getRestricted() { return restricted; }
    public boolean getFlashSMS() { return flashSMS; }
    public MessageStatus getMessageStatus() { return status; }
    public int getReturnCode() { return returnCode; }
    public String getReturnCodeDetails() { return returnCodeDetails; }
    public String getContactType() { return contactType; }

    //
    //  abstract
    //

    @Override public ActivityType getActivityType() { return ActivityType.Messages; }

    //
    //  setters
    //

    public void setConfirmationExpected(boolean confirmationExpected) { this.confirmationExpected = confirmationExpected; }
    public void setRestricted(boolean restricted) { this.restricted = restricted; }
    public void setFlashSMS(boolean flashSMS) { this.flashSMS = flashSMS; }
    public void setMessageStatus(MessageStatus status) { this.status = status; }
    public void setReturnCode(Integer returnCode) { this.returnCode = returnCode; }
    public void setReturnCodeDetails(String returnCodeDetails) { this.returnCodeDetails = returnCodeDetails; }

    //
    //  message delivery accessors
    //

    public int getMessageDeliveryReturnCode() { return getReturnCode(); }
    public String getMessageDeliveryReturnCodeDetails() { return getReturnCodeDetails(); }
    public String getMessageDeliveryOrigin() { return ""; }
    public String getMessageDeliveryMessageId() { return getEventID(); }

    /*****************************************
    *
    *  getText
    *
    *****************************************/

    public String getText(SubscriberMessageTemplateService subscriberMessageTemplateService)
    {
      SMSTemplate smsTemplate = (SMSTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(templateID, SystemTime.getCurrentTime());
      DialogMessage dialogMessage = (smsTemplate != null) ? smsTemplate.getMessageText() : null;
      String text = (dialogMessage != null) ? dialogMessage.resolve(language, messageTags, getTenantID()) : null;
      return text;
    }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public SMSNotificationManagerRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource, String destination, String source, String language, String templateID, List<String> messageTags, String contactType, int tenantID)
    {
      super(context, deliveryType, deliveryRequestSource, tenantID);
      this.destination = destination;
      this.source = source;
      this.language = language;
      this.templateID = templateID;
      this.messageTags = messageTags;
      this.status = MessageStatus.PENDING;
      this.returnCode = status.getReturnCode();
      this.returnCodeDetails = null;
      this.contactType = contactType;
    }

    /*****************************************
    *
    *  constructor : minimum for reports
    *
    *****************************************/

    public SMSNotificationManagerRequest(String templateID, String language, List<String> messageTags)
    {
      this.language = language;
      this.templateID = templateID;
      this.messageTags = messageTags;
    }

    /*****************************************
    *
    *  decodeMessageTags
    *
    *****************************************/

    private List<String> decodeMessageTags(JSONArray jsonArray)
    {
      List<String> messageTags = new ArrayList<String>();
      for (int i=0; i<jsonArray.size(); i++)
        {
          messageTags.add((String) jsonArray.get(i));
        }
      return messageTags;
    }

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private SMSNotificationManagerRequest(SchemaAndValue schemaAndValue, String destination, String source, String language, String templateID, List<String> messageTags, boolean confirmationExpected, boolean restricted, boolean flashSMS, MessageStatus status, String returnCodeDetails, String contactType)
    {
      super(schemaAndValue);
      this.destination = destination;
      this.source = source;
      this.language = language;
      this.templateID = templateID;
      this.messageTags = messageTags;
      this.confirmationExpected = confirmationExpected;
      this.restricted = restricted;
      this.flashSMS = flashSMS;
      this.status = status;
      this.returnCode = status.getReturnCode();
      this.returnCodeDetails = returnCodeDetails;
      this.contactType = contactType;
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
      this.language = smsNotificationManagerRequest.getLanguage();
      this.templateID = smsNotificationManagerRequest.getTemplateID();
      this.messageTags = smsNotificationManagerRequest.getMessageTags();
      this.confirmationExpected = smsNotificationManagerRequest.getConfirmationExpected();
      this.restricted = smsNotificationManagerRequest.getRestricted();
      this.flashSMS = smsNotificationManagerRequest.getFlashSMS();
      this.status = smsNotificationManagerRequest.getMessageStatus();
      this.returnCode = smsNotificationManagerRequest.getReturnCode();
      this.returnCodeDetails = smsNotificationManagerRequest.getReturnCodeDetails();
      this.contactType = smsNotificationManagerRequest.getContactType();
    }

    /*****************************************
    *
    *  constructor : es - minimum
    *
    *****************************************/
    
    public SMSNotificationManagerRequest(Map<String, Object> esFields)
    {
      super(esFields);
      setCreationDate(getDateFromESString(esDateFormat, (String) esFields.get("creationDate")));
      setDeliveryDate(getDateFromESString(esDateFormat, (String) esFields.get("deliveryDate")));
      
      this.destination = (String) esFields.get("destination");
      this.source = (String) esFields.get("source");
      this.language = (String) esFields.get("language");
      this.templateID = (String) esFields.get("templateID");
      if (esFields.get("tags") != null)
        {
          Map<String,List<String>> tags = (Map<String, List<String>>) esFields.get("tags");
          this.messageTags = tags.get("tags");
        }
      this.returnCode = (Integer) esFields.get("returnCode");
      this.returnCodeDetails = (String) esFields.get("returnCodeDetails");
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
      struct.put("language", notificationRequest.getLanguage());
      struct.put("templateID", notificationRequest.getTemplateID());
      struct.put("messageTags", notificationRequest.getMessageTags());
      struct.put("confirmationExpected", notificationRequest.getConfirmationExpected());
      struct.put("restricted", notificationRequest.getRestricted());
      struct.put("flashSMS", notificationRequest.getFlashSMS());
      struct.put("returnCode", notificationRequest.getReturnCode());
      struct.put("returnCodeDetails", notificationRequest.getReturnCodeDetails());
      struct.put("contactType", notificationRequest.getContactType());
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
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion2(schema.version()) : null;

      //
      //  unpack
      //

      Struct valueStruct = (Struct) value;
      String destination = valueStruct.getString("destination");
      String source = valueStruct.getString("source");
      String language = valueStruct.getString("language");
      String templateID = valueStruct.getString("templateID");
      List<String> messageTags = (List<String>) valueStruct.get("messageTags");
      boolean confirmationExpected = valueStruct.getBoolean("confirmationExpected");
      boolean restricted = (schemaVersion >= 2) ? valueStruct.getBoolean("restricted") : false;
      boolean flashSMS = valueStruct.getBoolean("flashSMS");
      Integer returnCode = valueStruct.getInt32("returnCode");
      String returnCodeDetails = valueStruct.getString("returnCodeDetails");
      MessageStatus status = MessageStatus.fromReturnCode(returnCode);
      String contactType = schemaVersion >= 9 ? valueStruct.getString("contactType") : "unknown";
      
      //
      //  return
      //

      return new SMSNotificationManagerRequest(schemaAndValue, destination, source, language, templateID, messageTags, confirmationExpected, restricted, flashSMS, status, returnCodeDetails, contactType);
    }
    
    /****************************************
    *
    *  presentation utilities
    *
    ****************************************/
    
    //
    //  addFieldsForGUIPresentation
    //

    @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService, int tenantID)
    {
      guiPresentationMap.put(CUSTOMERID, getSubscriberID());
      guiPresentationMap.put(EVENTID, null);
      guiPresentationMap.put(MODULEID, getModuleID());
      guiPresentationMap.put(MODULENAME, getModule().toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(FEATURENAME, getFeatureName(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
      guiPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
      guiPresentationMap.put(SOURCE, getSource());
      guiPresentationMap.put(RETURNCODE, getReturnCode());
      guiPresentationMap.put(RETURNCODEDETAILS, MessageStatus.fromReturnCode(getReturnCode()).toString());
      guiPresentationMap.put(NOTIFICATION_TEXT_BODY, getText(subscriberMessageTemplateService));
      guiPresentationMap.put(NOTIFICATION_CHANNEL, "SMS");
      guiPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
      guiPresentationMap.put("contactType", getContactType());
    }
    
    //
    //  addFieldsForThirdPartyPresentation
    //

    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService, int tenantID)
    {
      thirdPartyPresentationMap.put(DELIVERYSTATUS, getMessageStatus().toString()); // replace value set by the superclass 
      thirdPartyPresentationMap.put(EVENTID, null);
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, getModule().toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(SOURCE, getSource());
      thirdPartyPresentationMap.put(RETURNCODE, getReturnCode());
      thirdPartyPresentationMap.put(RETURNCODEDESCRIPTION, RESTAPIGenericReturnCodes.fromGenericResponseCode(getReturnCode()).getGenericResponseMessage());
      thirdPartyPresentationMap.put(RETURNCODEDETAILS, getReturnCodeDetails());
      thirdPartyPresentationMap.put(NOTIFICATION_TEXT_BODY, getText(subscriberMessageTemplateService));
      thirdPartyPresentationMap.put(NOTIFICATION_CHANNEL, "SMS");
      thirdPartyPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
      thirdPartyPresentationMap.put("contactType", getContactType());
    }
    
    @Override
    public void resetDeliveryRequestAfterReSchedule()
    {
      this.setReturnCode(MessageStatus.PENDING.getReturnCode());
      this.setMessageStatus(MessageStatus.PENDING);
      
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
    private boolean isFlashSMS;

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
      this.isFlashSMS = JSONUtilities.decodeBoolean(configuration, "isFlashSMS", false) != null ? JSONUtilities.decodeBoolean(configuration, "isFlashSMS", false) : false;
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

      SMSMessage smsMessage = (SMSMessage) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.message");
      ContactType contactType = ContactType.fromExternalRepresentation((String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.contacttype"));
      String source = (CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.source") != null) ? (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.source") : "TBD";
      boolean confirmationExpected = (Boolean) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.confirmationexpected");
      boolean restricted = contactType.getRestricted();
      boolean flashSMS = false;     

      if (this.isFlashSMS == true) {
        flashSMS = true;
     }
      

      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String journeyID = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      Journey journey = evolutionEventContext.getJourneyService().getActiveJourney(journeyID, evolutionEventContext.now());
      String newModuleID = moduleID;
      if (journey != null && journey.getGUIManagedObjectType() == GUIManagedObjectType.LoyaltyWorkflow)
        {
          newModuleID = Module.Loyalty_Program.getExternalRepresentation();
        }
      
      String deliveryRequestSource = extractWorkflowFeatureID(evolutionEventContext, subscriberEvaluationRequest, journeyID);
      
      String msisdn = ((SubscriberProfile) subscriberEvaluationRequest.getSubscriberProfile()).getMSISDN();
      String language = subscriberEvaluationRequest.getLanguage();
      SMSTemplate baseTemplate = (SMSTemplate) smsMessage.resolveTemplate(evolutionEventContext);
      SMSTemplate template = (baseTemplate != null) ? (SMSTemplate) baseTemplate.getReadOnlyCopy(evolutionEventContext) : null;
      DialogMessage messageText = (template != null) ? template.getMessageText() : null;
      List<String> messageTags = (messageText != null) ? messageText.resolveMessageTags(subscriberEvaluationRequest, language) : new ArrayList<String>();


      
      /*****************************************
      *
      *  request
      *
      *****************************************/

      SMSNotificationManagerRequest request = null;
      if (template != null && msisdn != null)
        {
          request = new SMSNotificationManagerRequest(evolutionEventContext, deliveryType, deliveryRequestSource, msisdn, source, language, template.getSMSTemplateID(), messageTags, contactType.getExternalRepresentation(), subscriberEvaluationRequest.getTenantID());
          
          request.setModuleID(newModuleID);
          request.setFeatureID(deliveryRequestSource);
          request.setConfirmationExpected(confirmationExpected);
          request.setRestricted(restricted);
          request.setFlashSMS(flashSMS);
          request.forceDeliveryPriority(contactType.getDeliveryPriority());
        }
      else if (template != null)
        {
          log.info("SMSNotificationManager unknown MSISDN for subscriberID {}" + subscriberEvaluationRequest.getSubscriberProfile().getSubscriberID());
        }
      else
        {
          log.info("SMSNotificationManager unknown template {}" + smsMessage.getSubscriberMessageTemplateID());
        }

      /*****************************************
      *
      *  return
      *
      *****************************************/

      return (request != null) ? Collections.<Action>singletonList(request) : Collections.<Action>emptyList();
    }
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/

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
        Date now = SystemTime.getCurrentTime();
        
        log.debug("SMSNotificationManagerRequest run deliveryRequest:" + deliveryRequest);

        SMSNotificationManagerRequest smsRequest = (SMSNotificationManagerRequest)deliveryRequest;
        if(smsRequest.getRestricted()) 
          {
            Date effectiveDeliveryTime = now;
            String channelID = Deployment.getDeployment(deliveryRequest.getTenantID()).getDeliveryTypeCommunicationChannelIDMap().get(smsRequest.getDeliveryType());
            CommunicationChannel channel = Deployment.getDeployment(deliveryRequest.getTenantID()).getCommunicationChannels().get(channelID);
            if(channel != null) 
              {
                effectiveDeliveryTime = channel.getEffectiveDeliveryTime(getBlackoutService(), getTimeWindowService(), now, smsRequest.getTenantID());
              }
            
            if(effectiveDeliveryTime.equals(now) || effectiveDeliveryTime.before(now))
              {
                log.debug("SMSNotificationManagerRequest SEND Immediately restricted " + smsRequest);
                smsNotification.send(smsRequest);
              }
            else
              {
                log.debug("SMSNotificationManagerRequest RESCHEDULE to " + effectiveDeliveryTime + " restricted " + smsRequest);
                smsRequest.setRescheduledDate(effectiveDeliveryTime);
                smsRequest.setDeliveryStatus(DeliveryStatus.Reschedule);
                smsRequest.setReturnCode(MessageStatus.RESCHEDULE.getReturnCode());
                smsRequest.setMessageStatus(MessageStatus.RESCHEDULE);
                completeDeliveryRequest((DeliveryRequest)smsRequest);
              }
          }
        else
          {
            log.debug("SMSNotificationManagerRequest SEND Immediately NON restricted " + smsRequest);
            smsNotification.send(smsRequest);
          }
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
    new LoggerInitialization().initLogger();
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
