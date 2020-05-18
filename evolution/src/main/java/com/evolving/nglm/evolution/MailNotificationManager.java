/*****************************************************************************
*
*  MailNotificationManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.HashMap;

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
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.ContactPolicyCommunicationChannels.ContactType;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.PushNotificationManager.PushMessageStatus;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSMessageStatus;
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
    RESCHEDULE(709),
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
        case RESCHEDULE:
          return DeliveryStatus.Reschedule;
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
  *  configuration
  *
  *****************************************/

  private int threadNumber = 5;   //TODO : make this configurable
  private ArrayList<Thread> threads = new ArrayList<Thread>();
  private MailNotificationInterface mailNotification;
  private NotificationStatistics stats = null;
  private static String applicationID = "deliverymanager-notificationmanagermail";
  public String pluginName;
  private SubscriberMessageTemplateService subscriberMessageTemplateService;
  private CommunicationChannelBlackoutService blackoutService;

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(MailNotificationManager.class);

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public SubscriberMessageTemplateService getSubscriberMessageTemplateService() { return subscriberMessageTemplateService; }
  public CommunicationChannelBlackoutService getBlackoutService() { return blackoutService; }

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

    super(applicationID, deliveryManagerKey, Deployment.getBrokerServers(), MailNotificationManagerRequest.serde, Deployment.getDeliveryManagers().get(pluginName));

    //
    //  service
    //

    subscriberMessageTemplateService = new SubscriberMessageTemplateService(Deployment.getBrokerServers(), "mailnotificationmanager-subscribermessagetemplateservice-" + deliveryManagerKey, Deployment.getSubscriberMessageTemplateTopic(), false);
    subscriberMessageTemplateService.start();

    //
    //  blackoutService
    //
        
    blackoutService = new CommunicationChannelBlackoutService(Deployment.getBrokerServers(), "mailnotificationmanager-communicationchannelblackoutservice-" + deliveryManagerKey, Deployment.getCommunicationChannelBlackoutTopic(), false);
    blackoutService.start();

    //
    //  manager
    //

    this.pluginName = pluginName;
    
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
    // statistics
    //
    
    try{
      stats = new NotificationStatistics(applicationID, pluginName);
    }catch(Exception e){
      log.error("MailNotificationManager: could not load statistics ", e);
      throw new RuntimeException("MailNotificationManager: could not load statistics  ", e);
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

  public static class MailNotificationManagerRequest extends DeliveryRequest implements MessageDelivery
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
      schemaBuilder.field("fromAddress", Schema.STRING_SCHEMA);
      schemaBuilder.field("language", Schema.STRING_SCHEMA);
      schemaBuilder.field("templateID", Schema.STRING_SCHEMA);
      schemaBuilder.field("subjectTags", SchemaBuilder.array(Schema.STRING_SCHEMA));
      schemaBuilder.field("htmlBodyTags", SchemaBuilder.array(Schema.STRING_SCHEMA));
      schemaBuilder.field("textBodyTags", SchemaBuilder.array(Schema.STRING_SCHEMA));
      schemaBuilder.field("confirmationExpected", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("restricted", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("returnCode", Schema.INT32_SCHEMA);
      schemaBuilder.field("returnCodeDetails", Schema.OPTIONAL_STRING_SCHEMA);
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
    private String fromAddress;
    private String language;
    private String templateID;
    private List<String> subjectTags;
    private List<String> htmlBodyTags;
    private List<String> textBodyTags;
    private boolean confirmationExpected;
    private boolean restricted;
    private MAILMessageStatus status;
    private int returnCode;
    private String returnCodeDetails;

    //
    //  accessors
    //

    public String getDestination() { return destination; }
    public String getFromAddress() { return fromAddress; }
    public String getLanguage() { return language; }
    public String getTemplateID() { return templateID; }
    public List<String> getSubjectTags() { return subjectTags; }
    public List<String> getHtmlBodyTags() { return htmlBodyTags; }
    public List<String> getTextBodyTags() { return textBodyTags; }
    public boolean getConfirmationExpected() { return confirmationExpected; }
    public boolean getRestricted() { return restricted; }
    public MAILMessageStatus getMessageStatus() { return status; }
    public int getReturnCode() { return returnCode; }
    public String getReturnCodeDetails() { return returnCodeDetails; }

    //
    //  abstract
    //

    @Override public ActivityType getActivityType() { return ActivityType.Messages; }

    //
    //  setters
    //

    public void setConfirmationExpected(boolean confirmationExpected) { this.confirmationExpected = confirmationExpected; }
    public void setRestricted(boolean restricted) { this.restricted = restricted; }
    public void setMessageStatus(MAILMessageStatus status) { this.status = status; }
    public void setReturnCode(Integer returnCode) { this.returnCode = returnCode; }
    public void setReturnCodeDetails(String returnCodeDetails) { this.returnCodeDetails = returnCodeDetails; }

    //
    //  message delivery accessors
    //

    public int getMessageDeliveryReturnCode() { return getReturnCode(); }
    public String getMessageDeliveryReturnCodeDetails() { return getReturnCodeDetails(); }
    public String getMessageDeliveryOrigin() { return getFromAddress(); }
    public String getMessageDeliveryMessageId() { return getEventID(); }

    /*****************************************
    *
    *  getSubject
    *
    *****************************************/

    public String getSubject(SubscriberMessageTemplateService subscriberMessageTemplateService)
    {
      MailTemplate mailTemplate = (MailTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(templateID, SystemTime.getCurrentTime());
      DialogMessage dialogMessage = (mailTemplate != null) ? mailTemplate.getSubject() : null;
      String text = (dialogMessage != null) ? dialogMessage.resolve(language, subjectTags) : null;
      return text;
    }

    /*****************************************
    *
    *  getHtmlBody
    *
    *****************************************/

    public String getHtmlBody(SubscriberMessageTemplateService subscriberMessageTemplateService)
    {
      MailTemplate mailTemplate = (MailTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(templateID, SystemTime.getCurrentTime());
      DialogMessage dialogMessage = (mailTemplate != null) ? mailTemplate.getHTMLBody() : null;
      String text = (dialogMessage != null) ? dialogMessage.resolve(language, htmlBodyTags) : null;
      return text;
    }

    /*****************************************
    *
    *  getTextBody
    *
    *****************************************/

    public String getTextBody(SubscriberMessageTemplateService subscriberMessageTemplateService)
    {
      MailTemplate mailTemplate = (MailTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(templateID, SystemTime.getCurrentTime());
      DialogMessage dialogMessage = (mailTemplate != null) ? mailTemplate.getTextBody() : null;
      String text = (dialogMessage != null) ? dialogMessage.resolve(language, textBodyTags) : null;
      return text;
    }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public MailNotificationManagerRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource, String destination, String fromAddress, String language, String templateID, List<String> subjectTags, List<String> htmlBodyTags, List<String> textBodyTags)
    {
      super(context, deliveryType, deliveryRequestSource);
      this.destination = destination;
      this.fromAddress = fromAddress;
      this.language = language;
      this.templateID = templateID;
      this.subjectTags = subjectTags;
      this.htmlBodyTags = htmlBodyTags;
      this.textBodyTags = textBodyTags;
      this.status = MAILMessageStatus.PENDING;
      this.returnCode = status.getReturnCode();
      this.returnCodeDetails = null;
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
      this.fromAddress = JSONUtilities.decodeString(jsonRoot, "fromAddress", true);
      this.language = JSONUtilities.decodeString(jsonRoot, "language", true);
      this.templateID = JSONUtilities.decodeString(jsonRoot, "templateID", true);
      this.subjectTags = decodeMessageTags(JSONUtilities.decodeJSONArray(jsonRoot, "subjectTags", new JSONArray()));
      this.htmlBodyTags = decodeMessageTags(JSONUtilities.decodeJSONArray(jsonRoot, "htmlBodyTags", new JSONArray()));
      this.textBodyTags = decodeMessageTags(JSONUtilities.decodeJSONArray(jsonRoot, "textBodyTags", new JSONArray()));
      this.status = MAILMessageStatus.PENDING;
      this.returnCode = MAILMessageStatus.PENDING.getReturnCode();
      this.returnCodeDetails = null;
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

    private MailNotificationManagerRequest(SchemaAndValue schemaAndValue, String destination, String fromAddress, String language, String templateID, List<String> subjectTags, List<String> htmlBodyTags, List<String> textBodyTags, boolean confirmationExpected, boolean restricted, MAILMessageStatus status, String returnCodeDetails)
    {
      super(schemaAndValue);
      this.destination = destination;
      this.fromAddress = fromAddress;
      this.language = language;
      this.templateID = templateID;
      this.subjectTags = subjectTags;
      this.htmlBodyTags = htmlBodyTags;
      this.textBodyTags = textBodyTags;
      this.confirmationExpected = confirmationExpected;
      this.restricted = restricted;
      this.status = status;
      this.returnCode = status.getReturnCode();
      this.returnCodeDetails = returnCodeDetails;
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
      this.fromAddress = mailNotificationManagerRequest.getFromAddress();
      this.language = mailNotificationManagerRequest.getLanguage();
      this.templateID = mailNotificationManagerRequest.getTemplateID();
      this.subjectTags = mailNotificationManagerRequest.getSubjectTags();
      this.htmlBodyTags = mailNotificationManagerRequest.getHtmlBodyTags();
      this.textBodyTags = mailNotificationManagerRequest.getTextBodyTags();
      this.confirmationExpected = mailNotificationManagerRequest.getConfirmationExpected();
      this.restricted = mailNotificationManagerRequest.getRestricted();
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
      struct.put("fromAddress", notificationRequest.getFromAddress());
      struct.put("language", notificationRequest.getLanguage());
      struct.put("templateID", notificationRequest.getTemplateID());
      struct.put("subjectTags", notificationRequest.getSubjectTags());
      struct.put("htmlBodyTags", notificationRequest.getHtmlBodyTags());
      struct.put("textBodyTags", notificationRequest.getTextBodyTags());
      struct.put("confirmationExpected", notificationRequest.getConfirmationExpected());
      struct.put("restricted", notificationRequest.getRestricted());
      struct.put("returnCode", notificationRequest.getReturnCode());
      struct.put("returnCodeDetails", notificationRequest.getReturnCodeDetails());
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
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

      //
      //  unpack
      //

      Struct valueStruct = (Struct) value;
      String destination = valueStruct.getString("destination");
      String fromAddress = valueStruct.getString("fromAddress");
      String language = valueStruct.getString("language");
      String templateID = valueStruct.getString("templateID");
      List<String> subjectTags = (List<String>) valueStruct.get("subjectTags");
      List<String> htmlBodyTags = (List<String>) valueStruct.get("htmlBodyTags");
      List<String> textBodyTags = (List<String>) valueStruct.get("textBodyTags");
      boolean confirmationExpected = valueStruct.getBoolean("confirmationExpected");
      boolean restricted = valueStruct.getBoolean("restricted");
      Integer returnCode = valueStruct.getInt32("returnCode");
      String returnCodeDetails = valueStruct.getString("returnCodeDetails");
      MAILMessageStatus status = MAILMessageStatus.fromReturnCode(returnCode);
      
      //
      //  return
      //

      return new MailNotificationManagerRequest(schemaAndValue, destination, fromAddress, language, templateID, subjectTags, htmlBodyTags, textBodyTags, confirmationExpected, restricted, status, returnCodeDetails);
    }
    
    /****************************************
    *
    *  presentation utilities
    *
    ****************************************/
    
    //
    //  addFieldsForGUIPresentation
    //

    @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
    {
      Module module = Module.fromExternalRepresentation(getModuleID());
      guiPresentationMap.put(CUSTOMERID, getSubscriberID());
      guiPresentationMap.put(EVENTID, null);
      guiPresentationMap.put(MODULEID, getModuleID());
      guiPresentationMap.put(MODULENAME, module.toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      guiPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      guiPresentationMap.put(SOURCE, getFromAddress());
      guiPresentationMap.put(RETURNCODE, getReturnCode());
      guiPresentationMap.put(RETURNCODEDETAILS, MAILMessageStatus.fromReturnCode(getReturnCode()).toString());
      guiPresentationMap.put(NOTIFICATION_SUBJECT, getSubject(subscriberMessageTemplateService));
      guiPresentationMap.put(NOTIFICATION_TEXT_BODY, getTextBody(subscriberMessageTemplateService));
      guiPresentationMap.put(NOTIFICATION_HTML_BODY, getHtmlBody(subscriberMessageTemplateService));
      guiPresentationMap.put(NOTIFICATION_CHANNEL, "EMAIL");
      guiPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
    }
    
    //
    //  addFieldsForThirdPartyPresentation
    //

    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
    {
      Module module = Module.fromExternalRepresentation(getModuleID());
      thirdPartyPresentationMap.put(DELIVERYSTATUS, getMessageStatus().toString()); // replace value set by the superclass 
      thirdPartyPresentationMap.put(EVENTID, null);
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, module.toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(SOURCE, getFromAddress());
      thirdPartyPresentationMap.put(RETURNCODE, getReturnCode());
      thirdPartyPresentationMap.put(RETURNCODEDESCRIPTION, RESTAPIGenericReturnCodes.fromGenericResponseCode(getReturnCode()).toString());
      thirdPartyPresentationMap.put(RETURNCODEDETAILS, getReturnCodeDetails());
      thirdPartyPresentationMap.put(NOTIFICATION_SUBJECT, getSubject(subscriberMessageTemplateService));
      thirdPartyPresentationMap.put(NOTIFICATION_TEXT_BODY, getTextBody(subscriberMessageTemplateService));
      thirdPartyPresentationMap.put(NOTIFICATION_HTML_BODY, getHtmlBody(subscriberMessageTemplateService));
      thirdPartyPresentationMap.put(NOTIFICATION_CHANNEL, "EMAIL");
      thirdPartyPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
    }
    
    @Override
    public void resetDeliveryRequestAfterReSchedule()
    {
      this.setReturnCode(MAILMessageStatus.PENDING.getReturnCode());
      this.setMessageStatus(MAILMessageStatus.PENDING);
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

    public ActionManager(JSONObject configuration) throws GUIManagerException
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

    @Override public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      /*****************************************
      *
      *  parameters
      *
      *****************************************/

      EmailMessage emailMessage = (EmailMessage) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.message");
      ContactType contactType = ContactType.fromExternalRepresentation((String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.contacttype"));
      String fromAddress = (CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.fromaddress") != null) ? (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.fromaddress") : "TBD";
      boolean confirmationExpected = (Boolean) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.confirmationexpected");
      boolean restricted = contactType.getRestricted();

      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      deliveryRequestSource = extractWorkflowFeatureID(evolutionEventContext, subscriberEvaluationRequest, deliveryRequestSource);
      
      String email = ((SubscriberProfile) subscriberEvaluationRequest.getSubscriberProfile()).getEmail();
      String language = subscriberEvaluationRequest.getLanguage();
      MailTemplate baseTemplate = (MailTemplate) emailMessage.resolveTemplate(evolutionEventContext);
      MailTemplate template = (baseTemplate != null) ? (MailTemplate) baseTemplate.getReadOnlyCopy(evolutionEventContext) : null;

      //
      //  subject
      //

      DialogMessage subject = (template != null) ? template.getSubject() : null;
      List<String> subjectTags = (subject != null) ? subject.resolveMessageTags(subscriberEvaluationRequest, language) : new ArrayList<String>();

      //
      //  htmlBody
      //

      DialogMessage htmlBody = (template != null) ? template.getHTMLBody() : null;
      List<String> htmlBodyTags = (htmlBody != null) ? htmlBody.resolveMessageTags(subscriberEvaluationRequest, language) : new ArrayList<String>();

      //
      //  textBody
      //

      DialogMessage textBody = (template != null) ? template.getTextBody() : null;
      List<String> textBodyTags = (textBody != null) ? textBody.resolveMessageTags(subscriberEvaluationRequest, language) : new ArrayList<String>();
      
      /*****************************************
      *
      *  request
      *
      *****************************************/

      MailNotificationManagerRequest request = null;
      if (template != null && email != null)
        {
          request = new MailNotificationManagerRequest(evolutionEventContext, deliveryType, deliveryRequestSource, email, fromAddress, language, template.getMailTemplateID(), subjectTags, htmlBodyTags, textBodyTags);
          request.setModuleID(moduleID);
          request.setFeatureID(deliveryRequestSource);
          request.setConfirmationExpected(confirmationExpected);
          request.setRestricted(restricted);
          request.setDeliveryPriority(contactType.getDeliveryPriority());
          request.setNotificationHistory(evolutionEventContext.getSubscriberState().getNotificationHistory());
        }
      else
        {
          log.info("MailNotificationManager unknown email for subscriberID {}", subscriberEvaluationRequest.getSubscriberProfile().getSubscriberID());
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
    while (isProcessing())
      {
        /*****************************************
         *
         *  nextRequest
         *
         *****************************************/

        DeliveryRequest deliveryRequest = nextRequest();
        Date now = SystemTime.getCurrentTime();

        log.info("MailNotificationManagerRequest run deliveryRequest;" + deliveryRequest);

        MailNotificationManagerRequest mailRequest = (MailNotificationManagerRequest)deliveryRequest;
        
        if(mailRequest.getRestricted()) 
          {
            Date effectiveDeliveryTime = now;
            CommunicationChannel channel = Deployment.getCommunicationChannels().get("mail");
            if(channel != null) 
              {
                effectiveDeliveryTime = channel.getEffectiveDeliveryTime(blackoutService, now);
              }
            
            if(effectiveDeliveryTime.equals(now) || effectiveDeliveryTime.before(now))
              {
                log.debug("MailNotificationManagerRequest SEND Immediately restricted " + mailRequest);
                mailNotification.send(mailRequest);
              }
            else
              {
                log.debug("MailNotificationManagerRequest RESCHEDULE to " + effectiveDeliveryTime + " restricted " + mailRequest);
                mailRequest.setRescheduledDate(effectiveDeliveryTime);
                mailRequest.setDeliveryStatus(DeliveryStatus.Reschedule);
                mailRequest.setReturnCode(SMSMessageStatus.RESCHEDULE.getReturnCode());
                mailRequest.setMessageStatus(MAILMessageStatus.RESCHEDULE);
                completeDeliveryRequest(mailRequest);
              }
          }
        else 
          {
            log.debug("MailNotificationManagerRequest SEND Immediately NON restricted " + mailRequest);
            mailNotification.send(mailRequest);
          }
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
    stats.updateMessageCount(pluginName, 1, deliveryRequest.getDeliveryStatus());
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
    new LoggerInitialization().initLogger();
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
