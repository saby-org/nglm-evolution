/*****************************************************************************
*
*  PushNotificationManager.java
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
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class PushNotificationManager extends DeliveryManager implements Runnable
{
  /*****************************************
  *
  *  enum - status
  *
  *****************************************/

  public enum PushMessageStatus
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
    private PushMessageStatus(Integer returncode) { this.returncode = returncode; }
    public Integer getReturnCode() { return returncode; }
    public static PushMessageStatus fromReturnCode(Integer externalRepresentation) { for (PushMessageStatus enumeratedValue : PushMessageStatus.values()) { if (enumeratedValue.getReturnCode().equals(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
  }

  /*****************************************
  *
  *  conversion method
  *
  *****************************************/

  public DeliveryStatus getMessageStatus (PushMessageStatus status)
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
  private PushNotificationInterface pushNotification;
  private NotificationStatistics stats = null;
  private static String applicationID = "deliverymanager-notificationmanagerpush";
  public String pluginName;
  private SubscriberMessageTemplateService subscriberMessageTemplateService;
  private CommunicationChannelService communicationChannelService;
  private CommunicationChannelBlackoutService blackoutService;

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(PushNotificationManager.class);

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public SubscriberMessageTemplateService getSubscriberMessageTemplateService() { return subscriberMessageTemplateService; }
  public CommunicationChannelService getCommunicationChannelService() { return communicationChannelService; }
  public CommunicationChannelBlackoutService getBlackoutService() { return blackoutService; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PushNotificationManager(String deliveryManagerKey, String pluginName)
  {
    //
    //  superclass
    //

    super(applicationID, deliveryManagerKey, Deployment.getBrokerServers(), PushNotificationManagerRequest.serde, Deployment.getDeliveryManagers().get(pluginName));

    //
    //  service
    //

    subscriberMessageTemplateService = new SubscriberMessageTemplateService(Deployment.getBrokerServers(), "pushnotificationmanager-subscribermessagetemplateservice-" + deliveryManagerKey, Deployment.getSubscriberMessageTemplateTopic(), false);
    subscriberMessageTemplateService.start();

    //
    //  communicationChannelService
    //

    communicationChannelService = new CommunicationChannelService(Deployment.getBrokerServers(), "pushnotificationmanager-communicationchannelservice-" + deliveryManagerKey, Deployment.getCommunicationChannelTopic(), false);
    communicationChannelService.start();

    //
    //  blackoutService
    //
        
    blackoutService = new CommunicationChannelBlackoutService(Deployment.getBrokerServers(), "pushnotificationmanager-communicationchannelblackoutservice-" + deliveryManagerKey, Deployment.getCommunicationChannelBlackoutTopic(), false);
    blackoutService.start();

    //
    //  manager
    //

    this.pluginName = pluginName;
    
    String pushPluginClassName = JSONUtilities.decodeString(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "notificationPluginClass", true);
    log.info("PushNotificationManager: plugin instanciation : pushPluginClassName = "+pushPluginClassName);

    JSONObject pushPluginConfiguration = JSONUtilities.decodeJSONObject(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "notificationPluginConfiguration", true);
    log.info("PushNotificationManager: plugin instanciation : pushPluginConfiguration = "+pushPluginConfiguration);

    try
      {
        pushNotification = (PushNotificationInterface) (Class.forName(pushPluginClassName).newInstance());
        pushNotification.init(this, pushPluginConfiguration, pluginName);
      }
    catch (InstantiationException | IllegalAccessException | IllegalArgumentException e)
      {
        log.error("PushNotificationManager: could not create new instance of class " + pushPluginClassName, e);
        throw new RuntimeException("PushNotificationManager: could not create new instance of class " + pushPluginClassName, e);
      }
    catch (ClassNotFoundException e)
      {
        log.error("PushNotificationManager: could not find class " + pushPluginClassName, e);
        throw new RuntimeException("PushNotificationManager: could not find class " + pushPluginClassName, e);
      }

    //
    // statistics
    //
    
    try{
      stats = new NotificationStatistics(applicationID, pluginName);
    }catch(Exception e){
      log.error("PushNotificationManager: could not load statistics ", e);
      throw new RuntimeException("PushNotificationManager: could not load statistics  ", e);
    }
    
    //
    //  threads
    //

    for(int i = 0; i < threadNumber; i++)
      {
        threads.add(new Thread(this, "PushNotificationManagerThread_"+i));
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

  public static class PushNotificationManagerRequest extends DeliveryRequest
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
      schemaBuilder.name("service_pushnotification_request");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("destination", Schema.STRING_SCHEMA);
      schemaBuilder.field("language", Schema.STRING_SCHEMA);
      schemaBuilder.field("templateID", Schema.STRING_SCHEMA);
      schemaBuilder.field("tags", SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).name("push_notification_tags").schema());
      schemaBuilder.field("confirmationExpected", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("restricted", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("returnCode", Schema.INT32_SCHEMA);
      schemaBuilder.field("returnCodeDetails", Schema.OPTIONAL_STRING_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<PushNotificationManagerRequest> serde = new ConnectSerde<PushNotificationManagerRequest>(schema, false, PushNotificationManagerRequest.class, PushNotificationManagerRequest::pack, PushNotificationManagerRequest::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<PushNotificationManagerRequest> serde() { return serde; }
    public Schema subscriberStreamEventSchema() { return schema(); }

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String destination;
    private String language;
    private String templateID;
    private Map<String, List<String>> tags;
    private boolean confirmationExpected;
    private boolean restricted;
    private PushMessageStatus status;
    private int returnCode;
    private String returnCodeDetails;

    //
    //  accessors
    //

    public String getDestination() { return destination; }
    public String getLanguage() { return language; }
    public String getTemplateID() { return templateID; }
    public Map<String, List<String>> getTags() { return tags; }
    public boolean getConfirmationExpected() { return confirmationExpected; }
    public boolean getRestricted() { return restricted; }
    public PushMessageStatus getMessageStatus() { return status; }
    public int getReturnCode() { return returnCode; }
    public String getReturnCodeDetails() { return returnCodeDetails; }

    //
    //  abstract
    //

    @Override public Integer getActivityType() { return ActivityType.Messages.getExternalRepresentation(); }

    //
    //  setters
    //

    public void setConfirmationExpected(boolean confirmationExpected) { this.confirmationExpected = confirmationExpected; }
    public void setRestricted(boolean restricted) { this.restricted = restricted; }
    public void setMessageStatus(PushMessageStatus status) { this.status = status; }
    public void setReturnCode(Integer returnCode) { this.returnCode = returnCode; }
    public void setReturnCodeDetails(String returnCodeDetails) { this.returnCodeDetails = returnCodeDetails; }
    
    /*****************************************
    *
    *  getMessage
    *
    *****************************************/

    public String getMessage(String messageField, SubscriberMessageTemplateService subscriberMessageTemplateService)
    {
      PushTemplate pushTemplate = (PushTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(templateID, SystemTime.getCurrentTime());
      DialogMessage dialogMessage = (pushTemplate != null) ? pushTemplate.getDialogMessage(messageField) : null;
      String text = (dialogMessage != null) ? dialogMessage.resolve(language, tags.get(messageField)) : null;
      return text;
    }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public PushNotificationManagerRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource, String destination, String language, String templateID, Map<String, List<String>> tags)
    {
      super(context, deliveryType, deliveryRequestSource);
      this.destination = destination;
      this.language = language;
      this.templateID = templateID;
      this.tags = tags;
      this.status = PushMessageStatus.PENDING;
      this.returnCode = status.getReturnCode();
      this.returnCodeDetails = null;
    }

    /*****************************************
    *
    *  constructor -- external
    *
    *****************************************/
    
    public PushNotificationManagerRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
    {
      super(jsonRoot);
      this.destination = JSONUtilities.decodeString(jsonRoot, "destination", true);
      this.language = JSONUtilities.decodeString(jsonRoot, "language", true);
      this.templateID = JSONUtilities.decodeString(jsonRoot, "templateID", true);
      this.tags = decodeTags(JSONUtilities.decodeJSONArray(jsonRoot, "tags", new JSONArray()));
      this.status = PushMessageStatus.PENDING;
      this.returnCode = PushMessageStatus.PENDING.getReturnCode();
      this.returnCodeDetails = null;
    }

    /*****************************************
    *
    *  decodeMessageTags
    *
    *****************************************/

    private Map<String, List<String>> decodeTags(JSONArray jsonArray) //TODO SCH : A TESTER !!! !!! !!! !!! !!! !!! !!! !!! !!! 
    {
      Map<String, List<String>> tags = new HashMap<String, List<String>>();
      if (jsonArray != null)
        {
          for (int i=0; i<jsonArray.size(); i++)
            {
              JSONObject messageTagJSON = (JSONObject) jsonArray.get(i);
              String messageField = JSONUtilities.decodeString(messageTagJSON, "messageField", true);
              List<String> messageTags = (List<String>) JSONUtilities.decodeJSONObject(messageTagJSON, "messageTags");
              tags.put(messageField, messageTags);
            }
        }
      return tags;
    
      
    }

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private PushNotificationManagerRequest(SchemaAndValue schemaAndValue, String destination, String language, String templateID, Map<String, List<String>> tags, boolean confirmationExpected, boolean restricted, PushMessageStatus status, String returnCodeDetails)
    {
      super(schemaAndValue);
      this.destination = destination;
      this.language = language;
      this.templateID = templateID;
      this.tags = tags;
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

    private PushNotificationManagerRequest(PushNotificationManagerRequest pushNotificationManagerRequest)
    {
      super(pushNotificationManagerRequest);
      this.destination = pushNotificationManagerRequest.getDestination();
      this.language = pushNotificationManagerRequest.getLanguage();
      this.templateID = pushNotificationManagerRequest.getTemplateID();
      this.tags = pushNotificationManagerRequest.getTags();
      this.confirmationExpected = pushNotificationManagerRequest.getConfirmationExpected();
      this.restricted = pushNotificationManagerRequest.getRestricted();
      this.status = pushNotificationManagerRequest.getMessageStatus();
      this.returnCode = pushNotificationManagerRequest.getReturnCode();
      this.returnCodeDetails = pushNotificationManagerRequest.getReturnCodeDetails();
    }

    /*****************************************
    *
    *  copy
    *
    *****************************************/

    public PushNotificationManagerRequest copy()
    {
      return new PushNotificationManagerRequest(this);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      PushNotificationManagerRequest notificationRequest = (PushNotificationManagerRequest) value;
      Struct struct = new Struct(schema);
      packCommon(struct, notificationRequest);
      struct.put("destination", notificationRequest.getDestination());
      struct.put("language", notificationRequest.getLanguage());
      struct.put("templateID", notificationRequest.getTemplateID());
      struct.put("tags", notificationRequest.getTags()); 
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

    public static PushNotificationManagerRequest unpack(SchemaAndValue schemaAndValue)
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
      String language = valueStruct.getString("language");
      String templateID = valueStruct.getString("templateID");
      Map<String, List<String>> tags = (Map<String, List<String>>) valueStruct.get("tags");
      boolean confirmationExpected = valueStruct.getBoolean("confirmationExpected");
      boolean restricted = valueStruct.getBoolean("restricted");
      Integer returnCode = valueStruct.getInt32("returnCode");
      String returnCodeDetails = valueStruct.getString("returnCodeDetails");
      PushMessageStatus status = PushMessageStatus.fromReturnCode(returnCode);
      
      //
      //  return
      //

      return new PushNotificationManagerRequest(schemaAndValue, destination, language, templateID, tags, confirmationExpected, restricted, status, returnCodeDetails);
    }
    
//    /*****************************************
//    *
//    *  unpackTags
//    *
//    *****************************************/
//
//    private static Map<String, List<String>> unpackTags(Schema schema, Object value)
//    {
//      //
//      //  get schema for JourneyNode
//      //
//
//      Schema journeyNodeSchema = schema.valueSchema();
//      
//      //
//      //  unpack
//      //
//
//      Map<String, List<String>> tagsStruct = (Map<String, List<String>>) value;
//      Map<String,List<String>> result = new LinkedHashMap<String,List<String>>();
//      for (String parameterName : tagsStruct.keySet())
//        {
//          List<String> values = tagsStruct.get(parameterName);
//          result.put(parameterName, values);
//        }
//
//      //
//      //  return
//      //
//
//      return result;
//    }
    
    /****************************************
    *
    *  presentation utilities
    *
    ****************************************/
    
    //
    //  addFieldsForGUIPresentation
    //

    @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
    {
      Module module = Module.fromExternalRepresentation(getModuleID());
      guiPresentationMap.put(CUSTOMERID, getSubscriberID());
      guiPresentationMap.put(EVENTID, null);
      guiPresentationMap.put(MODULEID, getModuleID());
      guiPresentationMap.put(MODULENAME, module.toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService));
      guiPresentationMap.put(RETURNCODE, getReturnCode());
      guiPresentationMap.put(RETURNCODEDETAILS, PushMessageStatus.fromReturnCode(getReturnCode()).toString());
//      guiPresentationMap.put(NOTIFICATION_SUBJECT, getSubject(subscriberMessageTemplateService));
//      guiPresentationMap.put(NOTIFICATION_TEXT_BODY, getTextBody(subscriberMessageTemplateService));
//      guiPresentationMap.put(NOTIFICATION_HTML_BODY, getHtmlBody(subscriberMessageTemplateService));
      guiPresentationMap.put(NOTIFICATION_CHANNEL, "PUSH");  // TODO SCH : should this be more specific (communication channel name ?) ?
      guiPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
    }
    
    //
    //  addFieldsForThirdPartyPresentation
    //

    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
    {
      Module module = Module.fromExternalRepresentation(getModuleID());
      thirdPartyPresentationMap.put(CUSTOMERID, getSubscriberID());
      thirdPartyPresentationMap.put(EVENTID, null);
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, module.toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService));
      thirdPartyPresentationMap.put(RETURNCODE, getReturnCode());
      thirdPartyPresentationMap.put(RETURNCODEDETAILS, PushMessageStatus.fromReturnCode(getReturnCode()).toString());
      thirdPartyPresentationMap.put(NOTIFICATION_CHANNEL, "PUSH");  // TODO SCH : should this be more specific (communication channel name ?) ?
      thirdPartyPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
    }
    
    /****************************************
    *
    *  getEffectiveDeliveryTime
    *
    ****************************************/

    public Date getEffectiveDeliveryTime(PushNotificationManager pushNotificationManager, String communicationChannelID, Date now)
    {
      //
      //  retrieve delivery time configuration
      //

      CommunicationChannel channel = (CommunicationChannel) pushNotificationManager.getCommunicationChannelService().getActiveCommunicationChannel(communicationChannelID, now);
      CommunicationChannelBlackoutPeriod blackoutPeriod = pushNotificationManager.getBlackoutService().getActiveCommunicationChannelBlackout("blackoutPeriod", now);

      //
      //  iterate until a valid date is found (give up after 7 days and reschedule even if not legal)
      //

      Date maximumDeliveryDate = RLMDateUtils.addDays(now, 7, Deployment.getBaseTimeZone());
      Date deliveryDate = now;
      while (deliveryDate.before(maximumDeliveryDate))
        {
          Date nextDailyWindowDeliveryDate = (channel != null) ? pushNotificationManager.getCommunicationChannelService().getEffectiveDeliveryTime(channel.getGUIManagedObjectID(), deliveryDate) : deliveryDate;
          Date nextBlackoutWindowDeliveryDate = (blackoutPeriod != null) ? pushNotificationManager.getBlackoutService().getEffectiveDeliveryTime(blackoutPeriod.getGUIManagedObjectID(), deliveryDate) : deliveryDate;
          Date nextDeliveryDate = nextBlackoutWindowDeliveryDate.after(nextDailyWindowDeliveryDate) ? nextBlackoutWindowDeliveryDate : nextDailyWindowDeliveryDate;
          if (nextDeliveryDate.after(deliveryDate))
            deliveryDate = nextDeliveryDate;
          else
            break;
        }

      //
      //  resolve
      //

      return deliveryDate;
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
      this.deliveryType = JSONUtilities.decodeString(configuration, "deliveryType", true);
      this.moduleID = JSONUtilities.decodeString(configuration, "moduleID", true);
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
      *  now
      *
      *****************************************/

      Date now = SystemTime.getCurrentTime();
      
      /*****************************************
      *
      *  parameters
      *
      *****************************************/

      String pushTemplateID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.message");

      /*****************************************
      *
      *  get pushTemplate
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      String language = subscriberEvaluationRequest.getLanguage();
      SubscriberMessageTemplateService subscriberMessageTemplateService = evolutionEventContext.getSubscriberMessageTemplateService();
      PushTemplate baseTemplate = (PushTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(pushTemplateID, now);
      PushTemplate template = (baseTemplate != null) ? ((PushTemplate) baseTemplate.getReadOnlyCopy(evolutionEventContext)) : null;

      String destAddress = null;

      //
      //  messages
      //

      Map<String, List<String>> tags = null;
      if (template != null)
        {
          //
          //  get communicationChannel
          //
          
          CommunicationChannelService communicationChannelService = evolutionEventContext.getCommunicationChannelService();
          CommunicationChannel communicationChannel = communicationChannelService.getActiveCommunicationChannel(template.getCommunicationChannelID(), now);
          
          //
          //  get dest address
          //
          
          CriterionField criterionField = Deployment.getProfileCriterionFields().get(communicationChannel.getProfileAddressField());
          destAddress = (String) criterionField.retrieveNormalized(subscriberEvaluationRequest);
          
          //
          //  get dialogMessageTags
          //
          
          tags = new HashMap<String, List<String>>();
          for(String messageField : template.getDialogMessageFields()){
            DialogMessage dialogMessage = template.getDialogMessage(messageField);
            List<String> dialogMessageTags = (dialogMessage != null) ? dialogMessage.resolveMessageTags(subscriberEvaluationRequest, language) : new ArrayList<String>();
            tags.put(messageField, dialogMessageTags);
          }
        }
      else
        {
          log.info("PushNotificationManager unknown push template ");
        }

      /*****************************************
      *
      *  request
      *
      *****************************************/

      PushNotificationManagerRequest request = null;
      if (destAddress != null)
        {
          request = new PushNotificationManagerRequest(evolutionEventContext, deliveryType, deliveryRequestSource, destAddress, language, template.getPushTemplateID(), tags);
          request.setModuleID(moduleID);
          request.setFeatureID(deliveryRequestSource);
        }
      else
        {
          log.info("PushNotificationManager unknown destination address for subscriberID " + subscriberEvaluationRequest.getSubscriberProfile().getSubscriberID());
        }

      /*****************************************
      *
      *  return
      *
      *****************************************/

      return Collections.<Action>singletonList(request);
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

        log.info("PushNotificationManagerRequest run deliveryRequest" + deliveryRequest);

        PushNotificationManagerRequest pushRequest = (PushNotificationManagerRequest)deliveryRequest;
        PushTemplate pushTemplate = (PushTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(pushRequest.getTemplateID(), now);
        
        if (pushTemplate != null) 
          {
            Date effectiveDeliveryTime = pushRequest.getEffectiveDeliveryTime(this, pushTemplate.getCommunicationChannelID(), now); 
            if(effectiveDeliveryTime.equals(now))
              {
                log.info("PushNotificationManagerRequest run deliveryRequest : sending notification now");
                pushNotification.send(pushRequest);
              }
            else
              {
                log.info("PushNotificationManagerRequest run deliveryRequest : notification rescheduled ("+effectiveDeliveryTime+")");
                pushRequest.setRescheduledDate(effectiveDeliveryTime);
                pushRequest.setDeliveryStatus(DeliveryStatus.Reschedule);
                pushRequest.setReturnCode(PushMessageStatus.RESCHEDULE.getReturnCode());
                pushRequest.setMessageStatus(PushMessageStatus.RESCHEDULE);
                completeDeliveryRequest(pushRequest);
              }
          }
        else
          {
            log.info("PushNotificationManagerRequest run deliveryRequest : ERROR : template not found");
            pushRequest.setDeliveryStatus(DeliveryStatus.Failed);
            pushRequest.setReturnCode(PushMessageStatus.UNKNOWN.getReturnCode());
            pushRequest.setMessageStatus(PushMessageStatus.UNKNOWN);
            completeDeliveryRequest(pushRequest);
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
    log.info("PushNotificationManager.updateDeliveryRequest(deliveryRequest="+deliveryRequest+")");
    updateRequest(deliveryRequest);
  }

  /*****************************************
  *
  *  completeDeliveryRequest
  *
  *****************************************/

  public void completeDeliveryRequest(DeliveryRequest deliveryRequest)
  {
    log.info("PushNotificationManager.updateDeliveryRequest(deliveryRequest="+deliveryRequest+")");
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
    log.info("PushNotificationManager.submitCorrelatorUpdateDeliveryRequest(correlator="+correlator+", correlatorUpdate="+correlatorUpdate.toJSONString()+")");
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
    PushNotificationManagerRequest pushRequest = (PushNotificationManagerRequest) deliveryRequest;
    if (pushRequest != null)
      {
        pushRequest.setMessageStatus(PushMessageStatus.fromReturnCode(result));
        pushRequest.setDeliveryStatus(getMessageStatus(pushRequest.getMessageStatus()));
        pushRequest.setDeliveryDate(SystemTime.getCurrentTime());
        completeRequest(pushRequest);
      }
  }

  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  @Override protected void shutdown()
  {
    log.info("PushNotificationManager:  shutdown");
  }

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    log.info("PushNotificationManager: recieved " + args.length + " args");
    for(String arg : args)
      {
        log.info("PushNotificationManager: arg " + arg);
      }

    //
    //  configuration
    //

    String deliveryManagerKey = args[0];
    String pluginName = args[1];

    //
    //  instance  
    //

    log.info("PushNotificationManager: configuration " + Deployment.getDeliveryManagers());

    PushNotificationManager manager = new PushNotificationManager(deliveryManagerKey, pluginName);

    //
    //  run
    //

    manager.run();
    
  }
}
