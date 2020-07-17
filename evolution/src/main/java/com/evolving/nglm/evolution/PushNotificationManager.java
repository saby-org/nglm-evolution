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
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class PushNotificationManager extends DeliveryManagerForNotifications implements Runnable
{
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

  public static class PushNotificationManagerRequest extends DeliveryRequest implements MessageDelivery, INotificationRequest
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
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),8));
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
    private MessageStatus status;
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
    public MessageStatus getMessageStatus() { return status; }
    public int getReturnCode() { return returnCode; }
    public String getReturnCodeDetails() { return returnCodeDetails; }

    
    /*****************************************
    *
    *  getResolvedParameters
    *
    *****************************************/

    public Map<String, String> getResolvedParameters(SubscriberMessageTemplateService subscriberMessageTemplateService)
    {
      Map<String, String> result = new HashMap<String, String>();
      PushTemplate template = (PushTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(templateID, SystemTime.getCurrentTime());
      if(template.getDialogMessages() != null)
        {
          for(Map.Entry<String, DialogMessage> dialogMessageEntry : template.getDialogMessages().entrySet())
            {
              DialogMessage dialogMessage = dialogMessageEntry.getValue();
              String parameterName = dialogMessageEntry.getKey();
              String resolved = dialogMessage.resolve(language, tags.get(parameterName));
              result.put(parameterName, resolved);
            }
        }
      return result;
    }
    //
    //  abstract
    //

    @Override public ActivityType getActivityType() { return ActivityType.Messages; }

    //
    //  setters
    //

    public void setConfirmationExpected(boolean confirmationExpected) { this.confirmationExpected = confirmationExpected; }
    public void setRestricted(boolean restricted) { this.restricted = restricted; }
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
      this.status = MessageStatus.PENDING;
      this.returnCode = status.getReturnCode();
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

    private PushNotificationManagerRequest(SchemaAndValue schemaAndValue, String destination, String language, String templateID, Map<String, List<String>> tags, boolean confirmationExpected, boolean restricted, MessageStatus status, String returnCodeDetails)
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
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion2(schema.version()) : null;

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
      MessageStatus status = MessageStatus.fromReturnCode(returnCode);
      
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

    @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService)
    {
      Module module = Module.fromExternalRepresentation(getModuleID());
      guiPresentationMap.put(CUSTOMERID, getSubscriberID());
      guiPresentationMap.put(EVENTID, null);
      guiPresentationMap.put(MODULEID, getModuleID());
      guiPresentationMap.put(MODULENAME, module.toString());
      guiPresentationMap.put(FEATUREID, getFeatureID());
      guiPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      guiPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      guiPresentationMap.put(RETURNCODE, getReturnCode());
      guiPresentationMap.put(RETURNCODEDETAILS, MessageStatus.fromReturnCode(getReturnCode()).toString());
      //todo check NOTIFICATION_CHANNEL is ID or display: getChannelID() or...
      PushTemplate template = (PushTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(templateID, SystemTime.getCurrentTime());
      guiPresentationMap.put(NOTIFICATION_CHANNEL, Deployment.getCommunicationChannels().get(template.getCommunicationChannelID()).getDisplay());
      guiPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
      Map<String, String> resolvedParameters = getResolvedParameters(subscriberMessageTemplateService);
      guiPresentationMap.putAll(resolvedParameters);
    }
    
    //
    //  addFieldsForThirdPartyPresentation
    //

    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService)
    {
      Module module = Module.fromExternalRepresentation(getModuleID());
      thirdPartyPresentationMap.put(DELIVERYSTATUS, getMessageStatus().toString()); // replace value set by the superclass 
      thirdPartyPresentationMap.put(CUSTOMERID, getSubscriberID());
      thirdPartyPresentationMap.put(EVENTID, null);
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, module.toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(RETURNCODE, getReturnCode());
      thirdPartyPresentationMap.put(RETURNCODEDESCRIPTION, RESTAPIGenericReturnCodes.fromGenericResponseCode(getReturnCode()).getGenericResponseMessage());
      thirdPartyPresentationMap.put(RETURNCODEDETAILS, getReturnCodeDetails());
      //todo check NOTIFICATION_CHANNEL is ID or display: getChannelID() or...
      PushTemplate template = (PushTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(templateID, SystemTime.getCurrentTime());
      thirdPartyPresentationMap.put(NOTIFICATION_CHANNEL, Deployment.getCommunicationChannels().get(template.getCommunicationChannelID()).getDisplay());
      thirdPartyPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
      Map<String, String> resolvedParameters = getResolvedParameters(subscriberMessageTemplateService);
      thirdPartyPresentationMap.putAll(resolvedParameters);
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
      deliveryRequestSource = extractWorkflowFeatureID(evolutionEventContext, subscriberEvaluationRequest, deliveryRequestSource);
      
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
          
          CommunicationChannel communicationChannel = Deployment.getCommunicationChannels().get(template.getCommunicationChannelID());
          
          //
          //  get dest address
          //
          
          CriterionField criterionField = Deployment.getProfileCriterionFields().get(communicationChannel.getProfileAddressField());
          destAddress = (String) criterionField.retrieveNormalized(subscriberEvaluationRequest);
          
          //
          //  get dialogMessageTags
          //
          
//          log.info(" ===================================");
//          log.info("destAddress = "+destAddress);

          tags = new HashMap<String, List<String>>();
          for(String messageField : template.getDialogMessageFields().keySet()){
            DialogMessage dialogMessage = template.getDialogMessage(messageField);
            List<String> dialogMessageTags = (dialogMessage != null) ? dialogMessage.resolveMessageTags(subscriberEvaluationRequest, language) : new ArrayList<String>();
            tags.put(messageField, dialogMessageTags);
            
            
//            log.info("  ------------------------");
//            log.info("template.getDialogMessageFields contains :");
//            for(String m : template.getDialogMessageFields()){log.info("     - "+m);}
//            log.info("template.getDialogMessageFields contains :");
//            for(DialogMessage dm : template.getDialogMessages()){
//              log.info("    => dialogMessage :");
//              for(String k : dm.getMessageTextByLanguage().keySet()){
//                log.info("     - "+k+" : "+dm.getMessageTextByLanguage().get(k));
//              }
//            }
//            log.info("handling messageField = "+messageField);
//            log.info("found dialogMessage = "+dialogMessage+" (SHOULD NOT BE NULL !!!)");
//            log.info("dialogMessageTags = "+dialogMessageTags+" ("+dialogMessageTags.size()+" elements)");

            
          }
//          log.info(" ===================================");
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
          request.setNotificationHistory(evolutionEventContext.getSubscriberState().getNotificationHistory());
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
            
            if(pushRequest.getRestricted()) 
              {

                Date effectiveDeliveryTime = now;
                //todo Not sure if this key (push) really exists
                CommunicationChannel channel = (CommunicationChannel) Deployment.getCommunicationChannels().get("push");
                if(channel != null) 
                  {
                    effectiveDeliveryTime = channel.getEffectiveDeliveryTime(blackoutService, now);
                  }

                if(effectiveDeliveryTime.equals(now) || effectiveDeliveryTime.before(now))
                  {
                    log.debug("PushNotificationManagerRequest SEND Immediately restricted " + pushRequest);
                    pushNotification.send(pushRequest);
                  }
                else
                  {
                    log.debug("PushNotificationManagerRequest RESCHEDULE to " + effectiveDeliveryTime + " restricted " + pushRequest);
                    pushRequest.setRescheduledDate(effectiveDeliveryTime);
                    pushRequest.setDeliveryStatus(DeliveryStatus.Reschedule);
                    pushRequest.setReturnCode(MessageStatus.RESCHEDULE.getReturnCode());
                    pushRequest.setMessageStatus(MessageStatus.RESCHEDULE);
                    completeDeliveryRequest((DeliveryRequest)pushRequest);
                  }      
              }
            else {
              log.debug("SMSNotificationManagerRequest SEND Immediately NON restricted " + pushRequest);
              pushNotification.send(pushRequest);
            }
          }
        else
          {
            log.info("PushNotificationManagerRequest run deliveryRequest : ERROR : template with id '"+pushRequest.getTemplateID()+"' not found");
            log.info("subscriberMessageTemplateService contains :");
            for(GUIManagedObject obj : subscriberMessageTemplateService.getActiveSubscriberMessageTemplates(now)){
              log.info("   - "+obj.getGUIManagedObjectName()+" (id "+obj.getGUIManagedObjectID()+") : "+obj.getClass().getName());
            }
            pushRequest.setDeliveryStatus(DeliveryStatus.Failed);
            pushRequest.setReturnCode(MessageStatus.UNKNOWN.getReturnCode());
            pushRequest.setMessageStatus(MessageStatus.UNKNOWN);
            completeDeliveryRequest((DeliveryRequest)pushRequest);
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
        pushRequest.setMessageStatus(MessageStatus.fromReturnCode(result));
        pushRequest.setDeliveryStatus(getDeliveryStatus(pushRequest.getMessageStatus()));
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
    new LoggerInitialization().initLogger();
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
