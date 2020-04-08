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
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.NodeType.OutputType;
import com.evolving.nglm.evolution.toolbox.ToolBoxBuilder;

public class NotificationManager extends DeliveryManager implements Runnable
{
  /*****************************************
  *
  *  enum - status
  *
  *****************************************/

  public enum MessageStatus
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
    private MessageStatus(Integer returncode) { this.returncode = returncode; }
    public Integer getReturnCode() { return returncode; }
    public static MessageStatus fromReturnCode(Integer externalRepresentation) { for (MessageStatus enumeratedValue : MessageStatus.values()) { if (enumeratedValue.getReturnCode().equals(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
  }

  /*****************************************
  *
  *  conversion method
  *
  *****************************************/

  public DeliveryStatus getMessageStatus (MessageStatus status)
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
  private NotificationInterface notification;
  private NotificationStatistics stats = null;
  private static String applicationID = "deliverymanager-notificationmanager";
  public String pluginName;
  private SubscriberMessageTemplateService subscriberMessageTemplateService;
    private CommunicationChannelBlackoutService blackoutService;
  private ContactPolicyProcessor contactPolicyProcessor;

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(NotificationManager.class);

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

  public NotificationManager(String deliveryManagerKey, String pluginName)
  {
    //
    //  superclass
    //

    super(applicationID, deliveryManagerKey, Deployment.getBrokerServers(), NotificationManagerRequest.serde, Deployment.getDeliveryManagers().get(pluginName));

    //
    //  service
    //

    subscriberMessageTemplateService = new SubscriberMessageTemplateService(Deployment.getBrokerServers(), "notificationmanager-subscribermessagetemplateservice-" + deliveryManagerKey, Deployment.getSubscriberMessageTemplateTopic(), false);
    subscriberMessageTemplateService.start();

    //
    //  blackoutService
    //
        
    blackoutService = new CommunicationChannelBlackoutService(Deployment.getBrokerServers(), "notificationmanager-communicationchannelblackoutservice-" + deliveryManagerKey, Deployment.getCommunicationChannelBlackoutTopic(), false);
    blackoutService.start();

    //
    //  contact policy processor
    //
    contactPolicyProcessor = new ContactPolicyProcessor("notificationmanager-communicationchannel",deliveryManagerKey);

    //
    //  manager
    //

    
    //
    // TODO get the channel configuration and instanciate all plugins to be called by the working thread on incoming request
    //

    
//    this.pluginName = pluginName;
//    
//    String pluginClassName = JSONUtilities.decodeString(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "notificationPluginClass", true);
//    log.info("NotificationManager: plugin instanciation : pluginClassName = "+pluginClassName);
//
//    JSONObject pluginConfiguration = JSONUtilities.decodeJSONObject(Deployment.getDeliveryManagers().get(pluginName).getJSONRepresentation(), "notificationPluginConfiguration", true);
//    log.info("NotificationManager: plugin instanciation : pluginConfiguration = "+pluginConfiguration);
//
//    try
//      {
//        notification = (NotificationInterface) (Class.forName(pluginClassName).newInstance());
//        notification.init(this, pluginConfiguration, pluginName);
//      }
//    catch (InstantiationException | IllegalAccessException | IllegalArgumentException e)
//      {
//        log.error("NotificationManager: could not create new instance of class " + pluginClassName, e);
//        throw new RuntimeException("NotificationManager: could not create new instance of class " + pluginClassName, e);
//      }
//    catch (ClassNotFoundException e)
//      {
//        log.error("NotificationManager: could not find class " + pluginClassName, e);
//        throw new RuntimeException("NotificationManager: could not find class " + pluginClassName, e);
//      }

    //
    // statistics
    //
    
    try{
      stats = new NotificationStatistics(applicationID, pluginName);
    }catch(Exception e){
      log.error("NotificationManager: could not load statistics ", e);
      throw new RuntimeException("NotificationManager: could not load statistics  ", e);
    }
    
    //
    //  threads
    //

    for(int i = 0; i < threadNumber; i++)
      {
        threads.add(new Thread(this, "NotificationManagerThread_"+i));
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

  public static class NotificationManagerRequest extends DeliveryRequest implements MessageDelivery
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
      schemaBuilder.name("service_notification_request");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("destination", Schema.STRING_SCHEMA);
      schemaBuilder.field("language", Schema.STRING_SCHEMA);
      schemaBuilder.field("templateID", Schema.STRING_SCHEMA);
      schemaBuilder.field("tags", SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).name("notification_tags").schema());
      schemaBuilder.field("confirmationExpected", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("restricted", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("returnCode", Schema.INT32_SCHEMA);
      schemaBuilder.field("returnCodeDetails", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("channelID", Schema.STRING_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<NotificationManagerRequest> serde = new ConnectSerde<NotificationManagerRequest>(schema, false, NotificationManagerRequest.class, NotificationManagerRequest::pack, NotificationManagerRequest::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<NotificationManagerRequest> serde() { return serde; }
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
    private String channelID;

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
    public String getChannelID() { return channelID; }

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
    public void setChannelID(String channelID) { this.channelID = channelID; }
    
    //
    //  message delivery accessors
    //

    public int getMessageDeliveryReturnCode() { return getReturnCode(); }
    public String getMessageDeliveryReturnCodeDetails() { return getReturnCodeDetails(); }
    public String getMessageDeliveryOrigin() { return ""; }
    public String getMessageDeliveryMessageId() { return getEventID(); }

    /*****************************************
    *
    *  getMessage
    *
    *****************************************/

    public String getMessage(String messageField, SubscriberMessageTemplateService subscriberMessageTemplateService)
    {
      DialogTemplate dialogTemplate = (DialogTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(templateID, SystemTime.getCurrentTime());
      DialogMessage dialogMessage = (dialogTemplate != null) ? dialogTemplate.getDialogMessage(messageField) : null;
      String text = (dialogMessage != null) ? dialogMessage.resolve(language, tags.get(messageField)) : null;
      return text;
    }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public NotificationManagerRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource, String destination, String language, String templateID, Map<String, List<String>> tags, String channelID)
    {
      super(context, deliveryType, deliveryRequestSource);
      this.destination = destination;
      this.language = language;
      this.templateID = templateID;
      this.tags = tags;
      this.status = MessageStatus.PENDING;
      this.returnCode = status.getReturnCode();
      this.returnCodeDetails = null;
      this.channelID = channelID;
    }

//    /*****************************************
//    *
//    *  constructor -- external
//    *
//    *****************************************/
//    
//    public NotificationManagerRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
//    {
//      super(jsonRoot);
//      this.destination = JSONUtilities.decodeString(jsonRoot, "destination", true);
//      this.language = JSONUtilities.decodeString(jsonRoot, "language", true);
//      this.templateID = JSONUtilities.decodeString(jsonRoot, "templateID", true);
//      this.tags = decodeTags(JSONUtilities.decodeJSONArray(jsonRoot, "tags", new JSONArray()));
//      this.status = MessageStatus.PENDING;
//      this.returnCode = MessageStatus.PENDING.getReturnCode();
//      this.returnCodeDetails = null;
//      this.channelID = JSONUtilities.decodeString(jsonRoot, "channelID", true);
//    }

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

    private NotificationManagerRequest(SchemaAndValue schemaAndValue, String destination, String language, String templateID, Map<String, List<String>> tags, boolean confirmationExpected, boolean restricted, MessageStatus status, String returnCodeDetails, String channelID)
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
      this.channelID = channelID;
    }

    /*****************************************
    *
    *  constructor -- copy
    *
    *****************************************/

    private NotificationManagerRequest(NotificationManagerRequest NotificationManagerRequest)
    {
      super(NotificationManagerRequest);
      this.destination = NotificationManagerRequest.getDestination();
      this.language = NotificationManagerRequest.getLanguage();
      this.templateID = NotificationManagerRequest.getTemplateID();
      this.tags = NotificationManagerRequest.getTags();
      this.confirmationExpected = NotificationManagerRequest.getConfirmationExpected();
      this.restricted = NotificationManagerRequest.getRestricted();
      this.status = NotificationManagerRequest.getMessageStatus();
      this.returnCode = NotificationManagerRequest.getReturnCode();
      this.returnCodeDetails = NotificationManagerRequest.getReturnCodeDetails();
      this.channelID = NotificationManagerRequest.getChannelID();
    }

    /*****************************************
    *
    *  copy
    *
    *****************************************/

    public NotificationManagerRequest copy()
    {
      return new NotificationManagerRequest(this);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      NotificationManagerRequest notificationRequest = (NotificationManagerRequest) value;
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
      struct.put("channelID", notificationRequest.getChannelID());
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

    public static NotificationManagerRequest unpack(SchemaAndValue schemaAndValue)
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
      String channelID = valueStruct.getString("channelID");
      MessageStatus status = MessageStatus.fromReturnCode(returnCode);
      
      //
      //  return
      //

      return new NotificationManagerRequest(schemaAndValue, destination, language, templateID, tags, confirmationExpected, restricted, status, returnCodeDetails, channelID);
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
      guiPresentationMap.put(RETURNCODE, getReturnCode());
      guiPresentationMap.put(RETURNCODEDETAILS, MessageStatus.fromReturnCode(getReturnCode()).toString());
      guiPresentationMap.put(NOTIFICATION_CHANNEL, getChannelID());
      guiPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
    }
    
    //
    //  addFieldsForThirdPartyPresentation
    //

    @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
    {
      Module module = Module.fromExternalRepresentation(getModuleID());
      thirdPartyPresentationMap.put(CUSTOMERID, getSubscriberID());
      thirdPartyPresentationMap.put(EVENTID, null);
      thirdPartyPresentationMap.put(MODULEID, getModuleID());
      thirdPartyPresentationMap.put(MODULENAME, module.toString());
      thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
      thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
      thirdPartyPresentationMap.put(RETURNCODE, getReturnCode());
      thirdPartyPresentationMap.put(RETURNCODEDETAILS, MessageStatus.fromReturnCode(getReturnCode()).toString());
      thirdPartyPresentationMap.put(NOTIFICATION_CHANNEL, getChannelID());
      thirdPartyPresentationMap.put(NOTIFICATION_RECIPIENT, getDestination());
    }
	
	public void resetDeliveryRequestAfterReSchedule()
    {
      this.setReturnCode(MessageStatus.PENDING.getReturnCode());
      this.setMessageStatus(MessageStatus.PENDING);
      
    }   

    
    @Override
    public String toString()
    {
      return "NotificationManagerRequest [destination=" + destination + ", language=" + language + ", templateID=" + templateID + ", tags=" + tags + ", confirmationExpected=" + confirmationExpected + ", restricted=" + restricted + ", status=" + status + ", returnCode=" + returnCode + ", returnCodeDetails=" + returnCodeDetails + ", channelID=" + channelID + "]";
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
    private String channelID;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ActionManager(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
      this.deliveryType = "notificationmanager";
      this.moduleID = JSONUtilities.decodeString(configuration, "moduleID", true);
      this.channelID = JSONUtilities.decodeString(configuration, "channelID", true);
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

      String templateID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.dialog_template");

      /*****************************************
      *
      *  get DialogTemplate
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      String language = subscriberEvaluationRequest.getLanguage();
      SubscriberMessageTemplateService subscriberMessageTemplateService = evolutionEventContext.getSubscriberMessageTemplateService();
      DialogTemplate baseTemplate = (DialogTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(templateID, now);
      DialogTemplate template = (baseTemplate != null) ? ((DialogTemplate) baseTemplate.getReadOnlyCopy(evolutionEventContext)) : null;

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
          log.info("NotificationManager unknown dialog template ");
        }

      /*****************************************
      *
      *  request
      *
      *****************************************/

      NotificationManagerRequest request = null;
      if (destAddress != null)
        {
          request = new NotificationManagerRequest(evolutionEventContext, deliveryType, deliveryRequestSource, destAddress, language, template.getDialogTemplateID(), tags, channelID);
          request.setModuleID(moduleID);
          request.setFeatureID(deliveryRequestSource);
          request.setNotificationHistory(evolutionEventContext.getSubscriberState().getNotificationHistory());
        }
      else
        {
          log.info("NotificationManager unknown destination address for subscriberID " + subscriberEvaluationRequest.getSubscriberProfile().getSubscriberID());
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

        log.info("NotificationManagerRequest run deliveryRequest" + deliveryRequest);

        NotificationManagerRequest dialogRequest = (NotificationManagerRequest)deliveryRequest;
//        DialogTemplate DialogTemplate = (DialogTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(dialogRequest.getTemplateID(), now);
//        
//        if (DialogTemplate != null) 
//          {
//            Date effectiveDeliveryTime = dialogRequest.getEffectiveDeliveryTime(this, DialogTemplate.getCommunicationChannelID(), now); 
//            if(effectiveDeliveryTime.equals(now))
//              {
//                log.info("NotificationManagerRequest run deliveryRequest : sending notification now");
//                notification.send(dialogRequest);
//              }
//            else
//              {
//                log.info("NotificationManagerRequest run deliveryRequest : notification rescheduled ("+effectiveDeliveryTime+")");
//                dialogRequest.setRescheduledDate(effectiveDeliveryTime);
//                dialogRequest.setDeliveryStatus(DeliveryStatus.Reschedule);
//                dialogRequest.setReturnCode(MessageStatus.RESCHEDULE.getReturnCode());
//                dialogRequest.setMessageStatus(MessageStatus.RESCHEDULE);
//                completeDeliveryRequest(dialogRequest);
//              }
//          }
//        else
//          {
//            log.info("NotificationManagerRequest run deliveryRequest : ERROR : template with id '"+dialogRequest.getTemplateID()+"' not found");
//            log.info("subscriberMessageTemplateService contains :");
//            for(GUIManagedObject obj : subscriberMessageTemplateService.getActiveSubscriberMessageTemplates(now)){
//              log.info("   - "+obj.getGUIManagedObjectName()+" (id "+obj.getGUIManagedObjectID()+") : "+obj.getClass().getName());
//            }
//            dialogRequest.setDeliveryStatus(DeliveryStatus.Failed);
//            dialogRequest.setReturnCode(MessageStatus.UNKNOWN.getReturnCode());
//            dialogRequest.setMessageStatus(MessageStatus.UNKNOWN);
//            completeDeliveryRequest(dialogRequest);
//          }
      }
  }

  /*****************************************
  *
  *  updateDeliveryRequest
  *
  *****************************************/

  public void updateDeliveryRequest(DeliveryRequest deliveryRequest)
  {
    log.info("NotificationManager.updateDeliveryRequest(deliveryRequest="+deliveryRequest+")");
    updateRequest(deliveryRequest);
  }

  /*****************************************
  *
  *  completeDeliveryRequest
  *
  *****************************************/

  public void completeDeliveryRequest(DeliveryRequest deliveryRequest)
  {
    log.info("NotificationManager.updateDeliveryRequest(deliveryRequest="+deliveryRequest+")");
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
    log.info("NotificationManager.submitCorrelatorUpdateDeliveryRequest(correlator="+correlator+", correlatorUpdate="+correlatorUpdate.toJSONString()+")");
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
    NotificationManagerRequest dialogRequest = (NotificationManagerRequest) deliveryRequest;
    if (dialogRequest != null)
      {
        dialogRequest.setMessageStatus(MessageStatus.fromReturnCode(result));
        dialogRequest.setDeliveryStatus(getMessageStatus(dialogRequest.getMessageStatus()));
        dialogRequest.setDeliveryDate(SystemTime.getCurrentTime());
        completeRequest(dialogRequest);
      }
  }

  /*****************************************
   *
   *  filterRequest
   *  verify contact policy rules
   *
   *****************************************/

  @Override public boolean filterRequest(DeliveryRequest request)
  {
    return false; //contactPolicyProcessor.ensureContactPolicy(request,this,log);
  }

  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  @Override protected void shutdown()
  {
    log.info("NotificationManager:  shutdown");
  }

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    log.info("NotificationManager: recieved " + args.length + " args");
    for(String arg : args)
      {
        log.info("NotificationManager: arg " + arg);
      }

    //
    //  configuration
    //

    String deliveryManagerKey = args[0];
    String pluginName = args[1];

    //
    //  instance  
    //

    log.info("NotificationManager: configuration " + Deployment.getDeliveryManagers());

    NotificationManager manager = new NotificationManager(deliveryManagerKey, pluginName);

    //
    //  run
    //

    manager.run();
    
  }
  public static String[] getNotificationNodeTypes()
  {
    
//    {
//      "id"                     : "143",
//      "name"                   : "appPush",
//      "display"                : "App Push",
//      "icon"                   : "jmr_components/styles/images/objects/app-push.png",
//      "height"                 : 70,
//      "width"                  : 70,
//      "outputType"             : "static",
//      "outputConnectors"       : 
//        [ 
//          { "name" : "delivered", "display" : "Delivered/Sent","transitionCriteria" : [ { "criterionField" : "node.action.deliverystatus", "criterionOperator" : "is in set", "argument" : { "expression" : "[ 'delivered', 'acknowledged' ]" } } ] },
//          { "name" : "failed",    "display" : "Failed",        "transitionCriteria" : [ { "criterionField" : "node.action.deliverystatus", "criterionOperator" : "is in set", "argument" : { "expression" : "[ 'failed', 'indeterminate', 'failedTimeout' ]" } } ] },
//          { "name" : "timeout",   "display" : "Timeout",       "transitionCriteria" : [ { "criterionField" : "evaluation.date", "criterionOperator" : ">=", "argument" : { "timeUnit" : "instant", "expression" : "dateAdd(node.entryDate, 1, 'minute')" } } ] },
//          { "name" : "unknown",   "display" : "UnknownAppID",  "transitionCriteria" : [ { "criterionField" : "subscriber.appID", "criterionOperator" : "is null" } ] }
//        ],
//      "parameters" :
//        [
//          { 
//            "id" : "node.parameter.dialog_template",
//            "display" : "Message Template",
//            "dataType" : "string",
//            "multiple" : false,
//            "mandatory" : true,
//            "availableValues" : [ "#dialog_template_3#" ],
//            "defaultValue" : null
//          },
//          { 
//            "id" : "node.parameter.contacttype",
//            "display" : "Contact Type",
//            "dataType" : "string",
//            "multiple" : false,
//            "mandatory" : true,
//            "availableValues" : 
//              [ 
//                { "id" : "callToAction",  "display" : "Call To Action" },
//                { "id" : "response", "display" : "Response" },
//                { "id" : "reminder", "display" : "Reminder" },
//                { "id" : "announcement", "display" : "Announcement" },
//                { "id" : "actionNotification", "display" : "Action Notification" }
//              ],
//            "defaultValue" : null
//          },
//
//          { 
//            "id" : "node.parameter.fromaddress",
//            "display" : "From Address",
//            "dataType" : "string",
//            "multiple" : false,
//            "mandatory" : true,
//            "availableValues" : [ "#dialog_source_address_3#" ],
//            "defaultValue" : null
//          }
//        ],
//      "action" : 
//        {
//          "actionManagerClass" : "com.evolving.nglm.evolution.NotificationManager$ActionManager",
//          "channelID" : "3",
//          "moduleID" : "1"
//        }
//     },

    for(CommunicationChannel current : Deployment.getCommunicationChannels().values()) {
      ToolBoxBuilder tb = new ToolBoxBuilder(
          "NotifChannel-" + current.getID(), current.getName(), current.getDisplay(), current.getIcon(), current.getToolboxHeight(), current.getToolboxWidth(), OutputType.Static);
    }
    
    return null;
  }
}
  
