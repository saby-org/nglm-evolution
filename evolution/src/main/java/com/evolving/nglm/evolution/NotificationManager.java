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
import com.evolving.nglm.evolution.ContactPolicyCommunicationChannels.ContactType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionOperator;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.NodeType.OutputType;
import com.evolving.nglm.evolution.notification.NotificationTemplateParameters;
import com.evolving.nglm.evolution.toolbox.ActionBuilder;
import com.evolving.nglm.evolution.toolbox.ArgumentBuilder;
import com.evolving.nglm.evolution.toolbox.AvailableValueDynamicBuilder;
import com.evolving.nglm.evolution.toolbox.AvailableValueStaticStringBuilder;
import com.evolving.nglm.evolution.toolbox.OutputConnectorBuilder;
import com.evolving.nglm.evolution.toolbox.ParameterBuilder;
import com.evolving.nglm.evolution.toolbox.ToolBoxBuilder;
import com.evolving.nglm.evolution.toolbox.TransitionCriteriaBuilder;

public class NotificationManager extends DeliveryManager implements Runnable
{
  /*****************************************
   *
   * enum - status
   *
   *****************************************/

  public enum MessageStatus
  {
    PENDING(10), SENT(1), NO_CUSTOMER_LANGUAGE(701), NO_CUSTOMER_CHANNEL(702), DELIVERED(0), EXPIRED(707), ERROR(706), UNDELIVERABLE(703), INVALID(704), QUEUE_FULL(705), RESCHEDULE(709), UNKNOWN(999);

    private Integer returncode;

    private MessageStatus(Integer returncode)
      {
        this.returncode = returncode;
      }

    public Integer getReturnCode()
    {
      return returncode;
    }

    public static MessageStatus fromReturnCode(Integer externalRepresentation)
    {
      for (MessageStatus enumeratedValue : MessageStatus.values())
        {
          if (enumeratedValue.getReturnCode().equals(externalRepresentation)) return enumeratedValue;
        }
      return UNKNOWN;
    }
  }

  /*****************************************
   *
   * conversion method
   *
   *****************************************/

  public DeliveryStatus getMessageStatus(MessageStatus status)
  {
    switch (status)
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
   * configuration
   *
   *****************************************/

  private int threadNumber = 5; // TODO : make this configurable
  private ArrayList<Thread> threads = new ArrayList<Thread>();
  private String channelsString; // List of Channels as a key for stats
  private Map<String, NotificationInterface> pluginInstances = new HashMap();
  private NotificationStatistics stats = null;
  private static String applicationID = "deliverymanager-notificationmanager";
  private SubscriberMessageTemplateService subscriberMessageTemplateService;
  private CommunicationChannelBlackoutService blackoutService;
  private ContactPolicyProcessor contactPolicyProcessor;

  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(NotificationManager.class);

  /*****************************************
   *
   * accessors
   *
   *****************************************/

  public SubscriberMessageTemplateService getSubscriberMessageTemplateService()
  {
    return subscriberMessageTemplateService;
  }

  public CommunicationChannelBlackoutService getBlackoutService()
  {
    return blackoutService;
  }

  /*****************************************
   *
   * constructor
   *
   *****************************************/

  public NotificationManager(String deliveryManagerKey, String channelsString)
    {
      //
      // superclass
      //

      super(applicationID, deliveryManagerKey, Deployment.getBrokerServers(), NotificationManagerRequest.serde, Deployment.getDeliveryManagers().get("notificationManager"));

      //
      // service
      //

      subscriberMessageTemplateService = new SubscriberMessageTemplateService(Deployment.getBrokerServers(), "notificationmanager-subscribermessagetemplateservice-" + deliveryManagerKey, Deployment.getSubscriberMessageTemplateTopic(), false);
      subscriberMessageTemplateService.start();

      //
      // blackoutService
      //

      blackoutService = new CommunicationChannelBlackoutService(Deployment.getBrokerServers(), "notificationmanager-communicationchannelblackoutservice-" + deliveryManagerKey, Deployment.getCommunicationChannelBlackoutTopic(), false);
      blackoutService.start();

      //
      // contact policy processor
      //
      contactPolicyProcessor = new ContactPolicyProcessor("notificationmanager-communicationchannel", deliveryManagerKey);

      //
      // manager
      //

      this.channelsString = channelsString;
      ArrayList<String> channels = new ArrayList<>();
      if (channelsString != null)
        {
          for (String channel : channelsString.split("."))
            {
              channels.add(channel);
            }
        }

      for (String channelName : channels)
        {
          log.info("NotificationManager: Instanciate communication channel " + channelName);
          Map<String, CommunicationChannel> configuredChannels = Deployment.getCommunicationChannels();
          for (CommunicationChannel cc : configuredChannels.values())
            {
              if (cc.getName().equals(channelName))
                {
                  // this channel's plugin must be initialized
                  try
                    {
                      NotificationInterface pluginInstance = (NotificationInterface) (Class.forName(cc.getNotificationPluginClass()).newInstance());
                      pluginInstance.init(cc.getNotificationPluginConfiguration());
                      pluginInstances.put(cc.getID(), (NotificationInterface) (Class.forName(cc.getNotificationPluginClass()).newInstance()));
                    }
                  catch (InstantiationException | IllegalAccessException | IllegalArgumentException e)
                    {
                      log.error("NotificationManager: could not create new instance of class " + cc.getNotificationPluginClass(), e);
                      throw new RuntimeException("NotificationManager: could not create new instance of class " + cc.getNotificationPluginClass(), e);
                    }
                  catch (ClassNotFoundException e)
                    {
                      log.error("NotificationManager: could not find class " + cc.getNotificationPluginClass(), e);
                      throw new RuntimeException("NotificationManager: could not find class " + cc.getNotificationPluginClass(), e);
                    }
                }
            }
        }

      //
      // statistics
      //

      try
        {
          stats = new NotificationStatistics(applicationID, channelsString);
        }
      catch (Exception e)
        {
          log.error("NotificationManager: could not load statistics ", e);
          throw new RuntimeException("NotificationManager: could not load statistics  ", e);
        }

      //
      // threads
      //

      for (int i = 0; i < threadNumber; i++)
        {
          threads.add(new Thread(this, "NotificationManagerThread_" + i));
        }

      //
      // startDelivery
      //

      startDelivery();
    }

  /*****************************************
   *
   * class NotificationManagerRequest
   *
   *****************************************/

  public static class NotificationManagerRequest extends DeliveryRequest implements MessageDelivery
  {
    /*****************************************
     *
     * schema
     *
     *****************************************/

    //
    // schema
    //

    private static Schema schema = null;
    static
      {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        schemaBuilder.name("service_notification_request");
        schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(), 1));
        for (Field field : commonSchema().fields())
          schemaBuilder.field(field.name(), field.schema());
        schemaBuilder.field("destination", Schema.STRING_SCHEMA);
        schemaBuilder.field("language", Schema.STRING_SCHEMA);
        schemaBuilder.field("templateID", Schema.STRING_SCHEMA);
        schemaBuilder.field("tags", SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).name("notification_tags").schema());
        schemaBuilder.field("confirmationExpected", Schema.BOOLEAN_SCHEMA);
        schemaBuilder.field("restricted", Schema.BOOLEAN_SCHEMA);
        schemaBuilder.field("returnCode", Schema.INT32_SCHEMA);
        schemaBuilder.field("returnCodeDetails", Schema.OPTIONAL_STRING_SCHEMA);
        schemaBuilder.field("channelID", Schema.STRING_SCHEMA);
        schemaBuilder.field("notificationParameters", ParameterMap.schema());
        
        schema = schemaBuilder.build();
      };

    //
    // serde
    //

    private static ConnectSerde<NotificationManagerRequest> serde = new ConnectSerde<NotificationManagerRequest>(schema, false, NotificationManagerRequest.class, NotificationManagerRequest::pack, NotificationManagerRequest::unpack);

    //
    // accessor
    //

    public static Schema schema()
    {
      return schema;
    }

    public static ConnectSerde<NotificationManagerRequest> serde()
    {
      return serde;
    }

    public Schema subscriberStreamEventSchema()
    {
      return schema();
    }

    /*****************************************
     *
     * data
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
    private ParameterMap notificationParameters;

    //
    // accessors
    //

    public String getDestination()
    {
      return destination;
    }

    public String getLanguage()
    {
      return language;
    }

    public String getTemplateID()
    {
      return templateID;
    }

    public Map<String, List<String>> getTags()
    {
      return tags;
    }

    public boolean getConfirmationExpected()
    {
      return confirmationExpected;
    }

    public boolean getRestricted()
    {
      return restricted;
    }

    public MessageStatus getMessageStatus()
    {
      return status;
    }

    public int getReturnCode()
    {
      return returnCode;
    }

    public String getReturnCodeDetails()
    {
      return returnCodeDetails;
    }

    public String getChannelID()
    {
      return channelID;
    }
    
    public ParameterMap getNotificationParameters()
    {
      return notificationParameters;
    }

    //
    // abstract
    //

    @Override
    public ActivityType getActivityType()
    {
      return ActivityType.Messages;
    }

    //
    // setters
    //

    public void setConfirmationExpected(boolean confirmationExpected)
    {
      this.confirmationExpected = confirmationExpected;
    }

    public void setRestricted(boolean restricted)
    {
      this.restricted = restricted;
    }

    public void setMessageStatus(MessageStatus status)
    {
      this.status = status;
    }

    public void setReturnCode(Integer returnCode)
    {
      this.returnCode = returnCode;
    }

    public void setReturnCodeDetails(String returnCodeDetails)
    {
      this.returnCodeDetails = returnCodeDetails;
    }

    public void setChannelID(String channelID)
    {
      this.channelID = channelID;
    }

    //
    // message delivery accessors
    //

    public int getMessageDeliveryReturnCode()
    {
      return getReturnCode();
    }

    public String getMessageDeliveryReturnCodeDetails()
    {
      return getReturnCodeDetails();
    }

    public String getMessageDeliveryOrigin()
    {
      return "";
    }

    public String getMessageDeliveryMessageId()
    {
      return getEventID();
    }

    /*****************************************
     *
     * getMessage
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
     * constructor
     *
     *****************************************/

    public NotificationManagerRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource, String destination, String language, String templateID, Map<String, List<String>> tags, String channelID, ParameterMap notificationParameters)
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
        this.notificationParameters = notificationParameters;
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
     * decodeMessageTags
     *
     *****************************************/

    private Map<String, List<String>> decodeTags(JSONArray jsonArray) // TODO SCH : A TESTER !!! !!! !!! !!! !!! !!! !!! !!! !!!
    {
      Map<String, List<String>> tags = new HashMap<String, List<String>>();
      if (jsonArray != null)
        {
          for (int i = 0; i < jsonArray.size(); i++)
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
     * constructor -- unpack
     *
     *****************************************/

    private NotificationManagerRequest(SchemaAndValue schemaAndValue, String destination, String language, String templateID, Map<String, List<String>> tags, boolean confirmationExpected, boolean restricted, MessageStatus status, String returnCodeDetails, String channelID, ParameterMap notificationParameters)
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
        this.notificationParameters = notificationParameters;
      }

    /*****************************************
     *
     * constructor -- copy
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
        this.notificationParameters = NotificationManagerRequest.getNotificationParameters();
      }

    /*****************************************
     *
     * copy
     *
     *****************************************/

    public NotificationManagerRequest copy()
    {
      return new NotificationManagerRequest(this);
    }

    /*****************************************
     *
     * pack
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
      struct.put("notificationParameters", ParameterMap.pack(notificationRequest.getNotificationParameters()));
      return struct;
    }

    //
    // subscriberStreamEventPack
    //

    public Object subscriberStreamEventPack(Object value)
    {
      return pack(value);
    }

    /*****************************************
     *
     * unpack
     *
     *****************************************/

    public static NotificationManagerRequest unpack(SchemaAndValue schemaAndValue)
    {
      //
      // data
      //

      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

      //
      // unpack
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
      ParameterMap notificationParameters =  ParameterMap.unpack(new SchemaAndValue(schema.field("notificationParameters").schema(), valueStruct.get("notificationParameters")));
      MessageStatus status = MessageStatus.fromReturnCode(returnCode);

      //
      // return
      //

      return new NotificationManagerRequest(schemaAndValue, destination, language, templateID, tags, confirmationExpected, restricted, status, returnCodeDetails, channelID, notificationParameters);
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
     * presentation utilities
     *
     ****************************************/

    //
    // addFieldsForGUIPresentation
    //

    @Override
    public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
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
    // addFieldsForThirdPartyPresentation
    //

    @Override
    public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
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
   * class ActionManager
   *
   *****************************************/

  public static class ActionManager extends com.evolving.nglm.evolution.ActionManager
  {
    /*****************************************
     *
     * data
     *
     *****************************************/

    private String deliveryType;
    private String moduleID;
    private String channelID;

    /*****************************************
     *
     * constructor
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
     * execute
     *
     *****************************************/

    @Override
    public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {

      /*****************************************
       *
       * now
       *
       *****************************************/

      Date now = SystemTime.getCurrentTime();

      /*****************************************
       *
       * template parameters
       *
       *****************************************/

      NotificationTemplateParameters templateParameters = (NotificationTemplateParameters) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest, "node.parameter.dialog_template");
      // templateParameters contains only field specific to a template, by example sms.body, but no isFlashSMS
      
      
      /*****************************************
       *
       * get DialogTemplate
       *
       *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      String language = subscriberEvaluationRequest.getLanguage();
      SubscriberMessageTemplateService subscriberMessageTemplateService = evolutionEventContext.getSubscriberMessageTemplateService();
      DialogTemplate baseTemplate = (DialogTemplate) subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(templateParameters.getSubscriberMessageTemplateID(), now);
      DialogTemplate template = (baseTemplate != null) ? ((DialogTemplate) baseTemplate.getReadOnlyCopy(evolutionEventContext)) : null;

      String destAddress = null;

      //
      // messages
      //

      Map<String, List<String>> tags = null;
      if (template != null)
        {
          //
          // get communicationChannel
          //

          CommunicationChannel communicationChannel = Deployment.getCommunicationChannels().get(template.getCommunicationChannelID());

          //
          // get dest address
          //

          CriterionField criterionField = Deployment.getProfileCriterionFields().get(communicationChannel.getProfileAddressField());
          destAddress = (String) criterionField.retrieveNormalized(subscriberEvaluationRequest);

          //
          // get dialogMessageTags
          //

//          log.info(" ===================================");
//          log.info("destAddress = "+destAddress);

          tags = new HashMap<String, List<String>>();
          for (String messageField : template.getDialogMessageFields().keySet())
            {
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

          //
          // Parameters specific to the channel but NOT related to template
          //
          
          ParameterMap notificationParameters = new ParameterMap();
          for(CriterionField field : communicationChannel.getParameters().values()) {
            if(!field.getFieldDataType().getExternalRepresentation().startsWith("template_")) {
              Object value = CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,field.getID());
              notificationParameters.put(field.getID(), value);
            }
          }
          
          // add also the mandatory parameters for all channels
          Object value = CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.contacttype");
          notificationParameters.put("node.parameter.contacttype", value);
          value = CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.fromaddress");
          notificationParameters.put("node.parameter.fromaddress", value);
          
          
          /*****************************************
          *
          * request
          *
          *****************************************/

         NotificationManagerRequest request = null;
         if (destAddress != null)
           {
             request = new NotificationManagerRequest(evolutionEventContext, deliveryType, deliveryRequestSource, destAddress, language, template.getDialogTemplateID(), tags, channelID, notificationParameters);
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
          * return
          *
          *****************************************/

         return Collections.<Action>singletonList(request);
        }
      
      else
        {
          log.info("NotificationManager unknown dialog template ");
          throw new RuntimeException("NotificationManager unknown dialog template for Journey " 
              + subscriberEvaluationRequest.getJourneyState().getJourneyID() 
              + " node " + subscriberEvaluationRequest.getJourneyNode().getNodeID() + "/" + subscriberEvaluationRequest.getJourneyNode().getNodeName());
        }
      }
  }

  /*****************************************
   *
   * run
   *
   *****************************************/

  public void run()
  {
    while (isProcessing())
      {
        /*****************************************
         *
         * nextRequest
         *
         *****************************************/

        DeliveryRequest deliveryRequest = nextRequest();
        Date now = SystemTime.getCurrentTime();

        log.info("NotificationManagerRequest run deliveryRequest" + deliveryRequest);

        NotificationManagerRequest dialogRequest = (NotificationManagerRequest) deliveryRequest;
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
   * updateDeliveryRequest
   *
   *****************************************/

  public void updateDeliveryRequest(DeliveryRequest deliveryRequest)
  {
    log.info("NotificationManager.updateDeliveryRequest(deliveryRequest=" + deliveryRequest + ")");
    updateRequest(deliveryRequest);
  }

  /*****************************************
   *
   * completeDeliveryRequest
   *
   *****************************************/

  public void completeDeliveryRequest(DeliveryRequest deliveryRequest)
  {
    log.info("NotificationManager.updateDeliveryRequest(deliveryRequest=" + deliveryRequest + ")");
    completeRequest(deliveryRequest);
    stats.updateMessageCount(channelsString, 1, deliveryRequest.getDeliveryStatus());
  }

  /*****************************************
   *
   * submitCorrelatorUpdateDeliveryRequest
   *
   *****************************************/

  public void submitCorrelatorUpdateDeliveryRequest(String correlator, JSONObject correlatorUpdate)
  {
    log.info("NotificationManager.submitCorrelatorUpdateDeliveryRequest(correlator=" + correlator + ", correlatorUpdate=" + correlatorUpdate.toJSONString() + ")");
    submitCorrelatorUpdate(correlator, correlatorUpdate);
  }

  /*****************************************
   *
   * processCorrelatorUpdate
   *
   *****************************************/

  @Override
  protected void processCorrelatorUpdate(DeliveryRequest deliveryRequest, JSONObject correlatorUpdate)
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
   * shutdown
   *
   *****************************************/

  @Override
  protected void shutdown()
  {
    log.info("NotificationManager:  shutdown");
  }

  /*****************************************
   *
   * main
   *
   *****************************************/

  public static void main(String[] args)
  {
    log.info("NotificationManager: recieved " + args.length + " args");
    for (String arg : args)
      {
        log.info("NotificationManager: arg " + arg);
      }

    //
    // configuration
    //

    String deliveryManagerKey = args[0];
    String listOfChannels = args[1]; // Point separated by example: sms.sms_flash.email.pushapp

    //
    // instance
    //

    log.info("NotificationManager: configuration " + Deployment.getDeliveryManagers());

    NotificationManager manager = new NotificationManager(deliveryManagerKey, listOfChannels);

    //
    // run
    //

    manager.run();

  }

  public static ArrayList<String> getNotificationNodeTypes()
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

    ArrayList<String> result = new ArrayList<>();
    for (CommunicationChannel current : Deployment.getCommunicationChannels().values())
      {
        if(!current.isGeneric()) {
          continue;
        }
        
        ToolBoxBuilder tb = new ToolBoxBuilder(current.getToolboxID(), current.getName(), current.getDisplay(), current.getIcon(), current.getToolboxHeight(), current.getToolboxWidth(), OutputType.Static);

        tb.addFlatStringField("communicationChannelID", current.getID());
        tb.addOutputConnector(new OutputConnectorBuilder("delivered", "Delivered/Sent").addTransitionCriteria(new TransitionCriteriaBuilder("node.action.deliverystatus", CriterionOperator.IsInSetOperator, new ArgumentBuilder("[ 'delivered', 'acknowledged' ]"))));
        tb.addOutputConnector(new OutputConnectorBuilder("failed", "Failed").addTransitionCriteria(new TransitionCriteriaBuilder("node.action.deliverystatus", CriterionOperator.IsInSetOperator, new ArgumentBuilder("[ 'failed', 'indeterminate', 'failedTimeout' ]"))));
        tb.addOutputConnector(new OutputConnectorBuilder("timeout", "Timeout").addTransitionCriteria(new TransitionCriteriaBuilder("evaluation.date", CriterionOperator.GreaterThanOrEqualOperator, new ArgumentBuilder("dateAdd(node.entryDate, 1, 'minute')").setTimeUnit(TimeUnit.Instant))));
        tb.addOutputConnector(new OutputConnectorBuilder("unknown", "UnknownAppID").addTransitionCriteria(new TransitionCriteriaBuilder("subscriber.appID", CriterionOperator.IsNullOperator, null)));

        // add manually all parameters common to any notification : contact type, from
        // address
        // node.parameter.contacttype
        ParameterBuilder parameterBuilder = new ParameterBuilder("node.parameter.contacttype", "Contact Type", CriterionDataType.StringCriterion, false, true, null);
        for (ContactType currentContactType : ContactType.values())
          {
            parameterBuilder.addAvailableValue(new AvailableValueStaticStringBuilder(currentContactType.name(), currentContactType.getExternalRepresentation()));
          }
        tb.addParameter(parameterBuilder);

        // node.parameter.fromaddress
        tb.addParameter(new ParameterBuilder("node.parameter.fromaddress", "From Address", CriterionDataType.StringCriterion, false, true, null).addAvailableValue(new AvailableValueDynamicBuilder("#dialog_source_address_" + current.getID() + "#")));

        // if the configuration of the communication channel allows the use the
        // templates that are created from template GUI, let add the following
        // parameter:
        if (current.allowGuiTemplate())
          {
            ParameterBuilder templateParameter = new ParameterBuilder("node.parameter.dialog_template", "Message Template", CriterionDataType.Dialog, false, false, null).addAvailableValue(new AvailableValueDynamicBuilder("#dialog_template_" + current.getID() + "#"));
            templateParameter.addFlatStringField("communicationChannelID", current.getID());
            tb.addParameter(templateParameter);
          }
        if (current.getJSONRepresentation().get("parameters") != null)
          {
            JSONArray paramsJSON = JSONUtilities.decodeJSONArray(current.getJSONRepresentation(), "parameters");
            for (int i = 0; i < paramsJSON.size(); i++)
              {
                JSONObject cp = (JSONObject) paramsJSON.get(i);
                String dataType = JSONUtilities.decodeString(cp, "dataType");
                if(dataType != null && dataType.startsWith("template_")) {
                  // this parameter must not be put into the toolbox as the GUI will retrieve it directly from the channel definition
                  continue;
                }
                parameterBuilder = new ParameterBuilder(JSONUtilities.decodeString(cp, "id"), JSONUtilities.decodeString(cp, "display"), CriterionDataType.fromExternalRepresentation(JSONUtilities.decodeString(cp, "dataType")), JSONUtilities.decodeBoolean(cp, "multiple"), JSONUtilities.decodeBoolean(cp, "mandatory"), cp.get("defaultValue"));
                tb.addParameter(parameterBuilder);
                // TODO EVPRO-146 Available Values
              }
          }
       

        // Action:
        tb.setAction(new ActionBuilder("com.evolving.nglm.evolution.NotificationManager$ActionManager").addManagerClassConfigurationField("channelID", current.getID()).addManagerClassConfigurationField("moduleID", "1"));
        result.add(tb.build(0));
      }

    return result;
  }
}
