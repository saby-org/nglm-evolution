package com.evolving.nglm.evolution;

import java.util.HashMap;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;

public abstract class DeliveryManagerForNotifications extends DeliveryManager
{

  /*****************************************
   *
   * enum - status
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
    THROTTLING(23), 
    UNKNOWN(999);
   
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

    public static MessageStatus fromExternalRepresentation(String value)
    {
      for (MessageStatus enumeratedValue : MessageStatus.values())
        {
          if (enumeratedValue.toString().equalsIgnoreCase(value)) return enumeratedValue;
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

  private static final Logger log = LoggerFactory.getLogger(DeliveryManagerForNotifications.class);

  private HashMap<String, NotificationStatistics> statsPerChannels = new HashMap<>();
  private SubscriberMessageTemplateService subscriberMessageTemplateService;
  private CommunicationChannelBlackoutService blackoutService;

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

  public HashMap<String, NotificationStatistics> getStatsPerChannels()
  {
    return statsPerChannels;
  }

  protected DeliveryManagerForNotifications(String applicationID, String deliveryManagerKey, String bootstrapServers, ConnectSerde<? extends DeliveryRequest> requestSerde, DeliveryManagerDeclaration deliveryManagerDeclaration)
    {
      super(applicationID, deliveryManagerKey, bootstrapServers, requestSerde, deliveryManagerDeclaration);

      //
      // service
      //

      subscriberMessageTemplateService = new SubscriberMessageTemplateService(Deployment.getBrokerServers(), "smsnotificationmanager-subscribermessagetemplateservice-" + deliveryManagerKey, Deployment.getSubscriberMessageTemplateTopic(), false);
      subscriberMessageTemplateService.start();

      //
      // blackoutService
      //

      blackoutService = new CommunicationChannelBlackoutService(Deployment.getBrokerServers(), "smsnotificationmanager-communicationchannelblackoutservice-" + deliveryManagerKey, Deployment.getCommunicationChannelBlackoutTopic(), false);
      blackoutService.start();
//      
//      //
//      // statistics
//      //
//      
//      try
//        {
//          stats = new NotificationStatistics(applicationID, channelID);
//        }
//      catch(Exception e)
//        {
//          log.error("SMSNotificationManager: could not load statistics ", e);
//          throw new RuntimeException("SMSNotificationManager: could not load statistics  ", e);
//        }
      // TODO que faire des stats ?

    }
  
  /*****************************************
  *
  *  updateDeliveryRequest
  *
  *****************************************/
  public void updateDeliveryRequest(INotificationRequest deliveryRequest)
  {
    updateDeliveryRequest((DeliveryRequest)deliveryRequest);
  }
  
  public void updateDeliveryRequest(DeliveryRequest deliveryRequest)
  {
    log.debug("SMSNotificationManager.updateDeliveryRequest(deliveryRequest="+deliveryRequest+")");
    updateRequest(deliveryRequest);
  }

  /*****************************************
  *
  * completeDeliveryRequest
  *
  *****************************************/

  public void completeDeliveryRequest(INotificationRequest deliveryRequest)
  {
    completeDeliveryRequest((DeliveryRequest)deliveryRequest);
  } 
  
  public void completeDeliveryRequest(DeliveryRequest deliveryRequest)
  {
    log.debug("DeliveryManagerForNotifications.completeDeliveryRequest(deliveryRequest=" + deliveryRequest + ")");
    completeRequest(deliveryRequest);
    // stats.updateMessageCount(pluginName, 1, deliveryRequest.getDeliveryStatus());
    // // TODO Stats ?
  }

  /*****************************************
   *
   * submitCorrelatorUpdateDeliveryRequest
   *
   *****************************************/

  public void submitCorrelatorUpdateDeliveryRequest(String correlator, JSONObject correlatorUpdate)
  {
    log.debug("DeliveryManagerForNotifications.submitCorrelatorUpdateDeliveryRequest(correlator=" + correlator + ", correlatorUpdate=" + correlatorUpdate.toJSONString() + ")");
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
    INotificationRequest notificationRequest = (INotificationRequest) deliveryRequest;
    if (notificationRequest != null)
      {
        log.debug("SMSNotificationManager.processCorrelatorUpdate(deliveryRequest=" + deliveryRequest.toString() + ", correlatorUpdate=" + correlatorUpdate.toJSONString() + ")");
        notificationRequest.setMessageStatus(MessageStatus.fromReturnCode(result));
        notificationRequest.setDeliveryStatus(getMessageStatus(notificationRequest.getMessageStatus()));
        notificationRequest.setDeliveryDate(SystemTime.getCurrentTime());
        completeDeliveryRequest((DeliveryRequest) notificationRequest);
      }
  }

}
