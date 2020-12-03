package com.evolving.nglm.evolution;

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
    PENDING(708, DeliveryStatus.Pending), 
    NO_CUSTOMER_LANGUAGE(701, DeliveryStatus.Failed), 
    NO_CUSTOMER_CHANNEL(702, DeliveryStatus.Failed), 
    DELIVERED(0, DeliveryStatus.Delivered), 
    EXPIRED(707, DeliveryStatus.Failed), 
    ERROR(24, DeliveryStatus.Failed), 
    UNDELIVERABLE(703, DeliveryStatus.Failed), 
    INVALID(704, DeliveryStatus.Failed), 
    QUEUE_FULL(705, DeliveryStatus.Failed), 
    RESCHEDULE(709, DeliveryStatus.Reschedule), 
    THROTTLING(23, DeliveryStatus.Failed), 
    UNKNOWN(-1, DeliveryStatus.Unknown);
   
    private Integer returncode;
    private DeliveryStatus associatedDeliveryStatus;

    private MessageStatus(Integer returncode, DeliveryStatus associatedDeliveryStatus)
      {
        this.returncode = returncode;
        this.associatedDeliveryStatus = associatedDeliveryStatus;
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
    
    public DeliveryStatus getAssociatedDeliveryStatus()
      {
        return associatedDeliveryStatus;
      }
  }

  private static final Logger log = LoggerFactory.getLogger(DeliveryManagerForNotifications.class);

  // lazy services instantiate using inner class holder
  private static class ServicesSingletonHolder{
    private static final SubscriberMessageTemplateService subscriberMessageTemplateService = new SubscriberMessageTemplateService(Deployment.getBrokerServers(), "NOT_USED", Deployment.getSubscriberMessageTemplateTopic(), false);
    private static final CommunicationChannelBlackoutService blackoutService = new CommunicationChannelBlackoutService(Deployment.getBrokerServers(), "NOT_USED", Deployment.getCommunicationChannelBlackoutTopic(), false);
    private static final CommunicationChannelTimeWindowService timeWindowService = new CommunicationChannelTimeWindowService(Deployment.getBrokerServers(), "NOT_USED", Deployment.getCommunicationChannelTimeWindowTopic(), false);
    private static final SourceAddressService sourceAddressService = new SourceAddressService(Deployment.getBrokerServers(), "NOT_USED", Deployment.getSourceAddressTopic(),false);
    static{
      subscriberMessageTemplateService.start();
      blackoutService.start();
      timeWindowService.start();
      sourceAddressService.start();
    }
  }


  /*****************************************
   *
   * accessors
   *
   *****************************************/

  public SubscriberMessageTemplateService getSubscriberMessageTemplateService()
  {
    return ServicesSingletonHolder.subscriberMessageTemplateService;
  }

  public CommunicationChannelBlackoutService getBlackoutService()
  {
    return ServicesSingletonHolder.blackoutService;
  }
  
  public CommunicationChannelTimeWindowService getTimeWindowService() 
  { 
    return ServicesSingletonHolder.timeWindowService;
  }

  public SourceAddressService getSourceAddressService() {
    return ServicesSingletonHolder.sourceAddressService;
  }

  protected DeliveryManagerForNotifications(String applicationID, String deliveryManagerKey, String bootstrapServers, ConnectSerde<? extends DeliveryRequest> requestSerde, DeliveryManagerDeclaration deliveryManagerDeclaration, int workerThreadNumber)
    {
      super(applicationID, deliveryManagerKey, bootstrapServers, requestSerde, deliveryManagerDeclaration, workerThreadNumber);
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
    if(log.isDebugEnabled()) log.debug("SMSNotificationManager.updateDeliveryRequest(deliveryRequest="+deliveryRequest+")");
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
    if(log.isDebugEnabled()) log.debug("DeliveryManagerForNotifications.completeDeliveryRequest(deliveryRequest=" + deliveryRequest + ")");
    completeRequest(deliveryRequest);
  }

  /*****************************************
   *
   * submitCorrelatorUpdateDeliveryRequest
   *
   *****************************************/

  public void submitCorrelatorUpdateDeliveryRequest(String correlator, JSONObject correlatorUpdate)
  {
    if(log.isDebugEnabled()) log.debug("DeliveryManagerForNotifications.submitCorrelatorUpdateDeliveryRequest(correlator=" + correlator + ", correlatorUpdate=" + correlatorUpdate.toJSONString() + ")");
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
        if(log.isDebugEnabled()) log.debug("SMSNotificationManager.processCorrelatorUpdate(deliveryRequest=" + deliveryRequest.toString() + ", correlatorUpdate=" + correlatorUpdate.toJSONString() + ")");
        notificationRequest.setMessageStatus(MessageStatus.fromReturnCode(result));
        notificationRequest.setDeliveryStatus(getDeliveryStatus(notificationRequest.getMessageStatus()));
        notificationRequest.setDeliveryDate(SystemTime.getCurrentTime());
        completeDeliveryRequest((DeliveryRequest) notificationRequest);
      }
  }
  
  public DeliveryStatus getDeliveryStatus(MessageStatus messageStatus)
  {
    return messageStatus != null ? messageStatus.getAssociatedDeliveryStatus() : DeliveryStatus.Unknown;
  }

}
