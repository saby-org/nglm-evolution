package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSMessageStatus;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;

public abstract class DeliveryManagerForNotifications extends DeliveryManager
{

  private static final Logger log = LoggerFactory.getLogger(DeliveryManagerForNotifications.class);
  
  private NotificationStatistics stats = null;
  private SubscriberMessageTemplateService subscriberMessageTemplateService;
  private CommunicationChannelBlackoutService blackoutService;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public SubscriberMessageTemplateService getSubscriberMessageTemplateService() { return subscriberMessageTemplateService; }
  public CommunicationChannelBlackoutService getBlackoutService() { return blackoutService; }
  public NotificationStatistics getStats() { return stats; };
  
  
  protected DeliveryManagerForNotifications(String applicationID, String deliveryManagerKey, String bootstrapServers, ConnectSerde<? extends DeliveryRequest> requestSerde, DeliveryManagerDeclaration deliveryManagerDeclaration)
    {
      super(applicationID, deliveryManagerKey, bootstrapServers, requestSerde, deliveryManagerDeclaration);
      
      //
      //  service
      //

      subscriberMessageTemplateService = new SubscriberMessageTemplateService(Deployment.getBrokerServers(), "smsnotificationmanager-subscribermessagetemplateservice-" + deliveryManagerKey, Deployment.getSubscriberMessageTemplateTopic(), false);
      subscriberMessageTemplateService.start();
          
      //
      //  blackoutService
      //
          
      blackoutService = new CommunicationChannelBlackoutService(Deployment.getBrokerServers(), "smsnotificationmanager-communicationchannelblackoutservice-" + deliveryManagerKey, Deployment.getCommunicationChannelBlackoutTopic(), false);
      blackoutService.start();
      
      //
      // statistics
      //
      
      try
        {
          stats = new NotificationStatistics(applicationID, pluginName);
        }
      catch(Exception e)
        {
          log.error("SMSNotificationManager: could not load statistics ", e);
          throw new RuntimeException("SMSNotificationManager: could not load statistics  ", e);
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

  public void completeDeliveryRequest(INotificationRequest deliveryRequest, String channelID)
  {
    log.debug("SMSNotificationManager.updateDeliveryRequest(deliveryRequest="+deliveryRequest+")");
    completeRequest((DeliveryRequest)deliveryRequest);
    stats.updateMessageCount(channelID, 1, deliveryRequest.getDeliveryStatus());
  }
  
  public void completeDeliveryRequest(DeliveryRequest deliveryRequest, String channelID)
  {
    log.debug("SMSNotificationManager.updateDeliveryRequest(deliveryRequest="+deliveryRequest+")");
    completeRequest(deliveryRequest);
    stats.updateMessageCount(channelID, 1, deliveryRequest.getDeliveryStatus());
  }

//  /*****************************************
//  *
//  *  submitCorrelatorUpdateDeliveryRequest
//  *
//  *****************************************/
//
//  public void submitCorrelatorUpdateDeliveryRequest(String correlator, JSONObject correlatorUpdate)
//  {
//    log.debug("SMSNotificationManager.submitCorrelatorUpdateDeliveryRequest(correlator="+correlator+", correlatorUpdate="+correlatorUpdate.toJSONString()+")");
//    submitCorrelatorUpdate(correlator, correlatorUpdate);
//  }

  /*****************************************
  *
  *  processCorrelatorUpdate
  *
  *****************************************/

  @Override protected void processCorrelatorUpdate(INotificationRequest deliveryRequest, JSONObject correlatorUpdate)
  {
    int result = JSONUtilities.decodeInteger(correlatorUpdate, "result", true);
    SMSNotificationManagerRequest smsRequest = (SMSNotificationManagerRequest) deliveryRequest;
    if (smsRequest != null)
      {
        log.debug("SMSNotificationManager.processCorrelatorUpdate(deliveryRequest="+deliveryRequest.toString()+", correlatorUpdate="+correlatorUpdate.toJSONString()+")");
        smsRequest.setMessageStatus(SMSMessageStatus.fromReturnCode(result));
        smsRequest.setDeliveryStatus(getMessageStatus(smsRequest.getMessageStatus()));
        smsRequest.setDeliveryDate(SystemTime.getCurrentTime());
        completeDeliveryRequest((DeliveryRequest)smsRequest);
      }
  }
  
  

}
