package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.NotificationManager.NotificationManagerRequest;
import com.evolving.nglm.evolution.PushNotificationManager.PushNotificationManagerRequest;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;

public class NotificationSinkConnector extends SimpleESSinkConnector
{
  private final Logger log = LoggerFactory.getLogger(NotificationSinkConnector.class);

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<NotificationSinkConnectorTask> taskClass()
  {
    return NotificationSinkConnectorTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class NotificationSinkConnectorTask extends StreamESSinkTask<MessageDelivery>
  {
    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> taskConfig)
    {
      //
      //  super
      //

      super.start(taskConfig);
    }

    /*****************************************
    *
    *  stop
    *
    *****************************************/

    @Override public void stop()
    {
      //
      //  super
      //

      super.stop();
    }

    /*****************************************
    *
    *  unpackRecord
    *
    *****************************************/
    
    @Override public MessageDelivery unpackRecord(SinkRecord sinkRecord) 
    {
      Object notificationValue = sinkRecord.value();
      Schema notificationValueSchema = sinkRecord.valueSchema();

      Struct valueStruct = (Struct) notificationValue;
      String type = valueStruct.getString("deliveryType");

      //  safety guard - return null
      if(type == null || type.equals("") || Deployment.getDeliveryManagers().get(type)==null ) {
        return null;
      }

      return (MessageDelivery) Deployment.getDeliveryManagers().get(type).getRequestSerde().unpack(new SchemaAndValue(notificationValueSchema, notificationValue));

    }
    
    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override
    public Map<String, Object> getDocumentMap(MessageDelivery notification)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();
      
      if (notification instanceof MailNotificationManagerRequest) {
        MailNotificationManagerRequest mailNotification = (MailNotificationManagerRequest) notification;
        if(mailNotification.getOriginatingSubscriberID() != null && mailNotification.getOriginatingSubscriberID().startsWith(DeliveryManager.TARGETED))
          {
            // case where this is a delegated request and its response is for the original subscriberID, so this response must be ignored.
            return null;
          }
        documentMap.put("subscriberID", mailNotification.getSubscriberID());
        SinkConnectorUtils.putAlternateIDs(mailNotification.getAlternateIDs(), documentMap);
        documentMap.put("deliveryRequestID", mailNotification.getDeliveryRequestID());
        documentMap.put("originatingDeliveryRequestID", mailNotification.getOriginatingDeliveryRequestID());
        documentMap.put("eventID", "");
        documentMap.put("creationDate", mailNotification.getCreationDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(mailNotification.getCreationDate()):"");
        documentMap.put("deliveryDate", mailNotification.getDeliveryDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(mailNotification.getDeliveryDate()):"");
        documentMap.put("moduleID", mailNotification.getModuleID());
        documentMap.put("featureID", mailNotification.getFeatureID());
        documentMap.put("source", mailNotification.getFromAddress());
        documentMap.put("returnCode", mailNotification.getReturnCode());
        documentMap.put("returnCodeDetails", mailNotification.getMessageDeliveryReturnCodeDetails());
        documentMap.put("templateID", mailNotification.getTemplateID());
        documentMap.put("language", mailNotification.getLanguage());
        Map<String,List<String>> tags = new HashMap<>();
        tags.put("subjectTags", mailNotification.getSubjectTags());
        tags.put("textBodyTags", mailNotification.getTextBodyTags());
        tags.put("htmlBodyTags", mailNotification.getHtmlBodyTags());
        documentMap.put("tags", tags);
        String deliveryType = mailNotification.getDeliveryType();
        String channelID = Deployment.getDeliveryTypeCommunicationChannelIDMap().get(deliveryType);
        documentMap.put("channelID", channelID);
        documentMap.put("contactType", mailNotification.getContactType());
      }
      else if (notification instanceof SMSNotificationManagerRequest) {
        SMSNotificationManagerRequest smsNotification = (SMSNotificationManagerRequest) notification;
        if(smsNotification.getOriginatingSubscriberID() != null && smsNotification.getOriginatingSubscriberID().startsWith(DeliveryManager.TARGETED))
          {
            // case where this is a delegated request and its response is for the original subscriberID, so this response must be ignored.
            return null;
          }
        documentMap.put("subscriberID", smsNotification.getSubscriberID());
        SinkConnectorUtils.putAlternateIDs(smsNotification.getAlternateIDs(), documentMap);
        documentMap.put("deliveryRequestID", smsNotification.getDeliveryRequestID());
        documentMap.put("originatingDeliveryRequestID", smsNotification.getOriginatingDeliveryRequestID());
        documentMap.put("eventID", "");
        documentMap.put("creationDate", smsNotification.getCreationDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(smsNotification.getCreationDate()):"");
        documentMap.put("deliveryDate", smsNotification.getDeliveryDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(smsNotification.getDeliveryDate()):"");
        documentMap.put("moduleID", smsNotification.getModuleID());
        documentMap.put("featureID", smsNotification.getFeatureID());
        documentMap.put("source", smsNotification.getSource());
        documentMap.put("returnCode", smsNotification.getReturnCode());
        documentMap.put("returnCodeDetails", smsNotification.getMessageDeliveryReturnCodeDetails());
        documentMap.put("templateID", smsNotification.getTemplateID());
        documentMap.put("language", smsNotification.getLanguage());
        documentMap.put("tags", smsNotification.getMessageTags());
        Map<String,List<String>> tags = new HashMap<>();
        tags.put("tags", smsNotification.getMessageTags());
        documentMap.put("tags", tags);
        String deliveryType = smsNotification.getDeliveryType();
        String channelID = Deployment.getDeliveryTypeCommunicationChannelIDMap().get(deliveryType);
        documentMap.put("channelID", channelID);
        documentMap.put("contactType", smsNotification.getContactType());
      }
      else if (notification instanceof NotificationManagerRequest) {
        NotificationManagerRequest notifNotification = (NotificationManagerRequest) notification;
        if(notifNotification.getOriginatingSubscriberID() != null && notifNotification.getOriginatingSubscriberID().startsWith(DeliveryManager.TARGETED))
          {
            // case where this is a delegated request and its response is for the original subscriberID, so this response must be ignored.
            return null;
          }
        documentMap.put("subscriberID", notifNotification.getSubscriberID());
        SinkConnectorUtils.putAlternateIDs(notifNotification.getAlternateIDs(), documentMap);
        documentMap.put("deliveryRequestID", notifNotification.getDeliveryRequestID());
        documentMap.put("originatingDeliveryRequestID", notifNotification.getOriginatingDeliveryRequestID());
        documentMap.put("eventID", "");
        documentMap.put("creationDate", notifNotification.getCreationDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(notifNotification.getCreationDate()):"");
        documentMap.put("deliveryDate", notifNotification.getDeliveryDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(notifNotification.getDeliveryDate()):"");
        documentMap.put("moduleID", notifNotification.getModuleID());
        documentMap.put("featureID", notifNotification.getFeatureID());
        documentMap.put("source", notifNotification.getSourceAddressParam());
        documentMap.put("returnCode", notifNotification.getReturnCode());
        documentMap.put("returnCodeDetails", notifNotification.getMessageDeliveryReturnCodeDetails());
        documentMap.put("templateID", notifNotification.getTemplateID());
        documentMap.put("language", notifNotification.getLanguage());
        documentMap.put("tags", notifNotification.getTags());        
        String channelID = notifNotification.getChannelID();
        documentMap.put("channelID", channelID);
        documentMap.put("contactType", notifNotification.getContactType());
      }
      else {
        PushNotificationManagerRequest pushNotification = (PushNotificationManagerRequest) notification;
        if(pushNotification.getOriginatingSubscriberID() != null && pushNotification.getOriginatingSubscriberID().startsWith(DeliveryManager.TARGETED))
          {
            // case where this is a delegated request and its response is for the original subscriberID, so this response must be ignored.
            return null;
          }
        documentMap.put("subscriberID", pushNotification.getSubscriberID());
        SinkConnectorUtils.putAlternateIDs(pushNotification.getAlternateIDs(), documentMap);
        documentMap.put("deliveryRequestID", pushNotification.getDeliveryRequestID());
        documentMap.put("originatingDeliveryRequestID", pushNotification.getOriginatingDeliveryRequestID());
        documentMap.put("eventID", "");
        documentMap.put("creationDate", pushNotification.getCreationDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(pushNotification.getCreationDate()):"");
        documentMap.put("deliveryDate", pushNotification.getDeliveryDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(pushNotification.getDeliveryDate()):"");
        documentMap.put("moduleID", pushNotification.getModuleID());
        documentMap.put("featureID", pushNotification.getFeatureID());
        documentMap.put("source", ""); // TODO SCH : what is the source of push notifications ?
        documentMap.put("returnCode", pushNotification.getReturnCode());
        documentMap.put("returnCodeDetails", pushNotification.getMessageDeliveryReturnCodeDetails());
        documentMap.put("templateID", pushNotification.getTemplateID());
        documentMap.put("language", pushNotification.getLanguage());
        documentMap.put("tags", pushNotification.getTags()); // TODO
        String deliveryType = pushNotification.getDeliveryType();
        String channelID = Deployment.getDeliveryTypeCommunicationChannelIDMap().get(deliveryType);
        documentMap.put("channelID", channelID);
        documentMap.put("contactType", pushNotification.getContactType());
      }
      
      return documentMap;
    }
  }
}
