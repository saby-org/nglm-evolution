package com.evolving.nglm.evolution;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.NotificationManager.NotificationManagerRequest;
import com.evolving.nglm.evolution.PushNotificationManager.PushNotificationManagerRequest;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;

public class NotificationSinkConnector extends SimpleESSinkConnector
{
  
  private static String elasticSearchDateFormat = Deployment.getElasticSearchDateFormat();
  private static DateFormat dateFormat = new SimpleDateFormat(elasticSearchDateFormat);
  
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
  
  public static class NotificationSinkConnectorTask extends StreamESSinkTask
  {

    /****************************************
    *
    *  attributes
    *
    ****************************************/
    private SubscriberProfileService subscriberProfileService;

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

      //
      //  services
      //
      
      subscriberProfileService = SinkConnectorUtils.init();

    }

    /*****************************************
    *
    *  stop
    *
    *****************************************/

    @Override public void stop()
    {
      //
      //  services
      //

      if (subscriberProfileService != null) subscriberProfileService.stop();
      
      //
      //  super
      //

      super.stop();
    }
    
    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override
    public Map<String, Object> getDocumentMap(SinkRecord sinkRecord)
    {
      /******************************************
      *
      *  extract SMSNotificationManagerRequest
      *
      *******************************************/

      Object smsNotificationValue = sinkRecord.value();
      Schema notificationValueSchema = sinkRecord.valueSchema();
      
      Struct valueStruct = (Struct) smsNotificationValue;
      String type = valueStruct.getString("deliveryType");
      HashMap<String,Object> documentMap = null;
      
      //
      //  safety guard - return null
      // 

      if(type == null || type.equals(""))
      {
        return documentMap;
      }

      if(type.equals("notificationmanagermail"))
      {
        documentMap = new HashMap<String,Object>();
        MailNotificationManagerRequest notification = MailNotificationManagerRequest.unpack(new SchemaAndValue(notificationValueSchema, smsNotificationValue));
        if(notification.getOriginatingSubscriberID() != null && notification.getOriginatingSubscriberID().startsWith(DeliveryManager.TARGETED))
          {
            // case where this is a delegated request and its response is for the original subscriberID, so this response must be ignored.
            return null;
          }
        documentMap = new HashMap<String,Object>();
        documentMap.put("subscriberID", notification.getSubscriberID());
        documentMap.put("deliveryRequestID", notification.getDeliveryRequestID());
        documentMap.put("originatingDeliveryRequestID", notification.getOriginatingDeliveryRequestID());
        documentMap.put("eventID", "");
        documentMap.put("creationDate", notification.getCreationDate()!=null?dateFormat.format(notification.getCreationDate()):"");
        documentMap.put("deliveryDate", notification.getDeliveryDate()!=null?dateFormat.format(notification.getDeliveryDate()):"");
        documentMap.put("moduleID", notification.getModuleID());
        documentMap.put("featureID", notification.getFeatureID());
        documentMap.put("source", notification.getFromAddress());
        documentMap.put("returnCode", notification.getReturnCode());
        documentMap.put("returnCodeDetails", notification.getMessageDeliveryReturnCodeDetails());
	      documentMap.put("templateID", notification.getTemplateID());
        documentMap.put("language", notification.getLanguage());
        Map<String,List<String>> tags = new HashMap<>();
        tags.put("subjectTags", notification.getSubjectTags());
        tags.put("textBodyTags", notification.getTextBodyTags());
        tags.put("htmlBodyTags", notification.getHtmlBodyTags());
        documentMap.put("tags", tags);

      }
        else if(type.equals("notificationmanagersms"))
        {
          documentMap = new HashMap<String,Object>();
          SMSNotificationManagerRequest notification = SMSNotificationManagerRequest.unpack(new SchemaAndValue(notificationValueSchema, smsNotificationValue));
          if(notification.getOriginatingSubscriberID() != null && notification.getOriginatingSubscriberID().startsWith(DeliveryManager.TARGETED))
            {
              // case where this is a delegated request and its response is for the original subscriberID, so this response must be ignored.
              return null;
            }
          documentMap = new HashMap<String,Object>();
          documentMap.put("subscriberID", notification.getSubscriberID());
          documentMap.put("deliveryRequestID", notification.getDeliveryRequestID());
          documentMap.put("originatingDeliveryRequestID", notification.getOriginatingDeliveryRequestID());
          documentMap.put("eventID", "");
          documentMap.put("creationDate", notification.getCreationDate()!=null?dateFormat.format(notification.getCreationDate()):"");
          documentMap.put("deliveryDate", notification.getDeliveryDate()!=null?dateFormat.format(notification.getDeliveryDate()):"");
          documentMap.put("moduleID", notification.getModuleID());
          documentMap.put("featureID", notification.getFeatureID());
          documentMap.put("source", notification.getSource());
          documentMap.put("returnCode", notification.getReturnCode());
          documentMap.put("returnCodeDetails", notification.getMessageDeliveryReturnCodeDetails());
	        documentMap.put("templateID", notification.getTemplateID());
          documentMap.put("language", notification.getLanguage());
          documentMap.put("tags", notification.getMessageTags());
          Map<String,List<String>> tags = new HashMap<>();
          tags.put("tags", notification.getMessageTags());
          documentMap.put("tags", tags);
        }
        else if(type.equals("notificationmanager"))
        {
          documentMap = new HashMap<String,Object>();
          NotificationManagerRequest notification = NotificationManagerRequest.unpack(new SchemaAndValue(notificationValueSchema, smsNotificationValue));
          if(notification.getOriginatingSubscriberID() != null && notification.getOriginatingSubscriberID().startsWith(DeliveryManager.TARGETED))
            {
              // case where this is a delegated request and its response is for the original subscriberID, so this response must be ignored.
              return null;
            }
          documentMap = new HashMap<String,Object>();
          documentMap.put("subscriberID", notification.getSubscriberID());
          documentMap.put("deliveryRequestID", notification.getDeliveryRequestID());
          documentMap.put("originatingDeliveryRequestID", notification.getOriginatingDeliveryRequestID());
          documentMap.put("eventID", "");
          documentMap.put("creationDate", notification.getCreationDate()!=null?dateFormat.format(notification.getCreationDate()):"");
          documentMap.put("deliveryDate", notification.getDeliveryDate()!=null?dateFormat.format(notification.getDeliveryDate()):"");
          documentMap.put("moduleID", notification.getModuleID());
          documentMap.put("featureID", notification.getFeatureID());
          documentMap.put("source", notification.getNotificationParameters().get("node.parameter.fromaddress"));
          documentMap.put("returnCode", notification.getReturnCode());
          documentMap.put("returnCodeDetails", notification.getMessageDeliveryReturnCodeDetails());
	        documentMap.put("templateID", notification.getTemplateID());
          documentMap.put("language", notification.getLanguage());
          documentMap.put("tags", notification.getTags());
        }
        else
        {
          documentMap = new HashMap<String,Object>();
          PushNotificationManagerRequest notification = PushNotificationManagerRequest.unpack(new SchemaAndValue(notificationValueSchema, smsNotificationValue));
          if(notification.getOriginatingSubscriberID() != null && notification.getOriginatingSubscriberID().startsWith(DeliveryManager.TARGETED))
            {
              // case where this is a delegated request and its response is for the original subscriberID, so this response must be ignored.
              return null;
            }
          documentMap = new HashMap<String,Object>();
          documentMap.put("subscriberID", notification.getSubscriberID());
          documentMap.put("deliveryRequestID", notification.getDeliveryRequestID());
          documentMap.put("originatingDeliveryRequestID", notification.getOriginatingDeliveryRequestID());
          documentMap.put("eventID", "");
          documentMap.put("creationDate", notification.getCreationDate()!=null?dateFormat.format(notification.getCreationDate()):"");
          documentMap.put("deliveryDate", notification.getDeliveryDate()!=null?dateFormat.format(notification.getDeliveryDate()):"");
          documentMap.put("moduleID", notification.getModuleID());
          documentMap.put("featureID", notification.getFeatureID());
          documentMap.put("source", ""); // TODO SCH : what is the source of push notifications ?
          documentMap.put("returnCode", notification.getReturnCode());
          documentMap.put("returnCodeDetails", notification.getMessageDeliveryReturnCodeDetails());
	        documentMap.put("templateID", notification.getTemplateID());
          documentMap.put("language", notification.getLanguage());
          documentMap.put("tags", notification.getTags()); // TODO
        }
      
      return documentMap;
    }
  }
}

