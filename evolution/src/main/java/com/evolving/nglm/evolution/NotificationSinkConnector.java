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

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.evolution.DeliveryManagerForNotifications.MessageStatus;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.NotificationManager.NotificationManagerRequest;
import com.evolving.nglm.evolution.PushNotificationManager.PushNotificationManagerRequest;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;

public class NotificationSinkConnector extends SimpleESSinkConnector
{
  
  private static String elasticSearchDateFormat = Deployment.getElasticSearchDateFormat();
  private static DateFormat dateFormat = new SimpleDateFormat(elasticSearchDateFormat);
  
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
        documentMap = new HashMap<String,Object>();
        documentMap.put("subscriberID", notification.getSubscriberID());
        SinkConnectorUtils.putAlternateIDs(notification.getSubscriberID(), documentMap, subscriberProfileService);
        documentMap.put("deliveryRequestID", notification.getDeliveryRequestID());
        documentMap.put("originatingDeliveryRequestID", notification.getOriginatingDeliveryRequestID());
        documentMap.put("eventID", "");
        documentMap.put("creationDate", notification.getCreationDate()!=null?dateFormat.format(notification.getCreationDate()):"");
        documentMap.put("deliveryDate", notification.getDeliveryDate()!=null?dateFormat.format(notification.getDeliveryDate()):"");
        documentMap.put("moduleID", notification.getModuleID());
        documentMap.put("featureID", notification.getFeatureID());
        documentMap.put("source", notification.getFromAddress());
        documentMap.put("returnCode", notification.getReturnCode());
        documentMap.put("deliveryStatus", notification.getMessageStatus().toString());
        documentMap.put("returnCodeDetails", MessageStatus.fromReturnCode(notification.getReturnCode()));
        documentMap.put("templateID", notification.getTemplateID());
        documentMap.put("language", notification.getLanguage());
        documentMap.put("subjectTags", notification.getSubjectTags());
        documentMap.put("textBodyTags", notification.getTextBodyTags());
        documentMap.put("htmlBodyTags", notification.getHtmlBodyTags());
      }
      else if(type.equals("notificationmanagersms"))
      {
          documentMap = new HashMap<String,Object>();
          SMSNotificationManagerRequest notification = SMSNotificationManagerRequest.unpack(new SchemaAndValue(notificationValueSchema, smsNotificationValue));
          documentMap = new HashMap<String,Object>();
          documentMap.put("subscriberID", notification.getSubscriberID());
          SinkConnectorUtils.putAlternateIDs(notification.getSubscriberID(), documentMap, subscriberProfileService);
          documentMap.put("deliveryRequestID", notification.getDeliveryRequestID());
          documentMap.put("originatingDeliveryRequestID", notification.getOriginatingDeliveryRequestID());
          documentMap.put("eventID", "");
          documentMap.put("creationDate", notification.getCreationDate()!=null?dateFormat.format(notification.getCreationDate()):"");
          documentMap.put("deliveryDate", notification.getDeliveryDate()!=null?dateFormat.format(notification.getDeliveryDate()):"");
          documentMap.put("moduleID", notification.getModuleID());
          documentMap.put("featureID", notification.getFeatureID());
          documentMap.put("source", notification.getSource());
          documentMap.put("returnCode", notification.getReturnCode());
          documentMap.put("deliveryStatus", notification.getMessageStatus().toString());
          documentMap.put("returnCodeDetails", MessageStatus.fromReturnCode(notification.getReturnCode()));
          documentMap.put("templateID", notification.getTemplateID());
          documentMap.put("language", notification.getLanguage());
          documentMap.put("tags", notification.getMessageTags());
      }
      else if(type.equals("notificationmanager"))
      {
          documentMap = new HashMap<String,Object>();
          NotificationManagerRequest notification = NotificationManagerRequest.unpack(new SchemaAndValue(notificationValueSchema, smsNotificationValue));
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
          documentMap.put("deliveryStatus", notification.getMessageStatus().toString());
          documentMap.put("returnCodeDetails", MessageStatus.fromReturnCode(notification.getReturnCode()));
          documentMap.put("templateID", notification.getTemplateID());
          documentMap.put("language", notification.getLanguage());
          documentMap.put("tags", notification.getTags()); // TODO : this is a map
          documentMap.put("other1", notification.getClass().getCanonicalName());
      }
      else if(type.equals("notificationmanagerpush"))
      {
          documentMap = new HashMap<String,Object>();
          PushNotificationManagerRequest notification = PushNotificationManagerRequest.unpack(new SchemaAndValue(notificationValueSchema, smsNotificationValue));
          documentMap = new HashMap<String,Object>();
          documentMap.put("subscriberID", notification.getSubscriberID());
          SinkConnectorUtils.putAlternateIDs(notification.getSubscriberID(), documentMap, subscriberProfileService);
          documentMap.put("deliveryRequestID", notification.getDeliveryRequestID());
          documentMap.put("originatingDeliveryRequestID", notification.getOriginatingDeliveryRequestID());
          documentMap.put("eventID", "");
          documentMap.put("creationDate", notification.getCreationDate()!=null?dateFormat.format(notification.getCreationDate()):"");
          documentMap.put("deliveryDate", notification.getDeliveryDate()!=null?dateFormat.format(notification.getDeliveryDate()):"");
          documentMap.put("moduleID", notification.getModuleID());
          documentMap.put("featureID", notification.getFeatureID());
          documentMap.put("source", ""); // TODO SCH : what is the source of push notifications ?
          documentMap.put("returnCode", notification.getReturnCode());
          documentMap.put("deliveryStatus", notification.getMessageStatus().toString());
          documentMap.put("returnCodeDetails", MessageStatus.fromReturnCode(notification.getReturnCode()));
          documentMap.put("templateID", notification.getTemplateID());
          documentMap.put("language", notification.getLanguage());
          documentMap.put("tags", notification.getTags()); // TODO
          documentMap.put("other2", notification.getClass().getCanonicalName());
      }
      
      return documentMap;
    }
  }
}
