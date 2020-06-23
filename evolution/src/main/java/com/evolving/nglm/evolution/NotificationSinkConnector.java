package com.evolving.nglm.evolution;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.RLMDateUtils;
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
      
      if(type.equals("notificationmanagermail")){
        documentMap = new HashMap<String,Object>();
        MailNotificationManagerRequest mailNotification = MailNotificationManagerRequest.unpack(new SchemaAndValue(notificationValueSchema, smsNotificationValue));
        documentMap = new HashMap<String,Object>();
        documentMap.put("subscriberID", mailNotification.getSubscriberID());
        documentMap.put("deliveryRequestID", mailNotification.getDeliveryRequestID());
        documentMap.put("originatingDeliveryRequestID", mailNotification.getOriginatingDeliveryRequestID());
        documentMap.put("eventID", "");
        documentMap.put("creationDate", mailNotification.getCreationDate()!=null?dateFormat.format(mailNotification.getCreationDate()):"");
        documentMap.put("deliveryDate", mailNotification.getDeliveryDate()!=null?dateFormat.format(mailNotification.getDeliveryDate()):"");
        documentMap.put("moduleID", mailNotification.getModuleID());
        documentMap.put("featureID", mailNotification.getFeatureID());
        documentMap.put("source", mailNotification.getFromAddress());
        documentMap.put("returnCode", mailNotification.getReturnCode());
        documentMap.put("returnCodeDetails", mailNotification.getReturnCodeDetails());
      }else if(type.equals("notificationmanagerpush")){
        documentMap = new HashMap<String,Object>();
        PushNotificationManagerRequest pushNotification = PushNotificationManagerRequest.unpack(new SchemaAndValue(notificationValueSchema, smsNotificationValue));
        documentMap = new HashMap<String,Object>();
        documentMap.put("subscriberID", pushNotification.getSubscriberID());
        documentMap.put("deliveryRequestID", pushNotification.getDeliveryRequestID());
        documentMap.put("originatingDeliveryRequestID", pushNotification.getOriginatingDeliveryRequestID());
        documentMap.put("eventID", "");
        documentMap.put("creationDate", pushNotification.getCreationDate()!=null?dateFormat.format(pushNotification.getCreationDate()):"");
        documentMap.put("deliveryDate", pushNotification.getDeliveryDate()!=null?dateFormat.format(pushNotification.getDeliveryDate()):"");
        documentMap.put("moduleID", pushNotification.getModuleID());
        documentMap.put("featureID", pushNotification.getFeatureID());
        documentMap.put("source", ""); // TODO SCH : what is the source of push notifications ?
        documentMap.put("returnCode", pushNotification.getReturnCode());
        documentMap.put("returnCodeDetails", pushNotification.getReturnCodeDetails());
      } else if(type.equals("notificationmanagersms"))
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
          documentMap.put("returnCodeDetails", MessageStatus.fromReturnCode(notification.getReturnCode()));
        }
        else if(type.equals("notificationmanager"))
        {
          documentMap = new HashMap<String,Object>();
          NotificationManagerRequest smsNotification = NotificationManagerRequest.unpack(new SchemaAndValue(notificationValueSchema, smsNotificationValue));
          documentMap = new HashMap<String,Object>();
          documentMap.put("subscriberID", smsNotification.getSubscriberID());
          documentMap.put("deliveryRequestID", smsNotification.getDeliveryRequestID());
          documentMap.put("originatingDeliveryRequestID", smsNotification.getOriginatingDeliveryRequestID());
          documentMap.put("eventID", "");
          documentMap.put("creationDate", smsNotification.getCreationDate()!=null?dateFormat.format(smsNotification.getCreationDate()):"");
          documentMap.put("deliveryDate", smsNotification.getDeliveryDate()!=null?dateFormat.format(smsNotification.getDeliveryDate()):"");
          documentMap.put("moduleID", smsNotification.getModuleID());
          documentMap.put("featureID", smsNotification.getFeatureID());
          documentMap.put("source", smsNotification.getNotificationParameters().get("node.parameter.fromaddress"));
          documentMap.put("returnCode", smsNotification.getReturnCode());
          documentMap.put("returnCodeDetails", MessageStatus.fromReturnCode(smsNotification.getReturnCode()));
        }
        else
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
          documentMap.put("returnCodeDetails", MessageStatus.fromReturnCode(notification.getReturnCode()));
        }
      
      return documentMap;
    }
  }
}
