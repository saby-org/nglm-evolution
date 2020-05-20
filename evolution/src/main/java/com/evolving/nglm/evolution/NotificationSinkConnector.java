package com.evolving.nglm.evolution;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.evolution.MailNotificationManager.MAILMessageStatus;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.PushNotificationManager.PushMessageStatus;
import com.evolving.nglm.evolution.PushNotificationManager.PushNotificationManagerRequest;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSMessageStatus;
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
        putAlternateIDs(notification.getSubscriberID(), documentMap);
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
        documentMap.put("returnCodeDetails", MAILMessageStatus.fromReturnCode(notification.getReturnCode()));
      }
        else if(type.equals("notificationmanagersms"))
        {
          documentMap = new HashMap<String,Object>();
          SMSNotificationManagerRequest notification = SMSNotificationManagerRequest.unpack(new SchemaAndValue(notificationValueSchema, smsNotificationValue));
          documentMap = new HashMap<String,Object>();
          documentMap.put("subscriberID", notification.getSubscriberID());
          putAlternateIDs(notification.getSubscriberID(), documentMap);
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
          documentMap.put("returnCodeDetails", SMSMessageStatus.fromReturnCode(notification.getReturnCode()));
        }
        else
        {
          documentMap = new HashMap<String,Object>();
          PushNotificationManagerRequest notification = PushNotificationManagerRequest.unpack(new SchemaAndValue(notificationValueSchema, smsNotificationValue));
          documentMap = new HashMap<String,Object>();
          documentMap.put("subscriberID", notification.getSubscriberID());
          putAlternateIDs(notification.getSubscriberID(), documentMap);
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
          documentMap.put("returnCodeDetails", PushMessageStatus.fromReturnCode(notification.getReturnCode()));
        }
      
      return documentMap;
    }
  }
}
