package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.evolution.MailNotificationManager.MAILMessageStatus;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentStatus;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSMessageStatus;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;

public class NotificationSinkConnector extends SimpleESSinkConnector
{
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
      Schema smsNotificationValueSchema = sinkRecord.valueSchema();
      
      Struct valueStruct = (Struct) smsNotificationValue;
      String type = valueStruct.getString("deliveryType");
      HashMap<String,Object> documentMap = null;
      
      if(type.equals("notificationmanagermail")){
        documentMap = new HashMap<String,Object>();
        MailNotificationManagerRequest mailNotification = MailNotificationManagerRequest.unpack(new SchemaAndValue(smsNotificationValueSchema, smsNotificationValue));
        documentMap = new HashMap<String,Object>();
        documentMap.put("subscriberID", mailNotification.getSubscriberID());
        documentMap.put("deliveryRequestID", mailNotification.getDeliveryRequestID());
        documentMap.put("eventID", mailNotification.getEventID());
        documentMap.put("eventDatetime", mailNotification.getEventDate());
        documentMap.put("moduleID", mailNotification.getModuleID());
        documentMap.put("featureID", mailNotification.getFeatureID());
        documentMap.put("origin", mailNotification.getDeliveryRequestSource());
        documentMap.put("responseCode", mailNotification.getReturnCode());
        documentMap.put("deliveryStatus", mailNotification.getMessageStatus().toString());
        documentMap.put("responseMessage", MAILMessageStatus.fromReturnCode(mailNotification.getReturnCode()));
      }else{
        documentMap = new HashMap<String,Object>();
        SMSNotificationManagerRequest smsNotification = SMSNotificationManagerRequest.unpack(new SchemaAndValue(smsNotificationValueSchema, smsNotificationValue));
        documentMap = new HashMap<String,Object>();
        documentMap.put("subscriberID", smsNotification.getSubscriberID());
        documentMap.put("deliveryRequestID", smsNotification.getDeliveryRequestID());
        documentMap.put("eventID", smsNotification.getEventID());
        documentMap.put("eventDatetime", smsNotification.getEventDate());
        documentMap.put("moduleID", smsNotification.getModuleID());
        documentMap.put("featureID", smsNotification.getFeatureID());
        documentMap.put("origin", smsNotification.getDeliveryRequestSource());
        documentMap.put("responseCode", smsNotification.getReturnCode());
        documentMap.put("deliveryStatus", smsNotification.getMessageStatus().toString());
        documentMap.put("responseMessage", SMSMessageStatus.fromReturnCode(smsNotification.getReturnCode()));
      }
      
      return documentMap;
    }
  }
}
