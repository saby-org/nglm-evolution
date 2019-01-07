package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.evolution.MailNotificationManager.MAILMessageStatus;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
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
        documentMap.put("customer_id", mailNotification.getSubscriberID());
        documentMap.put("delivery_request_id", mailNotification.getDeliveryRequestID());
        documentMap.put("event_id", mailNotification.getEventID());
        documentMap.put("event_datetime", mailNotification.getEventDate());
        documentMap.put("module_id", mailNotification.getModuleID());
        documentMap.put("feature_id", mailNotification.getFeatureID());
        documentMap.put("origin", "");
        documentMap.put("return_code", mailNotification.getReturnCode());
        documentMap.put("delivery_status", mailNotification.getMessageStatus().toString());
        documentMap.put("return_code_details", mailNotification.getReturnCodeDetails());
      }else{
        documentMap = new HashMap<String,Object>();
        SMSNotificationManagerRequest smsNotification = SMSNotificationManagerRequest.unpack(new SchemaAndValue(smsNotificationValueSchema, smsNotificationValue));
        documentMap = new HashMap<String,Object>();
        documentMap.put("customer_id", smsNotification.getSubscriberID());
        documentMap.put("delivery_request_id", smsNotification.getDeliveryRequestID());
        documentMap.put("event_id", smsNotification.getDeliveryRequestID());
        documentMap.put("event_datetime", smsNotification.getEventDate());
        documentMap.put("module_id", smsNotification.getModuleID());
        documentMap.put("feature_id", smsNotification.getFeatureID());
        documentMap.put("origin", "");
        documentMap.put("return_code", smsNotification.getReturnCode());
        documentMap.put("delivery_status", smsNotification.getMessageStatus().toString());
        documentMap.put("return_code_details", smsNotification.getReturnCodeDetails());
      }
      
      return documentMap;
    }
  }
}
