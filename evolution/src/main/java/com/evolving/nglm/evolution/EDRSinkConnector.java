package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EDREventsESFieldsDetails.ESField;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.NotificationManager.NotificationManagerRequest;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.PushNotificationManager.PushNotificationManagerRequest;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;

public class EDRSinkConnector extends SimpleESSinkConnector
{
  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<EDRSinkConnectorTask> taskClass()
  {
    return EDRSinkConnectorTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class EDRSinkConnectorTask extends StreamESSinkTask<Object>
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
    
    @Override public Object unpackRecord(SinkRecord sinkRecord) 
    {
      Object result = null;
      
      Object recordValue = sinkRecord.value();
      Schema recordValueSchema = sinkRecord.valueSchema();
      String schemaName = recordValueSchema.schema().name();
      if (schemaName.equals(PurchaseFulfillmentRequest.schema().name()))
        {
          //
          //  ODR
          //
          
          result =  PurchaseFulfillmentRequest.unpack(new SchemaAndValue(recordValueSchema, recordValue)); 
        }
      else
        {
          //
          //  BDR, MDR
          //
          
          for (String type : DeploymentCommon.getDeliveryManagers().keySet())
            {
              DeliveryManagerDeclaration manager = DeploymentCommon.getDeliveryManagers().get(type);
              String schema = manager.getRequestSerde().schema().name();
              if (schemaName.equals(schema))
                {
                  result = manager.getRequestSerde().unpack(new SchemaAndValue(recordValueSchema, recordValue));
                }
            }
        }
      
      //
      //  return 
      //
      
      return result;
    }

    /*****************************************
    *
    *  getDocumentIndexName
    *
    *****************************************/
    
    @Override
    protected String getDocumentIndexName(Object unchecked)
    {
      String timeZone = Deployment.getDefault().getTimeZone();
      Date eventDate = SystemTime.getCurrentTime();
      if (unchecked instanceof PurchaseFulfillmentRequest)
        {
          //
          //  ODR
          //
          
          timeZone = DeploymentCommon.getDeployment(((PurchaseFulfillmentRequest) unchecked).getTenantID()).getTimeZone();
          eventDate = ((PurchaseFulfillmentRequest) unchecked).getEventDate();
        } 
      else if (unchecked instanceof BonusDelivery)
        {
          //
          //  BDR
          //
          
          timeZone = DeploymentCommon.getDeployment(((BonusDelivery) unchecked).getTenantID()).getTimeZone();
          eventDate = ((BonusDelivery) unchecked).getEventDate();
        } 
      else if (unchecked instanceof MailNotificationManagerRequest)
        {
          //
          //  MDR
          //
          
          timeZone = DeploymentCommon.getDeployment(((MailNotificationManagerRequest) unchecked).getTenantID()).getTimeZone();
          eventDate = ((MailNotificationManagerRequest) unchecked).getCreationDate();
        } 
      else if (unchecked instanceof SMSNotificationManagerRequest)
        {
          timeZone = DeploymentCommon.getDeployment(((BonusDelivery) unchecked).getTenantID()).getTimeZone();
          eventDate = ((BonusDelivery) unchecked).getCreationDate();
        } 
      else if (unchecked instanceof NotificationManagerRequest)
        {
          timeZone = DeploymentCommon.getDeployment(((BonusDelivery) unchecked).getTenantID()).getTimeZone();
          eventDate = ((BonusDelivery) unchecked).getCreationDate();
        } 
      else if (unchecked instanceof PushNotificationManagerRequest)
        {
          timeZone = DeploymentCommon.getDeployment(((BonusDelivery) unchecked).getTenantID()).getTimeZone();
          eventDate = ((BonusDelivery) unchecked).getCreationDate();
        }
      return this.getDefaultIndexName() + RLMDateUtils.formatDateISOWeek(eventDate, timeZone);
    }
    
    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override
    public Map<String, Object> getDocumentMap(Object document)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();
      for (String eventName : Deployment.getEdrEventsESFieldsDetails().keySet())
        {
          EDREventsESFieldsDetails edrEventsESFieldsDetails = Deployment.getEdrEventsESFieldsDetails().get(eventName);
          if (edrEventsESFieldsDetails.getEsModelClass().equals(document.getClass()))
            {
              for (ESField field : edrEventsESFieldsDetails.getFields())
                {
                  String fieldName = field.getFieldName();
                  Object value = null;
                  if (field.getRetriever() != null)
                    {
                      try
                        {
                          value = field.getRetriever().invoke(document, null);
                        } 
                      catch (Throwable e)
                        {
                          e.printStackTrace();
                        }
                    }
                  documentMap.put(fieldName, value);
                  
                }
            }
        }
      return documentMap;
    }
  }
}

