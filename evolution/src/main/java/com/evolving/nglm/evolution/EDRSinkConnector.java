package com.evolving.nglm.evolution;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.DeploymentCommon;
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
  private static final Map<String, Method> EVENT_CLASS_METHOD_MAPPING = new ConcurrentHashMap<String, Method>();
  
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
      if (ignoreableDocument(document)) return null;
      Map<String,Object> documentMap = new HashMap<String,Object>();
      Class className = document.getClass();
      log.info("RAJ K className {}", className);
      boolean edrMapped = false;
      for (String eventName : Deployment.getEdrEventsESFieldsDetails().keySet())
        {
          EDREventsESFieldsDetails edrEventsESFieldsDetails = Deployment.getEdrEventsESFieldsDetails().get(eventName);
          if (edrEventsESFieldsDetails.getEsModelClasses().contains(className.getName()))
            {
              log.info("RAJ K EDREventsESFieldsDetails {}", edrEventsESFieldsDetails.getEventName());
              for (ESField field : edrEventsESFieldsDetails.getFields())
                {
                  Object value = null;
                  String methodToInvoke = className.getName().concat(".").concat(field.getRetrieverName());
                  Method m = null;
                  if (EVENT_CLASS_METHOD_MAPPING.containsKey(methodToInvoke))
                    {
                      m = EVENT_CLASS_METHOD_MAPPING.get(methodToInvoke);
                    }
                  else
                    {
                      try
                        {
                          m = className.getMethod(field.getRetrieverName(), null);
                          synchronized (EVENT_CLASS_METHOD_MAPPING)
                            {
                              EVENT_CLASS_METHOD_MAPPING.put(methodToInvoke, m);
                            }
                        } 
                      catch (NoSuchMethodException | SecurityException e)
                        {
                          if (log.isDebugEnabled()) log.debug("error {}", e.getMessage());
                        }
                    }
                  log.info("RAJ K method {}", m);
                  if (m != null)
                    {
                      log.info("RAJ K methodName {}", m.getName());
                      try
                        {
                          value = m.invoke(document, null);
                          value = normalize(value);
                          log.info("RAJ K value {}", value);
                          documentMap.put(field.getFieldName(), value);
                        } 
                      catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
                        {
                          if (log.isErrorEnabled()) log.error("error {}", e.getMessage());
                        }
                    }
                }
              edrMapped = true;
              break;
            }
        }
      if (!edrMapped) log.warn("EDR is not mapped for request {}", className.getName());
      return documentMap;
    }

    /*************************************************
     * 
     *  normalize
     * 
     ************************************************/
    
    private Object normalize(Object value)
    {
      Object result = value;
      if (value != null)
        {
          if (value instanceof Date) result = RLMDateUtils.formatDateForElasticsearchDefault((Date) value);
        }
      return result;
    }

    /*************************************************
     * 
     *  ignoreableDocument
     * 
     ************************************************/
    
    private boolean ignoreableDocument(Object document)
    {
      boolean result = false;
      if(document instanceof BonusDelivery)
        {
          BonusDelivery bonusDelivery = (BonusDelivery) document;
          result = bonusDelivery.getOriginatingSubscriberID() != null && bonusDelivery.getOriginatingSubscriberID().startsWith(DeliveryManager.TARGETED);
        }
      return result;
    }
  }
}

