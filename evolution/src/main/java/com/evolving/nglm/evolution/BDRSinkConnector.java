package com.evolving.nglm.evolution;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryRequest;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;


public class BDRSinkConnector extends SimpleESSinkConnector
{
  private static String elasticSearchDateFormat = Deployment.getElasticSearchDateFormat();
  private static DateFormat dateFormat = new SimpleDateFormat(elasticSearchDateFormat);
  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<BDRSinkConnectorTask> taskClass()
  {
    return BDRSinkConnectorTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class BDRSinkConnectorTask extends StreamESSinkTask
  {

    /****************************************
    *
    *  attributes
    *
    ****************************************/
    private SubscriberProfileService subscriberProfileService;
    
    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(BDRSinkConnector.class);

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
      String subscriberProfileEndpoints = Deployment.getSubscriberProfileEndpoints();
      subscriberProfileService = new EngineSubscriberProfileService(subscriberProfileEndpoints);
      subscriberProfileService.start();

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
      *  extract CommodityDeliveryRequest
      *
      *******************************************/
      log.debug("BDRSinkConnector.getDocumentMap: computing map to give to elastic search");
      Object commodityRequestValue = sinkRecord.value();
      Schema commodityRequestValueSchema = sinkRecord.valueSchema();
      CommodityDeliveryRequest commodityRequest = CommodityDeliveryRequest.unpack(new SchemaAndValue(commodityRequestValueSchema, commodityRequestValue));
      Map<String,Object> documentMap = null;
      
      if(commodityRequest != null){
        
        documentMap = new HashMap<String,Object>();
        documentMap.put("subscriberID", commodityRequest.getSubscriberID());
        putAlternateIDs(commodityRequest.getSubscriberID(), documentMap);
        documentMap.put("eventDatetime", commodityRequest.getEventDate()!=null?dateFormat.format(commodityRequest.getEventDate()):"");
        documentMap.put("deliveryRequestID", commodityRequest.getDeliveryRequestID());
        documentMap.put("originatingDeliveryRequestID", commodityRequest.getOriginatingDeliveryRequestID());
        documentMap.put("eventID", commodityRequest.getEventID());
        documentMap.put("deliverableExpirationDate", commodityRequest.getDeliverableExpirationDate());
        documentMap.put("providerID", commodityRequest.getProviderID());
        documentMap.put("deliverableID", commodityRequest.getCommodityID());
        documentMap.put("deliverableQty", commodityRequest.getAmount());
        documentMap.put("operation", commodityRequest.getOperation().toString().toUpperCase());
        documentMap.put("moduleID", commodityRequest.getModuleID());
        documentMap.put("featureID", commodityRequest.getFeatureID());
        documentMap.put("origin", "");
        documentMap.put("returnCode", commodityRequest.getCommodityDeliveryStatus().getReturnCode());
        documentMap.put("deliveryStatus", commodityRequest.getDeliveryStatus());
        documentMap.put("returnCodeDetails", commodityRequest.getCommodityDeliveryStatus());
         
      }
      log.debug("BDRSinkConnector.getDocumentMap: map computed, contents are="+documentMap.toString());
      return documentMap;
    }
    
    public void putAlternateIDs(String subscriberID, Map<String,Object> map)
    {
      try
        {
          log.info("subscriberID : " + subscriberID);
          SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
          log.info("subscriber profile : " + subscriberProfile);
          for (AlternateID alternateID : Deployment.getAlternateIDs().values())
            {
              String methodName;
              if ("msisdn".equals(alternateID.getID())) // special case
                {
                  methodName = "getMSISDN";
                }
              else
                {
                  methodName = "get" + StringUtils.capitalize(alternateID.getID());
                }
              log.info("method " + methodName);
              try
              {
                Method method = subscriberProfile.getClass().getMethod(methodName);
                Object alternateIDValue = method.invoke(subscriberProfile);
                //if (log.isTraceEnabled())
                  log.info("adding " + alternateID.getID() + " with " + alternateIDValue);
                map.put(alternateID.getID(), alternateIDValue);
              }
              catch (NoSuchMethodException|SecurityException|IllegalAccessException|IllegalArgumentException|InvocationTargetException e)
                {
                  log.warn("Problem retrieving alternateID " + alternateID.getID() + " : " + e.getLocalizedMessage());
                }
            }
        }
      catch (SubscriberProfileServiceException e1)
        {
          log.warn("Cannot resolve " + subscriberID);
        }
      }
  }

}
