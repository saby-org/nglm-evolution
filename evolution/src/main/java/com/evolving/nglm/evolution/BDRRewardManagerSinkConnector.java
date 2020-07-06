package com.evolving.nglm.evolution;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;

public class BDRRewardManagerSinkConnector extends SimpleESSinkConnector
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

    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(BDRRewardManagerSinkConnector.class);

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
      *  extract CommodityDeliveryRequest
      *
      *******************************************/
      log.debug("BDRSinkConnector.getDocumentMap: computing map to give to elastic search");
      Object rewardManagerRequestValue = sinkRecord.value();
      Schema rewardManagerRequestValueSchema = sinkRecord.valueSchema();
      RewardManagerRequest commodityRequest = RewardManagerRequest.unpack(new SchemaAndValue(rewardManagerRequestValueSchema, rewardManagerRequestValue));
      Map<String,Object> documentMap = null;
      
      if(commodityRequest != null){
        
        documentMap = new HashMap<String,Object>();
        documentMap.put("subscriberID", commodityRequest.getSubscriberID());
        documentMap.put("eventDatetime", commodityRequest.getEventDate()!=null?dateFormat.format(commodityRequest.getEventDate()):"");
        documentMap.put("deliveryRequestID", commodityRequest.getDeliveryRequestID());
        documentMap.put("originatingDeliveryRequestID", commodityRequest.getOriginatingDeliveryRequestID());
        documentMap.put("eventID", commodityRequest.getEventID());
        documentMap.put("deliverableExpirationDate", null); // always null for RM ?
        documentMap.put("providerID", commodityRequest.getProviderID());
        documentMap.put("deliverableID", commodityRequest.getDeliverableID());
        documentMap.put("deliverableQty", commodityRequest.getAmount());
        documentMap.put("operation", CommodityDeliveryOperation.Credit.toString().toUpperCase()); // Always credit for RW ?
        documentMap.put("moduleID", commodityRequest.getModuleID());
        documentMap.put("featureID", commodityRequest.getFeatureID());
        documentMap.put("origin", "");
        documentMap.put("returnCode", commodityRequest.getReturnCode());
        documentMap.put("deliveryStatus", commodityRequest.getDeliveryStatus());
        documentMap.put("returnCodeDetails", commodityRequest.getReturnCode());
         
      }
      log.debug("BDRSinkConnector.getDocumentMap: map computed, contents are="+documentMap.toString());
      return documentMap;
    }
    
  }

}
