package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryRequest;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryStatus;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentStatus;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentStatus;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.SystemTime;

public class BDRSinkConnector extends SimpleESSinkConnector
{
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
      log.info("BDRSinkConnector.getDocumentMap: computing map to give to elastic search");
      Object commodityRequestValue = sinkRecord.value();
      Schema commodityRequestValueSchema = sinkRecord.valueSchema();
      CommodityDeliveryRequest commodityRequest = CommodityDeliveryRequest.unpack(new SchemaAndValue(commodityRequestValueSchema, commodityRequestValue));
      Map<String,Object> documentMap = null;
      
      if(commodityRequest != null){
        Date expirationDate = EvolutionUtilities.addTime(SystemTime.getCurrentTime(), commodityRequest.getValidityPeriodQuantity(), commodityRequest.getValidityPeriodType(), Deployment.getBaseTimeZone());
        
        documentMap = new HashMap<String,Object>();
        documentMap.put("subscriberID", commodityRequest.getSubscriberID());
        documentMap.put("eventDatetime", commodityRequest.getEventDate());
        documentMap.put("deliverableExpiration", expirationDate);
        documentMap.put("providerID", commodityRequest.getProviderID());
        documentMap.put("deliverableID", commodityRequest.getCommodityID());
        documentMap.put("deliverableQty", commodityRequest.getAmount());
        documentMap.put("operation", commodityRequest.getOperation().toString().toUpperCase());
        documentMap.put("moduleID", commodityRequest.getModuleID());
        documentMap.put("featureID", commodityRequest.getFeatureID());
        documentMap.put("origin", commodityRequest.getDeliveryRequestSource());
        documentMap.put("responseCode", commodityRequest.getCommodityDeliveryStatus().getReturnCode());
        documentMap.put("deliveryStatus", commodityRequest.getDeliveryStatus());
        documentMap.put("responseMessage", commodityRequest.getCommodityDeliveryStatus());
         
      }
      log.info("BDRSinkConnector.getDocumentMap: map computed, contents are="+documentMap.toString());
      return documentMap;
    }
    
  }

}
