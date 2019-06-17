package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryRequest;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentStatus;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;

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
      *  extract INFulfillmentRequest
      *
      *******************************************/
      log.info("BDRSingConnector.getDocumentMap: computing map to give to elastic search");
      Object commodityRequestValue = sinkRecord.value();
      Schema commodityRequestValueSchema = sinkRecord.valueSchema();
      CommodityDeliveryRequest commodityRequest = CommodityDeliveryRequest.unpack(new SchemaAndValue(commodityRequestValueSchema, commodityRequestValue));
      
      Map<String,Object> documentMap = null;

      if(commodityRequest != null){
        documentMap = new HashMap<String,Object>();
        documentMap.put("subscriberID", commodityRequest.getSubscriberID());
        documentMap.put("deliveryRequestID", commodityRequest.getDeliveryRequestID());
        documentMap.put("eventID", commodityRequest.getEventID());
        documentMap.put("eventDatetime", commodityRequest.getEventDate());
        documentMap.put("providerID", commodityRequest.getCorrelator());
        documentMap.put("deliverableID", commodityRequest.getDeliveryRequestID());
        documentMap.put("deliverableQty", commodityRequest.getAmount());
        documentMap.put("operation", commodityRequest.getOperation().toString());
        documentMap.put("moduleID", commodityRequest.getModuleID());
        documentMap.put("featureID", commodityRequest.getFeatureID());
        documentMap.put("origin", "");
        documentMap.put("returnCode", commodityRequest.getCommodityDeliveryStatus().getReturnCode());
        documentMap.put("deliveryStatus", commodityRequest.getDeliveryStatus());
        documentMap.put("returnCodeDetails", commodityRequest.getCommodityDeliveryStatus().toString());
         
      }
      log.info("BDRSingConnector.getDocumentMap: map computed, contents are="+documentMap.toString());
      return documentMap;
    }
    
  }

}
