package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      Object inFulfillmentValue = sinkRecord.value();
      Schema inFulfillmentValueSchema = sinkRecord.valueSchema();
      INFulfillmentRequest inFulfillment = INFulfillmentRequest.unpack(new SchemaAndValue(inFulfillmentValueSchema, inFulfillmentValue));
      
      Map<String,Object> documentMap = null;

      if(inFulfillment != null){
        documentMap = new HashMap<String,Object>();
        documentMap.put("customer_id", inFulfillment.getSubscriberID());
        documentMap.put("delivery_request_id", inFulfillment.getDeliveryRequestID());
        documentMap.put("eventID", inFulfillment.getEventID());
        documentMap.put("event_datetime", inFulfillment.getEventDate());
        documentMap.put("provider_id", inFulfillment.getCorrelator());
        documentMap.put("deliverable_id", inFulfillment.getPaymentMeanID());
        documentMap.put("deliverable_qty", inFulfillment.getAmount());
        documentMap.put("operation", inFulfillment.getOperation().toString());
        documentMap.put("module_id", inFulfillment.getModuleID());
        documentMap.put("feature_id", inFulfillment.getFeatureID());
        documentMap.put("origin", "");
        documentMap.put("return_code", inFulfillment.getReturnCode());
        documentMap.put("delivery_status", inFulfillment.getDeliveryStatus());
        documentMap.put("return_code_details", inFulfillment.getReturnCodeDetails());
         
      }
      log.info("BDRSingConnector.getDocumentMap: map computed, contents are="+documentMap.toString());
      return documentMap;
    }
    
  }

}
