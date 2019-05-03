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
        documentMap.put("subscriberID", inFulfillment.getSubscriberID());
        documentMap.put("deliveryRequestID", inFulfillment.getDeliveryRequestID());
        documentMap.put("eventID", inFulfillment.getEventID());
        documentMap.put("eventDatetime", inFulfillment.getEventDate());
        documentMap.put("providerID", inFulfillment.getCorrelator());
        documentMap.put("deliverableID", inFulfillment.getPaymentMeanID());
        documentMap.put("deliverableQty", inFulfillment.getAmount());
        documentMap.put("operation", inFulfillment.getOperation().toString());
        documentMap.put("moduleID", inFulfillment.getModuleID());
        documentMap.put("featureID", inFulfillment.getFeatureID());
        documentMap.put("origin", "");
        documentMap.put("returnCode", inFulfillment.getReturnCode());
        documentMap.put("deliveryStatus", inFulfillment.getDeliveryStatus());
        documentMap.put("returnCodeDetails", inFulfillment.getReturnCodeDetails());
         
      }
      log.info("BDRSingConnector.getDocumentMap: map computed, contents are="+documentMap.toString());
      return documentMap;
    }
    
  }

}
