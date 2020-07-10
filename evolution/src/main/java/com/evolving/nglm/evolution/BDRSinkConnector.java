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
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryRequest;


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
        SinkConnectorUtils.putAlternateIDs(commodityRequest.getAlternateIDs(), documentMap);
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
        documentMap.put("returnCodeDetails", commodityRequest.getStatusMessage());
      }
      log.debug("BDRSinkConnector.getDocumentMap: map computed, contents are="+documentMap.toString());
      return documentMap;
    }
    

  }

}
