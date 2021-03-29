package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryRequest;


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
  
  public static class BDRSinkConnectorTask extends StreamESSinkTask<CommodityDeliveryRequest>
  {
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
    *  unpackRecord
    *
    *****************************************/
    
    @Override public CommodityDeliveryRequest unpackRecord(SinkRecord sinkRecord) 
    {
      log.debug("BDRSinkConnector.unpackRecord: extract CommodityDeliveryRequest");
      Object commodityRequestValue = sinkRecord.value();
      Schema commodityRequestValueSchema = sinkRecord.valueSchema();
      return CommodityDeliveryRequest.unpack(new SchemaAndValue(commodityRequestValueSchema, commodityRequestValue));
    }
    
    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override
    public Map<String, Object> getDocumentMap(CommodityDeliveryRequest commodityRequest)
    {

      if(commodityRequest.getOriginatingSubscriberID() != null && commodityRequest.getOriginatingSubscriberID().startsWith(DeliveryManager.TARGETED))
        {
          // case where this is a delegated request and its response is for the original subscriberID, so this response must be ignored.
          return null;
        }

      Map<String,Object> documentMap = new HashMap<String,Object>();
      documentMap.put("subscriberID", commodityRequest.getSubscriberID());
      SinkConnectorUtils.putAlternateIDs(commodityRequest.getAlternateIDs(), documentMap);
      documentMap.put("tenantID", commodityRequest.getTenantID());
      documentMap.put("eventDatetime", commodityRequest.getEventDate()!=null? RLMDateUtils.formatDateForElasticsearchDefault(commodityRequest.getEventDate()):"");
      documentMap.put("deliveryRequestID", commodityRequest.getDeliveryRequestID());
      documentMap.put("originatingDeliveryRequestID", commodityRequest.getOriginatingDeliveryRequestID());
      documentMap.put("eventID", commodityRequest.getEventID());
      documentMap.put("deliverableExpirationDate", commodityRequest.getDeliverableExpirationDate(commodityRequest.getTenantID()));
      documentMap.put("providerID", commodityRequest.getProviderID());
      documentMap.put("deliverableID", commodityRequest.getCommodityID());
      documentMap.put("deliverableQty", commodityRequest.getAmount());
      documentMap.put("operation", commodityRequest.getOperation().toString().toUpperCase());
      documentMap.put("moduleID", commodityRequest.getModuleID());
      documentMap.put("featureID", commodityRequest.getFeatureID());
      documentMap.put("origin", commodityRequest.getOrigin());
      documentMap.put("returnCode", commodityRequest.getCommodityDeliveryStatus().getReturnCode());
      documentMap.put("returnCodeDetails", commodityRequest.getStatusMessage());
      documentMap.put("creationDate", commodityRequest.getCreationDate()!=null?dateFormat.format(commodityRequest.getCreationDate()):"");
        
      log.debug("BDRSinkConnector.getDocumentMap: map computed, contents are="+documentMap.toString());
      return documentMap;
    }
  }
}
