package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.RLMDateUtils;
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
  
  public static class BDRSinkConnectorTask extends StreamESSinkTask<BonusDelivery>
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

    // closely duplicated in com.evolving.nglm.evolution.NotificationSinkConnector.NotificationSinkConnectorTask.unpackRecord()
    @Override public BonusDelivery unpackRecord(SinkRecord sinkRecord)
    {
      Object commodityValue = sinkRecord.value();
      Schema commodityValueSchema = sinkRecord.valueSchema();

      Struct valueStruct = (Struct) commodityValue;
      String type = valueStruct.getString("deliveryType");

      //  safety guard - return null
      if(type == null || type.equals("") || DeploymentCommon.getDeliveryManagers().get(type)==null ) {
        return null;
      }

      return (BonusDelivery) DeploymentCommon.getDeliveryManagers().get(type).getRequestSerde().unpack(new SchemaAndValue(commodityValueSchema, commodityValue));

    }
    
    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override
    public Map<String, Object> getDocumentMap(BonusDelivery commodityRequest)
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
      documentMap.put("eventDatetime", commodityRequest.getDeliveryDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(commodityRequest.getDeliveryDate()):"");
      documentMap.put("deliveryRequestID", commodityRequest.getDeliveryRequestID());
      documentMap.put("originatingDeliveryRequestID", commodityRequest.getOriginatingDeliveryRequestID());
      documentMap.put("eventID", commodityRequest.getEventID());
      documentMap.put("deliverableExpirationDate", RLMDateUtils.formatDateForElasticsearchDefault(commodityRequest.getBonusDeliveryDeliverableExpirationDate()));
      documentMap.put("providerID", commodityRequest.getBonusDeliveryProviderId());
      documentMap.put("deliverableID", commodityRequest.getBonusDeliveryDeliverableId());
      documentMap.put("deliverableQty", commodityRequest.getBonusDeliveryDeliverableQty());
      documentMap.put("operation", commodityRequest.getBonusDeliveryOperation().toUpperCase());
      documentMap.put("moduleID", commodityRequest.getModuleID());
      documentMap.put("featureID", commodityRequest.getFeatureID());
      documentMap.put("origin", commodityRequest.getBonusDeliveryOrigin());
      documentMap.put("returnCode", commodityRequest.getBonusDeliveryReturnCode());
      documentMap.put("returnCodeDetails", commodityRequest.getBonusDeliveryReturnCodeDetails());
      documentMap.put("creationDate", commodityRequest.getCreationDate() != null ? RLMDateUtils.formatDateForElasticsearchDefault(commodityRequest.getCreationDate()) : "");
        
      if(log.isDebugEnabled()) log.debug("BDRSinkConnector.getDocumentMap: map computed, contents are="+documentMap.toString());
      return documentMap;
    }
  }
}
