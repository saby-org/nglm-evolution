package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;

/**
 * Push a RewardManager as a BDR. 
 * (Used in BLK for instance)
 */
public class BDRRewardManagerSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<BDRRewardManagerSinkConnectorTask> taskClass()
  {
    return BDRRewardManagerSinkConnectorTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class BDRRewardManagerSinkConnectorTask extends StreamESSinkTask<RewardManagerRequest>
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

    @Override
    protected String getDocumentIndexName(RewardManagerRequest commodityRequest)
    {
      String timeZone = DeploymentCommon.getDeployment(commodityRequest.getTenantID()).getTimeZone();
      return this.getDefaultIndexName() + RLMDateUtils.formatDateISOWeek(commodityRequest.getEventDate(), timeZone);
    }
    
    /*****************************************
    *
    *  unpackRecord
    *
    *****************************************/
    
    @Override public RewardManagerRequest unpackRecord(SinkRecord sinkRecord) 
    {
      log.debug("BDRSinkConnector.getDocumentMap: computing map to give to elastic search");
      Object rewardManagerRequestValue = sinkRecord.value();
      Schema rewardManagerRequestValueSchema = sinkRecord.valueSchema();
      return RewardManagerRequest.unpack(new SchemaAndValue(rewardManagerRequestValueSchema, rewardManagerRequestValue));
    }
    
    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override
    public Map<String, Object> getDocumentMap(RewardManagerRequest commodityRequest)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();
      documentMap.put("subscriberID", commodityRequest.getSubscriberID());
      SinkConnectorUtils.putAlternateIDs(commodityRequest.getAlternateIDs(), documentMap);
      documentMap.put("tenantID", commodityRequest.getTenantID());
      documentMap.put("eventDatetime", commodityRequest.getEventDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(commodityRequest.getEventDate()):"");
      documentMap.put("deliveryRequestID", commodityRequest.getDeliveryRequestID());
      documentMap.put("originatingDeliveryRequestID", commodityRequest.getOriginatingDeliveryRequestID());
      documentMap.put("eventID", commodityRequest.getEventID());
      documentMap.put("deliverableExpirationDate", RLMDateUtils.formatDateForElasticsearchDefault(commodityRequest.getDeliverableExpirationDate()));
      documentMap.put("providerID", commodityRequest.getProviderID());
      documentMap.put("deliverableID", commodityRequest.getDeliverableID());
      documentMap.put("deliverableQty", (int) commodityRequest.getAmount()); // deliverableQty is typed as integer in ES
      documentMap.put("operation", CommodityDeliveryOperation.Credit);  //hardcoded also in RewM request
      documentMap.put("moduleID", commodityRequest.getModuleID());
      documentMap.put("featureID", commodityRequest.getFeatureID());
      documentMap.put("origin", "");
      documentMap.put("returnCode", commodityRequest.getReturnCode());
      documentMap.put("deliveryStatus", commodityRequest.getDeliveryStatus());
      documentMap.put("creationDate", commodityRequest.getCreationDate() != null ? RLMDateUtils.formatDateForElasticsearchDefault(commodityRequest.getCreationDate()) : "");
      documentMap.put("returnCodeDetails", commodityRequest.getReturnCodeDetails()); // returnCodeDetails is typed as keyword in ES

      log.debug("BDRSinkConnector.getDocumentMap: map computed, contents are="+documentMap.toString());
      return documentMap;
    }
  }
}
