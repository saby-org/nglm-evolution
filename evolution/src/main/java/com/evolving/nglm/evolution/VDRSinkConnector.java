/****************************************************************************
*
*  VDRSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class VDRSinkConnector extends SimpleESSinkConnector
{
  private static SegmentationDimensionService segmentationDimensionService;
  private static ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return VDRSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class VDRSinkTask extends StreamESSinkTask<VoucherChange>
  {
    public static final String ES_FIELD_SUBSCRIBER_ID = "subscriberID";
    public static final String ES_FIELD_VOUCHER_CODE = "voucherCode";
    

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

      subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("vdrsinkconnector-subscriberGroupEpoch", Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
   
      segmentationDimensionService = new SegmentationDimensionService(Deployment.getBrokerServers(), "vdrsinkconnector-segmentationDimensionservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getSegmentationDimensionTopic(), false);
      segmentationDimensionService.start();
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

      segmentationDimensionService.stop();
      
      //
      //  super
      //

      super.stop();
    }


    @Override public VoucherChange unpackRecord(SinkRecord sinkRecord) 
    {
      Object voucherChangeValue = sinkRecord.value();
      Schema voucherChangeValueSchema = sinkRecord.valueSchema();
      return VoucherChange.unpack(new SchemaAndValue(voucherChangeValueSchema, voucherChangeValue));
    }
    
    @Override
    protected String getDocumentIndexName(VoucherChange voucherChange)
    {
      String timeZone = DeploymentCommon.getDeployment(voucherChange.getTenantID()).getTimeZone();
      return this.getDefaultIndexName() + RLMDateUtils.formatDateISOWeek(voucherChange.getEventDate(), timeZone);
    }

    @Override public Map<String,Object> getDocumentMap(VoucherChange voucherChange)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();
      
      documentMap.put(ES_FIELD_VOUCHER_CODE, voucherChange.getVoucherCode());
      documentMap.put(ES_FIELD_SUBSCRIBER_ID, voucherChange.getSubscriberID());
      SinkConnectorUtils.putAlternateIDs(voucherChange.getAlternateIDs(), documentMap);
      documentMap.put("tenantID", voucherChange.getTenantID());
      documentMap.put("voucherID", voucherChange.getVoucherID());
      documentMap.put("action", voucherChange.getAction());
      documentMap.put("eventDatetime", voucherChange.getEventDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(voucherChange.getEventDate()):"");
      documentMap.put("eventID", voucherChange.getEventID());
      documentMap.put("returnCode", voucherChange.getReturnStatus().getGenericResponseCode());
      documentMap.put("returnCodeDetails", voucherChange.getReturnStatus().getGenericResponseMessage());
      documentMap.put("origin", voucherChange.getOrigin());
      documentMap.put("moduleID", voucherChange.getModuleID());
      documentMap.put("featureID", voucherChange.getFeatureID()); 
      documentMap.put("expiryDate", RLMDateUtils.formatDateForElasticsearchDefault(voucherChange.getNewVoucherExpiryDate())); 
      documentMap.put("stratum", voucherChange.getStatisticsSegmentsMap(subscriberGroupEpochReader, segmentationDimensionService));
      documentMap.put("fileID", voucherChange.getFileID());
      documentMap.put("offerID", voucherChange.getOfferID());
      documentMap.put("deliveryRequestID", voucherChange.getDeliveryRequestID() != null ? voucherChange.getDeliveryRequestID() : "");

      return documentMap;
    }
  }
}
