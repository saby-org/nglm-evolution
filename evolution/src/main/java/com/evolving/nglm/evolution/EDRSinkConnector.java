package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentStatus;

public class EDRSinkConnector extends SimpleESSinkConnector
{
  private static DynamicCriterionFieldService dynamicCriterionFieldService;
  private static OfferService offerService;
  private static ProductService productService;
  private static VoucherService voucherService;
  private static PaymentMeanService paymentMeanService;
  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<ODRSinkConnectorTask> taskClass()
  {
    return ODRSinkConnectorTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class ODRSinkConnectorTask extends StreamESSinkTask<PurchaseFulfillmentRequest>
  {
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
   
      dynamicCriterionFieldService = new DynamicCriterionFieldService(Deployment.getBrokerServers(), "odrsinkconnector-dynamiccriterionfieldservice-" + getTaskNumber(), Deployment.getDynamicCriterionFieldTopic(), false);
      CriterionContext.initialize(dynamicCriterionFieldService);
      dynamicCriterionFieldService.start();      
      
      offerService = new OfferService(Deployment.getBrokerServers(), "odrsinkconnector-offerservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getOfferTopic(), false);
      offerService.start();
      
      productService = new ProductService(Deployment.getBrokerServers(), "odrsinkconnector-productservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getProductTopic(), false);
      productService.start();

      voucherService = new VoucherService(Deployment.getBrokerServers(), "odrsinkconnector-voucherservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getVoucherTopic());
      voucherService.start();

      paymentMeanService = new PaymentMeanService(Deployment.getBrokerServers(), "odrsinkconnector-paymentmeanservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getPaymentMeanTopic(), false);
      paymentMeanService.start();
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

      offerService.stop();
      productService.stop();
      voucherService.stop();
      paymentMeanService.stop();
      
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
    
    @Override public PurchaseFulfillmentRequest unpackRecord(SinkRecord sinkRecord) 
    {
      Object purchaseManagertValue = sinkRecord.value();
      Schema purchaseManagerValueSchema = sinkRecord.valueSchema();
      return PurchaseFulfillmentRequest.unpack(new SchemaAndValue(purchaseManagerValueSchema, purchaseManagertValue)); 
    }

    /*****************************************
    *
    *  getDocumentIndexName
    *
    *****************************************/
    
    @Override
    protected String getDocumentIndexName(PurchaseFulfillmentRequest purchaseManager)
    {
      String timeZone = DeploymentCommon.getDeployment(purchaseManager.getTenantID()).getTimeZone();
      return this.getDefaultIndexName() + RLMDateUtils.formatDateISOWeek(purchaseManager.getEventDate(), timeZone);
    }
    
    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override
    public Map<String, Object> getDocumentMap(PurchaseFulfillmentRequest purchaseManager)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();
      documentMap.put("subscriberID", purchaseManager.getSubscriberID());
      documentMap.put("eventDatetime", RLMDateUtils.formatDateForElasticsearchDefault(SystemTime.getCurrentTime())); //RAJ K TO DO
      documentMap.put("eventID", purchaseManager.getEventID());
      documentMap.put("eventDetails", purchaseManager.getEventName());
      return documentMap;
    }
  }
}

