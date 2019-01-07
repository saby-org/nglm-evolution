package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;
import com.evolving.nglm.evolution.purchase.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.purchase.PurchaseFulfillmentManager.PurchaseFulfillmentStatus;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferSalesChannelsAndPrice;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.OfferService.OfferListener;

public class ODRSinkConnector extends SimpleESSinkConnector
{
  
  private static OfferService offerService;
  
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
  
  public static class ODRSinkConnectorTask extends StreamESSinkTask
  {

    /****************************************
    *
    *  attributes
    *
    ****************************************/
    

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
      
      OfferListener offerListener = new OfferListener()
      {
        @Override
        public void offerDeactivated(String guiManagedOfferID)
        {
           // must record the offer start date and initial propensity into the table 
           // TODO Auto-generated method stub
        }
        @Override
        public void offerActivated(Offer offer)
        {
          // TODO Auto-generated method stub
        }
      };
      offerService = new OfferService(Deployment.getBrokerServers(), "example-offerservice-673", Deployment.getOfferTopic(), false, offerListener);
      offerService.start();

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

      Object purchaseManagertValue = sinkRecord.value();
      Schema purchaseManagerValueSchema = sinkRecord.valueSchema();
      PurchaseFulfillmentRequest purchaseManager = PurchaseFulfillmentRequest.unpack(new SchemaAndValue(purchaseManagerValueSchema, purchaseManagertValue)); 

      Date now = SystemTime.getCurrentTime();
      Offer offer = offerService.getActiveOffer(purchaseManager.getOfferID(), now);
      Map<String,Object> documentMap = null;

      if(purchaseManager != null){
        documentMap = new HashMap<String,Object>();
        documentMap.put("customer_id", purchaseManager.getSubscriberID());
        documentMap.put("event_id", purchaseManager.getEventID());
        documentMap.put("event_datetime", purchaseManager.getEventDate());
        documentMap.put("purchase_id", purchaseManager.getEventID());
        documentMap.put("offer_id", purchaseManager.getOfferID());
        documentMap.put("offer_qty", purchaseManager.getQuantity());
        if(offer != null){
          if(offer.getOfferSalesChannelsAndPrices() != null){
            for(OfferSalesChannelsAndPrice channel : offer.getOfferSalesChannelsAndPrices()){
              documentMap.put("sales_channel_id", channel.getSalesChannelIDs());
              documentMap.put("offer_price", channel.getPrice().getAmount());
            }
          }
          documentMap.put("offer_stock", "");
          documentMap.put("offer_content", offer.getOfferProducts().toString());
        }
        documentMap.put("module_id", purchaseManager.getModuleID());
        documentMap.put("feature_id", purchaseManager.getFeatureID());
        documentMap.put("origin", purchaseManager.getDeliveryRequestSource());
        documentMap.put("return_code", purchaseManager.getReturnCode());
        documentMap.put("delivery_status", purchaseManager.getDeliveryStatus());
        documentMap.put("return_code_details", purchaseManager.getReturnCodeDetails());
        documentMap.put("voucher_code", "");
        documentMap.put("voucher_partner_id", "");
      }
      
      return documentMap;
    }
  }
}

