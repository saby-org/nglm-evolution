package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.OfferService.OfferListener;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;

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
        documentMap.put("subscriberID", purchaseManager.getSubscriberID());
        documentMap.put("eventID", purchaseManager.getEventID());
        documentMap.put("eventDatetime", purchaseManager.getEventDate());
        documentMap.put("purchaseID", purchaseManager.getEventID());
        documentMap.put("offerID", purchaseManager.getOfferID());
        documentMap.put("offerQty", purchaseManager.getQuantity());
        if(offer != null){
          if(offer.getOfferSalesChannelsAndPrices() != null){
            for(OfferSalesChannelsAndPrice channel : offer.getOfferSalesChannelsAndPrices()){
              documentMap.put("salesChannelID", channel.getSalesChannelIDs());
              documentMap.put("offerPrice", channel.getPrice().getAmount());
            }
          }
          documentMap.put("offerStock", "");
          documentMap.put("offerContent", offer.getOfferProducts().toString());
        }
        documentMap.put("moduleID", purchaseManager.getModuleID());
        documentMap.put("featureID", purchaseManager.getFeatureID());
        documentMap.put("origin", purchaseManager.getDeliveryRequestSource());
        documentMap.put("returnCode", purchaseManager.getReturnCode());
        documentMap.put("deliveryStatus", purchaseManager.getDeliveryStatus());
        documentMap.put("returnCodeDetails", purchaseManager.getReturnCodeDetails());
        documentMap.put("voucherCode", "");
        documentMap.put("voucherPartnerID", "");
      }
      
      return documentMap;
    }
  }
}

