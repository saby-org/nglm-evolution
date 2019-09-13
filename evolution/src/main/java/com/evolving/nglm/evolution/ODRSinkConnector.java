package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.OfferService.OfferListener;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentStatus;

public class ODRSinkConnector extends SimpleESSinkConnector
{
  
  private static OfferService offerService;
  private static ProductService productService;
  
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
    
      //
      //  services
      //
   
      offerService = new OfferService(Deployment.getBrokerServers(), "ordsinkconnector-offerservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getOfferTopic(), false);
      offerService.start();
      
      productService = new ProductService(Deployment.getBrokerServers(), "ordsinkconnector-productservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getProductTopic(), false);
      productService.start();
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
        documentMap.put("purchaseID", purchaseManager.getEventID());
        documentMap.put("eventDatetime", purchaseManager.getEventDate());
        documentMap.put("offerID", purchaseManager.getOfferID());
        documentMap.put("offerQty", purchaseManager.getQuantity());
        documentMap.put("salesChannelID", purchaseManager.getSalesChannelID());
        if(offer != null){
          if(offer.getOfferSalesChannelsAndPrices() != null){
            for(OfferSalesChannelsAndPrice channel : offer.getOfferSalesChannelsAndPrices()){
              if(channel.getSalesChannelIDs() != null) {
                for(String salesChannelID : channel.getSalesChannelIDs()) {
                  if(salesChannelID.equals(purchaseManager.getSalesChannelID())) {
                    documentMap.put("offerPrice", channel.getPrice().getAmount());
                  }
                }
              }
            }
          }
          documentMap.put("offerStock", offer.getStock());
          StringBuilder sb = new StringBuilder();
          if(offer.getOfferProducts() != null) {
            for(OfferProduct offerProduct : offer.getOfferProducts()) {
              Product product = (Product) productService.getStoredProduct(offerProduct.getProductID());
              sb.append(product!=null?product.getDisplay():offerProduct.getProductID()).append(";").append(offerProduct.getQuantity()).append(",");
            }
          }
          String offerContent = sb.toString().substring(0, sb.toString().length()-1);
          documentMap.put("offerContent", offerContent);
        }
        documentMap.put("moduleID", purchaseManager.getModuleID());
        documentMap.put("featureID", purchaseManager.getFeatureID());
        documentMap.put("origin", purchaseManager.getDeliveryRequestSource());
        documentMap.put("responseCode", purchaseManager.getReturnCode());
        documentMap.put("deliveryStatus", purchaseManager.getDeliveryStatus());
        documentMap.put("responseMessage", PurchaseFulfillmentStatus.fromReturnCode(purchaseManager.getReturnCode()));
        documentMap.put("voucherCode", "");
        documentMap.put("voucherPartnerID", "");
      }
      
      return documentMap;
    }
  }
}

