package com.evolving.nglm.evolution;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
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
  private static PaymentMeanService paymentMeanService;
  private static String elasticSearchDateFormat = Deployment.getElasticSearchDateFormat();
  private static DateFormat dateFormat = new SimpleDateFormat(elasticSearchDateFormat);
  
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
      
      paymentMeanService = new PaymentMeanService(Deployment.getBrokerServers(), "ordsinkconnector-paymentmeanservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getPaymentMeanTopic(), false);
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
        documentMap.put("deliveryRequestID", purchaseManager.getDeliveryRequestID());
        documentMap.put("originatingDeliveryRequestID", purchaseManager.getOriginatingDeliveryRequestID());
        documentMap.put("eventDatetime", purchaseManager.getEventDate()!=null?dateFormat.format(purchaseManager.getEventDate()):"");
        documentMap.put("eventID", purchaseManager.getEventID());
        documentMap.put("offerID", purchaseManager.getOfferID());
        documentMap.put("offerQty", purchaseManager.getQuantity());
        documentMap.put("salesChannelID", purchaseManager.getSalesChannelID());
        if(offer != null){
          if(offer.getOfferSalesChannelsAndPrices() != null){
            for(OfferSalesChannelsAndPrice channel : offer.getOfferSalesChannelsAndPrices()){
              if(channel.getSalesChannelIDs() != null) {
                for(String salesChannelID : channel.getSalesChannelIDs()) {
                  if(salesChannelID.equals(purchaseManager.getSalesChannelID())) {
                    PaymentMean paymentMean = (PaymentMean) paymentMeanService.getStoredPaymentMean(channel.getPrice().getPaymentMeanID());
                    if(paymentMean != null) {
                      documentMap.put("offerPrice", channel.getPrice().getAmount());
                      documentMap.put("meanOfPayment", paymentMean.getDisplay());
                    }
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
              sb.append(offerProduct.getQuantity()+" ").append(product!=null?product.getDisplay():offerProduct.getProductID()).append(",");
            }
          }
          String offerContent = sb.toString().substring(0, sb.toString().length()-1);
          documentMap.put("offerContent", offerContent);
        }
        documentMap.put("moduleID", purchaseManager.getModuleID());
        documentMap.put("featureID", purchaseManager.getFeatureID());
        documentMap.put("origin", purchaseManager.getOrigin());
        documentMap.put("returnCode", purchaseManager.getReturnCode());
        documentMap.put("deliveryStatus", purchaseManager.getDeliveryStatus());
        documentMap.put("returnCodeDetails", PurchaseFulfillmentStatus.fromReturnCode(purchaseManager.getReturnCode()));
        documentMap.put("voucherCode", "");
        documentMap.put("voucherPartnerID", "");
      }
      
      return documentMap;
    }
  }
}

