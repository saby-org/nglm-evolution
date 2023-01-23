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
import org.json.simple.JSONObject;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentStatus;
import com.evolving.nglm.evolution.complexobjects.ComplexObjectTypeService;

public class ODRSinkConnector extends SimpleESSinkConnector
{
  private static DynamicCriterionFieldService dynamicCriterionFieldService;
  private static OfferService offerService;
  private static ProductService productService;
  private static VoucherService voucherService;
  private static PaymentMeanService paymentMeanService;
  private static SegmentationDimensionService segmentationDimensionService;
  private static ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  
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

      subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("odrsinkconnector-subscriberGroupEpoch", Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
   
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
      
      segmentationDimensionService = new SegmentationDimensionService(Deployment.getBrokerServers(), "odrsinkconnector-segmentationDimensionservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getSegmentationDimensionTopic(), false);
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

      offerService.stop();
      productService.stop();
      voucherService.stop();
      paymentMeanService.stop();
      segmentationDimensionService.stop();
      
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
      Date now = SystemTime.getCurrentTime();
      Offer offer = offerService.getActiveOffer(purchaseManager.getOfferID(), now);

      List<Map<String, Object>> voucherList = new ArrayList<>();
      Map<String, Object> metadataJSON = new HashMap<String, Object>();
      
      Map<String,Object> documentMap = new HashMap<String,Object>();
      documentMap.put("subscriberID", purchaseManager.getSubscriberID());
      SinkConnectorUtils.putAlternateIDs(purchaseManager.getAlternateIDs(), documentMap);
      documentMap.put("tenantID", purchaseManager.getTenantID());
      documentMap.put("deliveryRequestID", purchaseManager.getDeliveryRequestID());
      documentMap.put("originatingDeliveryRequestID", purchaseManager.getOriginatingDeliveryRequestID());
      documentMap.put("eventDatetime", purchaseManager.getEventDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(purchaseManager.getEventDate()):"");
      documentMap.put("eventID", purchaseManager.getEventID());
      documentMap.put("offerID", purchaseManager.getOfferID());
      documentMap.put("offerQty", purchaseManager.getQuantity());
      documentMap.put("salesChannelID", purchaseManager.getSalesChannelID());
     
        if (offer != null)
          {
            metadataJSON.put("cancellable", offer.getCancellable());
            if (offer.getOfferSalesChannelsAndPrices() != null)
              {
                for (OfferSalesChannelsAndPrice channel : offer.getOfferSalesChannelsAndPrices())
                  {
                    if (channel.getSalesChannelIDs() != null)
                      {
                        for (String salesChannelID : channel.getSalesChannelIDs())
                          {
                            if (salesChannelID.equals(purchaseManager.getSalesChannelID()))
                              {
                                metadataJSON.put("salesChannelID", salesChannelID);
                                OfferPrice price = channel.getPrice();
                                if (price != null)
                                  {
                                    metadataJSON.put("offerPrice", price.getJSONRepresentation());
                                    PaymentMean paymentMean = (PaymentMean) paymentMeanService.getStoredPaymentMean(price.getPaymentMeanID());
                                    if (paymentMean != null)
                                      {
                                        documentMap.put("offerPrice", price.getAmount());
                                        documentMap.put("meanOfPayment", paymentMean.getDisplay());
                                      }
                                  }
                              }
                          }
                      }
                  }
              }
        documentMap.put("offerStock", offer.getStock());
        StringBuilder sb = new StringBuilder();
        if (offer.getOfferProducts() != null)
          {
            List<JSONObject> offerProducts = new ArrayList<JSONObject>();
            for (OfferProduct offerProduct : offer.getOfferProducts())
              {
                offerProducts.add(offerProduct.getJSONRepresentation());
                Product product = (Product) productService.getStoredProduct(offerProduct.getProductID());
                sb.append(offerProduct.getQuantity() + " ").append(product != null ? product.getDisplay() : "product" + offerProduct.getProductID()).append(",");
              }
            metadataJSON.put("offerProducts", JSONUtilities.encodeArray(offerProducts));
          }
          if (purchaseManager.getVoucherDeliveries() != null)
            {
              // StringBuilder voucherCodeSb=new StringBuilder("");//ready for
              // several vouchers in 1 purchase, though might only just allow
              // one for simplicity
              for (VoucherDelivery voucherDelivery : purchaseManager.getVoucherDeliveries())
                {
                  Map<String, Object> voucherJsonObject = new LinkedHashMap<String, Object>();
                  Voucher voucher = null;
                  if (voucherDelivery.getVoucherID() != null)
                    {
                      voucher = (Voucher) voucherService.getStoredVoucher(voucherDelivery.getVoucherID());
                    }
                  String supplierID = null;
                  String voucherCode = null;
                  if (voucher instanceof Voucher && voucher != null)
                    {
                      supplierID = voucher.getSupplierID();
                    }
                  if (voucherDelivery.getVoucherCode() != null && !voucherDelivery.getVoucherCode().isEmpty())
                    {
                      voucherCode = voucherDelivery.getVoucherCode();
                    }
                  voucherJsonObject.put("voucherCode", voucherCode);
                  voucherJsonObject.put("supplierID", supplierID);
                  voucherJsonObject.put("voucherID", voucherDelivery.getVoucherID());
                  voucherJsonObject.put("voucherFileID", voucherDelivery.getFileID());
                  voucherJsonObject.put("voucherExpiryDate", voucherDelivery.getVoucherExpiryDate()!=null?RLMDateUtils.formatDateForElasticsearchDefault(voucherDelivery.getVoucherExpiryDate()):"");
                  voucherList.add(voucherJsonObject);

                }

            }
        /*String offerContent = sb.length()>0?sb.toString().substring(0, sb.toString().length()-1):"";
        documentMap.put("offerContent", offerContent);*/
      }

      // populate with default values (for reports)
      if (documentMap.get("offerPrice") == null) documentMap.put("offerPrice", 0L);
      if (documentMap.get("meanOfPayment") == null) documentMap.put("meanOfPayment", "");
      if (documentMap.get("offerStock") == null) documentMap.put("offerStock", -1);
      
      documentMap.put("moduleID", purchaseManager.getModuleID());
      documentMap.put("featureID", purchaseManager.getFeatureID());
      documentMap.put("origin", purchaseManager.getOrigin());
      documentMap.put("resellerID", purchaseManager.getResellerID());
      Object code = purchaseManager.getReturnCode();
      documentMap.put("returnCode", code);
      documentMap.put("returnCodeDetails", purchaseManager.getOfferDeliveryReturnCodeDetails());
      documentMap.put("vouchers", voucherList);
      documentMap.put("creationDate", purchaseManager.getCreationDate() != null ? RLMDateUtils.formatDateForElasticsearchDefault(purchaseManager.getCreationDate()):"");
      documentMap.put("stratum", purchaseManager.getStatisticsSegmentsMap(subscriberGroupEpochReader, segmentationDimensionService));
      documentMap.put("metadata", JSONUtilities.encodeObject(metadataJSON));

      return documentMap;
    }
  }
}

