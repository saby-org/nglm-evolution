/*****************************************************************************
*
*  OfferReportDriver.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.reports.offer;

import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class OfferReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(OfferReportDriver.class);
  private OfferService offerService;
  private SalesChannelService salesChannelService;
  private OfferObjectiveService offerObjectiveService;
  private ProductService productService;
  private PaymentMeanService paymentmeanservice;
  private CatalogCharacteristicService catalogCharacteristicService;

  /****************************************
   * 
   * produceReport
   * 
   ****************************************/

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {
    log.info("Entered OfferReportDriver.produceReport");

    Random r = new Random();
    int apiProcessKey = r.nextInt(999);

    log.info("apiProcessKey" + apiProcessKey);

    offerService = new OfferService(kafka, "offerReportDriver-offerService-" + apiProcessKey, Deployment.getOfferTopic(), false);
    offerService.start();

    salesChannelService = new SalesChannelService(kafka, "offerReportDriver-saleschannelService-" + apiProcessKey, Deployment.getSalesChannelTopic(), false);
    salesChannelService.start();

    offerObjectiveService = new OfferObjectiveService(kafka, "offerReportDriver-offerObjectiveService-" + apiProcessKey, Deployment.getOfferObjectiveTopic(), false);
    offerObjectiveService.start();

    productService = new ProductService(kafka, "offerReportDriver-productService-" + apiProcessKey, Deployment.getProductTopic(), false);
    productService.start();

    paymentmeanservice = new PaymentMeanService(kafka, "offerReportDriver-paymentmeanservice-" + apiProcessKey, Deployment.getPaymentMeanTopic(), false);
    paymentmeanservice.start();

    catalogCharacteristicService = new CatalogCharacteristicService(kafka, "offerReportDriver-catalogcharacteristicservice-" + apiProcessKey, Deployment.getCatalogCharacteristicTopic(), false);
    catalogCharacteristicService.start();
    
    ReportsCommonCode.initializeDateFormats();

    boolean header = true;
    int first = 0;
    File file = new File(csvFilename + ".zip");
    FileOutputStream fos = null;
    ZipOutputStream writer = null;
    try
      {
        log.info("no. of Offers :" + offerService.getStoredOffers(tenantID).size());
        if (offerService.getStoredOffers(tenantID).size() == 0)
          {
            log.info("No Offers ");
          }
        else
          {
            fos = new FileOutputStream(file);
            writer = new ZipOutputStream(fos);
            // do not include tree structure in zipentry, just csv filename
            ZipEntry entry = new ZipEntry(new File(csvFilename).getName());
            writer.putNextEntry(entry);
            Collection<GUIManagedObject> offers = offerService.getStoredOffers(tenantID);
            int nbOffers = offers.size();
            log.debug("offer list size : " + nbOffers);

            for (GUIManagedObject guiManagedObject : offers)
              {
                try
                  {
                    Offer offer = (guiManagedObject instanceof Offer) ? (Offer) guiManagedObject : null;
                    dumpElementToCsv(offer, offerService.generateResponseJSON(guiManagedObject, true, reportGenerationDate), writer, header, (first == nbOffers - 1));
                    if (first == 0)
                      {
                        header = false;
                      }
                    ++first;
                  }
                catch (IOException | InterruptedException e)
                  {
                    log.info("exception " + e.getLocalizedMessage());
                  }
              }
          }
      }
    catch (IOException e)
      {
        log.info("exception " + e.getLocalizedMessage());
      }
    finally {
      offerService.stop();
      salesChannelService.stop();
      offerObjectiveService.stop();
      productService.stop();
      paymentmeanservice.stop();
      catalogCharacteristicService.stop();

      try
      {
        if (writer != null) writer.close();
      }
      catch (IOException e)
      {
        log.info("exception " + e.getLocalizedMessage());
      }
      if (fos != null)
        {
          try
          {
            fos.close();
          }
          catch (IOException e)
          {
            log.info("Exception " + e);
          }
        }
    }
  }

  /****************************************
   *
   * dumpElementToCsv
   *
   ****************************************/

  private void dumpElementToCsv(Offer offer, JSONObject recordJson, ZipOutputStream writer, Boolean addHeaders, boolean last)
      throws IOException, InterruptedException
  {
    // log.info("offer Records : {}",recordJson);

    String csvSeparator = ReportUtils.getSeparator();
    Map<String, Object> offerFields = new LinkedHashMap<>();

    try {
        offerFields.put("offerID", recordJson.get("id"));
        offerFields.put("offerName", recordJson.get("display"));
          {
            List<Map<String, Object>> offerContentJSON = new ArrayList<>();
            JSONArray elements = (JSONArray) recordJson.get("products");
            for (Object obj : elements)
              {
                JSONObject element = (JSONObject) obj;
                if (element != null)
                  {
                    Map<String, Object> outputJSON = new HashMap<>();
                    String objectid = (String) (element.get("productID"));
                    GUIManagedObject guiManagedObject = (GUIManagedObject) productService.getStoredProduct(objectid);
                    if (guiManagedObject != null && guiManagedObject instanceof Product)
                      {
                        Product product = (Product) guiManagedObject;
                        outputJSON.put(product.getDisplay(), element.get("quantity"));
                      }
                    offerContentJSON.add(outputJSON);
                  }
              }
            offerFields.put("offerContent", ReportUtils.formatJSON(offerContentJSON));
          }

        offerFields.put("offerDescription", recordJson.get("description"));

          {
            List<Map<String, Object>> outputJSON = new ArrayList<>();
            JSONObject obj = (JSONObject) recordJson.get("offerCharacteristics");
            if (obj != null)
              {
                JSONArray obj1 = (JSONArray) obj.get("languageProperties");
                if (obj1 != null && !obj1.isEmpty())
                  {
                    JSONObject obj2 = (JSONObject) obj1.get(0);
                    if (obj2 != null)
                      {
                        JSONArray obj3 = (JSONArray) obj2.get("properties");
                        if (obj3 != null && !obj3.isEmpty())
                          {
                            Map<String, Object> caracteristicsJSON = new HashMap<>();
                            for (int i = 0; i < obj3.size(); i++)
                              {
                                JSONObject offer2 = (JSONObject) obj3.get(i);
                                if (offer2 != null)
                                  {
                                    String name = "" + offer2.get("catalogCharacteristicName");                                
                                    caracteristicsJSON.put(name, offer2.get("value"));
                                  }
                              }
                            outputJSON.add(caracteristicsJSON);
                          }
                      }
                  }
              }
            offerFields.put("offerCharacteristics", ReportUtils.formatJSON(outputJSON));
          }

          {
            JSONArray elements = (JSONArray) recordJson.get("offerObjectives");
            List<Map<String, Object>> outputJSON = new ArrayList<>(); // to preserve order when displaying
            if (elements != null)
              {
                for (int i = 0; i < elements.size(); i++)
                  {
                    Map<String, Object> objectivesJSON = new LinkedHashMap<>(); // to preserve order when displaying
                    JSONObject element = (JSONObject) elements.get(i);
                    if (element != null)
                      {
                        String objeciveID = (String) (element.get("offerObjectiveID"));
                        GUIManagedObject guiManagedObject = (GUIManagedObject) offerObjectiveService.getStoredOfferObjective(objeciveID);
                        if (guiManagedObject != null && guiManagedObject instanceof OfferObjective)
                          {
                            OfferObjective offerObjective = (OfferObjective) guiManagedObject;
                            objectivesJSON.put("objectiveName", offerObjective.getOfferObjectiveDisplay());
                            if (offer != null)
                              {
                                List<Map<String, Object>> characteristicsJSON = new ArrayList<>();
                                for (OfferObjectiveInstance objective : offer.getOfferObjectives())
                                  {
                                    for (CatalogCharacteristicInstance characteristicInstance : objective.getCatalogCharacteristics())
                                      {
                                        Object value = characteristicInstance.getValue();
                                        String catalogCharacteristicID = characteristicInstance.getCatalogCharacteristicID();
                                        GUIManagedObject characteristic = catalogCharacteristicService.getStoredCatalogCharacteristic(catalogCharacteristicID);
                                        if (characteristic != null && characteristic instanceof CatalogCharacteristic)
                                          {
                                            Map<String, Object> characteristicJSON = new HashMap<>();
                                            CatalogCharacteristic characteristicObj = (CatalogCharacteristic) characteristic;
                                            String name = characteristicObj.getCatalogCharacteristicName();
                                            characteristicJSON.put(name, value);
                                            characteristicsJSON.add(characteristicJSON);
                                          }
                                      }
                                  }
                                objectivesJSON.put("characteristics", characteristicsJSON);
                              }
                            outputJSON.add(objectivesJSON);
                          }
                      }
                  }
              }
            offerFields.put("offerObjectives", ReportUtils.formatJSON(outputJSON));
          }
      
      offerFields.put("startDate", ReportsCommonCode.parseDate((String) recordJson.get("effectiveStartDate")));
      offerFields.put("endDate", ReportsCommonCode.parseDate((String) recordJson.get("effectiveEndDate")));      
      offerFields.put("availableStock", recordJson.get("presentationStock"));
      offerFields.put("availableStockThreshold", recordJson.get("presentationStockAlertThreshold"));

          {
            List<Map<String, Object>> outputJSON = new ArrayList<>();
            JSONArray elements = (JSONArray) recordJson.get("salesChannelsAndPrices");
            for (Object obj : elements)
              {
                JSONObject element = (JSONObject) obj;
                if (element != null)
                  {
                    JSONArray salesChannelIDs = (JSONArray) element.get("salesChannelIDs");
                    for (Object obj2 : salesChannelIDs)
                      {
                        String salesChannelID = (String) obj2;
                        GUIManagedObject guiManagedObject = (GUIManagedObject) salesChannelService.getStoredSalesChannel(salesChannelID);
                        if (guiManagedObject != null && guiManagedObject instanceof SalesChannel)
                          {
                            Map<String, Object> salesChannelJSON = new LinkedHashMap<>(); // to preserve order when displaying
                            SalesChannel salesChannel = (SalesChannel) guiManagedObject;
                            salesChannelJSON.put("salesChannelName", salesChannel.getGUIManagedObjectDisplay());

                            JSONObject price = (JSONObject) element.get("price");
                            if (price != null)
                              {
                                long amount = 0; // free by default
                                Object amountObject = price.get("amount");
                                if (amountObject != null && amountObject instanceof Long)
                                  {
                                    amount = (Long) amountObject;
                                  }
                                String id = "" + price.get("supportedCurrencyID");
                                String meansOfPayment = "" + price.get("paymentMeanID");
                                if (id != null && meansOfPayment != null)
                                  {
                                    String currency = null;
                                    GUIManagedObject meansOfPaymentObject = paymentmeanservice.getStoredPaymentMean(meansOfPayment);
                                    if (meansOfPaymentObject != null)
                                      {
                                        meansOfPayment = "" + meansOfPaymentObject.getJSONRepresentation().get("display");
                                        for (SupportedCurrency supportedCurrency : Deployment.getDeployment(offer.getTenantID()).getSupportedCurrencies().values())
                                          {
                                            JSONObject supportedCurrencyJSON = supportedCurrency.getJSONRepresentation();
                                            if (id.equals(supportedCurrencyJSON.get("id")))
                                              {
                                                currency = "" + supportedCurrencyJSON.get("display"); // TODO : not used
                                                                                                      // ??
                                                break;
                                              }
                                          }
                                        log.debug("amount: " + amount + ", mean of payment:" + meansOfPayment + ", currency:" + currency);
                                        salesChannelJSON.put("mean of payment", meansOfPayment);
                                        salesChannelJSON.put("amount", amount);
                                        salesChannelJSON.put("currency", currency);
                                      }
                                  }
                          }
                        else
                              {
                                salesChannelJSON.put("amount", 0);
                              }
                            outputJSON.add(salesChannelJSON);
                          }
                      }
                  }
              }
            offerFields.put("salesChannelAndPrices", ReportUtils.formatJSON(outputJSON));
          }

            // Arrays.sort(allFields);


      offerFields.put("active", recordJson.get("active"));      
     
      if (offerFields != null)
        {
          // Arrays.sort(allFields);
          
          if (addHeaders)
            {              
              String headers = "";              
              for (String fields : offerFields.keySet())
                {
                  
                  headers += fields + csvSeparator;
                }
              headers = headers.substring(0, headers.length() - 1);              
              writer.write(headers.getBytes());
              writer.write("\n".getBytes());
              addHeaders = false;
            }
          String line = ReportUtils.formatResult(offerFields);
          writer.write(line.getBytes());
        }
    }
    catch (Exception ex)
    {
      log.info("Exception while processing a line : " + ex.getLocalizedMessage());
    }
    

    if (last)
      {
        log.info("Last offer record inserted into csv");
        writeCompleted(writer);
      }
  }

  /****************************************
   *
   * writeCompleted
   *
   ****************************************/

  private void writeCompleted(ZipOutputStream writer) throws IOException, InterruptedException
  {
    log.info("offerService {}", offerService.toString());
    writer.flush();
    writer.closeEntry();
    writer.close();
    log.debug("csv Writer closed");
  }
}
