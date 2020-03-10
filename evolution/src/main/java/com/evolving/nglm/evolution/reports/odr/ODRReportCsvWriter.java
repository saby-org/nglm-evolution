package com.evolving.nglm.evolution.reports.odr;

import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipOutputStream;

public class ODRReportCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(ODRReportCsvWriter.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  private boolean addHeaders = true;
  private static JourneyService journeyService;
  private static OfferService offerService;
  private static SalesChannelService salesChannelService;
  private static LoyaltyProgramService loyaltyProgramService;
  private static ProductService productService;

  private final static String moduleId = "moduleID";
  private final static String featureId = "featureID";
  private final static String moduleName = "moduleName";
  private final static String featureDisplay = "featureDisplay";
  private final static String subscriberID = "subscriberID";
  private final static String offerID = "offerID";
  private final static String offerDisplay = "offerName";
  private final static String salesChannelID = "salesChannelID";
  private final static String voucherPartnerID = "voucherPartnerID";
  private final static String salesChannelDisplay = "salesChannelName";
  private final static String customerID = "customerID";
  private static final String offerContent = "offerContent";
  private static final String eventDatetime = "eventDatetime";
  private static final String originatingDeliveryRequestID = "originatingDeliveryRequestID";
  private static final String deliveryRequestID = "deliveryRequestID";
  private static final String deliveryStatus = "deliveryStatus";
  private static final String eventID = "eventID";
  private static final String meanOfPayment = "meanOfPayment";
  private static final String offerPrice = "offerPrice";
  private static final String offerQty = "offerQty";
  private static final String offerStock = "offerStock";
  private static final String origin = "origin";
  private static final String voucherCode = "voucherCode";
  private static final String returnCode = "returnCode";
  private static final String returnCodeDetails = "returnCodeDetails";

  List<String> headerFieldsOrder = new ArrayList<String>();
  
  /**
   * This methods writes a single {@link ReportElement} to the report (csv file).
   * @throws IOException in case anything goes wrong while writing to the report.
   */
  public void dumpElementToCsv(String key, ReportElement re, ZipOutputStream writer) throws IOException {
      if (re.type == ReportElement.MARKER) // We will find markers in the topic
          return;

      log.trace("We got "+key+" "+re);
      Map<String, Object> odrFieldsMap = re.fields.get(0);
      Map<String, Object> subscriberFields = re.fields.get(1);
      LinkedHashMap<String, Object> result = new LinkedHashMap<>();
      LinkedHashMap<String, Object> subscriberFieldsresult = new LinkedHashMap<>();
      for (Object odrFieldsObj : odrFieldsMap.values()) // we don't care about the keys
      {
        Map<String, Object> odrFields = (Map<String, Object>) odrFieldsObj;
        if (odrFields != null && !odrFields.isEmpty() && subscriberFields != null && !subscriberFields.isEmpty()) {
          if(odrFields.get(subscriberID) != null) {
            Object subscriberIDField = odrFields.get(subscriberID);
            result.put(customerID, subscriberIDField);
           // odrFields.remove(subscriberID);
          }
          for (AlternateID alternateID : Deployment.getAlternateIDs().values())
            {
              if (subscriberFields.get(alternateID.getESField()) != null)
                {
                  Object alternateId = subscriberFields.get(alternateID.getESField());
                  result.put(alternateID.getName(), alternateId);
                }
            }
          
          if (odrFields.containsKey(originatingDeliveryRequestID))
            {
              // TODO : same as salesChannelID
              result.put(originatingDeliveryRequestID, odrFields.get(originatingDeliveryRequestID));
            }
          if (odrFields.containsKey(deliveryRequestID))
            {
              // TODO : same as salesChannelID
              result.put(deliveryRequestID, odrFields.get(deliveryRequestID));
            }
          if (odrFields.containsKey(deliveryStatus))
            {
              // TODO : same as salesChannelID
              result.put(deliveryStatus, odrFields.get(deliveryStatus));
            }
          if (odrFields.containsKey(eventID))
            {
              // TODO : same as salesChannelID
              result.put(eventID, odrFields.get(eventID));
            }

          if (odrFields.get(eventDatetime) != null)
            {
              Object eventDatetimeObj = odrFields.get(eventDatetime);
              if (eventDatetimeObj instanceof String)
                {
                  
                  // TEMP fix for BLK : reformat date with correct template.

                  List<SimpleDateFormat> standardDateFormats = ReportsCommonCode.initializeDateFormats();
                  result.put(eventDatetime, ReportsCommonCode.parseDate(standardDateFormats, (String) eventDatetimeObj));

                  // END TEMP fix for BLK

                }
              else
                {
                  log.info(eventDatetime + " is of wrong type : "+eventDatetimeObj.getClass().getName());
                }
          }


          //Compute featureName and ModuleName from ID
          if(odrFields.containsKey(moduleId) && odrFields.containsKey(featureId)){
            Module module = Module.fromExternalRepresentation(String.valueOf(odrFields.get(moduleId)));
            String feature = DeliveryRequest.getFeatureDisplay(module, String.valueOf(odrFields.get(featureId).toString()), journeyService, offerService, loyaltyProgramService);
            result.put(featureDisplay, feature);
            result.put(moduleName, module.toString());           

           // odrFields.remove(featureId);
            //odrFields.remove(moduleId);
          }  
          if (odrFields.containsKey(meanOfPayment))
            {
              // TODO : same as salesChannelID
              result.put(meanOfPayment, odrFields.get(meanOfPayment));
            }
          if (odrFields.containsKey(offerID))
            {
              result.put(offerID, odrFields.get(offerID));
              GUIManagedObject offerObject = offerService.getStoredOffer(String.valueOf(odrFields.get(offerID)));
              if (offerObject instanceof Offer)
                {
                  result.put(offerDisplay, ((Offer)offerObject).getDisplay());
                  //odrFields.remove(offerID);                  
                }
              else {
                result.put(offerDisplay, "");
              }
              
          }
          if (odrFields.containsKey(offerPrice))
            {
              // TODO : same as salesChannelID
              result.put(offerPrice, odrFields.get(offerPrice));
            }
          
          if (odrFields.containsKey(offerContent))
            {
              List<Map<String, Object>> offerContentJSON = new ArrayList<>();
              GUIManagedObject offerObject = offerService.getStoredOffer(String.valueOf(odrFields.get(offerID)));
              if (offerObject != null && offerObject instanceof Offer)
                {
                  Offer offer = (Offer) offerObject;
                  for (OfferProduct offerProduct : offer.getOfferProducts())
                    {
                      Map<String, Object> outputJSON = new HashMap<>();
                      String objectid = offerProduct.getProductID();
                      GUIManagedObject guiManagedObject = (GUIManagedObject) productService.getStoredProduct(objectid);
                      if (guiManagedObject != null && guiManagedObject instanceof Product)
                        {
                          Product product = (Product) guiManagedObject;
                          outputJSON.put(product.getDisplay(), offerProduct.getQuantity());
                        }
                      offerContentJSON.add(outputJSON);
                    }
                }
              result.put(offerContent, ReportUtils.formatJSON(offerContentJSON));
            }
       
          if (odrFields.containsKey(offerQty))
            {
              // TODO : same as salesChannelID
              result.put(offerQty, odrFields.get(offerQty));
            }
          if (odrFields.containsKey(offerStock))
            {
              // TODO : same as salesChannelID
              result.put(offerStock, odrFields.get(offerStock));
            }
          if (odrFields.containsKey(origin))
            {
              // TODO : same as salesChannelID
              result.put(origin, odrFields.get(origin));
            }
          // get salesChannel display
          if (odrFields.containsKey(salesChannelID))
            {
              GUIManagedObject salesChannelObject = salesChannelService.getStoredSalesChannel(String.valueOf(odrFields.get(salesChannelID)));
              if(salesChannelObject instanceof SalesChannel)
                {
                  odrFields.put(salesChannelDisplay, ((SalesChannel)salesChannelObject).getGUIManagedObjectDisplay());
                  odrFields.remove(salesChannelID);
                }
            }
          if (odrFields.containsKey(voucherCode))
            {
              // TODO : same as salesChannelID
              result.put(voucherCode, odrFields.get(voucherCode));
            }
          if (odrFields.containsKey(returnCode))
            {
              // TODO : same as salesChannelID
              result.put(returnCode, odrFields.get(returnCode));
            }
          if (odrFields.containsKey(returnCodeDetails))
            {
              // TODO : same as salesChannelID
              result.put(returnCodeDetails, odrFields.get(returnCodeDetails));
            }      
         

          if (addHeaders)
            {
              addHeaders(writer, result.keySet(), 1);
            }
          String line = ReportUtils.formatResult(headerFieldsOrder, result);
          log.trace("Writing to csv file : "+line);
          writer.write(line.getBytes());
          writer.write("\n".getBytes());
        }
      }
  }

  public static void main(String[] args) {
      log.info("received " + args.length + " args");
      for(String arg : args){
          log.info("ODRReportCsvWriter: arg " + arg);
      }

      if (args.length < 3) {
          log.warn("Usage : ODRReportCsvWriter <KafkaNode> <topic in> <csvfile>");
          return;
      }
      String kafkaNode = args[0];
      String topic     = args[1];
      String csvfile   = args[2];
      log.info("Reading data from "+topic+" topic on broker "+kafkaNode
              + " producing "+csvfile+" with '"+CSV_SEPARATOR+"' separator");
      ReportCsvFactory reportFactory = new ODRReportCsvWriter();
      ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);
      
      String journeyTopic = Deployment.getJourneyTopic();
      String offerTopic = Deployment.getOfferTopic();
      String salesChannelTopic = Deployment.getSalesChannelTopic();
      String loyaltyProgramTopic = Deployment.getLoyaltyProgramTopic();
      String productTopic = Deployment.getProductTopic();

      salesChannelService = new SalesChannelService(kafkaNode, "odrreportcsvwriter-saleschannelservice-" + topic, salesChannelTopic, false);
      salesChannelService.start();
      offerService = new OfferService(kafkaNode, "odrreportcsvwriter-offerservice-" + topic, offerTopic, false);
      offerService.start();
      journeyService = new JourneyService(kafkaNode, "odrreportcsvwriter-journeyservice-" + topic, journeyTopic, false);
      journeyService.start();
      loyaltyProgramService = new LoyaltyProgramService(kafkaNode, "odrreportcsvwriter-loyaltyprogramservice-" + topic, loyaltyProgramTopic, false);
      loyaltyProgramService.start();
      productService = new ProductService(kafkaNode, "odrreportcsvwriter-productService-" + topic, productTopic, false);
      productService.start();
      
      if (!reportWriter.produceReport(csvfile)) {
          log.warn("An error occured, the report might be corrupted");
          return;
      }
  }
  
  private void addHeaders(ZipOutputStream writer, Set<String> headers, int offset) throws IOException
  {
    if (headers != null && !headers.isEmpty())
      {
        String header = "";
        for (String field : headers)
          {
            header += field + CSV_SEPARATOR;
          }
        header = header.substring(0, header.length() - offset);
        writer.write(header.getBytes());
        if (offset == 1)
          {
            writer.write("\n".getBytes());
          }
        addHeaders = false;
      }  
  }
}

