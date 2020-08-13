package com.evolving.nglm.evolution.reports.odr;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferProduct;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.Product;
import com.evolving.nglm.evolution.ProductService;
import com.evolving.nglm.evolution.RESTAPIGenericReturnCodes;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.SalesChannel;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportEsReader;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.evolution.reports.ReportEsReader.PERIOD;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import com.evolving.nglm.evolution.reports.bdr.BDRReportMonoPhase;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.zip.ZipOutputStream;

public class ODRReportMonoPhase implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(ODRReportMonoPhase.class);
  private static String elasticSearchDateFormat = Deployment.getElasticSearchDateFormat();
  private static final DateFormat DATE_FORMAT;
  
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
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

  private static List<String> headerFieldsOrder = new LinkedList<String>();
  static
  {
    DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));

    headerFieldsOrder.add(moduleId);
    headerFieldsOrder.add(featureId);
    headerFieldsOrder.add(moduleName);
    headerFieldsOrder.add(featureDisplay);
    headerFieldsOrder.add(offerID);
    headerFieldsOrder.add(offerDisplay);
    headerFieldsOrder.add(salesChannelID);
    headerFieldsOrder.add(voucherPartnerID);
    headerFieldsOrder.add(salesChannelDisplay);
    headerFieldsOrder.add(customerID);
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
      {
        headerFieldsOrder.add(alternateID.getName());
      }
    headerFieldsOrder.add(offerContent);
    headerFieldsOrder.add(eventDatetime);
    headerFieldsOrder.add(originatingDeliveryRequestID);
    headerFieldsOrder.add(deliveryRequestID);
    headerFieldsOrder.add(deliveryStatus);
    headerFieldsOrder.add(eventID);
    headerFieldsOrder.add(meanOfPayment);
    headerFieldsOrder.add(offerPrice);
    headerFieldsOrder.add(offerQty);
    headerFieldsOrder.add(offerStock);
    headerFieldsOrder.add(origin);
    headerFieldsOrder.add(voucherCode);
    headerFieldsOrder.add(returnCode);
    headerFieldsOrder.add(returnCodeDetails);
  }

  @Override public void dumpLineToCsv(Map<String, Object> lineMap, ZipOutputStream writer, boolean addHeaders)
  {
    try
      {
        if (addHeaders)
          {
            addHeaders(writer, headerFieldsOrder, 1);
          }
        String line = ReportUtils.formatResult(headerFieldsOrder, lineMap);
        log.trace("Writing to csv file : " + line);
        writer.write(line.getBytes());
      } 
    catch (IOException e)
      {
        e.printStackTrace();
      }
  }
  
  public Map<String, List<Map<String, Object>>> getSplittedReportElementsForFileMono(Map<String, Object> map)
  {
    Map<String, List<Map<String, Object>>> result = new LinkedHashMap<String, List<Map<String, Object>>>();
    Map<String, Object> odrFields = map;
    LinkedHashMap<String, Object> oderRecs = new LinkedHashMap<>();
    if (odrFields != null && !odrFields.isEmpty()) 
      {
        if(odrFields.get(subscriberID) != null) {
          Object subscriberIDField = odrFields.get(subscriberID);
          oderRecs.put(customerID, subscriberIDField);
        }
        for (AlternateID alternateID : Deployment.getAlternateIDs().values())
          {
            if (odrFields.get(alternateID.getID()) != null)
              {
                Object alternateId = odrFields.get(alternateID.getID());
                oderRecs.put(alternateID.getName(), alternateId);
              }
          }

        if (odrFields.containsKey(originatingDeliveryRequestID))
          {
            oderRecs.put(originatingDeliveryRequestID, odrFields.get(originatingDeliveryRequestID));
          }
        if (odrFields.containsKey(deliveryRequestID))
          {
            oderRecs.put(deliveryRequestID, odrFields.get(deliveryRequestID));
          }
        if (odrFields.containsKey(deliveryStatus))
          {
            oderRecs.put(deliveryStatus, odrFields.get(deliveryStatus));
          }
        if (odrFields.containsKey(eventID))
          {
            oderRecs.put(eventID, odrFields.get(eventID));
          }

        if (odrFields.get(eventDatetime) != null)
          {
            Object eventDatetimeObj = odrFields.get(eventDatetime);
            if (eventDatetimeObj instanceof String)
              {

                // TEMP fix for BLK : reformat date with correct template.

                oderRecs.put(eventDatetime, ReportsCommonCode.parseDate((String) eventDatetimeObj));

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
          oderRecs.put(featureDisplay, feature);
          oderRecs.put(moduleName, module.toString());
          oderRecs.put(featureId, odrFields.get(featureId));
          oderRecs.put(moduleId, odrFields.get(moduleId));
        }  
        if (odrFields.containsKey(meanOfPayment))
          {
            oderRecs.put(meanOfPayment, odrFields.get(meanOfPayment));
          }
        if (odrFields.containsKey(offerID))
          {
            oderRecs.put(offerID, odrFields.get(offerID));
            GUIManagedObject offerObject = offerService.getStoredOffer(String.valueOf(odrFields.get(offerID)));
            if (offerObject instanceof Offer)
              {
                oderRecs.put(offerDisplay, ((Offer)offerObject).getDisplay());
              }
            else {
              oderRecs.put(offerDisplay, "");
            }

          }
        if (odrFields.containsKey(offerPrice))
          {
            oderRecs.put(offerPrice, odrFields.get(offerPrice));
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
            oderRecs.put(offerContent, ReportUtils.formatJSON(offerContentJSON));
          }

        if (odrFields.containsKey(offerQty))
          {
            oderRecs.put(offerQty, odrFields.get(offerQty));
          }
        if (odrFields.containsKey(offerStock))
          {
            oderRecs.put(offerStock, odrFields.get(offerStock));
          }
        if (odrFields.containsKey(origin))
          {
            oderRecs.put(origin, odrFields.get(origin));
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
            oderRecs.put(voucherCode, odrFields.get(voucherCode));
          }
        if (odrFields.containsKey(returnCode))
          {
            oderRecs.put(returnCode, odrFields.get(returnCode));
          }
        if (odrFields.containsKey(returnCodeDetails))
          {
            Object code = odrFields.get(returnCode);
            oderRecs.put(returnCode, code);
            oderRecs.put(returnCodeDetails, (code != null && code instanceof Integer) ? RESTAPIGenericReturnCodes.fromGenericResponseCode((int) code).getGenericResponseMessage() : "");
          }    

        //
        // result
        //

        String rawEventDateTime = oderRecs.get(eventDatetime) == null ? null : oderRecs.get(eventDatetime).toString();
        if (rawEventDateTime == null) log.warn("bad EventDateTime -- report will be generated in 'null' file name -- for record {} ", odrFields );
        String evntDate = getEventDate(rawEventDateTime);
        if (result.containsKey(evntDate))
          {
            result.get(evntDate).add(oderRecs);
          } 
        else
          {
            List<Map<String, Object>> elements = new ArrayList<Map<String, Object>>();
            elements.add(oderRecs);
            result.put(evntDate, elements);
          }
      }
    return result;
  }
  
  private String getEventDate(String rawEventDateTime)
  {
    String result = "null";
    if (rawEventDateTime == null || rawEventDateTime.trim().isEmpty()) return result;
    String eventDateTimeFormat = "yyyy-MM-dd";
    result = rawEventDateTime.substring(0, eventDateTimeFormat.length());
    return result;
  }
    
  private void addHeaders(ZipOutputStream writer, List<String> headers, int offset) throws IOException
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
      }  
  }

  public static void main(String[] args)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("ODRReportMonoPhase: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : ODRReportMonoPhase <ESNode> <ES journey index> <csvfile> <defaultReportPeriodQuantity> <defaultReportPeriodUnit>");
        return;
      }
    String esNode     = args[0];
    String esIndexOdr = args[1];
    String csvfile    = args[2];

    Integer reportPeriodQuantity = 0;
    String reportPeriodUnit = null;
    if (args.length > 4 && args[3] != null && args[4] != null)
      {
        reportPeriodQuantity = Integer.parseInt(args[3]);
        reportPeriodUnit = args[4];
      }
    Date fromDate = getFromDate(reportPeriodUnit, reportPeriodQuantity);
    Date toDate = SystemTime.getCurrentTime();
    
    List<String> esIndexDates = getEsIndexDates(fromDate, toDate);
    StringBuilder esIndexOdrList = new StringBuilder();
    boolean firstEntry = true;
    for (String esIndexDate : esIndexDates)
      {
        if (!firstEntry) esIndexOdrList.append(",");
        String indexName = esIndexOdr + esIndexDate;
        esIndexOdrList.append(indexName);
        firstEntry = false;
      }
    ReportCsvFactory reportFactory = new ODRReportMonoPhase();
    log.info("Reading data from ES in (" + esIndexOdrList.toString() + ")  index and writing to " + csvfile);
    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexOdrList.toString(), QueryBuilders.matchAllQuery());

    String journeyTopic = Deployment.getJourneyTopic();
    String offerTopic = Deployment.getOfferTopic();
    String salesChannelTopic = Deployment.getSalesChannelTopic();
    String loyaltyProgramTopic = Deployment.getLoyaltyProgramTopic();
    String productTopic = Deployment.getProductTopic();

    salesChannelService = new SalesChannelService(Deployment.getBrokerServers(), "odrreportcsvwriter-saleschannelservice-ODRReportMonoPhase", salesChannelTopic, false);
    salesChannelService.start();
    offerService = new OfferService(Deployment.getBrokerServers(), "odrreportcsvwriter-offerservice-ODRReportMonoPhase", offerTopic, false);
    offerService.start();
    journeyService = new JourneyService(Deployment.getBrokerServers(), "odrreportcsvwriter-journeyservice-ODRReportMonoPhase", journeyTopic, false);
    journeyService.start();
    loyaltyProgramService = new LoyaltyProgramService(Deployment.getBrokerServers(), "odrreportcsvwriter-loyaltyprogramservice-ODRReportMonoPhase", loyaltyProgramTopic, false);
    loyaltyProgramService.start();
    productService = new ProductService(Deployment.getBrokerServers(), "odrreportcsvwriter-productService-ODRReportMonoPhase", productTopic, false);
    productService.start();

    ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
        esNode,
        esIndexWithQuery,
        reportFactory,
        csvfile
    );

    if (!reportMonoPhase.startOneToOne(true))
      {
        log.warn("An error occured, the report might be corrupted");
        return;
      }
    
    log.info("Finished ODRReport");
  }
  

  private static List<String> getEsIndexDates(final Date fromDate, Date toDate)
  {
    Date tempfromDate = fromDate;
    List<String> esIndexOdrList = new ArrayList<String>();
    while(tempfromDate.getTime() <= toDate.getTime())
      {
        esIndexOdrList.add(DATE_FORMAT.format(tempfromDate));
        tempfromDate = RLMDateUtils.addDays(tempfromDate, 1, Deployment.getBaseTimeZone());
      }
    return esIndexOdrList;
  }


  private static Date getFromDate(String reportPeriodUnit, Integer reportPeriodQuantity)
  {
    reportPeriodQuantity = reportPeriodQuantity == null || reportPeriodQuantity == 0 ? new Integer(1) : reportPeriodQuantity;
    if (reportPeriodUnit == null) reportPeriodUnit  = PERIOD.DAYS.getExternalRepresentation();
    
    //
    //
    //
    
    Date now = SystemTime.getCurrentTime();
    Date fromDate = null;
    switch (reportPeriodUnit.toUpperCase())
      {
        case "DAYS":
          fromDate = RLMDateUtils.addDays(now, -reportPeriodQuantity, com.evolving.nglm.core.Deployment.getBaseTimeZone());
          break;
          
        case "WEEKS":
          fromDate = RLMDateUtils.addWeeks(now, -reportPeriodQuantity, com.evolving.nglm.core.Deployment.getBaseTimeZone());
          break;
          
        case "MONTHS":
          fromDate = RLMDateUtils.addMonths(now, -reportPeriodQuantity, com.evolving.nglm.core.Deployment.getBaseTimeZone());
          break;
          
        default:
          break;
      }
    return fromDate;
  }
}
