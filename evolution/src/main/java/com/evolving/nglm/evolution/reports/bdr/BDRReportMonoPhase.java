package com.evolving.nglm.evolution.reports.bdr;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deliverable;
import com.evolving.nglm.evolution.DeliverableService;
import com.evolving.nglm.evolution.DeliveryManager;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.RESTAPIGenericReturnCodes;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportMonoPhase.PERIOD;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;

public class BDRReportMonoPhase implements ReportCsvFactory
{

  private static final Logger log = LoggerFactory.getLogger(BDRReportMonoPhase.class);
  private static final DateFormat DATE_FORMAT;
  static
  {
    DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
  }

  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  private DeliverableService deliverableService;
  private JourneyService journeyService;
  private OfferService offerService;
  private LoyaltyProgramService loyaltyProgramService;

  private static final String moduleId = "moduleID";
  private static final String featureId = "featureID";
  private static final String deliverableID = "deliverableID";
  private static final String deliverableQty = "deliverableQty";
  private static final String deliveryStatus = "deliveryStatus";
  static final String moduleName = "moduleName";
  private static final String featureDisplay = "featureName";
  static final String deliverableDisplay = "deliverableName";
  private static final String subscriberID = "subscriberID";
  private static final String customerID = "customerID";
  private static final String deliverableExpirationDate = "deliverableExpirationDate";
  private static final String eventDatetime = "eventDatetime";
  private static final String operation = "operation";
  private static final String orgin = "origin";
  private static final String providerId = "providerID";
  private static final String providerName = "providerName";
  private static final String returnCode = "returnCode";
  private static final String returnCodeDetails = "returnCodeDetails";
  private static final String returnCodeDescription = "returnCodeDescription";
  private static final String deliveryRequestID = "deliveryRequestID";
  private static final String originatingDeliveryRequestID = "originatingDeliveryRequestID";
  private static final String eventID = "eventID";
  private static SimpleDateFormat parseSDF1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
  private static SimpleDateFormat parseSDF2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSXX");

  private static List<String> headerFieldsOrder = new ArrayList<String>();
  static
  {
    headerFieldsOrder.add(moduleId);
    headerFieldsOrder.add(featureId);
    headerFieldsOrder.add(deliverableID);
    headerFieldsOrder.add(deliverableQty);
    headerFieldsOrder.add(moduleName);
    headerFieldsOrder.add(featureDisplay);
    headerFieldsOrder.add(deliverableDisplay);
    headerFieldsOrder.add(customerID);
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
      {
        headerFieldsOrder.add(alternateID.getName());
      }
    headerFieldsOrder.add(deliverableExpirationDate);
    headerFieldsOrder.add(eventDatetime);
    headerFieldsOrder.add(operation);
    headerFieldsOrder.add(orgin);
    headerFieldsOrder.add(providerName);
    headerFieldsOrder.add(returnCode);
    headerFieldsOrder.add(returnCodeDescription);
    headerFieldsOrder.add(returnCodeDetails);
    headerFieldsOrder.add(deliveryRequestID);
    headerFieldsOrder.add(originatingDeliveryRequestID);
    headerFieldsOrder.add(eventID);
    headerFieldsOrder.add(deliveryStatus);
  }

  /****************************************
  *
  * dumpElementToCsv
  *
  ****************************************/
 public boolean dumpElementToCsvMono(Map<String,Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
 {
   Map<String, List<Map<String, Object>>> mapLocal = getSplittedReportElementsForFileMono(map);  
   if(mapLocal.size() != 1) {
	   log.debug("We have multiple dates in the same index " + mapLocal.size());
   } else {
	   if(mapLocal.values().size() != 1) {
		   log.debug("We have multiple values for this date " + mapLocal.values().size());
	   }
	   else {
		   Set<Entry<String, List<Map<String, Object>>>> setLocal = mapLocal.entrySet();
		   if(setLocal.size() != 1) {
			   log.debug("We have multiple dates in this report " + setLocal.size());
		   } else {
			   for (Entry<String, List<Map<String, Object>>> entry : setLocal) {
				   List<Map<String, Object>> list = entry.getValue();

				   if(list.size() != 1) {
					   log.debug("We have multiple reports in this folder " + list.size());
				   } else {
					   Map<String, Object> reportMap = list.get(0);
					   dumpLineToCsv(reportMap, writer, addHeaders);
					   return false;
				   }
			   }
		   }
	   }
   }
   return true;
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
        if (log.isTraceEnabled()) log.trace("Writing to csv file : " + line);
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
    LinkedHashMap<String, Object> bdrRecs = new LinkedHashMap<>();
    Map<String, Object> bdrFields = map;
    if (bdrFields != null && !bdrFields.isEmpty())
      {
        // rename subscriberID
        if(bdrFields.get(subscriberID) != null) {
          Object subscriberIDField = bdrFields.get(subscriberID);
          bdrRecs.put(customerID, subscriberIDField);
          //bdrFields.remove(subscriberID);
        }
        for (AlternateID alternateID : Deployment.getAlternateIDs().values())
          {
            if (bdrFields.get(alternateID.getID()) != null)
              {
                Object alternateId = bdrFields.get(alternateID.getID());
                bdrRecs.put(alternateID.getName(), alternateId);
              }
          } 
        if (bdrFields.containsKey(eventID)) {
          bdrRecs.put(eventID, bdrFields.get(eventID));
        }
        if (bdrFields.containsKey(deliverableID)) {
          bdrRecs.put(deliverableID, bdrFields.get(deliverableID));
        }

        if (bdrFields.containsKey(deliverableID))
          {               
            GUIManagedObject deliverableObject = deliverableService
                .getStoredDeliverable(String.valueOf(bdrFields.get(deliverableID)));
            if (deliverableObject instanceof Deliverable)
              {
                bdrRecs.put(deliverableDisplay, ((Deliverable) deliverableObject).getDeliverableDisplay());                   
              }
            else {
              bdrRecs.put(deliverableDisplay, "");
            }
          }
        if (bdrFields.containsKey(deliverableQty))
          {
            bdrRecs.put(deliverableQty, bdrFields.get(deliverableQty));
          }
        if (bdrFields.containsKey(deliverableExpirationDate))
          {
            if (bdrFields.get(deliverableExpirationDate) != null)
              {
                Object deliverableExpirationDateObj = bdrFields.get(deliverableExpirationDate);
                if (deliverableExpirationDateObj instanceof String)
                  {
                    String deliverableExpirationDateStr = (String) deliverableExpirationDateObj;
                    // TEMP fix for BLK : reformat date with correct template.
                    // current format comes from ES and is :
                    // 2020-04-20T09:51:38.953Z
                    try
                    {
                      Date date = parseSDF1.parse(deliverableExpirationDateStr);
                      bdrRecs.put(deliverableExpirationDate, ReportsCommonCode.getDateString(date)); // replace with new value
                    }
                    catch (ParseException e1)
                    {
                      // Could also be 2019-11-27 15:39:30.276+0100
                      try
                      {
                        Date date = parseSDF2.parse(deliverableExpirationDateStr);
                        bdrRecs.put(deliverableExpirationDate, ReportsCommonCode.getDateString(date)); // replace with new value
                      }
                      catch (ParseException e2)
                      {
                        log.info("Unable to parse " + deliverableExpirationDateStr);
                      }
                    }

                  }
                else
                  {
                    log.info(deliverableExpirationDate + " is of wrong type : "
                        + deliverableExpirationDateObj.getClass().getName());
                  }
              }
            else {
              bdrRecs.put(deliverableExpirationDate, "");
            }
          }

        if (bdrFields.containsKey(deliveryRequestID))
          {
            bdrRecs.put(deliveryRequestID, bdrFields.get(deliveryRequestID));
          }
        if (bdrFields.containsKey(originatingDeliveryRequestID))
          {
            bdrRecs.put(originatingDeliveryRequestID, bdrFields.get(originatingDeliveryRequestID));
          }
       
        if (bdrFields.containsKey(eventDatetime))
          {
            if (bdrFields.get(eventDatetime) != null)
              {
                Object eventDatetimeObj = bdrFields.get(eventDatetime);
                if (eventDatetimeObj instanceof String)
                  {
                    // TEMP fix for BLK : reformat date with correct
                    // template.

                    bdrRecs.put(eventDatetime, ReportsCommonCode.parseDate((String) eventDatetimeObj));

                    // END TEMP fix for BLK
                  }
                else
                  {
                    log.info(eventDatetime + " is of wrong type : " + eventDatetimeObj.getClass().getName());
                  }

              }
            else {
              bdrRecs.put(eventDatetime, "");
            }
          }

        // Compute featureName and ModuleName from ID
        if (bdrFields.containsKey(moduleId) && bdrFields.containsKey(featureId))
          {
            Module module = Module.fromExternalRepresentation(String.valueOf(bdrFields.get(moduleId)));
            String featureDis = DeliveryRequest.getFeatureDisplay(module, String.valueOf(bdrFields.get(featureId).toString()), journeyService, offerService, loyaltyProgramService);                
            bdrRecs.put(featureDisplay, featureDis);
            bdrRecs.put(moduleName, module.toString());
            bdrRecs.put(featureId, bdrFields.get(featureId));
            bdrRecs.put(moduleId, bdrFields.get(moduleId));

            // bdrFields.remove(featureId);
            // bdrFields.remove(moduleId);
          }

            if (bdrFields.containsKey(operation))
              {
                bdrRecs.put(operation, bdrFields.get(operation));
              }
            if (bdrFields.containsKey(orgin))
              {
                bdrRecs.put(orgin, bdrFields.get(orgin));
              }

            if (bdrFields.containsKey(providerId))
              {
                String providerNam = Deployment.getFulfillmentProviders().get(bdrFields.get(providerId)).getProviderName();
                bdrRecs.put(providerName, providerNam);
              }
            if (bdrFields.containsKey(returnCode))
              {
                Object code = bdrFields.get(returnCode);
                bdrRecs.put(returnCode, code);
                bdrRecs.put(returnCodeDescription, (code != null && code instanceof Integer) ? RESTAPIGenericReturnCodes.fromGenericResponseCode((int) code).getGenericResponseMessage() : "");
                bdrRecs.put(returnCodeDetails, bdrFields.get(returnCodeDetails));
                if (code instanceof Integer && code != null)
                  {
                    int codeInt = (int) code;
                    bdrRecs.put(deliveryStatus, (codeInt == 0) ? DeliveryManager.DeliveryStatus.Delivered.toString() : DeliveryManager.DeliveryStatus.Failed.toString());
                  }
              }
            
            //
            // result
            //

        //
        // result
        //

        String rawEventDateTime = bdrRecs.get(eventDatetime) == null ? null : bdrRecs.get(eventDatetime).toString();
        if (rawEventDateTime == null) log.warn("bad EventDateTime -- report will be generated in 'null' file name -- for record {} ", bdrFields);
        String evntDate = getEventDate(rawEventDateTime);
        if (result.containsKey(evntDate))
          {
            result.get(evntDate).add(bdrRecs);
          } 
        else
          {
            List<Map<String, Object>> elements = new ArrayList<Map<String, Object>>();
            elements.add(bdrRecs);
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
  
  private static List<String> getEsIndexDates(final Date fromDate, Date toDate)
  {
    Date tempfromDate = fromDate;
    List<String> esIndexOdrList = new ArrayList<String>();
    // to get the reports with yesterday's date only
    while(tempfromDate.getTime() < toDate.getTime())
      {
        esIndexOdrList.add(DATE_FORMAT.format(tempfromDate));
        tempfromDate = RLMDateUtils.addDays(tempfromDate, 1, Deployment.getBaseTimeZone());
      }
    return esIndexOdrList;
  }
  
  private static Date getFromDate(final Date reportGenerationDate, String reportPeriodUnit, Integer reportPeriodQuantity)
  {
    reportPeriodQuantity = reportPeriodQuantity == null || reportPeriodQuantity == 0 ? new Integer(1) : reportPeriodQuantity;
    if (reportPeriodUnit == null) reportPeriodUnit = PERIOD.DAYS.getExternalRepresentation();
    Date now = reportGenerationDate;
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
    if (fromDate != null) fromDate = RLMDateUtils.truncate(now, Calendar.DATE, com.evolving.nglm.core.Deployment.getBaseTimeZone());
    return fromDate;
  }
  
  public static void main(String[] args, final Date reportGenerationDate)
  {
    BDRReportMonoPhase bdrReportMonoPhase = new BDRReportMonoPhase();
    bdrReportMonoPhase.start(args, reportGenerationDate);
  }
  
  private void start(String[] args, final Date reportGenerationDate)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("BDRReportESReader: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : BDRReportMonoPhase <ESNode> <ES BDR index> <csvfile> <defaultReportPeriodQuantity> <defaultReportPeriodUnit>");
        return;
      }
    String esNode     = args[0];
    String esIndexBdr = args[1];
    String csvfile    = args[2];
    Integer reportPeriodQuantity = 0;
    String reportPeriodUnit = null;
    if (args.length > 4 && args[3] != null && args[4] != null)
      {
        reportPeriodQuantity = Integer.parseInt(args[3]);
        reportPeriodUnit = args[4];
      }
    
    Date fromDate = getFromDate(reportGenerationDate, reportPeriodUnit, reportPeriodQuantity);
    Date toDate = reportGenerationDate;
    
    Set<String> esIndexWeeks = ReportCsvFactory.getEsIndexWeeks(fromDate, toDate);
    StringBuilder esIndexBdrList = new StringBuilder();
    boolean firstEntry = true;
    for (String esIndexDate : esIndexWeeks)
      {
        if (!firstEntry) esIndexBdrList.append(",");
        String indexName = esIndexBdr + esIndexDate;
        esIndexBdrList.append(indexName);
        firstEntry = false;
      }

    log.info("Reading data from ES in (" + esIndexBdrList.toString() + ") and writing to " + csvfile);

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexBdrList.toString(), QueryBuilders.rangeQuery("eventDatetime").gte(fromDate).lte(toDate)); 

    String deliverableServiceTopic = Deployment.getDeliverableTopic();
    String offerTopic = Deployment.getOfferTopic();
    String journeyTopic = Deployment.getJourneyTopic();
    String loyaltyProgramTopic = Deployment.getLoyaltyProgramTopic();

    deliverableService    = new DeliverableService(Deployment.getBrokerServers(), "bdrreportcsvwriter-deliverableserviceservice-BDRReportMonoPhase", deliverableServiceTopic, false);
    journeyService        = new JourneyService(Deployment.getBrokerServers(), "bdrreportcsvwriter-journeyservice-BDRReportMonoPhase", journeyTopic, false);
    offerService          = new OfferService(Deployment.getBrokerServers(), "bdrreportcsvwriter-offerservice-BDRReportMonoPhase", offerTopic, false);
    loyaltyProgramService = new LoyaltyProgramService(Deployment.getBrokerServers(), "bdrreportcsvwriter-loyaltyprogramservice-BDRReportMonoPhase", loyaltyProgramTopic, false);

    deliverableService.start();
    journeyService.start();
    offerService.start();
    loyaltyProgramService.start();

    try {
      ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
          esNode,
          esIndexWithQuery,
          this,
          csvfile
          );

      // check if a report with multiple dates is required in the zipped file 
      boolean isMultiDates = false;
      if (reportPeriodQuantity > 1)
        {
          isMultiDates = true;
        }

      if (!reportMonoPhase.startOneToOne(isMultiDates))
        {
          log.warn("An error occured, the report " + csvfile + "  might be corrupted");
          return;
        }

    } finally {
      deliverableService.stop();
      journeyService.stop();
      offerService.stop();
      loyaltyProgramService.stop();
      log.info("The report " + csvfile + " is finished");
    }
  }
}
