package com.evolving.nglm.evolution.reports.bgdr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.Badge;
import com.evolving.nglm.evolution.BadgeObjectiveInstance;
import com.evolving.nglm.evolution.BadgeObjectiveService;
import com.evolving.nglm.evolution.DeliveryManager;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.RESTAPIGenericReturnCodes;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportMonoPhase.PERIOD;
import com.evolving.nglm.evolution.reports.ReportUtils;

public class BGDRReportMonoPhase implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(BGDRReportMonoPhase.class);

  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  private SalesChannelService salesChannelService;
  private BadgeObjectiveService badgeObjectiveService = null;
  private JourneyService journeyService;
  private OfferService offerService;
  private LoyaltyProgramService loyaltyProgramService;
  private int tenantID = 0;

  private final static String badgeID = "badgeID";
  private final static String badgeDisplay = "badgeDisplay";
  private final static String badgeType = "badgeType";
  private final static String badgeObjective = "badgeObjective";
  private final static String operation = "operation";
  private final static String moduleId = "moduleID";
  private final static String featureId = "featureID";
  private final static String moduleName = "moduleName";
  private final static String featureName = "featureName";
  private final static String subscriberID = "subscriberID";
  private final static String customerID = "customerID";
  private static final String eventDatetime = "eventDatetime";
  private static final String deliveryRequestID = "deliveryRequestID";
  private static final String eventID = "eventID";
  private static final String origin = "origin";
  private static final String returnCode = "returnCode";
  private static final String returnCodeDescription = "returnCodeDescription";
  private static final String deliveryStatus = "deliveryStatus";

  static List<String> headerFieldsOrder = new LinkedList<String>();
  static
    {
      headerFieldsOrder.add(customerID);
      for (AlternateID alternateID : Deployment.getAlternateIDs().values())
        {
          headerFieldsOrder.add(alternateID.getName());
        }
      headerFieldsOrder.add(badgeID);
      headerFieldsOrder.add(badgeDisplay);
      headerFieldsOrder.add(badgeType);
      headerFieldsOrder.add(badgeObjective);
      headerFieldsOrder.add(operation);
      headerFieldsOrder.add(moduleId);
      headerFieldsOrder.add(featureId);
      headerFieldsOrder.add(moduleName);
      headerFieldsOrder.add(featureName);
      headerFieldsOrder.add(origin);
      headerFieldsOrder.add(eventID);
      headerFieldsOrder.add(eventDatetime);
      headerFieldsOrder.add(deliveryRequestID);
      headerFieldsOrder.add(returnCode);
      headerFieldsOrder.add(returnCodeDescription);
      headerFieldsOrder.add(deliveryStatus);
    }

  /****************************************
   *
   * dumpElementToCsv
   *
   ****************************************/
  public boolean dumpElementToCsvMono(Map<String, Object> map, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
    Map<String, List<Map<String, Object>>> mapLocal = getSplittedReportElementsForFileMono(map);
    if (mapLocal.size() != 1)
      {
        log.debug("We have multiple dates in the same index " + mapLocal.size());
      } 
    else
      {
        if (mapLocal.values().size() != 1)
          {
            log.debug("We have multiple values for this date " + mapLocal.values().size());
          } 
        else
          {
            Set<Entry<String, List<Map<String, Object>>>> setLocal = mapLocal.entrySet();
            if (setLocal.size() != 1)
              {
                log.debug("We have multiple dates in this report " + setLocal.size());
              } 
            else
              {
                for (Entry<String, List<Map<String, Object>>> entry : setLocal)
                  {
                    List<Map<String, Object>> list = entry.getValue();

                    if (list.size() != 1)
                      {
                        log.debug("We have multiple reports in this folder " + list.size());
                      } 
                    else
                      {
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
    Map<String, Object> bgdrFields = map;
    LinkedHashMap<String, Object> bgdrRecs = new LinkedHashMap<>();
    if (bgdrFields != null && !bgdrFields.isEmpty())
      {
        if (bgdrFields.get(subscriberID) != null)
          {
            Object subscriberIDField = bgdrFields.get(subscriberID);
            bgdrRecs.put(customerID, subscriberIDField);
          }
        for (AlternateID alternateID : Deployment.getAlternateIDs().values())
          {
            if (bgdrFields.get(alternateID.getID()) != null)
              {
                Object alternateId = bgdrFields.get(alternateID.getID());
                bgdrRecs.put(alternateID.getName(), alternateId);
              }
          }

        //
        // ESVal
        //

        String deliveryRequestIDESVal = (String) bgdrFields.get(deliveryRequestID);
        String badgeIDESVal = (String) bgdrFields.get(badgeID);
        String operationESVal = (String) bgdrFields.get("action");
        Object eventDatetimeObjESVal = bgdrFields.get(eventDatetime);
        String moduleIdESVal = (String) bgdrFields.get(moduleId);
        String featureIdESVal = (String) bgdrFields.get(featureId);
        String eventIDESVal = (String) bgdrFields.get(eventID);
        String originESVal = (String) bgdrFields.get(origin);
        Integer returnCodeESVal = (Integer) bgdrFields.get(returnCode);
        String deliveryStatusVal = 0 == returnCodeESVal ? DeliveryManager.DeliveryStatus.Delivered.toString() : DeliveryManager.DeliveryStatus.Failed.toString();

        //
        // derived
        //

        String badgeDisplayVal = "";
        String badgeTypeVal = "";
        StringBuilder badgeObjectives = new StringBuilder();
        if (badgeIDESVal != null)
          {
            GUIManagedObject guiManagedObject = loyaltyProgramService.getStoredLoyaltyProgram(badgeIDESVal, true);
            if (guiManagedObject != null)
              badgeDisplayVal = guiManagedObject.getGUIManagedObjectDisplay();
            if (guiManagedObject != null && guiManagedObject.getAccepted())
              {
                Badge badgeChecked = ((Badge) guiManagedObject);
                Set<BadgeObjectiveInstance> badgeObjectiveValues = badgeChecked.getBadgeObjectives();
                badgeTypeVal = badgeChecked.getBadgeType().getExternalRepresentation();
                if (badgeObjectiveValues != null && !badgeObjectiveValues.isEmpty())
                  {
                    for (BadgeObjectiveInstance badgeObjective : badgeObjectiveValues)
                      {
                        GUIManagedObject guiManagedObjectBadge = badgeObjectiveService.getStoredBadgeObjective(badgeObjective.getBadgeObjectiveID(), true);
                        if (guiManagedObjectBadge != null)
                          {
                            badgeObjectives.append(guiManagedObjectBadge.getGUIManagedObjectDisplay()).append(",");
                          }
                      }

                  }
              }

          }
        String badgeObjectivesValue = null;
        if (badgeObjectives.length() > 0)
          {
            badgeObjectivesValue = badgeObjectives.toString().substring(0, badgeObjectives.toString().length() - 1);
          }
        Module module = Module.fromExternalRepresentation(String.valueOf(moduleIdESVal));
        String featureESVal = DeliveryRequest.getFeatureDisplay(module, String.valueOf(bgdrFields.get(featureId).toString()), journeyService, offerService, loyaltyProgramService);

        //
        //  bgdrRecs
        //
        
        bgdrRecs.put(badgeID, badgeIDESVal);
        bgdrRecs.put(badgeDisplay, badgeDisplayVal);
        bgdrRecs.put(badgeType, badgeTypeVal);
        bgdrRecs.put(badgeObjective, badgeObjectivesValue);
        bgdrRecs.put(operation, operationESVal);
        bgdrRecs.put(moduleId, moduleIdESVal);
        bgdrRecs.put(featureId, featureIdESVal);
        bgdrRecs.put(moduleName, module.toString());
        bgdrRecs.put(featureName, featureESVal);
        bgdrRecs.put(origin, originESVal);
        bgdrRecs.put(eventID, eventIDESVal);
        bgdrRecs.put(eventDatetime, eventDatetimeObjESVal);
        bgdrRecs.put(deliveryRequestID, deliveryRequestIDESVal);
        bgdrRecs.put(returnCode, returnCodeESVal);
        bgdrRecs.put(returnCodeDescription, RESTAPIGenericReturnCodes.fromGenericResponseCode(returnCodeESVal).getGenericResponseMessage());
        bgdrRecs.put(deliveryStatus, deliveryStatusVal);

        //
        // result
        //

        String rawEventDateTime = bgdrRecs.get(eventDatetime) == null ? null : bgdrRecs.get(eventDatetime).toString();
        if (rawEventDateTime == null)
          log.warn("bad EventDateTime -- report (bgdr) will be generated in 'null' file name -- for record {} ", bgdrFields);
        String evntDate = getEventDate(rawEventDateTime);
        if (result.containsKey(evntDate))
          {
            result.get(evntDate).add(bgdrRecs);
          } else
          {
            List<Map<String, Object>> elements = new ArrayList<Map<String, Object>>();
            elements.add(bgdrRecs);
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

  public static void main(String[] args, final Date reportGenerationDate)
  {
    BGDRReportMonoPhase bgdrReportMonoPhase = new BGDRReportMonoPhase();
    bgdrReportMonoPhase.start(args, reportGenerationDate);
  }

  private void start(String[] args, final Date reportGenerationDate)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("BGDRReportMonoPhase: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : BGDRReportMonoPhase <ESNode> <ES journey index> <csvfile> <defaultReportPeriodQuantity> <defaultReportPeriodUnit>");
        return;
      }
    String esNode = args[0];
    String esIndexBgdr = args[1];
    String csvfile = args[2];

    Integer reportPeriodQuantity = 0;
    String reportPeriodUnit = null;
    if (args.length > 4 && args[3] != null && args[4] != null)
      {
        reportPeriodQuantity = Integer.parseInt(args[3]);
        reportPeriodUnit = args[4];
      }
    if (args.length > 5) tenantID = Integer.parseInt(args[5]);
    Date fromDate = getFromDate(reportGenerationDate, reportPeriodUnit, reportPeriodQuantity);
    Date toDate = reportGenerationDate;

    Set<String> esIndexWeeks = ReportCsvFactory.getEsIndexWeeks(fromDate, toDate);
    StringBuilder esIndexBgdrList = new StringBuilder();
    boolean firstEntry = true;
    for (String esIndexWk : esIndexWeeks)
      {
        if (!firstEntry)
          esIndexBgdrList.append(",");
        String indexName = esIndexBgdr + esIndexWk;
        esIndexBgdrList.append(indexName);
        firstEntry = false;
      }
    log.info("Reading data from ES in (" + esIndexBgdrList.toString() + ")  index and writing to " + csvfile);
    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(esIndexBgdrList.toString(), QueryBuilders.boolQuery()
        .filter(QueryBuilders.termQuery("tenantID", tenantID))
        .filter(QueryBuilders.rangeQuery("eventDatetime").gte(RLMDateUtils.formatDateForElasticsearchDefault(fromDate)).lte(RLMDateUtils.formatDateForElasticsearchDefault(toDate))));

    String salesChannelTopic = Deployment.getSalesChannelTopic();
    String offerTopic = Deployment.getOfferTopic();
    String journeyTopic = Deployment.getJourneyTopic();
    String loyaltyProgramTopic = Deployment.getLoyaltyProgramTopic();

    salesChannelService = new SalesChannelService(Deployment.getBrokerServers(), "bgdrreportcsvwriter-saleschannelservice-BGDRReportMonoPhase", salesChannelTopic, false);
    badgeObjectiveService = new BadgeObjectiveService(Deployment.getBrokerServers(), "bgdrreportcsvwriter-badgeObjectiveService-BGDRReportMonoPhase", Deployment.getBadgeObjectiveTopic(), false);
    journeyService = new JourneyService(Deployment.getBrokerServers(), "bgdrreportcsvwriter-journeyService-BGDRReportMonoPhase", journeyTopic, false);
    offerService = new OfferService(Deployment.getBrokerServers(), "bgdrreportcsvwriter-offerService-BGDRReportMonoPhase", offerTopic, false);
    loyaltyProgramService = new LoyaltyProgramService(Deployment.getBrokerServers(), "bgdrreportcsvwriter-loyaltyProgramService-BGDRReportMonoPhase", loyaltyProgramTopic, false);
    badgeObjectiveService.start();
    salesChannelService.start();
    journeyService.start();
    offerService.start();
    loyaltyProgramService.start();

    try
      {
        ReportMonoPhase reportMonoPhase = new ReportMonoPhase(esNode, esIndexWithQuery, this, csvfile);

        // check if a report with multiple dates is required in the zipped file
        boolean isMultiDates = false;
        if (reportPeriodQuantity > 1)
          {
            isMultiDates = true;
          }

        if (!reportMonoPhase.startOneToOne(isMultiDates))
          {
            log.warn("An error occured, the report " + csvfile + "  might be corrupted");
            throw new RuntimeException("An error occurred, report must be restarted");
          }
      } 
    finally
      {
        salesChannelService.stop();
        badgeObjectiveService.stop();
        journeyService.stop();
        offerService.stop();
        loyaltyProgramService.stop();
        log.info("The report " + csvfile + " is finished");
      }

  }

  @Deprecated // TO BE FACTORIZED
  public static List<String> getEsIndexDates(final Date fromDate, Date toDate)
  {
    Date tempfromDate = fromDate;
    List<String> esIndexBgdrList = new ArrayList<String>();
    while (tempfromDate.getTime() < toDate.getTime())
      {
        esIndexBgdrList.add(RLMDateUtils.formatDateDay(tempfromDate, Deployment.getDefault().getTimeZone())); // TODO EVPRO-99
        tempfromDate = RLMDateUtils.addDays(tempfromDate, 1, Deployment.getDefault().getTimeZone()); // TODO EVPRO-99 
      }
    return esIndexBgdrList;
  }

  private static Date getFromDate(final Date reportGenerationDate, String reportPeriodUnit, Integer reportPeriodQuantity)
  {
    reportPeriodQuantity = reportPeriodQuantity == null || reportPeriodQuantity == 0 ? new Integer(1) : reportPeriodQuantity;
    if (reportPeriodUnit == null) reportPeriodUnit = PERIOD.DAYS.getExternalRepresentation();

    //
    //
    //

    Date now = reportGenerationDate;
    Date fromDate = null;
    switch (reportPeriodUnit.toUpperCase())
    {
      case "DAYS":
        fromDate = RLMDateUtils.addDays(now, -reportPeriodQuantity, Deployment.getDefault().getTimeZone()); // TODO EVPRO-99
        break;

      case "WEEKS":
        fromDate = RLMDateUtils.addWeeks(now, -reportPeriodQuantity, Deployment.getDefault().getTimeZone()); // TODO EVPRO-99
        break;

      case "MONTHS":
        fromDate = RLMDateUtils.addMonths(now, -reportPeriodQuantity, Deployment.getDefault().getTimeZone()); // TODO EVPRO-99
        break;

      default:
        break;
    }
    if (fromDate != null) fromDate = RLMDateUtils.truncate(fromDate, Calendar.DATE, Deployment.getDefault().getTimeZone());
    return fromDate;
  }

  public static String getESAllIndices(String esIndexBgdrInitial)
  {
    return esIndexBgdrInitial + "*";
  }

  /*********************
   * 
   * getESIndices
   *
   ********************/

  public static String getESIndices(String esIndexBgdr, Set<String> esIndexWks)
  {
    StringBuilder esIndexBgdrList = new StringBuilder();
    boolean firstEntry = true;
    for (String esIndexWk : esIndexWks)
      {
        if (!firstEntry) esIndexBgdrList.append(",");
        String indexName = esIndexBgdr + esIndexWk;
        esIndexBgdrList.append(indexName);
        firstEntry = false;
      }
    return esIndexBgdrList.toString();
  }
}
