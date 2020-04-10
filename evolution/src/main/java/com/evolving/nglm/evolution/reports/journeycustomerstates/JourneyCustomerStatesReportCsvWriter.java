package com.evolving.nglm.evolution.reports.journeycustomerstates;

import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.JourneyNode;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.zip.ZipOutputStream;

/**
 * This implements the phase 3 for the Journey report.
 *
 */
public class JourneyCustomerStatesReportCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatesReportCsvWriter.class);
  final private static String CSV_SEPARATOR = ReportUtils.getSeparator();
  private static JourneyService journeyService;
  List<String> headerFieldsOrder = new ArrayList<String>();
  private final String subscriberID = "subscriberID";
  private final String customerID = "customerID";

  /**
   * This methods writes a single {@link ReportElement} to the report (csv file).
   * 
   * @throws IOException
   *           in case anything goes wrong while writing to the report.
   */
  public void dumpElementToCsv(String key, ReportElement re, ZipOutputStream writer, boolean addHeaders) throws IOException
  {
    //
    //
    //
  }
  
  public void dumpLineToCsv(Map<String, Object> lineMap, ZipOutputStream writer, boolean addHeaders)
  {

    try
      {
        if (addHeaders)
          {
            addHeaders(writer, lineMap.keySet(), 1);
          }
        String line = ReportUtils.formatResult(lineMap);
        log.trace("Writing to csv file : " + line);
        writer.write(line.getBytes());
        writer.write("\n".getBytes());
      } 
    catch (IOException e)
      {
        e.printStackTrace();
      }
  }

  public Map<String, List<Map<String, Object>>> getSplittedReportElementsForFile(ReportElement reportElement)
  {
    Map<String, List<Map<String, Object>>> result = new LinkedHashMap<String, List<Map<String, Object>>>();
    Map<String, Object> journeyStatsMap = reportElement.fields.get(0);
    Map<String, Object> subscriberFields = reportElement.fields.get(1);
    for (Object journeyStatsObj : journeyStatsMap.values())
      {
        Map<String, Object> journeyStats = (Map<String, Object>) journeyStatsObj;
        if (journeyStats != null && !journeyStats.isEmpty() && subscriberFields != null && !subscriberFields.isEmpty())
          {
            Journey journey = journeyService.getActiveJourney(journeyStats.get("journeyID").toString(), SystemTime.getCurrentTime());
            if (journey != null)
              {
                Map<String, Object> journeyInfo = new LinkedHashMap<String, Object>();
                if (journeyStats.get(subscriberID) != null)
                  {
                    Object subscriberIDField = journeyStats.get(subscriberID);
                    journeyInfo.put(customerID, subscriberIDField);
                  }
                for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                  {
                    if (subscriberFields.get(alternateID.getESField()) != null)
                      {
                        Object alternateId = subscriberFields.get(alternateID.getESField());
                        journeyInfo.put(alternateID.getName(), alternateId);
                      }
                  }
                journeyInfo.put("journeyID", journey.getJourneyID());
                journeyInfo.put("journeyName", journey.getGUIManagedObjectDisplay());
                journeyInfo.put("journeyType", journey.getTargetingType());

                if (journeyStats.get("sample") != null)
                  {
                    journeyInfo.put("sample", journeyStats.get("sample"));
                  }

                boolean statusNotified = (boolean) journeyStats.get("statusNotified");
                boolean journeyComplete = (boolean) journeyStats.get("journeyComplete");
                boolean statusConverted = (boolean) journeyStats.get("statusConverted");
                Boolean statusTargetGroup  = journeyStats.get("statusTargetGroup")  == null ? null : (boolean) journeyStats.get("statusTargetGroup");
                Boolean statusControlGroup = journeyStats.get("statusControlGroup") == null ? null : (boolean) journeyStats.get("statusControlGroup");
                Boolean statusUniversalControlGroup = journeyStats.get("statusUniversalControlGroup") == null ? null : (boolean) journeyStats.get("statusUniversalControlGroup");

                journeyInfo.put("customerStatus", getSubscriberJourneyStatus(journeyComplete, statusConverted, statusNotified, statusTargetGroup, statusControlGroup, statusUniversalControlGroup).getDisplay());

                List<String> nodeHistory = (List<String>) journeyStats.get("nodeHistory");
                StringBuilder sbStatus = new StringBuilder();
                if (nodeHistory != null && !nodeHistory.isEmpty())
                  {
                    for (String status : nodeHistory)
                      {
                        if (status != null)
                          {
                            String[] split = status.split(";");
                            String fromNodeName = decodeNodeName(journey, split, 0);
                            String toNodeName   = decodeNodeName(journey, split, 1);
                            Date   date         = decodeDate(split, 2);
                            sbStatus.append("(").append(fromNodeName).append("->").append(toNodeName).append(",").append(ReportsCommonCode.getDateString(date)).append("),");
                          }
                      }
                  }

                String states = null;
                if (sbStatus.length() > 0)
                  {
                    states = sbStatus.toString().substring(0, sbStatus.toString().length() - 1);
                  }

                StringBuilder sbStatuses = new StringBuilder();
                List<String> statusHistory = (List<String>) journeyStats.get("statusHistory");
                if (statusHistory != null && !statusHistory.isEmpty())
                  {
                    for (String status : statusHistory)
                      {
                        String statusNameToBeDisplayed = "";
                        String[] split = status.split(";");
                        String statusName = null;
                        if (split[0] != null && !split[0].equals("null"))
                          {
                            statusName = split[0];
                          }
                        Date date = decodeDate(split, 1);
                        sbStatuses.append("(").append(SubscriberJourneyStatus.fromExternalRepresentation(statusName).getDisplay()).append(",").append(ReportsCommonCode.getDateString(date)).append("),");
                      }
                  }

                String statuses = null;
                if (sbStatuses.length() > 0)
                  {
                    statuses = sbStatuses.toString().substring(0, sbStatuses.toString().length() - 1);
                  }

                journeyInfo.put("customerStates",   states);
                journeyInfo.put("customerStatuses", statuses);
                journeyInfo.put("dateTime",         ReportsCommonCode.getDateString(SystemTime.getCurrentTime()));
                journeyInfo.put("startDate",        ReportsCommonCode.getDateString(journey.getEffectiveStartDate()));
                journeyInfo.put("endDate",          ReportsCommonCode.getDateString(journey.getEffectiveEndDate()));

                List<String> rewardHistory = (List<String>) journeyStats.get("rewardHistory");
                List<Map<String, Object>> outputJSON = new ArrayList<>();

                if (rewardHistory != null && !rewardHistory.isEmpty())
                  {
                    for (String status : rewardHistory)
                      {
                        Map<String, Object> historyJSON = new LinkedHashMap<>(); // to preserve order when displaying
                        String[] split = status.split(";");
                        String rewardID = null;
                        String amount   = null;
                        Date   date     = null;
                        if (split != null && split.length >= 3)
                          {
                            rewardID = split[0];
                            amount   = split[1];
                            date     = decodeDate(split, 2);
                          }
                        historyJSON.put("reward", rewardID);
                        historyJSON.put("quantity", amount);
                        historyJSON.put("date", ReportsCommonCode.getDateString(date));
                        outputJSON.add(historyJSON);
                      }
                  }
                journeyInfo.put("rewards", ReportUtils.formatJSON(outputJSON));

                //
                // result
                //

                String journeyID = journeyInfo.get("journeyID").toString();
                if (result.containsKey(journeyID))
                  {
                    result.get(journeyID).add(journeyInfo);
                  } 
                else
                  {
                    List<Map<String, Object>> elements = new ArrayList<Map<String, Object>>();
                    elements.add(journeyInfo);
                    result.put(journeyID, elements);
                  }
              }
          }
      }
    return result;
  }

  private String decodeNodeName(Journey journey, String[] split, int index)
  {
    String nodeName = null; // default value if error
    if (split[index] != null && !split[index].equals("null"))
      {
        JourneyNode journeyNode = journey.getJourneyNode(split[index]);
        if (journeyNode == null)
          {
            log.info("unknown journey node with name " + split[index]);
          }
        else
          {
            nodeName = journeyNode.getNodeName();
          }
      }
    return nodeName;
  }
  
  private Date decodeDate(String[] split, int index)
  {
    Date date = null;
    if (split[index] != null && !split[index].equals("null"))
      {
        try
          {
            date = new Date(Long.valueOf(split[index]));
          }
        catch (Exception e)
          {
            log.info("unable to convert to date : " + split[index]);
          }
      }
    return date;
  }

  
  public static void main(String[] args)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("JourneyCustomerStatesReportCsvWriter: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : JourneyCustomerStatesReportCsvWriter <KafkaNode> <topic in> <csvfile>");
        return;
      }
    String kafkaNode = args[0];
    String topic = args[1];
    String csvfile = args[2];
    log.info("Reading data from " + topic + " topic on broker " + kafkaNode + " producing " + csvfile + " with '" + CSV_SEPARATOR + "' separator");

    ReportCsvFactory reportFactory = new JourneyCustomerStatesReportCsvWriter();
    ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);

    String journeyTopic = Deployment.getJourneyTopic();

    journeyService = new JourneyService(kafkaNode, "journeycustomerstatesreportcsvwriter-journeyservice-" + topic, journeyTopic, false);
    journeyService.start();

    if (!reportWriter.produceReport(csvfile, true))
      {
        log.warn("An issue occured while producing the report");
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
      }
  }

  public SubscriberJourneyStatus getSubscriberJourneyStatus(boolean journeyComplete, boolean statusConverted, boolean statusNotified, Boolean statusTargetGroup, Boolean statusControlGroup, Boolean statusUniversalControlGroup)
  {
    return Journey.getSubscriberJourneyStatus(journeyComplete, statusConverted, statusNotified, statusTargetGroup, statusControlGroup, statusUniversalControlGroup);
  }
}


