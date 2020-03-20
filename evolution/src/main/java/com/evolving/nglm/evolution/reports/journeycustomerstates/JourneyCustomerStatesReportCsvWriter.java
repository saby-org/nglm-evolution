package com.evolving.nglm.evolution.reports.journeycustomerstates;

import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
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
    if (re.type == ReportElement.MARKER) // We will find markers in the topic
      return;
    if (!re.isComplete)
      return;
    log.trace("We got " + key + " " + re);
    Map<String, Object> journeyStatsMap = re.fields.get(0);
    Map<String, Object> subscriberFields = re.fields.get(1);
    for (Object journeyStatsObj : journeyStatsMap.values()) // we don't care
                                                            // about the keys
      {
        Map<String, Object> journeyStats = (Map<String, Object>) journeyStatsObj;
        if (journeyStats != null && !journeyStats.isEmpty() && subscriberFields != null && !subscriberFields.isEmpty())
          {

            GUIManagedObject guiOb = journeyService.getStoredJourney(journeyStats.get("journeyID").toString());
            if (guiOb instanceof Journey)
              {
                Journey journey = (Journey) guiOb;

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
                if (journey != null)
                  {
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
                    Boolean statusTargetGroup = journeyStats.get("statusTargetGroup") == null ? null : (boolean) journeyStats.get("statusTargetGroup");
                    Boolean statusControlGroup = journeyStats.get("statusControlGroup") == null ? null : (boolean) journeyStats.get("statusControlGroup");
                    Boolean statusUniversalControlGroup = journeyStats.get("statusUniversalControlGroup") == null ? null : (boolean) journeyStats.get("statusUniversalControlGroup");

                    journeyInfo.put("customerStatus", getSubscriberJourneyStatus(journeyComplete, statusConverted, statusNotified, statusTargetGroup, statusControlGroup, statusUniversalControlGroup).toString());

                    List<String> nodeHistory = (List<String>) journeyStats.get("nodeHistory");
                    StringBuilder sbStatus = new StringBuilder();
                    if (nodeHistory != null && !nodeHistory.isEmpty())
                      {
                        for (String status : nodeHistory)
                          {
                            String[] split = status.split(";");

                            String fromNodeName = null;
                            if (split[0] != null && !split[0].equals("null"))
                              {
                                fromNodeName = journey.getJourneyNode(split[0]).getNodeName();
                              }

                            String toNodeName = null;
                            if (split[1] != null && !split[1].equals("null"))
                              {
                                toNodeName = journey.getJourneyNode(split[1]).getNodeName();
                              }

                            Date date = null;
                            if (split[2] != null && !split[2].equals("null"))
                              {
                                date = new Date(Long.valueOf(split[2]));
                              }

                            sbStatus.append("(").append(fromNodeName).append("->").append(toNodeName).append(",").append(date).append("),");
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

                            Date date = null;
                            if (split[1] != null && !split[1].equals("null"))
                              {
                                date = new Date(Long.valueOf(split[1]));
                              }

                            sbStatuses.append("(").append(SubscriberJourneyStatus.fromExternalRepresentation(statusName)).append(",").append(date).append("),");

                          }
                      }

                    String statuses = null;
                    if (sbStatuses.length() > 0)
                      {
                        statuses = sbStatuses.toString().substring(0, sbStatuses.toString().length() - 1);
                      }

                    journeyInfo.put("customerStates", states);
                    journeyInfo.put("customerStatuses", statuses);
                    journeyInfo.put("dateTime", ReportsCommonCode.getDateString(SystemTime.getCurrentTime()));
                    journeyInfo.put("startDate", ReportsCommonCode.getDateString(journey.getEffectiveStartDate()));
                    journeyInfo.put("endDate", ReportsCommonCode.getDateString(journey.getEffectiveEndDate()));

                    List<String> rewardHistory = (List<String>) journeyStats.get("rewardHistory");
                    StringBuilder sbHistory = new StringBuilder();
                    if (rewardHistory != null && !rewardHistory.isEmpty())
                      {
                        for (String status : rewardHistory)
                          {
                            String[] split = status.split(";");
                            String rewardID = split[0];
                            String amount = split[1];
                            Date date = new Date(Long.valueOf(split[2]));
                            sbHistory.append("(").append(rewardID).append(",").append(amount).append(",").append(date).append("),");
                          }
                      }

                    String history = null;
                    if (sbHistory.length() > 0)
                      {
                        history = sbHistory.toString().substring(0, sbHistory.toString().length() - 1);
                      }
                    journeyInfo.put("rewards", history);
                  }

                /*
                 * if(addHeaders){ headerFieldsOrder.clear(); addHeaders(writer,
                 * subscriberFields, 0); addHeaders(writer, journeyInfo, 1); }
                 */
                if (addHeaders)
                  {
                    addHeaders(writer, journeyInfo.keySet(), 1);
                  }
                String line = ReportUtils.formatResult(journeyInfo);
                log.trace("Writing to csv file : " + line);
                writer.write(line.getBytes());
                writer.write("\n".getBytes());
              }
          }
      }
  }
  
  public String getFileSplitBy(ReportElement re)
  {
    String result = null;
    Map<String, Object> journeyStatsMap = re.fields.get(0);
    for (Object journeyStatsObj : journeyStatsMap.values())
      {
        Map<String, Object> journeyStats = (Map<String, Object>) journeyStatsObj;
        result = journeyStats.get("journeyID").toString();
        break;
      }
    log.info("RAJ getFileSplitBy {}", result);
    return result;
    
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
