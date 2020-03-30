package com.evolving.nglm.evolution.reports.journeycustomerstatistics;

import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;

public class JourneyCustomerStatisticsReportCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatisticsReportCsvWriter.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
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

    log.info("We got " + key + " " + re);
    Map<String, Object> journeyStats = re.fields.get(0);
    Map<String, Object> subscriberFieldsMap = re.fields.get(1);
    Map<String, Object> journeyMetric = re.fields.get(2);
    for (Object subscriberFieldsObj : subscriberFieldsMap.values()) // we don't care about the keys
      {
        Map<String, Object> subscriberFields = (Map<String, Object>) subscriberFieldsObj;
        if (journeyStats != null && !journeyStats.isEmpty() && subscriberFields != null && !subscriberFields.isEmpty() && journeyMetric != null && !journeyMetric.isEmpty())
          {

            GUIManagedObject guiOb = journeyService.getStoredJourney(journeyStats.get("journeyID").toString());
            if (guiOb instanceof Journey)
              {
                Journey journey = (Journey) guiOb;

                Map<String, Object> journeyInfo = new LinkedHashMap<String, Object>();
                if (journey != null)
                  {
                    journeyInfo.put("journeyID", journey.getJourneyID());
                    journeyInfo.put("journeyName", journey.getGUIManagedObjectDisplay());
                    journeyInfo.put("journeyType", journey.getTargetingType());
                    journeyInfo.put("customerState", journey.getJourneyNodes().get(journeyStats.get("toNodeID")).getNodeName());
                  }

                boolean statusNotified = (boolean) journeyStats.get("statusNotified");
                boolean journeyComplete = (boolean) journeyStats.get("journeyComplete");
                boolean statusConverted = (boolean) journeyStats.get("statusConverted");
                Boolean statusTargetGroup = journeyStats.get("statusTargetGroup") == null ? null : (boolean) journeyStats.get("statusTargetGroup");
                Boolean statusControlGroup = journeyStats.get("statusControlGroup") == null ? null : (boolean) journeyStats.get("statusControlGroup");
                Boolean statusUniversalControlGroup = journeyStats.get("statusUniversalControlGroup") == null ? null : (boolean) journeyStats.get("statusUniversalControlGroup");

                if (journeyStats.get("sample") != null)
                  {
                    journeyInfo.put("sample", journeyStats.get("sample"));
                  }

                journeyInfo.put("customerStatus", getSubscriberJourneyStatus(journeyComplete, statusConverted, statusNotified, statusTargetGroup, statusControlGroup, statusUniversalControlGroup).toString());
                journeyInfo.put("dateTime", SystemTime.getCurrentTime());
                journeyInfo.put("startDate", ReportsCommonCode.getDateString(journey.getEffectiveStartDate()));
                journeyInfo.put("endDate", ReportsCommonCode.getDateString(journey.getEffectiveEndDate()));

                for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values())
                  {
                    journeyInfo.put(journeyMetricDeclaration.getESFieldPrior(), journeyMetric.get(journeyMetricDeclaration.getESFieldPrior()));
                    journeyInfo.put(journeyMetricDeclaration.getESFieldDuring(), journeyMetric.get(journeyMetricDeclaration.getESFieldDuring()));
                    journeyInfo.put(journeyMetricDeclaration.getESFieldPost(), journeyMetric.get(journeyMetricDeclaration.getESFieldPost()));
                  }

                if (journeyStats.get(subscriberID) != null)
                  {
                    Object subscriberIDField = journeyStats.get(subscriberID);
                    subscriberFields.put(customerID, subscriberIDField);
                  }
                for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                  {
                    if (journeyStats.get(alternateID.getESField()) != null)
                      {
                        Object alternateId = journeyStats.get(alternateID.getESField());
                        subscriberFields.put(alternateID.getName(), alternateId);
                      }
                  }

                if (addHeaders)
                  {
                    headerFieldsOrder.clear();
                    addHeaders(writer, subscriberFields, 0);
                    addHeaders(writer, journeyInfo, 1);
                  }
                String line = ReportUtils.formatResult(headerFieldsOrder, journeyInfo, subscriberFields);
                log.trace("Writing to csv file : " + line);
                writer.write(line.getBytes());
                writer.write("\n".getBytes());
              }
          }
      }
  }

  public static void main(String[] args)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("JourneyCustomerStatisticsReportCsvWriter: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : JourneyCustomerStatisticsReportCsvWriter <KafkaNode> <topic in> <csvfile>");
        return;
      }
    String kafkaNode = args[0];
    String topic = args[1];
    String csvfile = args[2];
    log.info("Reading data from " + topic + " topic on broker " + kafkaNode + " producing " + csvfile + " with '" + CSV_SEPARATOR + "' separator");
    ReportCsvFactory reportFactory = new JourneyCustomerStatisticsReportCsvWriter();
    ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);

    String journeyTopic = Deployment.getJourneyTopic();

    journeyService = new JourneyService(kafkaNode, "customerstatsreportcsvwriter-journeyservice-" + topic, journeyTopic, false);
    journeyService.start();

    if (!reportWriter.produceReport(csvfile))
      {
        log.warn("An error occured, the report might be corrupted");
        return;
      }
  }

  private void addHeaders(ZipOutputStream writer, Map<String, Object> values, int offset) throws IOException
  {
    if (values != null && !values.isEmpty())
      {
        String[] allFields = values.keySet().toArray(new String[0]);
        // Arrays.sort(allFields);
        String headers = "";
        for (String fields : allFields)
          {
            headerFieldsOrder.add(fields);
            headers += fields + CSV_SEPARATOR;
          }
        headers = headers.substring(0, headers.length() - offset);
        writer.write(headers.getBytes());
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
