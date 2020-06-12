package com.evolving.nglm.evolution.reports.journeycustomerstatistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.JourneyMetricDeclaration;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportCsvWriter;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;

public class JourneyCustomerStatisticsReportCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatisticsReportCsvWriter.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  private static JourneyService journeyServiceStatic;
  List<String> headerFieldsOrder = new ArrayList<String>();
  private final String subscriberID = "subscriberID";
  private final String customerID = "customerID";
  
  public void dumpLineToCsv(Map<String, Object> lineMap, ZipOutputStream writer, boolean addHeaders)
  {
    try
      {
        if (addHeaders)
          {
            addHeaders(writer, lineMap, 1);
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
    Map<String, Object> journeyStats = reportElement.fields.get(0);
    Map<String, Object> journeyMetric = reportElement.fields.get(1);
        if (journeyStats != null && !journeyStats.isEmpty() && journeyMetric != null && !journeyMetric.isEmpty())
          {
            Journey journey = journeyServiceStatic.getActiveJourney(journeyStats.get("journeyID").toString(), SystemTime.getCurrentTime());
            if (journey != null)
              {
                Map<String, Object> journeyInfo = new LinkedHashMap<String, Object>();
                journeyInfo.put("journeyID", journey.getJourneyID());
                journeyInfo.put("journeyName", journey.getGUIManagedObjectDisplay());
                journeyInfo.put("journeyType", journey.getTargetingType());
                journeyInfo.put("customerState", journey.getJourneyNodes().get(journeyStats.get("toNodeID")).getNodeName());
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

                journeyInfo.put("customerStatus", getSubscriberJourneyStatus(journeyComplete, statusConverted, statusNotified, statusTargetGroup, statusControlGroup, statusUniversalControlGroup).getDisplay());
                Date currentDate = SystemTime.getCurrentTime();
                journeyInfo.put("dateTime", ReportsCommonCode.getDateString(currentDate));
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
                    journeyInfo.put(customerID, subscriberIDField);
                  }
                for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                  {
                    if (journeyStats.get(alternateID.getID()) != null)
                      {
                        Object alternateId = journeyStats.get(alternateID.getID());
                        journeyInfo.put(alternateID.getID(), alternateId);
                      }
                  }

                /*
                 * if (addHeaders) { headerFieldsOrder.clear(); addHeaders(writer,
                 * subscriberFields, 0); addHeaders(writer, journeyInfo, 1); } String line =
                 * ReportUtils.formatResult(headerFieldsOrder, journeyInfo, subscriberFields);
                 * log.trace("Writing to csv file : " + line); writer.write(line.getBytes());
                 * writer.write("\n".getBytes());
                 */
                
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
    return result;
  }

  public static void main(String[] args, JourneyService journeyService)
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
    
    journeyServiceStatic = journeyService;
    ReportCsvFactory reportFactory = new JourneyCustomerStatisticsReportCsvWriter();
    ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);

    if (!reportWriter.produceReport(csvfile, true))
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
    return Journey.getSubscriberJourneyStatus(statusConverted, statusNotified, statusTargetGroup, statusControlGroup, statusUniversalControlGroup);
  }
}
