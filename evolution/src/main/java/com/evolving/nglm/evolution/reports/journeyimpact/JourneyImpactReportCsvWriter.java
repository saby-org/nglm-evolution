package com.evolving.nglm.evolution.reports.journeyimpact;

import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.evolution.reports.journeycustomerstatistics.JourneyCustomerStatisticsReportCsvWriter;
import com.evolving.nglm.core.ReferenceDataReader;
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
import java.util.Map.Entry;
import java.util.zip.ZipOutputStream;

/**
 * This implements the phase 3 for the Journey report.
 *
 */
public class JourneyImpactReportCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatisticsReportCsvWriter.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  private static JourneyService journeyServiceStatic;
  List<String> headerFieldsOrder = new ArrayList<String>();
  private static ReferenceDataReader<String, JourneyTrafficHistory> journeyTrafficReader;
  
  public void dumpLineToCsv(Map<String, Object> lineMap, ZipOutputStream writer, boolean addHeaders)
  {
    try
    {
      if (addHeaders)
        {
          addHeaders(writer, lineMap, 1);
        }
      String line = ReportUtils.formatResult(lineMap);
      if (log.isTraceEnabled()) log.trace("Writing to csv file : " + line);
      writer.write(line.getBytes());
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
        JourneyTrafficHistory journeyTrafficHistory = null;
        Map<String, Object> journeyInfo = new LinkedHashMap<String, Object>();
        if (journey != null)
          {
            journeyTrafficHistory = journeyTrafficReader.get(journey.getJourneyID());
            journeyInfo.put("journeyID", journey.getJourneyID());
            journeyInfo.put("journeyName", journey.getGUIManagedObjectDisplay());
            journeyInfo.put("journeyType", journey.getTargetingType());
            
            StringBuilder sbStatus = new StringBuilder();
            StringBuilder sbRewards = new StringBuilder();
            String journeyStatus = null;
            String journeyRewards = "";
            if (journeyTrafficHistory != null)
              {
                for (SubscriberJourneyStatus states : SubscriberJourneyStatus.values())
                  {
                    sbStatus.append(states.getDisplay()).append("=").append(journeyTrafficHistory.getCurrentData().getStatusSubscribersCount(states)).append(",");
                  }

                if (sbStatus.length() > 0)
                  {
                    journeyStatus = sbStatus.toString().substring(0, sbStatus.toString().length() - 1);
                  }

                if (journeyTrafficHistory.getCurrentData().getGlobal().getDistributedRewards() != null)
                  {
                    for (Entry<String, Integer> rewards : journeyTrafficHistory.getCurrentData().getGlobal().getDistributedRewards().entrySet())
                      {
                        sbRewards.append(rewards.getKey()).append("=").append(rewards.getValue()).append(",");
                      }
                    if (sbRewards.toString().length() > 0)
                      {
                        journeyRewards = sbRewards.toString().substring(0, sbRewards.toString().length() - 1);
                      }
                  }
              }
            
            StringBuilder abTesting = new StringBuilder();
            String journeyAbTesting = null;
            if (journeyTrafficHistory.getCurrentData().getByAbTesting() != null && !journeyTrafficHistory.getCurrentData().getByAbTesting().isEmpty())
              {
                for (Entry<String, Integer> value : journeyTrafficHistory.getCurrentData().getByAbTesting().entrySet())
                  {
                    abTesting.append(value.getKey()).append("=").append(value.getValue()).append(",");
                  }
                journeyAbTesting = abTesting.toString().substring(0, abTesting.toString().length() - 1);
              }
            journeyInfo.put("abTesting", journeyAbTesting);
            journeyInfo.put("customerStatuses", journeyStatus);
            journeyInfo.put("rewards", journeyRewards);
            journeyInfo.put("dateTime", ReportsCommonCode.getDateString(SystemTime.getCurrentTime()));
            journeyInfo.put("startDate", ReportsCommonCode.getDateString(journey.getEffectiveStartDate()));
            journeyInfo.put("endDate", ReportsCommonCode.getDateString(journey.getEffectiveEndDate()));
            
            for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values())
              {
                journeyInfo.put(journeyMetricDeclaration.getESFieldPrior(), journeyMetric.get(journeyMetricDeclaration.getESFieldPrior()));
                journeyInfo.put(journeyMetricDeclaration.getESFieldDuring(), journeyMetric.get(journeyMetricDeclaration.getESFieldDuring()));
                journeyInfo.put(journeyMetricDeclaration.getESFieldPost(), journeyMetric.get(journeyMetricDeclaration.getESFieldPost()));
              }
            
            List<Map<String, Object>> elements = new ArrayList<Map<String, Object>>();
            elements.add(journeyInfo);
            result.put(journey.getJourneyID(), elements);
          }
      }
    return result;
  }

  public static void main(String[] args, JourneyService journeyService)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("JourneyImpactReportCsvWriter: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : JourneyImpactReportCsvWriter <KafkaNode> <topic in> <csvfile>");
        return;
      }
    String kafkaNode = args[0];
    String topic = args[1];
    String csvfile = args[2];
    log.info("Reading data from " + topic + " topic on broker " + kafkaNode + " producing " + csvfile + " with '" + CSV_SEPARATOR + "' separator");
    ReportCsvFactory reportFactory = new JourneyImpactReportCsvWriter();
    ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);

    journeyServiceStatic = journeyService;
    journeyTrafficReader = ReferenceDataReader.<String, JourneyTrafficHistory>startReader("guimanager-journeytrafficservice", "journeysreportcsvwriter-journeytrafficservice-" + topic, kafkaNode, Deployment.getJourneyTrafficChangeLogTopic(), JourneyTrafficHistory::unpack);

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

}
