package com.evolving.nglm.evolution.reports.journeyimpact;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.JourneyMetricDeclaration;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.JourneyTrafficHistory;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.evolution.reports.journeycustomerstatistics.JourneyCustomerStatisticsReportMonoPhase;

/**
 * This implements the phase 3 for the Journey report.
 *
 */
public class JourneyImpactReportMonoPhase implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(JourneyImpactReportMonoPhase.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  private JourneyService journeyService;
  List<String> headerFieldsOrder = new ArrayList<String>();
  private ReferenceDataReader<String, JourneyTrafficHistory> journeyTrafficReader;
  
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
  
  public Map<String, List<Map<String, Object>>> getSplittedReportElementsForFileMono(Map<String,Object> map)
  {
    Map<String, List<Map<String, Object>>> result = new LinkedHashMap<String, List<Map<String, Object>>>();
    Map<String, Object> journeyStats = map;
    Map<String, Object> journeyMetric = map;
    if (journeyStats != null && !journeyStats.isEmpty() && journeyMetric != null && !journeyMetric.isEmpty())
      {
        Journey journey = journeyService.getActiveJourney(journeyStats.get("journeyID").toString(), SystemTime.getCurrentTime());
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

  public static void main(String[] args, final Date reportGenerationDate)
  {
    JourneyImpactReportMonoPhase journeyImpactReportMonoPhase = new JourneyImpactReportMonoPhase();
    journeyImpactReportMonoPhase.start(args, reportGenerationDate);
  }
  
  private void start(String[] args, final Date reportGenerationDate)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("JourneyImpactReportMonoPhase: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : JourneyImpactReportMonoPhase <ESNode> <ES journey index> <csvfile>");
        return;
      }
    String esNode = args[0];
    String esIndexJourney = args[1];
    String csvfile = args[2];
    
    journeyService = new JourneyService(Deployment.getBrokerServers(), "JourneyImpactReport-journeyservice-JourneyImpactReportMonoDriver", Deployment.getJourneyTopic(), false);
    journeyService.start();
    
    Collection<Journey> activeJourneys = journeyService.getActiveJourneys(reportGenerationDate);
    StringBuilder activeJourneyEsIndex = new StringBuilder();
    boolean firstEntry = true;
    for (Journey journey : activeJourneys)
      {
        if (!firstEntry) activeJourneyEsIndex.append(",");
        String indexName = esIndexJourney + journey.getJourneyID();
        activeJourneyEsIndex.append(indexName);
        firstEntry = false;
      }

    log.info("Reading data from ES in (" + activeJourneyEsIndex.toString() + ") and " + esIndexJourney + " index on " + esNode + " producing " + csvfile + " with '" + CSV_SEPARATOR + "' separator");
    
    journeyTrafficReader = ReferenceDataReader.<String, JourneyTrafficHistory>startReader("guimanager-journeytrafficservice", Deployment.getBrokerServers(), Deployment.getJourneyTrafficChangeLogTopic(), JourneyTrafficHistory::unpack);

    LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
    esIndexWithQuery.put(activeJourneyEsIndex.toString(), QueryBuilders.matchAllQuery());
    
    ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
        esNode,
        esIndexWithQuery,
        this,
        csvfile
    );

    if (!reportMonoPhase.startOneToOne(true))
      {
        log.warn("An error occured, the report might be corrupted");
        return;
      }
    journeyService.stop();
    log.info("Finished JourneyImpactReport");
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
