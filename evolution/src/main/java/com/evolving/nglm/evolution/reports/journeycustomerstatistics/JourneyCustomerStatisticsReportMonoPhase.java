package com.evolving.nglm.evolution.reports.journeycustomerstatistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.JourneyMetricDeclaration;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.evolution.reports.bdr.BDRReportMonoPhase;

public class JourneyCustomerStatisticsReportMonoPhase implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatisticsReportMonoPhase.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  private JourneyService journeyService;
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
                    if (journeyStats.get(alternateID.getID()) != null)
                      {
                        Object alternateId = journeyStats.get(alternateID.getID());
                        journeyInfo.put(alternateID.getID(), alternateId);
                      }
                  }
                journeyInfo.put("journeyID", journey.getJourneyID());
                journeyInfo.put("journeyName", journey.getGUIManagedObjectDisplay());
                journeyInfo.put("journeyType", journey.getTargetingType());
                journeyInfo.put("customerState", journey.getJourneyNodes().get(journeyStats.get("nodeID")).getNodeName());
                
                // No need to do all that anymore, "status" is already correct in ES
                /*
                boolean statusNotified = (boolean) journeyStats.get("statusNotified");
                boolean journeyComplete = (boolean) journeyStats.get("journeyComplete");
                boolean statusConverted = (boolean) journeyStats.get("statusConverted");
                Boolean statusTargetGroup = journeyStats.get("statusTargetGroup") == null ? null : (boolean) journeyStats.get("statusTargetGroup");
                Boolean statusControlGroup = journeyStats.get("statusControlGroup") == null ? null : (boolean) journeyStats.get("statusControlGroup");
                Boolean statusUniversalControlGroup = journeyStats.get("statusUniversalControlGroup") == null ? null : (boolean) journeyStats.get("statusUniversalControlGroup");
                journeyInfo.put("customerStatus", getSubscriberJourneyStatus(journeyComplete, statusConverted, statusNotified, statusTargetGroup, statusControlGroup, statusUniversalControlGroup).getDisplay());
                 */
                journeyInfo.put("customerStatus", journeyStats.get("status"));
                
                if (journeyStats.get("sample") != null)
                  {
                    journeyInfo.put("sample", journeyStats.get("sample"));
                  }
                else
                  {
                    journeyInfo.put("sample", "");
                  }
 //Required Changes in accordance to EVPRO-530          
//                String specialExit=journeyStats.get("status") == null ? null : (String) journeyStats.get("status");
//                if(specialExit!=null && !specialExit.equalsIgnoreCase("null") && !specialExit.isEmpty() &&  (SubscriberJourneyStatus.fromExternalRepresentation(specialExit).in(SubscriberJourneyStatus.NotEligible,SubscriberJourneyStatus.UniversalControlGroup,SubscriberJourneyStatus.Excluded,SubscriberJourneyStatus.ObjectiveLimitReached))
//                     )
//                journeyInfo.put("customerStatus", SubscriberJourneyStatus.fromExternalRepresentation(specialExit).getDisplay());
//                else 
                Date currentDate = SystemTime.getCurrentTime();
                journeyInfo.put("dateTime", ReportsCommonCode.getDateString(currentDate));
                journeyInfo.put("startDate", ReportsCommonCode.getDateString(journey.getEffectiveStartDate()));
                journeyInfo.put("endDate", ReportsCommonCode.getDateString(journey.getEffectiveEndDate()));

                for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricConfiguration().getMetrics().values())
                  {
                    journeyInfo.put(journeyMetricDeclaration.getESFieldPrior(), journeyMetric.get(journeyMetricDeclaration.getESFieldPrior()));
                    journeyInfo.put(journeyMetricDeclaration.getESFieldDuring(), journeyMetric.get(journeyMetricDeclaration.getESFieldDuring()));
                    journeyInfo.put(journeyMetricDeclaration.getESFieldPost(), journeyMetric.get(journeyMetricDeclaration.getESFieldPost()));
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

  public static void main(String[] args, Date reportGenerationDate)
  {
    JourneyCustomerStatisticsReportMonoPhase journeyCustomerStatisticsReportMonoPhase = new JourneyCustomerStatisticsReportMonoPhase();
    journeyCustomerStatisticsReportMonoPhase.start(args, reportGenerationDate);
  }
  
  private void start(String[] args, final Date reportGenerationDate)
  {
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("JourneyCustomerStatisticsReportMonoPhase: arg " + arg);
      }

    if (args.length < 3)
      {
        log.warn("Usage : JourneyCustomerStatisticsReportMonoPhase <ESNode> <ES journey index> <csvfile>");
        return;
      }
    String esNode = args[0];
    String esIndexJourney = args[1];
    String csvfile = args[2];
    
    journeyService = new JourneyService(Deployment.getBrokerServers(), "JourneyCustomerStatisticsReport-journeyservice-JourneyCustomerStatisticsReportMonoDriver", Deployment.getJourneyTopic(), false);
    journeyService.start();

    try {
      Collection<Journey> activeJourneys = journeyService.getActiveJourneys(reportGenerationDate, 0); // TODO EVPRO-99 is all tenant (0) ok here ?
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
        }
    } finally {

      journeyService.stop();
      log.info("Finished JourneyCustomerStatisticsReport");
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
