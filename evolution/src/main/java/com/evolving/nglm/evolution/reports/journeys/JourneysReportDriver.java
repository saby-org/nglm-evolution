/*****************************************************************************
*
*  JourneysReportMonoDriver.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.reports.journeys;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.elasticsearch.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientException;
import com.evolving.nglm.evolution.JourneyNode;
import com.evolving.nglm.evolution.JourneyObjective;
import com.evolving.nglm.evolution.JourneyObjectiveService;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.PointService;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import java.util.Date;

public class JourneysReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(JourneysReportDriver.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  private static JourneyService journeyService;
  private static PointService pointService;
  private static JourneyObjectiveService journeyObjectiveService;
  List<String> headerFieldsOrder = new ArrayList<String>();

  /****************************************
  *
  *  produceReport
  *
  ****************************************/

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafkaNode, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {
    log.info("Entered JourneysReportDriver.produceReport");

    Random r = new Random();
    int apiProcessKey = r.nextInt(999);

    log.info("apiProcessKey" + apiProcessKey);

    String journeyTopic = Deployment.getJourneyTopic();
    String pointTopic = Deployment.getPointTopic();
    String journeyObjectiveTopic = Deployment.getJourneyObjectiveTopic();
    
    journeyService = new JourneyService(kafkaNode, "journeysreportcsvwriter-journeyservice-" + apiProcessKey, journeyTopic, false);
    journeyService.start();
    
    pointService = new PointService(kafkaNode, "journeysreportcsvwriter-pointservice-" + apiProcessKey, pointTopic, false);
    pointService.start();
    
    journeyObjectiveService = new JourneyObjectiveService(kafkaNode, "journeysreportcsvwriter-journeyObjectiveService-" + apiProcessKey, journeyObjectiveTopic, false);
    journeyObjectiveService.start();
    
    // ESROUTER can have two access points
    // need to cut the string to get at least one
    String node = null;
    int port = 0;
    int connectTimeout = Deployment.getElasticsearchConnectionSettings().get("ReportManager").getConnectTimeout();
    int queryTimeout = Deployment.getElasticsearchConnectionSettings().get("ReportManager").getQueryTimeout();
    String username = null;
    String password = null;
    
    if (elasticSearch.contains(","))
      {
        String[] split = elasticSearch.split(",");
        if (split[0] != null)
          {
            Scanner s = new Scanner(split[0]);
            s.useDelimiter(":");
            node = s.next();
            port = s.nextInt();
            username = s.next();
            password = s.next();
            s.close();
          }
      } else
        {
          Scanner s = new Scanner(elasticSearch);
          s.useDelimiter(":");
          node = s.next();
          port = s.nextInt();
          username = s.next();
          password = s.next();
          s.close();
        }
    ElasticsearchClientAPI elasticsearchReaderClient = new ElasticsearchClientAPI(node, port, connectTimeout, queryTimeout, username, password);

    ReportsCommonCode.initializeDateFormats();
    
    File file = new File(csvFilename + ".zip");
    FileOutputStream fos = null;
    ZipOutputStream writer = null;
    try
      {
        fos = new FileOutputStream(file);
        writer = new ZipOutputStream(fos);
        // do not include tree structure in zipentry, just csv filename
        ZipEntry entry = new ZipEntry(new File(csvFilename).getName());
        writer.putNextEntry(entry);
        Collection<GUIManagedObject> journeys = journeyService.getStoredJourneys(tenantID);
        int nbJourneys = journeys.size();
        log.info("journeys list size : " + nbJourneys);

        boolean addHeaders = true;
        for (GUIManagedObject guiManagedObject : journeys)
          {
            try
            {
              if(guiManagedObject instanceof Journey) {
                Journey journey = (Journey) guiManagedObject;

                Map<String, Object> journeyInfo = new LinkedHashMap<String, Object>(); // to preserve order
                if (journey != null) {
                  StringBuilder sbJourneyObjectives = new StringBuilder();

                  String journeyID = journey.getJourneyID();
                  journeyInfo.put("journeyID", journeyID);
                  journeyInfo.put("journeyName", journey.getGUIManagedObjectDisplay());
                  journeyInfo.put("journeyDescription", journey.getJSONRepresentation().get("description"));
                  journeyInfo.put("journeyType", journey.getGUIManagedObjectType().getExternalRepresentation()); 
                  Set<JourneyObjective> activejourneyObjectives = journey.getAllObjectives(journeyObjectiveService, SystemTime.getCurrentTime());                 
                  for (JourneyObjective journeyObjective : activejourneyObjectives) {
                    sbJourneyObjectives.append(journeyObjective.getGUIManagedObjectDisplay()).append(",");            
                  }   

                  journeyInfo.put("journeyObjectives", sbJourneyObjectives);
                  journeyInfo.put("startDate", ReportsCommonCode.getDateString(journey.getEffectiveStartDate()));
                  journeyInfo.put("endDate", ReportsCommonCode.getDateString(journey.getEffectiveEndDate()));                

                  StringBuilder sbStatus = new StringBuilder();
                  StringBuilder sbStates = new StringBuilder();
                  StringBuilder sbRewards = new StringBuilder();          

                  String journeyStatus = null;
                  String journeyRewards = null;
                  String journeyStates = null;

                  Map<String, Long> journeyStatusCount = elasticsearchReaderClient.getJourneyStatusCount(journeyID);
                  for (SubscriberJourneyStatus states : SubscriberJourneyStatus.values()){
                    Long statusCount = journeyStatusCount.get(states.getDisplay());
                    sbStatus.append(states.getDisplay()).append("=").append((statusCount==null) ? "0" : statusCount.toString()).append(",");
                  }
                  journeyStatus = sbStatus.toString().substring(0, sbStatus.toString().length()-1);

                  Map<String, Long> nodeSubscribersCount = elasticsearchReaderClient.getJourneyNodeCount(journeyID);
                  for (JourneyNode journeyNode : journey.getJourneyNodes().values()) {
                    sbStates.append(journeyNode.getNodeName()).append("=").append(nodeSubscribersCount.get(journeyNode.getNodeID())).append(",");
                  }
                  journeyStates = sbStates.toString().substring(0, sbStates.toString().length()-1);

                  Map<String, Long> distributedRewards = elasticsearchReaderClient.getDistributedRewards(journeyID);
                  for (String rewards : distributedRewards.keySet()) {
                    if (pointService.getStoredPoint(rewards) != null) {
                      String rewardName = pointService.getStoredPoint(rewards).getGUIManagedObjectDisplay();
                      sbRewards.append(rewardName).append(",");
                    }
                    else {
                      sbRewards.append("").append(",");
                    }
                    journeyRewards = (sbRewards.length() > 0)? sbRewards.toString().substring(0, sbRewards.toString().length()-1) : "";
                  }
                  journeyInfo.put("customerStates", journeyStates);
                  journeyInfo.put("customerStatuses", journeyStatus);         
                  journeyInfo.put("listOfCommodities", journeyRewards);          
                }

                if(addHeaders){
                  headerFieldsOrder.clear();
                  addHeaders(writer, journeyInfo, 1);
                  addHeaders = false;
                }
                String line = ReportUtils.formatResult(journeyInfo);
                if (log.isTraceEnabled()) log.trace("Writing to csv file : "+line);
                writer.write(line.getBytes());
              }
            }
            catch (IOException | ElasticsearchClientException e)
            {
              log.info("Exception processing "+guiManagedObject.getGUIManagedObjectDisplay(), e);
            }
          }
        log.info("WriteCompleted ");
        log.info("csv Writer closed");
      }
    catch (IOException e)
      {
        log.info("Exception generating "+csvFilename, e);
      }
    finally {
      if (writer != null)
        {
          try
          {
            writer.flush();
            writer.closeEntry();
            writer.close();
          }
          catch (IOException e)
          {
            log.info("Exception generating "+csvFilename, e);
          }
        }
      if (fos != null)
        {
          try
          {
            fos.close();
          }
          catch (IOException e)
          {
            log.info("Exception generating "+csvFilename, e);
          }
        }
      if (elasticsearchReaderClient != null)
        {
          try
          {
            elasticsearchReaderClient.close();
          }
          catch (IOException e)
          {
            log.info("Exception generating "+csvFilename, e);
          }
        }
      journeyService.stop();
      pointService.stop();
      journeyObjectiveService.stop();
    }
  }
   
  private void addHeaders(ZipOutputStream writer, Map<String,Object> values, int offset) throws IOException {
    if(values != null && !values.isEmpty()) {
      String[] allFields = values.keySet().toArray(new String[0]);     
      String headers="";
      for(String fields : allFields){
        headerFieldsOrder.add(fields);
        headers += fields + CSV_SEPARATOR;
      }
      headers = headers.substring(0, headers.length() - offset);
      writer.write(headers.getBytes());
      if(offset == 1) {
        writer.write("\n".getBytes());
      }
    }
  }
  
}
