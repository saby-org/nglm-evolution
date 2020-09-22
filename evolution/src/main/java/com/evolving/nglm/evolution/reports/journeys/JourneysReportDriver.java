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
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.JourneyNode;
import com.evolving.nglm.evolution.JourneyObjective;
import com.evolving.nglm.evolution.JourneyObjectiveService;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.JourneyTrafficHistory;
import com.evolving.nglm.evolution.PointService;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;

public class JourneysReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(JourneysReportDriver.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  private static JourneyService journeyService;
  private static PointService pointService;
  private static JourneyObjectiveService journeyObjectiveService;
  List<String> headerFieldsOrder = new ArrayList<String>();
  private static ReferenceDataReader<String,JourneyTrafficHistory> journeyTrafficReader;

  /****************************************
  *
  *  produceReport
  *
  ****************************************/

  @Override
  public void produceReport(Report report, String zookeeper, String kafkaNode, String elasticSearch, String csvFilename,
      String[] params)
  {
    log.info("Entered OfferReportDriver.produceReport");

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
    
    journeyTrafficReader = ReferenceDataReader.<String,JourneyTrafficHistory>startReader("guimanager-journeytrafficservice", "journeysreportcsvwriter-journeytrafficservice-" + apiProcessKey, kafkaNode, Deployment.getJourneyTrafficChangeLogTopic(), JourneyTrafficHistory::unpack);
    
    ReportsCommonCode.initializeDateFormats();
    
    File file = new File(csvFilename + ".zip");
    FileOutputStream fos;
    try
      {
        fos = new FileOutputStream(file);
        ZipOutputStream writer = new ZipOutputStream(fos);
        // do not include tree structure in zipentry, just csv filename
        ZipEntry entry = new ZipEntry(new File(csvFilename).getName());
        writer.putNextEntry(entry);
        Collection<GUIManagedObject> journeys = journeyService.getStoredJourneys();
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
                  JourneyTrafficHistory journeyTrafficHistory = journeyTrafficReader.get(journey.getJourneyID());
                  StringBuilder sbJourneyObjectives = new StringBuilder();

                  journeyInfo.put("journeyID", journey.getJourneyID());
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

                  if (journeyTrafficHistory != null) {
                    for (SubscriberJourneyStatus states : SubscriberJourneyStatus.values()){
                      sbStatus.append(states.getDisplay()).append("=").append(journeyTrafficHistory.getCurrentData().getStatusSubscribersCount(states)).append(",");
                    }
                    journeyStatus = sbStatus.toString().substring(0, sbStatus.toString().length()-1);

                    for (JourneyNode node : journey.getJourneyNodes().values()) {
                      sbStates.append(node.getNodeName()).append("=").append(journeyTrafficHistory.getCurrentData().getNodeSubscribersCount(node.getNodeID())).append(",");
                    }
                    journeyStates = sbStates.toString().substring(0, sbStates.toString().length()-1);

                    if (journeyTrafficHistory.getCurrentData().getGlobal().getDistributedRewards() != null) {
                      for (String rewards : journeyTrafficHistory.getCurrentData().getGlobal().getDistributedRewards().keySet()) {
                        if (pointService.getStoredPoint(rewards) != null) {
                          String rewardName = pointService.getStoredPoint(rewards).getGUIManagedObjectDisplay();
                          sbRewards.append(rewardName).append(",");
                        }
                        else {
                          sbRewards.append("").append(",");
                        }
                      }
                      journeyRewards = (sbRewards.length() > 0)? sbRewards.toString().substring(0, sbRewards.toString().length()-1) : "";
                    }
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
            catch (IOException e)
            {
              log.info("Exception processing "+guiManagedObject.getGUIManagedObjectDisplay(), e);
            }
          }
        log.info(" writeCompleted ");
        log.info("journeyService {}", journeyService.toString());

        writer.flush();
        writer.closeEntry();
        writer.close();
        log.info("csv Writer closed");
        NGLMRuntime.addShutdownHook(
            new ShutdownHook(journeyService, pointService, journeyObjectiveService));
      }
    catch (IOException e)
      {
        log.info("Exception generating "+csvFilename, e);
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
  
  /****************************************
  *
  *  ShutdownHook
  *
  ****************************************/

  private static class ShutdownHook implements NGLMRuntime.NGLMShutdownHook
  {

    private JourneyService journeyService;
    private PointService pointService;
    private JourneyObjectiveService journeyObjectiveService;

    public ShutdownHook(JourneyService journeyService, PointService pointService, JourneyObjectiveService journeyObjectiveService)
    {

      this.journeyService = journeyService;
      this.pointService = pointService;
      this.journeyObjectiveService = journeyObjectiveService;
    }

    @Override
    public void shutdown(boolean normalShutdown)
    {

      if (journeyService != null)
        {
          journeyService.stop();
          log.trace("journeyService stopped..");
        }
      if (pointService != null)
        {
          pointService.stop();
          log.trace("pointService stopped..");
        }
      if (journeyObjectiveService != null)
        {
          journeyObjectiveService.stop();
          log.trace("journeyObjectiveService stopped..");
        }
    }
  }

  @Override
  public List<FilterObject> reportFilters() {
	  return null;
  }

  @Override
  public List<String> reportHeader() {
	  // TODO Auto-generated method stub
	  return null;
  }
}
