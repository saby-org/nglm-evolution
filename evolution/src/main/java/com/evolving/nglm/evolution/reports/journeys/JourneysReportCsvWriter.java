package com.evolving.nglm.evolution.reports.journeys;

import com.evolving.nglm.evolution.reports.ReportsCommonCode;
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
import java.util.*;
import java.util.zip.ZipOutputStream;

/**
 * This implements the phase 3 for the Journey report.
 *
 */
public class JourneysReportCsvWriter implements ReportCsvFactory
{
  private static final Logger log = LoggerFactory.getLogger(JourneysReportCsvWriter.class);
  private static final String CSV_SEPARATOR = ReportUtils.getSeparator();
  private static JourneyService journeyService;
  private static PointService pointService;
  private static JourneyObjectiveService journeyObjectiveService;
  List<String> headerFieldsOrder = new ArrayList<String>();
  private static ReferenceDataReader<String,JourneyTrafficHistory> journeyTrafficReader;
  private static Set<String> alreadyProcessedJourneysIDs = new HashSet<>();
  
  /**
   * This methods writes a single {@link ReportElement} to the report (csv file).
   * @throws IOException in case anything goes wrong while writing to the report.
   */
  public void dumpElementToCsv(String key, ReportElement re, ZipOutputStream writer, boolean addHeaders) throws IOException {
    if (re.type == ReportElement.MARKER) // We will find markers in the topic
      return;

    log.trace("We got "+key+" "+re);
    Map<String, Object> journeyStats = re.fields.get(0);
    if (journeyStats != null && !journeyStats.isEmpty()) {
      String journeyID = journeyStats.get("journeyID").toString();
      
      if (!alreadyProcessedJourneysIDs.add(journeyID))
          return;  // skip this journey as we already produced a report line for it
      
      GUIManagedObject guiOb = journeyService.getStoredJourney(journeyID);
      if(guiOb instanceof Journey) {
        Journey journey = (Journey) guiOb;

        Map<String, Object> journeyInfo = new LinkedHashMap<String, Object>(); // to preserve order
        if(journey != null) {
          JourneyTrafficHistory journeyTrafficHistory = journeyTrafficReader.get(journey.getJourneyID());
          StringBuilder sbJourneyObjectives = new StringBuilder();

          journeyInfo.put("journeyID", journey.getJourneyID());
          journeyInfo.put("journeyName", journey.getGUIManagedObjectDisplay());
          journeyInfo.put("journeyDescription", journey.getJSONRepresentation().get("description"));
          journeyInfo.put("journeyType", journey.getGUIManagedObjectType().getExternalRepresentation()); 
          Set<JourneyObjective> activejourneyObjectives = journey.getAllObjectives(journeyObjectiveService, SystemTime.getCurrentTime());                 
          for(JourneyObjective journeyObjective : activejourneyObjectives) {
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

          if(journeyTrafficHistory != null) {
            for(SubscriberJourneyStatus states : SubscriberJourneyStatus.values()){
              sbStatus.append(states).append("=").append(journeyTrafficHistory.getCurrentData().getStatusSubscribersCount(states)).append(",");
            }
            journeyStatus = sbStatus.toString().substring(0, sbStatus.toString().length()-1);

            for(JourneyNode node : journey.getJourneyNodes().values()) {
              sbStates.append(node.getNodeName()).append("=").append(journeyTrafficHistory.getCurrentData().getNodeSubscribersCount(node.getNodeID())).append(",");
            }
            journeyStates = sbStates.toString().substring(0, sbStates.toString().length()-1);


            if(journeyTrafficHistory.getCurrentData().getGlobal().getDistributedRewards() != null) {
              for(String rewards : journeyTrafficHistory.getCurrentData().getGlobal().getDistributedRewards().keySet()) {
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
        }
        String line = ReportUtils.formatResult(journeyInfo);
        log.trace("Writing to csv file : "+line);
        writer.write(line.getBytes());
        writer.write("\n".getBytes());
      }
    }
  }

  public static void main(String[] args) {
    log.info("received " + args.length + " args");
    for(String arg : args){
      log.info("JourneysReportCsvWriter: arg " + arg);
    }

    if (args.length < 3) {
      log.warn("Usage : JourneysReportCsvWriter <KafkaNode> <topic in> <csvfile>");
      return;
    }
    String kafkaNode = args[0];
    String topic     = args[1];
    String csvfile   = args[2];
    log.info("Reading data from "+topic+" topic on broker "+kafkaNode
        + " producing "+csvfile+" with '"+CSV_SEPARATOR+"' separator");
    ReportCsvFactory reportFactory = new JourneysReportCsvWriter();
    ReportCsvWriter reportWriter = new ReportCsvWriter(reportFactory, kafkaNode, topic);

    String journeyTopic = Deployment.getJourneyTopic();
    String pointTopic = Deployment.getPointTopic();
    String journeyObjectiveTopic = Deployment.getJourneyObjectiveTopic();


    journeyService = new JourneyService(kafkaNode, "journeysreportcsvwriter-journeyservice-" + topic, journeyTopic, false);
    journeyService.start();
    
    pointService = new PointService(kafkaNode, "journeysreportcsvwriter-pointservice-" + topic, pointTopic, false);
    pointService.start();
    
    journeyObjectiveService = new JourneyObjectiveService(kafkaNode, "journeysreportcsvwriter-journeyObjectiveService-" + topic, journeyObjectiveTopic, false);
    journeyObjectiveService.start();
    
    journeyTrafficReader = ReferenceDataReader.<String,JourneyTrafficHistory>startReader("guimanager-journeytrafficservice", "journeysreportcsvwriter-journeytrafficservice-" + topic, kafkaNode, Deployment.getJourneyTrafficChangeLogTopic(), JourneyTrafficHistory::unpack);
    
    if (!reportWriter.produceReport(csvfile)) {
      log.warn("An error occured, the report might be corrupted");
      return;
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

