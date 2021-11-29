/****************************************************************************
*
*  JourneyStatisticESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.SimpleESSinkTask;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.JourneyHistory.NodeHistory;
import com.evolving.nglm.evolution.JourneyHistory.RewardHistory;
import com.evolving.nglm.evolution.JourneyHistory.StatusHistory;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.Script;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JourneyStatisticESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  static
  *
  ****************************************/
  public static String WORKFLOW_ARCHIVE_INDEX = "workflowarchive";
  
  /**
   * Format journeyID to use it in an ES index name.
   */
  public static String journeyIDFormatterForESIndex(String journeyID) {
    return journeyID.toLowerCase();
  }
  
  public static String getJourneyStatisticIndex(String journeyID, String defaultIndexName) {
    String suffix = JourneyStatisticESSinkConnector.journeyIDFormatterForESIndex(journeyID);
    
    if (suffix.matches("[a-z0-9_-]*")) {
      return defaultIndexName + "-" + suffix; 
    }
    else {
      log.error("Unable to insert document in elasticsearch index: " + defaultIndexName + "-" + suffix + ". This is not a valid index name.");
      return defaultIndexName + "_unclassified"; 
    }
  }
  
  public static String getJourneyStatisticID(String subscriberID, String journeyID, String journeyInstanceID) {
    StringBuilder sb = new StringBuilder();
    sb.append(subscriberID).append("_").append(journeyID).append("_").append(journeyInstanceID);
    return sb.toString();
  }
  
  public static String getWorkflowArchiveID(Map<String, Object> archiveDoc) {
    StringBuilder sb = new StringBuilder();
    sb.append(archiveDoc.get("journeyID")).append("_");
    sb.append(archiveDoc.get("nodeID")).append("_");
    sb.append(archiveDoc.get("status"));
    return sb.toString();
  }
  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return JourneyStatisticESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  /**
   * ChangeLogESSinkTask but adapted for JourneyStatisticESSinkTask
   * We either 
   * 1. push in journeystatistic 
   * or
   * 2. delete from journeystatistic and push in workflowarchive
   *   - for workflow only
   *   - when subscriber left workflow (metrics are disabled)
   */
  public static class JourneyStatisticESSinkTask extends SimpleESSinkTask
  {
    private final Logger log = LoggerFactory.getLogger(JourneyStatisticESSinkConnector.class);
    
    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> taskConfig)
    {
      super.start(taskConfig);
    }
    
    /*****************************************
    *
    *  stop
    *
    *****************************************/

    @Override public void stop()
    {
      
      //
      //  super
      //

      super.stop();
    }

    /*****************************************
    *
    *  getRequests
    *
    *****************************************/

    @Override public List<DocWriteRequest> getRequests(SinkRecord sinkRecord)
    {
      if (sinkRecord.value() != null) {
        JourneyStatistic journeystatistic = unpackRecord(sinkRecord);
        if (journeystatistic != null) {
          // Normal case
          if(! journeystatistic.isArchive()) {
            UpdateRequest request = new UpdateRequest(
                getJourneyStatisticIndex(journeystatistic.getJourneyID(), this.getDefaultIndexName()), 
                getJourneyStatisticID(journeystatistic.getSubscriberID(), journeystatistic.getJourneyID(), journeystatistic.getJourneyInstanceID())
            );
            request.doc(getDocumentMap(journeystatistic));
            request.docAsUpsert(true);
            request.retryOnConflict(4);
            return Collections.<DocWriteRequest>singletonList(request);
          }
          // Archive case. 
          // EVPRO-1318. It is REALLY important that, once the journeystatistic is tagged as archive, it is never push again
          // Otherwise we would increment the count several time.
          // That's why 'archive' means that the journeystatistic will never change again.
          // And also: JourneyMetric is disabled here.
          else {
            List<DocWriteRequest> result = new ArrayList();
            // 1. Remove from journeystatistic index
            DeleteRequest delete = new DeleteRequest(
                getJourneyStatisticIndex(journeystatistic.getJourneyID(), this.getDefaultIndexName()), 
                getJourneyStatisticID(journeystatistic.getSubscriberID(), journeystatistic.getJourneyID(), journeystatistic.getJourneyInstanceID())
            );
            result.add(delete);
            
            // 2. Increment count in workflowarchive
            Map<String, Object> archiveDoc = getArchiveDocumentMap(journeystatistic);
            UpdateRequest update = new UpdateRequest(
                WORKFLOW_ARCHIVE_INDEX, 
                getWorkflowArchiveID(archiveDoc)
            );
            update.script(new Script(ScriptType.INLINE, "painless", "ctx._source.count += 1;", Collections.emptyMap())); // See upsert documentation
            update.upsert(archiveDoc);
            update.retryOnConflict(4);
            result.add(update);
            
            return result;
          }
        }
      }
      return Collections.<DocWriteRequest>emptyList();
    }
    
    public JourneyStatistic unpackRecord(SinkRecord sinkRecord) 
    {
      Object journeyStatisticValue = sinkRecord.value();
      Schema journeyStatisticValueSchema = sinkRecord.valueSchema();
      return JourneyStatistic.unpack(new SchemaAndValue(journeyStatisticValueSchema, journeyStatisticValue));
    }

    /*****************************************
    *
    *  journeystatistic
    *
    *****************************************/
    
    public Map<String,Object> getDocumentMap(JourneyStatistic journeyStatistic)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();

      //
      //  flat fields
      //
      
      Map<String, Integer> rewards = new HashMap<String, Integer>();
      List<String> journeyReward = new ArrayList<String>();
      for(RewardHistory history : journeyStatistic.getJourneyRewardHistory()) {
        journeyReward.add(history.toString());
        Integer previousValue = rewards.get(history.getRewardName());
        Integer sum = ((previousValue != null )? previousValue : 0) + history.getAmount(); 
        rewards.put(history.getRewardName(), sum);
      }
      
      List<String> journeyNode = new ArrayList<String>();
      for(NodeHistory node : journeyStatistic.getJourneyNodeHistory()) {
        journeyNode.add(node.toString());
      }

      List<String> journeyStatus = new ArrayList<String>();
      for(StatusHistory status : journeyStatistic.getJourneyStatusHistory()) {
        journeyStatus.add(status.toString());
      }
      
      documentMap.put("journeyInstanceID", journeyStatistic.getJourneyInstanceID());
      documentMap.put("journeyID", journeyStatistic.getJourneyID());
      documentMap.put("subscriberID", journeyStatistic.getSubscriberID());
      SinkConnectorUtils.putAlternateIDs(journeyStatistic.getAlternateIDs(), documentMap);
      documentMap.put("tenantID", journeyStatistic.getTenantID());
      documentMap.put("transitionDate", RLMDateUtils.formatDateForElasticsearchDefault(journeyStatistic.getTransitionDate()));
      documentMap.put("nodeHistory", journeyNode);
      documentMap.put("statusHistory", journeyStatus);
      documentMap.put("rewardHistory", journeyReward);
      // @rl THIS SHOULD BE REMOVED FROM ES template settings later
      // documentMap.put("deliveryRequestID", journeyStatistic.getDeliveryRequestID());
      documentMap.put("sample", journeyStatistic.getSample());
      documentMap.put("statusNotified", journeyStatistic.getStatusNotified());
      documentMap.put("statusConverted", journeyStatistic.getStatusConverted());
      documentMap.put("statusTargetGroup", journeyStatistic.getStatusTargetGroup());
      documentMap.put("statusControlGroup", journeyStatistic.getStatusControlGroup());
      documentMap.put("statusUniversalControlGroup", journeyStatistic.getStatusUniversalControlGroup());
      documentMap.put("conversionCount", journeyStatistic.getConversionCount());
      documentMap.put("lastConversionDate", journeyStatistic.getLastConversionDate() != null ? RLMDateUtils.formatDateForElasticsearchDefault(journeyStatistic.getLastConversionDate()) : null);
      documentMap.put("journeyComplete", journeyStatistic.getJourneyComplete());
      documentMap.put("journeyExitDate", (journeyStatistic.getJourneyExitDate() != null)? RLMDateUtils.formatDateForElasticsearchDefault(journeyStatistic.getJourneyExitDate()) : null);
      
      documentMap.put("nodeID", journeyStatistic.getCurrentNodeID());
      if(journeyStatistic.getSpecialExitStatus() != null && !journeyStatistic.getSpecialExitStatus().equalsIgnoreCase("null") && !journeyStatistic.getSpecialExitStatus().isEmpty()) {
        documentMap.put("status", SubscriberJourneyStatus.fromExternalRepresentation(journeyStatistic.getSpecialExitStatus()).getDisplay());
      } else {
        documentMap.put("status", journeyStatistic.getSubscriberJourneyStatus().getDisplay());
      }
      
      documentMap.put("subscriberStratum", journeyStatistic.getSubscriberStratum());
      documentMap.put("rewards", rewards);

      // 
      //  return
      //
      
      return documentMap;
    }

    /*****************************************
    *
    *  workflowarchive
    *
    *****************************************/
    
    /**
     * Get the default document for workflowarchive. This will be used only when it does not already exist (upsert)
     */
    public Map<String,Object> getArchiveDocumentMap(JourneyStatistic journeyStatistic)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();
     
      documentMap.put("journeyID", journeyStatistic.getJourneyID());
      documentMap.put("tenantID", journeyStatistic.getTenantID());
      documentMap.put("nodeID", journeyStatistic.getCurrentNodeID());
      if(journeyStatistic.getSpecialExitStatus() != null && !journeyStatistic.getSpecialExitStatus().equalsIgnoreCase("null") && !journeyStatistic.getSpecialExitStatus().isEmpty()) {
        documentMap.put("status", SubscriberJourneyStatus.fromExternalRepresentation(journeyStatistic.getSpecialExitStatus()).getDisplay());
      } else {
        documentMap.put("status", journeyStatistic.getSubscriberJourneyStatus().getDisplay());
      }
      documentMap.put("count", new Long(1));
      return documentMap;
    }
  }
}
