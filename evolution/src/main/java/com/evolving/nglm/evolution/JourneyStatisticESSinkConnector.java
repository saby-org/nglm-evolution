/****************************************************************************
*
*  JourneyStatisticESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.evolution.JourneyHistory.NodeHistory;
import com.evolving.nglm.evolution.JourneyHistory.RewardHistory;
import com.evolving.nglm.evolution.JourneyHistory.StatusHistory;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
  
  public static String getJourneyStatisticIndex(String journeyID, String defaultIndexName) {
    String suffix = journeyID.toLowerCase(); /////////////////////////////////////////
    
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
  
  public static class JourneyStatisticESSinkTask extends ChangeLogESSinkTask<JourneyStatistic>
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

    @Override public JourneyStatistic unpackRecord(SinkRecord sinkRecord) 
    {
      Object journeyStatisticValue = sinkRecord.value();
      Schema journeyStatisticValueSchema = sinkRecord.valueSchema();
      return JourneyStatistic.unpack(new SchemaAndValue(journeyStatisticValueSchema, journeyStatisticValue));
    }
    
    @Override
    protected String getDocumentIndexName(JourneyStatistic journeyStatistic)
    {
      return JourneyStatisticESSinkConnector.getJourneyStatisticIndex(journeyStatistic.getJourneyID(), this.getDefaultIndexName());
    }

    @Override
    public String getDocumentID(JourneyStatistic journeyStatistic)
    {
      return JourneyStatisticESSinkConnector.getJourneyStatisticID(journeyStatistic.getSubscriberID(), journeyStatistic.getJourneyID(), journeyStatistic.getJourneyInstanceID());
    }
    
    @Override public Map<String,Object> getDocumentMap(JourneyStatistic journeyStatistic)
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
      documentMap.put("transitionDate", journeyStatistic.getTransitionDate());
      documentMap.put("nodeHistory", journeyNode);
      documentMap.put("statusHistory", journeyStatus);
      documentMap.put("rewardHistory", journeyReward);
      
      documentMap.put("fromNodeID", journeyStatistic.getFromNodeID()); // TODO delete ?
      documentMap.put("toNodeID", journeyStatistic.getToNodeID()); // TODO delete ?
      documentMap.put("deliveryRequestID", journeyStatistic.getDeliveryRequestID());
      documentMap.put("sample", journeyStatistic.getSample());
      documentMap.put("markNotified", journeyStatistic.getMarkNotified());
      documentMap.put("markConverted", journeyStatistic.getMarkConverted());
      documentMap.put("statusNotified", journeyStatistic.getStatusNotified());
      documentMap.put("statusConverted", journeyStatistic.getStatusConverted());
      documentMap.put("statusTargetGroup", journeyStatistic.getStatusTargetGroup());
      documentMap.put("statusControlGroup", journeyStatistic.getStatusControlGroup());
      documentMap.put("statusUniversalControlGroup", journeyStatistic.getStatusUniversalControlGroup());
      documentMap.put("journeyComplete", journeyStatistic.getJourneyComplete());
      
      documentMap.put("nodeID", journeyStatistic.getToNodeID());
      documentMap.put("status", journeyStatistic.getSubscriberJourneyStatus().getDisplay());
      documentMap.put("subscriberStratum", journeyStatistic.getSubscriberStratum());
      documentMap.put("rewards", rewards);

      // 
      //  return
      //
      
      return documentMap;
    }
  }
}
