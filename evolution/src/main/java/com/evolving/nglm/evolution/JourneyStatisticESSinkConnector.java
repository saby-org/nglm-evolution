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
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
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
import java.util.Map.Entry;
import java.util.Random;

public class JourneyStatisticESSinkConnector extends SimpleESSinkConnector
{
  
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
  
  public static class JourneyStatisticESSinkTask extends ChangeLogESSinkTask
  {
    private SubscriberProfileService subscriberProfileService = null;
    private JourneyService journeyService = null;
    private final Logger log = LoggerFactory.getLogger(JourneyStatisticESSinkConnector.class);
    
    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> taskConfig)
    {
      super.start(taskConfig);
      subscriberProfileService = new EngineSubscriberProfileService(Deployment.getSubscriberProfileEndpoints());
      subscriberProfileService.start();
      
      journeyService = new JourneyService(Deployment.getBrokerServers(), "sinkconnector-journeyservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getJourneyTopic(), false);
      journeyService.start();
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
    *  getDocumentIndexName
    *
    *****************************************/
    
    @Override
    protected String getDocumentIndexName(SinkRecord sinkRecord)
    { 
      /****************************************
      *
      *  extract JourneyStatistic
      *
      ****************************************/

      Object journeyStatisticValue = sinkRecord.value();
      Schema journeyStatisticValueSchema = sinkRecord.valueSchema();
      JourneyStatistic journeyStatistic = JourneyStatistic.unpack(new SchemaAndValue(journeyStatisticValueSchema, journeyStatisticValue));
      
      String suffix = journeyStatistic.getJourneyID().toLowerCase();
      
      if (suffix.matches("[a-z0-9_-]*")) 
        {
          return this.getIndexName() + "-" + suffix; 
        }
      else
        {
          log.error("Unable to insert document in elasticsearch index: " + this.getIndexName() + "-" + suffix + ". This is not a valid index name.");
          return this.getIndexName() + "_unclassified"; 
        }
    }
    
    @Override public Map<String,Object> getDocumentMap(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract JourneyStatistic
      *
      ****************************************/

      Object journeyStatisticValue = sinkRecord.value();
      Schema journeyStatisticValueSchema = sinkRecord.valueSchema();
      JourneyStatistic journeyStatistic = JourneyStatistic.unpack(new SchemaAndValue(journeyStatisticValueSchema, journeyStatisticValue));

      /****************************************
      *
      *  documentMap
      *
      ****************************************/

      Map<String,Object> documentMap = new HashMap<String,Object>();

      //
      //  flat fields
      //
      
      Map<String, Integer> rewards = new HashMap<String, Integer>();
      List<String> journeyReward = new ArrayList<String>();
      for(RewardHistory history : journeyStatistic.getJourneyRewardHistory()) {
        journeyReward.add(history.toString());
        Integer previousValue = rewards.get(history.getRewardID());
        Integer sum = ((previousValue != null )? previousValue : 0) + history.getAmount(); 
        rewards.put(history.getRewardID(), sum);
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
      documentMap.put("statusControlGroup", journeyStatistic.getStatusControlGroup());
      documentMap.put("statusUniversalControlGroup", journeyStatistic.getStatusUniversalControlGroup());
      documentMap.put("journeyComplete", journeyStatistic.getJourneyComplete());
      
      documentMap.put("nodeID", journeyStatistic.getToNodeID());
      documentMap.put("status", journeyStatistic.getSubscriberJourneyStatus());
      documentMap.put("subscriberStratum", journeyStatistic.getSubscriberStratum());
      documentMap.put("rewards", rewards);

      // 
      //  return
      //
      
      return documentMap;
    }

    @Override
    public String getDocumentID(SinkRecord sinkRecord)
    {
      Object journeyStatisticValue = sinkRecord.value();
      Schema journeyStatisticValueSchema = sinkRecord.valueSchema();
      JourneyStatistic journeyStatistic = JourneyStatistic.unpack(new SchemaAndValue(journeyStatisticValueSchema, journeyStatisticValue));
      StringBuilder sb = new StringBuilder();
      sb.append(journeyStatistic.getSubscriberID()).append("_").append(journeyStatistic.getJourneyID()).append("_").append(journeyStatistic.getJourneyInstanceID());
      return sb.toString();
    }
   
  }
}
