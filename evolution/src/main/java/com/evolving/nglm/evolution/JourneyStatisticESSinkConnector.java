/****************************************************************************
*
*  JourneyStatisticESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
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
      
      journeyService = new JourneyService(Deployment.getBrokerServers(), "guimanager-journeyservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getJourneyTopic(), false);
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
      
      documentMap.put("journeyInstanceID", journeyStatistic.getJourneyInstanceID());
      documentMap.put("journeyID", journeyStatistic.getJourneyID());
      documentMap.put("subscriberID", journeyStatistic.getSubscriberID());
      documentMap.put("transitionDate", journeyStatistic.getTransitionDate());
      documentMap.put("statsHistory", journeyStatistic.getJourneyHistorySummary());
      documentMap.put("statusHistory", journeyStatistic.getJourneyStatusSummary());
      documentMap.put("deliveryRequestID", journeyStatistic.getDeliveryRequestID());
      documentMap.put("markNotified", journeyStatistic.getMarkNotified());
      documentMap.put("markConverted", journeyStatistic.getMarkConverted());
      documentMap.put("statusNotified", journeyStatistic.getStatusNotified());
      documentMap.put("statusConverted", journeyStatistic.getStatusConverted());
      documentMap.put("statusControlGroup", journeyStatistic.getStatusControlGroup());
      documentMap.put("statusUniversalControlGroup", journeyStatistic.getStatusUniversalControlGroup());
      documentMap.put("journeyComplete", journeyStatistic.getJourneyComplete());

      //
      //  return
      //
      
      log.info("JourneyStatisticESSinkConnector.getDocumentMap: map computed, contents are="+documentMap.toString());
      
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
