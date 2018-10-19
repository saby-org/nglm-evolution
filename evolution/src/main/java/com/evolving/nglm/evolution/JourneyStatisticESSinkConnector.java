/****************************************************************************
*
*  JourneyStatisticESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;

import com.evolving.nglm.core.SystemTime;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
  
  public static class JourneyStatisticESSinkTask extends StreamESSinkTask
  {
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
      documentMap.put("linkID", journeyStatistic.getLinkID());
      documentMap.put("fromNodeID", journeyStatistic.getFromNodeID());
      documentMap.put("toNodeID", journeyStatistic.getToNodeID());
      documentMap.put("exited", journeyStatistic.getExited());

      //
      //  return
      //
      
      return documentMap;
    }    
  }
}
