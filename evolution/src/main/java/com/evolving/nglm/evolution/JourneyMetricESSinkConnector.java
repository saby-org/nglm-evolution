/****************************************************************************
*
*  JourneyMetricESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class JourneyMetricESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return JourneyMetricESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class JourneyMetricESSinkTask extends StreamESSinkTask
  {
    @Override public Map<String,Object> getDocumentMap(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract JourneyMetric
      *
      ****************************************/

      Object journeyMetricValue = sinkRecord.value();
      Schema journeyMetricValueSchema = sinkRecord.valueSchema();
      JourneyMetric journeyMetric = JourneyMetric.unpack(new SchemaAndValue(journeyMetricValueSchema, journeyMetricValue));

      /****************************************
      *
      *  documentMap
      *
      ****************************************/

      Map<String,Object> documentMap = new HashMap<String,Object>();

      //
      //  flat fields
      //
      
      documentMap.put("journeyInstanceID", journeyMetric.getJourneyInstanceID());
      documentMap.put("journeyID", journeyMetric.getJourneyID());
      documentMap.put("subscriberID", journeyMetric.getSubscriberID());
      documentMap.put("journeyExitDate", journeyMetric.getJourneyExitDate());

      //
      //  metrics
      //

      for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values())
        {
          documentMap.put(journeyMetricDeclaration.getESFieldPrior(), journeyMetric.getJourneyMetricsPrior().get(journeyMetricDeclaration.getID()));
          documentMap.put(journeyMetricDeclaration.getESFieldDuring(), journeyMetric.getJourneyMetricsDuring().get(journeyMetricDeclaration.getID()));
          documentMap.put(journeyMetricDeclaration.getESFieldPost(), journeyMetric.getJourneyMetricsPost().get(journeyMetricDeclaration.getID()));
        }

      //
      //  return
      //
      
      return documentMap;
    }    
  }
}
