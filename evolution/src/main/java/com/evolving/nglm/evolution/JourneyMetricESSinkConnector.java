/****************************************************************************
*
*  JourneyMetricESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Extension of JourneyStatistic.
 * Those metrics are pushed in the journeystatistic index after the end of the journey. 
 */
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
  
  public static class JourneyMetricESSinkTask extends ChangeLogESSinkTask<JourneyMetric>
  {
    @Override public JourneyMetric unpackRecord(SinkRecord sinkRecord) 
    {
      Object journeyMetricValue = sinkRecord.value();
      Schema journeyMetricValueSchema = sinkRecord.valueSchema();
      return JourneyMetric.unpack(new SchemaAndValue(journeyMetricValueSchema, journeyMetricValue));
    }
    
    @Override
    protected String getDocumentIndexName(JourneyMetric journeyMetric)
    {
      return JourneyStatisticESSinkConnector.getJourneyStatisticIndex(journeyMetric.getJourneyID(), this.getDefaultIndexName());
    }

    @Override
    public String getDocumentID(JourneyMetric journeyMetric)
    {
      return JourneyStatisticESSinkConnector.getJourneyStatisticID(journeyMetric.getSubscriberID(), journeyMetric.getJourneyID(), journeyMetric.getJourneyInstanceID());
    }
    
    @Override public Map<String,Object> getDocumentMap(JourneyMetric journeyMetric)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();

      //
      //  flat fields
      //
      documentMap.put("journeyInstanceID", journeyMetric.getJourneyInstanceID());
      documentMap.put("journeyID", journeyMetric.getJourneyID());
      documentMap.put("subscriberID", journeyMetric.getSubscriberID());
      
      // @rl: check null to prevent overriding fields with null ?
      if(journeyMetric.getJourneyExitDate() != null) {
        documentMap.put("journeyExitDate", (journeyMetric.getJourneyExitDate() != null)? RLMDateUtils.printTimestamp(journeyMetric.getJourneyExitDate()) : null);
      }

      //
      //  metrics
      //
      for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricConfiguration().getMetrics().values()) {
        Long prior = journeyMetric.getJourneyMetricsPrior().get(journeyMetricDeclaration.getID());
        Long during = journeyMetric.getJourneyMetricsDuring().get(journeyMetricDeclaration.getID());
        Long post = journeyMetric.getJourneyMetricsPost().get(journeyMetricDeclaration.getID());
        
        if (prior != null) {
          documentMap.put(journeyMetricDeclaration.getESFieldPrior(), prior);
        }
        // @rl: special. It's because we use during field as a temporary variable while still in the journey 
        //   (to record the all-time value when entered to compare it to the exit value at the end of the journey)
        //   Therefore, we should not push this temporary value with no meaning.
        if (journeyMetric.getJourneyExitDate() != null && during != null) {
          documentMap.put(journeyMetricDeclaration.getESFieldDuring(), during);
        }
        if (post != null) {
          documentMap.put(journeyMetricDeclaration.getESFieldPost(), post);
        }
      }
      
      return documentMap;
    }    
  }
}
