package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;

public class EDRSinkConnector extends SimpleESSinkConnector
{
  
  private static final String timeZone = Deployment.getDefault().getTimeZone();
  
  @Override public void start(Map<String, String> properties)
  {
    super.start(properties);
  }
  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<EDRSinkConnectorTask> taskClass()
  {
    return EDRSinkConnectorTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class EDRSinkConnectorTask extends StreamESSinkTask<EDRDetails>
  {
    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> taskConfig)
    {
      //
      //  super
      //

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
    *  getDocumentIndexName
    *
    *****************************************/
    
    @Override
    protected String getDocumentIndexName(EDRDetails eDRDetails)
    {
      return this.getDefaultIndexName() + RLMDateUtils.formatDateISOWeek(eDRDetails.getEventDate(), timeZone);
    }
    
    /*****************************************
    *
    *  unpackRecord
    *
    *****************************************/
    
    @Override public EDRDetails unpackRecord(SinkRecord sinkRecord)
    {
      Object eventValue = sinkRecord.value();
      Schema eventValueSchema = sinkRecord.valueSchema();
      return EDRDetails.unpack(new SchemaAndValue(eventValueSchema, eventValue));
    }

    
    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override
    public Map<String, Object> getDocumentMap(EDRDetails edrDetails)
    {
      return prepareDocumentMap(edrDetails);
    }
    
    /*************************************************
     * 
     *  prepareDocumentMap
     * 
     ************************************************/
    
    private Map<String, Object> prepareDocumentMap(EDRDetails edrDetails)
    {
      Map<String, Object> result = new HashMap<String, Object>();
      ParameterMap parameterMap = edrDetails.getParameterMap();
      for (String field : parameterMap.keySet())
        {
          Object value = parameterMap.get(field);
          result.put(field, normalize(value));
        }
      result.put("subscriberID", edrDetails.getSubscriberID());
      result.put("eventDate", normalize(edrDetails.getEventDate()));
      result.put("eventName", normalize(edrDetails.getEventName()));
      result.put("evolutionEngineEventID", edrDetails.getEventID());
      return result;
    }

    /*************************************************
     * 
     *  normalize
     * 
     ************************************************/
    
    private Object normalize(Object value)
    {
      Object result = value;
      if (value != null)
        {
          if (value instanceof Date) result = RLMDateUtils.formatDateForElasticsearchDefault((Date) value);
        }
      return result;
    }
  }
}

