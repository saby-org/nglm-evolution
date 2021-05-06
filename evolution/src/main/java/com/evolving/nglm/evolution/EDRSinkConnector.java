package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.evolution.EvolutionEngine.GenerateEDR;

public class EDRSinkConnector extends SimpleESSinkConnector
{
  
  private static final String timeZone = Deployment.getDefault().getTimeZone();
  private static final Map<String, ConnectSerde<? extends EvolutionEngineEvent>> evolutionEngineEventSerdes = new HashMap<String, ConnectSerde<? extends EvolutionEngineEvent>>();
  
  @Override public void start(Map<String, String> properties)
  {
    List<String> toAdd = new ArrayList<>();
    for (String eventName : Deployment.getEvolutionEngineEvents().keySet())
      {
        EvolutionEngineEventDeclaration engineEventDeclaration = Deployment.getEvolutionEngineEvents().get(eventName);
        if (engineEventDeclaration.getEventClass() != null && engineEventDeclaration.getEventClass().getAnnotation(GenerateEDR.class) != null)
          {
            toAdd.add(engineEventDeclaration.getEventTopic());
            evolutionEngineEventSerdes.put(engineEventDeclaration.getEventTopic(), engineEventDeclaration.getEventSerde());
          }
      }
    
    if (toAdd.isEmpty())
      {
        stop();
        return;
      }
    else 
      {
        String topicToAdd = String.join(",",toAdd);
        properties.put("indexName", topicToAdd);
        super.start(properties);
      }
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
  
  public static class EDRSinkConnectorTask extends StreamESSinkTask<EvolutionEngineEvent>
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
    protected String getDocumentIndexName(EvolutionEngineEvent evolutionEngineEvent)
    {
      return this.getDefaultIndexName() + RLMDateUtils.formatDateISOWeek(evolutionEngineEvent.getEventDate(), timeZone);
    }
    
    /*****************************************
    *
    *  unpackRecord
    *
    *****************************************/
    
    @Override public EvolutionEngineEvent unpackRecord(SinkRecord sinkRecord)
    {
      Object eventValue = sinkRecord.value();
      Schema eventValueSchema = sinkRecord.valueSchema();
      ConnectSerde<? extends EvolutionEngineEvent> connectSerde = evolutionEngineEventSerdes.get(sinkRecord.topic());
      return connectSerde.unpack(new SchemaAndValue(eventValueSchema, eventValue));
    }

    
    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override
    public Map<String, Object> getDocumentMap(EvolutionEngineEvent evolutionEngineEvent)
    {
      if (ignoreableDocument(evolutionEngineEvent)) return null;
      return prepareDocumentMap(evolutionEngineEvent);
      
    }
    
    /*************************************************
     * 
     *  prepareDocumentMap
     * 
     ************************************************/
    
    private Map<String, Object> prepareDocumentMap(EvolutionEngineEvent evolutionEngineEvent)
    {
      Map<String, Object> result = new HashMap<String, Object>();
      for (String field : evolutionEngineEvent.getEDRDocumentMap().keySet())
        {
          Object value = evolutionEngineEvent.getEDRDocumentMap().get(field);
          result.put(field, normalize(value));
        }
      result.put("subscriberID", evolutionEngineEvent.getSubscriberID());
      result.put("eventDate", evolutionEngineEvent.getEventDate());
      result.put("evolutionEngineEventID", evolutionEngineEvent.getEvolutionEngineEventID());
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

    /*************************************************
     * 
     *  ignoreableDocument
     * 
     ************************************************/
    
    private boolean ignoreableDocument(EvolutionEngineEvent evolutionEngineEvent)
    {
      boolean result = false;
      result = evolutionEngineEvent.getEvolutionEngineEventID() == null;
      result = result || evolutionEngineEvent.getEDRDocumentMap() == null || evolutionEngineEvent.getEDRDocumentMap().isEmpty();
      return result;
    }

  }
}

