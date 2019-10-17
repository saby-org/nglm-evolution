package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StringKey;

public class JourneyTrafficESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return JourneyTrafficESSinkTask.class;
  }
  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class JourneyTrafficESSinkTask extends ChangeLogESSinkTask
  {

    /*****************************************
    *
    *  getDocumentID
    *
    *****************************************/
    
    @Override public String getDocumentID(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract JourneyID
      *
      ****************************************/
      
      Object journeyIDValue = sinkRecord.key();
      Schema journeyIDValueSchema = sinkRecord.keySchema();
      StringKey journeyID = StringKey.unpack(new SchemaAndValue(journeyIDValueSchema, journeyIDValue));
      
      /****************************************
      *
      *  use journeyID
      *
      ****************************************/
      
      return "_" + journeyID.hashCode();
    }
    
    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override public Map<String,Object> getDocumentMap(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract JourneyID
      *
      ****************************************/
      
      Object journeyIDValue = sinkRecord.key();
      Schema journeyIDValueSchema = sinkRecord.keySchema();
      StringKey journeyID = StringKey.unpack(new SchemaAndValue(journeyIDValueSchema, journeyIDValue));
      
      /****************************************
      *
      *  extract JourneyTraffic
      *
      ****************************************/
      
      Object journeyTrafficValue = sinkRecord.value();
      Schema journeyTrafficValueSchema = sinkRecord.valueSchema();
      JourneyTrafficHistory journeyTraffic = JourneyTrafficHistory.unpack(new SchemaAndValue(journeyTrafficValueSchema, journeyTrafficValue));
      
      /*****************************************
      *
      *  context
      *
      *****************************************/
      
      Map<String,Object> documentMap = new HashMap<String,Object>();
      JSONObject journeyTrafficJSON = journeyTraffic.getJSONRepresentation();
      documentMap.put("journeyID", journeyID.getKey());
      documentMap.put("lastUpdateDate", journeyTrafficJSON.get("lastUpdateDate"));
      documentMap.put("lastArchivedDataDate", journeyTrafficJSON.get("lastArchivedDataDate"));
      documentMap.put("archivePeriodInSeconds", journeyTrafficJSON.get("archivePeriodInSeconds"));
      documentMap.put("maxNumberOfPeriods", journeyTrafficJSON.get("maxNumberOfPeriods"));
      documentMap.put("currentData", journeyTrafficJSON.get("currentData"));
      documentMap.put("archivedData", journeyTrafficJSON.get("archivedData"));
      
      //
      //  return
      //

      return documentMap;
      
    }
  }
}
