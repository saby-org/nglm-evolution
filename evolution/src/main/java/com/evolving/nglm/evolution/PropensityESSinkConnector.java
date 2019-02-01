package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.SystemTime;

public class PropensityESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return PropensityESSinkTask.class;
  }
  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class PropensityESSinkTask extends ChangeLogESSinkTask
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
      *  extract Propensity
      *
      ****************************************/
      
      Object propensityStateValue = sinkRecord.value();
      Schema propensityStateValueSchema = sinkRecord.valueSchema();
      PropensityState propensityState = PropensityState.unpack(new SchemaAndValue(propensityStateValueSchema, propensityStateValue));
      
      /****************************************
      *
      *  use offerID_segment
      *
      ****************************************/
      
      return propensityState.getOfferID() + "_" + propensityState.getdimensionID() + "_" + propensityState.getSegmentID();
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
      *  extract Propensity
      *
      ****************************************/
      
      Object propensityStateValue = sinkRecord.value();
      Schema propensityStateValueSchema = sinkRecord.valueSchema();
      PropensityState propensityState = PropensityState.unpack(new SchemaAndValue(propensityStateValueSchema, propensityStateValue));
      
      /*****************************************
      *
      *  context
      *
      *****************************************/

      Date now = SystemTime.getCurrentTime();
      
      Map<String,Object> documentMap = new HashMap<String,Object>();
      documentMap.put("offerID", propensityState.getOfferID());
      documentMap.put("dimensionID", propensityState.getdimensionID());
      documentMap.put("segmentID", propensityState.getSegmentID());
      documentMap.put("propensity", propensityState.getPropensity());
      documentMap.put("evaluationDate", now);
      
      //
      //  return
      //

      return documentMap;
      
    }
  }
}
