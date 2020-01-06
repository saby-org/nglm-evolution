/****************************************************************************
*
*  JourneyMetricESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.evolution.Token.TokenStatus;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TokenChangeESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return TokenChangeESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class TokenChangeESSinkTask extends StreamESSinkTask
  {
    @Override public Map<String,Object> getDocumentMap(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract JourneyMetric
      *
      ****************************************/

      Object tokenChangeValue = sinkRecord.value();
      Schema tokenChangeValueSchema = sinkRecord.valueSchema();
      TokenChange tokenChange = TokenChange.unpack(new SchemaAndValue(tokenChangeValueSchema, tokenChangeValue));

      /****************************************
      *
      *  documentMap
      *
      ****************************************/

      Map<String,Object> documentMap = new HashMap<String,Object>();

      //
      //  flat fields
      //
      
      documentMap.put("tokenCode", tokenChange.getTokenCode());
      documentMap.put("action", tokenChange.getAction());
      documentMap.put("eventDatetime", tokenChange.geteventDateTime());
      
      documentMap.put("subscriberID", tokenChange.getSubscriberID());
      documentMap.put("eventID", tokenChange.getEventID());
      documentMap.put("returnCode", tokenChange.getReturnStatus());
      documentMap.put("origin", tokenChange.getOrigin());

      //
      //  return
      //
      
      return documentMap;
    }    
  }
}
