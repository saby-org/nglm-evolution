/****************************************************************************
*
*  SubscriberUpdateESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;

import com.rii.utilities.SystemTime;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SubscriberUpdateESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return SubscriberUpdateESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class SubscriberUpdateESSinkTask extends StreamESSinkTask
  {
    @Override public Map<String,Object> getDocumentMap(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract SubscriberProfile
      *
      ****************************************/

      Object subscriberProfileValue = sinkRecord.value();
      Schema subscriberProfileValueSchema = sinkRecord.valueSchema();
      SubscriberProfile subscriberProfile = SubscriberProfile.unpack(new SchemaAndValue(subscriberProfileValueSchema, subscriberProfileValue));

      /****************************************
      *
      *  documentMap
      *
      ****************************************/

      Map<String,Object> documentMap = new HashMap<String,Object>();

      //
      //  flat fields
      //
      
      documentMap.put("subscriberID", subscriberProfile.getSubscriberID());
      documentMap.put("activationDate", subscriberProfile.getActivationDate());
      documentMap.put("subscriberStatus", (subscriberProfile.getSubscriberStatus() != null) ? subscriberProfile.getSubscriberStatus().getExternalRepresentation() : null);
      documentMap.put("statusChangeDate", subscriberProfile.getStatusChangeDate());
      documentMap.put("previousSubscriberStatus", (subscriberProfile.getPreviousSubscriberStatus() != null) ? subscriberProfile.getPreviousSubscriberStatus().getExternalRepresentation() : null);

      //
      //  return
      //
      
      return documentMap;
    }    
  }
}
