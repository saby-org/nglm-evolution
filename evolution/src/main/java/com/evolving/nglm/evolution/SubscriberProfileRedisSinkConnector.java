/****************************************************************************
*
*  SubscriberProfileRedisSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SimpleRedisSinkConnector;
import com.evolving.nglm.core.SimpleRedisSinkConnector.SimpleRedisSinkTask;
import com.evolving.nglm.core.SimpleRedisSinkConnector.SimpleRedisSinkTask.CacheEntry;
import com.evolving.nglm.core.ServerRuntimeException;

import com.rii.utilities.DatabaseUtilities;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.json.simple.JSONObject;

import java.nio.charset.StandardCharsets;

import java.util.Collections;
import java.util.List;

public class SubscriberProfileRedisSinkConnector extends SimpleRedisSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return SubscriberProfileRedisSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class SubscriberProfileRedisSinkTask extends SimpleRedisSinkTask
  {
    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(java.util.Map<String, String> taskConfig)
    {
      super.start(taskConfig);
      SubscriberState.forceClassLoad();
    }

    /****************************************
    *
    *  getCacheEntries
    *
    ****************************************/

    @Override public List<CacheEntry> getCacheEntries(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract SubscriberProfile
      *
      ****************************************/

      Object subscriberStateValue = sinkRecord.value();
      Schema subscriberStateValueSchema = sinkRecord.valueSchema();
      SubscriberState subscriberState = SubscriberState.unpack(new SchemaAndValue(subscriberStateValueSchema, subscriberStateValue));
          
      /****************************************
      *
      *  extract key/value pair
      *  - key: "2125551212" (subscriberID)
      *  - value: Avro representation of subscriber profile
      *
      ****************************************/

      //
      //  data
      //
      
      byte[] rawKey = subscriberState.getSubscriberProfile().getSubscriberID().getBytes(StandardCharsets.UTF_8);
      byte[] rawValue = SubscriberProfile.getSubscriberProfileSerde().serializer().serialize(Deployment.getSubscriberProfileRegistrySubject(), subscriberState.getSubscriberProfile());

      //
      //  compress 
      //

      byte[] key = rawKey;
      byte[] value = SubscriberProfileService.compressSubscriberProfile(rawValue, Deployment.getSubscriberProfileCompressionType());

      /****************************************
      *
      *  return
      *
      ****************************************/

      return Collections.<CacheEntry>singletonList(new CacheEntry(key,value));
    }
  }
}
