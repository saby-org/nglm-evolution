/****************************************************************************
*
*  SubscriberProfileRedisSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SimpleRedisSinkConnector;
import com.evolving.nglm.core.SimpleRedisSinkConnector.SimpleRedisSinkTask;
import com.evolving.nglm.core.SimpleRedisSinkConnector.SimpleRedisSinkTask.CacheEntry;

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

      Object subscriberProfileValue = sinkRecord.value();
      Schema subscriberProfileValueSchema = sinkRecord.valueSchema();
      SubscriberProfile subscriberProfile = SubscriberProfile.unpack(new SchemaAndValue(subscriberProfileValueSchema, subscriberProfileValue));
          
      /****************************************
      *
      *  extract key/value pair
      *  - key: "2125551212" (subscriberID)
      *  - value: Avro representation of subscriber profile
      *
      ****************************************/

      byte[] key = subscriberProfile.getSubscriberID().getBytes(StandardCharsets.UTF_8);
      byte[] value = SubscriberProfile.serde().serializer().serialize(Deployment.getSubscriberProfileChangeLogTopic(), subscriberProfile);

      /****************************************
      *
      *  return
      *
      ****************************************/

      return Collections.<CacheEntry>singletonList(new CacheEntry(key,value));
    }
  }
}
