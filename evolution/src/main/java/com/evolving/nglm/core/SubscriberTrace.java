/****************************************************************************
*
*  SubscriberTrace.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SubscriberTrace extends SubscriberStreamOutput
{
  private String subscriberTraceMessage;

  /****************************************
  *
  *  constructor (simple)
  *
  ****************************************/

  public SubscriberTrace(String subscriberTraceMessage)
  {
    this.subscriberTraceMessage = subscriberTraceMessage;
  }

  /****************************************
  *
  *  getSubscriberTraceMessage
  *
  ****************************************/

  public String getSubscriberTraceMessage() { return subscriberTraceMessage; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static Serde<SubscriberTrace> serde()
  {
    return new SubscriberTraceSerde();
  }

  /*****************************************
  *
  *  SubscriberTraceSerde
  *
  *****************************************/

  private static class SubscriberTraceSerde implements Serde<SubscriberTrace>
  {
    //
    //  configure
    //

    @Override public void configure(Map<String, ?> configs, boolean isKey) { }

    //
    //  close
    //

    @Override public void close() { }

    //
    //  serializer
    //

    @Override public Serializer<SubscriberTrace> serializer()
    {
      return new Serializer<SubscriberTrace>()
      {
        @Override public void configure(Map<String, ?> configs, boolean isKey) { }
        @Override public void close() { }
        @Override public byte[] serialize(String topic, SubscriberTrace subscriberTrace) { try { return subscriberTrace.getSubscriberTraceMessage().getBytes(StandardCharsets.UTF_8); } catch (Exception e) { throw new SerializationException("error serializing SubscriberTrace", e); } }
      };
    }

    //
    //  deserializera
    //
    
    @Override public Deserializer<SubscriberTrace> deserializer()
    {
      return new Deserializer<SubscriberTrace>()
      {
        @Override public void configure(Map<String, ?> configs, boolean isKey) { }
        @Override public void close() { }
        @Override public SubscriberTrace deserialize(String topic, byte[] data) { try { return (data != null) ? new SubscriberTrace(new String(data, StandardCharsets.UTF_8)) : null; } catch (Exception e) { throw new SerializationException("error deserializing SubscriberTrace", e); } }
      };
    }
  }
}
