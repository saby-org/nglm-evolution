/****************************************************************************
*
*  NGLMKafkaClientSupplier.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

import java.util.Map;
import java.util.concurrent.Future;

public class NGLMKafkaClientSupplier extends DefaultKafkaClientSupplier
{
  /*****************************************
  *
  *  getProducer
  *
  *****************************************/

  @Override public Producer<byte[], byte[]> getProducer(Map<String, Object> config)
  {
    return new NGLMKafkaProducer(config);
  }

  /*****************************************
  *
  *  class NGLMKafkaProducer
  *
  *****************************************/

  public static class NGLMKafkaProducer extends KafkaProducer<byte[], byte[]>
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public NGLMKafkaProducer(Map<String, Object> config)
    {
      super(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    /*****************************************
    *
    *  send
    *
    *****************************************/

    @Override public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback)
    {
      return super.send(new ProducerRecord<byte[], byte[]>(record.topic(), record.partition(), SystemTime.getCurrentTime().getTime(), record.key(), record.value()), callback);
    }
  }
}

