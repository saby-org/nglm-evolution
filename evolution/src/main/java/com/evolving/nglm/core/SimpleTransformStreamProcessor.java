/****************************************************************************
*
*  SimpleTransformStreamProcessor.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public abstract class SimpleTransformStreamProcessor
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(SimpleTransformStreamProcessor.class);

  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  protected abstract KeyValue<Object, Object> transform(Object key, Object value);

  /*****************************************
  *
  *  callTransform
  *
  *****************************************/

  private Iterable<KeyValue<Object, Object>> callTransform(Object key, Object value)
  {
    KeyValue<Object,Object> transformResult = transform(key, value);
    List<KeyValue<Object,Object>> iterableResult = (transformResult != null) ? Collections.<KeyValue<Object,Object>>singletonList(transformResult) : Collections.<KeyValue<Object,Object>>emptyList();
    return iterableResult;
  }

  /****************************************
  *
  *  run
  *
  *****************************************/

  public void run(String[] args, String applicationID, Serde sourceKeySerde, Serde sourceValueSerde, Serde sinkKeySerde, Serde sinkValueSerde) throws Exception
  {
    /*****************************************
    *
    *  runtime
    *
    *****************************************/

    NGLMRuntime.initialize(true);
    
    /*****************************************
    *
    *  configuration
    *
    *****************************************/

    //
    //  stream properties
    //

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID);
    props.put(StreamsConfig.STATE_DIR_CONFIG, args[0]);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[1]);
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Integer.toString(3));

    //
    //  topics
    //

    String sourceTopic = args[2];
    String sinkTopic = args[3];

    //
    //  log
    //

    log.info("main START: {} {} {} {}", args[0], args[1], sourceTopic, sinkTopic);

    /*****************************************
    *
    *  stream processor
    *
    *****************************************/

    //
    //  stream builder
    //

    StreamsBuilder builder = new StreamsBuilder();

    //
    //  source stream
    //

    KStream<Object, Object> sourceStream = builder.stream(sourceTopic, Consumed.with(sourceKeySerde, sourceValueSerde));

    //
    //  sink stream -- kpi
    //
    
    KStream<Object, Object> transformedStream = sourceStream.flatMap((key, value) -> callTransform(key, value));

    //
    //  sink stream
    //

    transformedStream.to(sinkTopic, Produced.with(sinkKeySerde, sinkValueSerde));

    /*****************************************
    *
    *  runtime
    *
    *****************************************/

    KafkaStreams streams = new KafkaStreams(builder.build(), props);

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(streams));

    /*****************************************
    *
    *  start
    *
    *****************************************/

    streams.start();
  }
  
  /*****************************************
  *
  *  class ShutdownHook
  *
  *****************************************/

  private static class ShutdownHook implements NGLMRuntime.NGLMShutdownHook
  {
    //
    //  data
    //

    private KafkaStreams kafkaStreams;

    //
    //  constructor
    //

    private ShutdownHook(KafkaStreams kafkaStreams)
    {
      this.kafkaStreams = kafkaStreams;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      boolean streamsCloseSuccessful = kafkaStreams.close(60, TimeUnit.SECONDS);
      log.info("Stopped SimpleTransformStreamProcessor" + (streamsCloseSuccessful ? "" : " (timed out)"));
    }
  }
}
