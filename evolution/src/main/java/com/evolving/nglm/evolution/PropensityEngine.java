/****************************************************************************
*
*  PropensityEngine.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.KStreamsUniqueKeyServer;
import com.evolving.nglm.core.NGLMKafkaClientSupplier;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.RecordSubscriberID;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.StringValue;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.SubscriberTrace;
import com.evolving.nglm.core.SubscriberTraceControl;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.DeliveryRequest.DeliveryPriority;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvolutionUtilities.RoundingSelection;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.Expression.ExpressionEvaluationException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.MetricHistory.BucketRepresentation;
import com.evolving.nglm.evolution.Journey.ContextUpdate;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatusField;
import com.evolving.nglm.evolution.JourneyHistory.RewardHistory;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramOperation;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;
import com.evolving.nglm.evolution.SubscriberProfile.EvolutionSubscriberStatus;
import com.evolving.nglm.evolution.Token.TokenStatus;
import com.evolving.nglm.evolution.UCGState.UCGGroup;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class PropensityEngine
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(PropensityEngine.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  static data (for the singleton instance)
  //

  private static PropensityEngineStatistics propensityEngineStatistics;
  private static KafkaStreams streams = null;

  /****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    NGLMRuntime.initialize(true);
    PropensityEngine propensityEngine = new PropensityEngine();
    propensityEngine.start(args);
  }

  /****************************************
  *
  *  start
  *
  *****************************************/

  private void start(String[] args)
  {
    /*****************************************
    *
    *  configuration
    *
    *****************************************/

    //
    //  kafka configuration
    //

    String applicationID = "streams-propensityengine";
    String stateDirectory = args[0];
    String bootstrapServers = args[1];
    String propensityEngineKey = args[2];
    Integer kafkaReplicationFactor = Integer.parseInt(args[3]);
    Integer kafkaStreamsStandbyReplicas = Integer.parseInt(args[4]);
    Integer numberOfStreamThreads = Integer.parseInt(args[5]);

    //
    //  source topics
    //

    String propensityOutputTopic = Deployment.getPropensityOutputTopic();
    
    //
    //  sink topics
    //

    String propensityLogTopic = Deployment.getPropensityLogTopic();

    //
    //  changelogs
    //

    String propensityStateChangeLog = Deployment.getPropensityStateChangeLog();

    //
    //  log
    //

    log.info("main START: {} {} {} {} {}", stateDirectory, bootstrapServers, kafkaStreamsStandbyReplicas, numberOfStreamThreads, kafkaReplicationFactor);

    //
    //  create monitoring object
    //

    propensityEngineStatistics = new PropensityEngineStatistics(applicationID);

    /*****************************************
    *
    *  stream properties
    *
    *****************************************/

    Properties streamsProperties = new Properties();
    streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID);
    streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory);
    streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsProperties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, PropensityEventTimestampExtractor.class.getName());
    streamsProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Integer.toString(numberOfStreamThreads));
    streamsProperties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, Integer.toString(kafkaReplicationFactor));
    streamsProperties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, Integer.toString(kafkaStreamsStandbyReplicas));
    streamsProperties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString());
    streamsProperties.put("producer.batch.size", Integer.toString(100000));
    StreamsConfig streamsConfig = new StreamsConfig(streamsProperties);

    /*****************************************
    *
    *  stream builder
    *
    *****************************************/

    StreamsBuilder builder = new StreamsBuilder();

    /*****************************************
    *
    *  serdes
    *
    *****************************************/

    final Serde<byte[]> byteArraySerde = new Serdes.ByteArraySerde();
    final ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
    final ConnectSerde<PropensityEventOutput> propensityEventOutputSerde = PropensityEventOutput.serde();
    final ConnectSerde<PropensityKey> propensityKeySerde = PropensityKey.serde();
    final ConnectSerde<PropensityState> propensityStateSerde = PropensityState.serde();

    /*****************************************
    *
    *  source streams
    *
    *****************************************/

    KStream<PropensityKey, PropensityEventOutput> propensityEventOutputSourceStream = builder.stream(propensityOutputTopic, Consumed.with(propensityKeySerde, propensityEventOutputSerde));

    /*****************************************
    *
    *  propensityState -- update
    *
    *****************************************/

    //
    //  propensity aggregate
    //

    KeyValueBytesStoreSupplier supplier = Stores.persistentKeyValueStore(propensityStateChangeLog);
    Materialized propensityStateStore = Materialized.<PropensityKey, PropensityState>as(supplier).withKeySerde(propensityKeySerde).withValueSerde(propensityStateSerde.optionalSerde());
    KTable<PropensityKey, PropensityState> propensityState = propensityEventOutputSourceStream.groupByKey(Serialized.with(propensityKeySerde, propensityEventOutputSerde)).aggregate(PropensityEngine::nullPropensityState, PropensityEngine::updatePropensityState, propensityStateStore);

    //
    //  convert to stream
    //

    KStream<PropensityKey, PropensityState> propensityStateStream = propensityEventOutputSourceStream.leftJoin(propensityState, PropensityEngine::getPropensityState);

    /*****************************************
    *
    *  sink
    *
    *****************************************/

    //
    //  sink - core streams
    //

    propensityStateStream.to(propensityLogTopic, Produced.with(propensityKeySerde, propensityStateSerde));

    /*****************************************
    *
    *  runtime
    *
    *****************************************/

    streams = new KafkaStreams(builder.build(), streamsConfig, new NGLMKafkaClientSupplier());

    /*****************************************
    *
    *  state change listener
    *
    *****************************************/

    KafkaStreams.StateListener stateListener = new KafkaStreams.StateListener()
    {
      @Override public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState)
      {
        //
        //  streams state
        //

        synchronized (PropensityEngine.this)
          {
            PropensityEngine.this.notifyAll();
          }
      }
    };
    streams.setStateListener(stateListener);

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("streams starting");

    /*****************************************
    *
    *  start streams
    *
    *****************************************/

    streams.start();

    /*****************************************
    *
    *  waiting for streams initialization
    *
    *****************************************/

    waitForStreams();

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("streams started");

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/

    NGLMRuntime.addShutdownHook(new ShutdownHook(streams));

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("propensity engine started");
  }

  /*****************************************
  *
  *  waitForStreams
  *
  *****************************************/

  public void waitForStreams(Date timeout)
  {
    NGLMRuntime.registerSystemTimeDependency(this);
    boolean streamsInitialized = false;
    while (! streamsInitialized)
      {
        synchronized (this)
          {
            switch (streams.state())
              {
                case CREATED:
                case REBALANCING:
                  try
                    {
                      Date now = SystemTime.getCurrentTime();
                      long waitTime = timeout.getTime() - now.getTime();
                      if (waitTime > 0) this.wait(waitTime);
                    }
                  catch (InterruptedException e)
                    {
                    }
                  break;

                case RUNNING:
                  streamsInitialized = true;
                  break;

                case NOT_RUNNING:
                case ERROR:
                case PENDING_SHUTDOWN:
                  break;
              }
          }
      }
  }

  //
  //  waitForStreams
  //

  public void waitForStreams()
  {
    waitForStreams(NGLMRuntime.END_OF_TIME);
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
      //
      //  stop stats collection
      //

      if (propensityEngineStatistics != null) propensityEngineStatistics.unregister();

      //
      //  stop streams
      //

      boolean streamsCloseSuccessful = kafkaStreams.close(60, java.util.concurrent.TimeUnit.SECONDS);
      log.info("Stopped PropensityEngine" + (streamsCloseSuccessful ? "" : " (timed out)"));
    }
  }

  /*****************************************
  *
  *  nullPropensityState
  *
  ****************************************/

  public static PropensityState nullPropensityState() { return (PropensityState) null; }

  /*****************************************
  *
  *  updatePropensityState
  *
  ****************************************/

  public static PropensityState updatePropensityState(PropensityKey aggKey, PropensityEventOutput propensityEvent, PropensityState currentPropensityState)
  {
    /****************************************
    *
    *  get (or create) entry
    *
    ****************************************/

    PropensityState propensityState = (currentPropensityState != null) ? new PropensityState(currentPropensityState) : new PropensityState(aggKey);

    /*****************************************
    *
    *  update PropensityState
    *
    *****************************************/

    if (propensityEvent.isAccepted())
    {
      propensityState.setAcceptanceCount(propensityState.getAcceptanceCount() + 1L);
      propensityEngineStatistics.incrementAcceptanceCount();
    }

    propensityState.setPresentationCount(propensityState.getPresentationCount() + 1L);
    propensityEngineStatistics.incrementPresentationCount();

    /*****************************************
    *
    *  statistics
    *
    *****************************************/

    updatePropensityEngineStatistics(propensityEvent);

    /****************************************
    *
    *  return the updated PropensityState (it is always different than the current one)
    *
    ****************************************/

    return propensityState;
  }

  /*****************************************
  *
  *  getPropensityState
  *
  *****************************************/

  private static PropensityState getPropensityState(PropensityEventOutput propensityEventOutput, PropensityState propensityState)
  {
    return propensityState;
  }
  
  /*****************************************
  *
  *  updatePropensityEngineStatistics
  *
  *****************************************/

  private static void updatePropensityEngineStatistics(PropensityEventOutput event)
  {
    propensityEngineStatistics.updateEventProcessedCount(1);
  }

  /*****************************************
  *
  *  class PropensityEventTimestampExtractor
  *
  *****************************************/

  public static class PropensityEventTimestampExtractor implements TimestampExtractor
  {
    /*****************************************
    *
    *  extract
    *
    *****************************************/

    @Override public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp)
    {
      return SystemTime.getCurrentTime().getTime();
    }
  }
}
