/****************************************************************************
*
*  JourneyTrafficEngine.java
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

public class JourneyTrafficEngine
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(JourneyTrafficEngine.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  static data (for the singleton instance)
  //

  private static JourneyTrafficEngineStatistics journeyTrafficEngineStatistics;
  private static KafkaStreams streams = null;

  /****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    NGLMRuntime.initialize(true);
    JourneyTrafficEngine journeyTrafficEngine = new JourneyTrafficEngine();
    journeyTrafficEngine.start(args);
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

    String applicationID = "streams-journeytrafficengine";
    String stateDirectory = args[0];
    String bootstrapServers = args[1];
    String journeyTrafficEngineKey = args[2];
    Integer kafkaReplicationFactor = Integer.parseInt(args[3]);
    Integer kafkaStreamsStandbyReplicas = Integer.parseInt(args[4]);
    Integer numberOfStreamThreads = Integer.parseInt(args[5]);

    //
    //  source topics
    //

    String journeyTrafficTopic = Deployment.getJourneyTrafficTopic();
    
    //
    //  sink topics
    //

    // none

    //
    //  changelogs
    //

    String journeyTrafficChangeLog = Deployment.getJourneyTrafficChangeLog();

    //
    //  log
    //

    log.info("main START: {} {} {} {} {}", stateDirectory, bootstrapServers, kafkaStreamsStandbyReplicas, numberOfStreamThreads, kafkaReplicationFactor);

    //
    //  create monitoring object
    //

    journeyTrafficEngineStatistics = new JourneyTrafficEngineStatistics(applicationID);

    /*****************************************
    *
    *  stream properties
    *
    *****************************************/

    Properties streamsProperties = new Properties();
    streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID);
    streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory);
    streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsProperties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JourneyTrafficEventTimestampExtractor.class.getName());
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

    final ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
    final ConnectSerde<JourneyStatisticWrapper> journeyStatisticWrapperSerde = JourneyStatisticWrapper.serde();
    final ConnectSerde<JourneyTrafficHistory> journeyTrafficHistorySerde = JourneyTrafficHistory.serde();
    final ConnectSerde<GUIManagedObject> guiManagedObjectSerde = GUIManagedObject.commonSerde();

    /*****************************************
    *
    *  source streams
    *
    *****************************************/

    KStream<StringKey, JourneyStatisticWrapper> rekeyedJourneyStatisticStream = builder.stream(journeyTrafficTopic, Consumed.with(stringKeySerde, journeyStatisticWrapperSerde));

    /*****************************************
    *
    *  journeyTrafficState -- update
    *
    *****************************************/

    KeyValueBytesStoreSupplier journeyTrafficSupplier = Stores.persistentKeyValueStore(journeyTrafficChangeLog);
    Materialized journeyTrafficStore = Materialized.<StringKey, JourneyTrafficHistory>as(journeyTrafficSupplier).withKeySerde(stringKeySerde).withValueSerde(journeyTrafficHistorySerde);
    KTable<StringKey, JourneyTrafficHistory> unusedJourneyTraffic = rekeyedJourneyStatisticStream.groupByKey(Serialized.with(stringKeySerde, journeyStatisticWrapperSerde)).aggregate(JourneyTrafficEngine::nullJourneyTrafficHistory, JourneyTrafficEngine::updateJourneyTrafficHistory, journeyTrafficStore);
    
    /*****************************************
    *
    *  sink
    *
    *****************************************/

    //
    //  none
    //

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

        synchronized (JourneyTrafficEngine.this)
          {
            JourneyTrafficEngine.this.notifyAll();
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

    log.info("journey traffic engine started");
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

      if (journeyTrafficEngineStatistics != null) journeyTrafficEngineStatistics.unregister();

      //
      //  stop streams
      //

      boolean streamsCloseSuccessful = kafkaStreams.close(60, java.util.concurrent.TimeUnit.SECONDS);
      log.info("Stopped JourneyTrafficEngine" + (streamsCloseSuccessful ? "" : " (timed out)"));
    }
  }

  /*****************************************
  *
  *  nullJourneyTrafficHistory
  *
  ****************************************/

  public static JourneyTrafficHistory nullJourneyTrafficHistory() { 
    JourneyTrafficHistory history = new JourneyTrafficHistory(
        "",
        SystemTime.getCurrentTime(),
        SystemTime.getCurrentTime(),
        Deployment.getJourneyTrafficArchivePeriodInSeconds(),
        Deployment.getJourneyTrafficArchiveMaxNumberOfPeriods(),
        new JourneyTrafficSnapshot(),
        new HashMap<Integer, JourneyTrafficSnapshot>()); 
    
    //
    // add an empty record at archived start time 
    // 
    
    history.getArchivedData().put(0, new JourneyTrafficSnapshot());
    
    return history;
  }
  
  /*****************************************
  *
  *  updateJourneyTrafficHistory
  *
  *****************************************/

  public static JourneyTrafficHistory updateJourneyTrafficHistory(StringKey journeyID, JourneyStatisticWrapper event, JourneyTrafficHistory history)
  {
    Date currentDate = SystemTime.getCurrentTime();
    boolean journeyComplete = false;
    
    // 
    // start by updating archived data if needed
    // > We need to archive current data before updating it !
    // 
    
    Long timeDelta = currentDate.getTime() - history.getLastArchivedDataDate().getTime();
    int periodsSinceLastArchivedData = (int) ( timeDelta / (history.getArchivePeriodInSeconds() * 1000));
    
    if(periodsSinceLastArchivedData > 0) 
      {
        Map<Integer, JourneyTrafficSnapshot> map = history.getArchivedData();

        // 
        // move previous archived data
        // 

        for(int i = history.getMaxNumberOfPeriods()-1; i >= periodsSinceLastArchivedData; i--) 
          {
            JourneyTrafficSnapshot journeyTrafficByNode = map.get(i - periodsSinceLastArchivedData);
            if(journeyTrafficByNode != null) 
              {
                map.put(i, journeyTrafficByNode);
              } 
            else 
              {
                map.remove(i);
              }
          }

        // 
        // fill with current data (new archived data)
        //
        
        map.put(0, new JourneyTrafficSnapshot(history.getCurrentData()));
        for(int i = 1; i < periodsSinceLastArchivedData; i++) 
          {
            map.remove(i);
          }
      }

    history.setLastArchivedDataDate(RLMDateUtils.addSeconds(history.getLastArchivedDataDate(), history.getArchivePeriodInSeconds() * periodsSinceLastArchivedData));
    history.setLastUpdateDate(currentDate);
    history.setJourneyID(journeyID.getKey());
    
    // 
    // update current data
    //
    
    if(event.getJourneyStatistic() != null)
      {
      String fromNodeID = event.getJourneyStatistic().getFromNodeID();
      String toNodeID = event.getJourneyStatistic().getToNodeID();
      
      if(fromNodeID != null) 
        {
          SubscriberTraffic traffic = history.getCurrentData().getByNode().get(fromNodeID);
          if(traffic == null) 
            {
              traffic = new SubscriberTraffic();
              history.getCurrentData().getByNode().put(fromNodeID, traffic);
            }
          traffic.addOutflow();
        }
      else 
        {
          //
          // Update global entrance for this journey (global and byStratum)
          //
  
          history.getCurrentData().getGlobal().addInflow();
          SubscriberTraffic stratumTraffic = history.getCurrentData().getByStratum().get(event.getSubscriberStratum());
          if(stratumTraffic == null)
            {
              stratumTraffic = new SubscriberTraffic();
              stratumTraffic.setEmptyRewardsMap();
              history.getCurrentData().getByStratum().put(event.getSubscriberStratum(), stratumTraffic);
            }
          stratumTraffic.addInflow();
        }
      
      if(toNodeID != null) 
        {
          SubscriberTraffic traffic = history.getCurrentData().getByNode().get(toNodeID);
          if(traffic == null) 
            {
              traffic = new SubscriberTraffic();
              history.getCurrentData().getByNode().put(toNodeID, traffic);
            }
          traffic.addInflow();
        } 
      
      //
      // Update global exit for this journey (traffic for global and byStratum)
      //
      
      if (event.getJourneyStatistic().getJourneyComplete()) 
        {
          // 
          // Journey exit 
          // 
  
          journeyComplete = true;
          history.getCurrentData().getGlobal().addOutflow();
          SubscriberTraffic stratumTraffic = history.getCurrentData().getByStratum().get(event.getSubscriberStratum());
          if(stratumTraffic == null)
            {
              stratumTraffic = new SubscriberTraffic();
              stratumTraffic.setEmptyRewardsMap();
              history.getCurrentData().getByStratum().put(event.getSubscriberStratum(), stratumTraffic);
            }
          stratumTraffic.addOutflow();
        }
      
      // 
      // update status map
      // 
      
      if (event.isStatusUpdated()) 
        {
          SubscriberJourneyStatus previousStatus = (event.getJourneyStatistic().getPreviousJourneyStatus() != null) ? event.getJourneyStatistic().getPreviousJourneyStatus().getSubscriberJourneyStatus() : null;
          SubscriberJourneyStatus currentStatus = event.getJourneyStatistic().getSubscriberJourneyStatus();
          if (previousStatus == null || currentStatus != previousStatus)
            {
              //
              // by status map update
              //
      
              if (previousStatus != null)
                {
                  SubscriberTraffic previousStatusTraffic = history.getCurrentData().getByStatus().get(previousStatus.getExternalRepresentation());
                  if(previousStatusTraffic != null) 
                    {
                      previousStatusTraffic.addOutflow();
                    }
                }
      
              SubscriberTraffic currentStatusTraffic = history.getCurrentData().getByStatus().get(currentStatus.getExternalRepresentation());
              if (currentStatusTraffic == null) 
                {
                  currentStatusTraffic = new SubscriberTraffic();
                  history.getCurrentData().getByStatus().put(currentStatus.getExternalRepresentation(), currentStatusTraffic);
                }
              currentStatusTraffic.addInflow();
              
              //
              // by stratum map update
              //
              
              List<String> subscriberStratum = event.getSubscriberStratum();
              Map<String, SubscriberTraffic> stratumTraffic = history.getCurrentData().getByStatusByStratum().get(subscriberStratum);
              if (stratumTraffic == null) 
                {
                  stratumTraffic = new HashMap<String, SubscriberTraffic>();
                  history.getCurrentData().getByStatusByStratum().put(subscriberStratum, stratumTraffic);
                }
      
              if (previousStatus != null)
                {
                  SubscriberTraffic previousStatusTraffic = stratumTraffic.get(previousStatus.getExternalRepresentation());
                  if (previousStatusTraffic != null) 
                    {
                      previousStatusTraffic.addOutflow();
                    }
                }
      
              currentStatusTraffic = stratumTraffic.get(currentStatus.getExternalRepresentation());
              if (currentStatusTraffic == null) 
                {
                  currentStatusTraffic = new SubscriberTraffic();
                  stratumTraffic.put(currentStatus.getExternalRepresentation(), currentStatusTraffic);
                }
              currentStatusTraffic.addInflow();
            }
        }
      
        //
        // update abTesting
        //
        if(event.getJourneyStatistic().getSample() != null)
          {
            history.getCurrentData().incrementABTesting(event.getJourneyStatistic().getSample());
          }
      }
    
    // 
    // Update rewards
    // 
    else if (event.getLastRewards() != null && event.getLastRewards().getAmount() > 0)
      {
        RewardHistory rewards = event.getLastRewards();
        history.getCurrentData().getGlobal().addRewards(rewards.getRewardID(), rewards.getAmount());
        SubscriberTraffic stratumTraffic = history.getCurrentData().getByStratum().get(event.getSubscriberStratum());
        if(stratumTraffic == null)
          {
            stratumTraffic = new SubscriberTraffic();
            stratumTraffic.setEmptyRewardsMap();
            history.getCurrentData().getByStratum().put(event.getSubscriberStratum(), stratumTraffic);
          }
        stratumTraffic.addRewards(rewards.getRewardID(), rewards.getAmount());
      }

    //
    //  statistics
    //

    updateJourneyTrafficEngineStatistics(journeyComplete);

    //
    //  return
    //
    
    return history;
  }

  /*****************************************
  *
  *  updateJourneyTrafficEngineStatistics
  *
  *****************************************/

  private static void updateJourneyTrafficEngineStatistics(boolean journeyComplete)
  {
    journeyTrafficEngineStatistics.incrementEventProcessedCount();
    if (journeyComplete) journeyTrafficEngineStatistics.incrementCompletedJourneys();
  }

  /*****************************************
  *
  *  class JourneyTrafficEventTimestampExtractor
  *
  *****************************************/

  public static class JourneyTrafficEventTimestampExtractor implements TimestampExtractor
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
