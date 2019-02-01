/****************************************************************************
*
*  PropensityEngine.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.KStreamsUniqueKeyServer;
import com.evolving.nglm.core.NGLMKafkaClientSupplier;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberStreamEvent;

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

  //
  //  hardwired arguments
  //

  public static String baseTimeZone = Deployment.getBaseTimeZone();

  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private static KStreamsUniqueKeyServer uniqueKeyServer = new KStreamsUniqueKeyServer();
  private static ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private static PropensityEngineStatistics propensityEngineStatistics;
  
  /****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    /*****************************************
    *
    *  runtime
    *
    *****************************************/

    NGLMRuntime.initialize();
    
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
    
    String subscriberGroupEpochTopic = Deployment.getSubscriberGroupEpochTopic();
    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("propensityengine-subscribergroupepoch", propensityEngineKey, bootstrapServers, subscriberGroupEpochTopic, SubscriberGroupEpoch::unpack);

    //
    //  source topics 
    //
    
    String presentationLogTopic = Deployment.getPresentationLogTopic();
    String acceptanceLogTopic = Deployment.getAcceptanceLogTopic();
    String subscriberStateChangeLogTopic = Deployment.getSubscriberStateChangeLogTopic();


    //
    //  sink topics
    //
    
    String propensityLogTopic = Deployment.getPropensityLogTopic();
    String propensityStateChangeLog = Deployment.getPropensityStateChangeLog();
    String propensityStateChangeLogTopic = Deployment.getPropensityStateChangeLogTopic();
   
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
    final ConnectSerde<PropensityKey> propensityKeySerde = PropensityKey.serde();
    final ConnectSerde<PresentationLog> presentationLogSerde = PresentationLog.serde();
    final ConnectSerde<AcceptanceLog> acceptanceLogSerde = AcceptanceLog.serde();
    final ConnectSerde<PropensityState> propensityStateSerde = PropensityState.serde();
    final ConnectSerde<SubscriberState> subscriberStateSerde = SubscriberState.serde();
    final ConnectSerde<PropensitySegmentOutput> propensitySegmentOutputSerde = PropensitySegmentOutput.serde();
    
    /****************************************
    *
    *  ensure copartitioned
    *
    ****************************************/
    
    //
    //  no longer needed in streams 2.0
    //
    
    /*****************************************
    *
    *  source streams
    *
    *****************************************/

    KStream<StringKey, PresentationLog> presentationLogSourceStream = builder.stream(presentationLogTopic, Consumed.with(stringKeySerde, presentationLogSerde));
    KStream<StringKey, AcceptanceLog> acceptanceLogSourceStream = builder.stream(acceptanceLogTopic, Consumed.with(stringKeySerde, acceptanceLogSerde));
    
    //
    //  merge source streams
    //

    ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>> propensityEventStreams = new ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    propensityEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) presentationLogSourceStream);
    propensityEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) acceptanceLogSourceStream);
    KStream compositeStream = null;
    for (KStream<StringKey, ? extends SubscriberStreamEvent> eventStream : propensityEventStreams)
      {
        compositeStream = (compositeStream == null) ? eventStream : compositeStream.merge(eventStream);
      }
    KStream<StringKey, SubscriberStreamEvent> propensityEventStream = (KStream<StringKey, SubscriberStreamEvent>) compositeStream;
    
    //
    // KTable from subscriberStateChangeLogTopic
    //
    
    KTable<StringKey, SubscriberState> subscriberState = builder.table(subscriberStateChangeLogTopic, Consumed.with(stringKeySerde, subscriberStateSerde.optionalSerde()));
    
    //
    // left join
    //
    
    KStream<StringKey, List<PropensitySegmentOutput>> propensitySegmentsStream = propensityEventStream.leftJoin(subscriberState, PropensityEngine::getPropensitySegmentOutputs);
    
    /*****************************************
    *
    *  get outputs
    *
    *****************************************/

    KStream<StringKey, PropensitySegmentOutput> propensityOutputStream = propensitySegmentsStream.flatMapValues(PropensityEngine::getPropensityStateOutputs);
    
    //
    // rekey
    //

    KStream<PropensityKey, PropensitySegmentOutput> rekeyedpropensityStream = propensityOutputStream.map(PropensityEngine::rekeypropensityStream);
    
    /*****************************************
    *
    *  propensityState -- update
    *
    *****************************************/
    
    KeyValueBytesStoreSupplier supplier = Stores.persistentKeyValueStore(propensityStateChangeLog);
    Materialized propensityStateStore = Materialized.<PropensityKey, PropensityState>as(supplier).withKeySerde(propensityKeySerde).withValueSerde(propensityStateSerde.optionalSerde());
    KTable<PropensityKey, PropensityState> propensityState = rekeyedpropensityStream.groupByKey(Serialized.with(propensityKeySerde, propensitySegmentOutputSerde)).aggregate(PropensityEngine::nullPropensityState, PropensityEngine::updatePropensityState, propensityStateStore);

    /*****************************************
    *
    *  sink
    *
    *****************************************/
    
    propensityState.toStream().to(propensityLogTopic, Produced.with(propensityKeySerde, propensityStateSerde));
    
    /*****************************************
    *
    *  runtime
    *
    *****************************************/

    KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig, new NGLMKafkaClientSupplier());

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(streams, subscriberGroupEpochReader));

    /*****************************************
    *
    *  start
    *
    *****************************************/

    streams.start();
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
  
  public static PropensityState updatePropensityState(PropensityKey aggKey, PropensitySegmentOutput propensityEvent, PropensityState currentPropensityState)
  {
    /****************************************
    *
    *  get (or create) entry
    *
    ****************************************/

    PropensityState propensityState = (currentPropensityState != null) ? new PropensityState(currentPropensityState) : new PropensityState(aggKey.getOfferID(), aggKey.getDimensionID(), aggKey.getSegmentID());
    boolean propensityStateUpdated = (currentPropensityState != null) ? false : true;
    
    /*****************************************
    *
    *  update PropensityState
    *
    *****************************************/
    
    if (propensityEvent.isAccepted()) 
    {
      propensityState.setAcceptanceCount(propensityState.getAcceptanceCount() + 1L);
      propensityStateUpdated = true;
      propensityEngineStatistics.incrementAcceptanceCount();
    }
   else 
    {
      propensityState.setPresentationCount(propensityState.getPresentationCount() + 1L);
      propensityStateUpdated = true;
      propensityEngineStatistics.incrementPresentationCount();
    }

    /****************************************
    *
    *  return
    *
    ****************************************/

    return propensityStateUpdated ? propensityState : currentPropensityState;
  }
  
  /*****************************************
  *
  *  getPropensitySegmentOutputs
  *
  *****************************************/

  private static List<PropensitySegmentOutput> getPropensitySegmentOutputs(SubscriberStreamEvent propensityEvent, SubscriberState subscriberState)
  {
    List<PropensitySegmentOutput> result = new ArrayList<PropensitySegmentOutput>();
    if (subscriberState != null)
      {
        if (subscriberState != null)
          {
            if(subscriberState.getSubscriberProfile().getSubscriberGroups(subscriberGroupEpochReader) != null){
              Map<String, String> subscriberGroups = subscriberState.getSubscriberProfile().getSubscriberGroups(subscriberGroupEpochReader);
              for (String dimensionID : subscriberGroups.keySet())
                {
                  if (propensityEvent instanceof AcceptanceLog)
                    {
                      AcceptanceLog acceptanceLog = (AcceptanceLog) propensityEvent;
                      PropensitySegmentOutput propensitySegmentOutput = new PropensitySegmentOutput(acceptanceLog.getOfferID(), dimensionID, subscriberGroups.get(dimensionID), true);
                      result.add(propensitySegmentOutput);
                    }
                  else if (propensityEvent instanceof PresentationLog)
                    {
                      PresentationLog presentationLog = (PresentationLog) propensityEvent;
                      for (String offerID : presentationLog.getOfferIDs())
                        {
                          PropensitySegmentOutput propensitySegmentOutput = new PropensitySegmentOutput(offerID, dimensionID, subscriberGroups.get(dimensionID), false);
                          result.add(propensitySegmentOutput);
                        }
                    }
                }
            }
          }
      }
    return result;
  }
  
  /****************************************
  *
  *  rekeypropensityStream
  *
  ****************************************/
  
  private static KeyValue<PropensityKey, PropensitySegmentOutput> rekeypropensityStream(StringKey key, PropensitySegmentOutput propensitySegmentOutput)
  {
    return new KeyValue<PropensityKey, PropensitySegmentOutput>(new PropensityKey(propensitySegmentOutput.getOfferID(), propensitySegmentOutput.getDimensionID(), propensitySegmentOutput.getSegmentID()), propensitySegmentOutput);
  }
  
  /****************************************
  *
  *  getPropensityStateOutputs
  *
  ****************************************/

  private static List<PropensitySegmentOutput> getPropensityStateOutputs(List<PropensitySegmentOutput> propensitySegmentOutputs)
  {
    return propensitySegmentOutputs;
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
    private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;

    //
    //  constructor
    //

    private ShutdownHook(KafkaStreams kafkaStreams, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
    {
      this.kafkaStreams = kafkaStreams;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
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
      //  reference data reader
      //

      if (subscriberGroupEpochReader != null) subscriberGroupEpochReader.close();
      
      //
      //  stop stats collection
      //


      //
      //  stop streams
      //
      
      boolean streamsCloseSuccessful = kafkaStreams.close(60, TimeUnit.SECONDS);
      log.info("Stopped RoamingFilter" + (streamsCloseSuccessful ? "" : " (timed out)"));
    }
  }
}
