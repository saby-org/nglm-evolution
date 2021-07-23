/****************************************************************************
*
*  SubscriberManager.java 
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction;
import com.evolving.nglm.core.UpdateAlternateID.AssignmentType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SubscriberManager
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SubscriberManager.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  uniqueKeyServer
  //

  private static KStreamsUniqueKeyServer uniqueKeyServer = new KStreamsUniqueKeyServer();

  //
  //  statistics
  // 

  private static SubscriberManagerStatistics subscriberManagerStatistics;
  
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

    NGLMRuntime.initialize(true);
    
    /*****************************************
    *
    *  configuration
    *
    *****************************************/

    //
    //  kafka configuration
    //

    String applicationID = "streams-subscribermanager";
    String stateDirectory = args[0];
    String bootstrapServers = args[1];
    String subscriberManagerKey = args[2];
    Integer kafkaReplicationFactor = Integer.parseInt(args[3]);
    Integer kafkaStreamsStandbyReplicas = Integer.parseInt(args[4]);
    Integer numberOfStreamThreads = Integer.parseInt(args[5]);

    //
    //  source topics 
    //

    String assignExternalSubscriberIDsTopic =  Deployment.getAssignExternalSubscriberIDsTopic();
    String updateExternalSubscriberIDTopic =  Deployment.getUpdateExternalSubscriberIDTopic();
    String assignSubscriberIDsTopic = Deployment.getAssignSubscriberIDsTopic();

    //
    //  sink topics
    //

    String recordSubscriberIDTopic = Deployment.getRecordSubscriberIDTopic();
    String recordAlternateIDTopic = Deployment.getRecordAlternateIDTopic();

    //
    //  changelogs
    //

    String autoProvisionedSubscriberChangeLog = Deployment.getAutoProvisionedSubscriberChangeLog();
    String autoProvisionedSubscriberChangeLogTopic = Deployment.getAutoProvisionedSubscriberChangeLogTopic();

    //
    //  rekey topics
    //

    String rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic = Deployment.getRekeyedAutoProvisionedAssignSubscriberIDsStreamTopic();

    /*****************************************
    *
    *  log
    *
    *****************************************/

    //
    //  log
    //

    log.info("main START: {} {} {} {} {}", stateDirectory, bootstrapServers, kafkaStreamsStandbyReplicas, numberOfStreamThreads, kafkaReplicationFactor);

    //
    //  create monitoring object
    //

    subscriberManagerStatistics = new SubscriberManagerStatistics(applicationID);

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
    final ConnectSerde<AssignSubscriberIDs> assignSubscriberIDsSerde = AssignSubscriberIDs.serde();
    final ConnectSerde<AutoProvisionedSubscriber> autoProvisionedSubscriberSerde = AutoProvisionedSubscriber.serde();
    final ConnectSerde<UpdateSubscriberID> updateSubscriberIDSerde = UpdateSubscriberID.serde();
    final ConnectSerde<UpdateAlternateID> updateAlternateIDSerde = UpdateAlternateID.serde();
    final ConnectSerde<RecordSubscriberID> recordSubscriberIDSerde = RecordSubscriberID.serde();
    final ConnectSerde<RecordAlternateID> recordAlternateIDSerde = RecordAlternateID.serde();
    final ConnectSerde<ProvisionedSubscriberID> provisionedSubscriberIDSerde = ProvisionedSubscriberID.serde();
    final ConnectSerde<ProvisionedAlternateID> provisionedAlternateIDSerde = ProvisionedAlternateID.serde();
    final ConnectSerde<CleanupSubscriber> cleanupSubscriberSerde = CleanupSubscriber.serde();
        
    /*****************************************
    *
    *  deployment objects
    *  - serdes
    *  - source nodes (topics)
    *  - trigger event streams
    *
    *****************************************/

    Map<String,ConnectSerde<? extends com.evolving.nglm.core.SubscriberStreamEvent>> deploymentAutoProvisionEventSerdes = new LinkedHashMap<String,ConnectSerde<? extends com.evolving.nglm.core.SubscriberStreamEvent>>();
    List<String> deploymentSourceNodes = new ArrayList<String>();
    ArrayList<KStream<StringKey, ? extends com.evolving.nglm.core.SubscriberStreamEvent>> deploymentAutoProvisionEventStreams = new ArrayList<KStream<StringKey, ? extends com.evolving.nglm.core.SubscriberStreamEvent>>();
    for (AutoProvisionEvent autoProvisionEvent : Deployment.getAutoProvisionEvents().values())
      {
        try
          {
            //
            //  serde
            //
            
            String eventClassName = autoProvisionEvent.getEventClass();
            Class<? extends AutoProvisionSubscriberStreamEvent> eventClass = (Class<? extends AutoProvisionSubscriberStreamEvent>) Class.forName(eventClassName);
            Method serdeMethod = eventClass.getMethod("serde");
            ConnectSerde<? extends com.evolving.nglm.core.SubscriberStreamEvent> serde = (ConnectSerde<? extends com.evolving.nglm.core.SubscriberStreamEvent>) serdeMethod.invoke(null);
            deploymentAutoProvisionEventSerdes.put(eventClassName, serde);
            
            //
            //  source nodes
            //

            deploymentSourceNodes.add(autoProvisionEvent.getAutoProvisionTopic());

            //
            //  auto-provision event streams
            //

            deploymentAutoProvisionEventStreams.add(builder.stream(autoProvisionEvent.getAutoProvisionTopic(), Consumed.with(stringKeySerde, serde)));
          }            
        catch (ClassNotFoundException e)
          {
            throw new RuntimeException("deployment", e);
          }
        catch (NoSuchMethodException e)
          {
            throw new RuntimeException(e);
          }
        catch (IllegalAccessException e)
          {
            throw new RuntimeException(e);
          }
        catch (InvocationTargetException e)
          {
            throw new RuntimeException(e);
          }
      }

    /*****************************************
    *
    *  auto provision serde
    *
    *****************************************/

    ArrayList<Serde<? extends com.evolving.nglm.core.SubscriberStreamEvent>> autoProvisionSerdes = new ArrayList<Serde<? extends com.evolving.nglm.core.SubscriberStreamEvent>>();
    autoProvisionSerdes.addAll(deploymentAutoProvisionEventSerdes.values());
    autoProvisionSerdes.add(assignSubscriberIDsSerde);
    autoProvisionSerdes.add(updateSubscriberIDSerde);
    final ConnectSerde<com.evolving.nglm.core.SubscriberStreamEvent> autoProvisionSerde = new ConnectSerde<com.evolving.nglm.core.SubscriberStreamEvent>("autoprovisionevent", false, autoProvisionSerdes.toArray(new ConnectSerde[0]));

    /****************************************************************************
    *
    *  autoprovision topology
    *
    ****************************************************************************/
    
    /*****************************************
    *
    *  source streams
    *
    *****************************************/

    //
    //  source streams
    //
    
    KStream<StringKey, ? extends com.evolving.nglm.core.SubscriberStreamEvent> assignExternalSubscriberIDsSourceStream = builder.stream(assignExternalSubscriberIDsTopic, Consumed.with(stringKeySerde, assignSubscriberIDsSerde));
    KStream<StringKey, ? extends com.evolving.nglm.core.SubscriberStreamEvent> updateExternalSubscriberIDSourceStream = builder.stream(updateExternalSubscriberIDTopic, Consumed.with(stringKeySerde, updateAlternateIDSerde));
    
    //
    //  merge source streams
    //

    ArrayList<KStream<StringKey, ? extends com.evolving.nglm.core.SubscriberStreamEvent>> assignExternalSubscriberIDEventStreams = new ArrayList<KStream<StringKey, ? extends com.evolving.nglm.core.SubscriberStreamEvent>>();
    assignExternalSubscriberIDEventStreams.addAll(deploymentAutoProvisionEventStreams);
    assignExternalSubscriberIDEventStreams.add((KStream<StringKey, ? extends com.evolving.nglm.core.SubscriberStreamEvent>) assignExternalSubscriberIDsSourceStream);
    assignExternalSubscriberIDEventStreams.add((KStream<StringKey, ? extends com.evolving.nglm.core.SubscriberStreamEvent>) updateExternalSubscriberIDSourceStream);
    KStream tmpStream = null;
    for (KStream<StringKey, ? extends com.evolving.nglm.core.SubscriberStreamEvent> eventStream : assignExternalSubscriberIDEventStreams)
      {
        tmpStream = (tmpStream == null) ? eventStream : tmpStream.merge(eventStream);
      }
    KStream<StringKey, com.evolving.nglm.core.SubscriberStreamEvent> assignExternalSubscriberIDEventStream = tmpStream;

    /*****************************************
    *
    *  autoProvision -- update
    *
    *****************************************/

    KeyValueBytesStoreSupplier autoProvisionSupplier = Stores.persistentKeyValueStore(autoProvisionedSubscriberChangeLog);
    Materialized autoProvisionedSubscriberStore = Materialized.<StringKey, AutoProvisionedSubscriber>as(autoProvisionSupplier).withKeySerde(stringKeySerde).withValueSerde(autoProvisionedSubscriberSerde.optionalSerde());
    KTable<StringKey, AutoProvisionedSubscriber> autoProvisionedSubscriber = assignExternalSubscriberIDEventStream.groupByKey(Serialized.with(stringKeySerde, autoProvisionSerde)).aggregate(SubscriberManager::nullAutoProvisionedSubscriber, SubscriberManager::updateAutoProvisionedSubscriber, autoProvisionedSubscriberStore);

    /*****************************************
    *
    *  forward autoprovision events to their normal topic(s)
    *
    *****************************************/

    int counter = 0;
    for (AutoProvisionEvent autoProvisionEvent : Deployment.getAutoProvisionEvents().values())
      {
        KStream<StringKey, ? extends com.evolving.nglm.core.SubscriberStreamEvent> deploymentAutoProvisionEventStream = deploymentAutoProvisionEventStreams.get(counter);
        KStream<StringKey, ? extends com.evolving.nglm.core.SubscriberStreamEvent> autoProvisionEventStream = deploymentAutoProvisionEventStream.leftJoin(autoProvisionedSubscriber, SubscriberManager::getReboundAutoProvisionEvent);
        KStream<StringKey, com.evolving.nglm.core.SubscriberStreamEvent> rekeyedAutoProvisionEventStream = (KStream<StringKey, com.evolving.nglm.core.SubscriberStreamEvent>) autoProvisionEventStream.flatMap(SubscriberManager::getRekeyedAutoProvisionEventStream);
        rekeyedAutoProvisionEventStream.to(autoProvisionEvent.getEventTopic(), Produced.with(stringKeySerde, (Serde<com.evolving.nglm.core.SubscriberStreamEvent>) deploymentAutoProvisionEventSerdes.get(autoProvisionEvent.getEventClass())));
        counter += 1;
      }            

    /*****************************************
    *
    *  autoProvisionedAssignSubscriberIDsStream
    *
    *****************************************/

    KStream<StringKey, AutoProvisionedSubscriber> autoProvisionedSubscriberStream = assignExternalSubscriberIDEventStream.leftJoin(autoProvisionedSubscriber, SubscriberManager::getAutoProvisionedSubscriber);
    KStream<StringKey, AssignSubscriberIDs> autoProvisionedAssignSubscriberIDsStream = autoProvisionedSubscriberStream.flatMapValues(SubscriberManager::getAssignSubscriberIDsFromAutoProvisionedSubscriber);
    KStream<StringKey, AssignSubscriberIDs> rekeyedAutoProvisionedAssignSubscriberIDsStream = autoProvisionedAssignSubscriberIDsStream.flatMap(SubscriberManager::getRekeyedAssignSubscriberIDsStream).through(rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic, Produced.with(stringKeySerde, assignSubscriberIDsSerde));

    /****************************************************************************
    *
    *  provision topology
    *
    ****************************************************************************/

    /*****************************************
    *
    *  source streams
    *
    *****************************************/

    KStream<StringKey, AssignSubscriberIDs> assignSubscriberIDsSourceStream = builder.stream(assignSubscriberIDsTopic, Consumed.with(stringKeySerde, assignSubscriberIDsSerde));

    //
    //  merge source streams
    //

    KStream<StringKey, AssignSubscriberIDs> assignSubscriberIDsStream = assignSubscriberIDsSourceStream.merge(rekeyedAutoProvisionedAssignSubscriberIDsStream);

    /*****************************************
    *
    *  branch into updateAlternateID streams
    *
    *****************************************/

    //
    //  break assignSubscriberIDs into updateSubscriberIDs (keyed by subscriberID)
    //

    KStream<StringKey, UpdateSubscriberID> mainUpdateSubscriberIDStream = assignSubscriberIDsStream.flatMapValues(SubscriberManager::getUpdateSubscriberIDsFromAssignSubscriberIDs);

    //
    //  branch mainUpdateSubscriberIDStream into individal streams (keyed by subscriberID)
    //
    
    List<Predicate<StringKey,UpdateSubscriberID>> subscriberIDStreamPredicates = new ArrayList<Predicate<StringKey,UpdateSubscriberID>>();
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
      {
        subscriberIDStreamPredicates.add((key,value) -> (Objects.equals(value.getIDField(), alternateID.getID())));
      }
    KStream<StringKey, UpdateSubscriberID>[] branchedUpdateSubscriberIDStreams = mainUpdateSubscriberIDStream.branch(subscriberIDStreamPredicates.toArray(new Predicate[0]));

    /*****************************************
    *
    *  branch topology
    *
    *****************************************/

    counter = 0;
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
      {
        /*****************************************
        *
        *  subscriberID -> alternateID
        *
        *****************************************/

        //
        //  merge the stream from above with the backchannel topic
        //

        KStream<StringKey, UpdateSubscriberID> subscriberIDStream = branchedUpdateSubscriberIDStreams[counter];
        KStream<StringKey, UpdateSubscriberID> backChannelStreamKeyedBySubscriberID = builder.stream(alternateID.getBackChannelKeyedBySubscriberIDTopic(), Consumed.with(stringKeySerde, updateSubscriberIDSerde));
        KStream<StringKey, UpdateSubscriberID> updateSubscriberIDStream = subscriberIDStream.merge(backChannelStreamKeyedBySubscriberID);

        //
        //  KTable for subscriberID -> alternateID
        //
        
        KeyValueBytesStoreSupplier provisionedSubscriberIDStoreSupplier = Stores.persistentKeyValueStore(alternateID.getChangeLogKeyedBySubscriberID());
        Materialized provisionedSubscriberIDStore = Materialized.<StringKey, ProvisionedSubscriberID>as(provisionedSubscriberIDStoreSupplier).withKeySerde(stringKeySerde).withValueSerde(provisionedSubscriberIDSerde.optionalSerde());
        KTable<StringKey, ProvisionedSubscriberID> provisionedSubscriberID = updateSubscriberIDStream.groupByKey(Serialized.with(stringKeySerde, updateSubscriberIDSerde)).aggregate(SubscriberManager::nullProvisionedSubscriberID, SubscriberManager::updateProvisionedSubscriberID, provisionedSubscriberIDStore);
        KStream<StringKey, ProvisionedSubscriberID> provisionedSubscriberIDStream = updateSubscriberIDStream.leftJoin(provisionedSubscriberID, SubscriberManager::getProvisionedSubscriberID);

        //
        //  extract updateAlternateID to backchannel 
        //

        KStream<StringKey, UpdateAlternateID> backChannelKeyedBySubscriberID = provisionedSubscriberIDStream.flatMapValues(SubscriberManager::getBackChannelFromProvisionedSubscriberID);
        KStream<StringKey, UpdateAlternateID> rekeyedBackChannelKeyedBySubscriberID = backChannelKeyedBySubscriberID.flatMap(SubscriberManager::getRekeyedUpdateAlternateIDStream);
        rekeyedBackChannelKeyedBySubscriberID.to(alternateID.getBackChannelKeyedByAlternateIDTopic(), Produced.with(stringKeySerde, updateAlternateIDSerde));

        //
        //  extract updateSubscriberID for cleanupTable to backchannel
        //

        KStream<StringKey, UpdateSubscriberID> cleanupTableKeyedBySubscriberID = provisionedSubscriberIDStream.flatMapValues(SubscriberManager::getCleanupTableFromProvisionedSubscriberID);
        cleanupTableKeyedBySubscriberID.to(alternateID.getBackChannelKeyedBySubscriberIDTopic(), Produced.with(stringKeySerde, updateSubscriberIDSerde));

        /*****************************************
        *
        *  alternateID -> subscriberID
        *
        *****************************************/

        //
        //  merge the stream from above (rekeyed) with the backchannel topic
        //

        KStream<StringKey, UpdateAlternateID> alternateIDStream = subscriberIDStream.flatMap(SubscriberManager::getUpdateAlternateIDStream).through(alternateID.getRekeyedStreamTopic(), Produced.with(stringKeySerde, updateAlternateIDSerde)); 
        KStream<StringKey, UpdateAlternateID> backChannelStreamKeyedByAlternateID = builder.stream(alternateID.getBackChannelKeyedByAlternateIDTopic(), Consumed.with(stringKeySerde, updateAlternateIDSerde));
        KStream<StringKey, UpdateAlternateID> updateAlternateIDStream = alternateIDStream.merge(backChannelStreamKeyedByAlternateID);
        
        //
        //  KTable for alternateID -> subscriberID
        //
        
        KeyValueBytesStoreSupplier provisionedAlternateIDStoreSupplier = Stores.persistentKeyValueStore(alternateID.getChangeLogKeyedByAlternateID());
        Materialized provisionedAlternateIDStore = Materialized.<StringKey, ProvisionedAlternateID>as(provisionedAlternateIDStoreSupplier).withKeySerde(stringKeySerde).withValueSerde(provisionedAlternateIDSerde.optionalSerde());
        KTable<StringKey, ProvisionedAlternateID> provisionedAlternateID = updateAlternateIDStream.groupByKey(Serialized.with(stringKeySerde, updateAlternateIDSerde)).aggregate(SubscriberManager::nullProvisionedAlternateID, SubscriberManager::updateProvisionedAlternateID, provisionedAlternateIDStore);
        KStream<StringKey, ProvisionedAlternateID> provisionedAlternateIDStream = updateAlternateIDStream.leftJoin(provisionedAlternateID, SubscriberManager::getProvisionedAlternateID);

        //
        //  extract updateSubscriberID to backchannel 
        //

        KStream<StringKey, UpdateSubscriberID> backChannelKeyedByAlternateID = provisionedAlternateIDStream.flatMapValues(SubscriberManager::getBackChannelFromProvisionedAlternateID);
        KStream<StringKey, UpdateSubscriberID> rekeyedBackChannelKeyedByAlternateID = backChannelKeyedByAlternateID.flatMap(SubscriberManager::getRekeyedUpdateSubscriberIDStream);
        rekeyedBackChannelKeyedByAlternateID.to(alternateID.getBackChannelKeyedBySubscriberIDTopic(), Produced.with(stringKeySerde, updateSubscriberIDSerde));

        //
        //  extract updateAlternateID for cleanupTable to backchannel 
        //

        KStream<StringKey, UpdateAlternateID> cleanupTableKeyedByAlternateID = provisionedAlternateIDStream.flatMapValues(SubscriberManager::getCleanupTableFromProvisionedAlternateID);
        cleanupTableKeyedByAlternateID.to(alternateID.getBackChannelKeyedByAlternateIDTopic(), Produced.with(stringKeySerde, updateAlternateIDSerde));

        //
        //  extract recordSubscriberID to forward outside
        //

        KStream<StringKey, RecordSubscriberID> recordSubscriberIDStream = provisionedAlternateIDStream.flatMap(SubscriberManager::getRecordSubscriberIDsFromProvisionedAlternateID);
        recordSubscriberIDStream.to(recordSubscriberIDTopic, Produced.with(stringKeySerde, recordSubscriberIDSerde));

        //
        //  extract recordAlternateID to forward outside
        //

        KStream<StringKey, RecordAlternateID> recordAlternateIDStream = provisionedAlternateIDStream.flatMapValues(SubscriberManager::getRecordAlternateIDFromProvisionedAlternateID);
        recordAlternateIDStream.to(recordAlternateIDTopic, Produced.with(stringKeySerde, recordAlternateIDSerde));
        
        //
        //  extract updateAlternateID to send back to autoprovision (if this is externalSubscriberID field)
        //

        if (Objects.equals(alternateID.getID(), Deployment.getExternalSubscriberID()))
          {
            updateAlternateIDStream.to(updateExternalSubscriberIDTopic, Produced.with(stringKeySerde, updateAlternateIDSerde));
          }

        //
        //  counter
        //
        
        counter += 1;
      }
    
    /****************************************************************************
    *
    *  cleanup subscriber
    *
    ****************************************************************************/

    KStream<StringKey, CleanupSubscriber> cleanupSubscriberStream = assignSubscriberIDsStream.flatMapValues(SubscriberManager::getCleanupSubscriber);
    cleanupSubscriberStream.to(Deployment.getCleanupSubscriberTopic(), Produced.with(stringKeySerde, cleanupSubscriberSerde));
    
    /****************************************************************************
    *
    *  runtime
    *
    ****************************************************************************/
    
    //
    //  streams
    //

    KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig, new com.evolving.nglm.core.NGLMKafkaClientSupplier());

    /*****************************************
    *
    *  state change listener
    *
    *****************************************/

    KafkaStreams.StateListener stateListener = new KafkaStreams.StateListener()
    {
      @Override public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState)
      {

        // need to re-run unique key prefix computation
        if(newState==KafkaStreams.State.RUNNING) KStreamsUniqueKeyServer.streamRebalanced();

      }
    };
    streams.setStateListener(stateListener);

    //
    //  shutdown hook
    //

    NGLMRuntime.addShutdownHook(new ShutdownHook(streams));

    //
    //  start
    //

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
      //
      //  stop stats collection
      //

      if (subscriberManagerStatistics != null) subscriberManagerStatistics.unregister();

      //
      //  stop streams
      //
      
      boolean streamsCloseSuccessful = kafkaStreams.close(60, TimeUnit.SECONDS);
      log.info("Stopped SubscriberManager" + (streamsCloseSuccessful ? "" : " (timed out)"));
    }
  }

  /*****************************************
  *
  *  castToSubscriberStreamEvent
  *
  *****************************************/

  public static com.evolving.nglm.core.SubscriberStreamEvent castToSubscriberStreamEvent(com.evolving.nglm.core.SubscriberStreamEvent subscriberStreamEvent) { return subscriberStreamEvent; }
  
  /*****************************************
  *
  *  nullAutoProvisionedSubscriber
  *
  ****************************************/

  public static AutoProvisionedSubscriber nullAutoProvisionedSubscriber() { return (AutoProvisionedSubscriber) null; }

  /*****************************************
  *
  *  updateAutoProvisionedSubscriber
  *
  *****************************************/

  public static AutoProvisionedSubscriber updateAutoProvisionedSubscriber(StringKey aggKey, com.evolving.nglm.core.SubscriberStreamEvent autoProvisionEvent, AutoProvisionedSubscriber currentAutoProvisionedSubscriber)
  {
    /*****************************************
    *
    *  cleanup table (if necessary)
    *
    *****************************************/

    if (autoProvisionEvent instanceof UpdateAlternateID && ((UpdateAlternateID) autoProvisionEvent).getCleanupTableEntry())
      {
        return null;
      }
    
    /*****************************************
    *
    *  extract tenantID : Here comes the following events: AssignSubscriberID or UpdateAlternateID or any of event declared as AutoProvisionSubscriberStreamEvent
    *
    *****************************************/
    
    Integer tenantID = null;
    if(currentAutoProvisionedSubscriber != null) 
      {
        tenantID = currentAutoProvisionedSubscriber.getTenantID();
      }
    else if(autoProvisionEvent instanceof AssignSubscriberIDs)
      {
        tenantID = ((AssignSubscriberIDs)autoProvisionEvent).getTenantID();   
      }
    else if(autoProvisionEvent instanceof UpdateAlternateID)
      {
        tenantID = ((UpdateAlternateID)autoProvisionEvent).getTenantID();   
      }
    else if(autoProvisionEvent instanceof AutoProvisionSubscriberStreamEvent)
      {
        tenantID = ((AutoProvisionSubscriberStreamEvent)autoProvisionEvent).getTenantID();
      }
    else 
      {
        log.warn("Can't retrieve tenantID: autoProvisionEvent " + autoProvisionEvent  + " currentAutoProvisionedSubscriber " + currentAutoProvisionedSubscriber);
      }    

    /****************************************
    *
    *  get (or create) entry
    *
    ****************************************/

    AutoProvisionedSubscriber autoProvisionedSubscriber = (currentAutoProvisionedSubscriber != null) ? new AutoProvisionedSubscriber(currentAutoProvisionedSubscriber) : null;
    boolean autoProvisionedSubscriberUpdated = (currentAutoProvisionedSubscriber != null) ? false : true;

    /*****************************************
    *
    *  clear state from previous events
    *
    *****************************************/

    //
    //  assignSubscriberIDs
    //

    if (autoProvisionedSubscriber != null && autoProvisionedSubscriber.getAssignSubscriberIDs() != null)
      {
        autoProvisionedSubscriber.setAssignSubscriberIDs(null);
        autoProvisionedSubscriberUpdated = true;
      }

    /*****************************************
    *
    *  subscriberID is null?  (i.e., this externalSubscriberID has been previously cleared?)
    *
    *****************************************/

    if (autoProvisionedSubscriber != null && autoProvisionedSubscriber.getSubscriberID() == null)
      {
        autoProvisionedSubscriber = null;
      }
    
    /*****************************************
    *
    *  externalSubscriberID
    *
    *****************************************/

    String externalSubscriberID = (autoProvisionEvent instanceof UpdateAlternateID) ? ((UpdateAlternateID) autoProvisionEvent).getAlternateID() : autoProvisionEvent.getSubscriberID();
    
    /*****************************************
    *
    *  updateAlternateID
    *
    *****************************************/

    if (autoProvisionEvent instanceof UpdateAlternateID)
      {
        UpdateAlternateID updateAlternateID = (UpdateAlternateID) autoProvisionEvent;
        tenantID = updateAlternateID.getTenantID();
        switch (updateAlternateID.getAssignmentType())
          {
            case AssignSubscriberID:
              autoProvisionedSubscriber = new AutoProvisionedSubscriber(updateAlternateID.getSubscriberID(), externalSubscriberID, tenantID);
              autoProvisionedSubscriberUpdated = true;
              break;
              
            case ReassignSubscriberID:
            case UnassignSubscriberID:
              autoProvisionedSubscriber = new AutoProvisionedSubscriber(null, externalSubscriberID, tenantID);
              autoProvisionedSubscriberUpdated = true;
              break;
          }
      }

    /*****************************************
    *
    *  autoProvision (new subscriberID)
    *
    *****************************************/

    boolean autoProvisioned = false;
    if (autoProvisionedSubscriber == null)
      {
        autoProvisionedSubscriber = new AutoProvisionedSubscriber(generateSubscriberID(), externalSubscriberID, tenantID);
        autoProvisionedSubscriberUpdated = true;
        autoProvisioned = true;
      }

    /*****************************************
    *
    *  assignSubscriberIDs
    *
    *****************************************/

    //
    //  assignSubscriberIDs event
    //

    AssignSubscriberIDs assignSubscriberIDs = null;
    if (autoProvisionEvent instanceof AssignSubscriberIDs)
      {
        assignSubscriberIDs = new AssignSubscriberIDs((AssignSubscriberIDs) autoProvisionEvent);
        assignSubscriberIDs.setSubscriberID(autoProvisionedSubscriber.getSubscriberID());
      }

    //
    //  add/modify assignSubscriberIDs for autoProvision cases
    //

    if (autoProvisioned || !(autoProvisionEvent instanceof UpdateAlternateID)/*Xavier: this is an hack to force subscriber creation even if already there (mainly to keep redis in sync...)*/)
      {
        if (autoProvisionEvent instanceof AssignSubscriberIDs)
          {
            if (assignSubscriberIDs.getAlternateIDs().get(Deployment.getExternalSubscriberID()) == null) assignSubscriberIDs.getAlternateIDs().put(Deployment.getExternalSubscriberID(), externalSubscriberID);
          }
        else
          {
            Map<String,String> alternateIDs = new HashMap<String,String>();
            alternateIDs.put(Deployment.getExternalSubscriberID(), externalSubscriberID);
            assignSubscriberIDs = new AssignSubscriberIDs(autoProvisionedSubscriber.getSubscriberID(), autoProvisionEvent.getEventDate(), alternateIDs, tenantID);
          }
      }

    //
    //  emit assignSubscriberIDs (if necessary)
    //

    if (assignSubscriberIDs != null)
      {
        autoProvisionedSubscriber.setAssignSubscriberIDs(assignSubscriberIDs);
        autoProvisionedSubscriberUpdated = true;
      }

    /****************************************
    *
    *  statistics
    *
    ****************************************/

    updateSubscriberManagerStatistics(autoProvisionEvent);
    
    /****************************************
    *
    *  return
    *
    ****************************************/

    return autoProvisionedSubscriberUpdated ? autoProvisionedSubscriber : currentAutoProvisionedSubscriber;
  }

  /*****************************************
  *
  *  getReboundAutoProvisionEvent
  *
  *****************************************/

  private static com.evolving.nglm.core.SubscriberStreamEvent getReboundAutoProvisionEvent(com.evolving.nglm.core.SubscriberStreamEvent subscriberStreamEvent, AutoProvisionedSubscriber autoProvisionedSubscriber)
  {
    AutoProvisionSubscriberStreamEvent autoProvisionSubscriberStreamEvent = (AutoProvisionSubscriberStreamEvent) subscriberStreamEvent;
    autoProvisionSubscriberStreamEvent.rebindSubscriberID(autoProvisionedSubscriber.getSubscriberID());
    return autoProvisionSubscriberStreamEvent;
  }

  /*****************************************
  *
  *  getRekeyedAutoProvisionEventStream
  *
  *****************************************/
  
  private static Iterable<KeyValue<StringKey, com.evolving.nglm.core.SubscriberStreamEvent>> getRekeyedAutoProvisionEventStream(StringKey key, com.evolving.nglm.core.SubscriberStreamEvent subscriberStreamEvent)
  {
    KeyValue<StringKey, com.evolving.nglm.core.SubscriberStreamEvent> result = new KeyValue<StringKey, com.evolving.nglm.core.SubscriberStreamEvent>(new StringKey(subscriberStreamEvent.getSubscriberID()), subscriberStreamEvent);
    List<KeyValue<StringKey, com.evolving.nglm.core.SubscriberStreamEvent>> iterableResult = Collections.<KeyValue<StringKey, com.evolving.nglm.core.SubscriberStreamEvent>>singletonList(result);
    return iterableResult;
  }

  /*****************************************
  *
  *  getAutoProvisionedSubscriber
  *
  *****************************************/

  private static AutoProvisionedSubscriber getAutoProvisionedSubscriber(com.evolving.nglm.core.SubscriberStreamEvent profileEvent, AutoProvisionedSubscriber autoProvisionedSubscriber)
  {
    return autoProvisionedSubscriber;
  }

  /****************************************
  *
  *  getAssignSubscriberIDsFromAutoProvisionedSubscriber
  *
  ****************************************/

  private static List<AssignSubscriberIDs> getAssignSubscriberIDsFromAutoProvisionedSubscriber(AutoProvisionedSubscriber autoProvisionedSubscriber)
  {
    List<AssignSubscriberIDs> result = new ArrayList<AssignSubscriberIDs>();
    result.addAll((autoProvisionedSubscriber != null && autoProvisionedSubscriber.getAssignSubscriberIDs() != null) ? Collections.<AssignSubscriberIDs>singletonList(autoProvisionedSubscriber.getAssignSubscriberIDs()) : Collections.<AssignSubscriberIDs>emptyList());
    return result;
  }

  /*****************************************
  *
  *  getRekeyedAssignSubscriberIDsStream
  *
  *****************************************/
  
  private static Iterable<KeyValue<StringKey, AssignSubscriberIDs>> getRekeyedAssignSubscriberIDsStream(StringKey key, AssignSubscriberIDs assignSubscriberIDs)
  {
    KeyValue<StringKey,AssignSubscriberIDs> result = new KeyValue<StringKey,AssignSubscriberIDs>(new StringKey(assignSubscriberIDs.getSubscriberID()), assignSubscriberIDs);
    List<KeyValue<StringKey,AssignSubscriberIDs>> iterableResult = Collections.<KeyValue<StringKey,AssignSubscriberIDs>>singletonList(result);
    return iterableResult;
  }
  
  /****************************************
  *
  *  getUpdateSubscriberIDsFromAssignSubscriberIDs
  *
  ****************************************/

  private static List<UpdateSubscriberID> getUpdateSubscriberIDsFromAssignSubscriberIDs(AssignSubscriberIDs assignSubscriberIDs)
  {
    List<UpdateSubscriberID> result = new ArrayList<UpdateSubscriberID>();
    for (String idField : assignSubscriberIDs.getAlternateIDs().keySet())
      {
        result.add(new UpdateSubscriberID(assignSubscriberIDs.getSubscriberID(), idField, assignSubscriberIDs.getAlternateIDs().get(idField), assignSubscriberIDs.getEventDate(), assignSubscriberIDs.getSubscriberAction(), false, false, assignSubscriberIDs.getTenantID()));
      }
    return result;
  }    

  /****************************************
  *
  *  nullProvisionedSubscriberID
  *
  ****************************************/

  public static ProvisionedSubscriberID nullProvisionedSubscriberID() { return (ProvisionedSubscriberID) null; }

  /****************************************
  *
  *  updateProvisionedSubscriberID
  *
  ****************************************/

  public static ProvisionedSubscriberID updateProvisionedSubscriberID(StringKey aggKey, UpdateSubscriberID updateSubscriberID, ProvisionedSubscriberID currentProvisionedSubscriberID)
  {
    /*****************************************
    *
    *  cleanup table (if necessary)
    *
    *****************************************/

    if (updateSubscriberID.getCleanupTableEntry())
      {
        return null;
      }
    
    /****************************************
    *
    *  retrieve the tenantID 
    *
    ****************************************/
    
    int tenantID = updateSubscriberID.getTenantID();

    /****************************************
    *
    *  get (or create) entry
    *
    ****************************************/

    ProvisionedSubscriberID provisionedSubscriberID = (currentProvisionedSubscriberID != null) ? new ProvisionedSubscriberID(currentProvisionedSubscriberID) : null;
    boolean provisionedSubscriberIDUpdated = (currentProvisionedSubscriberID != null) ? false : true;

    /*****************************************
    *
    *  clear state from previous events
    *
    *****************************************/

    //
    //  backChannel
    //

    if (provisionedSubscriberID != null && provisionedSubscriberID.getBackChannel() != null)
      {
        provisionedSubscriberID.setBackChannel(null);
        provisionedSubscriberIDUpdated = true;
      }

    //
    //  cleanupTableEntry
    //

    if (provisionedSubscriberID != null && provisionedSubscriberID.getCleanupTable() != null)
      {
        provisionedSubscriberID.setCleanupTable(null);
        provisionedSubscriberIDUpdated = true;
      }

    /*****************************************
    *
    *  alternateID is null?  (i.e., this subscriberID has been previously cleared?)
    *
    *****************************************/

    if (provisionedSubscriberID != null && provisionedSubscriberID.getAlternateID() == null)
      {
        provisionedSubscriberID = null;
      }
    
    /*****************************************
    *
    *  provision (new subscriberID)
    *
    *****************************************/

    boolean newlyProvisioned = false;
    if (provisionedSubscriberID == null)
      {
        provisionedSubscriberID = new ProvisionedSubscriberID(updateSubscriberID.getSubscriberID(), updateSubscriberID.getAlternateID());
        provisionedSubscriberIDUpdated = true;
        newlyProvisioned = true;
      }

    /*****************************************
    *
    *  backChannel
    *
    *****************************************/

    if (! updateSubscriberID.getBackChannel())
      {
        if (! newlyProvisioned && ! Objects.equals(provisionedSubscriberID.getAlternateID(), updateSubscriberID.getAlternateID()))
          {
            provisionedSubscriberID.setBackChannel(new UpdateAlternateID(updateSubscriberID.getIDField(), provisionedSubscriberID.getAlternateID(), provisionedSubscriberID.getSubscriberID(), (updateSubscriberID.getAlternateID() != null) ? AssignmentType.ReassignSubscriberID : AssignmentType.UnassignSubscriberID, updateSubscriberID.getEventDate(), updateSubscriberID.getSubscriberAction(), true, false, tenantID));
            provisionedSubscriberIDUpdated = true;
          }
      }
    
    /*****************************************
    *
    *  ktable
    *
    *****************************************/

    provisionedSubscriberID.setAlternateID(updateSubscriberID.getAlternateID());
    provisionedSubscriberIDUpdated = true;

    /****************************************
    *
    *  cleanup (if necessary)
    *
    ****************************************/

    if (provisionedSubscriberID.getAlternateID() == null)
      {
        provisionedSubscriberID.setCleanupTable(new UpdateSubscriberID(updateSubscriberID.getSubscriberID(), updateSubscriberID.getIDField(), null, updateSubscriberID.getEventDate(), com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction.Standard, true, true, tenantID));
        provisionedSubscriberIDUpdated = true;
      }
    
    /****************************************
    *
    *  statistics
    *
    ****************************************/

    updateSubscriberManagerStatistics(updateSubscriberID);
    
    /****************************************
    *
    *  return
    *
    ****************************************/

    return provisionedSubscriberIDUpdated ? provisionedSubscriberID : currentProvisionedSubscriberID;
  }

  /****************************************
  *
  *  getProvisionedSubscriberID
  *
  ****************************************/

  private static ProvisionedSubscriberID getProvisionedSubscriberID(UpdateSubscriberID updateSubscriberID, ProvisionedSubscriberID provisionedSubscriberID)
  {
    return provisionedSubscriberID;
  }
  
  /****************************************
  *
  *  getBackChannelFromProvisionedSubscriberID
  *
  ****************************************/

  private static List<UpdateAlternateID> getBackChannelFromProvisionedSubscriberID(ProvisionedSubscriberID provisionedSubscriberID)
  {
    List<UpdateAlternateID> result = new ArrayList<UpdateAlternateID>();
    result.addAll((provisionedSubscriberID != null && provisionedSubscriberID.getBackChannel() != null) ? Collections.<UpdateAlternateID>singletonList(provisionedSubscriberID.getBackChannel()) : Collections.<UpdateAlternateID>emptyList());
    return result;
  }
  
  /****************************************
  *
  *  getCleanupTableFromProvisionedSubscriberID
  *
  ****************************************/

  private static List<UpdateSubscriberID> getCleanupTableFromProvisionedSubscriberID(ProvisionedSubscriberID provisionedSubscriberID)
  {
    List<UpdateSubscriberID> result = new ArrayList<UpdateSubscriberID>();
    result.addAll((provisionedSubscriberID != null && provisionedSubscriberID.getCleanupTable() != null) ? Collections.<UpdateSubscriberID>singletonList(provisionedSubscriberID.getCleanupTable()) : Collections.<UpdateSubscriberID>emptyList());
    return result;
  }
  
  /****************************************
  *
  *  getRekeyedUpdateAlternateIDStream
  *
  ****************************************/

  private static Iterable<KeyValue<StringKey, UpdateAlternateID>> getRekeyedUpdateAlternateIDStream(StringKey key, UpdateAlternateID updateAlternateID)
  {
    KeyValue<StringKey,UpdateAlternateID> result = new KeyValue<StringKey,UpdateAlternateID>(new StringKey(updateAlternateID.getAlternateID()), updateAlternateID);
    List<KeyValue<StringKey,UpdateAlternateID>> iterableResult = Collections.<KeyValue<StringKey,UpdateAlternateID>>singletonList(result);
    return iterableResult;
  }
  
  /****************************************
  *
  *  getUpdateAlternateIDStream
  *
  ****************************************/

  private static Iterable<KeyValue<StringKey, UpdateAlternateID>> getUpdateAlternateIDStream(StringKey key, UpdateSubscriberID updateSubscriberID)
  {
    UpdateAlternateID updateAlternateID = (updateSubscriberID.getAlternateID() != null) ? new UpdateAlternateID(updateSubscriberID.getIDField(), updateSubscriberID.getAlternateID(), updateSubscriberID.getSubscriberID(), AssignmentType.AssignSubscriberID, updateSubscriberID.getEventDate(), updateSubscriberID.getSubscriberAction(), false, false, updateSubscriberID.getTenantID()) : null;
    KeyValue<StringKey,UpdateAlternateID> result = (updateAlternateID != null) ?  new KeyValue<StringKey,UpdateAlternateID>(new StringKey(updateAlternateID.getAlternateID()), updateAlternateID) : null;
    List<KeyValue<StringKey,UpdateAlternateID>> iterableResult = (result != null) ? Collections.<KeyValue<StringKey,UpdateAlternateID>>singletonList(result) : Collections.<KeyValue<StringKey,UpdateAlternateID>>emptyList();
    return iterableResult;
  }
  
  /****************************************
  *
  *  nullProvisionedAlternateID
  *
  ****************************************/

  public static ProvisionedAlternateID nullProvisionedAlternateID() { return (ProvisionedAlternateID) null; }

  /****************************************
  *
  *  updateProvisionedAlternateID
  *
  ****************************************/

  public static ProvisionedAlternateID updateProvisionedAlternateID(StringKey aggKey, UpdateAlternateID updateAlternateID, ProvisionedAlternateID currentProvisionedAlternateID)
  {
    /*****************************************
    *
    *  cleanup table (if necessary)
    *
    *****************************************/

    if (updateAlternateID.getCleanupTableEntry())
      {
        return null;
      }

    /****************************************
    *
    *  get (or create) entry
    *
    ****************************************/

    ProvisionedAlternateID provisionedAlternateID = (currentProvisionedAlternateID != null) ? new ProvisionedAlternateID(currentProvisionedAlternateID) : null;
    boolean provisionedAlternateIDUpdated = (currentProvisionedAlternateID != null) ? false : true;
    
    
    /****************************************
    *
    *  retrieve the tenantID 
    *
    ****************************************/
    
    int tenantID = updateAlternateID.getTenantID();
    

    /*****************************************
    *
    *  clear state from previous events
    *
    *****************************************/

    //
    //  backChannel
    //

    if (provisionedAlternateID != null && provisionedAlternateID.getBackChannel() != null)
      {
        provisionedAlternateID.setBackChannel(null);
        provisionedAlternateIDUpdated = true;
      }

    //
    //  cleanupTable
    //

    if (provisionedAlternateID != null && provisionedAlternateID.getCleanupTable() != null)
      {
        provisionedAlternateID.setCleanupTable(null);
        provisionedAlternateIDUpdated = true;
      }

    //
    //  provisionedSubscriberID
    //

    if (provisionedAlternateID != null && provisionedAlternateID.getProvisionedSubscriberID() != null)
      {
        provisionedAlternateID.setProvisionedSubscriberID(null);
        provisionedAlternateIDUpdated = true;
      }

    //
    //  deprovisionedSubscriberID
    //

    if (provisionedAlternateID != null && provisionedAlternateID.getDeprovisionedSubscriberID() != null)
      {
        provisionedAlternateID.setDeprovisionedSubscriberID(null);
        provisionedAlternateIDUpdated = true;
      }

    //
    //  provisionedAlternateID
    //

    if (provisionedAlternateID != null && provisionedAlternateID.getProvisionedAlternateID() != null)
      {
        provisionedAlternateID.setProvisionedAlternateID(null);
        provisionedAlternateIDUpdated = true;
      }

    /*****************************************
    *
    *  allSubscriberIDs is empty?  (i.e., this alternateID has no subscriber ids?)
    *
    *****************************************/

    if (provisionedAlternateID != null && provisionedAlternateID.getAllSubscriberIDs().size() == 0)
      {
        provisionedAlternateID = null;
      }
    
    /*****************************************
    *
    *  provision (new alternateID)
    *
    *****************************************/

    boolean newlyProvisioned = false;
    if (provisionedAlternateID == null)
      {
        provisionedAlternateID = new ProvisionedAlternateID(updateAlternateID.getAlternateID(), Collections.<String>singleton(updateAlternateID.getSubscriberID()), tenantID);
        provisionedAlternateIDUpdated = true;
        newlyProvisioned = true;
      }

    /*****************************************
    *
    *  backChannel
    *
    *****************************************/

    if (! Deployment.getAlternateIDs().get(updateAlternateID.getIDField()).getSharedID())
      {
        if (! updateAlternateID.getBackChannel())
          {
            if (! newlyProvisioned && provisionedAlternateID.getAllSubscriberIDs().size() == 1 && ! provisionedAlternateID.getAllSubscriberIDs().contains(updateAlternateID.getSubscriberID()))
              {
                provisionedAlternateID.setBackChannel(new UpdateSubscriberID(provisionedAlternateID.getAllSubscriberIDs().iterator().next(), updateAlternateID.getIDField(), null, updateAlternateID.getEventDate(), com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction.Standard, true, false, tenantID));
                provisionedAlternateIDUpdated = true;
              }
          }
      }

    /*****************************************
    *
    *  ktable
    *
    *****************************************/

    switch (updateAlternateID.getAssignmentType())
      {
        case AssignSubscriberID:
          if (! Deployment.getAlternateIDs().get(updateAlternateID.getIDField()).getSharedID())
            {
              provisionedAlternateID.setAllSubscriberIDs(Collections.<String>singleton(updateAlternateID.getSubscriberID()));
              provisionedAlternateIDUpdated = true;
            }
          else
            {
              Set<String> updatedSubscriberIDs = new HashSet<String>(provisionedAlternateID.getAllSubscriberIDs());
              updatedSubscriberIDs.add(updateAlternateID.getSubscriberID());
              provisionedAlternateID.setAllSubscriberIDs(updatedSubscriberIDs);
              provisionedAlternateIDUpdated = true;
            }
          break;
          
        case ReassignSubscriberID:
        case UnassignSubscriberID:
          if (! Deployment.getAlternateIDs().get(updateAlternateID.getIDField()).getSharedID())
            {
              provisionedAlternateID.setAllSubscriberIDs(Collections.<String>emptySet());
              provisionedAlternateIDUpdated = true;
            }
          else
            {
              Set<String> updatedSubscriberIDs = new HashSet<String>(provisionedAlternateID.getAllSubscriberIDs());
              updatedSubscriberIDs.remove(updateAlternateID.getSubscriberID());
              provisionedAlternateID.setAllSubscriberIDs(updatedSubscriberIDs);
              provisionedAlternateIDUpdated = true;
            }
          break;
      }
    
    /*****************************************
    *
    *  provisionedSubscriberIDs
    *
    *  - one message for subscriberID newly assigned to alternateID
    *  - one message for the single subscriberID no longer assigned to alternateID (if necessary)
    *
    *****************************************/

    switch (updateAlternateID.getAssignmentType())
      {
        case AssignSubscriberID:
            provisionedAlternateID.setProvisionedSubscriberID(new RecordSubscriberID(updateAlternateID.getSubscriberID(), updateAlternateID.getIDField(), updateAlternateID.getAlternateID(), updateAlternateID.getEventDate(), com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction.Standard, tenantID));
            if (! newlyProvisioned && ! Deployment.getAlternateIDs().get(updateAlternateID.getIDField()).getSharedID() && !currentProvisionedAlternateID.getAlternateID().equals(updateAlternateID.getAlternateID()))
              {
                provisionedAlternateID.setDeprovisionedSubscriberID(new RecordSubscriberID(currentProvisionedAlternateID.getAllSubscriberIDs().iterator().next(), updateAlternateID.getIDField(), null, updateAlternateID.getEventDate(), com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction.Standard, tenantID));
              }
            provisionedAlternateIDUpdated = true;
          break;

        case UnassignSubscriberID:
          if (newlyProvisioned || ! Objects.equals(currentProvisionedAlternateID.getAllSubscriberIDs(), provisionedAlternateID.getAllSubscriberIDs()) || updateAlternateID.getSubscriberAction() == com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction.Delete || updateAlternateID.getSubscriberAction() == com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction.DeleteImmediate)
            {
              provisionedAlternateID.setDeprovisionedSubscriberID(new RecordSubscriberID(updateAlternateID.getSubscriberID(), updateAlternateID.getIDField(), null, updateAlternateID.getEventDate(), updateAlternateID.getSubscriberAction(), tenantID));
              provisionedAlternateIDUpdated = true;
            }
          break;
      }

    /*****************************************
    *
    *  provisionedAlternateID
    *
    *****************************************/

    // always produce recordalternateid in case redis data was not OK
    provisionedAlternateID.setProvisionedAlternateID(new RecordAlternateID(updateAlternateID.getIDField(), updateAlternateID.getAlternateID(), provisionedAlternateID.getAllSubscriberIDs(), updateAlternateID.getEventDate(), updateAlternateID.getSubscriberAction(), tenantID));
    provisionedAlternateIDUpdated = true;

    
    /*****************************************
    *
    *  cleanup (if necessary)
    *
    *****************************************/

    if (provisionedAlternateID.getAllSubscriberIDs().size() == 0)
      {
        provisionedAlternateID.setCleanupTable(new UpdateAlternateID(updateAlternateID.getIDField(), provisionedAlternateID.getAlternateID(), updateAlternateID.getSubscriberID(), AssignmentType.UnassignSubscriberID, updateAlternateID.getEventDate(), com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction.Standard, true, true, tenantID));
        provisionedAlternateIDUpdated = true;
      }
    
    /****************************************
    *
    *  return
    *
    ****************************************/

    return provisionedAlternateIDUpdated ? provisionedAlternateID : currentProvisionedAlternateID;
  }

  /****************************************
  *
  *  getProvisionedAlternateID
  *
  ****************************************/

  private static ProvisionedAlternateID getProvisionedAlternateID(UpdateAlternateID updateAlternateID, ProvisionedAlternateID provisionedAlternateID)
  {
    return provisionedAlternateID;
  }

  /****************************************
  *
  *  getBackChannelFromProvisionedAlternateID
  *
  ****************************************/

  private static List<UpdateSubscriberID> getBackChannelFromProvisionedAlternateID(ProvisionedAlternateID provisionedAlternateID)
  {
    List<UpdateSubscriberID> result = new ArrayList<UpdateSubscriberID>();
    result.addAll((provisionedAlternateID != null && provisionedAlternateID.getBackChannel() != null) ? Collections.<UpdateSubscriberID>singletonList(provisionedAlternateID.getBackChannel()) : Collections.<UpdateSubscriberID>emptyList());
    return result;
  }
  
  /****************************************
  *
  *  getCleanupTableFromProvisionedAlternateID
  *
  ****************************************/

  private static List<UpdateAlternateID> getCleanupTableFromProvisionedAlternateID(ProvisionedAlternateID provisionedAlternateID)
  {
    List<UpdateAlternateID> result = new ArrayList<UpdateAlternateID>();
    result.addAll((provisionedAlternateID != null && provisionedAlternateID.getCleanupTable() != null) ? Collections.<UpdateAlternateID>singletonList(provisionedAlternateID.getCleanupTable()) : Collections.<UpdateAlternateID>emptyList());
    return result;
  }
  
  /****************************************
  *
  *  getRekeyedUpdateSubscriberIDStream
  *
  ****************************************/

  private static Iterable<KeyValue<StringKey, UpdateSubscriberID>> getRekeyedUpdateSubscriberIDStream(StringKey key, UpdateSubscriberID updateSubscriberID)
  {
    KeyValue<StringKey,UpdateSubscriberID> result = new KeyValue<StringKey,UpdateSubscriberID>(new StringKey(updateSubscriberID.getSubscriberID()), updateSubscriberID);
    List<KeyValue<StringKey,UpdateSubscriberID>> iterableResult = Collections.<KeyValue<StringKey,UpdateSubscriberID>>singletonList(result);
    return iterableResult;
  }
  
  /****************************************
  *
  *  getRecordSubscriberIDsFromProvisionedAlternateID
  *
  ****************************************/

  private static Iterable<KeyValue<StringKey, RecordSubscriberID>> getRecordSubscriberIDsFromProvisionedAlternateID(StringKey key, ProvisionedAlternateID provisionedAlternateID)
  {
    List<KeyValue<StringKey, RecordSubscriberID>> result = new ArrayList<KeyValue<StringKey, RecordSubscriberID>>();
    if (provisionedAlternateID != null && provisionedAlternateID.getProvisionedSubscriberID() != null) result.add(new KeyValue<StringKey, RecordSubscriberID>(new StringKey(provisionedAlternateID.getProvisionedSubscriberID().getSubscriberID()), provisionedAlternateID.getProvisionedSubscriberID()));
    if (provisionedAlternateID != null && provisionedAlternateID.getDeprovisionedSubscriberID() != null) result.add(new KeyValue<StringKey, RecordSubscriberID>(new StringKey(provisionedAlternateID.getDeprovisionedSubscriberID().getSubscriberID()), provisionedAlternateID.getDeprovisionedSubscriberID()));
    return result;
  }

  /****************************************
  *
  *  getRecordAlternateIDFromProvisionedAlternateID
  *
  ****************************************/

  private static List<RecordAlternateID> getRecordAlternateIDFromProvisionedAlternateID(ProvisionedAlternateID provisionedAlternateID)
  {
    List<RecordAlternateID> result = new ArrayList<RecordAlternateID>();
    result.addAll((provisionedAlternateID != null && provisionedAlternateID.getProvisionedAlternateID() != null) ? Collections.<RecordAlternateID>singletonList(provisionedAlternateID.getProvisionedAlternateID()) : Collections.<RecordAlternateID>emptyList());
    return result;
  }

  /****************************************
  *
  *  getCleanupSubscriber
  *
  ****************************************/

  private static List<CleanupSubscriber> getCleanupSubscriber(AssignSubscriberIDs assignSubscriberIDs)
  {
    List<CleanupSubscriber> result = new ArrayList<CleanupSubscriber>();
    switch (assignSubscriberIDs.getSubscriberAction())
      {
        case Delete:
          result.add(new CleanupSubscriber(assignSubscriberIDs.getSubscriberID(), assignSubscriberIDs.getEventDate(), SubscriberAction.Cleanup));
          break;
        case DeleteImmediate:
          result.add(new CleanupSubscriber(assignSubscriberIDs.getSubscriberID(), assignSubscriberIDs.getEventDate(), SubscriberAction.CleanupImmediate));
          break;

      }
    return result;
  }

  /*****************************************
  *
  *  generateSubscriberID
  *
  *****************************************/

  private static String generateSubscriberID()
  {
    return uniqueKeyServer.getKey();
  }
  
  /*****************************************
  *
  *  updateSubscriberManagerStatistics
  *
  *****************************************/

  private static void updateSubscriberManagerStatistics(com.evolving.nglm.core.SubscriberStreamEvent event)
  {
    if (event instanceof AutoProvisionEvent)
      {
        subscriberManagerStatistics.updateAutoProvisionCount(1);
      }
    else if (event instanceof AssignSubscriberIDs)
      {
        subscriberManagerStatistics.updateAssignSubscriberIDCount(1);
      }
    else if (event instanceof UpdateSubscriberID)
      {
        subscriberManagerStatistics.updateUpdateSubscriberIDCount(1);
      }
  }

  /****************************************
  *
  *  class AutoProvisionedSubscriber
  *
  ****************************************/

  public static class AutoProvisionedSubscriber
  {
    /*****************************************
    *
    *  connect serdes
    *
    *****************************************/

    private static ConnectSerde<AssignSubscriberIDs> assignSubscriberIDsSerde = AssignSubscriberIDs.serde();

    /*****************************************
    *
    *  schema
    *
    *****************************************/

    //
    //  schema
    //

    private static Schema schema = null;
    static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("autoprovisioned_subscriber");
      schemaBuilder.version(com.evolving.nglm.core.SchemaUtilities.packSchemaVersion(2));
      schemaBuilder.field("subscriberID", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("externalSubscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("assignSubscriberIDs", assignSubscriberIDsSerde.optionalSchema());
      schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<AutoProvisionedSubscriber> serde = new ConnectSerde<AutoProvisionedSubscriber>(schema, false, AutoProvisionedSubscriber.class, AutoProvisionedSubscriber::pack, AutoProvisionedSubscriber::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<AutoProvisionedSubscriber> serde() { return serde; }

    /****************************************
    *
    *  data
    *
    ****************************************/

    private String subscriberID;
    private String externalSubscriberID;
    private AssignSubscriberIDs assignSubscriberIDs = null;
    private int tenantID;


    /****************************************
    *
    *  accessors
    *
    ****************************************/

    public String getSubscriberID() { return subscriberID; }
    public String getExternalSubscriberID() { return externalSubscriberID; }
    public AssignSubscriberIDs getAssignSubscriberIDs() { return assignSubscriberIDs; }
    public int getTenantID() { return tenantID; }
    
    //
    //  setter
    //

    public void setAssignSubscriberIDs(AssignSubscriberIDs assignSubscriberIDs)
    {
      this.assignSubscriberIDs = assignSubscriberIDs;
    }

    /*****************************************
    *
    *  constructor (simple)
    *
    *****************************************/

    public AutoProvisionedSubscriber(String subscriberID, String externalSubscriberID, int tenantID)
    {
      this.subscriberID = subscriberID;
      this.externalSubscriberID = externalSubscriberID;
      this.assignSubscriberIDs = null;
      this.tenantID = tenantID;
    }

    /*****************************************
    *
    *  constructor (unpack)
    *
    *****************************************/

    private AutoProvisionedSubscriber(String subscriberID, String externalSubscriberID, AssignSubscriberIDs assignSubscriberIDs, int tenantID)
    {
      this.subscriberID = subscriberID;
      this.externalSubscriberID = externalSubscriberID;
      this.assignSubscriberIDs = assignSubscriberIDs;
      this.tenantID = tenantID;
    }

    /*****************************************
    *
    *  constructor (copy)
    *
    *****************************************/

    public AutoProvisionedSubscriber(AutoProvisionedSubscriber autoProvisionedSubscriber)
    {
      this.subscriberID = autoProvisionedSubscriber.getSubscriberID();
      this.externalSubscriberID = autoProvisionedSubscriber.getExternalSubscriberID();
      this.assignSubscriberIDs = autoProvisionedSubscriber.getAssignSubscriberIDs();
      this.tenantID = autoProvisionedSubscriber.getTenantID();
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      AutoProvisionedSubscriber autoProvisionedSubscriber = (AutoProvisionedSubscriber) value;
      Struct struct = new Struct(schema);
      struct.put("subscriberID", autoProvisionedSubscriber.getSubscriberID());
      struct.put("externalSubscriberID", autoProvisionedSubscriber.getExternalSubscriberID());
      struct.put("assignSubscriberIDs", assignSubscriberIDsSerde.packOptional(autoProvisionedSubscriber.getAssignSubscriberIDs()));
      struct.put("tenantID", (short)autoProvisionedSubscriber.getTenantID());
      return struct;
    }

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static AutoProvisionedSubscriber unpack(SchemaAndValue schemaAndValue)
    {
      //
      //  data
      //

      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? com.evolving.nglm.core.SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

      //
      //  unpack
      //

      Struct valueStruct = (Struct) value;
      String subscriberID = valueStruct.getString("subscriberID");
      String externalSubscriberID = valueStruct.getString("externalSubscriberID");
      AssignSubscriberIDs assignSubscriberIDs = assignSubscriberIDsSerde.unpackOptional(new SchemaAndValue(schema.field("assignSubscriberIDs").schema(), valueStruct.get("assignSubscriberIDs")));
      int tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : 1; // by default tenantID = 1

      //
      //  return
      //

      return new AutoProvisionedSubscriber(subscriberID, externalSubscriberID, assignSubscriberIDs, tenantID);
    }
  }

  /****************************************
  *
  *  class ProvisionedSubscriberID
  *
  ****************************************/

  public static class ProvisionedSubscriberID
  {
    /*****************************************
    *
    *  connect serdes
    *
    *****************************************/

    private static ConnectSerde<UpdateAlternateID> updateAlternateIDSerde = UpdateAlternateID.serde();
    private static ConnectSerde<UpdateSubscriberID> updateSubscriberIDSerde = UpdateSubscriberID.serde();

    /*****************************************
    *
    *  schema
    *
    *****************************************/

    //
    //  schema
    //

    private static Schema schema = null;
    static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("provisioned_subscriberid");
      schemaBuilder.version(com.evolving.nglm.core.SchemaUtilities.packSchemaVersion(2));
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("alternateID", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("backChannel", updateAlternateIDSerde.optionalSchema());
      schemaBuilder.field("cleanupTable", updateSubscriberIDSerde.optionalSchema());
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<ProvisionedSubscriberID> serde = new ConnectSerde<ProvisionedSubscriberID>(schema, false, ProvisionedSubscriberID.class, ProvisionedSubscriberID::pack, ProvisionedSubscriberID::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<ProvisionedSubscriberID> serde() { return serde; }

    /****************************************
    *
    *  data
    *
    ****************************************/

    private String subscriberID;
    private String alternateID;
    private UpdateAlternateID backChannel;
    private UpdateSubscriberID cleanupTable;

    /****************************************
    *
    *  accessors
    *
    ****************************************/

    public String getSubscriberID() { return subscriberID; }
    public String getAlternateID() { return alternateID; }
    public UpdateAlternateID getBackChannel() { return backChannel; }
    public UpdateSubscriberID getCleanupTable() { return cleanupTable; }

    //
    //  setters
    //

    public void setSubscriberID(String subscriberID) { this.subscriberID = subscriberID; }
    public void setAlternateID(String alternateID) { this.alternateID = alternateID; }
    public void setBackChannel(UpdateAlternateID backChannel) { this.backChannel = backChannel; }
    public void setCleanupTable(UpdateSubscriberID cleanupTable) { this.cleanupTable = cleanupTable; }

    /*****************************************
    *
    *  constructor (simple)
    *
    *****************************************/

    public ProvisionedSubscriberID(String subscriberID, String alternateID)
    {
      this.subscriberID = subscriberID;
      this.alternateID = alternateID;
      this.backChannel = null;
      this.cleanupTable = null;
    }

    /*****************************************
    *
    *  constructor (unpack)
    *
    *****************************************/

    private ProvisionedSubscriberID(String subscriberID, String alternateID, UpdateAlternateID backChannel, UpdateSubscriberID cleanupTable)
    {
      this.subscriberID = subscriberID;
      this.alternateID = alternateID;
      this.backChannel = backChannel;
      this.cleanupTable = cleanupTable;
    }

    /*****************************************
    *
    *  constructor (copy)
    *
    *****************************************/

    public ProvisionedSubscriberID(ProvisionedSubscriberID provisionedSubscriberID)
    {
      this.subscriberID = provisionedSubscriberID.getSubscriberID();
      this.alternateID = provisionedSubscriberID.getAlternateID();
      this.backChannel = provisionedSubscriberID.getBackChannel();
      this.cleanupTable = provisionedSubscriberID.getCleanupTable();
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      ProvisionedSubscriberID provisionedSubscriberID = (ProvisionedSubscriberID) value;
      Struct struct = new Struct(schema);
      struct.put("subscriberID", provisionedSubscriberID.getSubscriberID());
      struct.put("alternateID", provisionedSubscriberID.getAlternateID());
      struct.put("backChannel", updateAlternateIDSerde.packOptional(provisionedSubscriberID.getBackChannel()));
      struct.put("cleanupTable", updateSubscriberIDSerde.packOptional(provisionedSubscriberID.getCleanupTable()));
      return struct;
    }

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static ProvisionedSubscriberID unpack(SchemaAndValue schemaAndValue)
    {
      //
      //  data
      //

      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? com.evolving.nglm.core.SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

      //
      //  unpack
      //

      Struct valueStruct = (Struct) value;
      String subscriberID = valueStruct.getString("subscriberID");
      String alternateID = valueStruct.getString("alternateID");
      UpdateAlternateID backChannel = updateAlternateIDSerde.unpackOptional(new SchemaAndValue(schema.field("backChannel").schema(), valueStruct.get("backChannel")));
      UpdateSubscriberID cleanupTable = (schemaVersion >= 2) ? updateSubscriberIDSerde.unpackOptional(new SchemaAndValue(schema.field("cleanupTable").schema(), valueStruct.get("cleanupTable"))) : null;
      
      //
      //  return
      //

      return new ProvisionedSubscriberID(subscriberID, alternateID, backChannel, cleanupTable);
    }
  }
  
  /****************************************
  *
  *  class ProvisionedAlternateID
  *
  ****************************************/

  public static class ProvisionedAlternateID
  {
    /*****************************************
    *
    *  connect serdes
    *
    *****************************************/

    private static ConnectSerde<UpdateSubscriberID> updateSubscriberIDSerde = UpdateSubscriberID.serde();
    private static ConnectSerde<UpdateAlternateID> updateAlternateIDSerde = UpdateAlternateID.serde();
    private static ConnectSerde<RecordSubscriberID> recordSubscriberIDSerde = RecordSubscriberID.serde();
    private static ConnectSerde<RecordAlternateID> recordAlternateIDSerde = RecordAlternateID.serde();

    /*****************************************
    *
    *  schema
    *
    *****************************************/

    //
    //  schema
    //

    private static Schema schema = null;
    static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("provisioned_alternateid");
      schemaBuilder.version(com.evolving.nglm.core.SchemaUtilities.packSchemaVersion(3));
      schemaBuilder.field("alternateID", Schema.STRING_SCHEMA);
      schemaBuilder.field("allSubscriberIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
      schemaBuilder.field("backChannel", updateSubscriberIDSerde.optionalSchema());
      schemaBuilder.field("cleanupTable", updateAlternateIDSerde.optionalSchema());
      schemaBuilder.field("provisionedSubscriberID", recordSubscriberIDSerde.optionalSchema());
      schemaBuilder.field("deprovisionedSubscriberID", recordSubscriberIDSerde.optionalSchema());
      schemaBuilder.field("provisionedAlternateID", recordAlternateIDSerde.optionalSchema());
      schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<ProvisionedAlternateID> serde = new ConnectSerde<ProvisionedAlternateID>(schema, false, ProvisionedAlternateID.class, ProvisionedAlternateID::pack, ProvisionedAlternateID::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<ProvisionedAlternateID> serde() { return serde; }

    /****************************************
    *
    *  data
    *
    ****************************************/

    private String alternateID;
    private Set<String> allSubscriberIDs;
    private UpdateSubscriberID backChannel;
    private UpdateAlternateID cleanupTable;
    private RecordSubscriberID provisionedSubscriberID;
    private RecordSubscriberID deprovisionedSubscriberID;
    private RecordAlternateID provisionedAlternateID;
    private int tenantID;

    /****************************************
    *
    *  accessors
    *
    ****************************************/

    public String getAlternateID() { return alternateID; }
    public Set<String> getAllSubscriberIDs() { return allSubscriberIDs; }
    public UpdateSubscriberID getBackChannel() { return backChannel; }
    public UpdateAlternateID getCleanupTable() { return cleanupTable; }
    public RecordSubscriberID getProvisionedSubscriberID() { return provisionedSubscriberID; }
    public RecordSubscriberID getDeprovisionedSubscriberID() { return deprovisionedSubscriberID; }
    public RecordAlternateID getProvisionedAlternateID() { return provisionedAlternateID; }
    public int getTenantID() { return tenantID; }

    //
    //  setters
    //

    public void setAlternateID(String alternateID) { this.alternateID = alternateID; }
    public void setAllSubscriberIDs(Set<String> allSubscriberIDs) { this.allSubscriberIDs = allSubscriberIDs; }
    public void setBackChannel(UpdateSubscriberID backChannel) { this.backChannel = backChannel; }
    public void setCleanupTable(UpdateAlternateID cleanupTable) { this.cleanupTable = cleanupTable; }
    public void setProvisionedSubscriberID(RecordSubscriberID provisionedSubscriberID) { this.provisionedSubscriberID = provisionedSubscriberID; }
    public void setDeprovisionedSubscriberID(RecordSubscriberID deprovisionedSubscriberID) { this.deprovisionedSubscriberID = deprovisionedSubscriberID; }
    public void setProvisionedAlternateID(RecordAlternateID provisionedAlternateID) { this.provisionedAlternateID = provisionedAlternateID; }

    /*****************************************
    *
    *  constructor (simple)
    *
    *****************************************/

    public ProvisionedAlternateID(String alternateID, Set<String> allSubscriberIDs, int tenantID)
    {
      this.alternateID = alternateID;
      this.allSubscriberIDs = allSubscriberIDs;
      this.backChannel = null;
      this.cleanupTable = null;
      this.provisionedSubscriberID = null;
      this.deprovisionedSubscriberID = null;
      this.provisionedAlternateID = null;
      this.tenantID = tenantID;
    }

    /*****************************************
    *
    *  constructor (unpack)
    *
    *****************************************/

    private ProvisionedAlternateID(String alternateID, Set<String> allSubscriberIDs, UpdateSubscriberID backChannel, UpdateAlternateID cleanupTable, RecordSubscriberID provisionedSubscriberID, RecordSubscriberID deprovisionedSubscriberID, RecordAlternateID provisionedAlternateID, int tenantID)
    {
      this.alternateID = alternateID;
      this.allSubscriberIDs = allSubscriberIDs;
      this.backChannel = backChannel;
      this.cleanupTable = cleanupTable;
      this.provisionedSubscriberID = provisionedSubscriberID;
      this.deprovisionedSubscriberID = deprovisionedSubscriberID;
      this.provisionedAlternateID = provisionedAlternateID;
      this.tenantID = tenantID;
    }

    /*****************************************
    *
    *  constructor (copy)
    *
    *****************************************/

    public ProvisionedAlternateID(ProvisionedAlternateID provisionedAlternateID)
    {
      this.alternateID = provisionedAlternateID.getAlternateID();
      this.allSubscriberIDs = provisionedAlternateID.getAllSubscriberIDs();
      this.backChannel = provisionedAlternateID.getBackChannel();
      this.cleanupTable = provisionedAlternateID.getCleanupTable();
      this.provisionedSubscriberID = provisionedAlternateID.getProvisionedSubscriberID();
      this.deprovisionedSubscriberID = provisionedAlternateID.getDeprovisionedSubscriberID();
      this.provisionedAlternateID = provisionedAlternateID.getProvisionedAlternateID();
      this.tenantID = provisionedAlternateID.getTenantID();
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      ProvisionedAlternateID provisionedAlternateID = (ProvisionedAlternateID) value;
      Struct struct = new Struct(schema);
      struct.put("alternateID", provisionedAlternateID.getAlternateID());
      struct.put("allSubscriberIDs", packAllSubscriberIDs(provisionedAlternateID.getAllSubscriberIDs()));
      struct.put("backChannel", updateSubscriberIDSerde.packOptional(provisionedAlternateID.getBackChannel()));
      struct.put("cleanupTable", updateAlternateIDSerde.packOptional(provisionedAlternateID.getCleanupTable()));
      struct.put("provisionedSubscriberID", recordSubscriberIDSerde.packOptional(provisionedAlternateID.getProvisionedSubscriberID()));
      struct.put("deprovisionedSubscriberID", recordSubscriberIDSerde.packOptional(provisionedAlternateID.getDeprovisionedSubscriberID()));
      struct.put("provisionedAlternateID", recordAlternateIDSerde.packOptional(provisionedAlternateID.getProvisionedAlternateID()));
      struct.put("tenantID", (short)provisionedAlternateID.getTenantID());
      return struct;
    }

    /*****************************************
    *
    *  packAllSubscriberIDs
    *
    *****************************************/

    private static List<Object> packAllSubscriberIDs(Set<String> allSubscriberIDs)
    {
      List<Object> result = new ArrayList<Object>();
      for (String subscriberID : allSubscriberIDs)
        {
          result.add(subscriberID);
        }
      return result;
    }

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static ProvisionedAlternateID unpack(SchemaAndValue schemaAndValue)
    {
      //
      //  data
      //

      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? com.evolving.nglm.core.SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

      //
      //  unpack
      //

      Struct valueStruct = (Struct) value;
      String alternateID = valueStruct.getString("alternateID");
      Set<String> allSubscriberIDs = unpackAllSubscriberIDs((List<String>) valueStruct.get("allSubscriberIDs"));
      UpdateSubscriberID backChannel = updateSubscriberIDSerde.unpackOptional(new SchemaAndValue(schema.field("backChannel").schema(), valueStruct.get("backChannel")));
      UpdateAlternateID cleanupTable = (schemaVersion >= 2) ? updateAlternateIDSerde.unpackOptional(new SchemaAndValue(schema.field("cleanupTable").schema(), valueStruct.get("cleanupTable"))) : null;
      RecordSubscriberID provisionedSubscriberID = recordSubscriberIDSerde.unpackOptional(new SchemaAndValue(schema.field("provisionedSubscriberID").schema(), valueStruct.get("provisionedSubscriberID")));
      RecordSubscriberID deprovisionedSubscriberID = recordSubscriberIDSerde.unpackOptional(new SchemaAndValue(schema.field("deprovisionedSubscriberID").schema(), valueStruct.get("deprovisionedSubscriberID")));
      RecordAlternateID provisionedAlternateID = recordAlternateIDSerde.unpackOptional(new SchemaAndValue(schema.field("provisionedAlternateID").schema(), valueStruct.get("provisionedAlternateID")));
      int tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : 1; // by default tenantID = 1
      
      //
      //  return
      //

      return new ProvisionedAlternateID(alternateID, allSubscriberIDs, backChannel, cleanupTable, provisionedSubscriberID, deprovisionedSubscriberID, provisionedAlternateID, tenantID);
    }

    /*****************************************
    *
    *  unpackAllSubscriberIDs
    *
    *****************************************/

    private static Set<String> unpackAllSubscriberIDs(List<String> allSubscriberIDs)
    {
      Set<String> result = new HashSet<String>();
      for (String subscriberID : allSubscriberIDs)
        {
          result.add(subscriberID);
        }
      return result;
    }
  }
}
