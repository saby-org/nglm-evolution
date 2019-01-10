/****************************************************************************
*
*  EvolutionEngine.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.kafka.streams.state.Stores;
import org.apache.zookeeper.ZooKeeper;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.KStreamsUniqueKeyServer;
import com.evolving.nglm.core.NGLMKafkaClientSupplier;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.RecordSubscriberID;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.SubscriberTrace;
import com.evolving.nglm.core.SubscriberTraceControl;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.SubscriberGroupLoader.LoadType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EvolutionEngine
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum API
  {
    getSubscriberProfile("getSubscriberProfile"),
    retrieveSubscriberProfile("retrieveSubscriberProfile"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private API(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static API fromExternalRepresentation(String externalRepresentation) { for (API enumeratedValue : API.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(EvolutionEngine.class);

  //
  //  hardwired arguments
  //

  public static String baseTimeZone = Deployment.getBaseTimeZone();

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  static data (for the singleton instance)
  //

  private static ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private static JourneyService journeyService;
  private static SegmentationRuleService segmentationRuleService;
  private static EvolutionEngineStatistics evolutionEngineStatistics;
  private static KStreamsUniqueKeyServer uniqueKeyServer = new KStreamsUniqueKeyServer();
  private static Method evolutionEngineExtensionUpdateSubscriberMethod;
  private static TimerService timerService;
  private static KafkaStreams streams = null;
  private static ReadOnlyKeyValueStore<StringKey, SubscriberState> subscriberStateStore = null;
  private static ReadOnlyKeyValueStore<StringKey, SubscriberHistory> subscriberHistoryStore = null;
  private static final int RESTAPIVersion = 1;
  private static HttpServer subscriberProfileServer;
  private static HttpServer internalServer;
  private static HttpClient httpClient;

  /****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    NGLMRuntime.initialize();
    EvolutionEngine evolutionEngine = new EvolutionEngine();
    evolutionEngine.start(args);
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

    String applicationID = "streams-evolutionengine";
    String stateDirectory = args[0];
    String bootstrapServers = args[1];
    String evolutionEngineKey = args[2];
    String subscriberProfileHost = args[3];
    Integer subscriberProfilePort = Integer.parseInt(args[4]);
    Integer internalPort = Integer.parseInt(args[5]);
    Integer kafkaReplicationFactor = Integer.parseInt(args[6]);
    Integer kafkaStreamsStandbyReplicas = Integer.parseInt(args[7]);
    Integer numberOfStreamThreads = Integer.parseInt(args[8]);

    //
    //  source topics 
    //

    String emptyTopic = Deployment.getEmptyTopic();
    String timedEvaluationTopic = Deployment.getTimedEvaluationTopic();
    String recordSubscriberIDTopic = Deployment.getRecordSubscriberIDTopic();
    String subscriberGroupTopic = Deployment.getSubscriberGroupTopic();
    String subscriberTraceControlTopic = Deployment.getSubscriberTraceControlTopic();

    //
    //  sink topics
    //

    String subscriberUpdateTopic = Deployment.getSubscriberUpdateTopic();
    String journeyStatisticTopic = Deployment.getJourneyStatisticTopic();
    String subscriberTraceTopic = Deployment.getSubscriberTraceTopic();

    //
    //  changelogs
    //

    String subscriberStateChangeLog = Deployment.getSubscriberStateChangeLog();
    String subscriberStateChangeLogTopic = Deployment.getSubscriberStateChangeLogTopic();
    String subscriberHistoryChangeLog = Deployment.getSubscriberHistoryChangeLog();
    String subscriberHistoryChangeLogTopic = Deployment.getSubscriberHistoryChangeLogTopic();

    //
    //  log
    //

    log.info("main START: {} {} {} {} {}", stateDirectory, bootstrapServers, kafkaStreamsStandbyReplicas, numberOfStreamThreads, kafkaReplicationFactor);

    //
    //  journeyService
    //

    journeyService = new JourneyService(bootstrapServers, "evolutionengine-journeyservice-" + evolutionEngineKey, Deployment.getJourneyTopic(), false);
    journeyService.start();

    //
    //  timerService (DO NOT START until streams is started)
    //

    timerService = new TimerService(this, bootstrapServers);

    /*****************************************
    *
    *  kafka producer for the segmentationRuleListener
    *
    *****************************************/

    Properties producerProperties = new Properties();
    producerProperties.put("bootstrap.servers", bootstrapServers);
    producerProperties.put("acks", "all");
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);
    
    //
    //  segmentationRuleService
    //
    
    SegmentationRuleService.SegmentationRuleListener segmentationRuleListener = new SegmentationRuleService.SegmentationRuleListener()
    {
      //
      //  segmentationRuleActivated
      //

      @Override public void segmentationRuleActivated(SegmentationRule segmentationRule)
      {
        /*****************************************
        *
        *  open zookeeper and lock group
        *
        *****************************************/

        ZooKeeper zookeeper = SubscriberGroupEpochService.openZooKeeperAndLockGroup(segmentationRule.getSubscriberGroupName());
        
        /*****************************************
        *
        *  create or ensure exists
        *
        *****************************************/

        SubscriberGroupEpoch existingEpoch = SubscriberGroupEpochService.retrieveSubscriberGroupEpoch(zookeeper, segmentationRule.getSubscriberGroupName(), LoadType.New, segmentationRule.getGUIManagedObjectID());
        
        /*****************************************
        *
        *  epoch
        *
        *****************************************/

        int epoch = existingEpoch.getEpoch() + 1;

        /*****************************************
        *
        *  submit new epoch (if necessary)
        *
        *****************************************/

        SubscriberGroupEpochService.updateSubscriberGroupEpoch(zookeeper, existingEpoch, epoch, true, kafkaProducer, Deployment.getSubscriberGroupEpochTopic());

        /*****************************************
        *
        *  close
        *
        *****************************************/

        SubscriberGroupEpochService.closeZooKeeperAndReleaseGroup(zookeeper, segmentationRule.getSubscriberGroupName());
      }

      //
      //  segmentationRuleDeactivated
      //

      @Override public void segmentationRuleDeactivated(String guiManagedObjectID)
      {
        throw new UnsupportedOperationException();
      }
    };

    segmentationRuleService = new SegmentationRuleService(bootstrapServers, "evolutionengine-segmentationruleservice-" + evolutionEngineKey, Deployment.getSegmentationRuleTopic(), false, segmentationRuleListener);
    segmentationRuleService.start();

    //
    //  subscriberGroupEpochReader
    //

    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("evolutionEngine-subscriberGroupEpoch", evolutionEngineKey, Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
    
    //
    //  create monitoring object
    //

    evolutionEngineStatistics = new EvolutionEngineStatistics(applicationID);

    //
    //  evolutionEngineExtensionUpdateSubscriberMethod
    //
    
    try
      {
        evolutionEngineExtensionUpdateSubscriberMethod = Deployment.getEvolutionEngineExtensionClass().getMethod("updateSubscriberProfile",EvolutionEventContext.class,SubscriberStreamEvent.class);
      }
    catch (NoSuchMethodException e)
      {
        throw new RuntimeException(e);
      }
    
    /*****************************************
    *
    *  stream properties
    *
    *****************************************/
    
    Properties streamsProperties = new Properties();
    streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID);
    streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory);
    streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsProperties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EvolutionEventTimestampExtractor.class.getName());
    streamsProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Integer.toString(numberOfStreamThreads));
    streamsProperties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, Integer.toString(kafkaReplicationFactor));
    streamsProperties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, Integer.toString(kafkaStreamsStandbyReplicas));
    streamsProperties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString());
    streamsProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, subscriberProfileHost + ":" + Integer.toString(internalPort));
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
    *  evolution engine event topics/serdes
    *
    *****************************************/

    Map<EvolutionEngineEventDeclaration,String> evolutionEngineEventTopics = new HashMap<EvolutionEngineEventDeclaration,String>();
    Map<EvolutionEngineEventDeclaration,ConnectSerde<? extends SubscriberStreamEvent>> evolutionEngineEventSerdes = new HashMap<EvolutionEngineEventDeclaration,ConnectSerde<? extends SubscriberStreamEvent>>();
    for (EvolutionEngineEventDeclaration evolutionEngineEvent : Deployment.getEvolutionEngineEvents().values())
      {
        evolutionEngineEventTopics.put(evolutionEngineEvent, evolutionEngineEvent.getEventTopic());
        evolutionEngineEventSerdes.put(evolutionEngineEvent, evolutionEngineEvent.getEventSerde());
      }

    /*****************************************
    *
    *  delivery managers topics/serdes
    *
    *****************************************/

    Map<DeliveryManagerDeclaration,String> deliveryManagerRequestTopics = new HashMap<DeliveryManagerDeclaration,String>();
    Map<DeliveryManagerDeclaration,String> deliveryManagerResponseTopics = new HashMap<DeliveryManagerDeclaration,String>();
    Map<DeliveryManagerDeclaration,ConnectSerde<? extends DeliveryRequest>> deliveryManagerResponseSerdes = new HashMap<DeliveryManagerDeclaration,ConnectSerde<? extends DeliveryRequest>>();
    for (DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values())
      {
        deliveryManagerRequestTopics.put(deliveryManager, deliveryManager.getRequestTopic());
        deliveryManagerResponseTopics.put(deliveryManager, deliveryManager.getResponseTopic());
        deliveryManagerResponseSerdes.put(deliveryManager, deliveryManager.getRequestSerde());
      }

    /*****************************************
    *
    *  serdes
    *
    *****************************************/

    final Serde<byte[]> byteArraySerde = new Serdes.ByteArraySerde();
    final ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
    final ConnectSerde<TimedEvaluation> timedEvaluationSerde = TimedEvaluation.serde();
    final ConnectSerde<RecordSubscriberID> recordSubscriberIDSerde = RecordSubscriberID.serde();
    final ConnectSerde<SubscriberGroup> subscriberGroupSerde = SubscriberGroup.serde();
    final ConnectSerde<SubscriberTraceControl> subscriberTraceControlSerde = SubscriberTraceControl.serde();
    final ConnectSerde<SubscriberState> subscriberStateSerde = SubscriberState.serde();
    final ConnectSerde<SubscriberHistory> subscriberHistorySerde = SubscriberHistory.serde();
    final ConnectSerde<SubscriberProfile> subscriberProfileSerde = SubscriberProfile.getSubscriberProfileSerde();
    final Serde<JourneyStatistic> journeyStatisticSerde = JourneyStatistic.serde();
    final Serde<SubscriberTrace> subscriberTraceSerde = SubscriberTrace.serde();

    /*****************************************
    *
    *  deployment objects
    *  - serdes
    *  - source nodes (topics)
    *  - trigger event streams
    *
    *****************************************/

    ArrayList<ConnectSerde<? extends SubscriberStreamEvent>> evolutionEventSerdes = new ArrayList<ConnectSerde<? extends SubscriberStreamEvent>>();
    evolutionEventSerdes.add(timedEvaluationSerde);
    evolutionEventSerdes.add(recordSubscriberIDSerde);
    evolutionEventSerdes.add(subscriberGroupSerde);
    evolutionEventSerdes.add(subscriberTraceControlSerde);
    evolutionEventSerdes.addAll(evolutionEngineEventSerdes.values());
    evolutionEventSerdes.addAll(deliveryManagerResponseSerdes.values());
    final ConnectSerde<SubscriberStreamEvent> evolutionEventSerde = new ConnectSerde<SubscriberStreamEvent>("evolution_event", false, evolutionEventSerdes.toArray(new ConnectSerde[0]));

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

    //
    //  empty
    //

    KStream emptySourceStream = builder.stream(emptyTopic, Consumed.with(byteArraySerde, byteArraySerde)).filter((key,value) -> false);

    //
    //  core streams
    //

    KStream<StringKey, TimedEvaluation> timedEvaluationSourceStream = builder.stream(timedEvaluationTopic, Consumed.with(stringKeySerde, timedEvaluationSerde));
    KStream<StringKey, RecordSubscriberID> recordSubscriberIDSourceStream = builder.stream(recordSubscriberIDTopic, Consumed.with(stringKeySerde, recordSubscriberIDSerde));
    KStream<StringKey, SubscriberGroup> subscriberGroupSourceStream = builder.stream(subscriberGroupTopic, Consumed.with(stringKeySerde, subscriberGroupSerde));
    KStream<StringKey, SubscriberTraceControl> subscriberTraceControlSourceStream = builder.stream(subscriberTraceControlTopic, Consumed.with(stringKeySerde, subscriberTraceControlSerde));
    
    //
    //  evolution engine event source streams
    //

    List<KStream<StringKey, ? extends SubscriberStreamEvent>> evolutionEngineEventStreams = new ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    for (EvolutionEngineEventDeclaration evolutionEngineEventDeclaration : Deployment.getEvolutionEngineEvents().values())
      {
        evolutionEngineEventStreams.add(builder.stream(evolutionEngineEventTopics.get(evolutionEngineEventDeclaration), Consumed.with(stringKeySerde, evolutionEngineEventSerdes.get(evolutionEngineEventDeclaration))));
      }

    //
    //  delivery manager response source streams
    //

    List<KStream<StringKey, ? extends SubscriberStreamEvent>> deliveryManagerResponseStreams = new ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    for (DeliveryManagerDeclaration deliveryManagerDeclaration : Deployment.getDeliveryManagers().values())
      {
        deliveryManagerResponseStreams.add(builder.stream(deliveryManagerResponseTopics.get(deliveryManagerDeclaration), Consumed.with(stringKeySerde, deliveryManagerResponseSerdes.get(deliveryManagerDeclaration))));
      }

    //
    //  merge source streams -- evolutionEventStream
    //

    ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>> evolutionEventStreams = new ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) timedEvaluationSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) recordSubscriberIDSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberGroupSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberTraceControlSourceStream);
    evolutionEventStreams.addAll(evolutionEngineEventStreams);
    evolutionEventStreams.addAll(deliveryManagerResponseStreams);
    KStream evolutionEventCompositeStream = null;
    for (KStream<StringKey, ? extends SubscriberStreamEvent> eventStream : evolutionEventStreams)
      {
        evolutionEventCompositeStream = (evolutionEventCompositeStream == null) ? eventStream : evolutionEventCompositeStream.merge(eventStream);
      }
    KStream<StringKey, SubscriberStreamEvent> evolutionEventStream = (KStream<StringKey, SubscriberStreamEvent>) evolutionEventCompositeStream;
    
    //
    //  merge source streams -- deliveryResponseStream
    //

    KStream deliveryResponseCompositeStream = emptySourceStream;
    for (KStream<StringKey, ? extends SubscriberStreamEvent> eventStream : deliveryManagerResponseStreams)
      {
        deliveryResponseCompositeStream = (deliveryResponseCompositeStream == null) ? eventStream : deliveryResponseCompositeStream.merge(eventStream);
      }
    KStream<StringKey, SubscriberStreamEvent> deliveryResponseStream = (KStream<StringKey, SubscriberStreamEvent>) deliveryResponseCompositeStream;

    /*****************************************
    *
    *  subscriberState -- update
    *
    *****************************************/

    KeyValueBytesStoreSupplier subscriberStateSupplier = Stores.persistentKeyValueStore(subscriberStateChangeLog);
    Materialized subscriberStateStoreSchema = Materialized.<StringKey, SubscriberState>as(subscriberStateSupplier).withKeySerde(stringKeySerde).withValueSerde(subscriberStateSerde.optionalSerde());
    KTable<StringKey, SubscriberState> subscriberState = evolutionEventStream.groupByKey(Serialized.with(stringKeySerde, evolutionEventSerde)).aggregate(EvolutionEngine::nullSubscriberState, EvolutionEngine::updateSubscriberState, subscriberStateStoreSchema);

    /*****************************************
    *
    *  convert to stream
    *
    *****************************************/
    
    KStream<StringKey, SubscriberState> subscriberStateStream = evolutionEventStream.leftJoin(subscriberState, EvolutionEngine::getSubscriberState);

    /*****************************************
    *
    *  get outputs
    *
    *****************************************/

    KStream<StringKey, SubscriberStreamOutput> evolutionEngineOutputs = subscriberStateStream.flatMapValues(EvolutionEngine::getEvolutionEngineOutputs);
    
    /*****************************************
    *
    *  branch output streams
    *
    *****************************************/

    KStream<StringKey, ? extends SubscriberStreamOutput>[] branchedEvolutionEngineOutputs = evolutionEngineOutputs.branch((key,value) -> (value instanceof SubscriberProfile), (key,value) -> (value instanceof DeliveryRequest), (key,value) -> (value instanceof JourneyStatistic), (key,value) -> (value instanceof SubscriberTrace));
    KStream<StringKey, SubscriberProfile> subscriberUpdateStream = (KStream<StringKey, SubscriberProfile>) branchedEvolutionEngineOutputs[0];
    KStream<StringKey, DeliveryRequest> deliveryRequestStream = (KStream<StringKey, DeliveryRequest>) branchedEvolutionEngineOutputs[1];
    KStream<StringKey, JourneyStatistic> journeyStatisticStream = (KStream<StringKey, JourneyStatistic>) branchedEvolutionEngineOutputs[2];
    KStream<StringKey, SubscriberTrace> subscriberTraceStream = (KStream<StringKey, SubscriberTrace>) branchedEvolutionEngineOutputs[3];

    /*****************************************
    *
    *  branch delivery requests
    *
    *****************************************/
    
    //
    //  build predicates for delivery requests
    //

    String[] deliveryManagerDeliveryTypes = new String[Deployment.getDeliveryManagers().size()];
    DeliveryManagerPredicate[] deliveryManagerPredicates = new DeliveryManagerPredicate[Deployment.getDeliveryManagers().size()];
    int i = 0;
    for (DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values())
      {
        deliveryManagerDeliveryTypes[i] = deliveryManager.getDeliveryType();
        deliveryManagerPredicates[i] = new DeliveryManagerPredicate(deliveryManager.getDeliveryType());
        i += 1;
      }

    //
    //  branch
    //

    KStream<StringKey, DeliveryRequest>[] branchedDeliveryRequestStreams = (Deployment.getDeliveryManagers().size() > 0) ? deliveryRequestStream.branch(deliveryManagerPredicates) : new KStream[0];

    //
    //  delivery request streams
    //

    Map<String, KStream<StringKey, DeliveryRequest>> deliveryRequestStreams = new HashMap<String, KStream<StringKey, DeliveryRequest>>();
    for (int j=0; j<branchedDeliveryRequestStreams.length; j++)
      {
        deliveryRequestStreams.put(deliveryManagerDeliveryTypes[j], branchedDeliveryRequestStreams[j]);
      }

    /*****************************************
    *
    *  sink
    *
    *****************************************/

    //
    //  sink - core streams
    //

    subscriberUpdateStream.to(subscriberUpdateTopic, Produced.with(stringKeySerde, (Serde<SubscriberProfile>) subscriberProfileSerde));
    journeyStatisticStream.to(journeyStatisticTopic, Produced.with(stringKeySerde, journeyStatisticSerde));
    subscriberTraceStream.to(subscriberTraceTopic, Produced.with(stringKeySerde, subscriberTraceSerde));
    
    //
    //  sink - delivery request streams
    //

    for (String deliveryType : deliveryRequestStreams.keySet())
      {
        DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get(deliveryType);
        String requestTopic = deliveryManager.getRequestTopic();
        ConnectSerde<DeliveryRequest> requestSerde = (ConnectSerde<DeliveryRequest>) deliveryManager.getRequestSerde();
        KStream<StringKey, DeliveryRequest> requestStream = deliveryRequestStreams.get(deliveryType);
        KStream<StringKey, DeliveryRequest> rekeyedRequestStream = requestStream.map(EvolutionEngine::rekeyDeliveryRequestStream);
        rekeyedRequestStream.to(requestTopic, Produced.with(stringKeySerde, requestSerde));
      }

    /*****************************************
    *
    *  subscriberHistory -- update
    *
    *****************************************/

    KeyValueBytesStoreSupplier subscriberHistorySupplier = Stores.persistentKeyValueStore(subscriberHistoryChangeLog);
    Materialized subscriberHistoryStoreSchema = Materialized.<StringKey, SubscriberHistory>as(subscriberHistorySupplier).withKeySerde(stringKeySerde).withValueSerde(subscriberHistorySerde.optionalSerde());
    KTable<StringKey, SubscriberHistory> subscriberHistory = deliveryResponseStream.groupByKey(Serialized.with(stringKeySerde, evolutionEventSerde)).aggregate(EvolutionEngine::nullSubscriberHistory, EvolutionEngine::updateSubscriberHistory, subscriberHistoryStoreSchema);

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

        synchronized (this)
          {
            this.notifyAll();
          }

        //
        //  timerService
        //

        if (timerService != null) timerService.forceLoadSchedule();
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
    *  state stores
    *
    *****************************************/

    boolean stateStoresInitialized = false;
    while (! stateStoresInitialized)
      {
        //
        //  initialize
        //

        try
          {
            subscriberStateStore = streams.store(Deployment.getSubscriberStateChangeLog(), QueryableStoreTypes.keyValueStore());
            subscriberHistoryStore = streams.store(Deployment.getSubscriberHistoryChangeLog(), QueryableStoreTypes.keyValueStore());
            stateStoresInitialized = true;
          }
        catch (InvalidStateStoreException e)
          {
            StringWriter stackTraceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
            log.debug(stackTraceWriter.toString());
          }

        //
        //  sleep (if necessary)
        //

        try
          {
            Thread.sleep(1000);
          }
        catch (InterruptedException e)
          {
          }
      }

    /*****************************************
    *
    *  REST interface -- http client
    *
    *****************************************/

    //
    //  default connections
    //

    PoolingHttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager();
    httpClientConnectionManager.setDefaultMaxPerRoute(50);
    httpClientConnectionManager.setMaxTotal(150);

    //
    //  httpClient
    //

    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    httpClientBuilder.setConnectionManager(httpClientConnectionManager);
    httpClient = httpClientBuilder.build();

    /*****************************************
    *
    *  REST interface -- subscriber profile server
    *
    *****************************************/

    try
      {
        InetSocketAddress addr = new InetSocketAddress(subscriberProfilePort);
        subscriberProfileServer = HttpServer.create(addr, 0);
        subscriberProfileServer.createContext("/nglm-evolutionengine/getSubscriberProfile", new APIHandler(API.getSubscriberProfile));
        subscriberProfileServer.setExecutor(Executors.newFixedThreadPool(50));
      }
    catch (IOException e)
      {
        throw new ServerRuntimeException("could not initialize REST server", e);
      }

    /*****************************************
    *
    *  REST interface -- internal server
    *
    *****************************************/

    try
      {
        InetSocketAddress addr = new InetSocketAddress(internalPort);
        internalServer = HttpServer.create(addr, 0);
        internalServer.createContext("/nglm-evolutionengine/retrieveSubscriberProfile", new APIHandler(API.retrieveSubscriberProfile));
        internalServer.setExecutor(Executors.newFixedThreadPool(50));
      }
    catch (IOException e)
      {
        throw new ServerRuntimeException("could not initialize REST server", e);
      }

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(streams, subscriberGroupEpochReader, journeyService, segmentationRuleService, timerService, subscriberProfileServer, internalServer));

    /*****************************************
    *
    *  start streams
    *
    *****************************************/

    timerService.start(subscriberStateStore);

    /*****************************************
    *
    *  start restServers
    *
    *****************************************/

    internalServer.start();
    subscriberProfileServer.start();

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("evolution engine started");
  }

  /*****************************************
  *
  *  waitForStreams
  *
  *****************************************/
  
  public void waitForStreams(Date timeout)
  {
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

  /****************************************
  *
  *  class DeliveryManagerPredicate
  *
  ****************************************/

  private static class DeliveryManagerPredicate implements Predicate<StringKey, DeliveryRequest>
  {
    //
    //  data
    //
    
    private String deliveryType;

    //
    //  constructor
    //

    private DeliveryManagerPredicate(String deliveryType)
    {
      this.deliveryType = deliveryType;
    }

    //
    //  test (Predicate interface)
    //

    @Override public boolean test(StringKey stringKey, DeliveryRequest deliveryRequest)
    {
      return deliveryType.equals(deliveryRequest.getDeliveryType());
    }
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
    private JourneyService journeyService;
    private SegmentationRuleService segmentationRuleService;
    private TimerService timerService;
    private HttpServer subscriberProfileServer;
    private HttpServer internalServer;

    //
    //  constructor
    //

    private ShutdownHook(KafkaStreams kafkaStreams, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, JourneyService journeyService, SegmentationRuleService segmentationRuleService, TimerService timerService, HttpServer subscriberProfileServer, HttpServer internalServer)
    {
      this.kafkaStreams = kafkaStreams;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
      this.journeyService = journeyService;
      this.segmentationRuleService = segmentationRuleService;
      this.timerService = timerService;
      this.subscriberProfileServer = subscriberProfileServer;
      this.internalServer = internalServer;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      //
      //  stop stats collection
      //

      if (evolutionEngineStatistics != null) evolutionEngineStatistics.unregister();

      //
      //  close reference data reader
      //

      subscriberGroupEpochReader.close();
      
      //
      //  stop services
      //
      
      journeyService.stop();
      segmentationRuleService.stop();
      timerService.stop();
      
      //
      //  rest server
      //

      subscriberProfileServer.stop(1);
      internalServer.stop(1);

      //
      //  stop streams
      //
      
      boolean streamsCloseSuccessful = kafkaStreams.close(60, TimeUnit.SECONDS);
      log.info("Stopped EvolutionEngine" + (streamsCloseSuccessful ? "" : " (timed out)"));
    }
  }

  /*****************************************
  *
  *  castToSubscriberStreamEvent
  *
  *****************************************/

  public static SubscriberStreamEvent castToSubscriberStreamEvent(SubscriberStreamEvent subscriberStreamEvent) { return subscriberStreamEvent; }
  
  /*****************************************
  *
  *  nullSubscriberState
  *
  ****************************************/

  public static SubscriberState nullSubscriberState() { return (SubscriberState) null; }

  /*****************************************
  *
  *  updateSubscriberState
  *
  *****************************************/

  public static SubscriberState updateSubscriberState(StringKey aggKey, SubscriberStreamEvent evolutionEvent, SubscriberState currentSubscriberState)
  {
    /****************************************
    *
    *  get (or create) entry
    *
    ****************************************/

    SubscriberState subscriberState = (currentSubscriberState != null) ? new SubscriberState(currentSubscriberState) : new SubscriberState(evolutionEvent.getSubscriberID());
    SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();
    EvolutionEventContext context = new EvolutionEventContext(subscriberState, subscriberGroupEpochReader, uniqueKeyServer, SystemTime.getCurrentTime());
    boolean subscriberStateUpdated = (currentSubscriberState != null) ? false : true;

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = context.now();

    /*****************************************
    *
    *  clear state
    *
    *****************************************/

    //
    //  recentJourneyStates
    //

    Date recentJourneyStateWindow = RLMDateUtils.addMonths(now, -3, Deployment.getBaseTimeZone());
    Iterator<JourneyState> recentJourneyStates = subscriberState.getRecentJourneyStates().iterator();
    while (recentJourneyStates.hasNext())
      {
        JourneyState recentJourneyState = recentJourneyStates.next();
        if (recentJourneyState.getJourneyExitDate().before(recentJourneyStateWindow))
          {
            recentJourneyStates.remove();
          }
      }

    //
    //  scheduledEvaluations
    //

    if (subscriberState.getScheduledEvaluations().size() > 0)
      {
        subscriberState.getScheduledEvaluations().clear();
        subscriberStateUpdated = true;
      }

    //
    //  statusUpdated
    //

    if (subscriberState.getEvolutionSubscriberStatusUpdated())
      {
        subscriberState.setEvolutionSubscriberStatusUpdated(false);
        subscriberStateUpdated = true;
      }

    //
    //  deliveryRequests
    //

    if (subscriberState.getDeliveryRequests().size() > 0)
      {
        subscriberState.getDeliveryRequests().clear();
        subscriberStateUpdated = true;
      }

    //
    //  journeyStatistics
    //

    if (subscriberState.getJourneyStatistics().size() > 0)
      {
        subscriberState.getJourneyStatistics().clear();
        subscriberStateUpdated = true;
      }

    //
    //  subscriberTrace
    //

    if (subscriberState.getSubscriberTrace() != null)
      {
        subscriberState.setSubscriberTrace(null);
        subscriberStateUpdated = true;
      }

    /*****************************************
    *
    *  update SubscriberProfile
    *
    *****************************************/

    subscriberStateUpdated = updateSubscriberProfile(context, evolutionEvent) || subscriberStateUpdated;

    /*****************************************
    *
    *  update journeyStates
    *
    *****************************************/
    
    subscriberStateUpdated = updateJourneys(context, evolutionEvent) || subscriberStateUpdated;

    /*****************************************
    *
    *  scheduledEvaluations
    *
    *****************************************/

    //
    //  previously scheduled
    //

    Set<TimedEvaluation> previouslyScheduledEvaluations = (currentSubscriberState != null) ? currentSubscriberState.getScheduledEvaluations() : Collections.<TimedEvaluation>emptySet();

    //
    //  deschedule no longer required events
    //

    for (TimedEvaluation scheduledEvaluation : previouslyScheduledEvaluations)
      {
        if (! subscriberState.getScheduledEvaluations().contains(scheduledEvaluation))
          {
            timerService.deschedule(scheduledEvaluation);
          }
      }

    //
    //  schedule new events
    //

    for (TimedEvaluation scheduledEvaluation : subscriberState.getScheduledEvaluations())
      {
        if (! previouslyScheduledEvaluations.contains(scheduledEvaluation))
          {
            timerService.schedule(scheduledEvaluation);
          }
      }

    /*****************************************
    *
    *  evolutionSubscriberStatusUpdated
    *
    *****************************************/

    if (currentSubscriberState == null || subscriberProfile.getEvolutionSubscriberStatus() != currentSubscriberState.getSubscriberProfile().getEvolutionSubscriberStatus())
      {
        subscriberState.setEvolutionSubscriberStatusUpdated(true);
        subscriberStateUpdated = true;
      }

    /*****************************************
    *
    *  subscriberTrace
    *
    *****************************************/

    if (subscriberProfile.getSubscriberTraceEnabled())
      {
        subscriberState.setSubscriberTrace(new SubscriberTrace(generateSubscriberTraceMessage(evolutionEvent, currentSubscriberState, subscriberState, context.getSubscriberTraceDetails())));
        subscriberStateUpdated = true;
      }

    /****************************************
    *
    *  return
    *
    ****************************************/

    return subscriberStateUpdated ? subscriberState : currentSubscriberState;
  }

  /*****************************************
  *
  *  updateSubscriberProfile
  *
  *****************************************/

  private static boolean updateSubscriberProfile(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent)
  {
    /*****************************************
    *
    *  result
    *
    *****************************************/

    SubscriberProfile subscriberProfile = context.getSubscriberState().getSubscriberProfile();
    boolean subscriberProfileUpdated = false;

    /*****************************************
    *
    *  process subscriberTraceControl
    *
    *****************************************/

    if (evolutionEvent instanceof SubscriberTraceControl)
      {
        SubscriberTraceControl subscriberTraceControl = (SubscriberTraceControl) evolutionEvent;
        subscriberProfile.setSubscriberTraceEnabled(subscriberTraceControl.getSubscriberTraceEnabled());
        subscriberProfileUpdated = true;
      }
    
    /*****************************************
    *
    *  invoke evolution engine extension
    *
    *****************************************/

    try
      {
        subscriberProfileUpdated = ((Boolean) evolutionEngineExtensionUpdateSubscriberMethod.invoke(null, context, evolutionEvent)).booleanValue() || subscriberProfileUpdated;
      }
    catch (IllegalAccessException|InvocationTargetException e)
      {
        throw new RuntimeException(e);
      }
    
    /*****************************************
    *
    *  process subscriberGroup
    *
    *****************************************/

    if (evolutionEvent instanceof SubscriberGroup)
      {
        //
        //  apply
        //
        
        SubscriberGroup subscriberGroup = (SubscriberGroup) evolutionEvent;
        subscriberProfile.setSubscriberGroup(subscriberGroup.getGroupName(), subscriberGroup.getEpoch(), subscriberGroup.getAddSubscriber());
        subscriberProfileUpdated = true;
      }

    /*****************************************
    *
    *  re-evaluate subscriberGroups for epoch changes and segmentation rules
    *
    *****************************************/

    for (SegmentationRule segmentationRule :  segmentationRuleService.getActiveSegmentationRules(evolutionEvent.getEventDate()))
      {
        //
        //  ignore if in temporal hole (segmentation rule has been activated but subscriberGroupEpochReader has not seen it yet)
        //

        if (subscriberGroupEpochReader.get(segmentationRule.getSubscriberGroupName()) != null)
          {
            SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, evolutionEvent.getEventDate());
            boolean inGroup = EvaluationCriterion.evaluateCriteria(evaluationRequest, segmentationRule.getSegmentationRuleCriteria());
            subscriberProfile.setSubscriberGroup(segmentationRule.getSubscriberGroupName(), subscriberGroupEpochReader.get(segmentationRule.getSubscriberGroupName()).getEpoch(), inGroup);
            subscriberProfileUpdated = true;
          }
      }

    /*****************************************
    *
    *  statistics
    *
    *****************************************/

    updateEvolutionEngineStatistics(evolutionEvent);
    
    /*****************************************
    *
    *  return
    *
    *****************************************/

    return subscriberProfileUpdated;
  }

  /*****************************************
  *
  *  updateJourneys
  *
  *****************************************/

  private static boolean updateJourneys(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent)
  {
    /*****************************************
    *
    *  result
    *
    *****************************************/

    SubscriberState subscriberState = context.getSubscriberState();
    boolean subscriberStateUpdated = false;

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = context.now();

    /*****************************************
    *
    *  update JourneyState(s) to enter new autoTargetedJourneys
    *
    *****************************************/

    for (Journey journey : journeyService.getActiveJourneys(now))
      {
        if (journey.getAutoTargeted())
          {
            /*****************************************
            *
            *  enterJourney
            *
            *****************************************/

            boolean enterJourney = true;

            /*****************************************
            *
            *  already in journey?
            *
            *****************************************/

            if (enterJourney)
              {
                for (JourneyState journeyState : subscriberState.getJourneyStates())
                  {
                    if (Objects.equals(journeyState.getJourneyID(), journey.getJourneyID()))
                      {
                        enterJourney = false;
                      }
                  }
              }

            /*****************************************
            *
            *  recently in journey?
            *
            *****************************************/

            if (enterJourney)
              {
                for (JourneyState journeyState : subscriberState.getRecentJourneyStates())
                  {
                    if (Objects.equals(journeyState.getJourneyID(), journey.getJourneyID()))
                      {
                        Date journeyReentryWindow = EvolutionUtilities.addTime(journeyState.getJourneyExitDate(), journey.getAutoTargetedWindowDuration(), journey.getAutoTargetedWindowUnit(), Deployment.getBaseTimeZone(), journey.getAutoTargetedWindowRoundUp());
                        if (journeyReentryWindow.after(now))
                          {
                            enterJourney = false;
                          }
                      }
                  }
              }

            /*****************************************
            *
            *  pass auto-targeting criteria
            *
            *****************************************/

            if (enterJourney)
              {
                SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, now);
                if (! EvaluationCriterion.evaluateCriteria(evaluationRequest, journey.getAutoTargetingCriteria()))
                  {
                    enterJourney = false;
                  }
                context.getSubscriberTraceDetails().addAll(evaluationRequest.getTraceDetails());
              }

            /*****************************************
            *
            *  enterJourney
            *
            *****************************************/

            if (enterJourney)
              {
                JourneyState journeyState = new JourneyState(context, journey, Collections.<String,Object>emptyMap(), now);
                subscriberState.getJourneyStates().add(journeyState);
                subscriberState.getJourneyStatistics().add(new JourneyStatistic(subscriberState.getSubscriberID(), journeyState));
                subscriberStateUpdated = true;
              }
          }
      }
        
    /*****************************************
    *
    *  update JourneyState(s) for all current journeys
    *
    *****************************************/

    //
    //  update
    //

    List<JourneyState> inactiveJourneyStates = new ArrayList<JourneyState>();
    for (JourneyState journeyState : subscriberState.getJourneyStates())
      {
        /*****************************************
        *
        *  get journey and journeyNode
        *
        *****************************************/

        Journey journey = journeyService.getActiveJourney(journeyState.getJourneyID(), now);
        JourneyNode journeyNode = (journey != null) ? journey.getJourneyNodes().get(journeyState.getJourneyNodeID()) : null;

        /*****************************************
        *
        *  inactive journey  
        *
        *****************************************/

        if (journey == null || journeyNode == null)
          {
            journeyState.setJourneyExitDate(now);
            subscriberState.getJourneyStatistics().add(new JourneyStatistic(subscriberState.getSubscriberID(), journeyState, now));
            inactiveJourneyStates.add(journeyState);
            break;
          }

        /*****************************************
        *
        *  transition(s)
        *
        *****************************************/

        JourneyLink firedLink = null;
        do
          {
            /*****************************************
            *
            *  transition?
            *
            *****************************************/

            SortedSet<Date> nextEvaluationDates = new TreeSet<Date>();
            firedLink = null;
            for (JourneyLink journeyLink : journeyNode.getOutgoingLinks().values())
              {
                //
                //  evaluationRequest (including link)
                //

                SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, journeyState, journeyNode, journeyLink, evolutionEvent, now);

                //
                //  evaluate
                //

                if (EvaluationCriterion.evaluateCriteria(evaluationRequest, journeyLink.getTransitionCriteria()))
                  {
                    firedLink = journeyLink;
                  }

                //
                //  store data on evaluationRequest
                //

                nextEvaluationDates.addAll(evaluationRequest.getNextEvaluationDates());
                context.getSubscriberTraceDetails().addAll(evaluationRequest.getTraceDetails());
                
                //
                //  break if this link has fired
                //

                if (firedLink != null)
                  {
                    break;
                  }
              }

            /*****************************************
            *
            *  stay in node (schedule evaluations as necessary)
            *
            *****************************************/

            if (firedLink == null)
              {
                for (Date nextEvaluationDate : nextEvaluationDates)
                  {
                    subscriberState.getScheduledEvaluations().add(new TimedEvaluation(subscriberState.getSubscriberID(), nextEvaluationDate));
                    subscriberStateUpdated = true;
                  }
              }

            /*****************************************
            *
            *  enter new node
            *
            *****************************************/

            if (firedLink != null)
              {
                /*****************************************
                *
                *  exit node action
                *
                *****************************************/

                if (journeyNode.getNodeType().getActionManager() != null)
                  {
                    SubscriberEvaluationRequest exitActionEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, journeyState, journeyNode, firedLink, evolutionEvent, now);
                    journeyNode.getNodeType().getActionManager().executeOnExit(context, exitActionEvaluationRequest, firedLink);
                    context.getSubscriberTraceDetails().addAll(exitActionEvaluationRequest.getTraceDetails());
                  }

                /*****************************************
                *
                *  enter node
                *
                *****************************************/

                JourneyNode nextJourneyNode = firedLink.getDestination();
                journeyState.setJourneyNodeID(nextJourneyNode.getNodeID(), now);
                subscriberState.getJourneyStatistics().add(new JourneyStatistic(subscriberState.getSubscriberID(), journeyState, firedLink));
                journeyNode = nextJourneyNode;
                subscriberStateUpdated = true;

                /*****************************************
                *
                *  enter node action
                *
                *****************************************/

                if (journeyNode.getNodeType().getActionManager() != null)
                  {
                    //
                    //  action
                    //

                    SubscriberEvaluationRequest entryActionEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, journeyState, journeyNode, firedLink, evolutionEvent, now);
                    DeliveryRequest deliveryRequest = journeyNode.getNodeType().getActionManager().executeOnEntry(context, entryActionEvaluationRequest);
                    context.getSubscriberTraceDetails().addAll(entryActionEvaluationRequest.getTraceDetails());

                    //
                    //  forward deliveryRequest (if necessary)
                    //

                    if (deliveryRequest != null)
                      {
                        subscriberState.getDeliveryRequests().add(deliveryRequest);
                        journeyState.setJourneyOutstandingDeliveryRequestID(deliveryRequest.getDeliveryRequestID());
                      }
                  }

                /*****************************************
                *
                *  exit (if exit node)
                *
                *****************************************/

                if (journeyNode.getExitNode())
                  {
                    journeyState.setJourneyExitDate(now);
                    inactiveJourneyStates.add(journeyState);
                  }
              }
            
            /*****************************************
            *
            *  subscriberTrace
            *
            *****************************************/


          }
        while (firedLink != null && journeyState.getJourneyExitDate() == null);
      }

    //
    //  remove inactive journeyStates
    //

    for (JourneyState journeyState : inactiveJourneyStates)
      {
        subscriberState.getRecentJourneyStates().add(journeyState);
        subscriberState.getJourneyStates().remove(journeyState);
        subscriberStateUpdated = true;
      }
    
    /*****************************************
    *
    *  return
    *
    *****************************************/

    return subscriberStateUpdated;
  }

  /*****************************************
  *
  *  nullSubscriberHistory
  *
  ****************************************/

  public static SubscriberHistory nullSubscriberHistory() { return (SubscriberHistory) null; }

  /*****************************************
  *
  *  updateSubscriberHistory
  *
  *****************************************/

  public static SubscriberHistory updateSubscriberHistory(StringKey aggKey, SubscriberStreamEvent evolutionEvent, SubscriberHistory currentSubscriberHistory)
  {
    /****************************************
    *
    *  get (or create) entry
    *
    ****************************************/

    SubscriberHistory subscriberHistory = (currentSubscriberHistory != null) ? new SubscriberHistory(currentSubscriberHistory) : new SubscriberHistory(evolutionEvent.getSubscriberID());
    boolean subscriberHistoryUpdated = (currentSubscriberHistory != null) ? false : true;

    /*****************************************
    *
    *  deliveryRequest
    *
    *****************************************/

    DeliveryRequest deliveryRequest = (DeliveryRequest) evolutionEvent;

    /*****************************************
    *
    *  clear older history
    *
    *****************************************/

    //
    //  TBD DEW
    //

    /*****************************************
    *
    *  add to history
    *
    *****************************************/

    //
    //  find sorted location to insert
    //

    int i = 0;
    while (i < subscriberHistory.getDeliveryRequests().size() && subscriberHistory.getDeliveryRequests().get(i).getDeliveryDate().compareTo(deliveryRequest.getDeliveryDate()) <= 0)
      {
        i += 1;
      }

    //
    //  insert
    //

    subscriberHistory.getDeliveryRequests().add(i, deliveryRequest);
    subscriberHistoryUpdated = true;

    /****************************************
    *
    *  return
    *
    ****************************************/

    return subscriberHistoryUpdated ? subscriberHistory : currentSubscriberHistory;
  }

  /*****************************************
  *
  *  updateEvolutionEngineStatistics
  *
  *****************************************/

  private static void updateEvolutionEngineStatistics(SubscriberStreamEvent event)
  {
    evolutionEngineStatistics.updateEventProcessedCount(1);
    evolutionEngineStatistics.updateEventCount(event, 1);
  }

  /*****************************************
  *
  *  getSubscriberState
  *
  *****************************************/

  private static SubscriberState getSubscriberState(SubscriberStreamEvent evolutionEvent, SubscriberState subscriberState)
  {
    return subscriberState;
  }

  /****************************************
  *
  *  getEvolutionEngineOutputs
  *
  ****************************************/

  private static List<SubscriberStreamOutput> getEvolutionEngineOutputs(SubscriberState subscriberState)
  {
    List<SubscriberStreamOutput> result = new ArrayList<SubscriberStreamOutput>();
    result.addAll(subscriberState.getEvolutionSubscriberStatusUpdated() ? Collections.<SubscriberProfile>singletonList(subscriberState.getSubscriberProfile()) : Collections.<SubscriberProfile>emptyList());
    result.addAll(subscriberState.getDeliveryRequests());
    result.addAll(subscriberState.getJourneyStatistics());
    result.addAll((subscriberState.getSubscriberTrace() != null) ? Collections.<SubscriberTrace>singletonList(subscriberState.getSubscriberTrace()) : Collections.<SubscriberTrace>emptyList());
    return result;
  }
  
  /****************************************
  *
  *  rekeyDeliveryRequestStream
  *
  ****************************************/

  private static KeyValue<StringKey, DeliveryRequest> rekeyDeliveryRequestStream(StringKey key, DeliveryRequest value)
  {
    return new KeyValue<StringKey, DeliveryRequest>(new StringKey(value.getDeliveryRequestID()), value);
  }
  
  /*****************************************
  *
  *  generateSubscriberTraceMessage
  *
  *****************************************/

  private static String generateSubscriberTraceMessage(SubscriberStreamEvent evolutionEvent, SubscriberState currentSubscriberState, SubscriberState subscriberState, List<String> subscriberTraceDetails)
  {
    /*****************************************
    *
    *  convert to JSON
    *
    *****************************************/

    //
    //  initialize converter
    //

    JsonConverter converter = new JsonConverter();
    Map<String, Object> converterConfigs = new HashMap<String, Object>();
    converterConfigs.put("schemas.enable","false");
    converter.configure(converterConfigs, false);
    JsonDeserializer deserializer = new JsonDeserializer();
    deserializer.configure(Collections.<String, Object>emptyMap(), false);

    //
    //  JsonNodeFactory
    //

    JsonNodeFactory jsonNodeFactory = new JsonNodeFactory(false);

    //
    //  field -- evolutionEvent
    //

    JsonNode evolutionEventNode = deserializer.deserialize(null, converter.fromConnectData(null, evolutionEvent.subscriberStreamEventSchema(),  evolutionEvent.subscriberStreamEventPack(evolutionEvent)));

    //
    //  field -- currentSubscriberState
    //

    JsonNode currentSubscriberStateNode = (currentSubscriberState != null) ? deserializer.deserialize(null, converter.fromConnectData(null, SubscriberState.schema(),  SubscriberState.pack(currentSubscriberState))) : null;

    //
    //  field -- subscriberState
    //

    JsonNode subscriberStateNode = deserializer.deserialize(null, converter.fromConnectData(null, SubscriberState.schema(),  SubscriberState.pack(subscriberState)));

    /*****************************************
    *
    *  hack/massage triggerStateNodes to remove unwanted/misleading/spurious fields from currentTriggerState
    *
    *****************************************/

    //
    //  outgoing messages (currentSubscriberStateNode)
    //

    if (currentSubscriberStateNode != null) ((ObjectNode) currentSubscriberStateNode).remove("subscriberTraceMessage");

    //
    //  other (subscriberStateNode)
    //

    ((ObjectNode) subscriberStateNode).remove("subscriberTraceMessage");

    /*****************************************
    *
    *  subscriberTraceDetails
    *
    *****************************************/

    ArrayNode subscriberTraceDetailsNode = jsonNodeFactory.arrayNode();
    for (String detail : subscriberTraceDetails)
      {
        subscriberTraceDetailsNode.add(detail);
      }

    /*****************************************
    *
    *  subscriberTraceMessage
    *
    *****************************************/

    //
    //  create parent JsonNode (to package fields)
    //

    ObjectNode subscriberTraceMessageNode = jsonNodeFactory.objectNode();
    subscriberTraceMessageNode.put("source", "EvolutionEngine");
    subscriberTraceMessageNode.put("subscriberID", evolutionEvent.getSubscriberID());
    subscriberTraceMessageNode.put("processingTime", SystemTime.getCurrentTime().getTime());
    subscriberTraceMessageNode.set(evolutionEvent.getClass().getSimpleName(), evolutionEventNode);
    subscriberTraceMessageNode.set("currentSubscriberState", currentSubscriberStateNode);
    subscriberTraceMessageNode.set("subscriberState", subscriberStateNode);
    subscriberTraceMessageNode.set("details", subscriberTraceDetailsNode);

    //
    //  convert to string
    //

    return subscriberTraceMessageNode.toString();
  }

  /*****************************************
  *
  *  class EvolutionEventContext
  *
  *****************************************/

  public static class EvolutionEventContext
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private SubscriberState subscriberState;
    private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
    private KStreamsUniqueKeyServer uniqueKeyServer;
    private Date now;
    private List<String> subscriberTraceDetails;
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public EvolutionEventContext(SubscriberState subscriberState, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, KStreamsUniqueKeyServer uniqueKeyServer, Date now)
    {
      this.subscriberState = subscriberState;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
      this.uniqueKeyServer = uniqueKeyServer;
      this.now = now;
      this.subscriberTraceDetails = new ArrayList<String>();
    }

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public SubscriberState getSubscriberState() { return subscriberState; }
    public ReferenceDataReader<String,SubscriberGroupEpoch> getSubscriberGroupEpochReader() { return subscriberGroupEpochReader; }
    public KStreamsUniqueKeyServer getUniqueKeyServer() { return uniqueKeyServer; }
    public List<String> getSubscriberTraceDetails() { return subscriberTraceDetails; }
    public Date now() { return now; }
    public boolean getSubscriberTraceEnabled() { return subscriberState.getSubscriberProfile().getSubscriberTraceEnabled(); }

    /*****************************************
    *
    *  getUniqueKey
    *
    *****************************************/

    public String getUniqueKey()
    {
      return String.format("%020d", uniqueKeyServer.getKey());
    }

    /*****************************************
    *
    *  subscriberTrace
    *
    *****************************************/

    public void subscriberTrace(String messageFormatString, Object... args)
    {
      if (getSubscriberTraceEnabled())
        {
          subscriberTraceDetails.add(MessageFormat.format(messageFormatString, args));
        }
    }
  }

  /*****************************************
  *
  *  class EvolutionEventTimestampExtractor
  *
  *****************************************/

  public static class EvolutionEventTimestampExtractor implements TimestampExtractor
  {
    /*****************************************
    *
    *  extract
    *
    *****************************************/

    @Override public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp)
    {
      long result = record.timestamp();

      //
      //  SubscriberStreamEvent
      //

      result = (record.value() instanceof SubscriberStreamEvent) ? ((SubscriberStreamEvent) record.value()).getEventDate().getTime() : result;

      //
      //  return (protect against time before the epoch)
      //

      return (result > 0L) ? result : record.timestamp();
    }
  }

  /*****************************************
  *
  *  class APIHandler
  *
  *****************************************/

  private class APIHandler implements HttpHandler
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private API api;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    private APIHandler(API api)
    {
      this.api = api;
    }

    /*****************************************
    *
    *  handle -- HttpHandler
    *
    *****************************************/

    public void handle(HttpExchange exchange) throws IOException
    {
      handleAPI(api, exchange);
    }
  }

  /*****************************************
  *
  *  handleAPI
  *
  *****************************************/

  private void handleAPI(API api, HttpExchange exchange) throws IOException
  {
    try
      {
        /*****************************************
        *
        *  get the body
        *
        *****************************************/

        StringBuilder requestBodyStringBuilder = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()));
        while (true)
          {
            String line = reader.readLine();
            if (line == null) break;
            requestBodyStringBuilder.append(line);
          }
        reader.close();
        log.debug("API (raw request): {} {}",api,requestBodyStringBuilder.toString());
        JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(requestBodyStringBuilder.toString());

        /*****************************************
        *
        *  validate
        *
        *****************************************/

        int apiVersion = JSONUtilities.decodeInteger(jsonRoot, "apiVersion", true);
        if (apiVersion > RESTAPIVersion)
          {
            throw new ServerRuntimeException("unknown api version " + apiVersion);
          }

        /*****************************************
        *
        *  arguments
        *
        *****************************************/

        String subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
        boolean includeHistory = JSONUtilities.decodeBoolean(jsonRoot, "includeHistory", Boolean.FALSE);

        /*****************************************
        *
        *  process
        *
        *****************************************/

        byte[] apiResponse = null;
        switch (api)
          {
            case getSubscriberProfile:
              apiResponse = processGetSubscriberProfile(subscriberID, includeHistory);
              break;

            case retrieveSubscriberProfile:
              apiResponse = processRetrieveSubscriberProfile(subscriberID, includeHistory);
              break;
          }

        //
        //  validate
        //

        if (apiResponse == null)
          {
            throw new ServerException("no handler for " + api);
          }

        /*****************************************
        *
        *  send response
        *
        *****************************************/

        //
        //  log
        //

        if (log.isDebugEnabled())
          {
            if (apiResponse.length > 0)
              {
                byte[] encodedSubscriberProfile = apiResponse;
                SubscriberProfile result = SubscriberProfile.getSubscriberProfileSerde().deserializer().deserialize(Deployment.getSubscriberProfileRegistrySubject(), encodedSubscriberProfile);
                log.debug("API (response): {}", result.toString(subscriberGroupEpochReader));
              }
            else
              {
                log.debug("API (response): {} not found", subscriberID);
              }
          }

        //
        //  responseCode
        //

        byte responseCode = (apiResponse.length > 0) ? (byte) 0x00 : (byte) 0x01;

        //
        //  apiResponseHeader
        //

        ByteBuffer apiResponseHeader = ByteBuffer.allocate(6);
        apiResponseHeader.put((byte) 0x01);             // version:  1
        apiResponseHeader.put(responseCode);            // return code: found (0) or notFound (1)
        apiResponseHeader.putInt(apiResponse.length);

        //
        //  response 
        //

        ByteBuffer response = ByteBuffer.allocate(apiResponseHeader.array().length + apiResponse.length);
        response.put(apiResponseHeader.array());
        response.put(apiResponse);

        //
        //  send
        //

        exchange.sendResponseHeaders(200, response.array().length);
        exchange.getResponseBody().write(response.array());
        exchange.close();
      }
    catch (org.json.simple.parser.ParseException | IOException | ServerException | RuntimeException e )
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  apiResponseHeader
        //

        ByteBuffer apiResponseHeader = ByteBuffer.allocate(6);
        apiResponseHeader.put((byte) 0x01);     // version:  1
        apiResponseHeader.put((byte) 0x02);     // return code: system error (2)
        apiResponseHeader.putInt(0);     

        //
        //  send
        //

        exchange.sendResponseHeaders(200, apiResponseHeader.array().length);
        exchange.getResponseBody().write(apiResponseHeader.array());
        exchange.close();
      }
  }

  /*****************************************
  *
  *  processGetSubscriberProfile
  *
  *****************************************/

  private byte[] processGetSubscriberProfile(String subscriberID, boolean includeHistory) throws ServerException
  {
    //
    //  timeout
    //

    Date now = SystemTime.getCurrentTime();
    Date timeout = RLMDateUtils.addSeconds(now, 10);

    //
    //  wait for streams
    //

    waitForStreams(timeout);

    //
    //  handler for subscriberID
    //

    StreamsMetadata metadata = streams.metadataForKey(Deployment.getSubscriberStateChangeLog(), new StringKey(subscriberID), StringKey.serde().serializer());

    //
    //  request
    //

    HashMap<String,Object> request = new HashMap<String,Object>();
    request.put("apiVersion", 1);
    request.put("subscriberID", subscriberID);
    request.put("includeHistory", includeHistory);
    JSONObject requestJSON = JSONUtilities.encodeObject(request);

    //
    //  httpPost
    //

    now = SystemTime.getCurrentTime();
    long waitTime = timeout.getTime() - now.getTime();
    HttpPost httpPost = new HttpPost("http://" + metadata.host() + ":" + metadata.port() + "/nglm-evolutionengine/retrieveSubscriberProfile");
    httpPost.setEntity(new StringEntity(requestJSON.toString(), ContentType.create("application/json")));
    httpPost.setConfig(RequestConfig.custom().setConnectTimeout((int) (waitTime > 0 ? waitTime : 1)).build());

    //
    //  submit
    //


    HttpResponse httpResponse = null;
    try
      {
        if (waitTime > 0) httpResponse = httpClient.execute(httpPost);
      }
    catch (IOException e)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception processing REST api: {}", stackTraceWriter.toString());
      }

    //
    //  success?
    //

    if (httpResponse == null || httpResponse.getStatusLine() == null || httpResponse.getStatusLine().getStatusCode() != 200 || httpResponse.getEntity() == null)
      {
        log.info("retrieveSubscriberProfile failed: {}", httpResponse);
        throw new ServerException("retrieveSubscriberProfile failed");
      }

    //
    //  read response
    //

    int responseCode;
    byte[] responseBody;
    InputStream responseStream = null;
    try
      {
        //
        //  stream
        //

        responseStream = httpResponse.getEntity().getContent();

        //
        //  header
        //

        byte[] rawResponseHeader = new byte[6];
        int totalBytesRead = 0;
        while (totalBytesRead < 6)
          {
            int bytesRead = responseStream.read(rawResponseHeader, totalBytesRead, 6-totalBytesRead);
            if (bytesRead == -1) break;
            totalBytesRead += bytesRead;
          }
        if (totalBytesRead < 6) throw new ServerException("retrieveSubscriberProfile failed (bad header)");
      
        //
        //  parse response header
        //

        ByteBuffer apiResponseHeader = ByteBuffer.allocate(6);
        apiResponseHeader.put(rawResponseHeader);
        int version = apiResponseHeader.get(0);
        responseCode = apiResponseHeader.get(1);
        int responseBodyLength = (responseCode == 0) ? apiResponseHeader.getInt(2) : 0;

        //
        //  body
        //

        responseBody = new byte[responseBodyLength];
        totalBytesRead = 0;
        while (totalBytesRead < responseBodyLength)
          {
            int bytesRead = responseStream.read(responseBody, totalBytesRead, responseBodyLength-totalBytesRead);
            if (bytesRead == -1) break;
            totalBytesRead += bytesRead;
          }
        if (totalBytesRead < responseBodyLength) throw new ServerException("retrieveSubscriberProfile failed (bad body)");
      }
    catch (IOException e)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  error
        //

        responseCode = 2;
        responseBody = null;
      }
    finally
      {
        if (responseStream != null) try { responseStream.close(); } catch (IOException e) { }
      }
    
    //
    //  result
    //

    byte[] result = null;
    switch (responseCode)
      {
        case 0:
        case 1:
          result = responseBody;
          break;

        default:
          throw new ServerException("retrieveSubscriberProfile response code: " + responseCode);
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  processRetrieveSubscriberProfile
  *
  *****************************************/

  private byte[] processRetrieveSubscriberProfile(String subscriberID, boolean includeHistory) throws ServerException
  {
    /*****************************************
    *
    *  subscriberProfile
    *
    *****************************************/

    SubscriberProfile subscriberProfile = null;

    /*****************************************
    *
    *  retrieve subscriberProfile from local store
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    Date timeout = RLMDateUtils.addSeconds(now, 10);
    ServerException retainedException = null;
    boolean stateStoreCallCompleted = false;
    while (! stateStoreCallCompleted && now.before(timeout))
      {
        //
        //  wait for streams
        //

        waitForStreams(timeout);

        //
        //  query
        //

        try
          {
            SubscriberState subscriberState = subscriberStateStore.get(new StringKey(subscriberID));
            if (subscriberState != null) subscriberProfile = subscriberState.getSubscriberProfile();
            stateStoreCallCompleted = true;
          }
        catch (InvalidStateStoreException e)
          {
            retainedException = new ServerException(e);
          }

        //
        //  now
        //

        now = SystemTime.getCurrentTime();
      }

    //
    //  timeout
    //

    if (! stateStoreCallCompleted)
      {
        throw retainedException;
      }

    /*****************************************
    *
    *  retrieve subscriberHistory from local store (if necessary)
    *
    *****************************************/

    if (subscriberProfile != null && includeHistory)
      {
        SubscriberHistory subscriberHistory = null;
        now = SystemTime.getCurrentTime();
        retainedException = null;
        stateStoreCallCompleted = false;
        while (! stateStoreCallCompleted && now.before(timeout))
          {
            //
            //  wait for streams
            //

            waitForStreams(timeout);

            //
            //  query
            //

            try
              {
                subscriberHistory = subscriberHistoryStore.get(new StringKey(subscriberID));
                stateStoreCallCompleted = true;
              }
            catch (InvalidStateStoreException e)
              {
                retainedException = new ServerException(e);
              }

            //
            //  now
            //

            now = SystemTime.getCurrentTime();
          }

        //
        //  timeout
        //

        if (! stateStoreCallCompleted)
          {
            throw retainedException;
          }

        //
        //  add subscriberHistory
        //

        if (subscriberHistory != null)
          {
            subscriberProfile = subscriberProfile.copy();
            subscriberProfile.setSubscriberHistory(subscriberHistory);
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    byte[] result;
    if (subscriberProfile != null)
      result = SubscriberProfile.getSubscriberProfileSerde().serializer().serialize(Deployment.getSubscriberProfileRegistrySubject(), subscriberProfile);
    else
      result = new byte[0];

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return result;
  }
}
