/****************************************************************************
*
*  EvolutionEngine.java
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
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.DeliveryRequest.DeliveryPriority;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvolutionUtilities.RoundingSelection;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.Expression.ExpressionEvaluationException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.MetricHistory.BucketRepresentation;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatusField;
import com.evolving.nglm.evolution.JourneyHistory.RewardHistory;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramOperation;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;
import com.evolving.nglm.evolution.PointFulfillmentRequest.PointOperation;
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
  private static ReferenceDataReader<String,UCGState> ucgStateReader;
  private static JourneyService journeyService;
  private static LoyaltyProgramService loyaltyProgramService;
  private static TargetService targetService;
  private static JourneyObjectiveService journeyObjectiveService;
  private static SegmentationDimensionService segmentationDimensionService;
  private static TokenTypeService tokenTypeService;
  private static SubscriberMessageTemplateService subscriberMessageTemplateService;
  private static DeliverableService deliverableService;
  private static SegmentContactPolicyService segmentContactPolicyService;
  private static EvolutionEngineStatistics evolutionEngineStatistics;
  private static KStreamsUniqueKeyServer uniqueKeyServer = new KStreamsUniqueKeyServer();
  private static Method evolutionEngineExtensionUpdateSubscriberMethod;
  private static Method evolutionEngineExtensionUpdateExtendedSubscriberMethod;
  private static Method evolutionEngineExternalAPIMethod;
  private static TimerService timerService;
  private static PointService pointService;
  private static KafkaStreams streams = null;
  private static ReadOnlyKeyValueStore<StringKey, SubscriberState> subscriberStateStore = null;
  private static ReadOnlyKeyValueStore<StringKey, ExtendedSubscriberProfile> extendedSubscriberProfileStore = null;
  private static ReadOnlyKeyValueStore<StringKey, SubscriberHistory> subscriberHistoryStore = null;
  private static TokenType externalTokenType = null;
  private static final String externalTokenTypeID = "external";
  private static final int RESTAPIVersion = 1;
  private static HttpServer subscriberProfileServer;
  private static HttpServer internalServer;
  private static HttpClient httpClient;
  private static ExclusionInclusionTargetService exclusionInclusionTargetService;

  /****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    NGLMRuntime.initialize(true);
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
    String subscriberProfileForceUpdateTopic = Deployment.getSubscriberProfileForceUpdateTopic();
    String profileChangeEventTopic = Deployment.getProfileChangeEventTopic();
    String profileSegmentChangeEventTopic = Deployment.getProfileSegmentChangeEventTopic();
    String profileLoyaltyProgramChangeEventTopic = Deployment.getProfileLoyaltyProgramChangeEventTopic();
    String journeyRequestTopic = Deployment.getJourneyRequestTopic();
    String journeyStatisticTopic = Deployment.getJourneyStatisticTopic();
    String journeyMetricTopic = Deployment.getJourneyMetricTopic();
    String loyaltyProgramRequestTopic = Deployment.getLoyaltyProgramRequestTopic();
    String recordSubscriberIDTopic = Deployment.getRecordSubscriberIDTopic();
    String subscriberGroupTopic = Deployment.getSubscriberGroupTopic();
    String subscriberTraceControlTopic = Deployment.getSubscriberTraceControlTopic();
    String presentationLogTopic = Deployment.getPresentationLogTopic();
    String acceptanceLogTopic = Deployment.getAcceptanceLogTopic();
    String pointFulfillmentRequestTopic = Deployment.getPointFulfillmentRequestTopic();

    //
    //  sink topics
    //

    String subscriberTraceTopic = Deployment.getSubscriberTraceTopic();
    String propensityLogTopic = Deployment.getPropensityLogTopic();
    String pointFulfillmentResponseTopic = Deployment.getPointFulfillmentResponseTopic();
    String journeyResponseTopic = Deployment.getJourneyResponseTopic();
    String loyaltyProgramResponseTopic = Deployment.getLoyaltyProgramResponseTopic();

    //
    //  changelogs
    //

    String subscriberStateChangeLog = Deployment.getSubscriberStateChangeLog();
    String extendedSubscriberProfileChangeLog = Deployment.getExtendedSubscriberProfileChangeLog();
    String subscriberHistoryChangeLog = Deployment.getSubscriberHistoryChangeLog();
    String propensityStateChangeLog = Deployment.getPropensityStateChangeLog();
    String journeyTrafficChangeLog = Deployment.getJourneyTrafficChangeLog();

    //
    // Internal repartitioning topic (when rekeyed)
    //

    String pointFufillmentRepartitioningTopic = Deployment.getPointFufillmentRepartitioningTopic();
    String propensityRepartitioningTopic = Deployment.getPropensityRepartitioningTopic();

    //
    //  (force load of SubscriberProfile class)
    //

    SubscriberProfile.getSubscriberProfileSerde();

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
    //  loyaltyProgramService
    // 
    
    loyaltyProgramService = new LoyaltyProgramService(bootstrapServers, "evolutionengine-loyaltyProgramService-" + evolutionEngineKey, Deployment.getLoyaltyProgramTopic(), false);
    loyaltyProgramService.start();
    
    //
    //  targetService
    //

    targetService = new TargetService(bootstrapServers, "evolutionengine-targetservice-" + evolutionEngineKey, Deployment.getTargetTopic(), false);
    targetService.start();

    //
    //  journeyObjectiveService
    //

    journeyObjectiveService = new JourneyObjectiveService(bootstrapServers, "evolutionengine-journeyobjectiveservice-" + evolutionEngineKey, Deployment.getJourneyObjectiveTopic(), false);
    journeyObjectiveService.start();

    //
    //  timerService (DO NOT START until streams is started)
    //

    timerService = new TimerService(this, bootstrapServers);

    //
    //  segmentationDimensionService
    //

    segmentationDimensionService = new SegmentationDimensionService(bootstrapServers, "evolutionengine-segmentationdimensionservice-" + evolutionEngineKey, Deployment.getSegmentationDimensionTopic(), false);
    segmentationDimensionService.start();

    //
    //  tokenTypeService
    //

    tokenTypeService = new TokenTypeService(bootstrapServers, "evolutionengine-tokentypeservice-" + evolutionEngineKey, Deployment.getTokenTypeTopic(), false);
    tokenTypeService.start();

    //
    //  subscriberMessageTemplateService
    //

    subscriberMessageTemplateService = new SubscriberMessageTemplateService(bootstrapServers, "evolutionengine-subscribermessagetemplateservice-" + evolutionEngineKey, Deployment.getSubscriberMessageTemplateTopic(), false);
    subscriberMessageTemplateService.start();

    //
    //  deliverableService
    //

    deliverableService = new DeliverableService(bootstrapServers, "evolutionengine-deliverableservice-" + evolutionEngineKey, Deployment.getDeliverableTopic(), false);
    deliverableService.start();

    //
    //  segmentContactPolicyService
    //

    segmentContactPolicyService = new SegmentContactPolicyService(bootstrapServers, "evolutionengine-segmentcontactpolicyservice-" + evolutionEngineKey, Deployment.getSegmentContactPolicyTopic(), false);
    segmentContactPolicyService.start();

    //
    // pointService
    //
    
    pointService = new PointService(bootstrapServers, "evolutionengine-pointservice-" + evolutionEngineKey, Deployment.getPointTopic(), false);
    pointService.start();
    
    //
    // exclusionInclusionTargetService
    //
    
    exclusionInclusionTargetService = new ExclusionInclusionTargetService(bootstrapServers, "evolutionengine-exclusioninclusiontargetservice-" + evolutionEngineKey, Deployment.getExclusionInclusionTargetTopic(), false);
    exclusionInclusionTargetService.start();
    
    //
    //  subscriberGroupEpochReader
    //

    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("evolutionengine-subscribergroupepoch", evolutionEngineKey, Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);

    //
    //  ucgStateReader
    //

    ucgStateReader = ReferenceDataReader.<String,UCGState>startReader("evolutionengine-ucgstate", evolutionEngineKey, Deployment.getBrokerServers(), Deployment.getUCGStateTopic(), UCGState::unpack);

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
        throw new ServerRuntimeException(e);
      }

    //
    //  journeyMetrics
    //

    try
      {
        for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values())
          {
            journeyMetricDeclaration.validate();
          }
      }
    catch (NoSuchMethodException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  evolutionEngineExtensionUpdateExtendedSubscriberMethod
    //

    try
      {
        evolutionEngineExtensionUpdateExtendedSubscriberMethod = Deployment.getEvolutionEngineExtensionClass().getMethod("updateExtendedSubscriberProfile",ExtendedProfileContext.class,SubscriberStreamEvent.class);
      }
    catch (NoSuchMethodException e)
      {
        throw new RuntimeException(e);
      }

    //
    //  evolutionEngineExternalAPIMethod
    //

    try
      {
        evolutionEngineExternalAPIMethod = (Deployment.getEvolutionEngineExternalAPIClass() != null) ? Deployment.getEvolutionEngineExternalAPIClass().getMethod("processDataSubscriber", SubscriberState.class, SubscriberState.class, SubscriberStreamEvent.class, JourneyService.class) : null;
      }
    catch (NoSuchMethodException e)
      {
        throw new ServerRuntimeException(e);
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

    Map<DeliveryManagerDeclaration,String> deliveryManagerResponseTopics = new HashMap<DeliveryManagerDeclaration,String>();
    Map<DeliveryManagerDeclaration,ConnectSerde<? extends DeliveryRequest>> deliveryManagerResponseSerdes = new HashMap<DeliveryManagerDeclaration,ConnectSerde<? extends DeliveryRequest>>();
    for (DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values())
      {
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
    final ConnectSerde<PresentationLog> presentationLogSerde = PresentationLog.serde();
    final ConnectSerde<AcceptanceLog> acceptanceLogSerde = AcceptanceLog.serde();
    final ConnectSerde<PointFulfillmentRequest> pointFulfillmentRequestSerde = PointFulfillmentRequest.serde();
    final ConnectSerde<SubscriberProfileForceUpdate> subscriberProfileForceUpdateSerde = SubscriberProfileForceUpdate.serde();
    final ConnectSerde<ProfileChangeEvent> profileChangeEventSerde = ProfileChangeEvent.serde();
    final ConnectSerde<ProfileSegmentChangeEvent> profileSegmentChangeEventSerde = ProfileSegmentChangeEvent.serde();
    final ConnectSerde<ProfileLoyaltyProgramChangeEvent> profileLoyaltyProgramChangeEventSerde = ProfileLoyaltyProgramChangeEvent.serde();
    final ConnectSerde<RecordSubscriberID> recordSubscriberIDSerde = RecordSubscriberID.serde();
    final ConnectSerde<JourneyRequest> journeyRequestSerde = JourneyRequest.serde();
    final ConnectSerde<JourneyStatistic> journeyStatisticSerde = JourneyStatistic.serde();
    final ConnectSerde<JourneyStatisticWrapper> journeyStatisticWrapperSerde = JourneyStatisticWrapper.serde();
    final ConnectSerde<JourneyTrafficHistory> journeyTrafficHistorySerde = JourneyTrafficHistory.serde();
    final ConnectSerde<JourneyMetric> journeyMetricSerde = JourneyMetric.serde();
    final ConnectSerde<LoyaltyProgramRequest> loyaltyProgramRequestSerde = LoyaltyProgramRequest.serde();
    final ConnectSerde<SubscriberGroup> subscriberGroupSerde = SubscriberGroup.serde();
    final ConnectSerde<SubscriberTraceControl> subscriberTraceControlSerde = SubscriberTraceControl.serde();
    final ConnectSerde<SubscriberProfile> subscriberProfileSerde = SubscriberProfile.getSubscriberProfileSerde();
    final ConnectSerde<PropensityEventOutput> propensityEventOutputSerde = PropensityEventOutput.serde();
    final ConnectSerde<PropensityKey> propensityKeySerde = PropensityKey.serde();
    final ConnectSerde<PropensityState> propensityStateSerde = PropensityState.serde();
    final Serde<SubscriberTrace> subscriberTraceSerde = SubscriberTrace.serde();
    final Serde<ExternalAPIOutput> externalAPISerde = ExternalAPIOutput.serde();

    //
    //  special serdes
    //

    final Serde<SubscriberState> subscriberStateSerde = SubscriberState.stateStoreSerde();
    final Serde<SubscriberHistory> subscriberHistorySerde = SubscriberHistory.stateStoreSerde();
    final Serde<ExtendedSubscriberProfile> extendedSubscriberProfileSerde = ExtendedSubscriberProfile.stateStoreSerde();

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
    evolutionEventSerdes.add(subscriberProfileForceUpdateSerde);
    evolutionEventSerdes.add(recordSubscriberIDSerde);
    evolutionEventSerdes.add(journeyRequestSerde);
    evolutionEventSerdes.add(journeyStatisticSerde);
    evolutionEventSerdes.add(loyaltyProgramRequestSerde);
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
    KStream<StringKey, SubscriberProfileForceUpdate> subscriberProfileForceUpdateSourceStream = builder.stream(subscriberProfileForceUpdateTopic, Consumed.with(stringKeySerde, subscriberProfileForceUpdateSerde));
    KStream<StringKey, RecordSubscriberID> recordSubscriberIDSourceStream = builder.stream(recordSubscriberIDTopic, Consumed.with(stringKeySerde, recordSubscriberIDSerde));
    KStream<StringKey, JourneyRequest> journeyRequestSourceStream = builder.stream(journeyRequestTopic, Consumed.with(stringKeySerde, journeyRequestSerde));
    KStream<StringKey, JourneyStatistic> journeyStatisticSourceStream = builder.stream(journeyStatisticTopic, Consumed.with(stringKeySerde, journeyStatisticSerde));
    KStream<StringKey, LoyaltyProgramRequest> loyaltyProgramRequestSourceStream = builder.stream(loyaltyProgramRequestTopic, Consumed.with(stringKeySerde, loyaltyProgramRequestSerde));
    KStream<StringKey, SubscriberGroup> subscriberGroupSourceStream = builder.stream(subscriberGroupTopic, Consumed.with(stringKeySerde, subscriberGroupSerde));
    KStream<StringKey, SubscriberTraceControl> subscriberTraceControlSourceStream = builder.stream(subscriberTraceControlTopic, Consumed.with(stringKeySerde, subscriberTraceControlSerde));
    KStream<StringKey, PresentationLog> presentationLogSourceStream = builder.stream(presentationLogTopic, Consumed.with(stringKeySerde, presentationLogSerde));
    KStream<StringKey, AcceptanceLog> acceptanceLogSourceStream = builder.stream(acceptanceLogTopic, Consumed.with(stringKeySerde, acceptanceLogSerde));
    KStream<StringKey, ProfileSegmentChangeEvent> profileSegmentChangeEventStream = builder.stream(profileSegmentChangeEventTopic, Consumed.with(stringKeySerde, profileSegmentChangeEventSerde));
    KStream<StringKey, ProfileLoyaltyProgramChangeEvent> profileLoyaltyProgramChangeEventStream = builder.stream(profileLoyaltyProgramChangeEventTopic, Consumed.with(stringKeySerde, profileLoyaltyProgramChangeEventSerde));

    //
    //  timedEvaluationStreams
    //

    KStream<StringKey, ? extends TimedEvaluation>[] branchedTimedEvaluationStreams = timedEvaluationSourceStream.branch((key,value) -> ((TimedEvaluation) value).getPeriodicEvaluation(), (key,value) -> true);
    KStream<StringKey, TimedEvaluation> periodicTimedEvaluationStream = (KStream<StringKey, TimedEvaluation>) branchedTimedEvaluationStreams[0];
    KStream<StringKey, TimedEvaluation> standardTimedEvaluationStream = (KStream<StringKey, TimedEvaluation>) branchedTimedEvaluationStreams[1];

    //
    //  pointFulfillmentRequest streams (keyed by deliveryRequestID)
    //

    KStream<StringKey, PointFulfillmentRequest> pointFulfillmentRequestSourceStream = builder.stream(pointFulfillmentRequestTopic, Consumed.with(stringKeySerde, pointFulfillmentRequestSerde));
    KStream<StringKey, PointFulfillmentRequest> rekeyedPointFulfillmentRequestSourceStream = pointFulfillmentRequestSourceStream.map(EvolutionEngine::rekeyPointFulfilmentRequestStream).through(pointFufillmentRepartitioningTopic, Produced.with(stringKeySerde, pointFulfillmentRequestSerde));

    //
    //  evolution engine event source streams
    //

    List<KStream<StringKey, ? extends SubscriberStreamEvent>> standardEvolutionEngineEventStreams = new ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    List<KStream<StringKey, ? extends SubscriberStreamEvent>> extendedProfileEvolutionEngineEventStreams = new ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    for (EvolutionEngineEventDeclaration evolutionEngineEventDeclaration : Deployment.getEvolutionEngineEvents().values())
      {
        KStream<StringKey, ? extends SubscriberStreamEvent> evolutionEngineEventStream = builder.stream(evolutionEngineEventTopics.get(evolutionEngineEventDeclaration), Consumed.with(stringKeySerde, evolutionEngineEventSerdes.get(evolutionEngineEventDeclaration)));
        switch (evolutionEngineEventDeclaration.getEventRule())
          {
            case All:
              standardEvolutionEngineEventStreams.add(evolutionEngineEventStream);
              extendedProfileEvolutionEngineEventStreams.add(evolutionEngineEventStream);
              break;

            case Standard:
              standardEvolutionEngineEventStreams.add(evolutionEngineEventStream);
              break;

            case Extended:
              extendedProfileEvolutionEngineEventStreams.add(evolutionEngineEventStream);
              break;
          }
      }

    //
    //  delivery manager response source streams
    //

    List<KStream<StringKey, ? extends SubscriberStreamEvent>> deliveryManagerResponseStreams = new ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    for (DeliveryManagerDeclaration deliveryManagerDeclaration : Deployment.getDeliveryManagers().values())
      {
        deliveryManagerResponseStreams.add(builder.stream(deliveryManagerResponseTopics.get(deliveryManagerDeclaration), Consumed.with(stringKeySerde, deliveryManagerResponseSerdes.get(deliveryManagerDeclaration))).filter((key,value) -> value.getOriginatingRequest()));
      }

    /*****************************************
    *
    *  extendedSubscriberProfile -- update
    *
    *****************************************/

    //
    //  merge source streams -- extendedProfileEventStream
    //

    ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>> extendedProfileEventStreams = new ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    extendedProfileEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberTraceControlSourceStream);
    extendedProfileEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) periodicTimedEvaluationStream);
    extendedProfileEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) recordSubscriberIDSourceStream);
    extendedProfileEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberProfileForceUpdateSourceStream);
    extendedProfileEventStreams.addAll(extendedProfileEvolutionEngineEventStreams);
    KStream extendedProfileEvolutionEventCompositeStream = null;
    for (KStream<StringKey, ? extends SubscriberStreamEvent> eventStream : extendedProfileEventStreams)
      {
        extendedProfileEvolutionEventCompositeStream = (extendedProfileEvolutionEventCompositeStream == null) ? eventStream : extendedProfileEvolutionEventCompositeStream.merge(eventStream);
      }
    KStream<StringKey, SubscriberStreamEvent> extendedProfileEventStream = (KStream<StringKey, SubscriberStreamEvent>) extendedProfileEvolutionEventCompositeStream;

    //
    //  aggregate
    //

    KeyValueBytesStoreSupplier extendedProfileSupplier = Stores.persistentKeyValueStore(extendedSubscriberProfileChangeLog);
    Materialized extendedProfileStoreSchema = Materialized.<StringKey, ExtendedSubscriberProfile>as(extendedProfileSupplier).withKeySerde(stringKeySerde).withValueSerde(extendedSubscriberProfileSerde);
    KTable<StringKey, ExtendedSubscriberProfile> extendedProfile = extendedProfileEventStream.groupByKey(Serialized.with(stringKeySerde, evolutionEventSerde)).aggregate(EvolutionEngine::nullExtendedSubscriberProfile, EvolutionEngine::updateExtendedSubscriberProfile, extendedProfileStoreSchema);

    //
    //  subscriberTrace for extended profile
    //

    KStream<StringKey, ExtendedSubscriberProfile> extendedProfileStream = extendedProfileEventStream.leftJoin(extendedProfile, EvolutionEngine::getExtendedProfile);
    KStream<StringKey, SubscriberStreamOutput> extendedProfileOutputs = extendedProfileStream.flatMapValues(EvolutionEngine::getExtendedProfileOutputs);
    KStream<StringKey, ? extends SubscriberStreamOutput>[] branchedExtendedProfileOutputs = extendedProfileOutputs.branch((key,value) -> (value instanceof SubscriberTrace));
    KStream<StringKey, SubscriberTrace> extendedProfileSubscriberTraceStream = (KStream<StringKey, SubscriberTrace>) branchedExtendedProfileOutputs[0];

    //
    //  enhanced periodic evaluation stream
    //

    KStream<StringKey, TimedEvaluation> enhancedPeriodicEvaluationStream = periodicTimedEvaluationStream.leftJoin(extendedProfile, EvolutionEngine::enhancePeriodicEvaluation);

    /*****************************************
    *
    *  subscriberState -- update
    *
    *****************************************/

    //
    //  merge source streams -- evolutionEventStream
    //

    ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>> evolutionEventStreams = new ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) standardTimedEvaluationStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) enhancedPeriodicEvaluationStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberProfileForceUpdateSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) recordSubscriberIDSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) journeyRequestSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) journeyStatisticSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) loyaltyProgramRequestSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberGroupSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberTraceControlSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) presentationLogSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) acceptanceLogSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) rekeyedPointFulfillmentRequestSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) profileSegmentChangeEventStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) profileLoyaltyProgramChangeEventStream);
    evolutionEventStreams.addAll(standardEvolutionEngineEventStreams);
    evolutionEventStreams.addAll(deliveryManagerResponseStreams);
    KStream evolutionEventCompositeStream = null;
    for (KStream<StringKey, ? extends SubscriberStreamEvent> eventStream : evolutionEventStreams)
      {
        evolutionEventCompositeStream = (evolutionEventCompositeStream == null) ? eventStream : evolutionEventCompositeStream.merge(eventStream);
      }
    KStream<StringKey, SubscriberStreamEvent> evolutionEventStream = (KStream<StringKey, SubscriberStreamEvent>) evolutionEventCompositeStream;

    //
    //  aggreate
    //

    KeyValueBytesStoreSupplier subscriberStateSupplier = Stores.persistentKeyValueStore(subscriberStateChangeLog);
    Materialized subscriberStateStoreSchema = Materialized.<StringKey, SubscriberState>as(subscriberStateSupplier).withKeySerde(stringKeySerde).withValueSerde(subscriberStateSerde);
    KTable<StringKey, SubscriberState> subscriberState = evolutionEventStream.groupByKey(Serialized.with(stringKeySerde, evolutionEventSerde)).aggregate(EvolutionEngine::nullSubscriberState, EvolutionEngine::updateSubscriberState, subscriberStateStoreSchema);

    //
    //  convert to stream
    //

    KStream<StringKey, SubscriberState> subscriberStateStream = evolutionEventStream.leftJoin(subscriberState, EvolutionEngine::getSubscriberState);

    //
    //  get outputs
    //

    KStream<StringKey, SubscriberStreamOutput> evolutionEngineOutputs = subscriberStateStream.flatMapValues(EvolutionEngine::getEvolutionEngineOutputs);

    //
    //  branch output streams
    //

    KStream<StringKey, ? extends SubscriberStreamOutput>[] branchedEvolutionEngineOutputs = evolutionEngineOutputs.branch(
        (key,value) -> (value instanceof JourneyRequest && !((JourneyRequest)value).getDeliveryStatus().equals(DeliveryStatus.Pending)), 
        (key,value) -> (value instanceof JourneyRequest), 
        (key,value) -> (value instanceof LoyaltyProgramRequest && !((LoyaltyProgramRequest)value).getDeliveryStatus().equals(DeliveryStatus.Pending)), 
        (key,value) -> (value instanceof LoyaltyProgramRequest), 
        (key,value) -> (value instanceof PointFulfillmentRequest && !((PointFulfillmentRequest)value).getDeliveryStatus().equals(DeliveryStatus.Pending)), 
        (key,value) -> (value instanceof DeliveryRequest), 
        (key,value) -> (value instanceof JourneyStatisticWrapper), 
        (key,value) -> (value instanceof JourneyStatistic), 
        (key,value) -> (value instanceof JourneyMetric), 
        (key,value) -> (value instanceof SubscriberTrace),
        (key,value) -> (value instanceof PropensityEventOutput),
        (key,value) -> (value instanceof ExternalAPIOutput),
		(key,value) -> (value instanceof ProfileChangeEvent), 
        (key,value) -> (value instanceof ProfileSegmentChangeEvent),
        (key,value) -> (value instanceof ProfileLoyaltyProgramChangeEvent));
    KStream<StringKey, JourneyRequest> journeyResponseStream = (KStream<StringKey, JourneyRequest>) branchedEvolutionEngineOutputs[0];
    KStream<StringKey, JourneyRequest> journeyRequestStream = (KStream<StringKey, JourneyRequest>) branchedEvolutionEngineOutputs[1];
    KStream<StringKey, LoyaltyProgramRequest> loyaltyProgramResponseStream = (KStream<StringKey, LoyaltyProgramRequest>) branchedEvolutionEngineOutputs[2];
    KStream<StringKey, LoyaltyProgramRequest> loyaltyProgramRequestStream = (KStream<StringKey, LoyaltyProgramRequest>) branchedEvolutionEngineOutputs[3];
    KStream<StringKey, PointFulfillmentRequest> pointResponseStream = (KStream<StringKey, PointFulfillmentRequest>) branchedEvolutionEngineOutputs[4];
    KStream<StringKey, DeliveryRequest> deliveryRequestStream = (KStream<StringKey, DeliveryRequest>) branchedEvolutionEngineOutputs[5];
    KStream<StringKey, JourneyStatisticWrapper> journeyStatisticWrapperStream = (KStream<StringKey, JourneyStatisticWrapper>) branchedEvolutionEngineOutputs[6];
    KStream<StringKey, JourneyStatistic> journeyStatisticStream = (KStream<StringKey, JourneyStatistic>) branchedEvolutionEngineOutputs[7];
    KStream<StringKey, JourneyMetric> journeyMetricStream = (KStream<StringKey, JourneyMetric>) branchedEvolutionEngineOutputs[8];
    KStream<StringKey, SubscriberTrace> subscriberTraceStream = (KStream<StringKey, SubscriberTrace>) branchedEvolutionEngineOutputs[9];
    KStream<StringKey, PropensityEventOutput> propensityOutputsStream = (KStream<StringKey, PropensityEventOutput>) branchedEvolutionEngineOutputs[10];

    KStream<StringKey, ExternalAPIOutput> externalAPIOutputsStream = (KStream<StringKey, ExternalAPIOutput>) branchedEvolutionEngineOutputs[11];
    KStream<StringKey, ProfileChangeEvent> profileChangeEventsStream = (KStream<StringKey, ProfileChangeEvent>) branchedEvolutionEngineOutputs[12];
    KStream<StringKey, ProfileSegmentChangeEvent> profileSegmentChangeEventsStream = (KStream<StringKey, ProfileSegmentChangeEvent>) branchedEvolutionEngineOutputs[13];
    KStream<StringKey, ProfileLoyaltyProgramChangeEvent> profileLoyaltyProgramChangeEventsStream = (KStream<StringKey, ProfileLoyaltyProgramChangeEvent>) branchedEvolutionEngineOutputs[14];
 

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
    //  branch delivery requests
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

    //
    //  rekey journeys responses 
    //

    KStream<StringKey, JourneyRequest> rekeyedJourneyResponseStream = journeyResponseStream.map(EvolutionEngine::rekeyJourneyResponseStream);

    //
    //  rekey loyalty programs responses 
    //

    KStream<StringKey, LoyaltyProgramRequest> rekeyedLoyaltyProgramResponseStream = loyaltyProgramResponseStream.map(EvolutionEngine::rekeyLoyaltyProgramResponseStream);

    //
    //  rekey points responses 
    //

    KStream<StringKey, PointFulfillmentRequest> rekeyedPointResponseStream = pointResponseStream.map(EvolutionEngine::rekeyPointResponseStream);

    /*****************************************
    *
    *  JourneyStatistics
    *
    *****************************************/

    KStream<StringKey, JourneyStatisticWrapper> rekeyedJourneyStatisticStream = journeyStatisticWrapperStream.map(EvolutionEngine::rekeyByJourneyID);
    KeyValueBytesStoreSupplier journeyTrafficSupplier = Stores.persistentKeyValueStore(journeyTrafficChangeLog);
    Materialized journeyTrafficStore = Materialized.<StringKey, JourneyTrafficHistory>as(journeyTrafficSupplier).withKeySerde(stringKeySerde).withValueSerde(journeyTrafficHistorySerde);
    KTable<StringKey, JourneyTrafficHistory> unusedJourneyTraffic = rekeyedJourneyStatisticStream.groupByKey(Serialized.with(stringKeySerde, journeyStatisticWrapperSerde)).aggregate(EvolutionEngine::nullJourneyTrafficHistory, EvolutionEngine::updateJourneyTrafficHistory, journeyTrafficStore);
    
    /*****************************************
    *
    *  propensityState -- update
    *
    *****************************************/

    //
    //  propensity rekey
    //
    // We manually write the rekeyed KStream in an underlying intermediary topic (propensityoutput-repartition)
    //
    // When going through a map operation, we change the key of the topic.
    // Therefore the repartition done by Kafka between the different topic partitions will not be the same.
    // For instance, a worker assigned to the treatment of a record may not be assigned to the treatment of its rekeyed record.
    // That is why, we need to write the rekeyed KStream in an intermediary topic.
    //
    // Usually it's automatically done in the next operation applied on this KStream as it is mentioned in groupByKey or leftJoin javadoc:
    //      "If a key changing operator was used before this operation and no data redistribution
    //      happened afterwards an internal repartitioning topic will be created in Kafka."
    // But here, if we let this happen, it will create two different internal repartitioning topics (both containing the same records), one for the groupeByKey and one for the leftJoin.
    // Firstly, it would be a waste of resources to duplicate those topics.
    // But, more important, by doing this, we will introduce indeterminism, and therefore errors.
    // Indeed, the groupByKey and leftJoin operations would not be done sequentially anymore but in parallel, and we could not ensure that groupByKey will be done before the leftJoin.
    //
    // For those reasons, we manually create the intermediary topic just after the map operation and we applied groupeByKey and leftJoin on this "well-partioned" stream, thus no other redistribution intermediary topic will be needed.
    //

    KStream<PropensityKey, PropensityEventOutput> rekeyedPropensityStream = propensityOutputsStream.map(EvolutionEngine::rekeyPropensityStream).through(propensityRepartitioningTopic, Produced.with(propensityKeySerde, propensityEventOutputSerde));

    //
    //  propensity aggregate
    //

    KeyValueBytesStoreSupplier supplier = Stores.persistentKeyValueStore(propensityStateChangeLog);
    Materialized propensityStateStore = Materialized.<PropensityKey, PropensityState>as(supplier).withKeySerde(propensityKeySerde).withValueSerde(propensityStateSerde.optionalSerde());
    KTable<PropensityKey, PropensityState> propensityState = rekeyedPropensityStream.groupByKey(Serialized.with(propensityKeySerde, propensityEventOutputSerde)).aggregate(EvolutionEngine::nullPropensityState, EvolutionEngine::updatePropensityState, propensityStateStore);

    //
    //  convert to stream
    //

    KStream<PropensityKey, PropensityState> propensityStateStream = rekeyedPropensityStream.leftJoin(propensityState, EvolutionEngine::getPropensityState);

    /*****************************************
    *
    *  subscriberHistory -- update
    *
    *****************************************/

    //
    //  merge source streams -- subscriberHistoryStream
    //

    KStream subscriberHistoryCompositeStream = journeyStatisticSourceStream;
    for (KStream<StringKey, ? extends SubscriberStreamEvent> eventStream : deliveryManagerResponseStreams)
      {
        subscriberHistoryCompositeStream = (subscriberHistoryCompositeStream == null) ? eventStream : subscriberHistoryCompositeStream.merge(eventStream);
      }
    KStream<StringKey, SubscriberStreamEvent> subscriberHistoryStream = (KStream<StringKey, SubscriberStreamEvent>) subscriberHistoryCompositeStream;

    //
    //  aggregate
    //

    KeyValueBytesStoreSupplier subscriberHistorySupplier = Stores.persistentKeyValueStore(subscriberHistoryChangeLog);
    Materialized subscriberHistoryStoreSchema = Materialized.<StringKey, SubscriberHistory>as(subscriberHistorySupplier).withKeySerde(stringKeySerde).withValueSerde(subscriberHistorySerde);
    KTable<StringKey, SubscriberHistory> subscriberHistory = subscriberHistoryStream.groupByKey(Serialized.with(stringKeySerde, evolutionEventSerde)).aggregate(EvolutionEngine::nullSubscriberHistory, EvolutionEngine::updateSubscriberHistory, subscriberHistoryStoreSchema);

    /*****************************************
    *
    *  sink
    *
    *****************************************/

    //
    //  sink - core streams
    //

    journeyRequestStream.to(journeyRequestTopic, Produced.with(stringKeySerde, journeyRequestSerde));
    rekeyedJourneyResponseStream.to(journeyResponseTopic, Produced.with(stringKeySerde, journeyRequestSerde));
    loyaltyProgramRequestStream.to(loyaltyProgramRequestTopic, Produced.with(stringKeySerde, loyaltyProgramRequestSerde));
    rekeyedLoyaltyProgramResponseStream.to(loyaltyProgramResponseTopic, Produced.with(stringKeySerde, loyaltyProgramRequestSerde));
    rekeyedPointResponseStream.to(pointFulfillmentResponseTopic, Produced.with(stringKeySerde, pointFulfillmentRequestSerde));
    journeyStatisticStream.to(journeyStatisticTopic, Produced.with(stringKeySerde, journeyStatisticSerde));
    journeyMetricStream.to(journeyMetricTopic, Produced.with(stringKeySerde, journeyMetricSerde));
    subscriberTraceStream.to(subscriberTraceTopic, Produced.with(stringKeySerde, subscriberTraceSerde));
    extendedProfileSubscriberTraceStream.to(subscriberTraceTopic, Produced.with(stringKeySerde, subscriberTraceSerde));
    propensityStateStream.to(propensityLogTopic, Produced.with(propensityKeySerde, propensityStateSerde));
    profileChangeEventsStream.to(profileChangeEventTopic, Produced.with(stringKeySerde, profileChangeEventSerde));
    profileSegmentChangeEventsStream.to(profileSegmentChangeEventTopic, Produced.with(stringKeySerde, profileSegmentChangeEventSerde));
    profileLoyaltyProgramChangeEventsStream.to(profileLoyaltyProgramChangeEventTopic, Produced.with(stringKeySerde, profileLoyaltyProgramChangeEventSerde));

    //
    //  sink - delivery request streams
    //

    for (String deliveryType : deliveryRequestStreams.keySet())
      {
        //
        //  branch by priority
        //

        DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get(deliveryType);
        DeliveryPriority[] deliveryPriorities = new DeliveryPriority[DeliveryPriority.values().length - 1];
        DeliveryPriorityPredicate[] deliveryPriorityPredicates = new DeliveryPriorityPredicate[DeliveryPriority.values().length - 1];
        int j = 0;
        for (DeliveryPriority deliveryPriority : DeliveryPriority.values())
          {
            if (deliveryPriority != DeliveryPriority.Unknown)
              {
                deliveryPriorities[j] = deliveryPriority;
                deliveryPriorityPredicates[j] = new DeliveryPriorityPredicate(deliveryPriority);
                j += 1;
              }
          }

        //
        //  branch 
        //

        KStream<StringKey, DeliveryRequest>[] branchedDeliveryRequestStreamsByPriority = deliveryRequestStreams.get(deliveryType).branch(deliveryPriorityPredicates);

        //
        //  sink
        //

        ConnectSerde<DeliveryRequest> requestSerde = (ConnectSerde<DeliveryRequest>) deliveryManager.getRequestSerde();
        for (int k=0; k<branchedDeliveryRequestStreamsByPriority.length; k++)
          {
            String requestTopic = deliveryManager.getRequestTopic(deliveryPriorities[k]);
            KStream<StringKey, DeliveryRequest> requestStream = branchedDeliveryRequestStreamsByPriority[k];
            KStream<StringKey, DeliveryRequest> rekeyedRequestStream = requestStream.map(EvolutionEngine::rekeyDeliveryRequestStream);
            rekeyedRequestStream.to(requestTopic, Produced.with(stringKeySerde, requestSerde));
          }
      }

    //
    //  sink -- externalAPI output stream
    //

    Map<String, ExternalAPITopic> externalAPITopics = Deployment.getExternalAPITopics();
    if (!externalAPITopics.values().isEmpty())
      {
        ExternalAPIOutputPredicate[] externalAPIOutputPredicates = new ExternalAPIOutputPredicate[externalAPITopics.values().size()];
        String[] externalAPIOutputTopics = new String[externalAPITopics.values().size()];
        int j = 0;
        for (String topicID : externalAPITopics.keySet())
          {
            externalAPIOutputPredicates[j] = new ExternalAPIOutputPredicate(topicID);
            externalAPIOutputTopics[j] = externalAPITopics.get(topicID).getName();
            j += 1;
          }

        //
        // ExternalAPIOutput branch
        //

        KStream<StringKey, ExternalAPIOutput>[] branchedExternalAPIStreamsByTopic = externalAPIOutputsStream.branch(externalAPIOutputPredicates);

        //
        // ExternalAPIOutput sink
        //

        for (int k = 0; k < externalAPIOutputPredicates.length; k++)
          {
            KStream<StringKey, ExternalAPIOutput> rekeyedExternalAPIStream = branchedExternalAPIStreamsByTopic[k].map(EvolutionEngine::rekeyExternalAPIOutputStream);
            // Only send the json part to the output topic 
            KStream<StringKey, StringValue> externalAPIStreamString = rekeyedExternalAPIStream.map(
                (key,value) -> new KeyValue<StringKey, StringValue>(new StringKey(value.getTopicID()), new StringValue(value.getJsonString())));
            externalAPIStreamString.to(externalAPIOutputTopics[k], Produced.with(stringKeySerde, StringValue.serde()));
          }
      }
    
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

        synchronized (EvolutionEngine.this)
          {
            EvolutionEngine.this.notifyAll();
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
            extendedSubscriberProfileStore = streams.store(Deployment.getExtendedSubscriberProfileChangeLog(), QueryableStoreTypes.keyValueStore());
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

    NGLMRuntime.addShutdownHook(new ShutdownHook(streams, subscriberGroupEpochReader, ucgStateReader, journeyService, targetService, journeyObjectiveService, segmentationDimensionService, timerService, exclusionInclusionTargetService, subscriberProfileServer, internalServer));

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
    *  start periodic logger
    *
    *****************************************/

    Runnable periodicLogger = new Runnable() { @Override public void run() { runPeriodicLogger(); } };
    Thread periodicLoggerThread = new Thread(periodicLogger, "PeriodicLogger");
    periodicLoggerThread.start();

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("evolution engine started");
  }

  /*****************************************
  *
  *  getExternalTokenType
  *
  *****************************************/

  public static TokenType getExternalTokenType()
  {
    if(externalTokenType == null)
      {
        externalTokenType = (TokenType) tokenTypeService.getStoredTokenType(externalTokenTypeID);
      }
    return externalTokenType;
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
    //  test (predicate interface)
    //

    @Override public boolean test(StringKey stringKey, DeliveryRequest deliveryRequest)
    {
      return deliveryType.equals(deliveryRequest.getDeliveryType());
    }
  }

  /****************************************
  *
  *  class DeliveryPriorityPredicate
  *
  ****************************************/

  private static class DeliveryPriorityPredicate implements Predicate<StringKey, DeliveryRequest>
  {
    //
    //  data
    //

    DeliveryPriority deliveryPriority;

    //
    //  constructor
    //

    private DeliveryPriorityPredicate(DeliveryPriority deliveryPriority)
    {
      this.deliveryPriority = deliveryPriority;
    }

    //
    //  test (predicate interface)
    //

    @Override public boolean test(StringKey stringKey, DeliveryRequest deliveryRequest)
    {
      return deliveryPriority == deliveryRequest.getDeliveryPriority();
    }
  }

  /****************************************
  *
  *  class ExternalAPIOutputPredicate
  *
  ****************************************/

  private static class ExternalAPIOutputPredicate implements Predicate<StringKey, ExternalAPIOutput>
  {
    //
    //  data
    //

    String topicId;

    //
    //  constructor
    //

    private ExternalAPIOutputPredicate(String topicId)
    {
      this.topicId = topicId;
    }

    //
    //  test (predicate interface)
    //

    @Override public boolean test(StringKey stringKey, ExternalAPIOutput externalAPIOutput)
    {
      return topicId.equals(externalAPIOutput.getTopicID());
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
    private ReferenceDataReader<String,UCGState> ucgStateReader;
    private JourneyService journeyService;
    private TargetService targetService;
    private JourneyObjectiveService journeyObjectiveService;
    private SegmentationDimensionService segmentationDimensionService;
    private TimerService timerService;
    private HttpServer subscriberProfileServer;
    private HttpServer internalServer;
    private ExclusionInclusionTargetService exclusionInclusionTargetService;

    //
    //  constructor
    //

    private ShutdownHook(KafkaStreams kafkaStreams, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,UCGState> ucgStateReader, JourneyService journeyService, TargetService targetService, JourneyObjectiveService journeyObjectiveService, SegmentationDimensionService segmentationDimensionService, TimerService timerService, ExclusionInclusionTargetService exclusionInclusionTargetService,  HttpServer subscriberProfileServer, HttpServer internalServer)
    {
      this.kafkaStreams = kafkaStreams;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
      this.ucgStateReader = ucgStateReader;
      this.targetService = targetService;
      this.journeyService = journeyService;
      this.journeyObjectiveService = journeyObjectiveService;
      this.segmentationDimensionService = segmentationDimensionService;
      this.timerService = timerService;
      this.subscriberProfileServer = subscriberProfileServer;
      this.internalServer = internalServer;
      this.exclusionInclusionTargetService = exclusionInclusionTargetService;
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
      //  close reference data readers
      //

      subscriberGroupEpochReader.close();
      ucgStateReader.close();

      //
      //  stop services
      //

      journeyService.stop();
      targetService.stop();
      journeyObjectiveService.stop();
      segmentationDimensionService.stop();
      timerService.stop();
      exclusionInclusionTargetService.stop();

      //
      //  rest server
      //

      subscriberProfileServer.stop(1);
      internalServer.stop(1);

      //
      //  stop streams
      //

      boolean streamsCloseSuccessful = kafkaStreams.close(60, java.util.concurrent.TimeUnit.SECONDS);
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
    ExtendedSubscriberProfile extendedSubscriberProfile = (evolutionEvent instanceof TimedEvaluation) ? ((TimedEvaluation) evolutionEvent).getExtendedSubscriberProfile() : null;
    EvolutionEventContext context = new EvolutionEventContext(subscriberState, extendedSubscriberProfile, subscriberGroupEpochReader, subscriberMessageTemplateService, deliverableService, segmentationDimensionService, segmentContactPolicyService, uniqueKeyServer, SystemTime.getCurrentTime());
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

    subscriberStateUpdated = cleanSubscriberState(subscriberState, now) || subscriberStateUpdated;

    /*****************************************
    *
    *  profileChangeEvent get Old Values
    *
    *****************************************/
    
    SubscriberEvaluationRequest changeEventEvaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, extendedSubscriberProfile, subscriberGroupEpochReader, now);
    ParameterMap profileChangeOldValues = saveProfileChangeOldValues(changeEventEvaluationRequest); 
    
    /*****************************************
    *
    *  profileSegmentChangeEvent get Old Values
    *
    *****************************************/
    
    ParameterMap profileSegmentChangeOldValues = saveProfileSegmentChangeOldValues(changeEventEvaluationRequest); 
    
    /*****************************************
    *
    *  update SubscriberProfile
    *
    *****************************************/

    subscriberStateUpdated = updateSubscriberProfile(context, evolutionEvent) || subscriberStateUpdated;

    /*****************************************
    *
    *  update SubscriberLoyaltyProgram
    *
    *****************************************/

    subscriberStateUpdated = updateSubscriberLoyaltyProgram(context, evolutionEvent) || subscriberStateUpdated;
    
    /*****************************************
    *
    *  update PropensityOutputs
    *
    *****************************************/

    subscriberStateUpdated = updatePropensity(context, evolutionEvent) || subscriberStateUpdated;

    /*****************************************
    *
    *  update journeyStates
    *
    *****************************************/

    subscriberStateUpdated = updateJourneys(context, evolutionEvent) || subscriberStateUpdated;

    /*****************************************
    *
    *  profile change detect changed values
    *
    *****************************************/
        
    updatePointBalances(context, subscriberState, now);
    
    /*****************************************
    *
    *  profile change detect changed values
    *
    *****************************************/
        
    updateChangeEvents(subscriberState, now, changeEventEvaluationRequest, profileChangeOldValues);
    
    /*****************************************
    *
    *  profile segment change detect changed segments
    *
    *****************************************/
        
    updateSegmentChangeEvents(subscriberState, subscriberProfile, now, changeEventEvaluationRequest, profileSegmentChangeOldValues);

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
    *  subscriberTrace
    *
    *****************************************/

    if (subscriberProfile.getSubscriberTraceEnabled())
      {
        subscriberState.setSubscriberTrace(new SubscriberTrace(generateSubscriberTraceMessage(evolutionEvent, currentSubscriberState, subscriberState, context.getSubscriberTraceDetails())));
        subscriberStateUpdated = true;
      }

    /*****************************************
    *
    *  trackingID
    *
    *****************************************/
    
    if (evolutionEvent.getTrackingID() != null) 
      {        
        subscriberState.setTrackingID(evolutionEvent.getTrackingID());
        subscriberStateUpdated = true;
      }

    /*****************************************
    *
    *  externalAPI
    *
    *****************************************/

    Pair<String,JSONObject> resExternalAPI = callExternalAPI(evolutionEvent, currentSubscriberState, subscriberState);
    JSONObject jsonObject = resExternalAPI.getSecondElement();
    if (jsonObject != null)
      {
        subscriberState.setExternalAPIOutput(new ExternalAPIOutput(resExternalAPI.getFirstElement(), jsonObject.toJSONString()));
        subscriberStateUpdated = true;
      }

    /*****************************************
    *
    *  lastEvaluationDate
    *
    *****************************************/

    subscriberState.setLastEvaluationDate(now);
    subscriberStateUpdated = true;

    /*****************************************
    *
    *  stateStoreSerde
    *
    *****************************************/

    if (subscriberStateUpdated)
      {
        subscriberState.stateStoreSerde().setKafkaRepresentation(Deployment.getSubscriberStateChangeLogTopic(), subscriberState);
        evolutionEngineStatistics.updateSubscriberStateSize(subscriberState.getKafkaRepresentation());
        if (subscriberState.getKafkaRepresentation().length > 1000000)
          {
            log.error("StateStore size error, ignoring event {} for subscriber {}: {}", evolutionEvent.getClass().toString(), evolutionEvent.getSubscriberID(), subscriberState.getKafkaRepresentation().length);
            cleanSubscriberState(currentSubscriberState, now);
            currentSubscriberState.stateStoreSerde().setKafkaRepresentation(Deployment.getSubscriberStateChangeLogTopic(), currentSubscriberState);
            subscriberStateUpdated = false;
          }
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
  *  cleanSubscriberState
  *
  *****************************************/

  private static boolean cleanSubscriberState(SubscriberState subscriberState, Date now)
  {
    //
    //  subscriberStateUpdated
    //

    boolean subscriberStateUpdated = false;

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
    //  journeyRequests
    //

    if (subscriberState.getJourneyRequests().size() > 0)
      {
        subscriberState.getJourneyRequests().clear();
        subscriberStateUpdated = true;
      }

    //
    //  journeyResponses
    //

    if (subscriberState.getJourneyResponses().size() > 0)
      {
        subscriberState.getJourneyResponses().clear();
        subscriberStateUpdated = true;
      }

    //
    //  loyaltyProgramRequests
    //

    if (subscriberState.getLoyaltyProgramRequests().size() > 0)
      {
        subscriberState.getLoyaltyProgramRequests().clear();
        subscriberStateUpdated = true;
      }

    //
    //  loyaltyProgramResponses
    //

    if (subscriberState.getLoyaltyProgramResponses().size() > 0)
      {
        subscriberState.getLoyaltyProgramResponses().clear();
        subscriberStateUpdated = true;
      }

    //
    //  pointFulfillmentResponses
    //

    if (subscriberState.getPointFulfillmentResponses().size() > 0)
      {
        subscriberState.getPointFulfillmentResponses().clear();
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

    if (subscriberState.getJourneyStatisticWrappers().size() > 0)
      {
        subscriberState.getJourneyStatisticWrappers().clear();
        subscriberStateUpdated = true;
      }

    //
    //  journeyMetrics
    //

    if (subscriberState.getJourneyMetrics().size() > 0)
      {
        subscriberState.getJourneyMetrics().clear();
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

    //
    //  propensityOutputs cleaning
    //

    if (subscriberState.getPropensityOutputs() != null)
      {
        subscriberState.getPropensityOutputs().clear();
        subscriberStateUpdated = true;
      }

        
    //
    //  profileChangeEvents cleaning
    //

    if (subscriberState.getProfileChangeEvents() != null)
      {
        subscriberState.getProfileChangeEvents().clear();
        subscriberStateUpdated = true;
      }
    
    
    //
    //  profileSegmentChangeEvents cleaning
    //

    if (subscriberState.getProfileSegmentChangeEvents() != null)
      {
        subscriberState.getProfileSegmentChangeEvents().clear();
        subscriberStateUpdated = true;
      }

	//
    //  profileLoayltyProgramChangeEvents cleaning
    //

    if (subscriberState.getProfileLoyaltyProgramChangeEvents() != null)
      {
        subscriberState.getProfileLoyaltyProgramChangeEvents().clear();
        subscriberStateUpdated = true;
      }
    

    //
    //  externalAPIOutput
    //
   if (subscriberState.getExternalAPIOutput() != null)
      {
        subscriberState.setExternalAPIOutput(null);
        subscriberStateUpdated = true;
      }
	  


    //
    //  return
    //

    return subscriberStateUpdated;
  }
  
  /*****************************************
  *
  *  saveProfileChangeOldValues
  *
  *****************************************/
  
  private static ParameterMap saveProfileChangeOldValues(SubscriberEvaluationRequest changeEventEvaluationRequest)
  {
    ParameterMap profileChangeOldValues = new ParameterMap();
    for(String criterionFieldID: Deployment.getProfileChangeDetectionCriterionFields().keySet()) {
      Object value = CriterionContext.Profile.getCriterionFields().get(criterionFieldID).retrieve(changeEventEvaluationRequest);
      profileChangeOldValues.put(criterionFieldID, value);      
    }
    return profileChangeOldValues;
  }
  
  /*****************************************
  *
  *  saveProfileSegmentChangeOldValues
  *
  *****************************************/
  
  private static ParameterMap saveProfileSegmentChangeOldValues(SubscriberEvaluationRequest changeEventEvaluationRequest)
  {
    if (!Deployment.getEnableProfileSegmentChange()) 
      {
        return null;
      }
    
    ParameterMap oldSubscriberSegmentPerDimension = new ParameterMap();
    Map<String, String> segmentsMap = changeEventEvaluationRequest.getSubscriberProfile().getSegmentsMap(changeEventEvaluationRequest.getSubscriberGroupEpochReader());
    for(String dimensionId : segmentsMap.keySet()){
      SegmentationDimension dimension = segmentationDimensionService.getActiveSegmentationDimension(dimensionId, SystemTime.getCurrentTime());
      if(dimension != null) {
        oldSubscriberSegmentPerDimension.put(dimension.getSegmentationDimensionName(), segmentsMap.get(dimensionId));
      }
    }
    
    // complete list with dimension not set so segment null
    
    for(SegmentationDimension dimension : segmentationDimensionService.getActiveSegmentationDimensions(SystemTime.getCurrentTime())){
      if(!oldSubscriberSegmentPerDimension.containsKey(dimension.getSegmentationDimensionName())) {
        oldSubscriberSegmentPerDimension.put(dimension.getSegmentationDimensionName(), null);
      }   
    }
    return oldSubscriberSegmentPerDimension;
  }
  
  /*****************************************
  *
  *  updatePointBalances
  *
  *****************************************/
  
  private static void updatePointBalances(EvolutionEventContext context, SubscriberState subscriberState, Date now)
  {
    Map<String, PointBalance> pointBalances = subscriberState.getSubscriberProfile().getPointBalances();
    if(pointBalances != null){
      for(String pointID: pointBalances.keySet()) {
        Point point = pointService.getActivePoint(pointID, now);
        if(point != null){
          updatePointBalance(context, subscriberState.getSubscriberProfile(), point, PointOperation.Expire, 0, now);
        }
      }
    }
  }

  /*****************************************
  *
  *  updateChangeEvents
  *
  *****************************************/
  
  private static void updateChangeEvents(SubscriberState subscriberState, Date now, SubscriberEvaluationRequest changeEventEvaluationRequest, ParameterMap profileChangeOldValues)
  {
    ParameterMap profileChangeNewValues = new ParameterMap();
    for(String criterionFieldID: Deployment.getProfileChangeDetectionCriterionFields().keySet()) {
      Object value = CriterionContext.Profile.getCriterionFields().get(criterionFieldID).retrieve(changeEventEvaluationRequest);
      if(!Objects.equals(profileChangeOldValues.get(criterionFieldID), value)) {
        profileChangeNewValues.put(criterionFieldID, value);
      }
      else {
        profileChangeOldValues.remove(criterionFieldID);
      }
    }
    if(profileChangeNewValues.size() > 0) {
      ProfileChangeEvent profileChangeEvent = new ProfileChangeEvent(changeEventEvaluationRequest.getSubscriberProfile().getSubscriberID(), now, profileChangeOldValues, profileChangeNewValues);
      subscriberState.getProfileChangeEvents().add(profileChangeEvent);
    }
  }

  /*****************************************
  *
  *  updateSegmentChangeEvents
  *
  *****************************************/
  
  private static void updateSegmentChangeEvents(SubscriberState subscriberState, SubscriberProfile subscriberProfile, Date now, SubscriberEvaluationRequest changeEventEvaluationRequest, ParameterMap profileSegmentChangeOldValues)
  {    
    if (!Deployment.getEnableProfileSegmentChange()) 
      {
        return;
      }
    
    ParameterMap profileSegmentChangeNewValues = new ParameterMap();
    Map<String, String> segmentsMap = changeEventEvaluationRequest.getSubscriberProfile().getSegmentsMap(changeEventEvaluationRequest.getSubscriberGroupEpochReader());
    for(String dimensionId : segmentsMap.keySet())
      {
        SegmentationDimension dimension = segmentationDimensionService.getActiveSegmentationDimension(dimensionId, SystemTime.getCurrentTime());
        profileSegmentChangeNewValues.put(dimension.getSegmentationDimensionName(), segmentsMap.get(dimensionId));
      }
    
    // complete list with dimension not set so segment null
    
    for(SegmentationDimension dimension : segmentationDimensionService.getActiveSegmentationDimensions(SystemTime.getCurrentTime()))
      {
        if(!profileSegmentChangeNewValues.containsKey(dimension.getSegmentationDimensionName())) 
          {
            profileSegmentChangeNewValues.put(dimension.getSegmentationDimensionName(), null);
          }   
      }
    
    // now compare entering, leaving, updating
    
    for(SegmentationDimension dimension : segmentationDimensionService.getActiveSegmentationDimensions(SystemTime.getCurrentTime()))
      {
        if(profileSegmentChangeOldValues.get(dimension.getSegmentationDimensionName()) == null)
          {
            if(profileSegmentChangeNewValues.get(dimension.getSegmentationDimensionName()) != null)
              {
                profileSegmentChangeOldValues.put(dimension.getSegmentationDimensionName(), ProfileSegmentChangeEvent.SEGMENT_ENTERING_LEAVING.ENTERING.name());
              }
            else 
              {
                profileSegmentChangeNewValues.remove(dimension.getSegmentationDimensionName());
              }
            continue;
          }
        if(profileSegmentChangeOldValues.get(dimension.getSegmentationDimensionName()) != null)
          {
            if(profileSegmentChangeNewValues.get(dimension.getSegmentationDimensionName()) != null 
                && profileSegmentChangeNewValues.get(dimension.getSegmentationDimensionName()).equals(profileSegmentChangeOldValues.get(dimension.getSegmentationDimensionName())))
              {
                profileSegmentChangeOldValues.remove(dimension.getSegmentationDimensionName());
                profileSegmentChangeNewValues.remove(dimension.getSegmentationDimensionName());
              }
            continue;
          }
        if(profileSegmentChangeNewValues.get(dimension.getSegmentationDimensionName()) == null)
          {
            if(profileSegmentChangeOldValues.get(dimension.getSegmentationDimensionName()) != null)
              {
                profileSegmentChangeNewValues.put(dimension.getSegmentationDimensionName(), ProfileSegmentChangeEvent.SEGMENT_ENTERING_LEAVING.LEAVING.name());
              }
            else 
              {
                profileSegmentChangeOldValues.remove(dimension.getSegmentationDimensionName());
              }
            continue;
          }        
      }
    if(profileSegmentChangeNewValues.size() > 0) {
      ProfileSegmentChangeEvent profileSegmentChangeEvent = new ProfileSegmentChangeEvent(subscriberProfile.getSubscriberID(), now, profileSegmentChangeOldValues, profileSegmentChangeNewValues);
      subscriberState.getProfileSegmentChangeEvents().add(profileSegmentChangeEvent);
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
    
    return history;
  }

  /*****************************************
  * 
  *  callExternalAPI
  *
  *****************************************/

  private static Pair<String,JSONObject> callExternalAPI(SubscriberStreamEvent evolutionEvent, SubscriberState currentSubscriberState, SubscriberState subscriberState)
  {
    /*****************************************
    *
    *  result
    *
    *****************************************/

    SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();
    Pair<String,JSONObject> result;

    /*****************************************
    *
    *  invoke evolution engine external API
    *
    *****************************************/

    if (evolutionEngineExternalAPIMethod != null)
      {
        try
        {
          result = (Pair<String,JSONObject>) evolutionEngineExternalAPIMethod.invoke(null, currentSubscriberState, subscriberState, evolutionEvent, journeyService);
        }
        catch (IllegalAccessException|InvocationTargetException e)
        {
          throw new RuntimeException(e);
        }
      }
    else
      {
        result = new Pair<String,JSONObject>("",null);
      }
    
    return result;
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
    ExtendedSubscriberProfile extendedSubscriberProfile = context.getExtendedSubscriberProfile();
    boolean subscriberProfileUpdated = false;

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = context.now();

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
    catch (IllegalAccessException|InvocationTargetException|RuntimeException e)
      {
        log.error("failed deployment update subscriber");
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error(stackTraceWriter.toString());
      }

    /*****************************************
    *
    *  process subscriber profile force update
    *
    *****************************************/

    if (evolutionEvent instanceof SubscriberProfileForceUpdate)
      {
        //
        //  subscriberProfileForceUpdate
        //

        SubscriberProfileForceUpdate subscriberProfileForceUpdate = (SubscriberProfileForceUpdate) evolutionEvent;

        //
        //  evolutionSubscriberStatus
        //

        if (subscriberProfileForceUpdate.getParameterMap().containsKey("evolutionSubscriberStatus"))
          {
            EvolutionSubscriberStatus currentEvolutionSubscriberStatus = subscriberProfile.getEvolutionSubscriberStatus();
            EvolutionSubscriberStatus updatedEvolutionSubscriberStatus = EvolutionSubscriberStatus.fromExternalRepresentation((String) subscriberProfileForceUpdate.getParameterMap().get("evolutionSubscriberStatus"));
            if (currentEvolutionSubscriberStatus != updatedEvolutionSubscriberStatus)
              {
                subscriberProfile.setEvolutionSubscriberStatus(updatedEvolutionSubscriberStatus);
                subscriberProfile.setEvolutionSubscriberStatusChangeDate(subscriberProfileForceUpdate.getEventDate());
                subscriberProfile.setPreviousEvolutionSubscriberStatus(currentEvolutionSubscriberStatus);
                subscriberProfileUpdated = true;
              }
          }

        //
        //  language
        //

        if (subscriberProfileForceUpdate.getParameterMap().containsKey("language"))
          {
            SupportedLanguage supportedLanguage = Deployment.getSupportedLanguages().get((String) subscriberProfileForceUpdate.getParameterMap().get("language"));
            if (supportedLanguage != null)
              {
                subscriberProfile.setLanguageID(supportedLanguage.getID());
                subscriberProfileUpdated = true;
              }
          }

        //
        //  Hierarchy modifications 
        //
        
        if (subscriberProfileForceUpdate.getParameterMap().containsKey("subscriberRelationsUpdateMethod"))
          {
            SubscriberRelationsUpdateMethod method = SubscriberRelationsUpdateMethod.fromExternalRepresentation((String) subscriberProfileForceUpdate.getParameterMap().get("subscriberRelationsUpdateMethod"));
            String relationshipID = (String) subscriberProfileForceUpdate.getParameterMap().get("relationshipID");
            String relativeSubscriberID = (String) subscriberProfileForceUpdate.getParameterMap().get("relativeSubscriberID");
            SubscriberRelatives relatives = subscriberProfile.getRelations().get(relationshipID);
            
            if(relatives == null) {
              relatives = new SubscriberRelatives();
              subscriberProfile.getRelations().put(relationshipID, relatives);
              subscriberProfileUpdated = true;
            }
            
            if (method == SubscriberRelationsUpdateMethod.SetParent)
              {
                relatives.setParentSubscriberID(relativeSubscriberID);
                subscriberProfileUpdated = true;
              }
            else if (method == SubscriberRelationsUpdateMethod.AddChild)
              {
                relatives.addChildSubscriberID(relativeSubscriberID);
                subscriberProfileUpdated = true;
              }
            else if (method == SubscriberRelationsUpdateMethod.RemoveChild)
              {
                relatives.removeChildSubscriberID(relativeSubscriberID);
                subscriberProfileUpdated = true;
              }
          }

        //
        //  token
        //

        if (subscriberProfileForceUpdate.getParameterMap().containsKey("tokenCode"))
          {
            String tokenCode = (String) subscriberProfileForceUpdate.getParameterMap().get("tokenCode");
            TokenType defaultDNBOTokenType = getExternalTokenType();
            if(defaultDNBOTokenType == null) {
              log.error("Could not find any default token type for external token. Check your configuration.");
              return false;
            }
            DNBOToken subscriberStoredToken = new DNBOToken(tokenCode, subscriberProfile.getSubscriberID(), defaultDNBOTokenType);
            List<Token> currentTokens = subscriberProfile.getTokens();
            for (Token token : currentTokens)
              {
                if (tokenCode.equals(token.getTokenCode()))
                    {
                      log.info("Duplicated token code {} for subscriber, ignore it.", tokenCode);
                      return false;
                    }
              }
            String strategyID = (String) subscriberProfileForceUpdate.getParameterMap().get("tokenStrategy");
            subscriberStoredToken.setPresentationStrategyID(strategyID);            
            subscriberStoredToken.setCreationDate(now);

            //
            // AutoBounded
            //
            
            Boolean isAutoBoundedBoolean = (Boolean) subscriberProfileForceUpdate.getParameterMap().get("tokenAutoBounded");
            boolean isAutoBounded = false; // default value 
            if (isAutoBoundedBoolean != null)
              {
                isAutoBounded = isAutoBoundedBoolean;
              }
            subscriberStoredToken.setAutoBounded(isAutoBounded);

            //
            // AutoRedeemed
            //

            Boolean isAutoRedeemedBoolean = (Boolean) subscriberProfileForceUpdate.getParameterMap().get("tokenAutoRedeemed");
            boolean isAutoRedeemed = false; // default value 
            if (isAutoRedeemedBoolean != null)
              {
                isAutoRedeemed = isAutoRedeemedBoolean;
              }
            subscriberStoredToken.setAutoRedeemed(isAutoRedeemed);
            
            if (subscriberProfileForceUpdate.getParameterMap().containsKey("presentedOfferIDs"))
              {
                // this was sent by a journey with AutomaticAllocation or AutomaticRedeem
                List<String> presentedOfferIDs = (List<String>) subscriberProfileForceUpdate.getParameterMap().get("presentedOfferIDs");
                subscriberStoredToken.setPresentedOfferIDs(presentedOfferIDs);
              }            
            if (subscriberProfileForceUpdate.getParameterMap().containsKey("acceptedOffersID"))
              {
                // this was sent by a journey with AutomaticRedeem
                String acceptedOfferID = (String) subscriberProfileForceUpdate.getParameterMap().get("acceptedOffersID");
                subscriberStoredToken.setAcceptedOfferID(acceptedOfferID);
              }            

            //
            // Store new token
            //
            
            currentTokens.add(subscriberStoredToken);
            subscriberProfileUpdated = true;
          }
      }
    
    /*****************************************
    *
    *  update point balance
    *
    *****************************************/
    
    if (evolutionEvent instanceof PointFulfillmentRequest && ((PointFulfillmentRequest) evolutionEvent).getDeliveryStatus().equals(DeliveryStatus.Pending))
      {
        //
        //  pointFulfillmentRequest
        //

        PointFulfillmentRequest pointFulfillmentRequest = (PointFulfillmentRequest) evolutionEvent;
        PointFulfillmentRequest pointFulfillmentResponse = pointFulfillmentRequest.copy();

        //
        //  point
        //

        Point point = pointService.getActivePoint(pointFulfillmentRequest.getPointID(), now);
        if (point == null)
          {
            log.info("pointFulfillmentRequest failed (no such point): {}", pointFulfillmentRequest.getPointID());
            pointFulfillmentResponse.setDeliveryStatus(DeliveryStatus.Failed);
          }

        //
        //  update
        //

        if (point != null)
          {
            
            //
            // copy point and update point validity
            //
            
            Point newPoint = point.copy();
            if(pointFulfillmentRequest.getValidityPeriodType() != null && !pointFulfillmentRequest.getValidityPeriodType().equals(TimeUnit.Unknown) && pointFulfillmentRequest.getValidityPeriodQuantity() > 0){
              newPoint.getValidity().setPeriodType(pointFulfillmentRequest.getValidityPeriodType());
              newPoint.getValidity().setPeriodQuantity(pointFulfillmentRequest.getValidityPeriodQuantity());
            }
            
            //
            // set the validity in the response
            //
            pointFulfillmentResponse.setValidityPeriodType(newPoint.getValidity().getPeriodType());
            pointFulfillmentResponse.setValidityPeriodQuantity(newPoint.getValidity().getPeriodQuantity());

            //
            // update balance 
            //
            
            boolean success = updatePointBalance(context, subscriberProfile, newPoint, pointFulfillmentRequest.getOperation(), pointFulfillmentRequest.getAmount(), now);
            
            //
            //  response
            //

            if (success)
              {
                PointBalance pointBalance = subscriberProfile.getPointBalances().get(pointFulfillmentRequest.getPointID());
                pointFulfillmentResponse.setDeliveryStatus(DeliveryStatus.Delivered);
                pointFulfillmentResponse.setDeliveryDate(now);
                pointFulfillmentResponse.setResultValidityDate(pointBalance.getFirstExpirationDate(now));
              }
            else
              {
                pointFulfillmentResponse.setDeliveryStatus(DeliveryStatus.Failed);
              }

            //
            //  return delivery response
            //

            context.getSubscriberState().getPointFulfillmentResponses().add(pointFulfillmentResponse);

            //
            //  subscriberProfileUpdated
            //

            subscriberProfileUpdated = true;
          }
      }

    /*****************************************
    *
    *  re-evaluate subscriberGroups for epoch changes and eligibility/range segmentation dimensions
    *
    *****************************************/

    for (SegmentationDimension segmentationDimension :  segmentationDimensionService.getActiveSegmentationDimensions(now))
      {
        /*****************************************
        *
        *  evaluate dimension
        *    -- if not dependent on extendedSubscriberProfile
        *    -- or if extendedSubscriberProfile is available
        *
        *****************************************/

        if (! segmentationDimension.getDependentOnExtendedSubscriberProfile() || extendedSubscriberProfile != null)
          {
            //
            //  ignore if in temporal hole (segmentation dimension has been activated/updated but subscriberGroupEpochReader has not seen it yet)
            //

            SubscriberGroupEpoch subscriberGroupEpoch = subscriberGroupEpochReader.get(segmentationDimension.getSegmentationDimensionID());
            if (subscriberGroupEpoch != null && subscriberGroupEpoch.getEpoch() == segmentationDimension.getSubscriberGroupEpoch().getEpoch())
              {
                boolean inGroup = false;
                SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, extendedSubscriberProfile, subscriberGroupEpochReader, now);
                switch (segmentationDimension.getTargetingType())
                  {
                    case ELIGIBILITY:
                      SegmentationDimensionEligibility segmentationDimensionEligibility = (SegmentationDimensionEligibility) segmentationDimension;
                      for(SegmentEligibility segment : segmentationDimensionEligibility.getSegments())
                        {
                          boolean addSegment = !inGroup && EvaluationCriterion.evaluateCriteria(evaluationRequest, segment.getProfileCriteria());
                          subscriberProfile.setSegment(segmentationDimension.getSegmentationDimensionID(), segment.getID(), subscriberGroupEpoch.getEpoch(), addSegment);
                          if (addSegment) inGroup = true;
                          subscriberProfileUpdated = true;
                        }
                      break;

                    case RANGES:
                      SegmentationDimensionRanges segmentationDimensionRanges = (SegmentationDimensionRanges) segmentationDimension;
                      List<BaseSplit> baseSplitList = segmentationDimensionRanges.getBaseSplit();
                      if(baseSplitList != null && !baseSplitList.isEmpty()){
                        for(BaseSplit baseSplit : baseSplitList){

                          //
                          // evaluate criteria defined in baseSplit
                          //

                          boolean profileCriteriaEvaluation = EvaluationCriterion.evaluateCriteria(evaluationRequest, baseSplit.getProfileCriteria());

                          //
                          // get field value of criterion used for ranges
                          //

                          String variableName = baseSplit.getVariableName();
                          CriterionField baseMetric = CriterionContext.FullProfile.getCriterionFields().get(variableName);
                          Object normalized = baseMetric == null ? null : baseMetric.retrieveNormalized(evaluationRequest);

                          //
                          // check if the subscriber belongs to the segment
                          //

                          List<SegmentRanges> segmentList = baseSplit.getSegments();
                          for(SegmentRanges segment : segmentList){
                            boolean minValueOK =true;
                            boolean maxValueOK =true;
                            if(baseMetric != null){
                              CriterionDataType dataType = baseMetric.getFieldDataType();
                              switch (dataType) {
                                  case IntegerCriterion:
                                    if(segment.getRangeMin() != null)
                                      {
                                        minValueOK = (normalized != null) && (Long) normalized >= segment.getRangeMin(); //TODO SCH : FIX OPERATOR ... for now we are assuming it is like [valMin - valMax[ ...
                                      }
                                    if(segment.getRangeMax() != null)
                                      {
                                        maxValueOK = (normalized == null) || (Long) normalized < segment.getRangeMax(); //TODO SCH : FIX OPERATOR ... for now we are assuming it is like [valMin - valMax[ ...
                                      }
                                    break;

                                  case DoubleCriterion:
                                      {
                                        minValueOK = (normalized != null) && (Double) normalized >= segment.getRangeMin(); //TODO SCH : FIX OPERATOR ... for now we are assuming it is like [valMin - valMax[ ...
                                      }
                                    if(segment.getRangeMax() != null)
                                      {
                                        maxValueOK = (normalized == null) || (Double) normalized < segment.getRangeMax(); //TODO SCH : FIX OPERATOR ... for now we are assuming it is like [valMin - valMax[ ...
                                      }
                                    break;

                                  default: //TODO : will need to handle those dataTypes in a future version ...
                                    // StringCriterion
                                    // BooleanCriterion
                                    // DateCriterion
                                    // StringSetCriterion
                                    break;
                              }
                            }

                            //
                            // update subscriberGroup
                            //

                            boolean addSegment = !inGroup && minValueOK && maxValueOK && profileCriteriaEvaluation;
                            subscriberProfile.setSegment(segmentationDimension.getSegmentationDimensionID(), segment.getID(), subscriberGroupEpoch.getEpoch(), addSegment);
                            if (addSegment) inGroup = true;
                            subscriberProfileUpdated = true;
                          }
                        }
                      }
                      break;
                  }
              }
          }
      }

    /*****************************************
    *
    *  default segments (if necessary)
    *
    *****************************************/

    Map<String, String> segmentsMap = subscriberProfile.getSegmentsMap(subscriberGroupEpochReader);
    for (SegmentationDimension segmentationDimension :  segmentationDimensionService.getActiveSegmentationDimensions(now))
      {
        if (segmentsMap.get(segmentationDimension.getSegmentationDimensionID()) == null && segmentationDimension.getDefaultSegmentID() != null)
          {
            subscriberProfile.setSegment(segmentationDimension.getSegmentationDimensionID(), segmentationDimension.getDefaultSegmentID(), (subscriberGroupEpochReader.get(segmentationDimension.getSegmentationDimensionID()) != null ? subscriberGroupEpochReader.get(segmentationDimension.getSegmentationDimensionID()).getEpoch() : 0), true);
            subscriberProfileUpdated = true;
          }
      }
    
    /*****************************************
    *
    *  process file-sourced subscriberGroup event
    *
    *****************************************/

    if (evolutionEvent instanceof SubscriberGroup)
      {
        SubscriberGroup subscriberGroup = (SubscriberGroup) evolutionEvent;
        switch (subscriberGroup.getSubscriberGroupType())
          {
            case SegmentationDimension:
              String dimensionID = subscriberGroup.getSubscriberGroupIDs().get(0);
              String segmentID = subscriberGroup.getSubscriberGroupIDs().get(1);
              subscriberProfile.setSegment(dimensionID, segmentID, subscriberGroup.getEpoch(), subscriberGroup.getAddSubscriber());
              subscriberProfileUpdated = true;
              break;

            case Target:
              String targetID = subscriberGroup.getSubscriberGroupIDs().get(0);
              subscriberProfile.setTarget(targetID, subscriberGroup.getEpoch(), subscriberGroup.getAddSubscriber());
              subscriberProfileUpdated = true;
              break;
              
            case ExclusionInclusionTarget:
              String exclusionInclusionTargetID = subscriberGroup.getSubscriberGroupIDs().get(0);
              subscriberProfile.setExclusionInclusionTarget(exclusionInclusionTargetID, subscriberGroup.getEpoch(), subscriberGroup.getAddSubscriber());
              subscriberProfileUpdated = true;
              break;
          }
      }

    /*****************************************
    *
    *  ucg evaluation
    *
    *****************************************/

    UCGState ucgState = ucgStateReader.get(UCGState.getSingletonKey());
    if (ucgState != null && ucgState.getRefreshEpoch() != null)
      {
        //
        //  refreshUCG -- should we refresh the UCG status of this subscriber?
        //

        boolean refreshUCG = false;
        refreshUCG = refreshUCG || context.getSubscriberState().getUCGEpoch() == null;
        refreshUCG = refreshUCG || ! Objects.equals(context.getSubscriberState().getUCGRuleID(), ucgState.getUCGRuleID());
        refreshUCG = refreshUCG || context.getSubscriberState().getUCGEpoch() < ucgState.getRefreshEpoch();

        //
        //  refreshWindow -- is this subscriber outside the window where we can refresh the UCG?
        //

        boolean refreshWindow = false;
        refreshWindow = refreshWindow || context.getSubscriberState().getUCGRefreshDay() == null;
        refreshWindow = refreshWindow || RLMDateUtils.addDays(context.getSubscriberState().getUCGRefreshDay(), ucgState.getRefreshWindowDays(), Deployment.getBaseTimeZone()).compareTo(now) <= 0;

        //
        //  refresh if necessary
        //

        if (refreshUCG && refreshWindow)
          {
            /*****************************************
            *
            *  UCG calculations
            *
            *****************************************/

            boolean addToUCG = false;
            boolean removeFromUCG = false;

            //
            // Retrieve the user stratum for UCG dimensions only 
            //
            
            boolean isInUCG = subscriberProfile.getUniversalControlGroup();
            Set<String> userStratum = new HashSet<String>();
            Map<String, String> userSegmentsMap = subscriberProfile.getSegmentsMap(subscriberGroupEpochReader);
            for (String dimensionID : ucgState.getUCGRule().getSelectedDimensions())
              {
                userStratum.add(userSegmentsMap.get(dimensionID));
              }
            
            //
            // Retrieve stratum probability for a customer to change its state
            //  A positive number is the probability for a customer outside UCG to enter it.
            //  A negative number is the (opposite) probability for a customer already inside UCG to leave it.
            //  TODO This could be optimized with a map ?!
            //
            
            double shiftProbability = 0.0d;
            Iterator<UCGGroup> iterator = ucgState.getUCGGroups().iterator();
            while(iterator.hasNext()) 
              {
                UCGGroup g = iterator.next();
                if(g.getSegmentIDs().equals(userStratum)) 
                  {
                    if (g.getShiftProbability() != null) 
                      {
                        shiftProbability = g.getShiftProbability();
                      }
                    // TODO What if shift probability is null ? manually re-compute it ?
                    break;
                  }
              }

            if(shiftProbability < 0 && isInUCG) 
              {
                // If there is already too much customers in the Universal Control Group.
                ThreadLocalRandom random = ThreadLocalRandom.current();
                removeFromUCG = (random.nextDouble() < -shiftProbability);
              }
            else if(shiftProbability > 0 && !isInUCG) 
              {
                // If there is not enough customers in the Universal Control Group.
                ThreadLocalRandom random = ThreadLocalRandom.current();
                addToUCG = (random.nextDouble() < shiftProbability);
              }

            /*****************************************
            *
            *  add/remove from UCG
            *
            *****************************************/

            if (addToUCG) subscriberProfile.setUniversalControlGroup(true);
            if (removeFromUCG) subscriberProfile.setUniversalControlGroup(false);
            context.getSubscriberState().setUCGState(ucgState, now);
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
  *  updatePointBalance
  *
  *****************************************/

  private static boolean updatePointBalance(EvolutionEventContext context, SubscriberProfile subscriberProfile, Point point, PointOperation operation, int amount, Date now)
  {

    //
    //  get (or create) balance
    //

    PointBalance pointBalance = subscriberProfile.getPointBalances().get(point.getPointID());
    if (pointBalance == null)
      {
        pointBalance = new PointBalance();
      }

    //
    //  copy the point balance (note:  NOT deep-copied when the subscriberProfile was copied)
    //

    pointBalance = new PointBalance(pointBalance);

    //
    //  update
    //

    boolean success = pointBalance.update(context, subscriberProfile.getSubscriberID(), operation, amount, point, now);

    //
    //  update balances
    //

    subscriberProfile.getPointBalances().put(point.getPointID(), pointBalance);
    
    //
    //  return
    //
    
    return success;

  }
  
  /*****************************************
  *
  *  updateSubscriberLoyaltyProgram
  *
  *****************************************/

  private static boolean updateSubscriberLoyaltyProgram(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent)
  {
    /*****************************************
    *
    *  result
    *
    *****************************************/

    SubscriberState subscriberState = context.getSubscriberState();
    SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();
    ExtendedSubscriberProfile extendedSubscriberProfile = context.getExtendedSubscriberProfile();
    boolean subscriberProfileUpdated = false;

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = context.now();

    /*****************************************
    *
    *  enter/exit loyalty program
    *
    *****************************************/

    if (evolutionEvent instanceof LoyaltyProgramRequest && ((LoyaltyProgramRequest) evolutionEvent).getDeliveryStatus().equals(DeliveryStatus.Pending))
      {

        //
        //  get loyaltyProgramRequest
        //

        LoyaltyProgramRequest loyaltyProgramRequest = (LoyaltyProgramRequest) evolutionEvent;
        LoyaltyProgramRequest loyaltyProgramResponse = loyaltyProgramRequest.copy();

        //
        //  get loyaltyProgram
        //

        LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramRequest.getLoyaltyProgramID(), now);
        if (loyaltyProgram == null)
          {
            log.info("loyaltyProgramRequest failed (no such loyalty program): {}", loyaltyProgramRequest.getLoyaltyProgramID());
            loyaltyProgramResponse.setDeliveryStatus(DeliveryStatus.Failed);
          }

        //
        //  update
        //

        else
          {

            boolean success = false;

            switch (loyaltyProgram.getLoyaltyProgramType()) {
            case POINTS:

              //
              //  get loyaltyProgramPoints
              //

              LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints) loyaltyProgram;

              //
              //  compute and update loyalty program state
              //

              switch (loyaltyProgramRequest.getOperation()) {
              case Optin:

                //
                //  determine tier
                //

                String newTierName = determineLoyaltyProgramPointsTier(subscriberProfile, loyaltyProgramPoints, now);

                //
                //  get current loyalty program state
                //

                LoyaltyProgramState currentLoyaltyProgramState = subscriberProfile.getLoyaltyPrograms().get(loyaltyProgramRequest.getLoyaltyProgramID());
                if (currentLoyaltyProgramState == null || !(currentLoyaltyProgramState instanceof LoyaltyProgramPointsState))
                  {
                    LoyaltyProgramHistory loyaltyProgramHistory = new LoyaltyProgramHistory(loyaltyProgram.getLoyaltyProgramID());
                    currentLoyaltyProgramState = new LoyaltyProgramPointsState(LoyaltyProgramType.POINTS, loyaltyProgram.getEpoch(), loyaltyProgram.getLoyaltyProgramName(), now, null, newTierName, null, now, loyaltyProgramHistory);
                  }

                //
                //  update loyalty program state
                //

                log.info("new loyaltyProgramRequest : for subscriber '"+subscriberProfile.getSubscriberID()+"' : loyaltyProgramState.update("+loyaltyProgram.getEpoch()+", "+loyaltyProgramRequest.getOperation()+", "+loyaltyProgram.getLoyaltyProgramName()+", "+newTierName+", "+now+", "+loyaltyProgramRequest.getDeliveryRequestID()+")");
                ((LoyaltyProgramPointsState)currentLoyaltyProgramState).update(loyaltyProgram.getEpoch(), loyaltyProgramRequest.getOperation(), loyaltyProgram.getLoyaltyProgramName(), newTierName, now, loyaltyProgramRequest.getDeliveryRequestID());

                //
                //  update subscriber loyalty programs state
                //

                subscriberProfile.getLoyaltyPrograms().put(loyaltyProgramRequest.getLoyaltyProgramID(), currentLoyaltyProgramState);
                
                //
                //  generate new event (opt-in)
                //
                
                ParameterMap infos = new ParameterMap();
                infos.put(LoyaltyProgramPointsEventInfos.ENTERING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                infos.put(LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation(), null);
                infos.put(LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation(), newTierName);
                ProfileLoyaltyProgramChangeEvent profileChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), infos);
                subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileChangeEvent);
                
                //
                //  return
                //
                
                success = true;

                break;

              case Optout:

                //
                //  determine tier
                //

                String tierName = null;

                //
                //  get current loyalty program state
                //

                LoyaltyProgramState loyaltyProgramState = subscriberProfile.getLoyaltyPrograms().get(loyaltyProgramRequest.getLoyaltyProgramID());

                if (loyaltyProgramState == null)
                  {
                    LoyaltyProgramHistory loyaltyProgramHistory = new LoyaltyProgramHistory(loyaltyProgram.getLoyaltyProgramID());
                    loyaltyProgramState = new LoyaltyProgramPointsState(LoyaltyProgramType.POINTS, loyaltyProgram.getEpoch(), loyaltyProgram.getLoyaltyProgramName(), now, null, tierName, null, now, loyaltyProgramHistory);
                  }

                String oldTier = ((LoyaltyProgramPointsState)loyaltyProgramState).getTierName();
                //
                //  update loyalty program state
                //

                log.info("new loyaltyProgramRequest : for subscriber '"+subscriberProfile.getSubscriberID()+"' : loyaltyProgramState.update("+loyaltyProgram.getEpoch()+", "+loyaltyProgramRequest.getOperation()+", "+loyaltyProgram.getLoyaltyProgramName()+", "+tierName+", "+now+", "+loyaltyProgramRequest.getDeliveryRequestID()+")");
                ((LoyaltyProgramPointsState)loyaltyProgramState).update(loyaltyProgram.getEpoch(), loyaltyProgramRequest.getOperation(), loyaltyProgram.getLoyaltyProgramName(), tierName, now, loyaltyProgramRequest.getDeliveryRequestID());

                //
                //  update subscriber loyalty programs state
                //

                subscriberProfile.getLoyaltyPrograms().put(loyaltyProgramRequest.getLoyaltyProgramID(), loyaltyProgramState);
                
                //
                //  generate new event (opt-out)
                //
                
                ParameterMap info = new ParameterMap();
                info.put(LoyaltyProgramPointsEventInfos.LEAVING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                info.put(LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation(), oldTier);
                info.put(LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation(), tierName);
                ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);
                
                //
                //  return
                //
                
                success = true;

                break;

              default:
                break;
              }

              break;

            case BADGES:
              // TODO
              break;

            default:
              break;
            }

            //
            //  response
            //

            if (success)
              {
                loyaltyProgramResponse.setDeliveryStatus(DeliveryStatus.Delivered);
                loyaltyProgramResponse.setDeliveryDate(now);
              }
            else
              {
                loyaltyProgramResponse.setDeliveryStatus(DeliveryStatus.Failed);
              }

            //
            //  return delivery response
            //

            context.getSubscriberState().getLoyaltyProgramResponses().add(loyaltyProgramResponse);

            //
            //  subscriberProfileUpdated
            //

            subscriberProfileUpdated = true;
          }

      }
    
    /*****************************************
    *
    *  update loyalty program
    *
    *****************************************/

    else 
      {
      
        //
        //  check all subscriber loyalty programs
        //
        
        for(String loyaltyProgramID : subscriberProfile.getLoyaltyPrograms().keySet()){
          
          //
          //  get current subscriber state in the loyalty program
          //

          LoyaltyProgramState loyaltyProgramState = subscriberProfile.getLoyaltyPrograms().get(loyaltyProgramID);

          //
          //  get loyalty program definition
          //
          
          LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramID, now);
                    
          //
          //  check that the subscriber is still in the program (no LoyaltyProgramExitDate)
          //
          
          if(loyaltyProgram != null && loyaltyProgram.getLoyaltyProgramType().equals(loyaltyProgramState.getLoyaltyProgramType()) && subscriberProfile.getLoyaltyPrograms().get(loyaltyProgramID).getLoyaltyProgramExitDate() == null){
              
              switch (loyaltyProgram.getLoyaltyProgramType()) {
              case POINTS:
                
                //
                //  get loyaltyProgramPoints
                //
                
                LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints) loyaltyProgram;

                //
                //  get subscriber current tier
                //
                
                Tier subscriberCurrentTierDefinition = null;
                for(Tier tier : loyaltyProgramPoints.getTiers()){
                  if(tier.getTierName().equals(((LoyaltyProgramPointsState)loyaltyProgramState).getTierName())){
                    subscriberCurrentTierDefinition = tier;
                  }
                }

                //
                //  update loyalty program status 
                //

                EvolutionEngineEventDeclaration statusEventDeclaration = Deployment.getEvolutionEngineEvents().get(subscriberCurrentTierDefinition.getStatusEventName()) ;
                if (statusEventDeclaration != null && statusEventDeclaration.getEventClassName().equals(evolutionEvent.getClass().getName()) && evolutionEvent instanceof LoyaltyProgramPointsEvent)
                  {

                    //
                    //  update status points
                    //

                    Point point = pointService.getActivePoint(loyaltyProgramPoints.getStatusPointsID(), now);
                    if(point != null)
                      {
                        
                        log.info("update loyalty program STATUS => adding "+((LoyaltyProgramPointsEvent)evolutionEvent).getUnit()+" x "+subscriberCurrentTierDefinition.getNumberOfStatusPointsPerUnit()+" of point "+point.getPointName());
                        int amount = ((LoyaltyProgramPointsEvent)evolutionEvent).getUnit() * subscriberCurrentTierDefinition.getNumberOfStatusPointsPerUnit();
                        updatePointBalance(context, subscriberProfile, point, PointOperation.Credit, amount, now);
                        subscriberProfileUpdated = true;

                      }
                    else
                      {
                        log.info("update loyalty program STATUS : point with ID '"+loyaltyProgramPoints.getStatusPointsID()+"' not found");
                      }

                    //
                    //  update tier
                    //
                    
                    String oldTier = ((LoyaltyProgramPointsState)loyaltyProgramState).getTierName();
                    String newTier = determineLoyaltyProgramPointsTier(subscriberProfile, loyaltyProgramPoints, now);
                    if(!oldTier.equals(newTier)){
                      ((LoyaltyProgramPointsState)loyaltyProgramState).update(loyaltyProgram.getEpoch(), LoyaltyProgramOperation.Optin, loyaltyProgram.getLoyaltyProgramName(), newTier, now, evolutionEvent.getClass().getName());
                      
                      //
                      //  generate new event (tier changed)
                      //
                      
                      ParameterMap info = new ParameterMap();
                      info.put(LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation(), oldTier);
                      info.put(LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation(), newTier);
                      ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                      subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);
                      
                    }

                  }

                //
                //  update loyalty program reward
                //

                EvolutionEngineEventDeclaration rewardEventDeclaration = Deployment.getEvolutionEngineEvents().get(subscriberCurrentTierDefinition.getRewardEventName()) ;
                if(rewardEventDeclaration != null && rewardEventDeclaration.getEventClassName().equals(evolutionEvent.getClass().getName()) && evolutionEvent instanceof LoyaltyProgramPointsEvent)
                  {

                    //  update reward points
                    
                    Point point = pointService.getActivePoint(loyaltyProgramPoints.getRewardPointsID(), now);
                    if(point != null)
                      {
                        
                        log.info("update loyalty program REWARD => adding "+((LoyaltyProgramPointsEvent)evolutionEvent).getUnit()+" x "+subscriberCurrentTierDefinition.getNumberOfRewardPointsPerUnit()+" of point with ID "+loyaltyProgramPoints.getRewardPointsID());
                        int amount = ((LoyaltyProgramPointsEvent)evolutionEvent).getUnit() * subscriberCurrentTierDefinition.getNumberOfRewardPointsPerUnit();
                        updatePointBalance(context, subscriberProfile, point, PointOperation.Credit, amount, now);
                        subscriberProfileUpdated = true;
                        
                      }
                    else
                      {
                        log.info("update loyalty program STATUS : point with ID '"+loyaltyProgramPoints.getRewardPointsID()+"' not found");
                      }

                  }
                break;

              case BADGES:
                // TODO
                break;
                
              default:
                break;
              }
              
            
          }
          
        }
        
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return subscriberProfileUpdated;
  }
  
  /*****************************************
  *
  *  determineLoyaltyProgramPointsTier
  *
  *****************************************/

  private static String determineLoyaltyProgramPointsTier(SubscriberProfile subscriberProfile, LoyaltyProgramPoints loyaltyProgramPoints, Date now){

    //
    //  determine tier
    //

    String newTierName = null;
    int currentStatusPointBalance = 0;
    if(subscriberProfile.getPointBalances() != null && subscriberProfile.getPointBalances().get(loyaltyProgramPoints.getStatusPointsID()) != null){
      currentStatusPointBalance = subscriberProfile.getPointBalances().get(loyaltyProgramPoints.getStatusPointsID()).getBalance(now);
    }
    for(Tier tier : loyaltyProgramPoints.getTiers()){
      if(currentStatusPointBalance >= tier.getStatusPointLevel()){
        newTierName = tier.getTierName();
      }
    }
    
    return newTierName;

  }

  /*****************************************
  *
  *  updatePropensity
  *
  *****************************************/

  private static boolean updatePropensity(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent)
  {
    /*****************************************
    *
    *  result
    *
    *****************************************/

    SubscriberState subscriberState = context.getSubscriberState();
    SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();
    boolean subscriberStateUpdated = false;

    /*****************************************
    *
    *  presentation and acceptance evaluation
    *
    *****************************************/

    if (evolutionEvent instanceof PresentationLog || evolutionEvent instanceof AcceptanceLog)
    {
      String eventTokenCode = null;
      List<Token> subscriberTokens = subscriberProfile.getTokens();
      DNBOToken subscriberStoredToken = null;
      TokenType defaultDNBOTokenType = getExternalTokenType();
      if(defaultDNBOTokenType == null) {
        log.error("Could not find any default token type for external token. Check your configuration.");
        return false;
      }

      //
      // Retrieve the token-code we are looking for, from the event log.
      //

      if(evolutionEvent instanceof PresentationLog) {
        eventTokenCode = ((PresentationLog) evolutionEvent).getPresentationToken();
      } else if(evolutionEvent instanceof AcceptanceLog) {
        eventTokenCode = ((AcceptanceLog) evolutionEvent).getPresentationToken();
      }

      //
      // Subscriber token list cleaning.
      // We will delete all already expired tokens before doing anything.
      //

      List<Token> cleanedList = new ArrayList<Token>();
      boolean changed = false;
      Date now = SystemTime.getCurrentTime();
      for(Token token : subscriberTokens) {
        if(token.getTokenExpirationDate().before(now)) {
          changed = true;
          break;
        }
        cleanedList.add(token);
      }

      if(changed) {
        subscriberProfile.setTokens(cleanedList);
        subscriberTokens = cleanedList;
        subscriberStateUpdated = true;
      }

      //
      // Retrieving the corresponding token from the subscriber token list, if it already exists.
      // We expect a DNBOToken, otherwise it means that there is a conflict with a token stored in the subscriber token list.
      // Maybe we had already generated an other kind of token with the exact same token-code that this one (that has been created outside).
      // If it happens, we ignore this new token and raise an error (that's the best we can do ATM but it could change later)
      //

      for(Token token : subscriberTokens) {
        if(Objects.equals(eventTokenCode, token.getTokenCode())) {
          if(token instanceof DNBOToken) {

            subscriberStoredToken = (DNBOToken) token;
          } else {
            log.error("Unexpected type (" + token.getClass().getName() + ") for an already existing token " + token);
            return subscriberStateUpdated;
          }
          break;
        }
      }

      //
      // We start by creating a new token if it does not exist in Evolution (if it has been created by an outside system)
      //

      if(subscriberStoredToken == null) {
        subscriberStoredToken = new DNBOToken(eventTokenCode, subscriberProfile.getSubscriberID(), defaultDNBOTokenType);
        subscriberTokens.add(subscriberStoredToken);
        subscriberStateUpdated = true;
      }

      //
      // Update the token with the incoming event
      //

      if(evolutionEvent instanceof PresentationLog) {

        //
        // Presentation event update
        // If token created outside Evolution, we do not know its actual creation date, so assume created when it was bound with offers
        //

        PresentationLog presentationLog = (PresentationLog) evolutionEvent;

        if(subscriberStoredToken.getTokenStatus() == TokenStatus.New) {
          subscriberStoredToken.setTokenStatus(TokenStatus.Bound);
          subscriberStateUpdated = true;
        }
        Date eventDate = presentationLog.getEventDate();
        if(subscriberStoredToken.getCreationDate() == null) {
          subscriberStoredToken.setCreationDate(eventDate);
          subscriberStoredToken.setTokenExpirationDate(defaultDNBOTokenType.getExpirationDate(eventDate));
          subscriberStateUpdated = true;
        }
        if(subscriberStoredToken.getBoundDate() == null || subscriberStoredToken.getBoundDate().before(eventDate)) {
          subscriberStoredToken.setBoundDate(eventDate);
          subscriberStateUpdated = true;
        }
        int boundCount = subscriberStoredToken.getBoundCount();
        Integer maxNumberofPlaysInt = defaultDNBOTokenType.getMaxNumberOfPlays();
        int maxNumberofPlays = (maxNumberofPlaysInt == null) ? Integer.MAX_VALUE : maxNumberofPlaysInt.intValue();
        if (boundCount < maxNumberofPlays)
          {
            subscriberStoredToken.setBoundCount(boundCount+1); // no concurrency issue as a given subscriber is always handled by the same partition/evolution engine instance, sequentially
            subscriberStoredToken.setPresentedOfferIDs(presentationLog.getOfferIDs()); // replace whatever was there 
            subscriberStateUpdated = true;
          }
      } else if(evolutionEvent instanceof AcceptanceLog) {

        //
        // Acceptance event update
        //

        AcceptanceLog acceptanceLog = (AcceptanceLog) evolutionEvent;

        if(subscriberStoredToken.getAcceptedOfferID() != null) {
          log.error("Unexpected acceptance record ("+ acceptanceLog.toString() +") for a token ("+ subscriberStoredToken.toString() +") already redeemed by a previous acceptance record");
          return subscriberStateUpdated;
        } else {
          subscriberStoredToken.setTokenStatus(TokenStatus.Redeemed);
          subscriberStoredToken.setRedeemedDate(acceptanceLog.getEventDate());
          subscriberStoredToken.setAcceptedOfferID(acceptanceLog.getOfferID());
        }
        subscriberStateUpdated = true;
      }

      //
      // Extract propensity information (only if we already acknowledged both Presentation & Acceptance events)
      //
      
      if(subscriberStoredToken.getPresentedOfferIDs().size() > 0 &&
          subscriberStoredToken.getAcceptedOfferID() != null) {
            
        // 
        // Validate propensity rule before using it (ignore any propensity outputs otherwise)
        //
        
        if(Deployment.getPropensityRule().validate(segmentationDimensionService))
          {
            subscriberState.getPropensityOutputs().addAll(retrievePropensityOutputs(subscriberStoredToken, subscriberProfile));
            subscriberStateUpdated = true;
          }
      }
    }

    return subscriberStateUpdated;
  }

  /*****************************************
  *
  *  retrievePropensityOutputs
  *
  *   once we have acknowledge both presentation & acceptance for a token, we can extract a list of propensity outputs.
  *
  ****************************************/

  private static List<PropensityEventOutput> retrievePropensityOutputs(DNBOToken token, SubscriberProfile subscriberProfile)
  {
    List<PropensityEventOutput> result = new ArrayList<PropensityEventOutput>();
    for(String offerID: token.getPresentedOfferIDs())
      {
        result.add(new PropensityEventOutput(new PropensityKey(offerID, subscriberProfile, subscriberGroupEpochReader), offerID.equals(token.getAcceptedOfferID())));
      }
    return result;
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
    *  determine permitted journeys by objective
    *
    *****************************************/

    //
    //  permitted journeys by limiting criteria
    //

    Map<JourneyObjective, Integer> permittedSimultaneousJourneys = new HashMap<JourneyObjective, Integer>();
    Map<JourneyObjective, Boolean> permittedWaitingPeriod = new HashMap<JourneyObjective, Boolean>();
    Map<JourneyObjective, Integer> permittedSlidingWindowJourneys = new HashMap<JourneyObjective, Integer>();
    for (JourneyState journeyState : Sets.union(subscriberState.getJourneyStates(), subscriberState.getRecentJourneyStates()))
      {
        //
        //  candidate journey
        //

        Journey candidateJourney = journeyService.getActiveJourney(journeyState.getJourneyID(), now);
        if (candidateJourney == null) continue;
        boolean activeJourney = subscriberState.getJourneyStates().contains(journeyState);

        //
        //  process journey objectives
        //

        Set<JourneyObjective> journeyObjectives = candidateJourney.getAllObjectives(journeyObjectiveService, now);
        for (JourneyObjective journeyObjective : journeyObjectives)
          {
            //
            //  ensure data structures
            //

            if (! permittedSimultaneousJourneys.containsKey(journeyObjective))
              {
                permittedSimultaneousJourneys.put(journeyObjective, journeyObjective.getEffectiveTargetingLimitMaxSimultaneous());
                permittedWaitingPeriod.put(journeyObjective, Boolean.TRUE);
                permittedSlidingWindowJourneys.put(journeyObjective, journeyObjective.getEffectiveTargetingLimitMaxOccurrence());
              }

            //
            //  update
            //

            if (activeJourney) permittedSimultaneousJourneys.put(journeyObjective, permittedSimultaneousJourneys.get(journeyObjective) - 1);
            if (activeJourney || journeyState.getJourneyExitDate().compareTo(journeyObjective.getEffectiveWaitingPeriodEndDate(now)) >= 0) permittedWaitingPeriod.put(journeyObjective, Boolean.FALSE);
            if (activeJourney || journeyState.getJourneyExitDate().compareTo(journeyObjective.getEffectiveSlidingWindowStartDate(now)) >= 0) permittedSlidingWindowJourneys.put(journeyObjective, permittedSlidingWindowJourneys.get(journeyObjective) - 1);
          }
      }

    //
    //  permitted journeys
    //

    Map<JourneyObjective, Integer> permittedJourneys = new HashMap<JourneyObjective, Integer>();
    for (JourneyObjective journeyObjective : permittedSimultaneousJourneys.keySet())
      {
        permittedJourneys.put(journeyObjective, Math.max(Math.min(permittedSimultaneousJourneys.get(journeyObjective), permittedSlidingWindowJourneys.get(journeyObjective)), 0));
        if (permittedWaitingPeriod.get(journeyObjective) == Boolean.FALSE) permittedJourneys.put(journeyObjective, 0);
      }

    /*****************************************
    *
    *  update JourneyState(s) to enter new journeys
    *
    *****************************************/

    List<Journey> activeJourneys = new ArrayList<Journey>(journeyService.getActiveJourneys(now));
    Collections.shuffle(activeJourneys, ThreadLocalRandom.current());
    
    /*****************************************
    *
    *  inclusion/exclusion lists
    *
    *****************************************/

    SubscriberEvaluationRequest inclusionExclusionEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, now);
    boolean inclusionList = (activeJourneys.size() > 0) ? subscriberState.getSubscriberProfile().getInInclusionList(inclusionExclusionEvaluationRequest, exclusionInclusionTargetService, subscriberGroupEpochReader, now) : false;
    boolean exclusionList = (activeJourneys.size() > 0) ? subscriberState.getSubscriberProfile().getInExclusionList(inclusionExclusionEvaluationRequest, exclusionInclusionTargetService, subscriberGroupEpochReader, now) : false;
    context.getSubscriberTraceDetails().addAll(inclusionExclusionEvaluationRequest.getTraceDetails());
    
    for (Journey journey : activeJourneys)
      {
        //
        //  entry period
        //

        if (now.compareTo(journey.getEffectiveEntryPeriodEndDate()) >= 0)
          {
            continue;
          }

        //
        //  called journey?
        //

        boolean calledJourney = true;
        calledJourney = calledJourney && evolutionEvent instanceof JourneyRequest;
        calledJourney = calledJourney && Objects.equals(((JourneyRequest) evolutionEvent).getJourneyID(), journey.getJourneyID());
        calledJourney = calledJourney && ! journey.getAutoTargeted();

        //
        //  enter journey?
        //

        if (calledJourney || journey.getAutoTargeted())
          {
            /*****************************************
            *
            *  retrieve relevant journey objectives, ensuring all are in permittedJourneys
            *
            *****************************************/

            Set<JourneyObjective> allObjectives = journey.getAllObjectives(journeyObjectiveService, now);
            for (JourneyObjective journeyObjective : allObjectives)
              {
                if (! permittedJourneys.containsKey(journeyObjective))
                  {
                    permittedJourneys.put(journeyObjective, Math.max(Math.min(journeyObjective.getEffectiveTargetingLimitMaxSimultaneous(), journeyObjective.getEffectiveTargetingLimitMaxOccurrence()), 0));
                  }
              }

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
                        context.subscriberTrace("NotEligible: already in journey {0}", journey.getJourneyID());
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
                        Date journeyReentryWindow = EvolutionUtilities.addTime(journeyState.getJourneyExitDate(), Deployment.getJourneyDefaultTargetingWindowDuration(), Deployment.getJourneyDefaultTargetingWindowUnit(), Deployment.getBaseTimeZone(), Deployment.getJourneyDefaultTargetingWindowRoundUp() ? RoundingSelection.RoundUp : RoundingSelection.NoRound);
                        if (journeyReentryWindow.after(now))
                          {
                            context.subscriberTrace("NotEligible: recently in journey {0}, window ends {1}", journey.getJourneyID(), journeyReentryWindow);
                            enterJourney = false;
                          }
                      }
                  }
              }

            /*****************************************
            *
            *  verify pass all objective-level targeting policies
            *
            *****************************************/

            if (enterJourney)
              {
                for (JourneyObjective journeyObjective : allObjectives)
                  {
                    if (permittedJourneys.get(journeyObjective) < 1)
                      {
                        enterJourney = false;
                        context.subscriberTrace("NotEligible: journey {0}, objective {1}", journey.getJourneyID(), journeyObjective.getJourneyObjectiveID());
                        break;
                      }
                  }
              }

            /*****************************************
            *
            *  pass is customer UCG?
            *
            *****************************************/

            if (enterJourney)
              {
                switch (journey.getTargetingType())
                  {
                    case Target:
                      if (subscriberState.getSubscriberProfile().getUniversalControlGroup())
                        {
                          enterJourney = false;
                          context.subscriberTrace("NotEligible: user is UCG {0}", journey.getJourneyID());
                        }
                      break;
                  }
              }
            
            /******************************************
            *
            *  pass is customer in the exclusion list?
            *
            *******************************************/

            if (enterJourney)
              {
                switch (journey.getTargetingType())
                  {
                    case Target:
                      if (exclusionList)
                        {
                          enterJourney = false;
                          context.subscriberTrace("NotEligible: user is in exclusion list {0}", journey.getJourneyID());
                        }
                      break;
                  }
              }

            /*********************************************
            *
            *  pass targeting criteria and inclusion list
            *
            **********************************************/

            if (enterJourney)
              {
                SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, now);
                boolean targetingCriteria = EvaluationCriterion.evaluateCriteria(evaluationRequest, journey.getAllCriteria(targetService, now));
                switch (journey.getTargetingType())
                  {
                    case Target:
                      if (! inclusionList && ! targetingCriteria)
                        {
                          enterJourney = false;
                          context.getSubscriberTraceDetails().addAll(evaluationRequest.getTraceDetails());
                          context.subscriberTrace("NotEligible: targeting criteria / inclusion list {0}", journey.getJourneyID());
                        }
                      break;

                    case Event:
                    case Manual:
                      if (! targetingCriteria)
                        {
                          enterJourney = false;
                          context.getSubscriberTraceDetails().addAll(evaluationRequest.getTraceDetails());
                          context.subscriberTrace("NotEligible: targeting criteria {0}", journey.getJourneyID());
                        }
                      break;
                  }
              }

            /*****************************************
            *
            *  enterJourney
            *
            *****************************************/

            if (enterJourney)
              {
                /*****************************************
                *
                *  subscriberTrace
                *
                *****************************************/

                context.subscriberTrace("Eligible: {0}", journey.getJourneyID());

                /*****************************************
                *
                *  enterJourney -- all journeys
                *
                *****************************************/

                JourneyHistory journeyHistory = new JourneyHistory(journey.getJourneyID());
                JourneyState journeyState = new JourneyState(context, journey, journey.getBoundParameters(), SystemTime.getCurrentTime(), journeyHistory);
                journeyState.getJourneyHistory().addNodeInformation(null, journeyState.getJourneyNodeID(), null, null);
                
                boolean statusUpdated = journeyState.getJourneyHistory().addStatusInformation(SystemTime.getCurrentTime(),journeyState, false);
                subscriberState.getJourneyStates().add(journeyState);
                subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(
                    subscriberState.getSubscriberProfile(),
                    subscriberGroupEpochReader,
                    ucgStateReader,
                    statusUpdated,
                    new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState)));
                subscriberStateUpdated = true;

                /*****************************************
                *
                *  update permittedJourneys
                *
                *****************************************/

                for (JourneyObjective journeyObjective : allObjectives)
                  {
                    permittedJourneys.put(journeyObjective, permittedJourneys.get(journeyObjective) - 1);
                  }

                /*****************************************
                *
                *  populate journeyMetrics (prior and "during")
                *
                *****************************************/

                for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values())
                  {
                    //
                    //  metricHistory
                    //

                    MetricHistory metricHistory = journeyMetricDeclaration.getMetricHistory(subscriberState.getSubscriberProfile());

                    //
                    //  prior
                    //

                    Date journeyEntryDay = RLMDateUtils.truncate(journeyState.getJourneyEntryDate(), Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                    Date metricStartDay = RLMDateUtils.addDays(journeyEntryDay, -1 * journeyMetricDeclaration.getPriorPeriodDays(), Deployment.getBaseTimeZone());
                    Date metricEndDay = RLMDateUtils.addDays(journeyEntryDay, -1, Deployment.getBaseTimeZone());
                    long priorMetricValue = metricHistory.getValue(metricStartDay, metricEndDay);
                    journeyState.getJourneyMetricsPrior().put(journeyMetricDeclaration.getID(), priorMetricValue);

                    //
                    //  during (note:  at entry these are set to the "all-time-total" and will be fixed up when the journey ends
                    //

                    Long startMetricValue = metricHistory.getAllTimeBucket();
                    journeyState.getJourneyMetricsDuring().put(journeyMetricDeclaration.getID(), startMetricValue);
                    subscriberStateUpdated = true;
                  }

                /*****************************************
                *
                *  enterJourney -- called journey
                *
                *****************************************/

                if (calledJourney)
                  {
                    //
                    //  journey request
                    //

                    JourneyRequest journeyRequest = (JourneyRequest) evolutionEvent;

                    //
                    //  mark eligible
                    //

                    journeyRequest.setEligible(true);

                    //
                    //  update calling journeyState with journeyInstanceID
                    //

                    for (JourneyState waitingJourneyState : subscriberState.getJourneyStates())
                      {
                        if (waitingJourneyState.getJourneyOutstandingJourneyRequestID() != null && Objects.equals(waitingJourneyState.getJourneyOutstandingJourneyRequestID(), journeyRequest.getJourneyRequestID()))
                          {
                            waitingJourneyState.setJourneyOutstandingJourneyInstanceID(journeyState.getJourneyInstanceID());
                          }
                      }
                  }
              }
            
            /*****************************************
            *
            *  journey response  -- called journey
            *
            *****************************************/
            
            if (calledJourney)
              {
                JourneyRequest journeyRequest = (JourneyRequest) evolutionEvent;
                if (journeyRequest.isPending())
                  {
                    JourneyRequest journeyResponse = journeyRequest.copy();
                    if(enterJourney)
                      {
                        journeyResponse.setDeliveryStatus(DeliveryStatus.Delivered);
                        journeyResponse.setDeliveryDate(now);
                      }
                    else
                      {
                        journeyResponse.setDeliveryStatus(DeliveryStatus.Failed);
                      }
                    context.getSubscriberState().getJourneyResponses().add(journeyResponse);
                    subscriberStateUpdated = true;
                  }
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
        *   get reward information 
        *
        *****************************************/
        if (evolutionEvent instanceof DeliveryRequest && !((DeliveryRequest)evolutionEvent).getDeliveryStatus().equals(DeliveryStatus.Pending)) 
          {
            DeliveryRequest deliveryResponse = (DeliveryRequest) evolutionEvent;
            if (Objects.equals(deliveryResponse.getModuleID(), DeliveryRequest.Module.Journey_Manager.getExternalRepresentation()) && Objects.equals(deliveryResponse.getFeatureID(), journeyState.getJourneyID()))
              {
                RewardHistory lastRewards = journeyState.getJourneyHistory().addRewardInformation(deliveryResponse);
                if(lastRewards != null)
                  {
                    subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(
                        subscriberState.getSubscriberProfile(),
                        subscriberGroupEpochReader,
                        ucgStateReader,
                        new RewardHistory(lastRewards),
                        journeyState.getJourneyID()));
                  }
              }
          }
        
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
            boolean statusUpdated = journeyState.getJourneyHistory().addStatusInformation(SystemTime.getCurrentTime(), journeyState, true);
            subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(
                subscriberState.getSubscriberProfile(),
                subscriberGroupEpochReader,
                ucgStateReader,
                statusUpdated,
                new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, now)));
            inactiveJourneyStates.add(journeyState);
            continue;
          }

        /*****************************************
        *
        *  transition(s)
        *
        *****************************************/

        Set<JourneyNode> visited = new HashSet<JourneyNode>();
        boolean terminateCycle = false;
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

                SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, journeyLink, evolutionEvent, now);

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
            *  terminateCycle?
            *
            *****************************************/

            if (terminateCycle)
              {
                firedLink = null;
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
                *  markNotified
                *  markConverted
                *
                *****************************************/

                boolean originalStatusNotified = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
                boolean originalStatusConverted = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;

                /*****************************************
                *
                *  set context variables when exiting node
                *
                *****************************************/

                if (firedLink.getEvaluateContextVariables())
                  {
                    for (ContextVariable contextVariable : journeyNode.getContextVariables())
                      {
                        try
                          {
                            SubscriberEvaluationRequest contextVariableEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, firedLink, evolutionEvent, now);
                            Object contextVariableValue = contextVariable.getExpression().evaluate(contextVariableEvaluationRequest, contextVariable.getBaseTimeUnit());
                            journeyState.getJourneyParameters().put(contextVariable.getID(), contextVariableValue);
                            context.getSubscriberTraceDetails().addAll(contextVariableEvaluationRequest.getTraceDetails());
                          }
                        catch (ExpressionEvaluationException|ArithmeticException e)
                          {
                            //
                            //  log
                            //

                            log.debug("invalid context variable {}", contextVariable.getExpressionString());
                            StringWriter stackTraceWriter = new StringWriter();
                            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                            log.debug(stackTraceWriter.toString());
                            context.subscriberTrace("Context Variable {0}: {1} / {2}", contextVariable.getID(), contextVariable.getExpressionString(), e.getMessage());

                            //
                            //  abort
                            //

                            journeyState.setJourneyExitDate(now);
                            boolean statusUpdated = journeyState.getJourneyHistory().addStatusInformation(SystemTime.getCurrentTime(), journeyState, true);
                            subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(
                                subscriberState.getSubscriberProfile(),
                                subscriberGroupEpochReader,
                                ucgStateReader,
                                statusUpdated,
                                new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, SystemTime.getCurrentTime())));
                            inactiveJourneyStates.add(journeyState);
                            break;
                          }
                      }

                    //
                    //  abort?
                    //

                    if (journeyState.getJourneyExitDate() != null)
                      {
                        continue;
                      }
                  }

                /*****************************************
                *
                *  exit node action
                *
                *****************************************/

                if (journeyNode.getNodeType().getActionManager() != null)
                  {
                    try
                      {
                        SubscriberEvaluationRequest exitActionEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, firedLink, evolutionEvent, now);
                        journeyNode.getNodeType().getActionManager().executeOnExit(context, exitActionEvaluationRequest, firedLink);
                        context.getSubscriberTraceDetails().addAll(exitActionEvaluationRequest.getTraceDetails());
                      }
                    catch (RuntimeException e)
                      {
                        log.error("failed action");
                        StringWriter stackTraceWriter = new StringWriter();
                        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                        log.error(stackTraceWriter.toString());
                      }
                  }

                /*****************************************
                *
                *  enter node
                *
                *****************************************/

                JourneyNode nextJourneyNode = firedLink.getDestination();
                journeyState.setJourneyNodeID(nextJourneyNode.getNodeID(), now);
                journeyState.getJourneyHistory().addNodeInformation(firedLink.getSourceReference(), firedLink.getDestinationReference(), journeyState.getJourneyOutstandingDeliveryRequestID(), firedLink.getLinkID()); 
                journeyNode = nextJourneyNode;
                subscriberStateUpdated = true;

                /*****************************************
                *
                *  terminate at this node if necessary
                *   -- second visit
                *   -- an "enableCycle" node
                *
                *****************************************/

                if (visited.contains(journeyNode) && journeyNode.getNodeType().getEnableCycle())
                  {
                    terminateCycle = true;
                  }

                /*****************************************
                *
                *  mark visited
                *
                *****************************************/

                visited.add(journeyNode);

                /*****************************************
                *
                *  set context variables when entering node
                *
                *****************************************/

                if (journeyNode.getEvaluateContextVariables())
                  {
                    for (ContextVariable contextVariable : journeyNode.getContextVariables())
                      {
                        try
                          {
                            SubscriberEvaluationRequest contextVariableEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, null, null, now);
                            Object contextVariableValue = contextVariable.getExpression().evaluate(contextVariableEvaluationRequest, contextVariable.getBaseTimeUnit());
                            journeyState.getJourneyParameters().put(contextVariable.getID(), contextVariableValue);
                            context.getSubscriberTraceDetails().addAll(contextVariableEvaluationRequest.getTraceDetails());
                          }
                        catch (ExpressionEvaluationException|ArithmeticException e)
                          {
                            //
                            //  log
                            //

                            log.debug("invalid context variable {}", contextVariable.getExpressionString());
                            StringWriter stackTraceWriter = new StringWriter();
                            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                            log.debug(stackTraceWriter.toString());
                            context.subscriberTrace("Context Variable {0}: {1} / {2}", contextVariable.getID(), contextVariable.getExpressionString(), e.getMessage());

                            //
                            //  abort
                            //

                            journeyState.setJourneyExitDate(now);
                            boolean statusUpdated = journeyState.getJourneyHistory().addStatusInformation(SystemTime.getCurrentTime(), journeyState, true);
                            subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(
                                subscriberState.getSubscriberProfile(),
                                subscriberGroupEpochReader,
                                ucgStateReader,
                                statusUpdated,
                                new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, SystemTime.getCurrentTime())));
                            inactiveJourneyStates.add(journeyState);
                            break;
                          }
                      }

                    //
                    //  abort?
                    //

                    if (journeyState.getJourneyExitDate() != null)
                      {
                        continue;
                      }
                  }

                /*****************************************
                *
                *  enter node action
                *
                *****************************************/

                if (journeyNode.getNodeType().getActionManager() != null)
                  {
                    try
                      {
                        //
                        //  evaluate action
                        //

                        SubscriberEvaluationRequest entryActionEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, null, null, now);
                        Action action = journeyNode.getNodeType().getActionManager().executeOnEntry(context, entryActionEvaluationRequest);
                        context.getSubscriberTraceDetails().addAll(entryActionEvaluationRequest.getTraceDetails());

                        //
                        //  execute action
                        //

                        if (action != null)
                          {
                            switch (action.getActionType())
                              {
                                case DeliveryRequest:
                                  DeliveryRequest deliveryRequest = (DeliveryRequest) action;
                                  subscriberState.getDeliveryRequests().add(deliveryRequest);
                                  journeyState.setJourneyOutstandingDeliveryRequestID(deliveryRequest.getDeliveryRequestID());
                                  break;

                                case JourneyRequest:
                                  JourneyRequest journeyRequest = (JourneyRequest) action;
                                  subscriberState.getJourneyRequests().add(journeyRequest);
                                  journeyState.setJourneyOutstandingJourneyRequestID(journeyRequest.getJourneyRequestID());
                                  break;
                              }
                          }
                      }
                    catch (RuntimeException e)
                      {
                        log.error("failed action");
                        StringWriter stackTraceWriter = new StringWriter();
                        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                        log.error(stackTraceWriter.toString());
                      }
                  }

                /*****************************************
                *
                *  exit (if exit node)
                *
                *****************************************/

                if (journeyNode.getExitNode())
                  {
                    /*****************************************
                    *
                    *  exitJourney
                    *
                    *****************************************/

                    journeyState.setJourneyExitDate(now);
                    inactiveJourneyStates.add(journeyState);

                    /*****************************************
                    *
                    *  populate journeyMetrics (during)
                    *
                    *****************************************/

                    for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values())
                      {
                        MetricHistory metricHistory = journeyMetricDeclaration.getMetricHistory(subscriberState.getSubscriberProfile());
                        long startMetricValue = journeyState.getJourneyMetricsDuring().get(journeyMetricDeclaration.getID());
                        long endMetricValue = metricHistory.getAllTimeBucket();
                        long duringMetricValue = endMetricValue - startMetricValue;
                        journeyState.getJourneyMetricsDuring().put(journeyMetricDeclaration.getID(), duringMetricValue);
                        subscriberStateUpdated = true;
                      }
                  }

                /*****************************************
                *
                *  journeyStatistic for node transition
                *
                *****************************************/

                //
                //  markNotified
                //

                boolean currentStatusNotified = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
                boolean markNotified = originalStatusNotified == false && currentStatusNotified == true;

                //
                //  markConverted
                //

                boolean currentStatusConverted = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
                boolean markConverted = originalStatusConverted == false && currentStatusConverted == true;
                
                //
                // abTesting (we remove it so its only counted once per journey)
                //
                
                String sample = null;
                if(journeyState.getJourneyParameters().get("sample.a") != null)
                  {
                    sample = (String) journeyState.getJourneyParameters().get("sample.a");
                    journeyState.getJourneyParameters().remove("sample.a");
                  }

                else if(journeyState.getJourneyParameters().get("sample.b") != null)
                  {
                    sample = (String) journeyState.getJourneyParameters().get("sample.b");
                    journeyState.getJourneyParameters().remove("sample.b");
                  }
                
                //
                //  journeyStatistic
                //
                
                boolean statusUpdated = journeyState.getJourneyHistory().addStatusInformation(SystemTime.getCurrentTime(), journeyState, firedLink.getDestination().getExitNode());
                subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(
                    subscriberState.getSubscriberProfile(),
                    subscriberGroupEpochReader,
                    ucgStateReader,
                    statusUpdated,
                    new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, firedLink, markNotified, markConverted, sample)));
              }
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
    *  close metrics
    *
    *****************************************/

    for (JourneyState journeyState : subscriberState.getRecentJourneyStates())
      {
        //
        //  close metrics
        //

        if (journeyState.getJourneyCloseDate() == null)
          {
            //
            //  post metrics
            //

            for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values())
              {
                if (! journeyState.getJourneyMetricsPost().containsKey(journeyMetricDeclaration.getID()))
                  {
                    Date journeyExitDay = RLMDateUtils.truncate(journeyState.getJourneyExitDate(), Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                    Date metricStartDay = RLMDateUtils.addDays(journeyExitDay, 1, Deployment.getBaseTimeZone());
                    Date metricEndDay = RLMDateUtils.addDays(journeyExitDay, journeyMetricDeclaration.getPostPeriodDays(), Deployment.getBaseTimeZone());
                    if (now.after(RLMDateUtils.addDays(metricEndDay, 1, Deployment.getBaseTimeZone())))
                      {
                        MetricHistory metricHistory = journeyMetricDeclaration.getMetricHistory(subscriberState.getSubscriberProfile());
                        long postMetricValue = metricHistory.getValue(metricStartDay, metricEndDay);
                        journeyState.getJourneyMetricsPost().put(journeyMetricDeclaration.getID(), postMetricValue);
                        subscriberStateUpdated = true;
                      }
                  }
              }

            //
            //  close?
            //

            boolean closeJourney = true;
            for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values())
              {
                closeJourney = closeJourney && journeyState.getJourneyMetricsPost().containsKey(journeyMetricDeclaration.getID());
              }
            
            //
            //  close
            //

            if (closeJourney)
              {
                subscriberState.getJourneyMetrics().add(new JourneyMetric(context, subscriberState.getSubscriberID(), journeyState));
                journeyState.setJourneyCloseDate(now);
                subscriberStateUpdated = true;
              }
          }
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return subscriberStateUpdated;
  }

  /****************************************
  *
  *  rekeyPointFulfilmentRequestStream
  *
  ****************************************/

  private static KeyValue<StringKey, PointFulfillmentRequest> rekeyPointFulfilmentRequestStream(StringKey key, PointFulfillmentRequest pointFulfillmentRequest)
  {
    return new KeyValue<StringKey, PointFulfillmentRequest>(new StringKey(pointFulfillmentRequest.getSubscriberID()), pointFulfillmentRequest);
  }

  /*****************************************
  *
  *  nullSubscriberState
  *
  ****************************************/

  public static ExtendedSubscriberProfile nullExtendedSubscriberProfile() { return (ExtendedSubscriberProfile) null; }

  /*****************************************
  *
  *  updateExtendedSubscriberProfile
  *
  *****************************************/

  public static ExtendedSubscriberProfile updateExtendedSubscriberProfile(StringKey aggKey, SubscriberStreamEvent evolutionEvent, ExtendedSubscriberProfile currentExtendedSubscriberProfile)
  {
    /****************************************
    *
    *  get (or create) entry
    *
    ****************************************/

    ExtendedSubscriberProfile extendedSubscriberProfile = (currentExtendedSubscriberProfile != null) ? ExtendedSubscriberProfile.copy(currentExtendedSubscriberProfile) : ExtendedSubscriberProfile.create(evolutionEvent.getSubscriberID());
    ExtendedProfileContext context = new ExtendedProfileContext(extendedSubscriberProfile, subscriberGroupEpochReader, uniqueKeyServer, SystemTime.getCurrentTime());
    boolean extendedSubscriberProfileUpdated = (currentExtendedSubscriberProfile != null) ? false : true;

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

    extendedSubscriberProfileUpdated = cleanExtendedSubscriberProfile(extendedSubscriberProfile, now) || extendedSubscriberProfileUpdated;

    /*****************************************
    *
    *  process subscriberTraceControl
    *
    *****************************************/

    if (evolutionEvent instanceof SubscriberTraceControl)
      {
        SubscriberTraceControl subscriberTraceControl = (SubscriberTraceControl) evolutionEvent;
        extendedSubscriberProfile.setSubscriberTraceEnabled(subscriberTraceControl.getSubscriberTraceEnabled());
        extendedSubscriberProfileUpdated = true;
      }

    /*****************************************
    *
    *  invoke evolution engine extension
    *
    *****************************************/

    try
      {
        extendedSubscriberProfileUpdated = ((Boolean) evolutionEngineExtensionUpdateExtendedSubscriberMethod.invoke(null, context, evolutionEvent)).booleanValue() || extendedSubscriberProfileUpdated;
      }
    catch (IllegalAccessException|InvocationTargetException e)
      {
        throw new RuntimeException(e);
      }

    /*****************************************
    *
    *  subscriberTrace
    *
    *****************************************/

    if (extendedSubscriberProfile.getSubscriberTraceEnabled())
      {
        extendedSubscriberProfile.setSubscriberTrace(new SubscriberTrace(generateSubscriberTraceMessage(evolutionEvent, currentExtendedSubscriberProfile, extendedSubscriberProfile, context.getSubscriberTraceDetails())));
        extendedSubscriberProfileUpdated = true;
      }

    /*****************************************
    *
    *  stateStoreSerde
    *
    *****************************************/

    if (extendedSubscriberProfileUpdated)
      {
        extendedSubscriberProfile.stateStoreSerde().setKafkaRepresentation(Deployment.getExtendedSubscriberProfileChangeLogTopic(), extendedSubscriberProfile);
        evolutionEngineStatistics.updateExtendedProfileSize(extendedSubscriberProfile.getKafkaRepresentation());
        if (extendedSubscriberProfile.getKafkaRepresentation().length > 1000000)
          {
            log.error("ExtendedSubscriberProfile size error, ignoring event {} for subscriber {}: {}", evolutionEvent.getClass().toString(), evolutionEvent.getSubscriberID(), extendedSubscriberProfile.getKafkaRepresentation().length);
            cleanExtendedSubscriberProfile(currentExtendedSubscriberProfile, now);
            currentExtendedSubscriberProfile.stateStoreSerde().setKafkaRepresentation(Deployment.getExtendedSubscriberProfileChangeLogTopic(), currentExtendedSubscriberProfile);
            extendedSubscriberProfileUpdated = false;
          }
      }

    /****************************************
    *
    *  return
    *
    ****************************************/

    return extendedSubscriberProfileUpdated ? extendedSubscriberProfile : currentExtendedSubscriberProfile;
  }

  /*****************************************
  *
  *  cleanExtendedSubscriberProfile
  *
  *****************************************/

  private static boolean cleanExtendedSubscriberProfile(ExtendedSubscriberProfile extendedSubscriberProfile, Date now)
  {
    //
    //  extendedSubscrberProfileUpdated
    //

    boolean extendedSubscriberProfileUpdated = false;

    //
    //  subscriberTrace
    //

    if (extendedSubscriberProfile.getSubscriberTrace() != null)
      {
        extendedSubscriberProfile.setSubscriberTrace(null);
        extendedSubscriberProfileUpdated = true;
      }

    //
    //  return
    //

    return extendedSubscriberProfileUpdated;
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
      evolutionEngineStatistics.incrementAcceptanceCount();
    }

    propensityState.setPresentationCount(propensityState.getPresentationCount() + 1L);
    evolutionEngineStatistics.incrementPresentationCount();

    /****************************************
    *
    *  return the updated PropensityState (it is always different than the current one)
    *
    ****************************************/

    return propensityState;
  }

  /****************************************
  *
  *  rekeyPropensityStream
  *
  ****************************************/

  private static KeyValue<PropensityKey, PropensityEventOutput> rekeyPropensityStream(StringKey key, PropensityEventOutput propensityEventOutput)
  {
    return new KeyValue<PropensityKey, PropensityEventOutput>(propensityEventOutput.getPropensityKey(), propensityEventOutput);
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
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /*****************************************
    *
    *  clear state
    *
    *****************************************/

    subscriberHistoryUpdated = cleanSubscriberHistory(subscriberHistory, now) || subscriberHistoryUpdated;

    /*****************************************
    *
    *  deliveryRequest
    *
    *****************************************/

    if (evolutionEvent instanceof DeliveryRequest)
      {
        subscriberHistoryUpdated = updateSubscriberHistoryDeliveryRequests((DeliveryRequest) evolutionEvent, subscriberHistory) || subscriberHistoryUpdated;
      }

    /*****************************************
    *
    *  journeyStatistic
    *
    *****************************************/

    if (evolutionEvent instanceof JourneyStatistic)
      {
        subscriberHistoryUpdated = updateSubscriberHistoryJourneyStatistics((JourneyStatistic) evolutionEvent, subscriberHistory) || subscriberHistoryUpdated;
      }

    /*****************************************
    *
    *  stateStoreSerde
    *
    *****************************************/

    if (subscriberHistoryUpdated)
      {
        subscriberHistory.stateStoreSerde().setKafkaRepresentation(Deployment.getSubscriberHistoryChangeLogTopic(), subscriberHistory);
        evolutionEngineStatistics.updateSubscriberHistorySize(subscriberHistory.getKafkaRepresentation());
        if (subscriberHistory.getKafkaRepresentation().length > 1000000)
          {
            log.error("HistoryStore size error, ignoring event {} for subscriber {}: {}", evolutionEvent.getClass().toString(), evolutionEvent.getSubscriberID(), subscriberHistory.getKafkaRepresentation().length);
            cleanSubscriberHistory(currentSubscriberHistory, now);
            currentSubscriberHistory.stateStoreSerde().setKafkaRepresentation(Deployment.getSubscriberHistoryChangeLogTopic(), currentSubscriberHistory);
            subscriberHistoryUpdated = false;
          }
      }

    /****************************************
    *
    *  return
    *
    ****************************************/

    return subscriberHistoryUpdated ? subscriberHistory : currentSubscriberHistory;
  }

  /*****************************************
  *
  *  cleanSubscriberHistory
  *
  *****************************************/

  private static boolean cleanSubscriberHistory(SubscriberHistory subscriberHistory, Date now)
  {
    //
    //  subscriberHistoryUpdated
    //

    boolean subscriberHistoryUpdated = false;

    //
    //  return
    //

    return subscriberHistoryUpdated;
  }

  /*****************************************
  *
  *  updateSubscriberHistoryDeliveryRequests
  *
  *****************************************/

  private static boolean updateSubscriberHistoryDeliveryRequests(DeliveryRequest deliveryRequest, SubscriberHistory subscriberHistory)
  {
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
    *  ignore journeyRequests
    *
    *****************************************/

    if (deliveryRequest instanceof JourneyRequest)
      {
        return false;
      }

    /*****************************************
    *
    *  add to history
    *
    *****************************************/

    //
    //  find sorted location to insert
    //

    int i = 0;
    while (i < subscriberHistory.getDeliveryRequests().size() && subscriberHistory.getDeliveryRequests().get(i).compareTo(deliveryRequest) <= 0)
      {
        i += 1;
      }

    //
    //  insert
    //

    subscriberHistory.getDeliveryRequests().add(i, deliveryRequest);

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return true;
  }

  /*****************************************
  *
  *  updateSubscriberHistoryJourneyStatistics
  *
  *****************************************/

  private static boolean updateSubscriberHistoryJourneyStatistics(JourneyStatistic journeyStatistic, SubscriberHistory subscriberHistory)
  {
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
    *  add to subscriberHistory
    *
    *****************************************/
    
    ListIterator<JourneyHistory> iterator = subscriberHistory.getJourneyHistory().listIterator();
    JourneyHistory updatedHistory = null;
    while (iterator.hasNext()) 
      {
        JourneyHistory history = iterator.next();
        if(history.getJourneyID().equals(journeyStatistic.getJourneyID())) 
          {
            updatedHistory = new JourneyHistory(journeyStatistic.getJourneyID(), journeyStatistic.getJourneyNodeHistory(), journeyStatistic.getJourneyStatusHistory(), journeyStatistic.getJourneyRewardHistory());
            iterator.remove();
            break;
          }
      }

    if(updatedHistory != null)
      {
        subscriberHistory.getJourneyHistory().add(new JourneyHistory(updatedHistory));
      }    
    else 
      {
        subscriberHistory.getJourneyHistory().add(new JourneyHistory(journeyStatistic.getJourneyID(), journeyStatistic.getJourneyNodeHistory(), journeyStatistic.getJourneyStatusHistory(), journeyStatistic.getJourneyRewardHistory()));
      }
    
    /*****************************************
     *
     *  return
     *
     *****************************************/

    return true;
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
  *  enhancePeriodicEvaluation
  *
  *****************************************/

  private static TimedEvaluation enhancePeriodicEvaluation(SubscriberStreamEvent evolutionEvent, ExtendedSubscriberProfile extendedSubscriberProfile)
  {
    TimedEvaluation periodicEvaluation = new TimedEvaluation((TimedEvaluation) evolutionEvent);
    periodicEvaluation.setExtendedSubscriberProfile(extendedSubscriberProfile);
    return periodicEvaluation;
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
  *  getExtendedProfile
  *
  *****************************************/

  private static ExtendedSubscriberProfile getExtendedProfile(SubscriberStreamEvent evolutionEvent, ExtendedSubscriberProfile extendedSubscriberProfile)
  {
    return extendedSubscriberProfile;
  }

  /****************************************
  *
  *  getEvolutionEngineOutputs
  *
  ****************************************/

  private static List<SubscriberStreamOutput> getEvolutionEngineOutputs(SubscriberState subscriberState)
  {
    List<SubscriberStreamOutput> result = new ArrayList<SubscriberStreamOutput>();
    result.addAll(subscriberState.getJourneyResponses());
    result.addAll(subscriberState.getJourneyRequests());
    result.addAll(subscriberState.getLoyaltyProgramResponses());
    result.addAll(subscriberState.getLoyaltyProgramRequests());
    result.addAll(subscriberState.getPointFulfillmentResponses());
    result.addAll(subscriberState.getDeliveryRequests());
    result.addAll(subscriberState.getJourneyStatisticWrappers());
    for (JourneyStatisticWrapper wrapper: subscriberState.getJourneyStatisticWrappers()) 
      {
        if (wrapper.getJourneyStatistic() != null)
          {
            result.add(wrapper.getJourneyStatistic());
          }
      }
    result.addAll(subscriberState.getJourneyMetrics());
    result.addAll((subscriberState.getSubscriberTrace() != null) ? Collections.<SubscriberTrace>singletonList(subscriberState.getSubscriberTrace()) : Collections.<SubscriberTrace>emptyList());
    result.addAll(subscriberState.getPropensityOutputs());
    result.addAll((subscriberState.getExternalAPIOutput() != null) ? Collections.<ExternalAPIOutput>singletonList(subscriberState.getExternalAPIOutput()) : Collections.<ExternalAPIOutput>emptyList());
    result.addAll(subscriberState.getProfileChangeEvents());
    result.addAll(subscriberState.getProfileSegmentChangeEvents());
    result.addAll(subscriberState.getProfileLoyaltyProgramChangeEvents());
    return result;
  }

  /****************************************
  *
  *  getExtendedProfileOutputs
  *
  ****************************************/

  private static List<SubscriberStreamOutput> getExtendedProfileOutputs(ExtendedSubscriberProfile extendedSubscriberProfile)
  {
    List<SubscriberStreamOutput> result = new ArrayList<SubscriberStreamOutput>();
    result.addAll((extendedSubscriberProfile.getSubscriberTrace() != null) ? Collections.<SubscriberTrace>singletonList(extendedSubscriberProfile.getSubscriberTrace()) : Collections.<SubscriberTrace>emptyList());
    return result;
  }
  
  /****************************************
  *
  *  getJourneyStatistic
  *
  ****************************************/

  private static JourneyStatistic getJourneyStatisticFromWrapper(JourneyStatisticWrapper value)
  {
    return value.getJourneyStatistic();
  }
  
  /****************************************
  *
  *  getJourneyNodeStatisticsFromWrapper
  *
  ****************************************/

  private static KeyValue<StringKey, JourneyStatisticWrapper> rekeyByJourneyID(StringKey key, JourneyStatisticWrapper value)
  {
    return new KeyValue<StringKey, JourneyStatisticWrapper>(new StringKey(value.getJourneyID()), value);
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
  
  /****************************************
  *
  *  rekeyExternalAPIOutputStream
  *
  ****************************************/

  private static KeyValue<StringKey, ExternalAPIOutput> rekeyExternalAPIOutputStream(StringKey key, ExternalAPIOutput value)
  {
    return new KeyValue<StringKey, ExternalAPIOutput>(new StringKey(value.getTopicID()), value);
  }

  /****************************************
  *
  *  rekeyJourneyResponseStream
  *
  ****************************************/

  private static KeyValue<StringKey, JourneyRequest> rekeyJourneyResponseStream(StringKey key, JourneyRequest value) 
  {
    StringKey rekey = value.getOriginatingRequest() ? new StringKey(value.getSubscriberID()) : new StringKey(value.getDeliveryRequestID());
    return new KeyValue<StringKey, JourneyRequest>(rekey, value);
  }

  /****************************************
  *
  *  rekeyLoyaltyProgramResponseStream
  *
  ****************************************/

  private static KeyValue<StringKey, LoyaltyProgramRequest> rekeyLoyaltyProgramResponseStream(StringKey key, LoyaltyProgramRequest value) 
  {
    StringKey rekey = value.getOriginatingRequest() ? new StringKey(value.getSubscriberID()) : new StringKey(value.getDeliveryRequestID());
    return new KeyValue<StringKey, LoyaltyProgramRequest>(rekey, value);
  }

  /****************************************
  *
  *  rekeyPointResponseStream
  *
  ****************************************/

  private static KeyValue<StringKey, PointFulfillmentRequest> rekeyPointResponseStream(StringKey key, PointFulfillmentRequest value)
  {
    StringKey rekey = value.getOriginatingRequest() ? new StringKey(value.getSubscriberID()) : new StringKey(value.getDeliveryRequestID());
    return new KeyValue<StringKey, PointFulfillmentRequest>(rekey, value);
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
    if (evolutionEvent instanceof TimedEvaluation && ((TimedEvaluation) evolutionEvent).getExtendedSubscriberProfile() != null)
      {
        JsonNode extendedSubscriberProfileNode = ((ObjectNode) evolutionEventNode).get("extendedSubscriberProfile");
        ((ObjectNode) extendedSubscriberProfileNode).remove("subscriberTraceMessage");
      }

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
    *  reformat metric histories
    *
    *****************************************/

    if (currentSubscriberStateNode != null) reformatMetricHistories((ObjectNode) currentSubscriberStateNode.get("subscriberProfile"));
    reformatMetricHistories((ObjectNode) subscriberStateNode.get("subscriberProfile"));
    
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
  *  generateSubscriberTraceMessage
  *
  *****************************************/

  private static String generateSubscriberTraceMessage(SubscriberStreamEvent evolutionEvent, ExtendedSubscriberProfile currentExtendedSubscriberProfile, ExtendedSubscriberProfile extendedSubscriberProfile, List<String> subscriberTraceDetails)
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
    //  field -- currentExtendedSubscriberProfile
    //

    JsonNode currentExtendedSubscriberProfileNode = (currentExtendedSubscriberProfile != null) ? deserializer.deserialize(null, converter.fromConnectData(null, ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().schema(),  ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().pack(currentExtendedSubscriberProfile))) : null;

    //
    //  field -- extendedSubscriberProfile
    //

    JsonNode extendedSubscriberProfileNode = deserializer.deserialize(null, converter.fromConnectData(null, ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().schema(),  ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().pack(extendedSubscriberProfile)));

    /*****************************************
    *
    *  hack/massage triggerStateNodes to remove unwanted/misleading/spurious fields from currentTriggerState
    *
    *****************************************/

    //
    //  outgoing messages (currentSubscriberStateNode)
    //

    if (currentExtendedSubscriberProfileNode != null) ((ObjectNode) currentExtendedSubscriberProfileNode).remove("subscriberTraceMessage");

    //
    //  other (subscriberStateNode)
    //

    ((ObjectNode) extendedSubscriberProfileNode).remove("subscriberTraceMessage");

    /*****************************************
    *
    *  reformat metric histories
    *
    *****************************************/

    if (currentExtendedSubscriberProfileNode != null) reformatMetricHistories((ObjectNode) currentExtendedSubscriberProfileNode);
    reformatMetricHistories((ObjectNode) extendedSubscriberProfileNode);
    
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
    subscriberTraceMessageNode.set("currentExtendedSubscriberProfile", currentExtendedSubscriberProfileNode);
    subscriberTraceMessageNode.set("extendedSubscriberProfile", extendedSubscriberProfileNode);
    subscriberTraceMessageNode.set("details", subscriberTraceDetailsNode);

    //
    //  convert to string
    //

    return subscriberTraceMessageNode.toString();
  }

  /*****************************************
  *
  *  reformatMetricHistories
  *
  *****************************************/

  private static void reformatMetricHistories(ObjectNode objectNode)
  {
    //
    //  break if null object
    //
    
    if (objectNode == null) return;

    //
    //  iterate through all fields - reformatting/replacing byte arrays with arrays of longs for any member that looks like a MetricsHistory
    //
    
    Iterator<String> fieldNames = objectNode.fieldNames();
    while (fieldNames.hasNext())
      {
        String name = fieldNames.next();
        JsonNode value = objectNode.get(name);
        if (value.getNodeType() == JsonNodeType.OBJECT && value.hasNonNull("dailyRepresentation") && value.hasNonNull("dailyBuckets") && value.hasNonNull("monthlyRepresentation")  && value.hasNonNull("monthlyBuckets"))
          {
            try
              {
                //
                //  replace dailyBuckets
                //

                JsonNode dailyRepresentationJson = value.get("dailyRepresentation");
                BucketRepresentation dailyBucketRepresentation = dailyRepresentationJson.isInt() ? BucketRepresentation.fromExternalRepresentation(dailyRepresentationJson.intValue()) : null;
                long[] dailyBuckets = (dailyBucketRepresentation != null && dailyBucketRepresentation != BucketRepresentation.UninitializedRepresentation) ? MetricHistory.unpackBuckets(dailyBucketRepresentation, value.get("dailyBuckets").binaryValue()) : null;
                ArrayNode dailyBucketsArrayNode = new ObjectMapper().createArrayNode();
                if (dailyBuckets != null)
                  {
                    for (int i = 0; i < dailyBuckets.length; i++)
                      {
                        dailyBucketsArrayNode.add(new Long(dailyBuckets[i]));
                      }
                  }
                ((ObjectNode) value).put("dailyBuckets", dailyBucketsArrayNode);

                //
                //  replace monthlyBuckets
                //

                JsonNode monthlyRepresentationJson = value.get("monthlyRepresentation");
                BucketRepresentation monthlyBucketRepresentation = monthlyRepresentationJson.isInt() ? BucketRepresentation.fromExternalRepresentation(monthlyRepresentationJson.intValue()) : null;
                long[] monthlyBuckets = (monthlyBucketRepresentation != null && monthlyBucketRepresentation != BucketRepresentation.UninitializedRepresentation) ? MetricHistory.unpackBuckets(monthlyBucketRepresentation, value.get("monthlyBuckets").binaryValue()) : null;
                ArrayNode monthlyBucketsArrayNode = new ObjectMapper().createArrayNode();
                if (monthlyBuckets != null)
                  {
                    for (int i = 0; i < monthlyBuckets.length; i++)
                      {
                        monthlyBucketsArrayNode.add(new Long(monthlyBuckets[i]));
                      }
                  }
                ((ObjectNode) value).put("monthlyBuckets", monthlyBucketsArrayNode);
              }
            catch (IOException e)
              {
                // ignore and continue
              }
          }
      }
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
    private ExtendedSubscriberProfile extendedSubscriberProfile;
    private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
    private SubscriberMessageTemplateService subscriberMessageTemplateService;
    private DeliverableService deliverableService;
    private SegmentationDimensionService segmentationDimensionService;
    private SegmentContactPolicyService segmentContactPolicyService;
    private KStreamsUniqueKeyServer uniqueKeyServer;
    private Date now;
    private List<String> subscriberTraceDetails;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public EvolutionEventContext(SubscriberState subscriberState, ExtendedSubscriberProfile extendedSubscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, SubscriberMessageTemplateService subscriberMessageTemplateService, DeliverableService deliverableService, SegmentationDimensionService segmentationDimensionService, SegmentContactPolicyService segmentContactPolicyService, KStreamsUniqueKeyServer uniqueKeyServer, Date now)
    {
      this.subscriberState = subscriberState;
      this.extendedSubscriberProfile = extendedSubscriberProfile;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
      this.subscriberMessageTemplateService = subscriberMessageTemplateService;
      this.deliverableService = deliverableService;
      this.segmentationDimensionService = segmentationDimensionService;
      this.segmentContactPolicyService = segmentContactPolicyService;
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
    public ExtendedSubscriberProfile getExtendedSubscriberProfile() { return extendedSubscriberProfile; }
    public ReferenceDataReader<String,SubscriberGroupEpoch> getSubscriberGroupEpochReader() { return subscriberGroupEpochReader; }
    public SubscriberMessageTemplateService getSubscriberMessageTemplateService() { return subscriberMessageTemplateService; }
    public DeliverableService getDeliverableService() { return deliverableService; }
    public SegmentationDimensionService getSegmentationDimensionService() { return segmentationDimensionService; }
    public SegmentContactPolicyService getSegmentContactPolicyService() { return segmentContactPolicyService; }
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
  *  class ExtendedProfileContext
  *
  *****************************************/

  public static class ExtendedProfileContext
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private ExtendedSubscriberProfile extendedSubscriberProfile;
    private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
    private KStreamsUniqueKeyServer uniqueKeyServer;
    private Date now;
    private List<String> subscriberTraceDetails;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ExtendedProfileContext(ExtendedSubscriberProfile extendedSubscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, KStreamsUniqueKeyServer uniqueKeyServer, Date now)
    {
      this.extendedSubscriberProfile = extendedSubscriberProfile;
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

    public ExtendedSubscriberProfile getExtendedSubscriberProfile() { return extendedSubscriberProfile; }
    public ReferenceDataReader<String,SubscriberGroupEpoch> getSubscriberGroupEpochReader() { return subscriberGroupEpochReader; }
    public KStreamsUniqueKeyServer getUniqueKeyServer() { return uniqueKeyServer; }
    public List<String> getSubscriberTraceDetails() { return subscriberTraceDetails; }
    public Date now() { return now; }
    public boolean getSubscriberTraceEnabled() { return extendedSubscriberProfile.getSubscriberTraceEnabled(); }

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
      return SystemTime.getCurrentTime().getTime();
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
        boolean includeExtendedSubscriberProfile = JSONUtilities.decodeBoolean(jsonRoot, "includeExtendedSubscriberProfile", Boolean.FALSE);
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
              apiResponse = processGetSubscriberProfile(subscriberID, includeExtendedSubscriberProfile, includeHistory);
              break;

            case retrieveSubscriberProfile:
              apiResponse = processRetrieveSubscriberProfile(subscriberID, includeExtendedSubscriberProfile, includeHistory);
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

  private byte[] processGetSubscriberProfile(String subscriberID, boolean includeExtendedSubscriberProfile, boolean includeHistory) throws ServerException
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
    request.put("includeExtendedSubscriberProfile", includeExtendedSubscriberProfile);
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

  private byte[] processRetrieveSubscriberProfile(String subscriberID, boolean includeExtendedSubscriberProfile, boolean includeHistory) throws ServerException
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
    *  retrieve extendedSubscriberProfile from local store (if necessary)
    *
    *****************************************/

    if (subscriberProfile != null && includeExtendedSubscriberProfile)
      {
        ExtendedSubscriberProfile extendedSubscriberProfile = null;
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
                extendedSubscriberProfile = extendedSubscriberProfileStore.get(new StringKey(subscriberID));
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
        //  add extendedSubscriberProfile
        //

        if (extendedSubscriberProfile != null)
          {
            subscriberProfile = subscriberProfile.copy();
            subscriberProfile.setExtendedSubscriberProfile(extendedSubscriberProfile);
          }
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

  /*****************************************
  *
  *  runPeriodicLogger
  *
  *****************************************/

  private void runPeriodicLogger()
  {
    NGLMRuntime.registerSystemTimeDependency(evolutionEngineStatistics);
    Date nextProcessingTime = RLMDateUtils.addSeconds(SystemTime.getCurrentTime(), 300);
    while (true)
      {
        synchronized (evolutionEngineStatistics)
          {
            //
            //  wait
            //

            Date now = SystemTime.getCurrentTime();
            while (now.before(nextProcessingTime))
              {
                try
                  {
                    evolutionEngineStatistics.wait(nextProcessingTime.getTime() - now.getTime());
                  }
                catch (InterruptedException e)
                  {
                    // ignore
                  }
                now = SystemTime.getCurrentTime();
              }

            //
            //  log
            //

            log.info("SubscriberStateSize: {}", evolutionEngineStatistics.getSubscriberStateSize().toString());
            log.info("SubscriberHistorySize: {}", evolutionEngineStatistics.getSubscriberHistorySize().toString());
            log.info("ExtendedProfileSize: {}", evolutionEngineStatistics.getExtendedProfileSize().toString());

            //
            //  nextProcessingTime
            //

            nextProcessingTime = RLMDateUtils.addSeconds(nextProcessingTime, 300);
          }
      }
  }

  /*****************************************
  *
  *  class EnterJourneyAction
  *
  *****************************************/

  public static class EnterJourneyAction extends ActionManager
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public EnterJourneyAction(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
    }

    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public JourneyRequest executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      String journeyID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.journey");

      /*****************************************
      *
      *  request
      *
      *****************************************/

      JourneyRequest request = new JourneyRequest(evolutionEventContext, deliveryRequestSource, journeyID);

      /*****************************************
      *
      *  return request
      *
      *****************************************/

      return request;
    }
  }
  
  /*****************************************
  *
  *  class OptAction
  *
  *****************************************/

  public static class LoyaltyProgramAction extends ActionManager
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String moduleID;
    private LoyaltyProgramOperation operation;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public LoyaltyProgramAction(JSONObject configuration)
    {
      super(configuration);
      this.moduleID = JSONUtilities.decodeString(configuration, "moduleID", true);
      this.operation = LoyaltyProgramOperation.fromExternalRepresentation(JSONUtilities.decodeString(configuration, "operation", true));
    }

    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public LoyaltyProgramRequest executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      
      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      String loyaltyProgramID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.loyaltyProgramId");

      /*****************************************
      *
      *  request
      *
      *****************************************/

      LoyaltyProgramRequest request = new LoyaltyProgramRequest(evolutionEventContext, deliveryRequestSource, operation, loyaltyProgramID);
      request.setModuleID(moduleID);
      request.setFeatureID(deliveryRequestSource);

      /*****************************************
      *
      *  return request
      *
      *****************************************/

      return request;
      
    }
  }
}
