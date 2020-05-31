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
import java.lang.reflect.Constructor;
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
import java.util.stream.Collectors;

import com.evolving.nglm.evolution.propensity.PropensityService;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import org.apache.kafka.streams.state.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.CleanupSubscriber;
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
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.SubscriberTrace;
import com.evolving.nglm.core.SubscriberTraceControl;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.DeliveryRequest.DeliveryPriority;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
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
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
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

  private static KafkaProducer<byte[], byte[]> kafkaProducer;
  private static ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private static ReferenceDataReader<String,UCGState> ucgStateReader;
  private static DynamicCriterionFieldService dynamicCriterionFieldService; 
  private static JourneyService journeyService;
  private static LoyaltyProgramService loyaltyProgramService;
  private static TargetService targetService;
  private static JourneyObjectiveService journeyObjectiveService;
  private static SegmentationDimensionService segmentationDimensionService;
  private static PresentationStrategyService presentationStrategyService;
  private static ScoringStrategyService scoringStrategyService;
  private static OfferService offerService;
  private static SalesChannelService salesChannelService;
  private static ProductService productService;
  private static ProductTypeService productTypeService;
  private static VoucherService voucherService;
  private static VoucherTypeService voucherTypeService;
  private static CatalogCharacteristicService catalogCharacteristicService;
  private static DNBOMatrixService dnboMatrixService;
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
  private static PaymentMeanService paymentMeanService;
  private static KafkaStreams streams = null;
  private static ReadOnlyKeyValueStore<StringKey, SubscriberState> subscriberStateStore = null;
  private static ReadOnlyKeyValueStore<StringKey, ExtendedSubscriberProfile> extendedSubscriberProfileStore = null;
  private static ReadOnlyKeyValueStore<StringKey, SubscriberHistory> subscriberHistoryStore = null;
  private static final int RESTAPIVersion = 1;
  private static HttpServer subscriberProfileServer;
  private static HttpServer internalServer;
  private static HttpClient httpClient;
  private static ExclusionInclusionTargetService exclusionInclusionTargetService;
  private static StockMonitor stockService;
  private static PropensityService propensityService;

  /****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    NGLMRuntime.initialize(true);
    EvolutionEngine evolutionEngine = new EvolutionEngine();
    new LoggerInitialization().initLogger();
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
    // for performance testing only, SHOULD NOT BE USED IN PROD, the right rocksDB configuration should be able to provide the same
    boolean isInMemoryStateStores = false;
    if(args.length>8 && args[8].toLowerCase().equals("1")) isInMemoryStateStores = true;
    // try as well some rocksdb config (not sure yet at all about all this, documentation is not so clear, so testing)
    int rocksDBCacheMBytes=-1;// will not change the default kstream rocksdb settings
    if(args.length>9 && !isInMemoryStateStores) rocksDBCacheMBytes = Integer.parseInt(args[9]);
    int rocksDBMemTableMBytes=-1;
    if(args.length>10 && !isInMemoryStateStores && rocksDBCacheMBytes!=-1) rocksDBMemTableMBytes = Integer.parseInt(args[10]);

    //
    //  source topics
    //

    String emptyTopic = Deployment.getEmptyTopic();
    String timedEvaluationTopic = Deployment.getTimedEvaluationTopic();
    String cleanupSubscriberTopic = Deployment.getCleanupSubscriberTopic();
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
    String productServiceTopic = Deployment.getProductTopic();
    String productTypeServiceTopic = Deployment.getProductTypeTopic();
    String catalogCharacteristicServiceTopic = Deployment.getCatalogCharacteristicTopic();
    String dnboMatrixServiceTopic = Deployment.getDNBOMatrixTopic();
    String pointFulfillmentRekeyedTopic = Deployment.getPointFulfillmentRekeyedTopic();
    String voucherChangeRequestTopic = Deployment.getVoucherChangeRequestTopic();

    //
    //  sink topics
    //

    String subscriberTraceTopic = Deployment.getSubscriberTraceTopic();
    String pointFulfillmentResponseTopic = Deployment.getPointFulfillmentResponseTopic();
    String journeyResponseTopic = Deployment.getJourneyResponseTopic();
    String loyaltyProgramResponseTopic = Deployment.getLoyaltyProgramResponseTopic();
    String journeyTrafficTopic = Deployment.getJourneyTrafficTopic();
    String tokenChangeTopic = Deployment.getTokenChangeTopic();
    String voucherChangeResponseTopic = Deployment.getVoucherChangeResponseTopic();

    //
    //  changelogs
    //

    String subscriberStateChangeLog = Deployment.getSubscriberStateChangeLog();
    String extendedSubscriberProfileChangeLog = Deployment.getExtendedSubscriberProfileChangeLog();
    String subscriberHistoryChangeLog = Deployment.getSubscriberHistoryChangeLog();

    //
    //  (force load of SubscriberProfile class)
    //

    SubscriberProfile.getSubscriberProfileSerde();

    //
    //  log
    //

    log.info("main START: {} {} {} {} {}", stateDirectory, bootstrapServers, kafkaStreamsStandbyReplicas, kafkaReplicationFactor);

    
    //
    // kafka producer
    //    

   Properties producerProperties = new Properties();
   producerProperties.put("bootstrap.servers", bootstrapServers);
   producerProperties.put("acks", "all");
   producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
   producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
   kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);
    
    
    //
    //  dynamicCriterionFieldsService
    //

    dynamicCriterionFieldService = new DynamicCriterionFieldService(bootstrapServers, "evolutionengine-dynamiccriterionfieldservice-" + evolutionEngineKey, Deployment.getDynamicCriterionFieldTopic(), false);
    dynamicCriterionFieldService.start();
    CriterionContext.initialize(dynamicCriterionFieldService);  

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

    timerService = new TimerService(this, bootstrapServers, evolutionEngineKey);

    //
    //  segmentationDimensionService
    //

    segmentationDimensionService = new SegmentationDimensionService(bootstrapServers, "evolutionengine-segmentationdimensionservice-" + evolutionEngineKey, Deployment.getSegmentationDimensionTopic(), false);
    segmentationDimensionService.start();

    //
    //  presentationStrategyService
    //

    presentationStrategyService = new PresentationStrategyService(bootstrapServers, "evolutionengine-presentationstrategyservice-" + evolutionEngineKey, Deployment.getPresentationStrategyTopic(), false);
    presentationStrategyService.start();

    //
    //  scoringStrategyService
    //

    scoringStrategyService = new ScoringStrategyService(bootstrapServers, "evolutionengine-scoringstrategyservice-" + evolutionEngineKey, Deployment.getScoringStrategyTopic(), false);
    scoringStrategyService.start();

    //
    //  offerService
    //

    offerService = new OfferService(bootstrapServers, "evolutionengine-offer-" + evolutionEngineKey, Deployment.getOfferTopic(), false);
    offerService.start();

    //
    //  salesChannelService
    //

    salesChannelService = new SalesChannelService(bootstrapServers, "evolutionengine-saleschannel-" + evolutionEngineKey, Deployment.getSalesChannelTopic(), false);
    salesChannelService.start();
    
    //
    //  productService
    //

    productService = new ProductService(bootstrapServers, "evolutionengine-product-" + evolutionEngineKey, Deployment.getProductTopic(), false);
    productService.start();
    
    //
    //  productTypeService
    //

    productTypeService = new ProductTypeService(bootstrapServers, "evolutionengine-producttype-" + evolutionEngineKey, Deployment.getProductTypeTopic(), false);
    productTypeService.start();

    //
    //  voucherService
    //

    voucherService = new VoucherService(bootstrapServers, "evolutionengine-voucher-" + evolutionEngineKey, Deployment.getVoucherTopic());
    voucherService.start();

    //
    //  voucherTypeService
    //

    voucherTypeService = new VoucherTypeService(bootstrapServers, "evolutionengine-vouchertype-" + evolutionEngineKey, Deployment.getVoucherTypeTopic(), false);
    voucherTypeService.start();

    //
    //  catalogCharacteristicService
    //

    catalogCharacteristicService = new CatalogCharacteristicService(bootstrapServers, "evolutionengine-catalogcharacteristic-" + evolutionEngineKey, Deployment.getCatalogCharacteristicTopic(), false);
    catalogCharacteristicService.start();

    //
    //  dnboMatrixService
    //

    dnboMatrixService = new DNBOMatrixService(bootstrapServers, "evolutionengine-dnbomatrix-" + evolutionEngineKey, Deployment.getDNBOMatrixTopic(), false);
    dnboMatrixService.start();
    
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
    // pointService
    //
    
    paymentMeanService = new PaymentMeanService(bootstrapServers, "evolutionengine-paymentmeanservice-" + evolutionEngineKey, Deployment.getPaymentMeanTopic(), false);
    paymentMeanService.start();

    //
    // exclusionInclusionTargetService
    //
    
    exclusionInclusionTargetService = new ExclusionInclusionTargetService(bootstrapServers, "evolutionengine-exclusioninclusiontargetservice-" + evolutionEngineKey, Deployment.getExclusionInclusionTargetTopic(), false);
    exclusionInclusionTargetService.start();
    
    //
    // stockService, used to apply campaign max number of customers limit
    //

    stockService = new StockMonitor("evolutionengine-stockservice-" + evolutionEngineKey, journeyService);
    stockService.start();


    //
    //  subscriberGroupEpochReader
    //

    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("evolutionengine-subscribergroupepoch", evolutionEngineKey, Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);

    //
    // propensity service
    //

    propensityService = new PropensityService(subscriberGroupEpochReader);

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
    streamsProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Deployment.getEvolutionEngineStreamThreads());
    streamsProperties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, Integer.toString(kafkaReplicationFactor));
    streamsProperties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, Integer.toString(kafkaStreamsStandbyReplicas));
    streamsProperties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString());
    streamsProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, subscriberProfileHost + ":" + Integer.toString(internalPort));
    streamsProperties.put("producer.batch.size", Integer.toString(100000));
    if(!isInMemoryStateStores && rocksDBCacheMBytes!=-1 && rocksDBMemTableMBytes!=-1)
      {
        BoundedMemoryRocksDBConfig.setRocksDBCacheMBytes(rocksDBCacheMBytes);
        BoundedMemoryRocksDBConfig.setRocksDBMemTableMBytes(rocksDBMemTableMBytes);
        streamsProperties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedMemoryRocksDBConfig.class);
      }
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
        switch (evolutionEngineEvent.getEventRule())
          {
            case All:
            case Standard:
            case Extended:
              evolutionEngineEventTopics.put(evolutionEngineEvent, evolutionEngineEvent.getEventTopic());
              evolutionEngineEventSerdes.put(evolutionEngineEvent, evolutionEngineEvent.getEventSerde());
              break;
          }
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
    final ConnectSerde<CleanupSubscriber> cleanupSubscriberSerde = CleanupSubscriber.serde();
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
    final ConnectSerde<JourneyMetric> journeyMetricSerde = JourneyMetric.serde();
    final ConnectSerde<LoyaltyProgramRequest> loyaltyProgramRequestSerde = LoyaltyProgramRequest.serde();
    final ConnectSerde<SubscriberGroup> subscriberGroupSerde = SubscriberGroup.serde();
    final ConnectSerde<SubscriberTraceControl> subscriberTraceControlSerde = SubscriberTraceControl.serde();
    final ConnectSerde<SubscriberProfile> subscriberProfileSerde = SubscriberProfile.getSubscriberProfileSerde();
    final Serde<SubscriberTrace> subscriberTraceSerde = SubscriberTrace.serde();
    final Serde<ExternalAPIOutput> externalAPISerde = ExternalAPIOutput.serde();
    final Serde<TokenChange> tokenChangeSerde = TokenChange.serde();
    final ConnectSerde<VoucherChange> voucherChangeSerde = VoucherChange.serde();

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
    evolutionEventSerdes.add(cleanupSubscriberSerde);
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
    //  core streams
    //

    KStream<StringKey, TimedEvaluation> timedEvaluationSourceStream = builder.stream(timedEvaluationTopic, Consumed.with(stringKeySerde, timedEvaluationSerde));
    KStream<StringKey, CleanupSubscriber> cleanupSubscriberSourceStream = builder.stream(cleanupSubscriberTopic, Consumed.with(stringKeySerde, cleanupSubscriberSerde));
    KStream<StringKey, SubscriberProfileForceUpdate> subscriberProfileForceUpdateSourceStream = builder.stream(subscriberProfileForceUpdateTopic, Consumed.with(stringKeySerde, subscriberProfileForceUpdateSerde));
    KStream<StringKey, RecordSubscriberID> recordSubscriberIDSourceStream = builder.stream(recordSubscriberIDTopic, Consumed.with(stringKeySerde, recordSubscriberIDSerde));
    KStream<StringKey, JourneyRequest> journeyRequestSourceStream = builder.stream(journeyRequestTopic, Consumed.with(stringKeySerde, journeyRequestSerde));
    KStream<StringKey, LoyaltyProgramRequest> loyaltyProgramRequestSourceStream = builder.stream(loyaltyProgramRequestTopic, Consumed.with(stringKeySerde, loyaltyProgramRequestSerde));
    KStream<StringKey, SubscriberGroup> subscriberGroupSourceStream = builder.stream(subscriberGroupTopic, Consumed.with(stringKeySerde, subscriberGroupSerde));
    KStream<StringKey, SubscriberTraceControl> subscriberTraceControlSourceStream = builder.stream(subscriberTraceControlTopic, Consumed.with(stringKeySerde, subscriberTraceControlSerde));
    KStream<StringKey, PresentationLog> presentationLogSourceStream = builder.stream(presentationLogTopic, Consumed.with(stringKeySerde, presentationLogSerde));
    KStream<StringKey, AcceptanceLog> acceptanceLogSourceStream = builder.stream(acceptanceLogTopic, Consumed.with(stringKeySerde, acceptanceLogSerde));
    KStream<StringKey, ProfileSegmentChangeEvent> profileSegmentChangeEventStream = builder.stream(profileSegmentChangeEventTopic, Consumed.with(stringKeySerde, profileSegmentChangeEventSerde));
    KStream<StringKey, ProfileLoyaltyProgramChangeEvent> profileLoyaltyProgramChangeEventStream = builder.stream(profileLoyaltyProgramChangeEventTopic, Consumed.with(stringKeySerde, profileLoyaltyProgramChangeEventSerde));
    KStream<StringKey, PointFulfillmentRequest> rekeyedPointFulfillmentRequestSourceStream = builder.stream(pointFulfillmentRekeyedTopic, Consumed.with(stringKeySerde, pointFulfillmentRequestSerde));
    KStream<StringKey, VoucherChange> voucherChangeRequestSourceStream = builder.stream(voucherChangeRequestTopic, Consumed.with(stringKeySerde, voucherChangeSerde));

    //
    //  timedEvaluationStreams
    //

    KStream<StringKey, ? extends TimedEvaluation>[] branchedTimedEvaluationStreams = timedEvaluationSourceStream.branch((key,value) -> ((TimedEvaluation) value).getPeriodicEvaluation(), (key,value) -> true);
    KStream<StringKey, TimedEvaluation> periodicTimedEvaluationStream = (KStream<StringKey, TimedEvaluation>) branchedTimedEvaluationStreams[0];
    KStream<StringKey, TimedEvaluation> standardTimedEvaluationStream = (KStream<StringKey, TimedEvaluation>) branchedTimedEvaluationStreams[1];

    //
    //  evolution engine event source streams
    //

    List<KStream<StringKey, ? extends SubscriberStreamEvent>> standardEvolutionEngineEventStreams = new ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    List<KStream<StringKey, ? extends SubscriberStreamEvent>> extendedProfileEvolutionEngineEventStreams = new ArrayList<KStream<StringKey, ? extends SubscriberStreamEvent>>();
    for (EvolutionEngineEventDeclaration evolutionEngineEventDeclaration : Deployment.getEvolutionEngineEvents().values())
      {
        KStream<StringKey, ? extends SubscriberStreamEvent> evolutionEngineEventStream;
        if (evolutionEngineEventTopics.get(evolutionEngineEventDeclaration) != null)
          {
            switch (evolutionEngineEventDeclaration.getEventRule())
              {
                case All:
                  evolutionEngineEventStream = builder.stream(
                      evolutionEngineEventTopics.get(evolutionEngineEventDeclaration),
                      Consumed.with(stringKeySerde, evolutionEngineEventSerdes.get(evolutionEngineEventDeclaration)));
                  standardEvolutionEngineEventStreams.add(evolutionEngineEventStream);
                  extendedProfileEvolutionEngineEventStreams.add(evolutionEngineEventStream);
                  break;

                case Standard:
                  evolutionEngineEventStream = builder.stream(
                      evolutionEngineEventTopics.get(evolutionEngineEventDeclaration),
                      Consumed.with(stringKeySerde, evolutionEngineEventSerdes.get(evolutionEngineEventDeclaration)));
                  standardEvolutionEngineEventStreams.add(evolutionEngineEventStream);
                  break;

                case Extended:
                  evolutionEngineEventStream = builder.stream(
                      evolutionEngineEventTopics.get(evolutionEngineEventDeclaration),
                      Consumed.with(stringKeySerde, evolutionEngineEventSerdes.get(evolutionEngineEventDeclaration)));
                  extendedProfileEvolutionEngineEventStreams.add(evolutionEngineEventStream);
                  break;
              }
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
    extendedProfileEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) cleanupSubscriberSourceStream);
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

    //KeyValueBytesStoreSupplier extendedProfileSupplier = Stores.persistentKeyValueStore(extendedSubscriberProfileChangeLog);
    KeyValueBytesStoreSupplier extendedProfileSupplier = isInMemoryStateStores?Stores.inMemoryKeyValueStore(extendedSubscriberProfileChangeLog):Stores.persistentKeyValueStore(extendedSubscriberProfileChangeLog);

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
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) cleanupSubscriberSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberProfileForceUpdateSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) recordSubscriberIDSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) journeyRequestSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) loyaltyProgramRequestSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberGroupSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberTraceControlSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) presentationLogSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) acceptanceLogSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) profileSegmentChangeEventStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) profileLoyaltyProgramChangeEventStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) rekeyedPointFulfillmentRequestSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) voucherChangeRequestSourceStream);
    evolutionEventStreams.addAll(standardEvolutionEngineEventStreams);
    evolutionEventStreams.addAll(deliveryManagerResponseStreams);
    KStream evolutionEventCompositeStream = null;
    for (KStream<StringKey, ? extends SubscriberStreamEvent> eventStream : evolutionEventStreams)
      {
        evolutionEventCompositeStream = (evolutionEventCompositeStream == null) ? eventStream : evolutionEventCompositeStream.merge(eventStream);
      }
    KStream<StringKey, SubscriberStreamEvent> evolutionEventStream = (KStream<StringKey, SubscriberStreamEvent>) evolutionEventCompositeStream;

    //
    //  aggregate
    //

    //KeyValueBytesStoreSupplier subscriberStateSupplier = Stores.persistentKeyValueStore(subscriberStateChangeLog);
    KeyValueBytesStoreSupplier subscriberStateSupplier = isInMemoryStateStores?Stores.inMemoryKeyValueStore(subscriberStateChangeLog):Stores.persistentKeyValueStore(subscriberStateChangeLog);
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

        (key,value) -> (value instanceof ExternalAPIOutput),
	    (key,value) -> (value instanceof ProfileChangeEvent),
        (key,value) -> (value instanceof ProfileSegmentChangeEvent),
        (key,value) -> (value instanceof ProfileLoyaltyProgramChangeEvent),
        (key,value) -> (value instanceof TokenChange),

        (key,value) -> (value instanceof VoucherChange));

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

    KStream<StringKey, ExternalAPIOutput> externalAPIOutputsStream = (KStream<StringKey, ExternalAPIOutput>) branchedEvolutionEngineOutputs[10];
    KStream<StringKey, ProfileChangeEvent> profileChangeEventsStream = (KStream<StringKey, ProfileChangeEvent>) branchedEvolutionEngineOutputs[11];
    KStream<StringKey, ProfileSegmentChangeEvent> profileSegmentChangeEventsStream = (KStream<StringKey, ProfileSegmentChangeEvent>) branchedEvolutionEngineOutputs[12];
    KStream<StringKey, ProfileLoyaltyProgramChangeEvent> profileLoyaltyProgramChangeEventsStream = (KStream<StringKey, ProfileLoyaltyProgramChangeEvent>) branchedEvolutionEngineOutputs[13];
    KStream<StringKey, TokenChange> tokenChangeStream = (KStream<StringKey, TokenChange>) branchedEvolutionEngineOutputs[14];

    KStream<StringKey, VoucherChange> voucherChangeStream = (KStream<StringKey, VoucherChange>) branchedEvolutionEngineOutputs[15];


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

    /*****************************************
    *
    *  subscriberHistory -- update
    *
    *****************************************/

    //
    //  merge source streams -- subscriberHistoryStream
    //

    KStream subscriberHistoryCompositeStream = journeyStatisticStream;
    subscriberHistoryCompositeStream = subscriberHistoryCompositeStream.merge(cleanupSubscriberSourceStream);
    for (KStream<StringKey, ? extends SubscriberStreamEvent> eventStream : deliveryManagerResponseStreams)
      {
        subscriberHistoryCompositeStream = (subscriberHistoryCompositeStream == null) ? eventStream : subscriberHistoryCompositeStream.merge(eventStream);
      }
    KStream<StringKey, SubscriberStreamEvent> subscriberHistoryStream = (KStream<StringKey, SubscriberStreamEvent>) subscriberHistoryCompositeStream;

    //
    //  aggregate
    //

    //KeyValueBytesStoreSupplier subscriberHistorySupplier = Stores.persistentKeyValueStore(subscriberHistoryChangeLog);
    KeyValueBytesStoreSupplier subscriberHistorySupplier = isInMemoryStateStores?Stores.inMemoryKeyValueStore(subscriberHistoryChangeLog):Stores.persistentKeyValueStore(subscriberHistoryChangeLog);
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
    profileChangeEventsStream.to(profileChangeEventTopic, Produced.with(stringKeySerde, profileChangeEventSerde));
    profileSegmentChangeEventsStream.to(profileSegmentChangeEventTopic, Produced.with(stringKeySerde, profileSegmentChangeEventSerde));
    profileLoyaltyProgramChangeEventsStream.to(profileLoyaltyProgramChangeEventTopic, Produced.with(stringKeySerde, profileLoyaltyProgramChangeEventSerde));
    rekeyedJourneyStatisticStream.to(journeyTrafficTopic, Produced.with(stringKeySerde, journeyStatisticWrapperSerde));
    tokenChangeStream.to(tokenChangeTopic, Produced.with(stringKeySerde, tokenChangeSerde));
    voucherChangeStream.to(voucherChangeResponseTopic, Produced.with(stringKeySerde, voucherChangeSerde));

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
            KStream<String, String> externalAPIStreamString = rekeyedExternalAPIStream.map(
                (key,value) -> new KeyValue<String, String>(value.getTopicID(), value.getJsonString()));
            externalAPIStreamString.to(externalAPIOutputTopics[k], Produced.with(new Serdes.StringSerde(), new Serdes.StringSerde()));
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

    Date errorLogDate = RLMDateUtils.addMinutes(SystemTime.getCurrentTime(), 1);
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
            if (SystemTime.getCurrentTime().after(errorLogDate))
              {
                log.error(stackTraceWriter.toString());
                errorLogDate = RLMDateUtils.addMinutes(errorLogDate, 1);
              }
            else
              {
                log.debug(stackTraceWriter.toString());
              }
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
    httpClientConnectionManager.setDefaultMaxPerRoute(Deployment.getEvolutionEngineStreamThreads());
    httpClientConnectionManager.setMaxTotal(Deployment.getEvolutionEngineInstanceNumbers()*Deployment.getEvolutionEngineStreamThreads());

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
        subscriberProfileServer.setExecutor(Executors.newFixedThreadPool(Deployment.getEvolutionEngineInstanceNumbers()*Deployment.getEvolutionEngineStreamThreads()));
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
        internalServer.setExecutor(Executors.newFixedThreadPool(Deployment.getEvolutionEngineStreamThreads()));
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

    NGLMRuntime.addShutdownHook(new ShutdownHook(streams, subscriberGroupEpochReader, ucgStateReader, dynamicCriterionFieldService, journeyService, loyaltyProgramService, targetService, journeyObjectiveService, segmentationDimensionService, presentationStrategyService, scoringStrategyService, offerService, salesChannelService, tokenTypeService, subscriberMessageTemplateService, deliverableService, segmentContactPolicyService, timerService, pointService, exclusionInclusionTargetService, productService, productTypeService, voucherService, voucherTypeService, catalogCharacteristicService, dnboMatrixService, paymentMeanService, subscriberProfileServer, internalServer, stockService));

    /*****************************************
    *
    *  start timerService
    *
    *****************************************/

    timerService.start(subscriberStateStore, subscriberGroupEpochReader, targetService, journeyService, exclusionInclusionTargetService);

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
   *  class BoundedMemoryRocksDBConfig
   *
   ****************************************/

  public static class BoundedMemoryRocksDBConfig implements RocksDBConfigSetter
  {

    // https://docs.confluent.io/current/streams/developer-guide/memory-mgmt.html#rocksdb
    // https://github.com/facebook/rocksdb/blob/master/java/src/main/java/org/rocksdb/LRUCache.java
    // https://github.com/apache/kafka/blob/2.3/streams/src/main/java/org/apache/kafka/streams/state/internals/RocksDBStore.java
    // https://docs.confluent.io/current/streams/developer-guide/config-streams.html#rocksdb-config-setter
    // https://kafka.apache.org/23/documentation/streams/developer-guide/memory-mgmt.html#id3
    // https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
    // https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB

    //
    //  data
    //

    //really just for logging, and make understand that the cache is shared for all stores of the app
    private static final String CACHE_NAME="evolution_rocksdb_cache";

    private static long TOTAL_OFF_HEAP_MEMORY=-1;
    private static long TOTAL_MEMTABLE_MEMORY=-1;

    // https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#flushing-options
    private static final int N_MEMTABLE=2;

    private static org.rocksdb.Cache cache = null;
    private static org.rocksdb.WriteBufferManager writeBufferManager = null;

    //
    //  setConfig
    //

    @Override public void setConfig(final String storeName, final Options options, final Map<String, Object> configs)
    {
      if(cache==null){
        synchronized (BoundedMemoryRocksDBConfig.class){
          if(cache==null){
            if(TOTAL_OFF_HEAP_MEMORY>-1){
              log.info("creating {} shared cache, {} off-heap", CACHE_NAME, FileUtils.byteCountToDisplaySize(TOTAL_OFF_HEAP_MEMORY));
              if(TOTAL_MEMTABLE_MEMORY<=TOTAL_MEMTABLE_MEMORY){
                log.info("{} of {} {} shared cache, will be allocate for memtable", FileUtils.byteCountToDisplaySize(TOTAL_MEMTABLE_MEMORY), FileUtils.byteCountToDisplaySize(TOTAL_OFF_HEAP_MEMORY), CACHE_NAME);
                cache=new org.rocksdb.LRUCache(TOTAL_OFF_HEAP_MEMORY, -1, false, 0.0d);
                writeBufferManager=new org.rocksdb.WriteBufferManager(TOTAL_MEMTABLE_MEMORY, cache);
              }else{
                log.error("memtable cache should fit in shared cache!");
                throw new RuntimeException();
              }
            }else{
              log.error("bug in code");
              throw new RuntimeException();
            }
          }
        }
      }
      log.info("Setting RocksDB config using {} off-heap shared cache for store {}", CACHE_NAME, storeName);
      BlockBasedTableConfig tableConfig = new org.rocksdb.BlockBasedTableConfig();
      tableConfig.setBlockCache(cache);
      tableConfig.setCacheIndexAndFilterBlocks(true);
      options.setWriteBufferManager(writeBufferManager);
      tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
      tableConfig.setPinTopLevelIndexAndFilter(true);
      tableConfig.setBlockSize(32*1024L);
      options.setMaxWriteBufferNumber(N_MEMTABLE);
      options.setWriteBufferSize(TOTAL_MEMTABLE_MEMORY/N_MEMTABLE);
      options.setTableFormatConfig(tableConfig);
    }

    @Override public void close(String storeName, Options options)
    {
      // https://docs.confluent.io/current/streams/developer-guide/memory-mgmt.html
      // shared "cache" for all stores, not closing it then
      log.warn("called BoundedMemoryRocksDBConfig.close for store {}, not doing anything as {} is shared", storeName, CACHE_NAME);
    }

    protected static void setRocksDBCacheMBytes(int cacheMBytes)
    {
      // might not yet able to create the static org.rocksdb.LRUCache object, cause of underlying rocksdb native lib "not yet deployed", so will lazy instantiate
      TOTAL_OFF_HEAP_MEMORY=cacheMBytes * 1024 * 1024L;
    }

    protected static void setRocksDBMemTableMBytes(int memtableMBytes)
    {
      TOTAL_MEMTABLE_MEMORY=memtableMBytes * 1024 * 1024L;
    }

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
    private DynamicCriterionFieldService dynamicCriterionFieldsService;
    private JourneyService journeyService;
    private LoyaltyProgramService loyaltyProgramService;
    private TargetService targetService;
    private JourneyObjectiveService journeyObjectiveService;
    private SegmentationDimensionService segmentationDimensionService;
    private PresentationStrategyService presentationStrategyService;
    private ScoringStrategyService scoringStrategyService;
    private OfferService offerService;
    private SalesChannelService salesChannelService;
    private ProductService productService;
    private ProductTypeService productTypeService;
    private VoucherService voucherService;
    private VoucherTypeService voucherTypeService;
    private CatalogCharacteristicService catalogCharacteristicService;
    private DNBOMatrixService dnboMatrixService;
    private TokenTypeService tokenTypeService;
    private SubscriberMessageTemplateService subscriberMessageTemplateService;
    private DeliverableService deliverableService;
    private SegmentContactPolicyService segmentContactPolicyService;
    private TimerService timerService;
    private PointService pointService;
    private PaymentMeanService paymentMeanService;
    private ExclusionInclusionTargetService exclusionInclusionTargetService;
    private HttpServer subscriberProfileServer;
    private HttpServer internalServer;
    private StockMonitor stockService;

    //
    //  constructor
    //

    private ShutdownHook(KafkaStreams kafkaStreams, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,UCGState> ucgStateReader, DynamicCriterionFieldService dynamicCriterionFieldsService, JourneyService journeyService, LoyaltyProgramService loyaltyProgramService, TargetService targetService, JourneyObjectiveService journeyObjectiveService, SegmentationDimensionService segmentationDimensionService, PresentationStrategyService presentationStrategyService, ScoringStrategyService scoringStrategyService, OfferService offerService, SalesChannelService salesChannelService, TokenTypeService tokenTypeService, SubscriberMessageTemplateService subscriberMessageTemplateService, DeliverableService deliverableService, SegmentContactPolicyService segmentContactPolicyService, TimerService timerService, PointService pointService, ExclusionInclusionTargetService exclusionInclusionTargetService, ProductService productService, ProductTypeService productTypeService, VoucherService voucherService, VoucherTypeService voucherTypeService, CatalogCharacteristicService catalogCharacteristicService, DNBOMatrixService dnboMatrixService, PaymentMeanService paymentMeanService, HttpServer subscriberProfileServer, HttpServer internalServer, StockMonitor stockService)
    {
      this.kafkaStreams = kafkaStreams;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
      this.ucgStateReader = ucgStateReader;
      this.dynamicCriterionFieldsService = dynamicCriterionFieldsService;
      this.journeyService = journeyService;
      this.loyaltyProgramService = loyaltyProgramService;
      this.targetService = targetService;
      this.journeyObjectiveService = journeyObjectiveService;
      this.segmentationDimensionService = segmentationDimensionService;
      this.presentationStrategyService = presentationStrategyService;
      this.scoringStrategyService = scoringStrategyService;
      this.offerService = offerService;
      this.salesChannelService = salesChannelService;
      this.tokenTypeService = tokenTypeService;
      this.subscriberMessageTemplateService = subscriberMessageTemplateService;
      this.deliverableService = deliverableService;
      this.segmentContactPolicyService = segmentContactPolicyService;
      this.timerService = timerService;
      this.pointService = pointService;
      this.exclusionInclusionTargetService = exclusionInclusionTargetService;
      this.productService = productService;
      this.productTypeService = productTypeService;
      this.voucherService = voucherService;
      this.voucherTypeService = voucherTypeService;
      this.catalogCharacteristicService = catalogCharacteristicService;
      this.dnboMatrixService = dnboMatrixService;
      this.paymentMeanService = paymentMeanService;
      this.subscriberProfileServer = subscriberProfileServer;
      this.internalServer = internalServer;
      this.stockService = stockService;
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

      dynamicCriterionFieldsService.stop();
      journeyService.stop();
      loyaltyProgramService.stop();
      targetService.stop();
      journeyObjectiveService.stop();
      segmentationDimensionService.stop();
      presentationStrategyService.stop();
      scoringStrategyService.stop();
      offerService.stop();
      salesChannelService.stop();
      productService.stop();
      productTypeService.stop();
      voucherService.stop();
      voucherTypeService.stop();
      catalogCharacteristicService.stop();
      dnboMatrixService.stop();
      paymentMeanService.stop();
      tokenTypeService.stop();
      subscriberMessageTemplateService.stop();
      deliverableService.stop();
      segmentContactPolicyService.stop();
      timerService.stop();
      pointService.stop();
      exclusionInclusionTargetService.stop();
      stockService.close();

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
    EvolutionEventContext context = new EvolutionEventContext(subscriberState, extendedSubscriberProfile, subscriberGroupEpochReader, journeyService, subscriberMessageTemplateService, deliverableService, segmentationDimensionService, presentationStrategyService, scoringStrategyService, offerService, salesChannelService, tokenTypeService, segmentContactPolicyService, productService, productTypeService, voucherService, voucherTypeService, catalogCharacteristicService, dnboMatrixService, paymentMeanService, uniqueKeyServer, SystemTime.getCurrentTime());
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
    *  cleanup
    *
    *****************************************/

    switch (evolutionEvent.getSubscriberAction())
      {
        case Cleanup:
          updateScheduledEvaluations((currentSubscriberState != null) ? currentSubscriberState.getScheduledEvaluations() : Collections.<TimedEvaluation>emptySet(), Collections.<TimedEvaluation>emptySet());
          return null;
          
        case Delete:
          cleanSubscriberState(currentSubscriberState, now);
          SubscriberState.stateStoreSerde().setKafkaRepresentation(Deployment.getSubscriberStateChangeLogTopic(), currentSubscriberState);
          return currentSubscriberState;
      }
    
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
    *  update Tokens
    *
    *****************************************/

    subscriberStateUpdated = updateTokens(context, evolutionEvent) || subscriberStateUpdated;

    /*****************************************
     *
     *  update Vouchers
     *
     *****************************************/

    subscriberStateUpdated = updateVouchers(context, evolutionEvent) || subscriberStateUpdated;

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
    *  detectReScheduledDeliveryRequests : add to scheduledEvaluations
    *
    *****************************************/

    subscriberStateUpdated = detectReScheduledDeliveryRequests(context, evolutionEvent) || subscriberStateUpdated;

    /*****************************************
    *
    *  handleReScheduledDeliveryRequest : trig again a Delivery Request
    *
    *****************************************/

    subscriberStateUpdated = handleReScheduledDeliveryRequest(context, evolutionEvent) || subscriberStateUpdated;
    
    /*****************************************
    *
    *  scheduledEvaluations
    *
    *****************************************/

    updateScheduledEvaluations((currentSubscriberState != null) ? currentSubscriberState.getScheduledEvaluations() : Collections.<TimedEvaluation>emptySet(), subscriberState.getScheduledEvaluations());

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
        SubscriberState.stateStoreSerde().setKafkaRepresentation(Deployment.getSubscriberStateChangeLogTopic(), subscriberState);
        evolutionEngineStatistics.updateSubscriberStateSize(subscriberState.getKafkaRepresentation());
        if (subscriberState.getKafkaRepresentation().length > 950000)
          {
            log.error("StateStore size error, ignoring event {} for subscriber {}: {}", evolutionEvent.getClass().toString(), evolutionEvent.getSubscriberID(), subscriberState.getKafkaRepresentation().length);
            cleanSubscriberState(currentSubscriberState, now);
            SubscriberState.stateStoreSerde().setKafkaRepresentation(Deployment.getSubscriberStateChangeLogTopic(), currentSubscriberState);
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
    //  abort if null
    //

    if (subscriberState == null)
      {
        return false;
      }
    
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
            subscriberState.getSubscriberProfile().getSubscriberJourneys().remove(recentJourneyState.getJourneyID());
            recentJourneyStates.remove();
            subscriberStateUpdated = true;
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
    //  profileChangeEvents cleaning
    //

    if (subscriberState.getProfileChangeEvents() != null)
      {
        subscriberState.getProfileChangeEvents().clear();
        subscriberStateUpdated = true;
      }

    //
    //  tokenChange cleaning
    //

    if (subscriberState.getTokenChanges() != null)
      {
        subscriberState.getTokenChanges().clear();
        subscriberStateUpdated = true;
      }
    
    //
    //  voucherChange cleaning
    //

    if (subscriberState.getVoucherChanges() != null)
    {
      subscriberState.getVoucherChanges().clear();
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
    Map<String, PointBalance> pointBalances = subscriberState.getSubscriberProfile().getPointBalances() != null ? subscriberState.getSubscriberProfile().getPointBalances() : Collections.<String,PointBalance>emptyMap();
    for(String pointID: pointBalances.keySet())
      {
        Point point = pointService.getActivePoint(pointID, now);
        if(point != null)
          {
            //TODO : what module is best here ?
            updatePointBalance(context, null, "checkBonusExpiration", Module.Unknown.getExternalRepresentation(), "checkBonusExpiration", subscriberState.getSubscriberProfile(), point, CommodityDeliveryOperation.Expire, 0, now, true);
          }
      }
  }

  /*****************************************
   *
   *  updateVouchers
   *
   *****************************************/

  private static boolean updateVouchers(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent)
  {

    SubscriberState subscriberState = context.getSubscriberState();
    SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();
    Date now = SystemTime.getCurrentTime();
    boolean subscriberUpdated = false;

    // first process the new ones coming with purchase response event
    if (evolutionEvent instanceof PurchaseFulfillmentRequest){

      PurchaseFulfillmentRequest purchaseFulfillmentRequest=(PurchaseFulfillmentRequest)evolutionEvent;
      if(log.isDebugEnabled()) log.debug("will process purchase request for vouchers : "+purchaseFulfillmentRequest);

      // check if there is vouchers delivery in this purchase request :
      if(purchaseFulfillmentRequest.getVoucherDeliveries()!=null && !purchaseFulfillmentRequest.getVoucherDeliveries().isEmpty()){
        if(log.isDebugEnabled()) log.debug("purchase request contains voucher deliveries to process");

        for(VoucherDelivery voucherDelivery:purchaseFulfillmentRequest.getVoucherDeliveries()){
          Voucher voucher = voucherService.getActiveVoucher(voucherDelivery.getVoucherID(),now);
          VoucherType voucherType = voucherTypeService.getActiveVoucherType(voucher.getVoucherTypeId(),now);
          if(voucherType==null){
            log.warn("no more voucher type for voucherId "+voucherDelivery.getVoucherID()+", skipping "+voucherDelivery.getVoucherCode()+" for "+context.getSubscriberState().getSubscriberID());
          }else{
            Date expiryDate=null;
            // compute expiry date here for relative expiry vouchers
            if(voucherType.getCodeType()==VoucherType.CodeType.Shared){
              expiryDate = EvolutionUtilities.addTime(now,voucherType.getValidity().getPeriodQuantity(),voucherType.getValidity().getPeriodType(),Deployment.getBaseTimeZone(),voucherType.getValidity().getRoundDown()? EvolutionUtilities.RoundingSelection.RoundDown: EvolutionUtilities.RoundingSelection.NoRound);
            }else if(voucherType.getCodeType()==VoucherType.CodeType.Personal){
              VoucherPersonal voucherPersonal = (VoucherPersonal) voucher;
              for(VoucherFile voucherFile:voucherPersonal.getVoucherFiles()){
                if(voucherFile.getFileId().equals(voucherDelivery.getFileID())){
                  if(voucherFile.getExpiryDate()==null){
                    expiryDate = EvolutionUtilities.addTime(now,voucherType.getValidity().getPeriodQuantity(),voucherType.getValidity().getPeriodType(),Deployment.getBaseTimeZone(),voucherType.getValidity().getRoundDown()? EvolutionUtilities.RoundingSelection.RoundDown: EvolutionUtilities.RoundingSelection.NoRound);
                  }
                  break;
                }
              }
            }
            // or the absolute one from ES
            if(expiryDate==null){
              expiryDate=voucherDelivery.getVoucherExpiryDate();
            }
            if(expiryDate==null){
              log.error("voucher "+voucherDelivery.getVoucherCode()+" for "+subscriberProfile.getSubscriberID()+" could not compute an expiryDate !! "+voucher.getVoucherID());
            }else{
              voucherDelivery.setVoucherStatus(VoucherDelivery.VoucherStatus.Delivered);
            }

            // storing the voucher
            VoucherProfileStored voucherToStore = new VoucherProfileStored(
              voucherDelivery.getVoucherID(),
              voucherDelivery.getFileID(),
              voucherDelivery.getVoucherCode(),
              voucherDelivery.getVoucherStatus(),
              expiryDate,
              purchaseFulfillmentRequest.getCreationDate(),
              null,
              purchaseFulfillmentRequest.getOfferID(),
              purchaseFulfillmentRequest.getEventID(),
              purchaseFulfillmentRequest.getModuleID(),
              purchaseFulfillmentRequest.getFeatureID(),
              purchaseFulfillmentRequest.getOrigin()
            );
            subscriberProfile.getVouchers().add(voucherToStore);
            subscriberUpdated = true;
          }
        }
      }else{
        if(log.isDebugEnabled()) log.debug("no voucher delivered in purchaseFulfillmentRequest");
      }
    }

    // no we check all, if some expired, to clean, expired to generate event ???
    if(subscriberProfile.getVouchers()!=null && !subscriberProfile.getVouchers().isEmpty()){
      Date cleanBeforeThisDate = EvolutionUtilities.addTime(now,-1*Deployment.getCleanUpExpiredVoucherDelayInDays(),TimeUnit.Day,Deployment.getBaseTimeZone());
      // safety check
      if(now.before(cleanBeforeThisDate)){
        log.warn("check me!! clean up date is in the future !!");
      }else{
        Iterator<VoucherProfileStored> iterator=subscriberProfile.getVouchers().iterator();
        while(iterator.hasNext()){
          VoucherProfileStored voucherStored = iterator.next();
          if(voucherStored.getVoucherExpiryDate()==null){
            log.warn("voucher expiration date is null ???");
            continue;
          }
          // clean up old ones
          if(voucherStored.getVoucherExpiryDate().before(cleanBeforeThisDate)){
            if(log.isDebugEnabled()) log.debug("removed expired voucher "+voucherStored+" from "+subscriberProfile.getSubscriberID()+" profile");
            iterator.remove();//just remove it from subscriber profile
            subscriberUpdated=true;
          }
          // change status to expired
          if(voucherStored.getVoucherStatus()!=VoucherDelivery.VoucherStatus.Expired
                  && voucherStored.getVoucherStatus()!=VoucherDelivery.VoucherStatus.Redeemed
                  && voucherStored.getVoucherExpiryDate().before(now)){
            voucherStored.setVoucherStatus(VoucherDelivery.VoucherStatus.Expired);
            subscriberUpdated=true;
          }

        }
      }
    }else{
      if(log.isDebugEnabled()) log.debug("no vouchers stored in profile for "+subscriberProfile.getSubscriberID());
    }

    // check if we have update request
    if (evolutionEvent instanceof VoucherChange){
      VoucherChange voucherChange = (VoucherChange)evolutionEvent;
      if(log.isDebugEnabled()) log.debug("voucher change to process : "+voucherChange);
      // basics checks for not OK
      if(voucherChange.getAction()==VoucherChange.VoucherChangeAction.Unknown){
        voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
      }else{
        if(subscriberProfile.getVouchers()!=null && !subscriberProfile.getVouchers().isEmpty()){
          boolean voucherFound=false;
          for(VoucherProfileStored voucherStored:subscriberProfile.getVouchers()){
            if(voucherStored.getVoucherCode().equals(voucherChange.getVoucherCode()) && voucherStored.getVoucherID().equals(voucherChange.getVoucherID())){
              voucherFound=true;
              if(log.isDebugEnabled()) log.debug("need to apply to stored voucher "+voucherStored);

              // redeem
              if(voucherChange.getAction()==VoucherChange.VoucherChangeAction.Redeem){
                if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Redeemed){
                  voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_ALREADY_REDEEMED);
                }
                if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Expired){
                  voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_EXPIRED);
                }
                // redeem voucher OK
                if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Delivered){
                  voucherStored.setVoucherStatus(VoucherDelivery.VoucherStatus.Redeemed);
                  voucherStored.setVoucherRedeemDate(now);
                  voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);
                  break;//NOTE WE DO NOT ORDER VOUCHER PER EXPIRY DATE OR, WE TAKE THE FISRT ONE OK
                }
                voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_NON_REDEEMABLE);
              }

              // extend
              if(voucherChange.getAction()==VoucherChange.VoucherChangeAction.Extend){
                if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Redeemed){
                  voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_ALREADY_REDEEMED);
                }
                // extend voucher OK
                voucherStored.setVoucherExpiryDate(voucherChange.getNewVoucherExpiryDate());
                if(voucherStored.getVoucherExpiryDate().after(now)) voucherStored.setVoucherStatus(VoucherDelivery.VoucherStatus.Delivered);
                voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);
                break;//NOTE WE DO NOT ORDER VOUCHER PER EXPIRY DATE OR, WE TAKE THE FISRT ONE OK
              }

              // delete (expire it)
              if(voucherChange.getAction()==VoucherChange.VoucherChangeAction.Expire){
                if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Redeemed){
                  voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_ALREADY_REDEEMED);
                }
                if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Expired){
                  voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_EXPIRED);
                }
                // expire voucher OK
                voucherStored.setVoucherExpiryDate(now);
                if(voucherStored.getVoucherExpiryDate().after(now)) voucherStored.setVoucherStatus(VoucherDelivery.VoucherStatus.Expired);
                voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);
                break;//NOTE WE DO NOT ORDER VOUCHER PER EXPIRY DATE OR, WE TAKE THE FISRT ONE OK
              }

            }
          }
          if(!voucherFound) voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_NOT_ASSIGNED);
        }else{
          if(log.isDebugEnabled()) log.debug("no vouchers stored in profile for "+subscriberProfile.getSubscriberID());
          voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_NOT_ASSIGNED);
        }
      }
      //need to respond
      subscriberState.getVoucherChanges().add(voucherChange);
      subscriberUpdated=true;
    }

    return subscriberUpdated;
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
        if (dimension == null)
          {
            log.warn("unknown dimensionID " + dimensionId + " from keyset " + segmentsMap.keySet());
          }
        else
          {
            profileSegmentChangeNewValues.put(dimension.getSegmentationDimensionName(), segmentsMap.get(dimensionId));
          }
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
  *  detectReScheduledDeliveryRequests
  *
  *****************************************/

  private static boolean detectReScheduledDeliveryRequests(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent)
  {
    /*****************************************
    *
    *  result
    *
    *****************************************/

    SubscriberState subscriberState = context.getSubscriberState();
    SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();
    boolean subscriberProfileUpdated = false;

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = context.now();

    /*****************************************
    *
    *  for DeliveryRequest with status Reschedule
    *
    *****************************************/

    if (evolutionEvent instanceof DeliveryRequest && ((DeliveryRequest) evolutionEvent).getDeliveryStatus().equals(DeliveryStatus.Reschedule))
      {

        //
        // Construct a ReScheduleDeliveryRequest and to SubscriberState
        //

        DeliveryRequest deliveryRequest = (DeliveryRequest)evolutionEvent;
        log.info("Rescheduled Request for " + deliveryRequest.getRescheduledDate());
        ReScheduledDeliveryRequest reScheduledDeliveryRequest = new ReScheduledDeliveryRequest(subscriberProfile.getSubscriberID(), deliveryRequest.getRescheduledDate(), deliveryRequest);
        subscriberState.getReScheduledDeliveryRequests().add(reScheduledDeliveryRequest);
        
        TimedEvaluation timedEvaluation = new TimedEvaluation(subscriberProfile.getSubscriberID(), deliveryRequest.getRescheduledDate());
        subscriberState.getScheduledEvaluations().add(timedEvaluation);
        subscriberProfileUpdated = true;
      }
    
    return subscriberProfileUpdated;
  }

  /*****************************************
  *
  *  handleReScheduledDeliveryRequest
  *
  *****************************************/

  private static boolean handleReScheduledDeliveryRequest(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent)
  {
    /*****************************************
    *
    *  On any event, check if ReScheduledDeliveryRequest's time has been reached, if yes trig the DeliveryRequest again
    *
    *****************************************/

    SubscriberState subscriberState = context.getSubscriberState();
    SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();
    boolean subscriberProfileUpdated = false;
    Date now = context.now();
    
    
    List<ReScheduledDeliveryRequest> toTrig = new ArrayList<>();
    for(ReScheduledDeliveryRequest reScheduledDeliveryRequest : subscriberState.getReScheduledDeliveryRequests()) 
      {
        if(reScheduledDeliveryRequest.getEvaluationDate().before(now)) {
          toTrig.add(reScheduledDeliveryRequest);          
        }
      }
    for(ReScheduledDeliveryRequest toHandle : toTrig) {
      subscriberState.getReScheduledDeliveryRequests().remove(toHandle);
      DeliveryRequest deliveryRequest = toHandle.getDeliveryRequest();
      deliveryRequest.setRescheduledDate(null);
      deliveryRequest.setDeliveryStatus(DeliveryStatus.Pending);
      deliveryRequest.resetDeliveryRequestAfterReSchedule();
      subscriberState.getDeliveryRequests().add(deliveryRequest);
      subscriberProfileUpdated = true;      
    }
    return subscriberProfileUpdated;
  }

  
  
  /*****************************************
  * 
  *  updateScheduledEvaluations
  *
  *****************************************/

  private static void updateScheduledEvaluations(Set<TimedEvaluation> previouslyScheduledEvaluations, Set<TimedEvaluation> scheduledEvaluations)
  {
    //
    //  deschedule no longer required events
    //

    for (TimedEvaluation scheduledEvaluation : previouslyScheduledEvaluations)
      {
        if (! scheduledEvaluations.contains(scheduledEvaluation))
          {
            timerService.deschedule(scheduledEvaluation);
          }
      }

    //
    //  schedule new events
    //

    for (TimedEvaluation scheduledEvaluation : scheduledEvaluations)
      {
        if (! previouslyScheduledEvaluations.contains(scheduledEvaluation))
          {
            timerService.schedule(scheduledEvaluation);
          }
      }
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
            // update balance 
            //
            
            boolean success = updatePointBalance(context, pointFulfillmentResponse, pointFulfillmentRequest.getEventID(), pointFulfillmentRequest.getModuleID(), pointFulfillmentRequest.getFeatureID(), subscriberProfile, newPoint, pointFulfillmentRequest.getOperation(), pointFulfillmentRequest.getAmount(), now, false);
            
            //
            //  response
            //

            if (success)
              {
                pointFulfillmentResponse.setDeliveryStatus(DeliveryStatus.Delivered);
                pointFulfillmentResponse.setDeliveryDate(now);
                
                //
                //  check loyalty program (may need to change tier if credited/debited point is the one used as status point in the program)
                //
                
                checkForLoyaltyProgramStateChanges(context.getSubscriberState(), pointFulfillmentRequest.getDeliveryRequestID(), now);
                
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
                      for (SegmentEligibility segment : segmentationDimensionEligibility.getSegments())
                        {
                          boolean addSegment = !inGroup && EvaluationCriterion.evaluateCriteria(evaluationRequest, segment.getProfileCriteria());
                          context.subscriberTrace("SegmentEligibility: segment {0} match {1}", segment.getName(), addSegment);
                          context.getSubscriberTraceDetails().addAll(evaluationRequest.getTraceDetails());
                          evaluationRequest.clearSubscriberTrace();
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
    *  process rule based target lists
    *
    *****************************************/

    for(Target target : targetService.getActiveTargets(now))
      {
        SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, extendedSubscriberProfile, subscriberGroupEpochReader, now);
        if (target.getTargetingType().equals(Target.TargetingType.Eligibility))
          {
            boolean addTarget = EvaluationCriterion.evaluateCriteria(evaluationRequest, target.getTargetingCriteria());
            subscriberProfile.setTarget(target.getTargetID(), subscriberGroupEpochReader.get(target.getTargetID()) != null ? subscriberGroupEpochReader.get(target.getTargetID()).getEpoch() : 0, addTarget);
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
     *  increment metric history for delivered message in notificatioStatus
     *
     *****************************************/
    if(evolutionEvent instanceof MessageDelivery)
    {
      MessageDelivery messageDelivery = (MessageDelivery)evolutionEvent;
      if(messageDelivery.getMessageDeliveryDeliveryStatus() == DeliveryStatus.Delivered)
        {
          try {
            MetricHistory channelMetricHistory = context.getSubscriberState().getNotificationHistory().stream().filter(p -> p.getFirstElement().equals(Deployment.getDeliveryTypeCommunicationChannelIDMap().get(((DeliveryRequest)messageDelivery).getDeliveryType()))).collect(Collectors.toList()).get(0).getSecondElement();
            Date messageDeliveryDate = RLMDateUtils.truncate(messageDelivery.getMessageDeliveryEventDate(), Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            //long messageCount = channelMetricHistory.getValue(messageDeliveryDate, messageDeliveryDate).longValue() + 1;
            //this update should be called increment for metric history.
            channelMetricHistory.update(messageDeliveryDate,1);
          }
          catch(Exception e) {
            log.warn("CATCHED Exception " + e.getClass().getName() + " while updating channelMetricHistory", e);
          }
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

  private static void checkForLoyaltyProgramStateChanges(SubscriberState subscriberState, String deliveryRequestID, Date now)
  {
    SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();
    for(Entry<String, LoyaltyProgramState> entry : subscriberProfile.getLoyaltyPrograms().entrySet())
      {
        //
        //  get current loyalty program state
        //

        LoyaltyProgramState loyaltyProgramState = entry.getValue();
        LoyaltyProgramPointsState loyaltyProgramPointState = (LoyaltyProgramPointsState) loyaltyProgramState;

        //
        //  check if loyalty program state needs to be updated
        //

        if(loyaltyProgramState.getLoyaltyProgramExitDate() == null && loyaltyProgramState.getLoyaltyProgramType().equals(LoyaltyProgramType.POINTS))
          {

            //
            //  get point loyalty program 
            //
            
            LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramState.getLoyaltyProgramID(), now);
            if(loyaltyProgram != null){ // may be suspended


              //
              //  determine current tier
              //

              String currentTier = loyaltyProgramPointState.getTierName();

              //
              //  determine new tier
              //

              LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints) loyaltyProgram;
              String newTierName = determineLoyaltyProgramPointsTier(subscriberProfile, loyaltyProgramPoints, now);

              //
              //  compare to current tier
              //

              if((currentTier != null && !currentTier.equals(newTierName)) || (currentTier == null && newTierName != null))
                {

                  //
                  //  update loyalty program state
                  //

                  loyaltyProgramPointState.update(loyaltyProgram.getEpoch(), LoyaltyProgramOperation.Optin, loyaltyProgram.getLoyaltyProgramName(), newTierName, now, deliveryRequestID);

                  //
                  //  generate new event (tier changed)
                  //

                  ParameterMap info = new ParameterMap();
                  info.put(LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation(), currentTier);
                  info.put(LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation(), newTierName);
                  ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                  subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);

                }
            }

          }
      }
  }
  
  /*****************************************
  *
  *  updatePointBalance
  *
  *****************************************/

  private static boolean updatePointBalance(EvolutionEventContext context, PointFulfillmentRequest pointFulfillmentResponse, String eventID, String moduleID, String featureID, SubscriberProfile subscriberProfile, Point point, CommodityDeliveryOperation operation, int amount, Date now, boolean generateBDR)
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

    boolean success = pointBalance.update(context, pointFulfillmentResponse, eventID, moduleID, featureID, subscriberProfile.getSubscriberID(), operation, amount, point, now, generateBDR);

    //
    //  update balances
    //

    subscriberProfile.getPointBalances().put(point.getPointID(), pointBalance);

    //
    //  update loyalty program balances
    //

    for (LoyaltyProgramState loyaltyProgramState : subscriberProfile.getLoyaltyPrograms().values())
      {
        LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramState.getLoyaltyProgramID(), now);
        if (loyaltyProgram != null && loyaltyProgram instanceof LoyaltyProgramPoints)
          {
            LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints) loyaltyProgram;
            LoyaltyProgramPointsState loyaltyProgramPointsState = (LoyaltyProgramPointsState) loyaltyProgramState;
            if (Objects.equals(point.getPointID(), loyaltyProgramPoints.getStatusPointsID())) loyaltyProgramPointsState.setStatusPoints(pointBalance.getBalance(now));
            if (Objects.equals(point.getPointID(), loyaltyProgramPoints.getRewardPointsID())) loyaltyProgramPointsState.setRewardPoints(pointBalance.getBalance(now));
          }
      }
    
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
                    currentLoyaltyProgramState = new LoyaltyProgramPointsState(LoyaltyProgramType.POINTS, loyaltyProgram.getEpoch(), loyaltyProgram.getLoyaltyProgramName(), loyaltyProgram.getLoyaltyProgramID(), now, null, newTierName, null, now, loyaltyProgramHistory);

                    //
                    //  update loyalty program state
                    //

                    if(log.isDebugEnabled()) log.debug("new loyaltyProgramRequest : for subscriber '"+subscriberProfile.getSubscriberID()+"' : loyaltyProgramState.update("+loyaltyProgram.getEpoch()+", "+loyaltyProgramRequest.getOperation()+", "+loyaltyProgram.getLoyaltyProgramName()+", "+newTierName+", "+now+", "+loyaltyProgramRequest.getDeliveryRequestID()+")");
                    ((LoyaltyProgramPointsState)currentLoyaltyProgramState).update(loyaltyProgram.getEpoch(), loyaltyProgramRequest.getOperation(), loyaltyProgram.getLoyaltyProgramName(), newTierName, now, loyaltyProgramRequest.getDeliveryRequestID());

                    //
                    //  generate new event (opt-in)
                    //
                    
                    ParameterMap infos = new ParameterMap();
                    infos.put(LoyaltyProgramPointsEventInfos.ENTERING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                    infos.put(LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation(), null);
                    infos.put(LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation(), newTierName);
                    ProfileLoyaltyProgramChangeEvent profileChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), infos);
                    subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileChangeEvent);
                    
                  }
                else
                  {
                    //
                    //  get current tier
                    //
                    
                    LoyaltyProgramPointsState loyaltyProgramPointsState = ((LoyaltyProgramPointsState) currentLoyaltyProgramState);
                    String currentTier = loyaltyProgramPointsState.getTierName();
                    
                    //
                    //  
                    //
                    
                    if((currentTier != null && !currentTier.equals(newTierName)) || (currentTier == null && newTierName != null))
                      {

                        //
                        //  update loyalty program state
                        //

                        loyaltyProgramPointsState.update(loyaltyProgram.getEpoch(), LoyaltyProgramOperation.Optin, loyaltyProgram.getLoyaltyProgramName(), newTierName, now, loyaltyProgramRequest.getDeliveryRequestID());

                        //
                        //  generate new event (tier changed)
                        //
                        
                        ParameterMap info = new ParameterMap();
                        info.put(LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation(), currentTier);
                        info.put(LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation(), newTierName);
                        ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                        subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);
                        
                      }
                  }

                //
                //  update subscriber loyalty programs state
                //

                subscriberProfile.getLoyaltyPrograms().put(loyaltyProgramRequest.getLoyaltyProgramID(), currentLoyaltyProgramState);
                
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
                    loyaltyProgramState = new LoyaltyProgramPointsState(LoyaltyProgramType.POINTS, loyaltyProgram.getEpoch(), loyaltyProgram.getLoyaltyProgramName(), loyaltyProgram.getLoyaltyProgramID(), now, null, tierName, null, now, loyaltyProgramHistory);
                  }

                String oldTier = ((LoyaltyProgramPointsState)loyaltyProgramState).getTierName();
                
                //
                //  update loyalty program state
                //

                if(log.isDebugEnabled()) log.debug("new loyaltyProgramRequest : for subscriber '"+subscriberProfile.getSubscriberID()+"' : loyaltyProgramState.update("+loyaltyProgram.getEpoch()+", "+loyaltyProgramRequest.getOperation()+", "+loyaltyProgram.getLoyaltyProgramName()+", "+tierName+", "+now+", "+loyaltyProgramRequest.getDeliveryRequestID()+")");
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
                info.put(LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation(), null);
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

//            case BADGES:
//              // TODO
//              break;

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

                if(subscriberCurrentTierDefinition != null){
                  
                  //
                  //  update loyalty program status 
                  //

                  EvolutionEngineEventDeclaration statusEventDeclaration = Deployment.getEvolutionEngineEvents().get(subscriberCurrentTierDefinition.getStatusEventName()) ;
                  if (statusEventDeclaration != null && statusEventDeclaration.getEventClassName().equals(evolutionEvent.getClass().getName()) && evolutionEvent instanceof LoyaltyProgramPointsEvent && ((LoyaltyProgramPointsEvent)evolutionEvent).getUnit()!=0)
                    {

                      //
                      //  update status points
                      //

                      Point point = pointService.getActivePoint(loyaltyProgramPoints.getStatusPointsID(), now);
                      if(point != null)
                        {

                          if(log.isDebugEnabled()) log.debug("update loyalty program STATUS => adding "+((LoyaltyProgramPointsEvent)evolutionEvent).getUnit()+" x "+subscriberCurrentTierDefinition.getNumberOfStatusPointsPerUnit()+" of point "+point.getPointName());
                          int amount = ((LoyaltyProgramPointsEvent)evolutionEvent).getUnit() * subscriberCurrentTierDefinition.getNumberOfStatusPointsPerUnit();
                          updatePointBalance(context, null, statusEventDeclaration.getEventClassName(), Module.Loyalty_Program.getExternalRepresentation(), loyaltyProgram.getLoyaltyProgramID(), subscriberProfile, point, CommodityDeliveryOperation.Credit, amount, now, true);
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
                  if(rewardEventDeclaration != null && rewardEventDeclaration.getEventClassName().equals(evolutionEvent.getClass().getName()) && evolutionEvent instanceof LoyaltyProgramPointsEvent && ((LoyaltyProgramPointsEvent)evolutionEvent).getUnit()!=0)
                    {

                      //  update reward points
                      
                      Point point = pointService.getActivePoint(loyaltyProgramPoints.getRewardPointsID(), now);
                      if(point != null)
                        {

                          if(log.isDebugEnabled()) log.debug("update loyalty program REWARD => adding "+((LoyaltyProgramPointsEvent)evolutionEvent).getUnit()+" x "+subscriberCurrentTierDefinition.getNumberOfRewardPointsPerUnit()+" of point with ID "+loyaltyProgramPoints.getRewardPointsID());
                          int amount = ((LoyaltyProgramPointsEvent)evolutionEvent).getUnit() * subscriberCurrentTierDefinition.getNumberOfRewardPointsPerUnit();
                          updatePointBalance(context, null, rewardEventDeclaration.getEventClassName(), Module.Loyalty_Program.getExternalRepresentation(), loyaltyProgram.getLoyaltyProgramID(), subscriberProfile, point, CommodityDeliveryOperation.Credit, amount, now, true);
                          subscriberProfileUpdated = true;
                          
                        }
                      else
                        {
                          log.info("update loyalty program STATUS : point with ID '"+loyaltyProgramPoints.getRewardPointsID()+"' not found");
                        }

                    }
                }

                break;

//              case BADGES:
//                // TODO
//                break;
                
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

  private static String determineLoyaltyProgramPointsTier(SubscriberProfile subscriberProfile, LoyaltyProgramPoints loyaltyProgramPoints, Date now)
  {
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
        String moduleID = null;
        String userID = null;
        String featureIDStr = null;
        int featureID = 0;
        List<Token> subscriberTokens = subscriberProfile.getTokens();
        String tokenTypeID = null;
        DNBOToken presentationLogToken = null;

        //
        // Retrieve the token-code we are looking for, from the event log.
        //

        if (evolutionEvent instanceof PresentationLog)
          {
            eventTokenCode = ((PresentationLog) evolutionEvent).getPresentationToken();
            moduleID = ((PresentationLog)evolutionEvent).getModuleID();
            featureIDStr = ((PresentationLog)evolutionEvent).getFeatureID();
            tokenTypeID = ((PresentationLog)evolutionEvent).getTokenTypeID();
            presentationLogToken = ((PresentationLog) evolutionEvent).getToken();
          }
        else if (evolutionEvent instanceof AcceptanceLog)
          {
            eventTokenCode = ((AcceptanceLog) evolutionEvent).getPresentationToken();
            moduleID = ((AcceptanceLog)evolutionEvent).getModuleID();
            featureIDStr = ((AcceptanceLog)evolutionEvent).getFeatureID();
            tokenTypeID = ((AcceptanceLog)evolutionEvent).getTokenTypeID();
          }

        DNBOToken subscriberStoredToken = null;
        if (tokenTypeID == null)
          {
            tokenTypeID = "external"; // predefined tokenTypeID for tokens created externally
          }
        TokenType defaultDNBOTokenType = tokenTypeService.getActiveTokenType(tokenTypeID, SystemTime.getCurrentTime());
        if (defaultDNBOTokenType == null)
          {
            log.error("Could not find token type with ID " + tokenTypeID + " Check your configuration.");
            return false;
          }

        try
        {
          featureID = Integer.parseInt(featureIDStr);
        }
        catch (NumberFormatException e)
        {
          log.warn("featureID is not an integer : " + featureIDStr + " using " + featureID);
        }
        if (moduleID == null)
          {
            moduleID = DeliveryRequest.Module.Unknown.getExternalRepresentation();
          }

        //
        // Subscriber token list cleaning.
        // We will delete all already expired tokens before doing anything.
        //

        List<Token> cleanedList = new ArrayList<Token>();
        boolean changed = false;
        Date now = SystemTime.getCurrentTime();
        for (Token token : subscriberTokens)
          {
            if (token.getTokenExpirationDate().before(now))
              {
                changed = true;
                break;
              }
            cleanedList.add(token);
          }

        if (changed)
          {
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

        for (Token token : subscriberTokens)
          {
            if(Objects.equals(eventTokenCode, token.getTokenCode()))
              {
                if(token instanceof DNBOToken)
                  {
                    subscriberStoredToken = (DNBOToken) token;
                  }
                else
                  {
                    log.error("Unexpected type (" + token.getClass().getName() + ") for an already existing token " + token);
                    return subscriberStateUpdated;
                  }
                break;
              }
          }

        if (subscriberStoredToken == null)
          {
            if (presentationLogToken == null)
              {
                // We start by creating a new token if it does not exist in Evolution (if it has been created by an outside system)
                subscriberStoredToken = new DNBOToken(eventTokenCode, subscriberProfile.getSubscriberID(), defaultDNBOTokenType);
              }
            else
              {
                subscriberStoredToken = presentationLogToken;
              }
            subscriberTokens.add(subscriberStoredToken);
            subscriberStoredToken.setFeatureID(featureID);
            subscriberStoredToken.setModuleID(moduleID);
            subscriberState.getTokenChanges().add(new TokenChange(subscriberState.getSubscriberID(), SystemTime.getCurrentTime(), "", eventTokenCode, "Create", "OK", evolutionEvent.getClass().getSimpleName(), moduleID, featureID));
            subscriberStateUpdated = true;
          }

        //
        // Update the token with the incoming event
        //

        if (evolutionEvent instanceof PresentationLog)
          {
            //
            // Presentation event update
            // If token created outside Evolution, we do not know its actual creation date, so assume created when it was bound with offers
            //

            PresentationLog presentationLog = (PresentationLog) evolutionEvent;

            if(subscriberStoredToken.getTokenStatus() == TokenStatus.New)
              {
                subscriberStoredToken.setTokenStatus(TokenStatus.Bound);
                subscriberStateUpdated = true;
              }
            Date eventDate = presentationLog.getEventDate();
            subscriberState.getTokenChanges().add(new TokenChange(subscriberState.getSubscriberID(), eventDate, "", eventTokenCode, "Allocate", "OK", "PresentationLog", moduleID, featureID));
            if (subscriberStoredToken.getCreationDate() == null)
              {
                subscriberStoredToken.setCreationDate(eventDate);
                subscriberStoredToken.setTokenExpirationDate(defaultDNBOTokenType.getExpirationDate(eventDate));
                subscriberStateUpdated = true;
              }
            List<Date> presentationDates = presentationLog.getPresentationDates();
            if (presentationDates != null && !presentationDates.isEmpty()) // if empty : bound has failed
              {
                subscriberStoredToken.setPresentationDates(presentationDates);
                subscriberStateUpdated = true;
                if (subscriberStoredToken.getBoundDate() == null || subscriberStoredToken.getBoundDate().before(eventDate))
                  {
                    subscriberStoredToken.setBoundDate(eventDate);
                  }
                int boundCount = subscriberStoredToken.getBoundCount();
                Integer maxNumberofPlaysInt = defaultDNBOTokenType.getMaxNumberOfPlays();
                int maxNumberofPlays = (maxNumberofPlaysInt == null) ? Integer.MAX_VALUE : maxNumberofPlaysInt.intValue();
                if (boundCount < maxNumberofPlays)
                  {
                    subscriberStoredToken.setBoundCount(boundCount+1); // no concurrency issue as a given subscriber is always handled by the same partition/evolution engine instance, sequentially
                    subscriberStoredToken.setPresentedOfferIDs(presentationLog.getOfferIDs()); // replace whatever was there
                    String salesChannelID = presentationLog.getSalesChannelID();
                    subscriberStoredToken.setPresentedOffersSalesChannel(salesChannelID); // replace whatever was there
                  }
              }
          }
        else if (evolutionEvent instanceof AcceptanceLog)
          {

            //
            // Acceptance event update
            //

            AcceptanceLog acceptanceLog = (AcceptanceLog) evolutionEvent;

            if (subscriberStoredToken.getAcceptedOfferID() != null)
              {
                log.error("Unexpected acceptance record ("+ acceptanceLog.toString() +") for a token ("+ subscriberStoredToken.toString() +") already redeemed by a previous acceptance record");
                return subscriberStateUpdated;
              }
            else
              {
                subscriberStoredToken.setTokenStatus(TokenStatus.Redeemed);
                subscriberStoredToken.setRedeemedDate(acceptanceLog.getEventDate());
                subscriberStoredToken.setAcceptedOfferID(acceptanceLog.getOfferID());
                subscriberState.getTokenChanges().add(new TokenChange(subscriberState.getSubscriberID(), acceptanceLog.getEventDate(), "", eventTokenCode, "Redeem", "OK", "AcceptanceLog", moduleID, featureID));
              }
            subscriberStateUpdated = true;
          }

        //
        // update global propensity information (only if we already acknowledged both Presentation & Acceptance events)
        //

        if (subscriberStoredToken.getPresentedOfferIDs().size() > 0 && subscriberStoredToken.getAcceptedOfferID() != null)
          {

            // 
            // Validate propensity rule before using it (ignore any propensity outputs otherwise)
            //

            if (Deployment.getPropensityRule().validate(segmentationDimensionService))
              {
                for(String offerID: subscriberStoredToken.getPresentedOfferIDs())
                  {
                    propensityService.incrementPropensity(offerID,subscriberProfile,true,offerID.equals(subscriberStoredToken.getAcceptedOfferID()));
                  }
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

  /*****************************************
  *
  *  updateTokens
  *
  *****************************************/

  private static boolean updateTokens(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent)
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
    *  return
    *
    *****************************************/

    return subscriberStateUpdated;
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
                if (log.isTraceEnabled()) log.trace("init permittedSimultaneousJourneys put " + journeyObjective.getJourneyObjectiveName() + ":" + journeyObjective.getEffectiveTargetingLimitMaxSimultaneous());
                permittedSimultaneousJourneys.put(journeyObjective, journeyObjective.getEffectiveTargetingLimitMaxSimultaneous());
                if (log.isTraceEnabled()) log.trace("init permittedWaitingPeriod put " + journeyObjective.getJourneyObjectiveName() + ": static TRUE");
                permittedWaitingPeriod.put(journeyObjective, Boolean.TRUE);
                if (log.isTraceEnabled()) log.trace("init permittedSlidingWindowJourneys put " + journeyObjective.getJourneyObjectiveName() + ":" + journeyObjective.getEffectiveTargetingLimitMaxOccurrence());
                permittedSlidingWindowJourneys.put(journeyObjective, journeyObjective.getEffectiveTargetingLimitMaxOccurrence());
              }

            //
            //  update
            //

            if (activeJourney)
              {
                if (log.isTraceEnabled()) log.trace("permittedSimultaneousJourneys put " + journeyObjective.getJourneyObjectiveName() + ":" + (permittedSimultaneousJourneys.get(journeyObjective) - 1));
                permittedSimultaneousJourneys.put(journeyObjective, permittedSimultaneousJourneys.get(journeyObjective) - 1);
              }
            if (journeyObjective.getEffectiveTargetingLimitMaxSimultaneous()==1 && (activeJourney || journeyState.getJourneyExitDate().compareTo(journeyObjective.getEffectiveWaitingPeriodEndDate(now)) >= 0))
              {
                if (log.isTraceEnabled()) log.trace("permittedWaitingPeriod put " + journeyObjective.getJourneyObjectiveName() + ": static FALSE");
                permittedWaitingPeriod.put(journeyObjective, Boolean.FALSE);
              }
            if (activeJourney || journeyState.getJourneyExitDate().compareTo(journeyObjective.getEffectiveSlidingWindowStartDate(now)) >= 0)
              {
                if (log.isTraceEnabled()) log.trace("permittedSlidingWindowJourneys put " + journeyObjective.getJourneyObjectiveName() + ":" + (permittedSlidingWindowJourneys.get(journeyObjective) - 1));
                permittedSlidingWindowJourneys.put(journeyObjective, permittedSlidingWindowJourneys.get(journeyObjective) - 1);
              }
          }
      }

    //
    //  permitted journeys
    //

    Map<JourneyObjective, Integer> permittedJourneys = new HashMap<JourneyObjective, Integer>();
    for (JourneyObjective journeyObjective : permittedSimultaneousJourneys.keySet())
      {
        permittedJourneys.put(journeyObjective, Math.max(Math.min(permittedSimultaneousJourneys.get(journeyObjective), permittedSlidingWindowJourneys.get(journeyObjective)), 0));
        if (permittedWaitingPeriod.get(journeyObjective) == Boolean.FALSE){
            if (log.isTraceEnabled()) log.trace("permittedJourneys put " + journeyObjective.getJourneyObjectiveName() + ": static 0");
            permittedJourneys.put(journeyObjective, 0);
        }
      }

    // log final numbers computation of journeys allowed per journey objectives
    if (log.isDebugEnabled())
      {
        for(Entry<JourneyObjective,Integer> entry:permittedJourneys.entrySet())
          {
            log.debug("permittedJourneys : " + entry.getKey().getJourneyObjectiveName() + ":" + entry.getValue());
          }
      }

    /*****************************************
    *
    *  activeJourneys (shuffled)
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
    
    /*****************************************
    *
    *  update JourneyState(s) to enter new journeys
    *
    *****************************************/

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
        calledJourney = calledJourney && ((JourneyRequest) evolutionEvent).isPending();
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
            boolean journeyMaxNumberOfCustomersReserved = false;

            /*****************************************
            *
            *  journey entry conditions (for non-workflows)
            *
            *****************************************/

            if (! journey.isWorkflow())
              {
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
                          if (!journey.getAppendExclusionLists() && exclusionList)
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
                    List<EvaluationCriterion> eligibilityAndTargetting = new ArrayList<>();
                    eligibilityAndTargetting.addAll(journey.getEligibilityCriteria());
                    eligibilityAndTargetting.addAll(journey.getTargetingCriteria());
                    boolean subscriberToBeProvisionned = EvaluationCriterion.evaluateCriteria(evaluationRequest, eligibilityAndTargetting);
                    context.getSubscriberTraceDetails().addAll(evaluationRequest.getTraceDetails());
                
                    List<List<EvaluationCriterion>> targetsCriteria = journey.getAllTargetsCriteria(targetService, now);
                    boolean inAnyTarget = targetsCriteria.size() == 0 ? true : false; // if no target is defined into the journey, then this boolean is true otherwise, false by default 
                    List<EvaluationCriterion> targets = new ArrayList<>();

                    for(List<EvaluationCriterion> current : targetsCriteria)
                      {
                        if(inAnyTarget == false) { // avoid evaluating target is already true
                          evaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, now);
                          context.getSubscriberTraceDetails().addAll(evaluationRequest.getTraceDetails());
                          boolean inThisTarget = EvaluationCriterion.evaluateCriteria(evaluationRequest, current);
                          if(inThisTarget)
                            {
                              inAnyTarget = true;
                            }
                        }                    
                      }

                    boolean targeting = subscriberToBeProvisionned && inAnyTarget;
                    switch (journey.getTargetingType())
                      {
                        case Target:
                          if (!(journey.getAppendInclusionLists() && inclusionList) && ! targeting)
                            {
                              enterJourney = false;
                              context.subscriberTrace("NotEligible: targeting criteria / inclusion list {0}", journey.getJourneyID());
                            }
                          break;

                        case Event:
                        case Manual:
                          if (! targeting)
                            {
                              enterJourney = false;
                              context.subscriberTrace("NotEligible: targeting criteria {0}", journey.getJourneyID());
                            }
                          break;
                      }
                  }

                  /*********************************************
                  *
                  *  pass max number of customers in journey
                  *
                  **********************************************/

                  if (enterJourney)
                    {
                      if (!stockService.reserve(journey,1))
                        {
                          enterJourney = false;
                          context.subscriberTrace("NotEligible: max number of customers {0}", journey.getJourneyID());
                        }
                      else
                        {
                          journeyMaxNumberOfCustomersReserved=true;
                        }

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
                *  boundParameters
                *
                *****************************************/

                JourneyRequest journeyRequest;
                ParameterMap boundParameters;
                if (calledJourney)
                  {
                    journeyRequest = (JourneyRequest) evolutionEvent;
                    boundParameters = new ParameterMap(journey.getBoundParameters());
                    boundParameters.putAll(journeyRequest.getBoundParameters());
                  }
                else
                  {
                    journeyRequest = null;
                    boundParameters = journey.getBoundParameters();
                  }

                /*****************************************
                *
                *  enterJourney -- all journeys
                *
                *****************************************/

                //
                // confirm "stock reservation" (journey max number of customers limits)
                //

                if(journeyMaxNumberOfCustomersReserved)
                  {
                    stockService.confirmReservation(journey,1);
                  }

                JourneyHistory journeyHistory = new JourneyHistory(journey.getJourneyID());
                JourneyState journeyState = new JourneyState(context, journey, journeyRequest, boundParameters, SystemTime.getCurrentTime(), journeyHistory);
                journeyState.getJourneyHistory().addNodeInformation(null, journeyState.getJourneyNodeID(), null, null);
                boolean statusUpdated = journeyState.getJourneyHistory().addStatusInformation(SystemTime.getCurrentTime(),journeyState, false);
                subscriberState.getJourneyStates().add(journeyState);
                subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, ucgStateReader, statusUpdated, new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, subscriberState.getSubscriberProfile().getSegmentsMap(subscriberGroupEpochReader))));
                subscriberState.getSubscriberProfile().getSubscriberJourneys().put(journey.getJourneyID(), Journey.getSubscriberJourneyStatus(journeyState));
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
                    journeyRequest.setEligible(true);
                  }
              }
            else
              {

                //
                // rollback the "stock reservation" (journey max number of customers limits)
                //

                if(journeyMaxNumberOfCustomersReserved)
                  {
                    stockService.voidReservation(journey,1);
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
                    if (! journeyRequest.getEligible() || ! journeyRequest.getWaitForCompletion())
                      {
                        JourneyRequest journeyResponse = journeyRequest.copy();
                        if(enterJourney)
                          {
                            journeyResponse.setJourneyStatus(SubscriberJourneyStatus.Entered);
                            journeyResponse.setDeliveryStatus(DeliveryStatus.Delivered);
                            journeyResponse.setDeliveryDate(now);
                          }
                        else
                          {
                            journeyResponse.setJourneyStatus(SubscriberJourneyStatus.NotEligible);
                            journeyResponse.setDeliveryStatus(DeliveryStatus.Failed);
                          }
                        context.getSubscriberState().getJourneyResponses().add(journeyResponse);
                        subscriberStateUpdated = true;
                      }
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
            subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, ucgStateReader, statusUpdated, new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, subscriberState.getSubscriberProfile().getSegmentsMap(subscriberGroupEpochReader), now)));
            inactiveJourneyStates.add(journeyState);
            continue;
          }

        /*****************************************
        *
        *  process journey response
        *
        *****************************************/

        //
        //  journey response?
        //

        boolean isJourneyResponse = true;
        isJourneyResponse = isJourneyResponse && evolutionEvent instanceof JourneyRequest;
        isJourneyResponse = isJourneyResponse && ! ((JourneyRequest) evolutionEvent).isPending();

        //
        //  awaited journey response
        //

        JourneyRequest journeyResponse = isJourneyResponse ? (JourneyRequest) evolutionEvent : null;
        boolean awaitedJourneyResponse = false;
        if (isJourneyResponse)
          {
            String journeyInstanceID = (journeyState != null) ? journeyState.getJourneyInstanceID() : null;
            awaitedJourneyResponse = journeyResponse.getCallingJourneyInstanceID() != null && journeyInstanceID != null && journeyResponse.getCallingJourneyInstanceID().equals(journeyInstanceID);
          }

        //
        //  process response
        //

        if (awaitedJourneyResponse)
          {
            JourneyNode callingJourneyNode = journey.getJourneyNodes().get(journeyResponse.getCallingJourneyNodeID());
            if (callingJourneyNode != null)
              {
                for (ContextVariable contextVariable : callingJourneyNode.getContextVariables())
                  {
                    switch (contextVariable.getVariableType())
                      {
                        case JourneyResult:
                          try
                            {
                              SubscriberEvaluationRequest contextVariableEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, callingJourneyNode, null, evolutionEvent, now);
                              Object contextVariableValue = contextVariable.getExpression().evaluateExpression(contextVariableEvaluationRequest, contextVariable.getBaseTimeUnit());
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
                              subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, ucgStateReader, statusUpdated, new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, subscriberState.getSubscriberProfile().getSegmentsMap(subscriberGroupEpochReader), SystemTime.getCurrentTime())));
                              inactiveJourneyStates.add(journeyState);
                              break;
                            }
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
          }

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
                RewardHistory lastRewards = journeyState.getJourneyHistory().addRewardInformation(deliveryResponse, deliverableService, now);
                if (lastRewards != null)
                  {
                    subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, ucgStateReader, new RewardHistory(lastRewards), journeyState.getJourneyID()));
                  }
              }
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
                        switch (contextVariable.getVariableType())
                          {
                            case Local:
                              try
                                {
                                  SubscriberEvaluationRequest contextVariableEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, firedLink, evolutionEvent, now);
                                  Object contextVariableValue = contextVariable.getExpression().evaluateExpression(contextVariableEvaluationRequest, contextVariable.getBaseTimeUnit());
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
                                  subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, ucgStateReader, statusUpdated, new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, subscriberState.getSubscriberProfile().getSegmentsMap(subscriberGroupEpochReader), SystemTime.getCurrentTime())));
                                  inactiveJourneyStates.add(journeyState);
                                  break;
                                }
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
                        //
                        //  evaluate action
                        //

                        SubscriberEvaluationRequest exitActionEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, firedLink, evolutionEvent, now);
                        List<Action> actions = journeyNode.getNodeType().getActionManager().executeOnExit(context, exitActionEvaluationRequest, firedLink);
                        context.getSubscriberTraceDetails().addAll(exitActionEvaluationRequest.getTraceDetails());

                        //
                        //  execute action
                        //

                        for (Action action : actions)
                          {
                            switch (action.getActionType())
                              {
                                case JourneyContextUpdate:
                                  ContextUpdate journeyContextUpdate = (ContextUpdate) action;
                                  journeyState.getJourneyParameters().putAll(journeyContextUpdate.getParameters());
                                  break;

                                default:
                                  log.error("unsupported action {} on actionManager.executeOnExit", action.getActionType());
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
                *  mark visited
                *
                *****************************************/

                visited.add(journeyNode);

                /*****************************************
                *
                *  enter node
                *
                *****************************************/

                JourneyNode nextJourneyNode = firedLink.getDestination();
                journeyState.setJourneyNodeID(nextJourneyNode.getNodeID(), now);
                journeyState.getJourneyHistory().addNodeInformation(firedLink.getSourceReference(), firedLink.getDestinationReference(), journeyState.getJourneyOutstandingDeliveryRequestID(), firedLink.getLinkID()); 
                journeyState.getJourneyActionManagerContext().clear();
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
                        List<Action> actions = journeyNode.getNodeType().getActionManager().executeOnEntry(context, entryActionEvaluationRequest);
                        context.getSubscriberTraceDetails().addAll(entryActionEvaluationRequest.getTraceDetails());

                        //
                        //  execute action
                        //

                        for (Action action : actions)
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
                                  break;

                                case JourneyContextUpdate:
                                  ContextUpdate journeyContextUpdate = (ContextUpdate) action;
                                  journeyState.getJourneyParameters().putAll(journeyContextUpdate.getParameters());
                                  break;

                                case ActionManagerContextUpdate:
                                  ContextUpdate actionManagerContext = (ContextUpdate) action;
                                  journeyState.getJourneyActionManagerContext().putAll(actionManagerContext.getParameters());
                                  break;

                                case TokenUpdate:
                                  Token token = (Token) action;
                                  subscriberState.getSubscriberProfile().getTokens().add(token);
                                  int featureID = 0;
                                  try
                                  {
                                    featureID = Integer.parseInt(journey.getJourneyID());
                                  }
                                  catch (NumberFormatException e)
                                  {
                                    log.warn("journeyID is not an integer : "+journey.getJourneyID()+" using "+featureID);
                                  }
                                  token.setFeatureID(featureID);
                                  switch (token.getTokenStatus())
                                  {
                                    case New:
                                      subscriberState.getTokenChanges().add(generateTokenChange(subscriberState.getSubscriberID(), token.getCreationDate(), TokenChange.CREATE, token, featureID, "JourneyToken"));
                                      break;
                                    case Bound: // must record the token creation
                                      subscriberState.getTokenChanges().add(generateTokenChange(subscriberState.getSubscriberID(), token.getCreationDate(), TokenChange.CREATE, token, featureID, "JourneyNBO"));
                                      subscriberState.getTokenChanges().add(generateTokenChange(subscriberState.getSubscriberID(), token.getBoundDate(), TokenChange.ALLOCATE, token, featureID, "JourneyNBO"));
                                      break;
                                    case Redeemed: // must record the token creation & allocation
                                      subscriberState.getTokenChanges().add(generateTokenChange(subscriberState.getSubscriberID(), token.getCreationDate(), TokenChange.CREATE, token, featureID, "JourneyBestOffer"));
                                      subscriberState.getTokenChanges().add(generateTokenChange(subscriberState.getSubscriberID(), token.getBoundDate(), TokenChange.ALLOCATE, token, featureID, "JourneyBestOffer"));
                                      subscriberState.getTokenChanges().add(generateTokenChange(subscriberState.getSubscriberID(), token.getRedeemedDate(), TokenChange.REDEEM, token, featureID, "JourneyBestOffer"));
                                      break;
                                    case Expired :
                                      // TODO
                                      break;
                                    default :
                                      log.error("unsupported token status {} for {} on actionManager.executeOnExit", token.getTokenStatus(), token.getTokenCode());
                                      break;
                                  }
                                  break;

                                case TokenChange:
                                  TokenChange tokenChange = (TokenChange) action;
                                  subscriberState.getTokenChanges().add(tokenChange);
                                  break;
                                  
                                case TriggerEvent:
                                  JourneyTriggerEventAction triggerEventAction = (JourneyTriggerEventAction) action;
                                  EvolutionEngineEventDeclaration eventDeclaration =  triggerEventAction.getEventDeclaration();
                                  kafkaProducer.send(new ProducerRecord<byte[], byte[]>(eventDeclaration.getEventTopic(), StringKey.serde().serializer().serialize(eventDeclaration.getEventTopic(), new StringKey(subscriberState.getSubscriberProfile().getSubscriberID())), eventDeclaration.getEventSerde().serializer().serialize(eventDeclaration.getEventTopic(), triggerEventAction.getEventToTrigger())));
                                  break;
                                  
                                default:
                                  log.error("unsupported action {} on actionManager.executeOnExit", action.getActionType());
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
                *  set context variables when entering node
                *
                *****************************************/

                if (journeyNode.getEvaluateContextVariables())
                  {
                    for (ContextVariable contextVariable : journeyNode.getContextVariables())
                      {
                        switch (contextVariable.getVariableType())
                          {
                            case Local:
                              try
                                {
                                  SubscriberEvaluationRequest contextVariableEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, null, null, now);
                                  Object contextVariableValue = contextVariable.getExpression().evaluateExpression(contextVariableEvaluationRequest, contextVariable.getBaseTimeUnit());
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
                                  subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, ucgStateReader, statusUpdated, new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, subscriberState.getSubscriberProfile().getSegmentsMap(subscriberGroupEpochReader), SystemTime.getCurrentTime())));
                                  inactiveJourneyStates.add(journeyState);
                                  break;
                                }
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
                subscriberState.getJourneyStatisticWrappers().add(new JourneyStatisticWrapper(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, ucgStateReader, statusUpdated, new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, firedLink, markNotified, markConverted, sample, subscriberState.getSubscriberProfile().getSegmentsMap(subscriberGroupEpochReader))));

                /*****************************************
                *
                *  update subscriberJourneys in profile
                *
                *****************************************/
                
                subscriberState.getSubscriberProfile().getSubscriberJourneys().put(journey.getJourneyID(), Journey.getSubscriberJourneyStatus(journeyState));
              }
          }
        while (firedLink != null && journeyState.getJourneyExitDate() == null);
      }

    //
    //  journey end -- send response
    //

    for (JourneyState journeyState : inactiveJourneyStates)
      {
        if (journeyState.getCallingJourneyRequest() != null && journeyState.getCallingJourneyRequest().getWaitForCompletion())
          {
            //
            //  response
            //

            JourneyRequest journeyResponse = journeyState.getCallingJourneyRequest().copy();

            //
            //  status
            //
                
            journeyResponse.setJourneyStatus(Journey.getSubscriberJourneyStatus(journeyState));
            journeyResponse.setDeliveryStatus(DeliveryStatus.Delivered);
            journeyResponse.setDeliveryDate(SystemTime.getCurrentTime());

            //
            //  journeyResults
            //

            SimpleParameterMap journeyResults = new SimpleParameterMap();
            Journey journey = journeyService.getActiveJourney(journeyState.getJourneyID(), now);
            if(journey != null) 
              {
                           
                log.debug("Gotten Journey for " + journeyState.getJourneyID() + " " + journey);
                for (CriterionField contextVariable : journey.getContextVariables().values())
                  {
                    switch (contextVariable.getFieldDataType())
                      {
                        case IntegerCriterion:
                        case DoubleCriterion:
                        case StringCriterion:
                        case BooleanCriterion:
                        case DateCriterion:
                          journeyResults.put(Journey.generateJourneyResultID(journey, contextVariable), journeyState.getJourneyParameters().get(contextVariable.getID()));
                          break;
                        case TimeCriterion:
                          throw new RuntimeException("unsupported contextVariable field datatype " + contextVariable.getFieldDataType());
                      }
                  }
                journeyResponse.setJourneyResults(journeyResults);
  
                //
                //  send response
                //
                context.getSubscriberState().getJourneyResponses().add(journeyResponse);
                subscriberStateUpdated = true;
              }
            else 
              {
                log.info("Null Journey for " + journeyState.getJourneyID());
              }
          }
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

  private static TokenChange generateTokenChange(String subscriberId, Date eventDateTime, String action, Token token, int journeyID, String origin)
  {
    return new TokenChange(subscriberId, eventDateTime, "", token.getTokenCode(), action, "OK", origin, Module.Journey_Manager, journeyID);
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
    *  cleanup
    *
    *****************************************/

    switch (evolutionEvent.getSubscriberAction())
      {
        case Cleanup:
          return null;
          
        case Delete:
          cleanExtendedSubscriberProfile(currentExtendedSubscriberProfile, now);
          ExtendedSubscriberProfile.stateStoreSerde().setKafkaRepresentation(Deployment.getExtendedSubscriberProfileChangeLogTopic(), currentExtendedSubscriberProfile);
          return currentExtendedSubscriberProfile;
      }
    
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
        ExtendedSubscriberProfile.stateStoreSerde().setKafkaRepresentation(Deployment.getExtendedSubscriberProfileChangeLogTopic(), extendedSubscriberProfile);
        evolutionEngineStatistics.updateExtendedProfileSize(extendedSubscriberProfile.getKafkaRepresentation());
        if (extendedSubscriberProfile.getKafkaRepresentation().length > 1000000)
          {
            log.error("ExtendedSubscriberProfile size error, ignoring event {} for subscriber {}: {}", evolutionEvent.getClass().toString(), evolutionEvent.getSubscriberID(), extendedSubscriberProfile.getKafkaRepresentation().length);
            cleanExtendedSubscriberProfile(currentExtendedSubscriberProfile, now);
            ExtendedSubscriberProfile.stateStoreSerde().setKafkaRepresentation(Deployment.getExtendedSubscriberProfileChangeLogTopic(), currentExtendedSubscriberProfile);
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
    //  abort if null
    //

    if (extendedSubscriberProfile == null)
      {
        return false;
      }
    
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
    *  cleanup
    *
    *****************************************/

    switch (evolutionEvent.getSubscriberAction())
      {
        case Cleanup:
          return null;
          
        case Delete:
          cleanSubscriberHistory(currentSubscriberHistory, now);
          SubscriberHistory.stateStoreSerde().setKafkaRepresentation(Deployment.getSubscriberHistoryChangeLogTopic(), currentSubscriberHistory);
          return currentSubscriberHistory;
      }
    
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
        SubscriberHistory.stateStoreSerde().setKafkaRepresentation(Deployment.getSubscriberHistoryChangeLogTopic(), subscriberHistory);
        evolutionEngineStatistics.updateSubscriberHistorySize(subscriberHistory.getKafkaRepresentation());
        if (subscriberHistory.getKafkaRepresentation().length > 1000000)
          {
            log.error("HistoryStore size error, ignoring event {} for subscriber {}: {}", evolutionEvent.getClass().toString(), evolutionEvent.getSubscriberID(), subscriberHistory.getKafkaRepresentation().length);
            cleanSubscriberHistory(currentSubscriberHistory, now);
            SubscriberHistory.stateStoreSerde().setKafkaRepresentation(Deployment.getSubscriberHistoryChangeLogTopic(), currentSubscriberHistory);
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
    //  abort if null
    //

    if (subscriberHistory == null)
      {
        return false;
      }
    
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
    if (subscriberState != null)
      {
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
        result.addAll((subscriberState.getExternalAPIOutput() != null) ? Collections.<ExternalAPIOutput>singletonList(subscriberState.getExternalAPIOutput()) : Collections.<ExternalAPIOutput>emptyList());
        result.addAll(subscriberState.getProfileChangeEvents());
        result.addAll(subscriberState.getTokenChanges());
        result.addAll(subscriberState.getVoucherChanges());
        result.addAll(subscriberState.getProfileSegmentChangeEvents());
        result.addAll(subscriberState.getProfileLoyaltyProgramChangeEvents());
      }
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
    result.addAll((extendedSubscriberProfile != null && extendedSubscriberProfile.getSubscriberTrace() != null) ? Collections.<SubscriberTrace>singletonList(extendedSubscriberProfile.getSubscriberTrace()) : Collections.<SubscriberTrace>emptyList());
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
    private JourneyService journeyService;
    private SubscriberMessageTemplateService subscriberMessageTemplateService;
    private DeliverableService deliverableService;
    private SegmentationDimensionService segmentationDimensionService;
    private PresentationStrategyService presentationStrategyService;
    private ScoringStrategyService scoringStrategyService;
    private OfferService offerService;
    private SalesChannelService salesChannelService;
    
    private ProductService productService;
    private ProductTypeService productTypeService;
    private VoucherService voucherService;
    private VoucherTypeService voucherTypeService;
    private CatalogCharacteristicService catalogCharacteristicService;
    private DNBOMatrixService dnboMatrixService;
    
    private TokenTypeService tokenTypeService;
    private PaymentMeanService paymentMeanService;
    private SegmentContactPolicyService segmentContactPolicyService;
    private KStreamsUniqueKeyServer uniqueKeyServer;
    private Date now;
    private List<String> subscriberTraceDetails;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public EvolutionEventContext(SubscriberState subscriberState, ExtendedSubscriberProfile extendedSubscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, JourneyService journeyService, SubscriberMessageTemplateService subscriberMessageTemplateService, DeliverableService deliverableService, SegmentationDimensionService segmentationDimensionService, PresentationStrategyService presentationStrategyService, ScoringStrategyService scoringStrategyService, OfferService offerService, SalesChannelService salesChannelService, TokenTypeService tokenTypeService, SegmentContactPolicyService segmentContactPolicyService, ProductService productService, ProductTypeService productTypeService, VoucherService voucherService, VoucherTypeService voucherTypeService, CatalogCharacteristicService catalogCharacteristicService, DNBOMatrixService dnboMatrixService, PaymentMeanService paymentMeanService, KStreamsUniqueKeyServer uniqueKeyServer, Date now)
    {
      this.subscriberState = subscriberState;
      this.extendedSubscriberProfile = extendedSubscriberProfile;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
      this.journeyService = journeyService;
      this.subscriberMessageTemplateService = subscriberMessageTemplateService;
      this.deliverableService = deliverableService;
      this.segmentationDimensionService = segmentationDimensionService;
      this.presentationStrategyService = presentationStrategyService;
      this.scoringStrategyService = scoringStrategyService;
      this.offerService = offerService;
      this.salesChannelService = salesChannelService;
      this.tokenTypeService = tokenTypeService;    
      this.segmentContactPolicyService = segmentContactPolicyService;
      this.productService = productService;
      this.productTypeService = productTypeService;
      this.voucherService = voucherService;
      this.voucherTypeService = voucherTypeService;
      this.catalogCharacteristicService = catalogCharacteristicService;
      this.dnboMatrixService = dnboMatrixService;
      this.paymentMeanService = paymentMeanService;
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
    public JourneyService getJourneyService() { return journeyService; }
    public SubscriberMessageTemplateService getSubscriberMessageTemplateService() { return subscriberMessageTemplateService; }
    public DeliverableService getDeliverableService() { return deliverableService; }
    public SegmentationDimensionService getSegmentationDimensionService() { return segmentationDimensionService; }
    public PresentationStrategyService getPresentationStrategyService() { return presentationStrategyService; }
    public ScoringStrategyService getScoringStrategyService() { return scoringStrategyService; }
    public OfferService getOfferService() { return offerService; }
    public SalesChannelService getSalesChannelService() { return salesChannelService; }
    public ProductService getProductService() { return productService; }
    public ProductTypeService getProductTypeService() { return productTypeService; }
    public VoucherService getVoucherService() { return voucherService; }
    public VoucherTypeService getVoucherTypeService() { return voucherTypeService; }
    public CatalogCharacteristicService getCatalogCharacteristicService() { return catalogCharacteristicService; }
    public DNBOMatrixService getDnboMatrixService() { return dnboMatrixService; }
    public TokenTypeService getTokenTypeService() { return tokenTypeService; }
    public PaymentMeanService getPaymentMeanService() { return paymentMeanService; }
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
            //  log state store sizes
            //

            log.info("SubscriberStateSize: {}", evolutionEngineStatistics.getSubscriberStateSize().toString());
            log.info("SubscriberHistorySize: {}", evolutionEngineStatistics.getSubscriberHistorySize().toString());
            log.info("ExtendedProfileSize: {}", evolutionEngineStatistics.getExtendedProfileSize().toString());

            //
            //  log task/partition assignments
            //

            for (ThreadMetadata threadMetadata : streams.localThreadsMetadata())
              {
                log.info(threadMetadata.toString());
                for (TaskMetadata taskMetadata : threadMetadata.activeTasks()) log.info("active " + taskMetadata.toString());
                for (TaskMetadata taskMetadata : threadMetadata.standbyTasks()) log.info("standby " + taskMetadata.toString());
              }

            //
            //  memory usage
            //

            log.info("JVM free memory : {} over total of {}", FileUtils.byteCountToDisplaySize(Runtime.getRuntime().freeMemory()), FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory()));

            //
            //  nextprocessingtime
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

    @Override public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      deliveryRequestSource = extractWorkflowFeatureID(evolutionEventContext, subscriberEvaluationRequest, deliveryRequestSource);
      
      String journeyID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.journey");

      /*****************************************
      *
      *  request
      *
      *****************************************/

      JourneyRequest request = new JourneyRequest(evolutionEventContext, subscriberEvaluationRequest, deliveryRequestSource, journeyID);

      /*****************************************
      *
      *  return request
      *
      *****************************************/

      return Collections.<Action>singletonList(request);
    }
  }
  
  /*****************************************
  *
  *  class EnterWorkflowAction
  *
  *****************************************/

  public static class EnterWorkflowAction extends ActionManager
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public EnterWorkflowAction(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
    }

    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      deliveryRequestSource = extractWorkflowFeatureID(evolutionEventContext, subscriberEvaluationRequest, deliveryRequestSource);
      
      WorkflowParameter workflowParameter = (WorkflowParameter) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.workflow");

      /*****************************************
      *
      *  evaluate bound parameters
      *
      *****************************************/

      boolean validParameters = true;
      SimpleParameterMap boundParameters = new SimpleParameterMap();
      for (String parameterName : workflowParameter.getWorkflowParameters().keySet())
        {
          if (workflowParameter.getWorkflowParameters().get(parameterName) instanceof ParameterExpression)
            {
              try
                {
                  Expression parameterExpression = ((ParameterExpression) workflowParameter.getWorkflowParameters().get(parameterName)).getExpression();
                  TimeUnit baseTimeUnit = ((ParameterExpression) workflowParameter.getWorkflowParameters().get(parameterName)).getBaseTimeUnit();
                  Object parameterValue = parameterExpression.evaluateExpression(subscriberEvaluationRequest, baseTimeUnit);
                  boundParameters.put(parameterName, parameterValue);
                }
              catch (ExpressionEvaluationException|ArithmeticException e)
                {
                  //
                  //  log
                  //

                  log.info("invalid workflow parameter {} = {}", parameterName, ((ParameterExpression) workflowParameter.getWorkflowParameters().get(parameterName)).getExpressionString());
                  StringWriter stackTraceWriter = new StringWriter();
                  e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                  log.debug(stackTraceWriter.toString());
                  evolutionEventContext.subscriberTrace("Workflow Parameter {0}: {1} / {2}", parameterName, ((ParameterExpression) workflowParameter.getWorkflowParameters().get(parameterName)).getExpressionString(), e.getMessage());

                  //
                  //  abort
                  //

                  validParameters = false;
                  break;
                }
            }
          else
            {
              boundParameters.put(parameterName, workflowParameter.getWorkflowParameters().get(parameterName));
            }
        }

      /*****************************************
      *
      *  request
      *
      *****************************************/

      JourneyRequest request = validParameters ? new JourneyRequest(evolutionEventContext, subscriberEvaluationRequest, deliveryRequestSource, workflowParameter.getWorkflowID(), boundParameters) : null;

      /*****************************************
      *
      *  return request
      *
      *****************************************/

      return (request != null) ? Collections.<Action>singletonList(request) : Collections.<Action>emptyList();
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

    public LoyaltyProgramAction(JSONObject configuration) throws GUIManagerException
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

    @Override public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      
      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getJourneyID();
      deliveryRequestSource = extractWorkflowFeatureID(evolutionEventContext, subscriberEvaluationRequest, deliveryRequestSource);
      
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

      return Collections.<Action>singletonList(request);
    }
  }
  
  /*****************************************
  *
  *  class TriggerEventAction
  *
  *****************************************/

  public static class TriggerEventAction extends ActionManager
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private EvolutionEngineEventDeclaration eventDeclaration;
    private HashMap<String, String> eventFieldMappings = new HashMap<>();
    private Constructor<? extends EvolutionEngineEvent> eventConstructor = null; 

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public TriggerEventAction(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
      String eventName = JSONUtilities.decodeString(configuration, "eventName", true);
      JSONArray fieldMappingsArray = JSONUtilities.decodeJSONArray(configuration, "fieldMappings");
      if(fieldMappingsArray != null) {
        for(int i = 0; i < fieldMappingsArray.size(); i++) {
          JSONObject currentMapping = (JSONObject) fieldMappingsArray.get(i);
          String eventFieldName = (String)currentMapping.get("eventFieldName");
          String eventFieldValue = (String)currentMapping.get("eventFieldValue");
          this.eventFieldMappings.put(eventFieldName, eventFieldValue);
          log.info("TriggerEventAction Event Name " + eventName + " field " + eventFieldName + " mapped to " + eventFieldValue);
        }
      }
      
      //
      // let retrieve the constructor (String subscriberID, Date eventDate, JSONObject jsonRoot) or throw an Exception
      //
      
      eventDeclaration = Deployment.getEvolutionEngineEvents().get(eventName);
      if(eventDeclaration== null) {
        throw new GUIManagerException("TriggerEventAction Can't retrieve EvolutionEngineEventDeclaration with name ", eventName);
      }
      
      try
      {
        Class eventClass = (Class<? extends EvolutionEngineEvent>) Class.forName(eventDeclaration.getEventClassName());
        eventConstructor = eventClass.getConstructor(new Class<?>[]{String.class, Date.class, JSONObject.class });
      }
    catch (Exception e)
      {
        log.error("TriggerEventAction Exception " + e.getClass().getName() + " while getting Constructor(String subscriberID, Date eventDate, JSONObject jsonRoot) of " + eventDeclaration.getEventClassName() + " for eventDeclaration eventName");
        throw new GUIManagerException(e);
      }
    }

    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      
      String subscriberID = subscriberEvaluationRequest.getSubscriberProfile().getSubscriberID();
      
      /*****************************************
      *
      *  List of values for each fields
      *
      *****************************************/
      JSONObject eventJSON = new JSONObject();
      for(String eventFieldName : eventFieldMappings.keySet()) {
        String eventFieldValueJourney = eventFieldMappings.get(eventFieldName);
        Object value = CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest, eventFieldValueJourney);
        if(value != null) {
          eventJSON.put(eventFieldName, value);
        }
      }
      
      try 
        {
          EvolutionEngineEvent event = eventConstructor.newInstance(subscriberID, SystemTime.getCurrentTime(), eventJSON);
          JourneyTriggerEventAction action = new JourneyTriggerEventAction();
          action.setEventDeclaration(eventDeclaration);
          action.setEventToTrigger(event);
          
          return Collections.<Action>singletonList(action);
          

        }
      catch(Exception e)
        {
          log.error("TriggerEventAction.executeOnEntry Can't instanciate event class " + eventConstructor.getDeclaringClass().getName(), e);          
        }     
      
      // result empty list as action already done...
      return new ArrayList<Action>();
    }
  }
  
  public static class JourneyTriggerEventAction implements Action {

    private EvolutionEngineEventDeclaration eventDeclaration;
    private EvolutionEngineEvent eventToTrigger;
    
    @Override
    public ActionType getActionType()
    {
      return ActionType.TriggerEvent;
    }
    
    public EvolutionEngineEventDeclaration getEventDeclaration()
    {
      return eventDeclaration;
    }

    public void setEventDeclaration(EvolutionEngineEventDeclaration eventDeclaration)
    {
      this.eventDeclaration = eventDeclaration;
    }

    public EvolutionEngineEvent getEventToTrigger()
    {
      return eventToTrigger;
    }

    public void setEventToTrigger(EvolutionEngineEvent eventToTrigger)
    {
      this.eventToTrigger = eventToTrigger;
    }
  }
}
