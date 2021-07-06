/****************************************************************************
*
*  EvolutionEngine.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.*;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.evolving.nglm.evolution.kafka.EvolutionProductionExceptionHandler;
import com.evolving.nglm.evolution.preprocessor.Preprocessor;
import com.evolving.nglm.evolution.propensity.PropensityService;
import com.evolving.nglm.evolution.retention.RetentionService;
import com.evolving.nglm.evolution.statistics.CounterStat;
import com.evolving.nglm.evolution.statistics.DurationStat;
import com.evolving.nglm.evolution.statistics.StatBuilder;
import com.evolving.nglm.evolution.statistics.StatsBuilders;
import io.confluent.kafka.formatter.AvroMessageFormatter;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.*;
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

import com.evolving.nglm.core.AutoProvisionSubscriberStreamEvent;
import com.evolving.nglm.core.CleanupSubscriber;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
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
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration.EventRule;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.Expression.ExpressionEvaluationException;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.MetricHistory.BucketRepresentation;
import com.evolving.nglm.evolution.Journey.ContextUpdate;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatusField;
import com.evolving.nglm.evolution.Journey.TargetingType;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramOperation;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.LoyaltyProgramChallenge.ChallengeLevel;
import com.evolving.nglm.evolution.LoyaltyProgramChallenge.LoyaltyProgramChallengeEventInfos;
import com.evolving.nglm.evolution.LoyaltyProgramChallenge.LoyaltyProgramLevelChange;
import com.evolving.nglm.evolution.LoyaltyProgramMission.LoyaltyProgramMissionEventInfos;
import com.evolving.nglm.evolution.LoyaltyProgramMission.LoyaltyProgramStepChange;
import com.evolving.nglm.evolution.LoyaltyProgramMission.MissionSchedule;
import com.evolving.nglm.evolution.LoyaltyProgramMission.MissionStep;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.LoyaltyProgramTierChange;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;
import com.evolving.nglm.evolution.SubscriberProfile.EvolutionSubscriberStatus;
import com.evolving.nglm.evolution.ThirdPartyManager.ThirdPartyManagerException;
import com.evolving.nglm.evolution.Token.TokenStatus;
import com.evolving.nglm.evolution.VoucherChange.VoucherChangeAction;
import com.evolving.nglm.evolution.UCGState.UCGGroup;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class EvolutionEngine
{

  public static final String DELIMITER = "-X-";
  static final String INTERNAL_VARIABLE_SUPPLIER = "XXEvolSupplier".toLowerCase();
  static final String INTERNAL_VARIABLE_RESELLER = "XXEvolReseller".toLowerCase();
  static final String INTERNAL_ID_SUPPLIER = "InternalIDSupplier";
  static final String INTERNAL_ID_RESELLER = "InternalIDReseller";
  
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
  // can not remove yet all of it, keep it for state store log stats, but not jmx exported anymore
  private static EvolutionEngineStatistics evolutionEngineStatistics;
  // evolution event count
  private static StatBuilder<CounterStat> statsEventCounter;
  // voucher change count
  private static StatBuilder<CounterStat> statsVoucherChangeCounter;
  // evolution REST API stats
  private static StatBuilder<DurationStat> statsApiDuration;
  // evolution state store operation api
  private static StatBuilder<DurationStat> statsStateStoresDuration;

  private static KStreamsUniqueKeyServer uniqueKeyServer = new KStreamsUniqueKeyServer();
  private static Method evolutionEngineExtensionUpdateSubscriberMethod;
  private static Method evolutionEngineExtensionUpdateExtendedSubscriberMethod;
  private static Method evolutionEngineExternalAPIMethod;
  private static TimerService timerService;
  private static PointService pointService;
  private static PaymentMeanService paymentMeanService;
  private static ResellerService resellerService;
  private static SupplierService supplierService;
  private static CustomCriteriaService customCriteriaService;

  private static KafkaStreams streams = null;
  private static ReadOnlyKeyValueStore<StringKey, SubscriberState> subscriberStateStore = null;
  private static ReadOnlyKeyValueStore<StringKey, ExtendedSubscriberProfile> extendedSubscriberProfileStore = null;
  private static final int RESTAPIVersion = 1;
  private static HttpServer subscriberProfileServer;
  private static HttpServer internalServer;
  private static HttpClient httpClient;
  private static ExclusionInclusionTargetService exclusionInclusionTargetService;
  private static StockMonitor stockService;
  private static PropensityService propensityService;
  private static RetentionService retentionService;

  private String evolutionEngineKey;
  public String getEvolutionEngineKey(){return evolutionEngineKey;}
  private static EvolutionEngineEventDeclaration voucherActionEventDeclaration = null;
  public static EvolutionEngineEventDeclaration fileWithVariableEventDeclaration = null;
  static
    {
      try
        {
          voucherActionEventDeclaration = new EvolutionEngineEventDeclaration("VoucherAction", "com.evolving.nglm.evolution.VoucherAction", Deployment.getVoucherActionTopic(), EventRule.Standard, null);
          Map<String, Object> fileWithVariableEventCriteriaMap = new LinkedHashMap<String, Object>(); fileWithVariableEventCriteriaMap.put("id", "event.fileID"); fileWithVariableEventCriteriaMap.put("display", "Event FileID"); fileWithVariableEventCriteriaMap.put("dataType", "string"); fileWithVariableEventCriteriaMap.put("retriever", "getFileWithVariableID");
          Map<String, CriterionField> fileWithVariableEventCriterias = new LinkedHashMap<String, CriterionField>(); fileWithVariableEventCriterias.put((String) fileWithVariableEventCriteriaMap.get("id"), new CriterionField(JSONUtilities.encodeObject(fileWithVariableEventCriteriaMap)));
          fileWithVariableEventDeclaration = new EvolutionEngineEventDeclaration("FileWithVariableEvent", "com.evolving.nglm.evolution.FileWithVariableEvent", Deployment.getFileWithVariableEventTopic(), EventRule.Standard, fileWithVariableEventCriterias);
        }
      catch (GUIManagerException e)
        {
          e.printStackTrace();
        }
    }
  

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
    evolutionEngineKey = args[2];
    String subscriberProfileHost = args[3];
    Integer subscriberProfilePort = Integer.parseInt(args[4]);
    Integer internalPort = Integer.parseInt(args[5]);
    Integer kafkaReplicationFactor = Integer.parseInt(args[6]);
    Integer kafkaStreamsStandbyReplicas = Integer.parseInt(args[7]);

    // for performance testing only, SHOULD NOT BE USED IN PROD, the right rocksDB configuration should be able to provide the same
    boolean isInMemoryStateStores = false;
    if("true".equals(System.getenv("EVOLUTIONENGINE_IN_MEMORY_STATE_STORES"))) isInMemoryStateStores = true;
    // try as well some rocksdb config (not sure yet at all about all this, documentation is not so clear, so testing)
    int rocksDBCacheMBytes=-1;// will not change the default kstream rocksdb settings
    if(!isInMemoryStateStores) try{ rocksDBCacheMBytes = Integer.parseInt(System.getenv("EVOLUTIONENGINE_ROCKSDB_CACHE_MB")); } catch(NumberFormatException e){}
    int rocksDBMemTableMBytes=-1;
    if(!isInMemoryStateStores && rocksDBCacheMBytes!=-1) try{ rocksDBMemTableMBytes = Integer.parseInt(System.getenv("EVOLUTIONENGINE_ROCKSDB_MEMTABLE_MB")); } catch(NumberFormatException e){}
    

    //
    //  source topics
    //

    String timedEvaluationTopic = Deployment.getTimedEvaluationTopic();
    String cleanupSubscriberTopic = Deployment.getCleanupSubscriberTopic();
    String subscriberProfileForceUpdateTopic = Deployment.getSubscriberProfileForceUpdateTopic();
    String executeActionOtherSubscriberTopic = Deployment.getExecuteActionOtherSubscriberTopic();
    String recordSubscriberIDTopic = Deployment.getRecordSubscriberIDTopic();
    String subscriberGroupTopic = Deployment.getSubscriberGroupTopic();
    String subscriberTraceControlTopic = Deployment.getSubscriberTraceControlTopic();
    String presentationLogTopic = Deployment.getPresentationLogTopic();
    String acceptanceLogTopic = Deployment.getAcceptanceLogTopic();
    String voucherChangeRequestTopic = Deployment.getVoucherChangeRequestTopic();
    String workflowEventTopic = Deployment.getWorkflowEventTopic();

    //
    //  changelogs
    //

    String subscriberStateChangeLog = Deployment.getSubscriberStateChangeLog();
    String extendedSubscriberProfileChangeLog = Deployment.getExtendedSubscriberProfileChangeLog();

    //
    //  (force load of SubscriberProfile class)
    //

    SubscriberProfile.getSubscriberProfileSerde();

    //
    //  log
    //

    log.info("main START: {} {} {} {} {}", stateDirectory, bootstrapServers, kafkaStreamsStandbyReplicas, kafkaReplicationFactor);

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
    //  presentationStrategyService
    //

    supplierService = new SupplierService(bootstrapServers, "evolutionengine-supplierservice-" + evolutionEngineKey, Deployment.getSupplierTopic(), false);
    supplierService.start();
    
    customCriteriaService = new CustomCriteriaService(bootstrapServers, "evolutionengine-customcriteriaservice-" + evolutionEngineKey, Deployment.getCustomCriteriaTopic(), false);
    customCriteriaService.start();
    

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
    //  resellerService
    //

    resellerService = new ResellerService(bootstrapServers, "evolutionengine-resellerservice-" + evolutionEngineKey, Deployment.getResellerTopic(), false);
    resellerService.start();

    //
    //  subscriberGroupEpochReader
    //

    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("evolutionengine-subscribergroupepoch", Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);

    //
    // propensity service
    //

    propensityService = new PropensityService(subscriberGroupEpochReader);

    //
    // retention service
    //
    //EVPRO-574
    retentionService = new RetentionService(journeyService,targetService);
    
    //
    //  ucgStateReader
    //

    ucgStateReader = ReferenceDataReader.<String,UCGState>startReader("evolutionengine-ucgstate", Deployment.getBrokerServers(), Deployment.getUCGStateTopic(), UCGState::unpack);

    //
    //  create monitoring object
    //

    evolutionEngineStatistics = new EvolutionEngineStatistics(applicationID);
    statsEventCounter = StatsBuilders.getEvolutionCounterStatisticsBuilder("evolutionengine_event","evolutionengine-"+evolutionEngineKey);
    statsVoucherChangeCounter = StatsBuilders.getEvolutionCounterStatisticsBuilder("voucher_change","evolutionengine-"+evolutionEngineKey);
    statsApiDuration = StatsBuilders.getEvolutionDurationStatisticsBuilder("evolutionengine_api","evolutionengine-"+evolutionEngineKey);
    statsStateStoresDuration = StatsBuilders.getEvolutionDurationStatisticsBuilder("evolutionengine_statestore","evolutionengine-"+evolutionEngineKey);

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
        for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricConfiguration().getMetrics().values())
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

    Properties streamsProperties = ConfigUtils.envPropertiesWithPrefix("KAFKA_STREAMS");
    streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID);
    streamsProperties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, applicationID+"-"+evolutionEngineKey);
    streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory);
    streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsProperties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EvolutionEventTimestampExtractor.class.getName());
    streamsProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Deployment.getEvolutionEngineStreamThreads());
    streamsProperties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, Integer.toString(kafkaReplicationFactor));
    streamsProperties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, Integer.toString(kafkaStreamsStandbyReplicas));
    streamsProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, subscriberProfileHost + ":" + Integer.toString(internalPort));
    streamsProperties.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, EvolutionProductionExceptionHandler.class);
    if(!isInMemoryStateStores && rocksDBCacheMBytes!=-1 && rocksDBMemTableMBytes!=-1)
      {
        BoundedMemoryRocksDBConfig.setRocksDBCacheMBytes(rocksDBCacheMBytes);
        BoundedMemoryRocksDBConfig.setRocksDBMemTableMBytes(rocksDBMemTableMBytes);
        streamsProperties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedMemoryRocksDBConfig.class);
      }
    // Please let this as a println and not logger message, will spit it even with ERROR level as it's vital
    System.out.println(" -- Running streams with the following config");
    System.out.println(streamsProperties);
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
    Map<String,EvolutionEngineEventDeclaration> evolutionEngineEvents = Deployment.getEvolutionEngineEvents();
    evolutionEngineEvents.put(voucherActionEventDeclaration.getName(), voucherActionEventDeclaration);
    evolutionEngineEvents.put(fileWithVariableEventDeclaration.getName(), fileWithVariableEventDeclaration);
    for (EvolutionEngineEventDeclaration evolutionEngineEvent : evolutionEngineEvents.values())
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
    final ConnectSerde<ExecuteActionOtherSubscriber> executeActionOtherSubscriberSerde = ExecuteActionOtherSubscriber.serde();
    final ConnectSerde<ProfileChangeEvent> profileChangeEventSerde = ProfileChangeEvent.serde();
    final ConnectSerde<ProfileSegmentChangeEvent> profileSegmentChangeEventSerde = ProfileSegmentChangeEvent.serde();
    final ConnectSerde<ProfileLoyaltyProgramChangeEvent> profileLoyaltyProgramChangeEventSerde = ProfileLoyaltyProgramChangeEvent.serde();
    final ConnectSerde<RecordSubscriberID> recordSubscriberIDSerde = RecordSubscriberID.serde();
    final ConnectSerde<JourneyRequest> journeyRequestSerde = JourneyRequest.serde();
    final ConnectSerde<JourneyStatistic> journeyStatisticSerde = JourneyStatistic.serde();
    final ConnectSerde<JourneyMetric> journeyMetricSerde = JourneyMetric.serde();
    final ConnectSerde<LoyaltyProgramRequest> loyaltyProgramRequestSerde = LoyaltyProgramRequest.serde();
    final ConnectSerde<SubscriberGroup> subscriberGroupSerde = SubscriberGroup.serde();
    final ConnectSerde<SubscriberTraceControl> subscriberTraceControlSerde = SubscriberTraceControl.serde();
    final ConnectSerde<SubscriberProfile> subscriberProfileSerde = SubscriberProfile.getSubscriberProfileSerde();
    final Serde<SubscriberTrace> subscriberTraceSerde = SubscriberTrace.serde();
    final ConnectSerde<ExternalAPIOutput> externalAPISerde = ExternalAPIOutput.serde();
    final ConnectSerde<TokenChange> tokenChangeSerde = TokenChange.serde();
    final ConnectSerde<VoucherChange> voucherChangeSerde = VoucherChange.serde();
    final ConnectSerde<VoucherAction> voucherActionSerde = VoucherAction.serde();
    final ConnectSerde<EDRDetails> edrDetailsSerde = EDRDetails.serde();
    final ConnectSerde<WorkflowEvent> workflowEventSerde = WorkflowEvent.serde();

    //
    //  special serdes
    //

    final Serde<SubscriberState> subscriberStateSerde = SubscriberState.stateStoreSerde();
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
    evolutionEventSerdes.add(executeActionOtherSubscriberSerde);
    evolutionEventSerdes.add(recordSubscriberIDSerde);
    evolutionEventSerdes.add(journeyRequestSerde);
    evolutionEventSerdes.add(loyaltyProgramRequestSerde);
    evolutionEventSerdes.add(subscriberGroupSerde);
    evolutionEventSerdes.add(subscriberTraceControlSerde);
    evolutionEventSerdes.addAll(evolutionEngineEventSerdes.values());
    for(DeliveryManagerDeclaration dmd:Deployment.getDeliveryManagers().values()) evolutionEventSerdes.add(dmd.getRequestSerde());
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
    KStream<StringKey, ExecuteActionOtherSubscriber> executeActionOtherSubscriberSourceStream = builder.stream(executeActionOtherSubscriberTopic, Consumed.with(stringKeySerde, executeActionOtherSubscriberSerde));
    KStream<StringKey, RecordSubscriberID> recordSubscriberIDSourceStream = builder.stream(recordSubscriberIDTopic, Consumed.with(stringKeySerde, recordSubscriberIDSerde));
    KStream<StringKey, SubscriberGroup> subscriberGroupSourceStream = builder.stream(subscriberGroupTopic, Consumed.with(stringKeySerde, subscriberGroupSerde));
    KStream<StringKey, SubscriberTraceControl> subscriberTraceControlSourceStream = builder.stream(subscriberTraceControlTopic, Consumed.with(stringKeySerde, subscriberTraceControlSerde));
    KStream<StringKey, PresentationLog> presentationLogSourceStream = builder.stream(presentationLogTopic, Consumed.with(stringKeySerde, presentationLogSerde));
    KStream<StringKey, AcceptanceLog> acceptanceLogSourceStream = builder.stream(acceptanceLogTopic, Consumed.with(stringKeySerde, acceptanceLogSerde));
    KStream<StringKey, ProfileSegmentChangeEvent> profileSegmentChangeEventStream = builder.stream(Deployment.getProfileSegmentChangeEventTopic(), Consumed.with(stringKeySerde, profileSegmentChangeEventSerde));
    KStream<StringKey, ProfileLoyaltyProgramChangeEvent> profileLoyaltyProgramChangeEventStream = builder.stream(Deployment.getProfileLoyaltyProgramChangeEventTopic(), Consumed.with(stringKeySerde, profileLoyaltyProgramChangeEventSerde));
    KStream<StringKey, VoucherChange> voucherChangeRequestSourceStream = builder.stream(voucherChangeRequestTopic, Consumed.with(stringKeySerde, voucherChangeSerde));
    KStream<StringKey, WorkflowEvent> workflowEventStream = builder.stream(workflowEventTopic, Consumed.with(stringKeySerde, workflowEventSerde));
    
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
    Map<String,EvolutionEngineEventDeclaration> evolutionEngineEventsDeclr = Deployment.getEvolutionEngineEvents();
    evolutionEngineEventsDeclr.put(voucherActionEventDeclaration.getName(), voucherActionEventDeclaration);
    evolutionEngineEventsDeclr.put(fileWithVariableEventDeclaration.getName(), fileWithVariableEventDeclaration);
    for (EvolutionEngineEventDeclaration evolutionEngineEventDeclaration : evolutionEngineEventsDeclr.values())
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

    List<KStream<StringKey, ? extends DeliveryRequest>> deliveryManagerResponseStreams = new ArrayList<>();
    for (DeliveryManagerDeclaration deliveryManagerDeclaration : Deployment.getDeliveryManagers().values())
      {
        deliveryManagerResponseStreams.add(builder.stream(deliveryManagerDeclaration.getResponseTopicsList(), Consumed.with(stringKeySerde, deliveryManagerDeclaration.getRequestSerde())).filter((key,value) -> value.getOriginatingRequest()));
      }

    //
    //  delivery manager request for evolution engine to process source streams
    //

    List<KStream<StringKey, ? extends DeliveryRequest>> deliveryManagerRequestToProcessStreams = new ArrayList<>();
    for (DeliveryManagerDeclaration deliveryManagerDeclaration : Deployment.getDeliveryManagers().values())
      {
        if(deliveryManagerDeclaration.isProcessedByEvolutionEngine()) deliveryManagerRequestToProcessStreams.add(builder.stream(deliveryManagerDeclaration.getRequestTopicsList(), Consumed.with(stringKeySerde, deliveryManagerDeclaration.getRequestSerde())));
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
    extendedProfileEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) executeActionOtherSubscriberSourceStream);
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
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) executeActionOtherSubscriberSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) recordSubscriberIDSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberGroupSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) subscriberTraceControlSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) presentationLogSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) acceptanceLogSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) profileSegmentChangeEventStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) profileLoyaltyProgramChangeEventStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) voucherChangeRequestSourceStream);
    evolutionEventStreams.add((KStream<StringKey, ? extends SubscriberStreamEvent>) workflowEventStream);
    evolutionEventStreams.addAll(standardEvolutionEngineEventStreams);
    evolutionEventStreams.addAll(deliveryManagerResponseStreams);
    evolutionEventStreams.addAll(deliveryManagerRequestToProcessStreams);
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

    KStream<StringKey,SubscriberStateOutputWrapper> forOutputsStream = evolutionEventStream.mapValues(event->new SubscriberStateOutputWrapper((event)));
    forOutputsStream.groupByKey().aggregate(EvolutionEngine::nullSubscriberState, EvolutionEngine::updateSubscriberState, subscriberStateStoreSchema);

    //
    //  get outputs
    //

    KStream<StringKey, SubscriberStateOutputWrapper> evolutionEngineOutputsWithSubscriberState = forOutputsStream.filter((key,value)->value.getSubscriberState()!=null);
    KStream<StringKey, SubscriberStreamOutput> evolutionEngineOutputs = evolutionEngineOutputsWithSubscriberState.flatMapValues(EvolutionEngine::getEvolutionEngineOutputs);

    //
    //  branch output streams
    //

    KStream<StringKey, ? extends SubscriberStreamOutput>[] branchedEvolutionEngineOutputs = evolutionEngineOutputs.branch(

        (key,value) -> (value instanceof DeliveryRequest), 
        (key,value) -> (value instanceof JourneyStatistic), 
        (key,value) -> (value instanceof JourneyMetric), 
        (key,value) -> (value instanceof SubscriberTrace),
        (key,value) -> (value instanceof ExternalAPIOutput),

        (key,value) -> (value instanceof ProfileChangeEvent),
        (key,value) -> (value instanceof ProfileSegmentChangeEvent),
        (key,value) -> (value instanceof ProfileLoyaltyProgramChangeEvent),
        (key,value) -> (value instanceof TokenChange),
        (key,value) -> (value instanceof VoucherChange),

        (key,value) -> (value instanceof ExecuteActionOtherSubscriber),
        (key,value) -> (value instanceof VoucherAction),
        (key,value) -> (value instanceof JourneyTriggerEventAction),
        (key,value) -> (value instanceof SubscriberProfileForceUpdate),
        (key,value) -> (value instanceof EDRDetails),
        (key,value) -> (value instanceof WorkflowEvent)
    );

    KStream<StringKey, DeliveryRequest> deliveryRequestStream = (KStream<StringKey, DeliveryRequest>) branchedEvolutionEngineOutputs[0];
    KStream<StringKey, JourneyStatistic> journeyStatisticStream = (KStream<StringKey, JourneyStatistic>) branchedEvolutionEngineOutputs[1];
    KStream<StringKey, JourneyMetric> journeyMetricStream = (KStream<StringKey, JourneyMetric>) branchedEvolutionEngineOutputs[2];
    KStream<StringKey, SubscriberTrace> subscriberTraceStream = (KStream<StringKey, SubscriberTrace>) branchedEvolutionEngineOutputs[3];
    KStream<StringKey, ExternalAPIOutput> externalAPIOutputsStream = (KStream<StringKey, ExternalAPIOutput>) branchedEvolutionEngineOutputs[4];

    KStream<StringKey, ProfileChangeEvent> profileChangeEventsStream = (KStream<StringKey, ProfileChangeEvent>) branchedEvolutionEngineOutputs[5];
    KStream<StringKey, ProfileSegmentChangeEvent> profileSegmentChangeEventsStream = (KStream<StringKey, ProfileSegmentChangeEvent>) branchedEvolutionEngineOutputs[6];
    KStream<StringKey, ProfileLoyaltyProgramChangeEvent> profileLoyaltyProgramChangeEventsStream = (KStream<StringKey, ProfileLoyaltyProgramChangeEvent>) branchedEvolutionEngineOutputs[7];
    KStream<StringKey, TokenChange> tokenChangeStream = (KStream<StringKey, TokenChange>) branchedEvolutionEngineOutputs[8];
    KStream<StringKey, VoucherChange> voucherChangeStream = (KStream<StringKey, VoucherChange>) branchedEvolutionEngineOutputs[9];

    KStream<StringKey, ExecuteActionOtherSubscriber> executeActionOtherSubscriberStream = (KStream<StringKey, ExecuteActionOtherSubscriber>) branchedEvolutionEngineOutputs[10];
    KStream<StringKey, VoucherAction> voucherActionStream = (KStream<StringKey, VoucherAction>) branchedEvolutionEngineOutputs[11];
    KStream<StringKey, JourneyTriggerEventAction> journeyTriggerEventActionStream = (KStream<StringKey, JourneyTriggerEventAction>) branchedEvolutionEngineOutputs[12];
    KStream<StringKey, SubscriberProfileForceUpdate> subscriberProfileForceUpdateStream = (KStream<StringKey, SubscriberProfileForceUpdate>) branchedEvolutionEngineOutputs[13];
    KStream<StringKey, EDRDetails> edrDetailsStream = (KStream<StringKey, EDRDetails>) branchedEvolutionEngineOutputs[14];
    KStream<StringKey, WorkflowEvent> workflowEventsStream = (KStream<StringKey, WorkflowEvent>) branchedEvolutionEngineOutputs[15];
    /*****************************************
    *
    *  sink
    *
    *****************************************/

    //
    //  sink - core streams
    //

    journeyStatisticStream.to(Deployment.getJourneyStatisticTopic(), Produced.with(stringKeySerde, journeyStatisticSerde));
    journeyMetricStream.to(Deployment.getJourneyMetricTopic(), Produced.with(stringKeySerde, journeyMetricSerde));
    subscriberTraceStream.to(Deployment.getSubscriberTraceTopic(), Produced.with(stringKeySerde, subscriberTraceSerde));
    extendedProfileSubscriberTraceStream.to(Deployment.getSubscriberTraceTopic(), Produced.with(stringKeySerde, subscriberTraceSerde));
    profileChangeEventsStream.to(Deployment.getProfileChangeEventTopic(), Produced.with(stringKeySerde, profileChangeEventSerde));
    profileSegmentChangeEventsStream.to(Deployment.getProfileSegmentChangeEventTopic(), Produced.with(stringKeySerde, profileSegmentChangeEventSerde));
    profileLoyaltyProgramChangeEventsStream.to(Deployment.getProfileLoyaltyProgramChangeEventTopic(), Produced.with(stringKeySerde, profileLoyaltyProgramChangeEventSerde));
    tokenChangeStream.to(Deployment.getTokenChangeTopic(), Produced.with(stringKeySerde, tokenChangeSerde));
    voucherChangeStream.to(Deployment.getVoucherChangeResponseTopic(), Produced.with(stringKeySerde, voucherChangeSerde));
    executeActionOtherSubscriberStream.map((key,value)->new KeyValue<>(new StringKey(value.getSubscriberID()),value)).to(Deployment.getExecuteActionOtherSubscriberTopic(), Produced.with(stringKeySerde, executeActionOtherSubscriberSerde));
    voucherActionStream.to(Deployment.getVoucherActionTopic(), Produced.with(stringKeySerde, voucherActionSerde));
    subscriberProfileForceUpdateStream.to(Deployment.getSubscriberProfileForceUpdateTopic(), Produced.with(stringKeySerde, subscriberProfileForceUpdateSerde));
    edrDetailsStream.to(Deployment.getEdrDetailsTopic(), Produced.with(stringKeySerde, edrDetailsSerde));

    //
	//  sink DeliveryRequest
	//

	// rekeyed as needed
	KStream<StringKey, DeliveryRequest> rekeyedDeliveryRequestStream = deliveryRequestStream.map(EvolutionEngine::rekeyDeliveryRequestStream);
	// topics/predicates/serdes for branching by request or response and priority
	// important to keep those 2 lists coherent one with the other!
	LinkedList<String> topics = new LinkedList<>();
	LinkedList<Predicate<StringKey,DeliveryRequest>> deliveryRequestPredicates = new LinkedList<>();
	Map<String,ConnectSerde<DeliveryRequest>> deliveryRequestSerdes = new HashMap<>();// <topic,serde>
	// populate
	for(DeliveryManagerDeclaration deliveryManagerDeclaration:Deployment.getDeliveryManagers().values()){
	  for(DeliveryPriority priority:DeliveryPriority.values()){
	  	if(deliveryManagerDeclaration.isProcessedByEvolutionEngine() || deliveryManagerDeclaration.getDeliveryType().equals(CommodityDeliveryManager.COMMODITY_DELIVERY_TYPE)/*or special case the "hacky loyalty point update BDR only" (sounds to me that is actually not the hacky at all, sending point request to commodity delivery manager to send back to engine to send back to commodity delivery manager to send back to engine feels a bit more shitty)*/){
	  	  String topic = deliveryManagerDeclaration.getResponseTopic(priority);// a response of a request we did process
		  topics.add(topic);
	  	  deliveryRequestPredicates.add((key,value)->value.getDeliveryType().equals(deliveryManagerDeclaration.getDeliveryType()) && value.getDeliveryPriority()==priority && !value.isPending());
	  	  deliveryRequestSerdes.put(topic,(ConnectSerde<DeliveryRequest>) deliveryManagerDeclaration.getRequestSerde());
		}
		String topic = deliveryManagerDeclaration.getRequestTopic(priority);// a request we are doing
		topics.add(topic);
		deliveryRequestPredicates.add((key,value)->value.getDeliveryType().equals(deliveryManagerDeclaration.getDeliveryType()) && value.getDeliveryPriority()==priority && value.isPending());
		deliveryRequestSerdes.put(topic,(ConnectSerde<DeliveryRequest>) deliveryManagerDeclaration.getRequestSerde());
	  }
	}
	//branch and sink
	Iterator<String> topicIterator = topics.iterator();
	for(KStream<StringKey,DeliveryRequest> stream:rekeyedDeliveryRequestStream.branch(deliveryRequestPredicates.toArray(new Predicate[deliveryRequestPredicates.size()]))){
	  String topic = topicIterator.next();
	  stream.to(topic,Produced.with(stringKeySerde,deliveryRequestSerdes.get(topic)));
	}

	//
    // sink TriggerEvent
    //

    // important to keep those 2 lists coherent one with the other!
    LinkedList<EvolutionEngineEventDeclaration> triggerEventsDeclarations = new LinkedList<>();
    LinkedList<Predicate<StringKey,JourneyTriggerEventAction>> triggerEventsPredicates = new LinkedList<>();
	for(EvolutionEngineEventDeclaration eventDeclaration:Deployment.getEvolutionEngineEvents().values()){
	  if(!eventDeclaration.isTriggerEvent()) continue;
	  triggerEventsDeclarations.add(eventDeclaration);
	  triggerEventsPredicates.add((key,value)->value.getEventDeclaration().getEventClass().equals(eventDeclaration.getEventClass()));
	}
	//branch and sink if needed
    if(!triggerEventsDeclarations.isEmpty()){
      Iterator<EvolutionEngineEventDeclaration> eventDeclarationIterator = triggerEventsDeclarations.iterator();
      for(KStream<StringKey,JourneyTriggerEventAction> stream:journeyTriggerEventActionStream.branch(triggerEventsPredicates.toArray(new Predicate[triggerEventsPredicates.size()]))){
        EvolutionEngineEventDeclaration eventDeclaration = eventDeclarationIterator.next();
        KStream<StringKey,EvolutionEngineEvent> outputStream = stream.mapValues(value->value.getEventToTrigger());
        outputStream.to(eventDeclaration.getEventTopic(),Produced.with(stringKeySerde,eventDeclaration.getEventSerde()));
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

        // need to re-run unique key prefix computation
        if(newState==KafkaStreams.State.RUNNING) KStreamsUniqueKeyServer.streamRebalanced();
        partitionHostMap.clear();// cache of which host have which partition for HTPP API

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
    httpClientConnectionManager.setDefaultMaxPerRoute(Deployment.getEvolutionEngineStreamThreads()*Deployment.getHttpServerScalingFactor());
    httpClientConnectionManager.setMaxTotal(Deployment.getEvolutionEngineInstanceNumbers()*Deployment.getEvolutionEngineStreamThreads()*Deployment.getHttpServerScalingFactor());

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
        subscriberProfileServer.setExecutor(Executors.newFixedThreadPool(Deployment.getEvolutionEngineInstanceNumbers()*Deployment.getEvolutionEngineStreamThreads()*Deployment.getHttpServerScalingFactor()));        
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
        internalServer.setExecutor(Executors.newFixedThreadPool(Deployment.getEvolutionEngineStreamThreads()*Deployment.getHttpServerScalingFactor()));
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

    NGLMRuntime.addShutdownHook(new ShutdownHook(streams, subscriberGroupEpochReader, ucgStateReader, dynamicCriterionFieldService, journeyService, loyaltyProgramService, targetService, journeyObjectiveService, segmentationDimensionService, presentationStrategyService, scoringStrategyService, offerService, salesChannelService, tokenTypeService, subscriberMessageTemplateService, deliverableService, segmentContactPolicyService, timerService, pointService, exclusionInclusionTargetService, productService, productTypeService, voucherService, voucherTypeService, catalogCharacteristicService, dnboMatrixService, paymentMeanService, subscriberProfileServer, internalServer, stockService, resellerService, supplierService));

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

    // start the secondary preprocessor streams if needed
    Preprocessor.startPreprocessorStreams(evolutionEngineKey,supplierService,voucherService,voucherTypeService);

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
    private ResellerService resellerService;
    private SupplierService supplierService;
    //
    //  constructor
    //

    private ShutdownHook(KafkaStreams kafkaStreams, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,UCGState> ucgStateReader, DynamicCriterionFieldService dynamicCriterionFieldsService, JourneyService journeyService, LoyaltyProgramService loyaltyProgramService, TargetService targetService, JourneyObjectiveService journeyObjectiveService, SegmentationDimensionService segmentationDimensionService, PresentationStrategyService presentationStrategyService, ScoringStrategyService scoringStrategyService, OfferService offerService, SalesChannelService salesChannelService, TokenTypeService tokenTypeService, SubscriberMessageTemplateService subscriberMessageTemplateService, DeliverableService deliverableService, SegmentContactPolicyService segmentContactPolicyService, TimerService timerService, PointService pointService, ExclusionInclusionTargetService exclusionInclusionTargetService, ProductService productService, ProductTypeService productTypeService, VoucherService voucherService, VoucherTypeService voucherTypeService, CatalogCharacteristicService catalogCharacteristicService, DNBOMatrixService dnboMatrixService, PaymentMeanService paymentMeanService, HttpServer subscriberProfileServer, HttpServer internalServer, StockMonitor stockService, ResellerService resellerService, SupplierService supplierService)
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
      this.supplierService = supplierService;
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
      this.resellerService = resellerService;
      this.supplierService = supplierService;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {

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
      supplierService.stop();
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
      resellerService.stop();
      supplierService.stop();

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

  public static SubscriberState updateSubscriberState(StringKey aggKey, SubscriberStateOutputWrapper evolutionHackyEvent, SubscriberState previousSubscriberState)
  {

    SubscriberStreamEvent evolutionEvent = evolutionHackyEvent.getOriginalEvent();

    /****************************************
    *
    *  retrieve TenantID either from event or from SubscriberProfile
    *
    ****************************************/
    int tenantID;
    if(previousSubscriberState == null)
      {
        // ensure this event is of type Auto
        if(evolutionEvent instanceof AutoProvisionSubscriberStreamEvent)
          {
            tenantID = ((AutoProvisionSubscriberStreamEvent)evolutionEvent).getTenantID();
          }
        else {
          log.warn("Event " + evolutionEvent.getClass() + " does not implement AutoProvisionSubscriberStreamEvent, can't retrieve the tenantID for SubscriberState creation");
          return null;
        }
      }
    else
      {
        tenantID = previousSubscriberState.getSubscriberProfile().getTenantID();
      }

    // NO MORE DEEP COPY !!!!
	// previous one or new empty
    SubscriberState subscriberState = (previousSubscriberState != null) ? previousSubscriberState : new SubscriberState(evolutionEvent.getSubscriberID(), tenantID);
    // keep the previous stored byte[]
    byte[] storedBefore = (previousSubscriberState != null) ? previousSubscriberState.getKafkaRepresentation() : new byte[0];// this empty means as well subscriber was not existing before
    // need to save the previous scheduled evaluation
	Set<TimedEvaluation> scheduledEvaluationsBefore = new TreeSet<>(subscriberState.getScheduledEvaluations());
	// and clean it
	subscriberState.getScheduledEvaluations().clear();

    boolean subscriberStateUpdated = previousSubscriberState == null;

    SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();
    ExtendedSubscriberProfile extendedSubscriberProfile = (evolutionEvent instanceof TimedEvaluation) ? ((TimedEvaluation) evolutionEvent).getExtendedSubscriberProfile() : null;
    EvolutionEventContext context = new EvolutionEventContext(subscriberState, evolutionEvent, extendedSubscriberProfile, subscriberGroupEpochReader, journeyService, subscriberMessageTemplateService, deliverableService, segmentationDimensionService, presentationStrategyService, scoringStrategyService, offerService, salesChannelService, tokenTypeService, segmentContactPolicyService, productService, productTypeService, voucherService, voucherTypeService, catalogCharacteristicService, dnboMatrixService, paymentMeanService, uniqueKeyServer, resellerService, supplierService, customCriteriaService, SystemTime.getCurrentTime());

    if(log.isTraceEnabled()) log.trace("updateSubscriberState on event "+evolutionEvent.getClass().getSimpleName()+ " for "+evolutionEvent.getSubscriberID());

    Date now = context.now();

    /*****************************************
    *
    *  cleanup
    *
    *****************************************/

    switch (evolutionEvent.getSubscriberAction())
      {
        case Cleanup:
          updateScheduledEvaluations(scheduledEvaluationsBefore, Collections.<TimedEvaluation>emptySet());
          return null;
          
        case Delete:
          if(previousSubscriberState==null) return null;
          SubscriberState.stateStoreSerde().setKafkaRepresentation(Deployment.getSubscriberStateChangeLogTopic(), subscriberState);
          return subscriberState;
      }



    /*****************************************
    *
    *  update subscriber hierarchy
    *
    *****************************************/

    if(evolutionEvent instanceof UpdateParentRelationshipEvent && ((UpdateParentRelationshipEvent)evolutionEvent).getNewParent() != null && ((UpdateParentRelationshipEvent)evolutionEvent).getRelationshipDisplay() != null)
      {
        UpdateParentRelationshipEvent updateParentRelationshipEvent = (UpdateParentRelationshipEvent)evolutionEvent;
        // This is the children that set or unset a parent for a given type of relation
        String relationshipDisplay = updateParentRelationshipEvent.getRelationshipDisplay();
        String relationshipID = null;
        for (SupportedRelationship supportedRelationship : Deployment.getDeployment(tenantID).getSupportedRelationships().values())
          {
            if (supportedRelationship.getDisplay().equals(relationshipDisplay))
              {
                relationshipID = supportedRelationship.getID();
                break;
              }
          }
        if(relationshipID != null)
          {
            if(!updateParentRelationshipEvent.isDeletion())
              {
                // set a parent
                SubscriberRelatives sr = subscriberProfile.getRelations().get(relationshipID);
                if(sr == null)
                  {
                    sr = new SubscriberRelatives();
                    subscriberProfile.getRelations().put(relationshipID, sr);
                  }
                sr.setParentSubscriberID(updateParentRelationshipEvent.getNewParent());
              }
            else
              {
                // delete a parent
                SubscriberRelatives sr = subscriberProfile.getRelations().get(relationshipID);
                if(sr != null)
                  {
                    // no need to keep this parent
                    if(!updateParentRelationshipEvent.getNewParent().equals(sr.getParentSubscriberID()))
                      {
                        // just log a warn as we remove a parent for a relation that is not the expected one...
                        log.warn("Delete parent " + sr.getParentSubscriberID() + " instead of " + updateParentRelationshipEvent.getNewParent() + " for suscriber " + evolutionEvent.getSubscriberID());
                      }
                    sr.setParentSubscriberID(null);
                  }
                else
                  {
                    // we tried to delete a parent that does not exist, just log a WARN
                    log.warn("Delete parent where no parent is referenced " + updateParentRelationshipEvent.getNewParent() + " for suscriber " + evolutionEvent.getSubscriberID());
                  }
              }
          }
        else
          {
            log.warn("relationshipDisplay is unknown " + relationshipDisplay);
          }
      }
    if(evolutionEvent instanceof UpdateChildrenRelationshipEvent)
      {
        // This is the parent that inserts or remove a childrem from its list for a given type of relation
        UpdateChildrenRelationshipEvent updateChildrenRelationshipEvent = (UpdateChildrenRelationshipEvent)evolutionEvent;
        String relationshipDisplay = updateChildrenRelationshipEvent.getRelationshipDisplay();
        String relationshipID = null;
        for (SupportedRelationship supportedRelationship : Deployment.getDeployment(tenantID).getSupportedRelationships().values())
          {
            if (supportedRelationship.getDisplay().equals(relationshipDisplay))
              {
                relationshipID = supportedRelationship.getID();
                break;
              }
          }
        if(relationshipID != null)
          {
            if(!updateChildrenRelationshipEvent.isDeletion())
              {
                // add a children
                SubscriberRelatives sr = subscriberProfile.getRelations().get(relationshipID);
                if(sr == null)
                  {
                    sr = new SubscriberRelatives();
                    subscriberProfile.getRelations().put(relationshipID, sr);
                  }
                sr.addChildSubscriberID(updateChildrenRelationshipEvent.getChildren());
              }
            else
              {
                // remove a children
                SubscriberRelatives sr = subscriberProfile.getRelations().get(relationshipID);
                if(sr != null)
                  {
                    sr.removeChildSubscriberID(updateChildrenRelationshipEvent.getChildren());
                  }
                else
                  {
                    // we tried to delete a child that does not exist, just log a WARN
                    log.warn("Delete child where no child is referenced " + updateChildrenRelationshipEvent.getChildren() + " for suscriber " + evolutionEvent.getSubscriberID());
                  }
              }
          }
        else
          {
            log.warn("relationshipDisplay is unknown " + relationshipDisplay );
          }
      }

    // now clean unused relations
    ArrayList<String> toRemove = new ArrayList<>();
    for(Map.Entry<String, SubscriberRelatives> sr : subscriberProfile.getRelations().entrySet())
      {
        if((sr.getValue().getChildrenSubscriberIDs() == null || sr.getValue().getChildrenSubscriberIDs().size() == 0) && sr.getValue().getParentSubscriberID() == null)
          {
            toRemove.add(sr.getKey());
          }
      }
    for(String s : toRemove)
      {
        subscriberProfile.getRelations().remove(s);
      }
    
    /*****************************************
    *
    *  profileChangeEvent get Old Values
    *
    *****************************************/

    SubscriberEvaluationRequest changeEventEvaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, extendedSubscriberProfile, subscriberGroupEpochReader, now, tenantID);
    ParameterMap profileChangeOldValues = saveProfileChangeOldValues(changeEventEvaluationRequest); 
    
    /*****************************************
    *
    *  profileSegmentChangeEvent get Old Values
    *
    *****************************************/
    
    ParameterMap profileSegmentChangeOldValues = saveProfileSegmentChangeOldValues(changeEventEvaluationRequest);
    
    /*****************************************
    *
    *  generate EDR
    *
    *****************************************/
    
    if (shoudGenerateEDR(evolutionEvent))
      {
        subscriberStateUpdated = updateEDRs(context, evolutionEvent) || subscriberStateUpdated;
      }

    /*****************************************
    *
    *  update SubscriberProfile
    *
    *****************************************/

    subscriberStateUpdated = updateSubscriberProfile(context, evolutionEvent) || subscriberStateUpdated;

    /*****************************************
    *
    *  executeActionOtherSubscriber : Actions triggered by another subscriber for this one.
    *
    *****************************************/

    subscriberStateUpdated = executeActionOtherSubscriber(context, evolutionEvent, context.getSubscriberState().getSubscriberProfile().getTenantID()) || subscriberStateUpdated;
    
    /*****************************************
    *
    *  update workflow
    *
    *****************************************/

    subscriberStateUpdated = updateWorkflows(context, evolutionEvent) || subscriberStateUpdated;


    /*****************************************
    *
    *  update SubscriberLoyaltyProgram
    *
    *****************************************/

    subscriberStateUpdated = updateSubscriberLoyaltyProgram(context, evolutionEvent, context.getSubscriberState().getSubscriberProfile().getTenantID()) || subscriberStateUpdated;
    
    /*****************************************
    *
    *  update PropensityOutputs
    *
    *****************************************/

    subscriberStateUpdated = updatePropensity(context, evolutionEvent, tenantID) || subscriberStateUpdated;

    /*****************************************
    *
    *  update journeyStates
    *
    *****************************************/

    subscriberStateUpdated = updateJourneys(context, evolutionEvent, context.getSubscriberState().getSubscriberProfile().getTenantID()) || subscriberStateUpdated;
    
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

    subscriberStateUpdated = updateVouchers(context, evolutionEvent, tenantID) || subscriberStateUpdated;

    /*****************************************
    *
    *  profile change detect changed values
    *
    *****************************************/
        
    updatePointBalances(context, subscriberState, now, context.getSubscriberState().getSubscriberProfile().getTenantID());
    
    /*****************************************
    *
    *  profile change detect changed values
    *
    *****************************************/
        
    updateChangeEvents(subscriberState, now, changeEventEvaluationRequest, profileChangeOldValues, tenantID);
    
    /*****************************************
    *
    *  profile segment change detect changed segments
    *
    *****************************************/
        
    updateSegmentChangeEvents(subscriberState, subscriberProfile, now, changeEventEvaluationRequest, profileSegmentChangeOldValues, tenantID);

    /*****************************************
    *
    *  detectReScheduledDeliveryRequests : add to scheduledEvaluations
    *
    *****************************************/

    subscriberStateUpdated = detectReScheduledDeliveryRequests(context, evolutionEvent) || subscriberStateUpdated;
    
    /*****************************************
    *
    *  scheduledEvaluations
    *
    *****************************************/

    updateScheduledEvaluations(scheduledEvaluationsBefore, subscriberState.getScheduledEvaluations());

    /*****************************************
    *
    *  if notification
    *  populate request with the notificationMetricHistory for the external processor to apply contact policy
    *  update subscriberState with the +1 notif for the channel
    *
    *****************************************/
    subscriberStateUpdated = updateNotificationMetricHistory(context.getSubscriberState(), now, tenantID) || subscriberStateUpdated;

    /*****************************************
    *
    *  subscriberTrace
    *
    *****************************************/

    if (subscriberProfile.getSubscriberTraceEnabled())
      {
        try{
          subscriberState.setSubscriberTrace(new SubscriberTrace(generateSubscriberTraceMessage(evolutionEvent, subscriberState, context.getSubscriberTraceDetails())));
        }catch(Exception e){
          log.warn("subscriber trace exception",e);
        }
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

    Pair<String,JSONObject> resExternalAPI = callExternalAPI(evolutionEvent, subscriberState);
    JSONObject jsonObject = resExternalAPI.getSecondElement();
    if (jsonObject != null)
      {
        subscriberState.setExternalAPIOutput(new ExternalAPIOutput(resExternalAPI.getFirstElement(), jsonObject.toJSONString()));
        subscriberStateUpdated = true;
      }

    /*****************************************
    *
    *  lastEvaluationDate, only if current event is an effective periodic evaluation, i.e. TimedEvaluation + periodicEvaluation = true
    *
    *****************************************/

    if(evolutionEvent instanceof TimedEvaluation && ((TimedEvaluation)evolutionEvent).getPeriodicEvaluation())
      {
        subscriberState.setLastEvaluationDate(now);
      }
    subscriberStateUpdated = true;

    /*****************************************
    *
    *  stateStoreSerde
    *
    *****************************************/

    if (subscriberStateUpdated)
      {
        SubscriberState.stateStoreSerde().setKafkaRepresentation(Deployment.getSubscriberStateChangeLogTopic(), subscriberState);//build the new
        evolutionEngineStatistics.updateSubscriberStateSize(subscriberState.getKafkaRepresentation());
        if (subscriberState.getKafkaRepresentation().length > 950000)
          {
            log.error("StateStore size error, ignoring event {} for subscriber {}: {}", evolutionEvent.getClass().toString(), evolutionEvent.getSubscriberID(), subscriberState.getKafkaRepresentation().length);
            if(storedBefore.length>0) subscriberState.setKafkaRepresentation(storedBefore);//put back the old
            subscriberStateUpdated = false;
          }
      }

    /****************************************
    *
    *  return
    *
    ****************************************/

    if(subscriberStateUpdated){
        log.trace("updateSubscriberState : subscriberStateUpdated enriching event with it for down stream processing");
        evolutionHackyEvent.enrichWithSubscriberState(subscriberState);
    }

    return subscriberState;
  }
  
  /*****************************************
  *
  *  saveProfileChangeOldValues
  *
  *****************************************/
  
  private static ParameterMap saveProfileChangeOldValues(SubscriberEvaluationRequest changeEventEvaluationRequest)
  {
    ParameterMap profileChangeOldValues = new ParameterMap();
    for(String criterionFieldID: Deployment.getDeployment(changeEventEvaluationRequest.getTenantID()).getProfileChangeDetectionCriterionFields().keySet()) {
      Object value = CriterionContext.Profile(changeEventEvaluationRequest.getTenantID()).getCriterionFields(changeEventEvaluationRequest.getTenantID()).get(criterionFieldID).retrieve(changeEventEvaluationRequest);
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
    if (!Deployment.getDeployment(changeEventEvaluationRequest.getTenantID()).getEnableProfileSegmentChange())
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
    
    for(SegmentationDimension dimension : segmentationDimensionService.getActiveSegmentationDimensions(SystemTime.getCurrentTime(), changeEventEvaluationRequest.getTenantID())){
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
  
  private static void updatePointBalances(EvolutionEventContext context, SubscriberState subscriberState, Date now, int tenantID)
  {
    Map<String, PointBalance> pointBalances = subscriberState.getSubscriberProfile().getPointBalances() != null ? subscriberState.getSubscriberProfile().getPointBalances() : Collections.<String,PointBalance>emptyMap();
    for(String pointID: pointBalances.keySet())
      {
        Point point = pointService.getActivePoint(pointID, now);
        if(point != null)
          {
            //TODO : what module is best here ?
            updatePointBalance(context, null, "checkBonusExpiration", Module.Unknown.getExternalRepresentation(), "checkBonusExpiration", subscriberState.getSubscriberProfile(), point, CommodityDeliveryOperation.Expire, 0, now, true, "", tenantID);
          }
      }
  }
  

  /*****************************************
   *
   *  updateVouchers
   *
   *****************************************/

  private static boolean updateVouchers(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent, int tenantID)
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
              expiryDate = EvolutionUtilities.addTime(now,voucherType.getValidity().getPeriodQuantity(),voucherType.getValidity().getPeriodType(),Deployment.getDeployment(tenantID).getTimeZone(),voucherType.getValidity().getRoundDown()? EvolutionUtilities.RoundingSelection.RoundDown: EvolutionUtilities.RoundingSelection.NoRound);
            }else if(voucherType.getCodeType()==VoucherType.CodeType.Personal){
              VoucherPersonal voucherPersonal = (VoucherPersonal) voucher;
              for(VoucherFile voucherFile:voucherPersonal.getVoucherFiles()){
                if(voucherFile.getFileId().equals(voucherDelivery.getFileID())){
                  if(voucherFile.getExpiryDate()==null){
                    expiryDate = EvolutionUtilities.addTime(now,voucherType.getValidity().getPeriodQuantity(),voucherType.getValidity().getPeriodType(),Deployment.getDeployment(tenantID).getTimeZone(),voucherType.getValidity().getRoundDown()? EvolutionUtilities.RoundingSelection.RoundDown: EvolutionUtilities.RoundingSelection.NoRound);
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
            int returnCode = purchaseFulfillmentRequest.getReturnCode();
         // storing the voucherChange
            VoucherChange voucherChange = new VoucherChange(
                purchaseFulfillmentRequest.getSubscriberID(),
                purchaseFulfillmentRequest.getEventDate(),
                null,
                purchaseFulfillmentRequest.getEventID(),
                VoucherChangeAction.Deliver,
                voucherDelivery.getVoucherCode(),
                voucherDelivery.getVoucherID(),
                voucherDelivery.getFileID(),
                purchaseFulfillmentRequest.getModuleID(),
                purchaseFulfillmentRequest.getFeatureID(),
                purchaseFulfillmentRequest.getOrigin(),
                RESTAPIGenericReturnCodes.fromGenericResponseCode(returnCode),
                tenantID);
            subscriberProfile.getVouchers().add(voucherToStore);
            subscriberState.getVoucherChanges().add(voucherChange);
            // we keep voucher ordered by expiry data, this is important when we will apply change
            sortVouchersPerExpiryDate(subscriberProfile);
            subscriberUpdated = true;
          }
        }
      }else{
        if(log.isDebugEnabled()) log.debug("no voucher delivered in purchaseFulfillmentRequest");
      }
    }

    // no we check all, if some expired, to clean, expired to generate event ???
    if(subscriberProfile.getVouchers()!=null && !subscriberProfile.getVouchers().isEmpty()){

      for(VoucherProfileStored voucherStored:subscriberProfile.getVouchers()){
        if(voucherStored.getVoucherExpiryDate()==null){
          log.warn("voucher expiration date is null ???");
          continue;
        }
        // change status to expired
        if(voucherStored.getVoucherStatus()!=VoucherDelivery.VoucherStatus.Expired
                && voucherStored.getVoucherStatus()!=VoucherDelivery.VoucherStatus.Redeemed
                && retentionService.isExpired(voucherStored)){
          voucherStored.setVoucherStatus(VoucherDelivery.VoucherStatus.Expired); 
          VoucherChange voucherChange = new VoucherChange(
              subscriberProfile.getSubscriberID(),              
              SystemTime.getCurrentTime(),
              null,
              voucherStored.getEventID(),
              VoucherChangeAction.Expire,
              voucherStored.getVoucherCode(),
              voucherStored.getVoucherID(),
              voucherStored.getFileID(),
              voucherStored.getModuleID(),
              voucherStored.getFeatureID(),
              voucherStored.getOrigin(),
              RESTAPIGenericReturnCodes.SUCCESS,
              tenantID);
          subscriberState.getVoucherChanges().add(voucherChange);
          subscriberUpdated=true;
        }

      }

    }else{
      if(log.isTraceEnabled()) log.trace("no vouchers to expire in vouchers stored in profile for "+subscriberProfile.getSubscriberID());
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
            // note that this check can still match more than one voucher, but subscriberProfile.getVouchers() should be ordered soonest expiry date first
            if(voucherStored.getVoucherCode().equals(voucherChange.getVoucherCode()) && voucherStored.getVoucherID().equals(voucherChange.getVoucherID())){
              voucherFound=true;
              if(log.isDebugEnabled()) log.debug("need to apply to stored voucher "+voucherStored);

              // redeem
              if(voucherChange.getAction()==VoucherChange.VoucherChangeAction.Redeem){
                checkRedeemVoucher(voucherStored, voucherChange, true);
                if (voucherStored.getVoucherStatus() == VoucherDelivery.VoucherStatus.Redeemed) break;
                
                /*
                 * if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Redeemed){
                 * // already redeemed voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.
                 * VOUCHER_ALREADY_REDEEMED); } else
                 * if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Expired){
                 * // already expired
                 * voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_EXPIRED); }
                 * else
                 * if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Delivered)
                 * { // redeem voucher OK
                 * voucherStored.setVoucherStatus(VoucherDelivery.VoucherStatus.Redeemed);
                 * voucherStored.setVoucherRedeemDate(now);
                 * voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS); break; }
                 * else{ // default KO voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.
                 * VOUCHER_NON_REDEEMABLE); }
                 */
              }

              // extend
              if(voucherChange.getAction()==VoucherChange.VoucherChangeAction.Extend){
                if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Redeemed){
                  // already redeemed
                  voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_ALREADY_REDEEMED);
                } else{
                  // extend voucher OK
                  voucherStored.setVoucherExpiryDate(voucherChange.getNewVoucherExpiryDate());
                  sortVouchersPerExpiryDate(subscriberProfile);
                  if(voucherStored.getVoucherExpiryDate().after(now)) voucherStored.setVoucherStatus(VoucherDelivery.VoucherStatus.Delivered);
                  voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);
                  break;
                }
              }

              // delete (expire it)
              if(voucherChange.getAction()==VoucherChange.VoucherChangeAction.Expire){
                if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Redeemed){
                  // already redeemed
                  voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_ALREADY_REDEEMED);
                } else if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Expired){
                  // already expired
                  voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_EXPIRED);
                } else {
                  // expire voucher OK
                  voucherStored.setVoucherExpiryDate(now);
                  sortVouchersPerExpiryDate(subscriberProfile);
                  voucherStored.setVoucherStatus(VoucherDelivery.VoucherStatus.Expired);
                  voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);
                  break;
                }
              }

            }
          }
          if(!voucherFound) voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_NOT_ASSIGNED);
        }else{
          if(log.isDebugEnabled()) log.debug("no vouchers stored for action " + voucherChange.getAction().getExternalRepresentation() + " in profile for "+subscriberProfile.getSubscriberID());
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
  *  updateWorkflows
  *
  *****************************************/

 private static boolean updateWorkflows(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent)
  {

    SubscriberState subscriberState = context.getSubscriberState();
    boolean subscriberUpdated = false;

    // first process the new ones coming with purchase response event
    if (evolutionEvent instanceof WorkflowEvent)
      {

        WorkflowEvent workflowEvent = (WorkflowEvent) evolutionEvent;
        String workflowID = workflowEvent.getWorkflowID();
        if (workflowID == null)
          {
            if (log.isDebugEnabled()) {
              log.warn("Worflow is empty");
            }
          }

        String toBeAdded = evolutionEvent.getClass().getName() + ":" + evolutionEvent.getEventDate().getTime() + ":"
            + workflowID + ":" + workflowEvent.getFeatureID();
        List<String> workflowTriggering = subscriberState.getWorkflowTriggering();
        if (workflowTriggering.contains(toBeAdded))
          {
            if (log.isDebugEnabled())
              {
                log.warn("triggerLoyaltyWorflow already has " + toBeAdded);
              }
          }
        workflowTriggering.add(toBeAdded);
        subscriberUpdated=true;
      }
    return subscriberUpdated;
  }

  private static void checkRedeemVoucher(VoucherProfileStored voucherStored, VoucherChange voucherChange, boolean redeem)
  {
    if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Redeemed){
      // already redeemed
      voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_ALREADY_REDEEMED);
    } else if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Expired){
      // already expired
      voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_EXPIRED);
    } else if(voucherStored.getVoucherStatus()==VoucherDelivery.VoucherStatus.Delivered){
      // redeem voucher OK
      if (redeem)
        {
          voucherStored.setVoucherStatus(VoucherDelivery.VoucherStatus.Redeemed);
          voucherStored.setVoucherRedeemDate(SystemTime.getCurrentTime());
        }
      voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);
    } else{
      // default KO
      voucherChange.setReturnStatus(RESTAPIGenericReturnCodes.VOUCHER_NON_REDEEMABLE);
    }
    // TODO Auto-generated method stub
    
  }


  // sort vouchers stored in subscriberProfile soones expiry date first
  private static void sortVouchersPerExpiryDate(SubscriberProfile subscriberProfile)
  {
    if(log.isTraceEnabled()) log.trace("sorting vouchers on expiry date, from soonest expiry to latest "+subscriberProfile.getVouchers());
    subscriberProfile.getVouchers().sort(Comparator.comparing(VoucherProfileStored::getVoucherExpiryDate));
    if(log.isTraceEnabled()) log.trace("sorting vouchers result "+subscriberProfile.getVouchers());
  }

  /*****************************************
  *
  *  updateChangeEvents
  *
  *****************************************/
  
  private static void updateChangeEvents(SubscriberState subscriberState, Date now, SubscriberEvaluationRequest changeEventEvaluationRequest, ParameterMap profileChangeOldValues, int tenantID)
  {
    ParameterMap profileChangeNewValues = new ParameterMap();
    for(String criterionFieldID: Deployment.getDeployment(tenantID).getProfileChangeDetectionCriterionFields().keySet()) {
      Object value = CriterionContext.Profile(changeEventEvaluationRequest.getTenantID()).getCriterionFields(changeEventEvaluationRequest.getTenantID()).get(criterionFieldID).retrieve(changeEventEvaluationRequest);
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
  
  private static void updateSegmentChangeEvents(SubscriberState subscriberState, SubscriberProfile subscriberProfile, Date now, SubscriberEvaluationRequest changeEventEvaluationRequest, ParameterMap profileSegmentChangeOldValues, int tenantID)
  {    
    if (!Deployment.getDeployment(tenantID).getEnableProfileSegmentChange())
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
    
    for(SegmentationDimension dimension : segmentationDimensionService.getActiveSegmentationDimensions(SystemTime.getCurrentTime(), changeEventEvaluationRequest.getTenantID()))
      {
        if(!profileSegmentChangeNewValues.containsKey(dimension.getSegmentationDimensionName())) 
          {
            profileSegmentChangeNewValues.put(dimension.getSegmentationDimensionName(), null);
          }   
      }
    
    // now compare entering, leaving, updating
    
    for(SegmentationDimension dimension : segmentationDimensionService.getActiveSegmentationDimensions(SystemTime.getCurrentTime(), changeEventEvaluationRequest.getTenantID()))
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

      }

    // On any event, check if ReScheduledDeliveryRequest's time has been reached, if yes trig the DeliveryRequest again
    List<ReScheduledDeliveryRequest> toTrig = new ArrayList<>();
    for(ReScheduledDeliveryRequest reScheduledDeliveryRequest : subscriberState.getReScheduledDeliveryRequests())
      {
        if(reScheduledDeliveryRequest.getEvaluationDate().before(now)) {
          // the one we have to trig
          toTrig.add(reScheduledDeliveryRequest);
        }else{
          // or the future one we need to schedule again ( we always clean up entirely scheduled requests on any event )
          TimedEvaluation timedEvaluation = new TimedEvaluation(subscriberProfile.getSubscriberID(), reScheduledDeliveryRequest.getDeliveryRequest().getRescheduledDate());
          subscriberState.getScheduledEvaluations().add(timedEvaluation);
          subscriberProfileUpdated = true;
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

    private static boolean updateNotificationMetricHistory(SubscriberState subscriberState, Date now, int tenantID)
    {
      // if there are no DeliveryRequests to be sent out, nothing to do
      if(subscriberState.getDeliveryRequests()==null || subscriberState.getDeliveryRequests().isEmpty()) return false;

      boolean updated = false;
      for(DeliveryRequest deliveryRequest:subscriberState.getDeliveryRequests()){

        // skip if not a notification
        if(!(deliveryRequest instanceof INotificationRequest)) continue;
        // skip if not "pending" (means not a "request", but normally only pending in that temp list)
        if(!deliveryRequest.isPending()) continue;

        INotificationRequest notificationRequest = (INotificationRequest)deliveryRequest;
        String channel = Deployment.getDeployment(tenantID).getDeliveryTypeCommunicationChannelIDMap().get(notificationRequest.getDeliveryType());
        // "warn" and skip if no channel (conf change?)
        if(channel==null){
          log.info("delivery type {} not matching channel, can not update notificationHistory for {}",notificationRequest.getDeliveryType(),subscriberState.getSubscriberID());
          continue;
        }
        // get notification metric history for that channel
        MetricHistory channelMetricHistory = subscriberState.getNotificationHistory().get(channel);
        // not yet any notif sent for this channel to this subs, init empty one
        if(channelMetricHistory==null){
          channelMetricHistory = new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS, tenantID);
          subscriberState.getNotificationHistory().put(channel,channelMetricHistory);
        }
        // FIRST enrich request with previous data (keep previous behaviour for downstream processor)
        deliveryRequest.setNotificationHistory(channelMetricHistory);
        // THEN increment by one
        Date notifDate = deliveryRequest.getEventDate()!=null ? deliveryRequest.getEventDate() : now ;
        channelMetricHistory.update(notifDate,1);
        updated = true;
      }

      return updated;

    }
  
  /*****************************************
  * 
  *  callExternalAPI
  *
  *****************************************/

  private static Pair<String,JSONObject> callExternalAPI(SubscriberStreamEvent evolutionEvent, SubscriberState subscriberState)
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
          //TODO: twice the same subscriberState now (EVPRO-885 remove our broken deep copy of SubscriberState)
          result = (Pair<String,JSONObject>) evolutionEngineExternalAPIMethod.invoke(null, subscriberState, subscriberState, evolutionEvent, journeyService);
        }
        catch (RuntimeException|IllegalAccessException|InvocationTargetException e)
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
    int tenantID = context.getSubscriberState().getSubscriberProfile().getTenantID();

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

    if (evolutionEvent instanceof TimedEvaluation && ((TimedEvaluation)evolutionEvent).getPeriodicEvaluation())
      {
        for (String lpID : subscriberProfile.getLoyaltyPrograms().keySet())
          {
            LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(lpID, now);
            if (loyaltyProgram instanceof LoyaltyProgramPoints) //"instanceof" returns false for nulls.
              {
                LoyaltyProgramState lps = subscriberProfile.getLoyaltyPrograms().get(lpID);
                if (lps instanceof LoyaltyProgramPointsState)
                  {
                    String currentTier = ((LoyaltyProgramPointsState) lps).getTierName();
                    Tier tier = ((LoyaltyProgramPoints) loyaltyProgram).getTier(currentTier);
                    if (tier != null)
                      {
                        subscriberProfileUpdated = triggerLoyaltyWorflow(evolutionEvent, context.getSubscriberState(), tier.getWorkflowDaily(), loyaltyProgram.getLoyaltyProgramID(), currentTier) || subscriberProfileUpdated;
                      }
                  }
              } 
            else if (loyaltyProgram instanceof LoyaltyProgramChallenge)
              {
                LoyaltyProgramState lps = subscriberProfile.getLoyaltyPrograms().get(lpID);
                if (lps instanceof LoyaltyProgramChallengeState)
                  {
                    String currentLevel = ((LoyaltyProgramChallengeState) lps).getLevelName();
                    ChallengeLevel level = ((LoyaltyProgramChallenge) loyaltyProgram).getLevel(currentLevel);
                    if (level != null)
                      {
                        subscriberProfileUpdated = triggerLoyaltyWorflow(evolutionEvent, context.getSubscriberState(), level.getWorkflowDaily(), loyaltyProgram.getLoyaltyProgramID(), currentLevel) || subscriberProfileUpdated;
                      }
                  }
              }
            else if (loyaltyProgram instanceof LoyaltyProgramMission)
              {
                LoyaltyProgramState lps = subscriberProfile.getLoyaltyPrograms().get(lpID);
                if (lps instanceof LoyaltyProgramMissionState)
                  {
                    String currentStep = ((LoyaltyProgramMissionState) lps).getStepName();
                    MissionStep step = ((LoyaltyProgramMission) loyaltyProgram).getStep(currentStep);
                    if (step != null)
                      {
                        subscriberProfileUpdated = triggerLoyaltyWorflow(evolutionEvent, context.getSubscriberState(), step.getWorkflowDaily(), loyaltyProgram.getLoyaltyProgramID(), currentStep) || subscriberProfileUpdated;
                      }
                  }
              }
          }        
      }
    
    /*****************************************
    *
    *  apply retention
    *
    *****************************************/

    if (evolutionEvent instanceof TimedEvaluation && ((TimedEvaluation)evolutionEvent).getPeriodicEvaluation())
      {
        subscriberProfileUpdated = retentionService.cleanSubscriberState(context.getSubscriberState()) || subscriberProfileUpdated;
      }

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
            SupportedLanguage supportedLanguage = Deployment.getDeployment(tenantID).getSupportedLanguages().get((String) subscriberProfileForceUpdate.getParameterMap().get("language"));
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
        
        if (subscriberProfileForceUpdate.getParameterMap().containsKey("fromJourney"))
          {
            for (String attribute : subscriberProfileForceUpdate.getParameterMap().keySet())
              {
                if ("fromJourney".equals(attribute)) continue;
                Pattern attributePattern = Pattern.compile("^([^-]+)" + DELIMITER + "(.+)$"); // name --- dataType
                Matcher attributeMatcher = attributePattern.matcher(attribute);
                if (! attributeMatcher.find())
                  {
                    log.info("unable to parse id " + attribute);
                  }
                else
                  {
                    String valueStr = (String) subscriberProfileForceUpdate.getParameterMap().get(attribute);
                    Object value = null;
                    String attributeName = attributeMatcher.group(1);
                    String attributeType = attributeMatcher.group(2);
                    String methodName = "set" + attributeName.substring(0, 1).toUpperCase() + attributeName.substring(1);
                    Class<?> parameterType = null;
                    switch (CriterionDataType.fromExternalRepresentation(attributeType))
                    {
                      case StringCriterion:
                        parameterType = String.class;
                        value = valueStr;
                        break;
                      case IntegerCriterion:
                        parameterType = Integer.class;
                        value = Integer.parseInt(valueStr);
                        break;
                      case BooleanCriterion:
                        parameterType = Boolean.class;
                        value = Boolean.parseBoolean(valueStr);
                        break;
                      default:
                        log.info("unsupported dataType : " + attributeType + " " + CriterionDataType.fromExternalRepresentation(attributeType));
                    }
                    if (parameterType != null)
                      {
                        // special case for evolutionSubscriberStatus
                        if ("evolutionSubscriberStatus".equals(attributeName) && (value instanceof String))
                          {
                            EvolutionSubscriberStatus currentEvolutionSubscriberStatus = subscriberProfile.getEvolutionSubscriberStatus();
                            EvolutionSubscriberStatus updatedEvolutionSubscriberStatus = EvolutionSubscriberStatus.fromExternalRepresentation((String) value);
                            if (currentEvolutionSubscriberStatus != updatedEvolutionSubscriberStatus)
                              {
                                subscriberProfile.setEvolutionSubscriberStatus(updatedEvolutionSubscriberStatus);
                                subscriberProfile.setEvolutionSubscriberStatusChangeDate(subscriberProfileForceUpdate.getEventDate());
                                subscriberProfile.setPreviousEvolutionSubscriberStatus(currentEvolutionSubscriberStatus);
                                subscriberProfileUpdated = true;
                              }
                          }
                        else
                          {
                            try
                            {
                              Method setter = subscriberProfile.getClass().getMethod(methodName, parameterType);
                              setter.invoke(subscriberProfile, value);
                              subscriberProfileUpdated = true;
                            }
                            catch (NoSuchMethodException|SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e1)
                              {
                                // setters defined as Integer can be declared with long param (ex: setLastRechargeAmount())
                                if (Integer.class.equals(parameterType) && (value instanceof Integer))
                                  {
                                    try
                                    {
                                      Method setter = subscriberProfile.getClass().getMethod(methodName, Long.class);
                                      setter.invoke(subscriberProfile, new Long((long) (int) value));
                                      subscriberProfileUpdated = true;
                                     }
                                     catch (NoSuchMethodException|SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
                                     {
                                       log.info("unable to set profile attribute with long " + attributeName + " : " + e.getLocalizedMessage());
                                     }
                                   }
                                 else
                                   {
                                     log.info("unable to set profile attribute " + attributeName + " : " + e1.getLocalizedMessage());
                                   }
                               }
                           }
                       }
                  }
              }
          }
        
        //
        //  score
        //
        
        if (subscriberProfileForceUpdate.getParameterMap().containsKey("score"))
          {
            String challengeID = (String) subscriberProfileForceUpdate.getParameterMap().get("challengeID");
            Integer score = (Integer) subscriberProfileForceUpdate.getParameterMap().get("score");
            Boolean isPeriodChange = (Boolean) subscriberProfileForceUpdate.getParameterMap().get("isPeriodChange");
            
            int oldScore = 0;
            if (subscriberProfile.getLoyaltyPrograms() != null && !subscriberProfile.getLoyaltyPrograms().isEmpty() && subscriberProfile.getLoyaltyPrograms().get(challengeID) != null && subscriberProfile.getLoyaltyPrograms().get(challengeID) instanceof LoyaltyProgramChallengeState)
              {
                oldScore = ((LoyaltyProgramChallengeState) subscriberProfile.getLoyaltyPrograms().get(challengeID)).getCurrentScore();
              }
            
            if (challengeID != null && score != null)
              {
                subscriberProfileUpdated = updateScore(subscriberProfile, challengeID, score, now);
                if (subscriberProfileUpdated)
                  {
                    boolean periodChange = isPeriodChange != null && isPeriodChange;
                    checkForLoyaltyProgramStateChanges(context.getSubscriberState(), null, now, oldScore, periodChange);
                  }
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

            //
            //  return delivery response
            //

            context.getSubscriberState().getPointFulfillmentResponses().add(pointFulfillmentResponse);
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
            if (pointFulfillmentRequest.getValidityPeriodType() != null && !pointFulfillmentRequest.getValidityPeriodType().equals(TimeUnit.Unknown) && pointFulfillmentRequest.getValidityPeriodQuantity() != null && pointFulfillmentRequest.getValidityPeriodQuantity() > 0)
              {
                newPoint.getValidity().setPeriodType(pointFulfillmentRequest.getValidityPeriodType());
                newPoint.getValidity().setPeriodQuantity(pointFulfillmentRequest.getValidityPeriodQuantity());
              }
            
            //
            // update balance 
            //
            
            boolean success = updatePointBalance(context, pointFulfillmentResponse, pointFulfillmentRequest.getEventID(), pointFulfillmentRequest.getModuleID(), pointFulfillmentRequest.getFeatureID(), subscriberProfile, newPoint, pointFulfillmentRequest.getOperation(), pointFulfillmentRequest.getAmount(), now, false, "", tenantID);
            
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
                
                checkForLoyaltyProgramStateChanges(context.getSubscriberState(), pointFulfillmentRequest.getDeliveryRequestID(), now, null, false);
                
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

    for (SegmentationDimension segmentationDimension :  segmentationDimensionService.getActiveSegmentationDimensions(now, tenantID))
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
                SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, extendedSubscriberProfile, subscriberGroupEpochReader, now, tenantID);
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
                          CriterionField baseMetric = CriterionContext.FullProfile(tenantID).getCriterionFields(tenantID).get(variableName);
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
    for (SegmentationDimension segmentationDimension :  segmentationDimensionService.getActiveSegmentationDimensions(now, tenantID))
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

    for(Target target : targetService.getActiveTargets(now, tenantID))
      {
        SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, extendedSubscriberProfile, subscriberGroupEpochReader, now, tenantID);
        if (target.getTargetingType().equals(Target.TargetingType.Eligibility))
          {
            boolean addTarget = EvaluationCriterion.evaluateCriteria(evaluationRequest, target.getTargetingCriteria());
            subscriberProfile.setTarget(target.getTargetID(), subscriberGroupEpochReader.get(target.getTargetID()) != null ? subscriberGroupEpochReader.get(target.getTargetID()).getEpoch() : 0, addTarget);
            subscriberProfileUpdated = true;
          }          
      }
    
    /*****************************************
    *
    *  process rule based exclusionInclusion lists
    *
    *****************************************/

    for(ExclusionInclusionTarget exclusionInclusionTarget : exclusionInclusionTargetService.getActiveExclusionInclusionTargets(now, tenantID))
      {
        SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, extendedSubscriberProfile, subscriberGroupEpochReader, now, tenantID);
        if (exclusionInclusionTarget.getCriteriaList().size() > 0)
          {
            boolean addExclusionInclusionTarget = EvaluationCriterion.evaluateCriteria(evaluationRequest, exclusionInclusionTarget.getCriteriaList());
            subscriberProfile.setExclusionInclusionTarget(exclusionInclusionTarget.getExclusionInclusionTargetID(), subscriberGroupEpochReader.get(exclusionInclusionTarget.getExclusionInclusionTargetID()) != null ? subscriberGroupEpochReader.get(exclusionInclusionTarget.getExclusionInclusionTargetID()).getEpoch() : 0, addExclusionInclusionTarget);
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
    
    if (evolutionEvent instanceof PurchaseFulfillmentRequest)
      {
        PurchaseFulfillmentRequest purchaseFulfillmentRequest = (PurchaseFulfillmentRequest) evolutionEvent;
        String offerID = purchaseFulfillmentRequest.getOfferID();
        Offer offer = offerService.getActiveOffer(offerID, now);
        if (offer == null)
          {
            log.info("Got a purchase for inexistent offer " + offerID);
          }
        else
          {
            // EVPRO-1061 : For day unit, limit should be 0h on (today - number of days + 1)
            // for month unit, the limit should be on 0h on (1st day of this month - number of months + 1)
            Date earliestDateToKeep = null;
            Integer maximumAcceptancesPeriodDays = offer.getMaximumAcceptancesPeriodDays();
            Integer maximumAcceptancesPeriodMonths = offer.getMaximumAcceptancesPeriodMonths();
            if (maximumAcceptancesPeriodDays != Offer.UNSET) {
              earliestDateToKeep = RLMDateUtils.addDays(now, -maximumAcceptancesPeriodDays+1, Deployment.getDeployment(tenantID).getTimeZone());
              earliestDateToKeep = RLMDateUtils.truncate(earliestDateToKeep, Calendar.DATE, Deployment.getDeployment(tenantID).getTimeZone());
            } else if (maximumAcceptancesPeriodMonths != Offer.UNSET) {
              earliestDateToKeep = RLMDateUtils.addMonths(now, -maximumAcceptancesPeriodMonths+1, Deployment.getDeployment(tenantID).getTimeZone());
              earliestDateToKeep = RLMDateUtils.truncate(earliestDateToKeep, Calendar.MONTH, Deployment.getDeployment(tenantID).getTimeZone());
            } else {
              log.info("internal error : maximumAcceptancesPeriodDays & maximumAcceptancesPeriodMonths are both unset, using 1 day");
              earliestDateToKeep = RLMDateUtils.addDays(now, -1, Deployment.getDeployment(tenantID).getTimeZone());
            }
            log.debug("earliestDateToKeep for " + offerID + " : " + earliestDateToKeep + " maximumAcceptancesPeriodDays: " + maximumAcceptancesPeriodDays + " maximumAcceptancesPeriodMonths: " + maximumAcceptancesPeriodMonths);
            List<Date> cleanPurchaseHistory = new ArrayList<Date>();
            Map<String,List<Date>> fullPurchaseHistory = subscriberProfile.getOfferPurchaseHistory();
            List<Date> purchaseHistory = fullPurchaseHistory.get(offerID);
            if (purchaseHistory != null)
              {
                // clean list : only keep relevant purchase dates
                for (Date purchaseDate : purchaseHistory)
                  {
                    if (purchaseDate.after(earliestDateToKeep))
                      {
                        cleanPurchaseHistory.add(purchaseDate);
                      }
                  }
              }
            // TODO : this could be size-optimized by storing date/quantity in a new object
            for (int n=0; n<purchaseFulfillmentRequest.getQuantity(); n++)
              {
                cleanPurchaseHistory.add(now); // add new purchase
              }
            fullPurchaseHistory.put(offerID, cleanPurchaseHistory);
            subscriberProfileUpdated = true;
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
        refreshWindow = refreshWindow || RLMDateUtils.addDays(context.getSubscriberState().getUCGRefreshDay(), ucgState.getRefreshWindowDays(), Deployment.getDeployment(tenantID).getTimeZone()).compareTo(now) <= 0;

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
            context.getSubscriberState().setUCGState(ucgState, now, tenantID);
          }
      }

    /*****************************************
    *
    *  statistics
    *
    *****************************************/

    String eventName = (evolutionEvent instanceof EvolutionEngineEvent) ? ((EvolutionEngineEvent)evolutionEvent).getEventName() : evolutionEvent.getClass().getSimpleName();
    statsEventCounter.withLabel(StatsBuilders.LABEL.name.name(),eventName).getStats().increment();

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return subscriberProfileUpdated;
  }


  private static boolean executeActionOtherSubscriber(EvolutionEventContext evolutionEventContext, SubscriberStreamEvent evolutionEvent, int tenantID)
  {
    if(evolutionEvent instanceof ExecuteActionOtherSubscriber)
      {
        // This the request of an action (cf ActionManager)
        ExecuteActionOtherSubscriber executeActionOtherSubscriber = (ExecuteActionOtherSubscriber)evolutionEvent;
        JourneyState originalJourneyState = executeActionOtherSubscriber.getOriginatedJourneyState();
        Journey originalJourney = evolutionEventContext.getJourneyService().getActiveJourney(executeActionOtherSubscriber.getOriginalJourneyID(), evolutionEventContext.now());
        if (originalJourney != null) {
          ActionManager actionManager = null;
          JourneyNode originalJourneyNode = null;
          if(executeActionOtherSubscriber.getOriginatingNodeID() != null)
            {
              originalJourneyNode = originalJourney.getJourneyNode(executeActionOtherSubscriber.getOriginatingNodeID());
              actionManager = originalJourneyNode.getNodeType().getActionManager();
            }
          if (actionManager != null) {
            // set some data used to execute the associated actions (those data are cleaned a bit later just before return)
            evolutionEventContext.setExecuteActionOtherSubscriberDeliveryRequestID(executeActionOtherSubscriber.getOutstandingDeliveryRequestID());
            evolutionEventContext.setExecuteActionOtherUserOriginalSubscriberID(executeActionOtherSubscriber.getOriginatingSubscriberID());

	        // build new SubscriberEvaluationRequest
	        SubscriberEvaluationRequest subscriberEvaluationRequest = new SubscriberEvaluationRequest(
	            evolutionEventContext.getSubscriberState().getSubscriberProfile(),
	            (ExtendedSubscriberProfile) null,
	            subscriberGroupEpochReader,
	            originalJourneyState,
	            originalJourneyNode,
	            null,
	            null,
	            evolutionEventContext.now, tenantID);
	        List<Action> actions = actionManager.executeOnEntry(evolutionEventContext, subscriberEvaluationRequest);
	        handleExecuteOnEntryActions(evolutionEventContext.getSubscriberState(), originalJourneyState, originalJourney, actions, subscriberEvaluationRequest, evolutionEventContext.getEventID());

            // clean temporary data to avoid next nodes considering this event
            evolutionEventContext.setExecuteActionOtherSubscriberDeliveryRequestID(null);
            evolutionEventContext.setExecuteActionOtherUserOriginalSubscriberID(null);

            return true;
          } else {
            log.info("No action manager originatingSubscriberID : " + executeActionOtherSubscriber.getOriginatingSubscriberID() + " journeyID : " + executeActionOtherSubscriber.getOriginalJourneyID() + " nodeID : " + executeActionOtherSubscriber.getOriginatingNodeID());
          }
        } else {
          log.info("journeyID : " + executeActionOtherSubscriber.getOriginalJourneyID() + " cannot be found");
        }
      }
    return false;
  }

  private static void checkForLoyaltyProgramStateChanges(SubscriberState subscriberState, String deliveryRequestID, Date now, Integer oldScore, boolean periodChange)
  {
    SubscriberProfile subscriberProfile = subscriberState.getSubscriberProfile();
    for(Entry<String, LoyaltyProgramState> entry : subscriberProfile.getLoyaltyPrograms().entrySet())
      {
        //
        //  get current loyalty program state
        //

        LoyaltyProgramState loyaltyProgramState = entry.getValue();

        //
        //  check if loyalty program state needs to be updated
        //

        if(loyaltyProgramState.getLoyaltyProgramExitDate() == null && loyaltyProgramState.getLoyaltyProgramType().equals(LoyaltyProgramType.POINTS))
          {
            LoyaltyProgramPointsState loyaltyProgramPointState = (LoyaltyProgramPointsState) loyaltyProgramState;

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

                  LoyaltyProgramTierChange tierChangeType = loyaltyProgramPointState.update(loyaltyProgram.getEpoch(), LoyaltyProgramOperation.Optin, loyaltyProgram.getLoyaltyProgramName(), newTierName, now, deliveryRequestID, loyaltyProgramService);

                  //
                  //  generate new event (tier changed)
                  //

                  ParameterMap info = new ParameterMap();
                  info.put(LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation(), currentTier);
                  info.put(LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation(), newTierName);
                  info.put(LoyaltyProgramPointsEventInfos.TIER_UPDATE_TYPE.getExternalRepresentation(), tierChangeType.getExternalRepresentation());
                  ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                  subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);

                  launchChangeTierWorkflows(profileLoyaltyProgramChangeEvent, subscriberState, loyaltyProgramPoints, currentTier, newTierName, loyaltyProgramState.getLoyaltyProgramID());
                }
            }
          }
        else if (loyaltyProgramState.getLoyaltyProgramExitDate() == null && loyaltyProgramState.getLoyaltyProgramType().equals(LoyaltyProgramType.CHALLENGE))
          {
            //
            //  get challenge loyalty program 
            //
            
            LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramState.getLoyaltyProgramID(), now);
            if(loyaltyProgram != null)
              {
                LoyaltyProgramChallengeState loyaltyProgramChallengeState = (LoyaltyProgramChallengeState) loyaltyProgramState;
                String currentLevel = loyaltyProgramChallengeState.getLevelName();
                
                //
                //  determine new level
                //

                LoyaltyProgramChallenge loyaltyProgramChallenge = (LoyaltyProgramChallenge) loyaltyProgram;
                String newLevelName = determineLoyaltyProgramChallengeLevel(loyaltyProgramState, loyaltyProgramChallenge, now);
                
                //
                //  periodChange
                //
                
                boolean isPeriodChange = periodChange;
                
                //
                //  compare to current tier
                //
                
                if ((currentLevel != null && !currentLevel.equals(newLevelName)) || (currentLevel == null && newLevelName != null) || isPeriodChange)
                  {
                    //
                    //  update loyalty program state
                    //

                    LoyaltyProgramLevelChange loyaltyProgramLevelChange = loyaltyProgramChallengeState.update(loyaltyProgram.getEpoch(), LoyaltyProgramOperation.Optin, loyaltyProgram.getLoyaltyProgramName(), newLevelName, now, deliveryRequestID, loyaltyProgramService, isPeriodChange, oldScore);
                    
                    //
                    //  generate new event (tier changed)
                    //

                    ParameterMap info = new ParameterMap();
                    info.put(LoyaltyProgramChallengeEventInfos.OLD_LEVEL.getExternalRepresentation(), currentLevel);
                    info.put(LoyaltyProgramChallengeEventInfos.NEW_LEVEL.getExternalRepresentation(), newLevelName);
                    info.put(LoyaltyProgramChallengeEventInfos.LEVEL_UPDATE_TYPE.getExternalRepresentation(), loyaltyProgramLevelChange.getExternalRepresentation());
                    ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                    subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);

                    launchChangeTierWorkflows(profileLoyaltyProgramChangeEvent, subscriberState, loyaltyProgramChallenge, currentLevel, newLevelName, loyaltyProgramState.getLoyaltyProgramID());
                  }
              }
          }
      }
  }


  public static void launchChangeTierWorkflows(ProfileLoyaltyProgramChangeEvent event, SubscriberState subscriberState, LoyaltyProgram loyaltyProgram, String oldTierName, String newTierName, String featureID)
  {
    switch (loyaltyProgram.getLoyaltyProgramType())
    {
      case POINTS:
        LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints) loyaltyProgram;
        
        /* change tier should be executed only in enter EVPRO-922
        Tier oldTier = loyaltyProgramPoints.getTier(oldTierName);
        if (oldTier != null) triggerLoyaltyWorflow(event, subscriberState, oldTier.getWorkflowChange(), featureID, oldTier.getTierName());
        */

        // Enter tier workflow
        Tier newTier = loyaltyProgramPoints.getTier(newTierName);
        if (newTier != null) triggerLoyaltyWorflow(event, subscriberState, newTier.getWorkflowChange(), featureID, newTier.getTierName());
        break;
        
      case CHALLENGE:
        LoyaltyProgramChallenge loyaltyProgramChallenge = (LoyaltyProgramChallenge) loyaltyProgram;
        
        /* change tier should be executed only in enter EVPRO-922
        ChallengeLevel oldLevel = loyaltyProgramChallenge.getLevel(oldTierName);
        if (oldLevel != null) triggerLoyaltyWorflow(event, subscriberState, oldLevel.getWorkflowChange(), featureID, oldLevel.getLevelName());
        */

        // Enter tier workflow
        ChallengeLevel newLevel = loyaltyProgramChallenge.getLevel(newTierName);
        if (newLevel != null) triggerLoyaltyWorflow(event, subscriberState, newLevel.getWorkflowChange(), featureID, newLevel.getLevelName());
        break;
        
      case MISSION:
        LoyaltyProgramMission loyaltyProgramMission = (LoyaltyProgramMission) loyaltyProgram;
        
        MissionStep previousStep = loyaltyProgramMission.getStep(oldTierName);
        MissionStep currentStep = loyaltyProgramMission.getStep(newTierName);
        
        if (previousStep != null) 
          {
            triggerLoyaltyWorflow(event, subscriberState, previousStep.getWorkflowCompletion(), featureID, previousStep.getStepName());
          } 
        if (currentStep != null) 
          {
            triggerLoyaltyWorflow(event, subscriberState, currentStep.getWorkflowStepUP(), featureID, currentStep.getStepName());
          } 
        break;
        
      default:
        break;
    }
    
  }


  public static boolean triggerLoyaltyWorflow(SubscriberStreamEvent eventToTrigWorkflow, SubscriberState subscriberState, String loyaltyWorflowID, String featureID, String origin)
  {
    // 
    // Tag the subscriber state with the event's information, log a warn if a conflict appears (is the date enough to segregate 2 
    //
    
    if(loyaltyWorflowID == null) { return false; }
    
    String toBeAdded = eventToTrigWorkflow.getClass().getName() + ":" + eventToTrigWorkflow.getEventDate().getTime() + ":" + loyaltyWorflowID + ":" + featureID + ":" + DeliveryRequest.Module.Loyalty_Program.getExternalRepresentation() + ":" + origin ;
    List<String> workflowTriggering = subscriberState.getWorkflowTriggering();
    if(workflowTriggering.contains(toBeAdded))
      {
        // there is a conflict, i.e. this has already be requested, which means the date is not enough to discriminate... will see
        log.warn("triggerLoyaltyWorflow already has " + toBeAdded);
        return false;
      }
    workflowTriggering.add(toBeAdded);
    return true;
  }
  
  /*****************************************
  *
  *  updatePointBalance
  *
  *****************************************/

  private static boolean updatePointBalance(EvolutionEventContext context, PointFulfillmentRequest pointFulfillmentResponse, String eventID, String moduleID, String featureID, SubscriberProfile subscriberProfile, Point point, CommodityDeliveryOperation operation, int amount, Date now, boolean generateBDR, String tier, int tenantID)
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

    boolean success = pointBalance.update(context, pointFulfillmentResponse, eventID, moduleID, featureID, subscriberProfile.getSubscriberID(), operation, amount, point, now, generateBDR, tier, tenantID);

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
        if (loyaltyProgram != null)
          {
            if (loyaltyProgram instanceof LoyaltyProgramPoints)
              {
                LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints) loyaltyProgram;
                LoyaltyProgramPointsState loyaltyProgramPointsState = (LoyaltyProgramPointsState) loyaltyProgramState;
                if (Objects.equals(point.getPointID(), loyaltyProgramPoints.getStatusPointsID())) loyaltyProgramPointsState.setStatusPoints(pointBalance.getBalance(now));
                if (Objects.equals(point.getPointID(), loyaltyProgramPoints.getRewardPointsID())) loyaltyProgramPointsState.setRewardPoints(pointBalance.getBalance(now));
              }
          }
      }
    
    //
    //  return
    //
    
    return success;

  }
  
  
  /*****************************************
  *
  *  updateScore
  *
  *****************************************/

  private static boolean updateScore(SubscriberProfile subscriberProfile, String loyaltyChallengeID, int amount, Date now)
  {
    boolean success = false;
    
    LoyaltyProgramState loyaltyProgramState = subscriberProfile.getLoyaltyPrograms().get(loyaltyChallengeID);
    LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(loyaltyChallengeID, now);
    
    if (loyaltyProgramState != null && loyaltyProgramState.getLoyaltyProgramExitDate() == null)
      {
        if (loyaltyProgram instanceof LoyaltyProgramChallenge && loyaltyProgramState instanceof LoyaltyProgramChallengeState)
          {
            LoyaltyProgramChallengeState loyaltyProgramChallengeState = (LoyaltyProgramChallengeState) loyaltyProgramState;
            
            //
            // get current score
            //
            
            int score = loyaltyProgramChallengeState.getCurrentScore();
            
            //
            //  update
            //
            
            score = score + amount;
            
            success = score >= 0;
            
            if (success)
              {
                //
                //  update score
                //
                
                loyaltyProgramChallengeState.setCurrentScore(score);
                loyaltyProgramChallengeState.setLastScoreChangeDate(now);
              }
            else
              {
                if (log.isErrorEnabled()) log.error("updateScore failed -> score can not be -ve {} in Challenge {} for subsciberID {}", score, loyaltyProgram.getGUIManagedObjectDisplay(), subscriberProfile.getSubscriberID());
              }
          }
        else
          {
            if (log.isErrorEnabled()) log.error("updateScore failed -> invalid loyaltyProgram {}", loyaltyProgram.getGUIManagedObjectDisplay());
          }
      }
    else
      {
        if (log.isErrorEnabled()) log.error("updateScore failed -> customer is not in loyaltyProgram {}", loyaltyProgram.getGUIManagedObjectDisplay());
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

  private static boolean updateSubscriberLoyaltyProgram(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent, int tenantID)
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
        else
          {

            boolean success = false;

            switch (loyaltyProgram.getLoyaltyProgramType())
            {
              case POINTS:
                LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints) loyaltyProgram;
                switch (loyaltyProgramRequest.getOperation())
                {
                  case Optin:

                    //
                    // determine tier
                    //

                    String newTierName = determineLoyaltyProgramPointsTier(subscriberProfile, loyaltyProgramPoints, now);

                    //
                    // get current loyalty program state
                    //

                    LoyaltyProgramState currentLoyaltyProgramState = subscriberProfile.getLoyaltyPrograms().get(loyaltyProgramRequest.getLoyaltyProgramID());
                    if (currentLoyaltyProgramState == null)
                      {
                        LoyaltyProgramHistory loyaltyProgramHistory = new LoyaltyProgramHistory(loyaltyProgram.getLoyaltyProgramID());
                        currentLoyaltyProgramState = new LoyaltyProgramPointsState(LoyaltyProgramType.POINTS, loyaltyProgram.getEpoch(), loyaltyProgram.getLoyaltyProgramName(), loyaltyProgram.getLoyaltyProgramID(), now, null, newTierName, null, now, loyaltyProgramHistory);

                        //
                        // update loyalty program state
                        //

                        if (log.isDebugEnabled()) log.debug("new loyaltyProgramRequest : for subscriber '" + subscriberProfile.getSubscriberID() + "' : loyaltyProgramState.update(" + loyaltyProgram.getEpoch() + ", " + loyaltyProgramRequest.getOperation() + ", " + loyaltyProgram.getLoyaltyProgramName() + ", " + newTierName + ", " + now + ", " + loyaltyProgramRequest.getDeliveryRequestID() + ")");
                        LoyaltyProgramTierChange tierChangeType = ((LoyaltyProgramPointsState) currentLoyaltyProgramState).update(loyaltyProgram.getEpoch(), loyaltyProgramRequest.getOperation(), loyaltyProgram.getLoyaltyProgramName(), newTierName, now, loyaltyProgramRequest.getDeliveryRequestID(), loyaltyProgramService);

                        //
                        // generate new event (opt-in)
                        //

                        ParameterMap infos = new ParameterMap();
                        infos.put(LoyaltyProgramPointsEventInfos.ENTERING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                        infos.put(LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation(), null);
                        infos.put(LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation(), newTierName);
                        infos.put(LoyaltyProgramPointsEventInfos.TIER_UPDATE_TYPE.getExternalRepresentation(), tierChangeType.getExternalRepresentation());
                        ProfileLoyaltyProgramChangeEvent profileChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), infos);
                        subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileChangeEvent);

                        launchChangeTierWorkflows(profileChangeEvent, subscriberState, loyaltyProgramPoints, null, newTierName, currentLoyaltyProgramState.getLoyaltyProgramID());
                      } 
                    else if (currentLoyaltyProgramState instanceof LoyaltyProgramPointsState)
                      {
                        //
                        // get current tier
                        //

                        LoyaltyProgramPointsState loyaltyProgramPointsState = ((LoyaltyProgramPointsState) currentLoyaltyProgramState);
                        String currentTier = loyaltyProgramPointsState.getTierName();

                        //
                        //
                        //

                        if ((currentTier != null && !currentTier.equals(newTierName)) || (currentTier == null && newTierName != null))
                          {

                            //
                            // update loyalty program state
                            //

                            LoyaltyProgramTierChange tierChangeType = loyaltyProgramPointsState.update(loyaltyProgram.getEpoch(), LoyaltyProgramOperation.Optin, loyaltyProgram.getLoyaltyProgramName(), newTierName, now, loyaltyProgramRequest.getDeliveryRequestID(), loyaltyProgramService);

                            //
                            // generate new event (tier changed)
                            //

                            ParameterMap info = new ParameterMap();
                            info.put(LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation(), currentTier);
                            info.put(LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation(), newTierName);
                            info.put(LoyaltyProgramPointsEventInfos.TIER_UPDATE_TYPE.getExternalRepresentation(), tierChangeType.getExternalRepresentation());
                            ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                            subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);

                            launchChangeTierWorkflows(profileLoyaltyProgramChangeEvent, subscriberState, loyaltyProgramPoints, currentTier, newTierName, currentLoyaltyProgramState.getLoyaltyProgramID());
                          }
                      }

                    //
                    // update subscriber state - return
                    //

                    subscriberProfile.getLoyaltyPrograms().put(loyaltyProgramRequest.getLoyaltyProgramID(), currentLoyaltyProgramState);
                    success = true;

                    break;

                  case Optout:

                    //
                    // determine tier
                    //

                    String tierName = null;

                    //
                    // get current loyalty program state
                    //

                    LoyaltyProgramState loyaltyProgramState = subscriberProfile.getLoyaltyPrograms().get(loyaltyProgramRequest.getLoyaltyProgramID());

                    if (loyaltyProgramState == null)
                      {
                        LoyaltyProgramHistory loyaltyProgramHistory = new LoyaltyProgramHistory(loyaltyProgram.getLoyaltyProgramID());
                        loyaltyProgramState = new LoyaltyProgramPointsState(LoyaltyProgramType.POINTS, loyaltyProgram.getEpoch(), loyaltyProgram.getLoyaltyProgramName(), loyaltyProgram.getLoyaltyProgramID(), now, null, tierName, null, now, loyaltyProgramHistory);
                      }

                    String oldTier = ((LoyaltyProgramPointsState) loyaltyProgramState).getTierName();

                    //
                    // update loyalty program state
                    //

                    if (log.isDebugEnabled()) log.debug("new loyaltyProgramRequest : for subscriber '" + subscriberProfile.getSubscriberID() + "' : loyaltyProgramState.update(" + loyaltyProgram.getEpoch() + ", " + loyaltyProgramRequest.getOperation() + ", " + loyaltyProgram.getLoyaltyProgramName() + ", " + tierName + ", " + now + ", " + loyaltyProgramRequest.getDeliveryRequestID() + ")");
                    LoyaltyProgramTierChange tierChangeType = ((LoyaltyProgramPointsState) loyaltyProgramState).update(loyaltyProgram.getEpoch(), loyaltyProgramRequest.getOperation(), loyaltyProgram.getLoyaltyProgramName(), tierName, now, loyaltyProgramRequest.getDeliveryRequestID(), loyaltyProgramService);

                    //
                    // update subscriber loyalty programs state
                    //

                    subscriberProfile.getLoyaltyPrograms().put(loyaltyProgramRequest.getLoyaltyProgramID(), loyaltyProgramState);

                    //
                    // generate new event (opt-out)
                    //

                    ParameterMap info = new ParameterMap();
                    info.put(LoyaltyProgramPointsEventInfos.LEAVING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                    info.put(LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation(), oldTier);
                    info.put(LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation(), null);
                    info.put(LoyaltyProgramPointsEventInfos.TIER_UPDATE_TYPE.getExternalRepresentation(), tierChangeType.getExternalRepresentation());
                    ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                    subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);

                    launchChangeTierWorkflows(profileLoyaltyProgramChangeEvent, subscriberState, loyaltyProgramPoints, oldTier, null, loyaltyProgram.getLoyaltyProgramID());

                    //
                    // return
                    //

                    success = true;
                    break;

                  default:
                    break;
                }
                break;
                
              case CHALLENGE:
                LoyaltyProgramChallenge loyaltyProgramChallenge = (LoyaltyProgramChallenge) loyaltyProgram;
                switch (loyaltyProgramRequest.getOperation())
                {
                  case Optin:
                    LoyaltyProgramState currentLoyaltyProgramState = subscriberProfile.getLoyaltyPrograms().get(loyaltyProgramRequest.getLoyaltyProgramID());
                    String newLevelName = determineLoyaltyProgramChallengeLevel(currentLoyaltyProgramState, loyaltyProgramChallenge, now);
                    if (currentLoyaltyProgramState == null)
                      {
                        //
                        //  LoyaltyProgramChallengeHistory
                        //
                        
                        LoyaltyProgramChallengeHistory loyaltyProgramChallengeHistory = new LoyaltyProgramChallengeHistory(loyaltyProgram.getLoyaltyProgramID());
                        currentLoyaltyProgramState = new LoyaltyProgramChallengeState(LoyaltyProgramType.CHALLENGE, loyaltyProgram.getEpoch(), loyaltyProgram.getLoyaltyProgramName(), loyaltyProgram.getLoyaltyProgramID(), now, null, newLevelName, null, now, loyaltyProgramChallengeHistory);
                        if (log.isDebugEnabled()) log.debug("new loyaltyProgramRequest : for subscriber '" + subscriberProfile.getSubscriberID() + "' : loyaltyProgramState.update(" + loyaltyProgram.getEpoch() + ", " + loyaltyProgramRequest.getOperation() + ", " + loyaltyProgram.getLoyaltyProgramName() + ", " + newLevelName + ", " + now + ", " + loyaltyProgramRequest.getDeliveryRequestID() + ")");
                        LoyaltyProgramLevelChange levelChangeType = ((LoyaltyProgramChallengeState) currentLoyaltyProgramState).update(loyaltyProgram.getEpoch(), loyaltyProgramRequest.getOperation(), loyaltyProgram.getLoyaltyProgramName(), newLevelName, now, loyaltyProgramRequest.getDeliveryRequestID(), loyaltyProgramService);

                        //
                        // generate new event (opt-in)
                        //

                        ParameterMap infos = new ParameterMap();
                        infos.put(LoyaltyProgramChallengeEventInfos.ENTERING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                        infos.put(LoyaltyProgramChallengeEventInfos.OLD_LEVEL.getExternalRepresentation(), null);
                        infos.put(LoyaltyProgramChallengeEventInfos.NEW_LEVEL.getExternalRepresentation(), newLevelName);
                        infos.put(LoyaltyProgramChallengeEventInfos.LEVEL_UPDATE_TYPE.getExternalRepresentation(), levelChangeType.getExternalRepresentation());
                        ProfileLoyaltyProgramChangeEvent profileChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), infos);
                        subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileChangeEvent);
                        
                        //
                        //  launchChangeTierWorkflows
                        //

                        launchChangeTierWorkflows(profileChangeEvent, subscriberState, loyaltyProgramChallenge, null, newLevelName, currentLoyaltyProgramState.getLoyaltyProgramID());
                      } 
                    else if (currentLoyaltyProgramState instanceof LoyaltyProgramChallengeState)
                      {
                        //
                        // get current tier
                        //

                        LoyaltyProgramChallengeState loyaltyProgramChallengeState = ((LoyaltyProgramChallengeState) currentLoyaltyProgramState);
                        if (loyaltyProgramChallengeState.getLoyaltyProgramExitDate() != null)
                          {
                            String currentLevel = loyaltyProgramChallengeState.getLevelName();
                            if ((currentLevel != null && !currentLevel.equals(newLevelName)) || (currentLevel == null && newLevelName != null))
                              {

                                //
                                // update loyalty program state
                                //

                                LoyaltyProgramLevelChange loyaltyProgramLevelChange = loyaltyProgramChallengeState.update(loyaltyProgram.getEpoch(), LoyaltyProgramOperation.Optin, loyaltyProgram.getLoyaltyProgramName(), newLevelName, now, loyaltyProgramRequest.getDeliveryRequestID(), loyaltyProgramService);

                                //
                                // generate new event (tier changed)
                                //

                                ParameterMap info = new ParameterMap();
                                info.put(LoyaltyProgramChallengeEventInfos.ENTERING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                                info.put(LoyaltyProgramChallengeEventInfos.OLD_LEVEL.getExternalRepresentation(), currentLevel);
                                info.put(LoyaltyProgramChallengeEventInfos.NEW_LEVEL.getExternalRepresentation(), newLevelName);
                                info.put(LoyaltyProgramChallengeEventInfos.LEVEL_UPDATE_TYPE.getExternalRepresentation(), loyaltyProgramLevelChange.getExternalRepresentation());
                                ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                                subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);
                                launchChangeTierWorkflows(profileLoyaltyProgramChangeEvent, subscriberState, loyaltyProgramChallenge, currentLevel, newLevelName, currentLoyaltyProgramState.getLoyaltyProgramID());
                              }
                            else
                              {
                                loyaltyProgramChallengeState.setLoyaltyProgramExitDate(null);
                                LoyaltyProgramLevelChange loyaltyProgramLevelChange = LoyaltyProgramLevelChange.NoChange;
                                ParameterMap info = new ParameterMap();
                                info.put(LoyaltyProgramChallengeEventInfos.ENTERING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                                info.put(LoyaltyProgramChallengeEventInfos.OLD_LEVEL.getExternalRepresentation(), currentLevel);
                                info.put(LoyaltyProgramChallengeEventInfos.NEW_LEVEL.getExternalRepresentation(), newLevelName);
                                info.put(LoyaltyProgramChallengeEventInfos.LEVEL_UPDATE_TYPE.getExternalRepresentation(), loyaltyProgramLevelChange.getExternalRepresentation());
                                ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                                subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);
                              }
                            
                          }
                      }

                    //
                    // update subscriber state - return
                    //

                    subscriberProfile.getLoyaltyPrograms().put(loyaltyProgramRequest.getLoyaltyProgramID(), currentLoyaltyProgramState);
                    success = true;
                    break;

                  case Optout:
                    String levelName = null;
                    LoyaltyProgramState loyaltyProgramState = subscriberProfile.getLoyaltyPrograms().get(loyaltyProgramRequest.getLoyaltyProgramID());
                    if (loyaltyProgramState == null)
                      {
                        LoyaltyProgramChallengeHistory loyaltyProgramChallengeHistory = new LoyaltyProgramChallengeHistory(loyaltyProgram.getLoyaltyProgramID());
                        loyaltyProgramState = new LoyaltyProgramChallengeState(LoyaltyProgramType.CHALLENGE, loyaltyProgram.getEpoch(), loyaltyProgram.getLoyaltyProgramName(), loyaltyProgram.getLoyaltyProgramID(), now, null, levelName, null, now, loyaltyProgramChallengeHistory);
                      }

                    LoyaltyProgramLevelChange loyaltyProgramLevelChange = LoyaltyProgramLevelChange.NoChange;
                    String oldLevel = ((LoyaltyProgramChallengeState) loyaltyProgramState).getLevelName();
                    if (oldLevel != null)
                      {
                        loyaltyProgramLevelChange = ((LoyaltyProgramChallengeState) loyaltyProgramState).update(loyaltyProgram.getEpoch(), loyaltyProgramRequest.getOperation(), loyaltyProgram.getLoyaltyProgramName(), levelName, now, loyaltyProgramRequest.getDeliveryRequestID(), loyaltyProgramService);

                      }
                    else
                      {
                        loyaltyProgramState.setLoyaltyProgramExitDate(now);
                        ((LoyaltyProgramChallengeState) loyaltyProgramState).setCurrentScore(0);
                      }
                    
                    //
                    // update subscriber loyalty programs state
                    //

                    subscriberProfile.getLoyaltyPrograms().put(loyaltyProgramRequest.getLoyaltyProgramID(), loyaltyProgramState);

                    //
                    // generate new event (opt-out)
                    //

                    ParameterMap info = new ParameterMap();
                    info.put(LoyaltyProgramChallengeEventInfos.LEAVING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                    info.put(LoyaltyProgramChallengeEventInfos.OLD_LEVEL.getExternalRepresentation(), oldLevel);
                    info.put(LoyaltyProgramChallengeEventInfos.NEW_LEVEL.getExternalRepresentation(), null);
                    info.put(LoyaltyProgramChallengeEventInfos.LEVEL_UPDATE_TYPE.getExternalRepresentation(), loyaltyProgramLevelChange.getExternalRepresentation());
                    ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                    subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);
                    launchChangeTierWorkflows(profileLoyaltyProgramChangeEvent, subscriberState, loyaltyProgramChallenge, oldLevel, null, loyaltyProgram.getLoyaltyProgramID());

                    //
                    // return
                    //

                    success = true;
                    break;

                  default:
                    break;
                }
                break;
                
              case MISSION:
                LoyaltyProgramMission loyaltyProgramMission = (LoyaltyProgramMission) loyaltyProgram;
                switch (loyaltyProgramRequest.getOperation())
                {
                  case Optin:
                    
                    if (MissionSchedule.FIXDURATION == MissionSchedule.fromExternalRepresentation(loyaltyProgramMission.getScheduleType()))
                      {
                        String tz = Deployment.getDeployment(loyaltyProgramMission.getTenantID()).getTimeZone();
                        Date entryEndDate = loyaltyProgramMission.getEntryEndDate();
                        if (log.isDebugEnabled()) log.debug("entry date for mission {} is {}, subscriberID {}", loyaltyProgramMission.getGUIManagedObjectDisplay(), RLMDateUtils.formatDateForREST(entryEndDate, tz), subscriberProfile.getSubscriberID());
                        if (now.after(entryEndDate))
                          {
                            if (log.isDebugEnabled()) log.debug("entry date over for mission {}", loyaltyProgramMission.getGUIManagedObjectDisplay());
                            break;
                          }
                      }
                    
                    LoyaltyProgramState currentLoyaltyProgramState = subscriberProfile.getLoyaltyPrograms().get(loyaltyProgramRequest.getLoyaltyProgramID());
                    String newStepName = loyaltyProgramMission.getFirstStep().getStepName(); //determineLoyaltyProgramChallengeLevel(currentLoyaltyProgramState, loyaltyProgramMission, now);
                    if (currentLoyaltyProgramState == null)
                      {
                        //
                        //  LoyaltyProgramMissionHistory
                        //
                        
                        LoyaltyProgramMissionHistory loyaltyProgramMissionHistory = new LoyaltyProgramMissionHistory(loyaltyProgram.getLoyaltyProgramID());
                        currentLoyaltyProgramState = new LoyaltyProgramMissionState(LoyaltyProgramType.MISSION, loyaltyProgram.getEpoch(), loyaltyProgram.getLoyaltyProgramName(), loyaltyProgram.getLoyaltyProgramID(), now, null, newStepName, null, now, Double.valueOf(0.0), loyaltyProgramMissionHistory, false);
                        if (log.isDebugEnabled()) log.debug("new loyaltyProgramRequest : for subscriber '" + subscriberProfile.getSubscriberID() + "' : loyaltyProgramState.update(" + loyaltyProgram.getEpoch() + ", " + loyaltyProgramRequest.getOperation() + ", " + loyaltyProgram.getLoyaltyProgramName() + ", " + newStepName + ", " + now + ", " + loyaltyProgramRequest.getDeliveryRequestID() + ")");
                        LoyaltyProgramStepChange stepChangeType = ((LoyaltyProgramMissionState) currentLoyaltyProgramState).update(loyaltyProgram.getEpoch(), loyaltyProgramRequest.getOperation(), loyaltyProgram.getLoyaltyProgramName(), newStepName, now, loyaltyProgramRequest.getDeliveryRequestID(), loyaltyProgramService);

                        //
                        // generate new event (opt-in)
                        //

                        ParameterMap infos = new ParameterMap();
                        infos.put(LoyaltyProgramMissionEventInfos.ENTERING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                        infos.put(LoyaltyProgramMissionEventInfos.OLD_STEP.getExternalRepresentation(), null);
                        infos.put(LoyaltyProgramMissionEventInfos.NEW_STEP.getExternalRepresentation(), newStepName);
                        infos.put(LoyaltyProgramMissionEventInfos.STEP_UPDATE_TYPE.getExternalRepresentation(), stepChangeType.getExternalRepresentation());
                        ProfileLoyaltyProgramChangeEvent profileChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), infos);
                        subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileChangeEvent);
                        
                        //
                        //  launchChangeTierWorkflows
                        //

                        launchChangeTierWorkflows(profileChangeEvent, subscriberState, loyaltyProgramMission, null, newStepName, currentLoyaltyProgramState.getLoyaltyProgramID());
                      } 
                    else if (currentLoyaltyProgramState instanceof LoyaltyProgramMissionState)
                      {
                        //
                        // get current tier
                        //

                        LoyaltyProgramMissionState loyaltyProgramMissionState = ((LoyaltyProgramMissionState) currentLoyaltyProgramState);
                        if (loyaltyProgramMissionState.getLoyaltyProgramExitDate() != null)
                          {
                            String currentStep = loyaltyProgramMissionState.getStepName();
                            if ((currentStep != null && !currentStep.equals(newStepName)) || (currentStep == null && newStepName != null))
                              {

                                //
                                // update loyalty program state
                                //

                                LoyaltyProgramStepChange loyaltyProgramStepChange = loyaltyProgramMissionState.update(loyaltyProgram.getEpoch(), LoyaltyProgramOperation.Optin, loyaltyProgram.getLoyaltyProgramName(), newStepName, now, loyaltyProgramRequest.getDeliveryRequestID(), loyaltyProgramService);

                                //
                                // generate new event (tier changed)
                                //

                                ParameterMap info = new ParameterMap();
                                info.put(LoyaltyProgramMissionEventInfos.ENTERING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                                info.put(LoyaltyProgramMissionEventInfos.OLD_STEP.getExternalRepresentation(), currentStep);
                                info.put(LoyaltyProgramMissionEventInfos.NEW_STEP.getExternalRepresentation(), newStepName);
                                info.put(LoyaltyProgramMissionEventInfos.STEP_UPDATE_TYPE.getExternalRepresentation(), loyaltyProgramStepChange.getExternalRepresentation());
                                ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                                subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);
                                launchChangeTierWorkflows(profileLoyaltyProgramChangeEvent, subscriberState, loyaltyProgramMission, currentStep, newStepName, currentLoyaltyProgramState.getLoyaltyProgramID());
                              }
                            else
                              {
                                loyaltyProgramMissionState.setLoyaltyProgramExitDate(null);
                                LoyaltyProgramStepChange loyaltyProgramStepChange = LoyaltyProgramStepChange.NoChange;
                                ParameterMap info = new ParameterMap();
                                info.put(LoyaltyProgramMissionEventInfos.ENTERING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                                info.put(LoyaltyProgramMissionEventInfos.OLD_STEP.getExternalRepresentation(), currentStep);
                                info.put(LoyaltyProgramMissionEventInfos.NEW_STEP.getExternalRepresentation(), newStepName);
                                info.put(LoyaltyProgramMissionEventInfos.STEP_UPDATE_TYPE.getExternalRepresentation(), loyaltyProgramStepChange.getExternalRepresentation());
                                ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                                subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);
                              }
                          }
                      }

                    //
                    // update subscriber state - return
                    //

                    subscriberProfile.getLoyaltyPrograms().put(loyaltyProgramRequest.getLoyaltyProgramID(), currentLoyaltyProgramState);
                    success = true;
                    break;

                  case Optout:
                    String stepName = null;
                    LoyaltyProgramState loyaltyProgramState = subscriberProfile.getLoyaltyPrograms().get(loyaltyProgramRequest.getLoyaltyProgramID());
                    if (loyaltyProgramState == null)
                      {
                        LoyaltyProgramMissionHistory loyaltyProgramMissionHistory = new LoyaltyProgramMissionHistory(loyaltyProgram.getLoyaltyProgramID());
                        loyaltyProgramState = new LoyaltyProgramMissionState(LoyaltyProgramType.MISSION, loyaltyProgram.getEpoch(), loyaltyProgram.getLoyaltyProgramName(), loyaltyProgram.getLoyaltyProgramID(), now, now, stepName, null, now, Double.valueOf(0.0), loyaltyProgramMissionHistory, false);
                      }

                    LoyaltyProgramStepChange loyaltyProgramStepChange = LoyaltyProgramStepChange.NoChange;
                    String oldStep = ((LoyaltyProgramMissionState) loyaltyProgramState).getStepName();
                    if (oldStep != null)
                      {
                        loyaltyProgramStepChange = ((LoyaltyProgramMissionState) loyaltyProgramState).update(loyaltyProgram.getEpoch(), loyaltyProgramRequest.getOperation(), loyaltyProgram.getLoyaltyProgramName(), stepName, now, loyaltyProgramRequest.getDeliveryRequestID(), loyaltyProgramService);
                      }
                    else
                      {
                        loyaltyProgramState.setLoyaltyProgramExitDate(now);
                      }
                    
                    //
                    // update subscriber loyalty programs state
                    //

                    subscriberProfile.getLoyaltyPrograms().put(loyaltyProgramRequest.getLoyaltyProgramID(), loyaltyProgramState);

                    //
                    // generate new event (opt-out)
                    //

                    ParameterMap info = new ParameterMap();
                    info.put(LoyaltyProgramMissionEventInfos.LEAVING.getExternalRepresentation(), loyaltyProgramRequest.getLoyaltyProgramID());
                    info.put(LoyaltyProgramMissionEventInfos.OLD_STEP.getExternalRepresentation(), oldStep);
                    info.put(LoyaltyProgramMissionEventInfos.NEW_STEP.getExternalRepresentation(), null);
                    info.put(LoyaltyProgramMissionEventInfos.STEP_UPDATE_TYPE.getExternalRepresentation(), loyaltyProgramStepChange.getExternalRepresentation());
                    ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                    subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);
                    launchChangeTierWorkflows(profileLoyaltyProgramChangeEvent, subscriberState, loyaltyProgramMission, oldStep, null, loyaltyProgram.getLoyaltyProgramID());

                    //
                    // return
                    //

                    success = true;
                    break;

                  default:
                    break;
                }
                break;

              default:
                break;
            }

            //
            // response
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
            // return delivery response
            //

            context.getSubscriberState().getLoyaltyProgramResponses().add(loyaltyProgramResponse);

            //
            // subscriberProfileUpdated
            //

            subscriberProfileUpdated = true;
          }

      }
    else 
      {
        
        /*****************************************
        *
        *  update loyalty program
        *
        *****************************************/
      
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
          
          if(loyaltyProgram != null && loyaltyProgram.getLoyaltyProgramType().equals(loyaltyProgramState.getLoyaltyProgramType()) && loyaltyProgramState.getLoyaltyProgramExitDate() == null){
              
              switch (loyaltyProgram.getLoyaltyProgramType()) 
              {
                case POINTS:

                  //
                  // get loyaltyProgramPoints
                  //

                  LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints) loyaltyProgram;

                  //
                  // get subscriber current tier
                  //

                  String oldTier = ((LoyaltyProgramPointsState) loyaltyProgramState).getTierName();
                  Tier subscriberCurrentTierDefinition = loyaltyProgramPoints.getTier(oldTier);
                  if (subscriberCurrentTierDefinition != null)
                    {

                      //
                      // update loyalty program status
                      //
                      EvolutionEngineEventDeclaration statusEventDeclaration = Deployment.getEvolutionEngineEvents().get(subscriberCurrentTierDefinition.getStatusEventName());
                      if (statusEventDeclaration != null && statusEventDeclaration.getEventClassName().equals(evolutionEvent.getClass().getName()) && evolutionEvent instanceof LoyaltyProgramPointsEvent && ((LoyaltyProgramPointsEvent) evolutionEvent).getUnit() != 0)
                        {

                      //
                      //  update status points
                      //
                      Point point = pointService.getActivePoint(loyaltyProgramPoints.getStatusPointsID(), now);
                      if(point != null)
                        {

                          if(log.isDebugEnabled()) log.debug("update loyalty program STATUS => adding "+((LoyaltyProgramPointsEvent)evolutionEvent).getUnit()+" x "+subscriberCurrentTierDefinition.getNumberOfStatusPointsPerUnit()+" of point "+point.getPointName());
                          int amount = ((LoyaltyProgramPointsEvent)evolutionEvent).getUnit() * subscriberCurrentTierDefinition.getNumberOfStatusPointsPerUnit();
                          updatePointBalance(context, null, statusEventDeclaration.getEventClassName(), Module.Loyalty_Program.getExternalRepresentation(), loyaltyProgram.getLoyaltyProgramID(), subscriberProfile, point, CommodityDeliveryOperation.Credit, amount, now, true, oldTier, tenantID);
                          triggerLoyaltyWorflow(evolutionEvent, subscriberState, subscriberCurrentTierDefinition.getWorkflowStatus(), loyaltyProgramID, subscriberCurrentTierDefinition.getTierName());
                          subscriberProfileUpdated = true;
                        }
                      else
                        {
                          log.info("update loyalty program STATUS : point with ID '"+loyaltyProgramPoints.getStatusPointsID()+"' not found");
                        }

                      //
                      //  update tier
                      //
                      
                      String newTier = determineLoyaltyProgramPointsTier(subscriberProfile, loyaltyProgramPoints, now);
                      if(!oldTier.equals(newTier)){
                        LoyaltyProgramTierChange tierChangeType = ((LoyaltyProgramPointsState)loyaltyProgramState).update(loyaltyProgram.getEpoch(), LoyaltyProgramOperation.Optin, loyaltyProgram.getLoyaltyProgramName(), newTier, now, evolutionEvent.getClass().getName(),loyaltyProgramService);
                        
                        //
                        //  generate new event (tier changed)
                        //
                        
                        ParameterMap info = new ParameterMap();
                        info.put(LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation(), oldTier);
                        info.put(LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation(), newTier);
                        info.put(LoyaltyProgramPointsEventInfos.TIER_UPDATE_TYPE.getExternalRepresentation(), tierChangeType.getExternalRepresentation());
                        ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                        subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);
                        launchChangeTierWorkflows(profileLoyaltyProgramChangeEvent, subscriberState, loyaltyProgramPoints, oldTier, newTier, loyaltyProgramState.getLoyaltyProgramID());
                      }

                        }

                      //
                      // update loyalty program reward
                      //

                      EvolutionEngineEventDeclaration rewardEventDeclaration = Deployment.getEvolutionEngineEvents().get(subscriberCurrentTierDefinition.getRewardEventName());
                      if (rewardEventDeclaration != null && rewardEventDeclaration.getEventClassName().equals(evolutionEvent.getClass().getName()) && evolutionEvent instanceof LoyaltyProgramPointsEvent && ((LoyaltyProgramPointsEvent) evolutionEvent).getUnit() != 0)
                        {

                          // update reward points

                          Point point = pointService.getActivePoint(loyaltyProgramPoints.getRewardPointsID(), now);
                          if (point != null)
                            {
                              if (log.isDebugEnabled()) log.debug("update loyalty program REWARD => adding " + ((LoyaltyProgramPointsEvent) evolutionEvent).getUnit() + " x " + subscriberCurrentTierDefinition.getNumberOfRewardPointsPerUnit() + " of point with ID " + loyaltyProgramPoints.getRewardPointsID());
                              int amount = ((LoyaltyProgramPointsEvent) evolutionEvent).getUnit() * subscriberCurrentTierDefinition.getNumberOfRewardPointsPerUnit();
                              updatePointBalance(context, null, rewardEventDeclaration.getEventClassName(), Module.Loyalty_Program.getExternalRepresentation(), loyaltyProgram.getLoyaltyProgramID(), subscriberProfile, point, CommodityDeliveryOperation.Credit, amount, now, true, oldTier, tenantID);

                              // TODO Previous call might have changed tier -> do we need to generate tier
                              // changed event + trigger workflow for tier change ?
                              triggerLoyaltyWorflow(evolutionEvent, subscriberState, subscriberCurrentTierDefinition.getWorkflowReward(), loyaltyProgramID, oldTier);
                              subscriberProfileUpdated = true;
                            } 
                          else
                            {
                              log.info("update loyalty program STATUS : point with ID '" + loyaltyProgramPoints.getRewardPointsID() + "' not found");
                            }
                        }
                    }
                  break;
                  
                case CHALLENGE:
                  
                  //
                  // get loyaltyProgramChallenge
                  //

                  LoyaltyProgramChallenge loyaltyProgramChallenge = (LoyaltyProgramChallenge) loyaltyProgram;

                  //
                  // get subscriber current level
                  //

                  String oldLevel = ((LoyaltyProgramChallengeState) loyaltyProgramState).getLevelName();
                  ChallengeLevel subscriberCurrentLevelDefinition = loyaltyProgramChallenge.getLevel(oldLevel);
                  if (subscriberCurrentLevelDefinition != null)
                    {

                      //
                      // update loyalty program status
                      //

                      EvolutionEngineEventDeclaration scoreEventDeclaration = Deployment.getEvolutionEngineEvents().get(subscriberCurrentLevelDefinition.getScoreEventName());
                      if (scoreEventDeclaration != null && scoreEventDeclaration.getEventClassName().equals(evolutionEvent.getClass().getName()) && evolutionEvent instanceof LoyaltyProgramPointsEvent && ((LoyaltyProgramPointsEvent) evolutionEvent).getScoreUnit() != 0)
                        {

                          if (log.isDebugEnabled()) log.debug("update loyalty program Score => adding " + ((LoyaltyProgramPointsEvent) evolutionEvent).getScoreUnit() + " x " + subscriberCurrentLevelDefinition.getNumberOfscorePerEvent() + " of score " + loyaltyProgram.getGUIManagedObjectDisplay());
                          int amount = ((LoyaltyProgramPointsEvent) evolutionEvent).getScoreUnit() * subscriberCurrentLevelDefinition.getNumberOfscorePerEvent();
                          updateScore(subscriberProfile, loyaltyProgram.getLoyaltyProgramID(), amount, now);
                          triggerLoyaltyWorflow(evolutionEvent, subscriberState, subscriberCurrentLevelDefinition.getWorkflowScore(), loyaltyProgramID, oldLevel);
                          subscriberProfileUpdated = true;

                          //
                          // update tier
                          //

                          String newLevel = determineLoyaltyProgramChallengeLevel(loyaltyProgramState, loyaltyProgramChallenge, now);
                          if (!oldLevel.equals(newLevel))
                            {
                              LoyaltyProgramLevelChange levelChangeType = ((LoyaltyProgramChallengeState) loyaltyProgramState).update(loyaltyProgram.getEpoch(), LoyaltyProgramOperation.Optin, loyaltyProgram.getLoyaltyProgramName(), newLevel, now, evolutionEvent.getClass().getName(), loyaltyProgramService);

                              //
                              // generate new event (level changed)
                              //

                              ParameterMap info = new ParameterMap();
                              info.put(LoyaltyProgramChallengeEventInfos.OLD_LEVEL.getExternalRepresentation(), oldLevel);
                              info.put(LoyaltyProgramChallengeEventInfos.NEW_LEVEL.getExternalRepresentation(), newLevel);
                              info.put(LoyaltyProgramChallengeEventInfos.LEVEL_UPDATE_TYPE.getExternalRepresentation(), levelChangeType.getExternalRepresentation());
                              ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                              subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);
                              launchChangeTierWorkflows(profileLoyaltyProgramChangeEvent, subscriberState, loyaltyProgramChallenge, oldLevel, newLevel, loyaltyProgramState.getLoyaltyProgramID());
                            }
                        }
                    }
                  break;
                  
                case MISSION:
                  
                  //
                  // get loyaltyProgramMission
                  //

                  LoyaltyProgramMission loyaltyProgramMission = (LoyaltyProgramMission) loyaltyProgram;
                  
                  //
                  //  calculate and check lastDate if FIXDURATION
                  //
                  
                  if (MissionSchedule.FIXDURATION == MissionSchedule.fromExternalRepresentation(loyaltyProgramMission.getScheduleType()))
                    {
                      String tz = Deployment.getDeployment(loyaltyProgramMission.getTenantID()).getTimeZone();
                      int duration = loyaltyProgramMission.getDuration();
                      Date entryDate = ((LoyaltyProgramMissionState) loyaltyProgramState).getLoyaltyProgramEnrollmentDate();
                      Date lastDate = RLMDateUtils.addDays(entryDate, duration, tz);
                      if (log.isDebugEnabled()) log.debug("mission {} entry date was {} and lastDate was {} as duration was {} days, subscriberID {}", loyaltyProgramMission.getGUIManagedObjectDisplay(), RLMDateUtils.formatDateForREST(entryDate, tz), RLMDateUtils.formatDateForREST(lastDate, tz), duration, subscriberProfile.getSubscriberID());
                      if (now.after(lastDate))
                        {
                          if (log.isDebugEnabled()) log.debug("time ended for mission {} entry date was {} and lastDate was {} as duration was {} days, subscriberID {}", loyaltyProgramMission.getGUIManagedObjectDisplay(), RLMDateUtils.formatDateForREST(entryDate, tz), RLMDateUtils.formatDateForREST(lastDate, tz), duration, subscriberProfile.getSubscriberID());
                          break;
                        }
                    }
                  

                  //
                  // get subscriber current step
                  //

                  String currentStep = ((LoyaltyProgramMissionState) loyaltyProgramState).getStepName();
                  MissionStep subscriberCurrentStepDefinition = loyaltyProgramMission.getStep(currentStep);
                  if (subscriberCurrentStepDefinition != null && !((LoyaltyProgramMissionState) loyaltyProgramState).isMissionCompleted())
                    {
                      boolean exitStep = subscriberCurrentStepDefinition.evaluateStepChangeCriteria(evolutionEvent);
                      if (exitStep)
                        {
                          subscriberProfileUpdated = true;
                          
                          //
                          // update loyalty program status
                          //
                          
                          MissionStep newMissionStep = loyaltyProgramMission.getNextStep(subscriberCurrentStepDefinition.getStepID());
                          String newStep = newMissionStep == null ? null : newMissionStep.getStepName();
                          if (!currentStep.equals(newStep))
                            {
                              if (newStep == null)
                                {
                                  //
                                  //  only mark as complete
                                  //
                                  
                                  ((LoyaltyProgramMissionState) loyaltyProgramState).markAsCompleted();
                                  triggerLoyaltyWorflow(evolutionEvent, context.getSubscriberState(), subscriberCurrentStepDefinition.getWorkflowCompletion(), loyaltyProgram.getLoyaltyProgramID(), currentStep);
                                  
                                }
                              else
                                {
                                  LoyaltyProgramStepChange stepChangeType = ((LoyaltyProgramMissionState) loyaltyProgramState).update(loyaltyProgram.getEpoch(), LoyaltyProgramOperation.Optin, loyaltyProgram.getLoyaltyProgramName(), newStep, now, evolutionEvent.getClass().getName(), loyaltyProgramService);

                                  //
                                  // generate new event (step changed)
                                  //

                                  ParameterMap info = new ParameterMap();
                                  info.put(LoyaltyProgramMissionEventInfos.OLD_STEP.getExternalRepresentation(), currentStep);
                                  info.put(LoyaltyProgramMissionEventInfos.NEW_STEP.getExternalRepresentation(), newStep);
                                  info.put(LoyaltyProgramMissionEventInfos.STEP_UPDATE_TYPE.getExternalRepresentation(), stepChangeType.getExternalRepresentation());
                                  ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = new ProfileLoyaltyProgramChangeEvent(subscriberProfile.getSubscriberID(), now, loyaltyProgram.getLoyaltyProgramID(), loyaltyProgram.getLoyaltyProgramType(), info);
                                  subscriberState.getProfileLoyaltyProgramChangeEvents().add(profileLoyaltyProgramChangeEvent);
                                  launchChangeTierWorkflows(profileLoyaltyProgramChangeEvent, subscriberState, loyaltyProgramMission, currentStep, newStep, loyaltyProgramState.getLoyaltyProgramID());
                                }
                            }
                        }
                    }
                  break;

                default:
                  break;
              }

          // if loyalty got deleted (NOT JUST "suspended"), we need to optout to clean it from profile after a while
          }
        else if (loyaltyProgram == null && subscriberProfile.getLoyaltyPrograms().get(loyaltyProgramID).getLoyaltyProgramExitDate() == null)
          {
            if (loyaltyProgramService.getStoredLoyaltyProgram(loyaltyProgramID) == null)
              {
                if (log.isDebugEnabled()) log.debug("exiting deleted from conf loyalty program " + loyaltyProgramID + " for subscriber " + subscriberProfile.getSubscriberID());
                loyaltyProgramState = subscriberProfile.getLoyaltyPrograms().get(loyaltyProgramID);
                if (loyaltyProgramState instanceof LoyaltyProgramPointsState)
                  {
                    ((LoyaltyProgramPointsState) loyaltyProgramState).update(loyaltyProgramState.getLoyaltyProgramEpoch(), LoyaltyProgramOperation.Optout, loyaltyProgramState.getLoyaltyProgramName(), null, now, null, loyaltyProgramService);
                  }
                else if (loyaltyProgramState instanceof LoyaltyProgramChallengeState)
                  {
                    ((LoyaltyProgramChallengeState) loyaltyProgramState).update(loyaltyProgramState.getLoyaltyProgramEpoch(), LoyaltyProgramOperation.Optout, loyaltyProgramState.getLoyaltyProgramName(), null, now, null, loyaltyProgramService);
                  }
                else if (loyaltyProgramState instanceof LoyaltyProgramMissionState)
                  {
                    ((LoyaltyProgramMissionState) loyaltyProgramState).update(loyaltyProgramState.getLoyaltyProgramEpoch(), LoyaltyProgramOperation.Optout, loyaltyProgramState.getLoyaltyProgramName(), null, now, null, loyaltyProgramService);
                  }
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
  *  determineLoyaltyProgramChallengeLevel
  *
  *****************************************/

  private static String determineLoyaltyProgramChallengeLevel(LoyaltyProgramState loyaltyProgramState, LoyaltyProgramChallenge loyaltyProgramChallenge, Date now)
  {
    String newLevelName = null;
    int currentSubsriberScores = 0;
    if (loyaltyProgramState != null && loyaltyProgramState instanceof LoyaltyProgramChallengeState)
      {
        currentSubsriberScores = ((LoyaltyProgramChallengeState) loyaltyProgramState).getCurrentScore();
      }
    
    for (ChallengeLevel level : loyaltyProgramChallenge.getLevels())
      {
        if (currentSubsriberScores >= level.getScoreLevel())
          {
            newLevelName = level.getLevelName();
          }
      }
    return newLevelName;
  }
  
  /*****************************************
  *
  *  updatePropensity
  *
  *****************************************/

  private static boolean updatePropensity(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent, int tenantID)
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
        String featureID = null;
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
            featureID = ((PresentationLog)evolutionEvent).getFeatureID();
            tokenTypeID = ((PresentationLog)evolutionEvent).getTokenTypeID();
            presentationLogToken = ((PresentationLog) evolutionEvent).getToken();
          }
        else if (evolutionEvent instanceof AcceptanceLog)
          {
            eventTokenCode = ((AcceptanceLog) evolutionEvent).getPresentationToken();
            moduleID = ((AcceptanceLog)evolutionEvent).getModuleID();
            featureID = ((AcceptanceLog)evolutionEvent).getFeatureID();
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

        if (moduleID == null)
          {
            moduleID = DeliveryRequest.Module.Unknown.getExternalRepresentation();
          }

        //
        // Subscriber token list cleaning.
        // We will delete all already expired tokens before doing anything.
        //TODO: do this cleaning using com.evolving.nglm.evolution.retention.RetentionService, as other objects

        List<Token> cleanedList = new ArrayList<Token>();
        boolean changed = false;
        Date now = SystemTime.getCurrentTime();
        for (Token token : subscriberTokens)
          {
            if (token.getTokenExpirationDate().before(now))
              {
                if(log.isTraceEnabled()) log.trace("removing token "+token.getTokenCode()+" expired on "+token.getTokenExpirationDate()+" for "+subscriberProfile.getSubscriberID());
                changed=true;
              }
            else
              {
                cleanedList.add(token);
              }
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
            subscriberState.getTokenChanges().add(new TokenChange(subscriberState.getSubscriberID(), SystemTime.getCurrentTime(), context.getEventID(), eventTokenCode, "Create", "OK", evolutionEvent.getClass().getSimpleName(), moduleID, featureID, tenantID));
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
            subscriberState.getTokenChanges().add(new TokenChange(subscriberState.getSubscriberID(), eventDate, context.getEventID(), eventTokenCode, "Allocate", "OK", "PresentationLog", moduleID, featureID, tenantID));
            if (subscriberStoredToken.getCreationDate() == null)
              {
                subscriberStoredToken.setCreationDate(eventDate);
                subscriberStoredToken.setTokenExpirationDate(defaultDNBOTokenType.getExpirationDate(eventDate, tenantID));
                subscriberStateUpdated = true;
              }
            List<Date> presentationDates = presentationLog.getPresentationDates();
            if (presentationDates != null && !presentationDates.isEmpty()) // if empty : bound has failed
              {
                if (log.isTraceEnabled()) log.trace("setPresentationDates with " + presentationDates.size());
                subscriberStoredToken.setPresentationDates(presentationDates);
                List<Presentation> presentationHistory = presentationLog.getPresentationHistory();
                if (log.isTraceEnabled()) log.trace("set token presentationHistory with " + presentationHistory + " size " + presentationHistory.size());
                subscriberStoredToken.setPresentationHistory(presentationHistory);
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
                subscriberState.getTokenChanges().add(new TokenChange(subscriberState.getSubscriberID(), acceptanceLog.getEventDate(), context.getEventID(), eventTokenCode, "Redeem", "OK", "AcceptanceLog", moduleID, featureID, tenantID));
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

            if (Deployment.getDeployment(tenantID).getPropensityRule().validate(segmentationDimensionService))
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
  *  updateEDRs
  *
  *****************************************/
  
  private static boolean updateEDRs(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent)
  {
    SubscriberState subscriberState = context.getSubscriberState();
    int tenantID = subscriberState.getSubscriberProfile().getTenantID();
    EvolutionEngineEvent engineEvent = (EvolutionEngineEvent) evolutionEvent;
    EvolutionEngineEventDeclaration declaration = Deployment.getEvolutionEngineEvents().get(engineEvent.getEventName());
    ParameterMap parameterMap = new ParameterMap();
    SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, evolutionEvent, SystemTime.getCurrentTime(), subscriberState.getSubscriberProfile().getTenantID());
    for (String field : declaration.getEdrCriterionFieldsMapping().keySet())
      {
        CriterionField criterionField = declaration.getEdrCriterionFieldsMapping().get(field);
        Object value = criterionField.retrieve(evaluationRequest);
        parameterMap.put(field, value);
      }
    EDRDetails edrDetails = new EDRDetails(context, subscriberState.getSubscriberID(), context.getEventID(), engineEvent.getEventName(), engineEvent.getEventDate(), parameterMap, tenantID);
    subscriberState.addEDRDetails(edrDetails);
    return true;
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

  private static boolean updateJourneys(EvolutionEventContext context, SubscriberStreamEvent evolutionEvent, int tenantID)
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

    //TODO: before EVPRO-325 all restriction to enter journey were done based on subscriberState.getRecentJourneyStates(), now it is on subscriberState.getSubscriberProfile().getSubscriberJourneysEnded()
    // so it is important to migrate data, but once all customer run over this version, this should be removed
    // ------ START DATA MIGRATION COULD BE REMOVED
    if(subscriberState.getSubscriberProfile().getSubscriberJourneysEnded().isEmpty()){
      for(JourneyState recentJourneyState:subscriberState.getOldRecentJourneyStates()){
        if(subscriberState.getSubscriberProfile().getSubscriberJourneysEnded().get(recentJourneyState.getJourneyID())==null && !recentJourneyState.isSpecialExit()){
          subscriberState.getSubscriberProfile().getSubscriberJourneysEnded().put(recentJourneyState.getJourneyID(),recentJourneyState.getJourneyExitDate()!=null?recentJourneyState.getJourneyExitDate():now);
        }
      }
    }
    // ------ END DATA MIGRATION COULD BE REMOVED


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
    // this will contains all journeyId subscriber has or ever had
    Set<String> allJourneyId = new HashSet<>(subscriberState.getSubscriberProfile().getSubscriberJourneysEnded().keySet());
    if(!subscriberState.getJourneyStates().isEmpty()) subscriberState.getJourneyStates().stream().forEach(state->allJourneyId.add(state.getJourneyID()));
    for (String journeyID : allJourneyId)
      {
        //
        //  candidate journey
        //

        Journey candidateJourney = journeyService.getActiveJourney(journeyID, now);
        if (candidateJourney == null) continue;
        boolean activeJourney = subscriberState.getJourneyStates().stream().anyMatch(state->state.getJourneyID().equals(journeyID));
        Date oldJourneyEndDate = null;//should never be used if journey is active!
        oldJourneyEndDate = subscriberState.getSubscriberProfile().getSubscriberJourneysEnded().get(journeyID);

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
            if (journeyObjective.getEffectiveTargetingLimitMaxSimultaneous()==1 && (activeJourney || oldJourneyEndDate.compareTo(journeyObjective.getEffectiveWaitingPeriodEndDate(now, tenantID)) >= 0))
              {
                if (log.isTraceEnabled()) log.trace("permittedWaitingPeriod put " + journeyObjective.getJourneyObjectiveName() + ": static FALSE");
                permittedWaitingPeriod.put(journeyObjective, Boolean.FALSE);
              }
            if (activeJourney || oldJourneyEndDate.compareTo(journeyObjective.getEffectiveSlidingWindowStartDate(now, tenantID)) >= 0)
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

    List<Journey> activeJourneys = new ArrayList<Journey>(journeyService.getActiveJourneys(now, tenantID));
    // Sort journeys by priorities, and randomize those with equal priorities

    // 1) sort randomly
    Collections.shuffle(activeJourneys, ThreadLocalRandom.current());
    
    // 2) sort by priorities, do not reorder journeys with same priorities
    //logCollectionPrioritiesJourneys("Before sort journeys", activeJourneys);
    Collections.sort(activeJourneys, ((j1, j2) -> j1.getPriority()-j2.getPriority()));
    //logCollectionPrioritiesJourneys("After sort journeys", activeJourneys);

    /*****************************************
    *
    *  inclusion/exclusion lists
    *
    *****************************************/

    SubscriberEvaluationRequest inclusionExclusionEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, now, tenantID);
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
        // In case of Workflow, it can be triggered through a JourneyRequest or from a loyalty program
        //
        String sourceFeatureIDFromWorkflowTriggering = null;
        String origin = null;
        String sourceModuleIDFromWorkflowTriggering = null;
        List<String> workflowTriggering = subscriberState.getWorkflowTriggering();
        List<String> toBeRemoved = new ArrayList<>();
        if(journey.isWorkflow())
          {
            // check if this workflow has to be triggered
            for(String currentWFToTrigger : workflowTriggering)
              {
                String[] elements = currentWFToTrigger.split(":");
                String eventClass = elements[0];
                if(eventClass.equals(evolutionEvent.getClass().getName()))
				          {
				            String eventDateLong = elements[1];
				            if(eventDateLong.equals("" + evolutionEvent.getEventDate().getTime())) 
				              {
				                // let compare the workflowID:
				                String workflowID = elements[2];
				                if(workflowID.equals(journey.getJourneyID()))
				                  {
				                    // this is the workflow to trig (good event, good date, good required workflow
				                    calledJourney = true;
				                    toBeRemoved.add(currentWFToTrigger);
				                    sourceFeatureIDFromWorkflowTriggering=elements[3];
                                                    if (elements.length > 4) { sourceModuleIDFromWorkflowTriggering=elements[4]; }
                                                    if (elements.length > 5) { origin = elements[5]; }
				                  }
				              }
				            else 
				              {
				                // make a cleanup if the date is too old and log a warn because that should not happen
				                try 
				                  {
				                    long dateLong = Long.parseLong(eventDateLong);
				                    if(SystemTime.getCurrentTime().getTime() > dateLong + 432000000)
				                      {
				                        // 5 days too old
				                        toBeRemoved.add(currentWFToTrigger);
		                            log.warn("currentWFToTrigger's date is too old " + currentWFToTrigger);
				                      }				                  
				                  }
				                catch(NumberFormatException e)
				                  {
				                    // should normally never happen
				                    toBeRemoved.add(currentWFToTrigger);
				                    log.warn("currentWFToTrigger's date is not parsable " + currentWFToTrigger);
				                  }				                
				              }
				          }
			        }
           }
        subscriberStateUpdated = subscriberStateUpdated || workflowTriggering.removeAll(toBeRemoved);
        
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
            SubscriberJourneyStatus currentStatus = null;
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
                *  already been in journey?
                *
                *****************************************/

                if (enterJourney && journey.getGUIManagedObjectType()!=GUIManagedObjectType.Journey/*allowed for Journey to reenter but none of the other type*/)
                  {
                    for (String previousJourneyID : subscriberState.getSubscriberProfile().getSubscriberJourneysEnded().keySet())
                      {
                        if (Objects.equals(previousJourneyID, journey.getJourneyID()))
                          {
                            context.subscriberTrace("NotEligible: already been in journey {0}", journey.getJourneyID());
                            enterJourney = false;
                          }
                      }
                  }

                /*****************************************
                *
                *  Check target / trigger / Inclusion / Eligibility
                *
                *****************************************/
                if(enterJourney && currentStatus == null)
                  {
                    // check if the subscriber should enter by 
                    // - Target (file or rules)
                    // - Trigger Event
                    // - Manual : already filtered with CalledJourney, so no need to take this case in account
                    // - All Subscribers is considered as Target with no target specified, so this case is handled by Target case                   
                    
                    // 1- Compute inAnyTargetOrTrigger: if no trigger or no target, then this boolean is set to true 
                    List<List<EvaluationCriterion>> targetsAndTriggerCriteria = journey.getAllTargetsCriteria(targetService, now);
                    if(journey.getTargetingEventCriteria() != null && !journey.getTargetingEventCriteria().isEmpty()) { targetsAndTriggerCriteria.add(journey.getTargetingEventCriteria()); }
                    boolean inAnyTargetOrTrigger = targetsAndTriggerCriteria.size() == 0 ? true : false; // if no target is defined into the journey, then this boolean is true otherwise, false by default 
                    List<EvaluationCriterion> targets = new ArrayList<>();

                    for(List<EvaluationCriterion> current : targetsAndTriggerCriteria)
                      {
                        if(inAnyTargetOrTrigger == false) { // avoid evaluating target is already true
                          SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, evolutionEvent, now, tenantID);
                          context.getSubscriberTraceDetails().addAll(evaluationRequest.getTraceDetails());
                          boolean inThisTarget = EvaluationCriterion.evaluateCriteria(evaluationRequest, current);
                          if(inThisTarget)
                            {
                              inAnyTargetOrTrigger = true;
                            }
                        }                    
                      }
                    
                    boolean enterAfterInclusionList = inAnyTargetOrTrigger;
                    // 2. At this stage: Apply Inclusion List in case in AnyTarget = false
                    if(!inAnyTargetOrTrigger)
                      {
                        if(journey.getAppendInclusionLists() && inclusionList)
                          {
                            enterAfterInclusionList = true;
                          }
                      }
                    // 3. Now                     
                    //    - if enterAfterInclusionList = false, enterJourney = false and currentStatus = null
                    //    - if enterAfterInclusionList = true, then apply the Eligibility Criterion
                    if(enterAfterInclusionList == false)
                      {
                        enterJourney = false;
                        currentStatus = null;
                        context.subscriberTrace("NotEligible: journey or subscriber not inclusion list {0}", journey.getJourneyID());
                      }
                    else 
                      {
                        SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, evolutionEvent, now, tenantID);
                        List<EvaluationCriterion> eligibilityAndTargetting = new ArrayList<>();
                        eligibilityAndTargetting.addAll(journey.getEligibilityCriteria());
                        //eligibilityAndTargetting.addAll(journey.getTargetingCriteria());
                        
                        boolean  enterAfterEligibilityCriteria = EvaluationCriterion.evaluateCriteria(evaluationRequest, eligibilityAndTargetting);
                        
                        // 4. if enterAfterEligibilityCriteria :
                        //     - is false : The would enter without Eligibility Criterion, so enterJourney = true and currentStatus = NotEligible
                        //     - is true : all ok! enterJourney = true and currentStatus = null;
                        if(enterAfterEligibilityCriteria == false)
                          {
                            enterJourney = true;
                            currentStatus = SubscriberJourneyStatus.NotEligible;
                          }
                        else 
                          {
                            // 5. Apply the journeyUniversalEligibilityCriteria
                            evaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, evolutionEvent, now, tenantID);
                            eligibilityAndTargetting = new ArrayList<>();
                            eligibilityAndTargetting.addAll(Deployment.getDeployment(tenantID).getJourneyUniversalEligibilityCriteria());
                            
                            boolean  enterAfterJourneyUniversalCriteria = EvaluationCriterion.evaluateCriteria(evaluationRequest, eligibilityAndTargetting);
                            if(enterAfterJourneyUniversalCriteria == false) {
                              // Do not enter into the Journey
                              enterJourney = false;
                              context.subscriberTrace("NotEligible: journey universal criteria {0}", journey.getJourneyID());
                            }
                            else {
                              // Enter into the Journey
                              enterJourney = true;
                              currentStatus = null;
                            }
                          }
                      }
                  }

                /*****************************************
                *
                *  verify pass all objective-level targeting policies
                *
                *****************************************/

                if (enterJourney && currentStatus == null)
                  {
                    for (JourneyObjective journeyObjective : allObjectives)
                      {
                        if (permittedJourneys.get(journeyObjective) < 1)
                          {
                            currentStatus = SubscriberJourneyStatus.ObjectiveLimitReached;
                            context.subscriberTrace("NotEligible: journey {0}, objective {1}", journey.getJourneyID(), journeyObjective.getJourneyObjectiveID());
                            break;
                          }
                      }
                  }

                /******************************************
                *
                *  pass is customer in the exclusion list?
                *
                *******************************************/

                if (enterJourney && currentStatus == null)
                  {
                    if (!journey.getAppendExclusionLists() && exclusionList)
                      {
                        currentStatus = SubscriberJourneyStatus.Excluded;
                        context.subscriberTrace("NotEligible: user is in exclusion list {0}", journey.getJourneyID());
                      }
                  }

                /*****************************************
                *
                * pass is customer UCG?
                *
                *****************************************/

                if (enterJourney && currentStatus == null)
                  {
                    if (!journey.getAppendUCG() && subscriberState.getSubscriberProfile().getUniversalControlGroup())
                      {
                        currentStatus = SubscriberJourneyStatus.UniversalControlGroup;
                        context.subscriberTrace("NotEligible: user is UCG {0}", journey.getJourneyID());
                      }
                  }

                /*********************************************
                *
                *  pass max number of customers in journey !!KEEP THIS THE LAST CHECK OR DOUBLE THINK THE STOCK MANAGEMENT!!
                *
                **********************************************/

                if (enterJourney && currentStatus == null)
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
            * enterJourney
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
                String sourceOrigin = "";
                String sourceFeatureID;
                String sourceModuleID;
                ParameterMap boundParameters;
                if (calledJourney && evolutionEvent instanceof JourneyRequest)
                  {
                    journeyRequest = (JourneyRequest) evolutionEvent;
                    boundParameters = new ParameterMap(journey.getBoundParameters());
                    boundParameters.putAll(journeyRequest.getBoundParameters());
                    // get featureID from the source
                    sourceFeatureID = journeyRequest.getDeliveryRequestSource();
                    sourceModuleID = DeliveryRequest.Module.Journey_Manager.getExternalRepresentation();
                  }
                else
                  {
                    journeyRequest = null;
                    boundParameters = journey.getBoundParameters();
                    // get the featureID from the CalledJourney computation
                    sourceFeatureID = sourceFeatureIDFromWorkflowTriggering;
                    sourceOrigin = origin;
                    sourceModuleID = sourceModuleIDFromWorkflowTriggering;
                  }

                //
                //  bound file variables
                //

                if (journey.getTargetingType() == TargetingType.FileVariables && evolutionEvent instanceof FileWithVariableEvent)
                  {
                    FileWithVariableEvent fileWithVariableEvent = (FileWithVariableEvent) evolutionEvent;
                    Map<String, CriterionField> allContextVars = journey.getContextVariables();
                    ParameterMap parameterMap = fileWithVariableEvent.getParameterMap();
                    for (String key : parameterMap.keySet())
                      {
                        String id = "variablefile" + "." + key;
                        if (allContextVars.get(id) != null)
                          {
                            boundParameters.put(id, parameterMap.get(key));
                          }
                      }
                  }

                /*****************************************
                *
                *  enterJourney -- all journeys
                *
                *****************************************/

                boundParameters.put(CriterionContext.JOURNEY_DISPLAY_PARAMETER_ID, journey.getGUIManagedObjectDisplay());

                //
                // confirm "stock reservation" (journey max number of customers limits)
                //

                if (journeyMaxNumberOfCustomersReserved) {
                  stockService.confirmReservation(journey, 1);
                }

                JourneyHistory journeyHistory = new JourneyHistory(journey.getJourneyID());
                JourneyState journeyState = new JourneyState(context, journey, journeyRequest, sourceModuleID, sourceFeatureID, boundParameters, SystemTime.getCurrentTime(), journeyHistory, sourceOrigin);

                if (currentStatus != null) // EVPRO-530
                {
                  // keep the current status only if it has not been kept before...
                  SubscriberJourneyStatus previousStatus = subscriberState.getSubscriberProfile().getSubscriberJourneys().get(journey.getJourneyID());
                  if(previousStatus==null)
                    {
                      journeyState.setJourneyNodeID(journey.getEndNodeID());
                      journeyState.setSpecialExitReason(currentStatus);
                      boolean metricsUpdated = journeyState.setJourneyExitDate(now, subscriberState, journey, context); // populate journeyMetrics (during)
                      subscriberStateUpdated = subscriberStateUpdated || metricsUpdated;
                    }
                  else
                    {
                      // just avoid the entry in the journey without status
                      continue; // continue of the main journey loop
                    }
                }

                journeyState.getJourneyHistory().addNodeInformation(null, journeyState.getJourneyNodeID(), null, null);
                boolean statusUpdated = journeyState.getJourneyHistory()
                            .addStatusInformation(SystemTime.getCurrentTime(), journeyState, false, currentStatus);
                subscriberState.getJourneyStates().add(journeyState);
                subscriberState.addJourneyStatistic(new JourneyStatistic(context,
                                          subscriberState.getSubscriberID(),
                                          journeyState.getJourneyHistory(),
                                          journeyState, subscriberState.getSubscriberProfile().getStatisticsSegmentsMap(subscriberGroupEpochReader, segmentationDimensionService),
                                          subscriberState.getSubscriberProfile()));
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
                //
                // check if JourneyMetrics enabled: Metrics should be generated for campaigns only (not journeys nor bulk campaigns)
                //
                if (journey.journeyMetricsNeeded())
                  {
                    boolean metricsUpdated = journeyState.populateMetricsPrior(subscriberState, tenantID);
                    subscriberStateUpdated = subscriberStateUpdated || metricsUpdated;

                    // Create a JourneyMetric to be added to JourneyStatistic from journeyState
                    subscriberState.getJourneyMetrics().add(new JourneyMetric(context, subscriberState.getSubscriberID(), journeyState));
                  }

                /*****************************************
                *
                *  enterJourney -- called journey
                *
                *****************************************/

                if (calledJourney && journeyRequest != null) // calledJourney can be true and journeyRequest null in case of LoyaltyProgram workflow 
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
            
            if (calledJourney && evolutionEvent instanceof JourneyRequest)
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

    List<JourneyState> orderedJourneyStates = new ArrayList<>();
    orderedJourneyStates.addAll(subscriberState.getJourneyStates());

    // sort by priorities, do not change order if same priority
    // from Collections.sort Javadoc : This sort is guaranteed to be stable: equal elements will not be reordered as a result of the sort.
    //logCollectionPrioritiesJourneyStates("Before sort nodes", orderedJourneyStates);
    Collections.sort(orderedJourneyStates, ((j1, j2) -> j1.getPriority()-j2.getPriority()));
    //logCollectionPrioritiesJourneyStates("After sort nodes", orderedJourneyStates);

    for (JourneyState journeyState : orderedJourneyStates)
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
        if(journeyState.isSpecialExit()) {
          inactiveJourneyStates.add(journeyState);
          continue;
        }
        
        
        /******************************************************
        *
        * ignore workflow execution in case source is stopped/finished/removed
        * 
        *******************************************************/
        if (journey != null && journey.isWorkflow()) {
          String featureID = journeyState.getsourceFeatureID();
          DeliveryRequest.Module moduleID = DeliveryRequest.Module.fromExternalRepresentation(journeyState.getSourceModuleID());
          GUIManagedObject caller = null;
          boolean mustCheck = true;
          switch (moduleID) {
            case Journey_Manager:
              caller = journeyService.getStoredJourney(featureID);
              break;
            case Loyalty_Program:
              caller = loyaltyProgramService.getStoredLoyaltyProgram(featureID);
              break;
            case Unknown:
              log.debug("Unknown moduleID : " + moduleID.getExternalRepresentation());
              mustCheck = false;
              break;
          }
          if (mustCheck) {
            if (caller == null) {
              // if source was removed, just stop workflow
              inactiveJourneyStates.add(journeyState);
              continue;
            } else if (!caller.getActive()) {
              // if source stopped, just ignore workflow
              continue;
            } 
          }
        }

        if (journey == null || journeyNode == null) {
          // possible temporary inactive journey, do nothing at all ( so no reporting or anything here )
          if(journeyService.getInterruptedGUIManagedObject(journeyState.getJourneyID(), now) != null) {
            context.subscriberTrace("ignoring inactive for now journey {0}", journeyState.getJourneyID());
            continue;
          }

          boolean metricsUpdated = journeyState.setJourneyExitDate(now, subscriberState, journey, context); // populate journeyMetrics (during)
          subscriberStateUpdated = subscriberStateUpdated || metricsUpdated;
          boolean statusUpdated = journeyState.getJourneyHistory().addStatusInformation(SystemTime.getCurrentTime(), journeyState, true);
          subscriberState.addJourneyStatistic(new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, subscriberState.getSubscriberProfile().getStatisticsSegmentsMap(subscriberGroupEpochReader, segmentationDimensionService), subscriberState.getSubscriberProfile(), now));
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
                              SubscriberEvaluationRequest contextVariableEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, callingJourneyNode, null, evolutionEvent, now, tenantID);
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

                              boolean metricsUpdated = journeyState.setJourneyExitDate(now, subscriberState, journey, context); // populate journeyMetrics (during)
                              subscriberStateUpdated = subscriberStateUpdated || metricsUpdated;
                              boolean statusUpdated = journeyState.getJourneyHistory().addStatusInformation(SystemTime.getCurrentTime(), journeyState, true);
                              subscriberState.addJourneyStatistic(new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, subscriberState.getSubscriberProfile().getStatisticsSegmentsMap(subscriberGroupEpochReader, segmentationDimensionService), subscriberState.getSubscriberProfile(), SystemTime.getCurrentTime()));
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
            if(deliveryResponse.getOriginatingSubscriberID() == null || deliveryResponse.getOriginatingSubscriberID().startsWith(DeliveryManager.ORIGIN))
              {
                // case where the response is to the parent into the relationship, so the history must be taken in account...
                // this history is not taken in account for a response to the original subscriber as this response is only here to unlock the Journey
                if (Objects.equals(deliveryResponse.getModuleID(), DeliveryRequest.Module.Journey_Manager.getExternalRepresentation()) && Objects.equals(deliveryResponse.getFeatureID(), journeyState.getJourneyID()))
                  {
                    journeyState.getJourneyHistory().addRewardInformation(deliveryResponse, deliverableService, now);
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
        String sample = null;
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

                SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, journeyLink, evolutionEvent, now, tenantID);

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

            //TODO: should we create JourneyStatistics if reward history added previously (journeyState.getJourneyHistory().addRewardInformation) but no node transition happens ?
            if (firedLink == null)
              {
                for (Date nextEvaluationDate : nextEvaluationDates)
                  {
                    if(nextEvaluationDate.before(RLMDateUtils.addDays(SystemTime.getCurrentTime(), 2, Deployment.getDeployment(tenantID).getTimeZone())))
                      {
                        subscriberState.getScheduledEvaluations().add(new TimedEvaluation(subscriberState.getSubscriberID(), nextEvaluationDate));
                        subscriberStateUpdated = true;
                      }
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
                
                // Check for new conversion - setup
                Boolean convertedReference = null;
                if(originalStatusConverted) {
                  convertedReference = new Boolean(true); // We track for change (Boolean object, we will compare reference and not value) 
                  journeyState.getJourneyParameters().put(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName(), convertedReference);
                }
                
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
                                  SubscriberEvaluationRequest contextVariableEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, firedLink, evolutionEvent, now, tenantID);
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

                                  boolean metricsUpdated = journeyState.setJourneyExitDate(now, subscriberState, journey, context); // populate journeyMetrics (during)
                                  subscriberStateUpdated = subscriberStateUpdated || metricsUpdated;
                                  boolean statusUpdated = journeyState.getJourneyHistory().addStatusInformation(SystemTime.getCurrentTime(), journeyState, true);
                                  subscriberState.addJourneyStatistic(new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, subscriberState.getSubscriberProfile().getStatisticsSegmentsMap(subscriberGroupEpochReader, segmentationDimensionService), subscriberState.getSubscriberProfile(), SystemTime.getCurrentTime()));
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

                        SubscriberEvaluationRequest exitActionEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, firedLink, evolutionEvent, now, tenantID);
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

                        List<Action> actions = new ArrayList<>();
                        SubscriberEvaluationRequest entryActionEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, null, null, now, tenantID);
                        String hierarchyRelationship = (String) CriterionFieldRetriever.getJourneyNodeParameter(entryActionEvaluationRequest, "node.parameter.relationship");
                        if (hierarchyRelationship != null && hierarchyRelationship.trim().equals(INTERNAL_ID_SUPPLIER))
                          {
                            if (!addActionForPartner(context, journeyState, actions, entryActionEvaluationRequest, supplierService, INTERNAL_VARIABLE_SUPPLIER))
                              {
                                subscriberState.getSubscriberProfile().getUnknownRelationships().add(new Pair(entryActionEvaluationRequest.getJourneyState().getJourneyID(), entryActionEvaluationRequest.getJourneyNode().getNodeID()));
                              }
                          }
                        else if (hierarchyRelationship != null && hierarchyRelationship.trim().equals(INTERNAL_ID_RESELLER))
                          {
                            if (!addActionForPartner(context, journeyState, actions, entryActionEvaluationRequest, resellerService, INTERNAL_VARIABLE_RESELLER))
                              {
                                subscriberState.getSubscriberProfile().getUnknownRelationships().add(new Pair(entryActionEvaluationRequest.getJourneyState().getJourneyID(), entryActionEvaluationRequest.getJourneyNode().getNodeID()));
                              }
                          }
                        else if (hierarchyRelationship != null && !hierarchyRelationship.trim().equals("customer"))
                          {
                            //
                            // evaluate hierarchy relationship
                            //

                            SubscriberRelatives subscriberRelatives = context.getSubscriberState().getSubscriberProfile().getRelations().get(hierarchyRelationship);
                            if (subscriberRelatives != null && subscriberRelatives.getParentSubscriberID() != null)
                              {
                                // generate a new message that will go for the parrent
                                ExecuteActionOtherSubscriber action = new ExecuteActionOtherSubscriber(subscriberRelatives.getParentSubscriberID(), entryActionEvaluationRequest.getSubscriberProfile().getSubscriberID(), entryActionEvaluationRequest.getJourneyState().getJourneyID(), entryActionEvaluationRequest.getJourneyNode().getNodeID(), context.getUniqueKey(), entryActionEvaluationRequest.getJourneyState());
                                actions.add(action);
                              }
                            else
                              {
                                // in case there is nobody as father in the given relation, then add this information temporarly into the subscriber profile
                                subscriberState.getSubscriberProfile().getUnknownRelationships().add(new Pair(entryActionEvaluationRequest.getJourneyState().getJourneyID(), entryActionEvaluationRequest.getJourneyNode().getNodeID()));
                              }
                          }
                        else
                          {
                            //
                            // evaluate action normally as no hierarchy
                            //

                            actions = journeyNode.getNodeType().getActionManager().executeOnEntry(context, entryActionEvaluationRequest);
                            context.getSubscriberTraceDetails().addAll(entryActionEvaluationRequest.getTraceDetails());
                          }

                          //
                          // execute action
                          //

                          handleExecuteOnEntryActions(subscriberState, journeyState, journey, actions, entryActionEvaluationRequest, context.getEventID());
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
                                  SubscriberEvaluationRequest contextVariableEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), (ExtendedSubscriberProfile) null, subscriberGroupEpochReader, journeyState, journeyNode, null, null, now, tenantID);
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

                                  boolean metricsUpdated = journeyState.setJourneyExitDate(now, subscriberState, journey, context); // populate journeyMetrics (during)
                                  subscriberStateUpdated = subscriberStateUpdated || metricsUpdated;
                                  boolean statusUpdated = journeyState.getJourneyHistory().addStatusInformation(SystemTime.getCurrentTime(), journeyState, true);
                                  subscriberState.addJourneyStatistic(new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), journeyState, subscriberState.getSubscriberProfile().getStatisticsSegmentsMap(subscriberGroupEpochReader, segmentationDimensionService), subscriberState.getSubscriberProfile(), SystemTime.getCurrentTime()));
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

                    boolean metricsUpdated = journeyState.setJourneyExitDate(now, subscriberState, journey, context); // populate journeyMetrics (during)
                    subscriberStateUpdated = subscriberStateUpdated || metricsUpdated;
                    inactiveJourneyStates.add(journeyState);
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
                //  markConverted & conversionCount
                //
                boolean markConverted = false;
                
                if(originalStatusConverted == false) { // first conversion 
                  markConverted = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
                }
                else { // conversion on already converted -- Be careful, we are comparing references here, not the value (as intended) !
                  markConverted = (convertedReference != journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()));
                }
                
                if(markConverted) {
                  journeyState.getJourneyHistory().incrementConversions(now);
                }
                
                //
                // abTesting (we remove it so its only counted once per journey)
                //
                
               
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
                
                boolean statusUpdated = journeyState.getJourneyHistory().addStatusInformation(SystemTime.getCurrentTime(), journeyState, journeyNode.getExitNode());
                if(journey.isWorkflow())
                  {
                    // retrieve the journey state of the calling campaign
                	JourneyState sourceJourneyState = null;
                    if(subscriberState.getJourneyStates() != null)
                      {
                        for(JourneyState state : subscriberState.getJourneyStates())
                          {
                            if(state.getJourneyID().equals(journeyState.getsourceFeatureID()) && DeliveryRequest.Module.Journey_Manager.getExternalRepresentation().equals(journeyState.getSourceModuleID()))
                              {
                                sourceJourneyState = state;
                              }
                          }
                      }
                    if(sourceJourneyState != null)
                      {
                        subscriberState.addJourneyStatistic(new JourneyStatistic(context, subscriberState.getSubscriberID(), sourceJourneyState.getJourneyHistory(), journeyState.getJourneyHistory(), sourceJourneyState, journeyState, firedLink, markNotified, markConverted, sample, subscriberState.getSubscriberProfile().getStatisticsSegmentsMap(subscriberGroupEpochReader, segmentationDimensionService), subscriberState.getSubscriberProfile()));
                      }
                    else
                      {
                        // error...generate a jounrey statistic for the workflow as it it was a normal journey for debug help
                        log.warn("Can't retrieve source journey state for workflow " + journeyState.getJourneyID() + " and subscriber " + subscriberState.getSubscriberID());
                        subscriberState.addJourneyStatistic(new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), null, journeyState, null, firedLink, markNotified, markConverted, sample, subscriberState.getSubscriberProfile().getStatisticsSegmentsMap(subscriberGroupEpochReader, segmentationDimensionService), subscriberState.getSubscriberProfile()));
                      }
                  }
                else
                  {
                	// normal case
                	subscriberState.addJourneyStatistic(new JourneyStatistic(context, subscriberState.getSubscriberID(), journeyState.getJourneyHistory(), null, journeyState, null, firedLink, markNotified, markConverted, sample, subscriberState.getSubscriberProfile().getStatisticsSegmentsMap(subscriberGroupEpochReader, segmentationDimensionService), subscriberState.getSubscriberProfile()));
                  }
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
                          
                        case AniversaryCriterion:
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
        // MIGHT BE WRONG? : assuming we will have to do post metrics if we already have prior or during
        if(!journeyState.getJourneyMetricsPrior().isEmpty() || !journeyState.getJourneyMetricsDuring().isEmpty()) subscriberState.getJourneyEndedStates().add(journeyState.getJourneyEndedState());

        subscriberState.getJourneyStates().remove(journeyState);
        if(!journeyState.isSpecialExit()) 
          {
            // this if because if we have a special exit means the Journey has not been effectively executed
            subscriberState.getSubscriberProfile().getSubscriberJourneysEnded().put(journeyState.getJourneyID(),now);
          }
        subscriberStateUpdated = true;
      }

    /*****************************************
    *
    *  close metrics
    *
    *****************************************/

    Iterator<JourneyEndedState> journeyEndedStateIterator = subscriberState.getJourneyEndedStates().iterator();
    while (journeyEndedStateIterator.hasNext())
      {
        JourneyEndedState journeyEndedState = journeyEndedStateIterator.next();
        GUIManagedObject journey = journeyService.getStoredJourney(journeyEndedState.getJourneyID(), true);

        //
        // Check if JourneyMetrics are enabled.
        // JourneyMetrics should only be generated for Campaigns (not journeys nor bulk campaigns)
        //
        if (journey == null) {
          log.warn("Unable to retrieve journey " + journeyEndedState.getJourneyID() + ". It will be closed without publishing any JourneyMetrics.");
          journeyEndedStateIterator.remove();
          subscriberStateUpdated = true;
        }
        else if (((Journey) journey).journeyMetricsNeeded()) {
          boolean metricsUpdated = journeyEndedState.populateMetricsPost(subscriberState, now, tenantID);
          subscriberStateUpdated = subscriberStateUpdated || metricsUpdated;

          if (metricsUpdated) {
            // Create a JourneyMetric to be added to JourneyStatistic from journeyState
            subscriberState.getJourneyMetrics().add(new JourneyMetric(context, subscriberState.getSubscriberID(), journeyEndedState));
            journeyEndedStateIterator.remove();
            subscriberStateUpdated = true;
          }
        }
        else {
          //
          // Close journey if JourneyMetrics are disabled
          //
          journeyEndedStateIterator.remove();
          subscriberStateUpdated = true;
        }
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return subscriberStateUpdated;
  }

  private static void logCollectionPrioritiesJourneys(String msg, List<Journey> list)
  {
    if (!list.isEmpty())
      {
        StringBuffer sb = new StringBuffer();
        list.stream().forEach(j->sb.append(j.getPriority()+","));
        if (sb.length() > 0) sb.deleteCharAt(sb.length()-1);
        log.info(msg + " : " + sb);
      }
  }
  private static void logCollectionPrioritiesJourneyStates(String msg, List<JourneyState> list)
  {
    if (!list.isEmpty())
      {
        StringBuffer sb = new StringBuffer();
        list.stream().forEach(j->sb.append(j.getPriority()+","));
        if (sb.length() > 0) sb.deleteCharAt(sb.length()-1);
        log.info(msg + ":" + sb);
      }
  }

  private static boolean addActionForPartner(EvolutionEventContext context, JourneyState journeyState, List<Action> actions, SubscriberEvaluationRequest entryActionEvaluationRequest, GUIService guiService, String variableName)
  {
    boolean res = false; // fail by default
    String variableID = "variable." + variableName;// if you have to modify this, you have to modify as well the param set in com.evolving.nglm.evolution.EvolutionEngine.VoucherActionManager.executeOnEntry()
    Object partnerName = (journeyState.getJourneyParameters() != null) ? journeyState.getJourneyParameters().get(variableID) : null;
    if (partnerName == null) {
      log.info("Unable to find partner in variable " + variableName);
    } else if (partnerName instanceof String)
      {
        String customerIDPartner = null;
        String partnerNameStr = (String) partnerName;
        for (GUIManagedObject partner : guiService.getActiveGUIManagedObjects(context.now(), context.getSubscriberState().getSubscriberProfile().getTenantID()))
          {
            if (partnerNameStr.equals(partner.getGUIManagedObjectDisplay()))
              {
                // get customerID that was set when creating the partner
                customerIDPartner = (String) partner.getJSONRepresentation().get("customerIDfromAlternateID");
                break;
              }
          }
        if (customerIDPartner != null) {
            log.trace("Will do action for partner " + customerIDPartner);
            ExecuteActionOtherSubscriber action = new ExecuteActionOtherSubscriber(customerIDPartner, entryActionEvaluationRequest.getSubscriberProfile().getSubscriberID(), entryActionEvaluationRequest.getJourneyState().getJourneyID(), entryActionEvaluationRequest.getJourneyNode().getNodeID(), context.getUniqueKey(), entryActionEvaluationRequest.getJourneyState());
            actions.add(action);
            res = true;
          } else {
            log.info("Error : need to do action for " + partnerNameStr + " but found no associated customerID");
          }
      }
    else
      {
        log.info("Expecting partner in variable " + variableName + " found " + partnerName + " " + ((partnerName != null) ? partnerName.getClass().getCanonicalName() : "null"));
      }
    return res;
  }

  private static void handleExecuteOnEntryActions(SubscriberState subscriberState, JourneyState journeyState, Journey journey, List<Action> actions, SubscriberEvaluationRequest subscriberEvaluationRequest, String eventID)
  {
    for (Action action : actions)
      {
        switch (action.getActionType())
          {
            case DeliveryRequest:
              DeliveryRequest deliveryRequest = (DeliveryRequest) action;
              subscriberState.getDeliveryRequests().add(deliveryRequest);
              journeyState.setJourneyOutstandingDeliveryRequestID(deliveryRequest.getDeliveryRequestID());
              break;

            case ExecuteActionOtherSubscriber:
              ExecuteActionOtherSubscriber executeActionOtherSubscriber = (ExecuteActionOtherSubscriber) action;
              if(executeActionOtherSubscriber.getOutstandingDeliveryRequestID() != null) { journeyState.setJourneyOutstandingDeliveryRequestID(executeActionOtherSubscriber.getOutstandingDeliveryRequestID()); }
              subscriberState.getExecuteActionOtherSubscribers().add(executeActionOtherSubscriber);
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
              String featureID = journey.getJourneyID();
              token.setFeatureID(featureID);
              int tenantID = subscriberState.getSubscriberProfile().getTenantID();
              String nodeName = null;
              if (subscriberEvaluationRequest.getJourneyNode() != null)
                {
                  nodeName = subscriberEvaluationRequest.getJourneyNode().getNodeName();
                }
              switch (token.getTokenStatus())
              {
                case New:
                  subscriberState.getTokenChanges().add(generateTokenChange(subscriberState.getSubscriberID(), token.getCreationDate(), eventID, TokenChange.CREATE, token, featureID, nodeName, tenantID));
                  break;
                case Bound: // must record the token creation
                  subscriberState.getTokenChanges().add(generateTokenChange(subscriberState.getSubscriberID(), token.getCreationDate(), eventID, TokenChange.CREATE, token, featureID, nodeName, tenantID));
                  subscriberState.getTokenChanges().add(generateTokenChange(subscriberState.getSubscriberID(), token.getBoundDate(), eventID, TokenChange.ALLOCATE, token, featureID, nodeName, tenantID));
                  break;
                case Redeemed: // must record the token creation & allocation
                  subscriberState.getTokenChanges().add(generateTokenChange(subscriberState.getSubscriberID(), token.getCreationDate(), eventID, TokenChange.CREATE, token, featureID, nodeName, tenantID));
                  subscriberState.getTokenChanges().add(generateTokenChange(subscriberState.getSubscriberID(), token.getBoundDate(), eventID, TokenChange.ALLOCATE, token, featureID, nodeName, tenantID));
                  subscriberState.getTokenChanges().add(generateTokenChange(subscriberState.getSubscriberID(), token.getRedeemedDate(), eventID, TokenChange.REDEEM, token, featureID, nodeName, tenantID));
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
              
            case VoucherChange:
              if (action instanceof VoucherAction)
                {
                  VoucherAction event = (VoucherAction) action;
                  subscriberState.getVoucherActions().add(event);
                }
              break;
              
            case TriggerEvent:
              JourneyTriggerEventAction triggerEventAction = (JourneyTriggerEventAction) action;
              subscriberState.getJourneyTriggerEventActions().add(triggerEventAction);
              break;

            case UpdateProfile:
              SubscriberProfileForceUpdate subscriberProfileForceUpdate = (SubscriberProfileForceUpdate) action;
              subscriberState.getSubscriberProfileForceUpdates().add(subscriberProfileForceUpdate);
              break;

            default:
              log.error("unsupported action {} on actionManager.executeOnExit", action.getActionType());
              break;
          }
      }
  }

  private static TokenChange generateTokenChange(String subscriberId, Date eventDateTime, String eventID, String action, Token token, String journeyID, String origin, int tenantID)
  {
    return new TokenChange(subscriberId, eventDateTime, eventID, token.getTokenCode(), action, "OK", origin, Module.Journey_Manager, journeyID, tenantID);
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
    catch (RuntimeException|IllegalAccessException|InvocationTargetException e)
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
        try{
          extendedSubscriberProfile.setSubscriberTrace(new SubscriberTrace(generateSubscriberTraceMessage(evolutionEvent, currentExtendedSubscriberProfile, extendedSubscriberProfile, context.getSubscriberTraceDetails())));
        }catch(Exception e){
          log.warn("subscriber trace exception",e);
        }
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

  private static List<SubscriberStreamOutput> getEvolutionEngineOutputs(SubscriberStateOutputWrapper subscriberStateHackyWrapper)
  {
    SubscriberState subscriberState = subscriberStateHackyWrapper.getSubscriberState();
    List<SubscriberStreamOutput> result = new ArrayList<SubscriberStreamOutput>();
    if (subscriberState != null)
      {
        result.addAll(subscriberState.getJourneyResponses());
        result.addAll(subscriberState.getJourneyRequests());
        result.addAll(subscriberState.getLoyaltyProgramResponses());
        result.addAll(subscriberState.getLoyaltyProgramRequests());
        result.addAll(subscriberState.getPointFulfillmentResponses());
        result.addAll(subscriberState.getDeliveryRequests());
        result.addAll(subscriberState.getJourneyStatisticWrappers().values());
        result.addAll(subscriberState.getJourneyMetrics());
        result.addAll((subscriberState.getSubscriberTrace() != null) ? Collections.<SubscriberTrace>singletonList(subscriberState.getSubscriberTrace()) : Collections.<SubscriberTrace>emptyList());
        result.addAll((subscriberState.getExternalAPIOutput() != null) ? Collections.<ExternalAPIOutput>singletonList(subscriberState.getExternalAPIOutput()) : Collections.<ExternalAPIOutput>emptyList());
        result.addAll(subscriberState.getProfileChangeEvents());
        result.addAll(subscriberState.getTokenChanges());
        result.addAll(subscriberState.getVoucherChanges());
        result.addAll(subscriberState.getProfileSegmentChangeEvents());
        result.addAll(subscriberState.getProfileLoyaltyProgramChangeEvents());
        result.addAll(subscriberState.getExecuteActionOtherSubscribers());
        result.addAll(subscriberState.getVoucherActions());
        result.addAll(subscriberState.getJourneyTriggerEventActions());
        result.addAll(subscriberState.getSubscriberProfileForceUpdates());
        result.addAll(subscriberState.getEdrDetailsWrappers());
      }

    // add stats about voucherChange done
    if(!subscriberState.getVoucherChanges().isEmpty())
    {
      subscriberState.getVoucherChanges().stream().forEach(voucherChange -> statsVoucherChangeCounter
            .withLabel(StatsBuilders.LABEL.operation.name(),voucherChange.getAction().getExternalRepresentation())
            .withLabel(StatsBuilders.LABEL.module.name(),Module.fromExternalRepresentation(voucherChange.getModuleID()).name())
            .withLabel(StatsBuilders.LABEL.status.name(),voucherChange.getReturnStatus().getGenericResponseMessage())
            .getStats().increment()
      );
    }

    // enrich with needed output all here
    result.stream().forEach(subscriberStreamOutput -> subscriberStreamOutput.enrichSubscriberStreamOutput(subscriberStateHackyWrapper.getOriginalEvent(),subscriberState.getSubscriberProfile(),subscriberGroupEpochReader, subscriberState.getSubscriberProfile().getTenantID()));
    // as well as output wrapped in
    subscriberState.getJourneyTriggerEventActions().forEach(journeyTriggerEventAction -> ((SubscriberStreamOutput)journeyTriggerEventAction.getEventToTrigger()).enrichSubscriberStreamOutput(subscriberStateHackyWrapper.getOriginalEvent(),subscriberState.getSubscriberProfile(),subscriberGroupEpochReader, subscriberState.getSubscriberProfile().getTenantID()));
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
  
  // sorry I'm lost between all cases, not sure at all this cover all (note sure we have to key by deliveryRequest at all now, so TODO: double check if we can just key all deliveryRequest by subsId always everywhere instead of that mess)
  private static KeyValue<StringKey, DeliveryRequest> rekeyDeliveryRequestStream(StringKey key, DeliveryRequest value)
  {
    boolean isEvolutionEngineProcessor = Deployment.getDeliveryManagers().get(value.getDeliveryType()).isProcessedByEvolutionEngine();
    if(isEvolutionEngineProcessor && value.isPending()) return new KeyValue<>(new StringKey(value.getSubscriberID()), value);//loop back of request to be processed by evolution engine
  	if(isEvolutionEngineProcessor && !value.isPending() && value.getOriginatingRequest()) return new KeyValue<>(new StringKey(value.getSubscriberID()), value);// response to evolution engine directly (compare to response to purchase or commodityDeliveryManager I guess...)
	// is the "hacky BDR" (not so hacky to me, refer comment at "stream branching")
	if(value.getDeliveryType().equals(CommodityDeliveryManager.COMMODITY_DELIVERY_TYPE) && !value.isPending()) return new KeyValue<>(new StringKey(value.getSubscriberID()), value);// response to evolution engine directly
	return new KeyValue<>(new StringKey(value.getDeliveryRequestID()), value);//all the other
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

  /*****************************************
  *
  *  generateSubscriberTraceMessage
  *
  *****************************************/

  private static String generateSubscriberTraceMessage(SubscriberStreamEvent evolutionEvent, SubscriberState subscriberState, List<String> subscriberTraceDetails) throws Exception
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

    JsonNode currentSubscriberStateNode = (subscriberState.getKafkaRepresentation()!=null && subscriberState.getKafkaRepresentation().length>0) ? deserializer.deserialize(null, subscriberState.getKafkaRepresentation()) : null;

    //
    //  field -- subscriberState
    //

    JsonNode subscriberStateNode = deserializer.deserialize(null, converter.fromConnectData(null, SubscriberState.schema(),  SubscriberState.pack(subscriberState)));

    converter.close(); // to make Eclipse happy
    deserializer.close(); // to make Eclipse happy

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

  private static String generateSubscriberTraceMessage(SubscriberStreamEvent evolutionEvent, ExtendedSubscriberProfile currentExtendedSubscriberProfile, ExtendedSubscriberProfile extendedSubscriberProfile, List<String> subscriberTraceDetails) throws Exception
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

    converter.close(); // to make Eclipse happy
    deserializer.close(); // to make Eclipse happy

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
    private SubscriberStreamEvent event;
    private ExtendedSubscriberProfile extendedSubscriberProfile;
    private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
    private String executeActionOtherUserDeliveryRequestID;
    private String executeActionOtherUserOriginalSubscriberID;
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
    private ResellerService resellerService;
    private SupplierService supplierService;
    private CustomCriteriaService customCriteriaService;
    private KStreamsUniqueKeyServer uniqueKeyServer;
    private Date now;
    private List<String> subscriberTraceDetails;
    private String eventID;
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public EvolutionEventContext(SubscriberState subscriberState, SubscriberStreamEvent event, ExtendedSubscriberProfile extendedSubscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, JourneyService journeyService, SubscriberMessageTemplateService subscriberMessageTemplateService, DeliverableService deliverableService, SegmentationDimensionService segmentationDimensionService, PresentationStrategyService presentationStrategyService, ScoringStrategyService scoringStrategyService, OfferService offerService, SalesChannelService salesChannelService, TokenTypeService tokenTypeService, SegmentContactPolicyService segmentContactPolicyService, ProductService productService, ProductTypeService productTypeService, VoucherService voucherService, VoucherTypeService voucherTypeService, CatalogCharacteristicService catalogCharacteristicService, DNBOMatrixService dnboMatrixService, PaymentMeanService paymentMeanService, KStreamsUniqueKeyServer uniqueKeyServer, ResellerService resellerService, SupplierService supplierService, CustomCriteriaService customCriteriaService, Date now)
    {
      this.subscriberState = subscriberState;
      this.event = event;
      this.extendedSubscriberProfile = extendedSubscriberProfile;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
      this.journeyService = journeyService;
      this.subscriberMessageTemplateService = subscriberMessageTemplateService;
      this.deliverableService = deliverableService;
      this.segmentationDimensionService = segmentationDimensionService;
      this.presentationStrategyService = presentationStrategyService;
      this.supplierService = supplierService;
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
      this.resellerService = resellerService;
      this.supplierService = supplierService;
      this.customCriteriaService = customCriteriaService;
      this.uniqueKeyServer = uniqueKeyServer;
      this.now = now;
      this.subscriberTraceDetails = new ArrayList<String>();
      this.eventID = generateEventID(event);
    }

    private String generateEventID(SubscriberStreamEvent event)
    {
      String result = null;
      String className = event.getClass().getSimpleName();
      if (event instanceof EvolutionEngineEvent)
        {
          EvolutionEngineEvent engineEvent = (EvolutionEngineEvent) event;
          EvolutionEngineEventDeclaration declaration = Deployment.getEvolutionEngineEvents().get(engineEvent.getEventName());
          if (declaration != null && declaration.getEdrCriterionFieldsMapping() != null && !declaration.getEdrCriterionFieldsMapping().isEmpty())
            {
              result = getUniqueKey();
            }
          else if (event instanceof DeliveryRequest)
            {
              //
              //  propagate the eventID as long as we can track - if campaign is configureID like chain of bonus/message
              //
              
              DeliveryRequest deliveryRes = (DeliveryRequest) event;
              if (deliveryRes.getEventID() != null)
                {
                  result = deliveryRes.getEventID(); //deliveryRes.getEventName() != null ? deliveryRes.getEventName().concat("-").concat(deliveryRes.getEventID()) : className.concat("-").concat(deliveryRes.getEventID());
                } 
              else
                {
                  result = deliveryRes.getEventName() != null ? deliveryRes.getEventName() : className;
                } 
            }
          else
            {
              result = engineEvent.getEventName();
            }
        }
      else
        {
          result = className;
        }
      return result;
    }

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public SubscriberState getSubscriberState() { return subscriberState; }
    public SubscriberStreamEvent getEvent() { return event; }
    public ExtendedSubscriberProfile getExtendedSubscriberProfile() { return extendedSubscriberProfile; }
    public ReferenceDataReader<String,SubscriberGroupEpoch> getSubscriberGroupEpochReader() { return subscriberGroupEpochReader; }
    public String getExecuteActionOtherSubscriberDeliveryRequestID() { return executeActionOtherUserDeliveryRequestID; }
    public String getExecuteActionOtherUserOriginalSubscriberID() { return executeActionOtherUserOriginalSubscriberID; }
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
    public ResellerService getResellerService() { return resellerService; }
    public SupplierService getSupplierService() { return supplierService; }
    public CustomCriteriaService getCustomCriteriaService() { return customCriteriaService; }

    public KStreamsUniqueKeyServer getUniqueKeyServer() { return uniqueKeyServer; }
    public List<String> getSubscriberTraceDetails() { return subscriberTraceDetails; }
    public Date now() { return now; }
    public boolean getSubscriberTraceEnabled() { return subscriberState.getSubscriberProfile().getSubscriberTraceEnabled(); }

    public void setExecuteActionOtherSubscriberDeliveryRequestID(String requestID) { this.executeActionOtherUserDeliveryRequestID = requestID; }
    public void setExecuteActionOtherUserOriginalSubscriberID(String subscriberID) { this.executeActionOtherUserOriginalSubscriberID = subscriberID; }
    public String getEventID() { return eventID; }

    /*****************************************
    *
    *  getUniqueKey
    *
    *****************************************/

    public String getUniqueKey()
    {
      return uniqueKeyServer.getKey();
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
      return uniqueKeyServer.getKey();
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
    long startTime = DurationStat.startTime();
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
        boolean forSupport = JSONUtilities.decodeBoolean(jsonRoot, "forSupport", Boolean.FALSE);// this is not a getSubscriberProfile this is a "get statestores data" call, only meant for support troubleshooting

        /*****************************************
        *
        *  process
        *
        *****************************************/

        byte[] apiResponse = null;
        switch (api)
          {
            case getSubscriberProfile:
              apiResponse = processGetSubscriberProfile(subscriberID, includeExtendedSubscriberProfile, forSupport);
              break;

            case retrieveSubscriberProfile:
              apiResponse = processRetrieveSubscriberProfile(subscriberID, includeExtendedSubscriberProfile, forSupport);
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

        boolean isForSupportResponse = forSupport && api.equals(API.getSubscriberProfile);

        ByteBuffer response = ByteBuffer.allocate( (!isForSupportResponse?apiResponseHeader.array().length:0) + apiResponse.length);
        if(!isForSupportResponse) response.put(apiResponseHeader.array());
        response.put(apiResponse);

        //
        //  send
        //

        exchange.sendResponseHeaders(200, response.array().length);
        exchange.getResponseBody().write(response.array());
        exchange.close();

        //stats
        statsApiDuration.withLabel(StatsBuilders.LABEL.name.name(),api.getExternalRepresentation())
                       .withLabel(StatsBuilders.LABEL.status.name(),apiResponse.length>0?StatsBuilders.STATUS.ok.name():StatsBuilders.STATUS.unknown.name())
                       .getStats().add(startTime);
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

        //stats
        statsApiDuration.withLabel(StatsBuilders.LABEL.name.name(),api.getExternalRepresentation())
                       .withLabel(StatsBuilders.LABEL.status.name(),StatsBuilders.STATUS.ko.name())
                       .getStats().add(startTime);
      }
  }

  /*****************************************
  *
  *  processGetSubscriberProfile
  *
  *****************************************/

  // this cached mecanism exist only because :
  // org.apache.kafka.streams.KafkaStreams.metadataForKey(java.lang.String, K, org.apache.kafka.common.serialization.Serializer<K>)
  // which call : org.apache.kafka.streams.processor.internals.StreamsMetadataState.getMetadataWithKey(java.lang.String, K, org.apache.kafka.common.serialization.Serializer<K>)
  // is synchronized, and so a real bottleneck on higly threaded HttpServer (we probably should move asyn NIO http server)
  private static Map<Integer,String> partitionHostMap = new ConcurrentHashMap<>();
  private static String getHostForKey(String key){
      // WARN: copy past default from org.apache.kafka.clients.producer.internals.DefaultPartitioner.partition()
      byte[] byteKey = StringKey.serde().serializer().serialize(null,new StringKey(key));
      int partition = Utils.toPositive(Utils.murmur2(byteKey)) % Deployment.getTopicSubscriberPartitions();
      // get from cache if possible
      if(partitionHostMap.get(partition)!=null) return partitionHostMap.get(partition);
      // else construct and update cache
      StreamsMetadata metadata = streams.metadataForKey(Deployment.getSubscriberStateChangeLog(), new StringKey(key), StringKey.serde().serializer());
      String toRet = metadata.host()+":"+metadata.port();
      partitionHostMap.put(partition,toRet);
      return toRet;
  }
  private byte[] processGetSubscriberProfile(String subscriberID, boolean includeExtendedSubscriberProfile, boolean forSupport) throws ServerException
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

    String hostPort = getHostForKey(subscriberID);

    //
    //  request
    //

    HashMap<String,Object> request = new HashMap<String,Object>();
    request.put("apiVersion", 1);
    request.put("subscriberID", subscriberID);
    request.put("includeExtendedSubscriberProfile", includeExtendedSubscriberProfile);
    request.put("forSupport", forSupport);
    JSONObject requestJSON = JSONUtilities.encodeObject(request);

    //
    //  httpPost
    //

    now = SystemTime.getCurrentTime();
    long waitTime = timeout.getTime() - now.getTime();
    HttpPost httpPost = new HttpPost("http://" + hostPort + "/nglm-evolutionengine/retrieveSubscriberProfile");
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

  private byte[] processRetrieveSubscriberProfile(String subscriberID, boolean includeExtendedSubscriberProfile, boolean forSupport) throws ServerException
  {
    /*****************************************
    *
    *  subscriberProfile
    *
    *****************************************/

    SubscriberProfile subscriberProfile = null;
    SubscriberState subscriberState = null;
    ExtendedSubscriberProfile extendedSubscriberProfile = null;

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
            long startTime = DurationStat.startTime();
            subscriberState = subscriberStateStore.get(new StringKey(subscriberID));
            statsStateStoresDuration.withLabel(StatsBuilders.LABEL.name.name(),"subscriberStateStore")
                                    .withLabel(StatsBuilders.LABEL.operation.name(),"get")
                                    .getStats().add(startTime);
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
                long startTime = DurationStat.startTime();
                extendedSubscriberProfile = extendedSubscriberProfileStore.get(new StringKey(subscriberID));
                statsStateStoresDuration.withLabel(StatsBuilders.LABEL.name.name(),"extendedSubscriberProfileStore")
                                        .withLabel(StatsBuilders.LABEL.operation.name(),"get")
                                        .getStats().add(startTime);
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
    *  response
    *
    *****************************************/

    // for support only, containing entire objects of statestores
    if(forSupport){

      byte[] toRet;

      // use the Avro formatter directly (same as avro console)
      Properties props = new Properties();
      props.put("schema.registry.url", System.getProperty("nglm.schemaRegistryURL"));
      props.put("print.key","false");
      props.put("line.separator","");
      AvroMessageFormatter messageFormatter = new AvroMessageFormatter();
      messageFormatter.init(props);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos);

      try {
        ps.write("{".getBytes());
        if(subscriberState!=null){
          // a fake record (partition and offset not accurate)
          ConsumerRecord<byte[],byte[]> record = new ConsumerRecord<>(Deployment.getSubscriberStateChangeLogTopic(),0,0L,null, SubscriberState.serde().serializer().serialize(Deployment.getSubscriberStateChangeLogTopic(), subscriberState));
          ps.write("\"subscriberState\":".getBytes());
          messageFormatter.writeTo(record,ps);
        }
        if(extendedSubscriberProfile!=null){
          // a fake record (partition and offset not accurate)
          ConsumerRecord<byte[],byte[]> record = new ConsumerRecord<>(Deployment.getExtendedSubscriberProfileChangeLogTopic(),0,0L,null, ExtendedSubscriberProfile.stateStoreSerde().serializer().serialize(Deployment.getExtendedSubscriberProfileChangeLogTopic(), extendedSubscriberProfile));
          if(subscriberState!=null) ps.write(",".getBytes());
          ps.write("\"extendedSubscriberProfile\":".getBytes());
          messageFormatter.writeTo(record,ps);
        }
        ps.write("}".getBytes());
        toRet = baos.toByteArray();
      } catch (IOException e) {
        throw new ServerException(e);
      }
      finally {
        ps.close();
        try { baos.close(); } catch (IOException e) {}
        messageFormatter.close();
      }

      // support call just care about this so far
      return toRet;
    }

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
    Date nextProcessingTime = RLMDateUtils.addSeconds(SystemTime.getActualCurrentTime(), 300);
    while (true)
      {
        synchronized (evolutionEngineStatistics)
          {
            //
            //  wait
            //

            Date now = SystemTime.getActualCurrentTime();
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
                now = SystemTime.getActualCurrentTime();
              }

            //
            //  log state store sizes
            //

            log.info("SubscriberStateSize: {}", evolutionEngineStatistics.getSubscriberStateSize().toString());
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

      JourneyRequest request = new JourneyRequest(evolutionEventContext, subscriberEvaluationRequest, deliveryRequestSource, journeyID, subscriberEvaluationRequest.getTenantID());

      /*****************************************
      *
      *  return request
      *
      *****************************************/

      return Collections.<Action>singletonList(request);
    }

    @Override public Map<String, String> getGUIDependencies(JourneyNode journeyNode, int tenantID)
    {
      Map<String, String> result = new HashMap<String, String>();
      String journeyID = (String) journeyNode.getNodeParameters().get("node.parameter.journey");
      if (journeyID != null) result.put("journey", journeyID);
      return result;
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

      JourneyRequest request = validParameters ? new JourneyRequest(evolutionEventContext, subscriberEvaluationRequest, deliveryRequestSource, workflowParameter.getWorkflowID(), boundParameters, subscriberEvaluationRequest.getTenantID()) : null;

      /*****************************************
      *
      *  return request
      *
      *****************************************/

      return (request != null) ? Collections.<Action>singletonList(request) : Collections.<Action>emptyList();
    }

    @Override public Map<String, String> getGUIDependencies(JourneyNode journeyNode, int tenantID)
    {
      Map<String, String> result = new HashMap<String, String>();
      WorkflowParameter workflowparam= (WorkflowParameter) journeyNode.getNodeParameters().get("node.parameter.workflow");
      String workflowID =workflowparam.getWorkflowID();
      if (workflowID != null) result.put("workflow", workflowID);
      return result;
    }
  }

  /*****************************************
  *
  *  class LoyaltyProgramAction
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

      LoyaltyProgramRequest request = new LoyaltyProgramRequest(evolutionEventContext, deliveryRequestSource, operation, loyaltyProgramID, subscriberEvaluationRequest.getTenantID());
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
  *  class UpdateProfileAction
  *
  *****************************************/

  public static class UpdateProfileAction extends ActionManager
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String attribute;
    private CriterionDataType dataType;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public UpdateProfileAction(JSONObject configuration) throws GUIManagerException
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
      
      Object value = null;
      String paramName = null;
      String attributeName = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.attribute.name");
      String attributeValue = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.attribute.value");
      SubscriberProfileForceUpdate update = new SubscriberProfileForceUpdate("dummy", evolutionEventContext.now(), new ParameterMap());
      update.getParameterMap().put(attributeName, attributeValue);
      update.getParameterMap().put("fromJourney", true);

      /*****************************************
      *
      *  return request
      *
      *****************************************/

      return Collections.<Action>singletonList(update);
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
    private Constructor<? extends EvolutionEngineEvent> eventConstructor;
    private boolean isAutoProvisionSubscriberStreamEvent;

    public EvolutionEngineEventDeclaration getEventDeclaration() { return eventDeclaration; }

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
        if(isAutoProvisionSubscriberStreamEvent = AutoProvisionSubscriberStreamEvent.class.isAssignableFrom(eventDeclaration.getEventClass()))
          {
            eventConstructor = eventDeclaration.getEventClass().getConstructor(new Class<?>[]{String.class, Date.class, JSONObject.class, int.class});
          }
        else
          {
            eventConstructor = eventDeclaration.getEventClass().getConstructor(new Class<?>[]{String.class, Date.class, JSONObject.class });
          }
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
          EvolutionEngineEvent event;
          event = (isAutoProvisionSubscriberStreamEvent)?
              eventConstructor.newInstance(subscriberID, SystemTime.getCurrentTime(), eventJSON, subscriberEvaluationRequest.getTenantID()):
              eventConstructor.newInstance(subscriberID, SystemTime.getCurrentTime(), eventJSON);

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
  
  public static class JourneyTriggerEventAction extends SubscriberStreamOutput implements Action {

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
  
  public static class VoucherActionManager extends ActionManager
  {
    public enum Operation 
    {
      Redeem("redeem"),
      Validate("validate"),
      Unknown("(unknown)");
      private String externalRepresentation;
      private Operation(String externalRepresentation) { this.externalRepresentation = externalRepresentation;}
      public String getExternalRepresentation() { return externalRepresentation; }
      public static Operation fromExternalRepresentation(String externalRepresentation) { for (Operation enumeratedValue : Operation.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
    }
    
    /*****************************************
    *
    *  data
    *
    *****************************************/
    
    private String origin;
    private String moduleID;
    private Operation operation;


    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public VoucherActionManager(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
      this.origin = JSONUtilities.decodeString(configuration, "origin", true);
      this.moduleID = JSONUtilities.decodeString(configuration, "moduleID", true);
      this.operation = Operation.fromExternalRepresentation(JSONUtilities.decodeString(configuration, "operation", true));
    }

    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      List<Action> actions = new ArrayList<Action>();
      SubscriberProfile subscriberProfile = subscriberEvaluationRequest.getSubscriberProfile();
      int tenantID = subscriberProfile.getTenantID();
      subscriberEvaluationRequest.getJourneyState().getVoucherChanges().clear();
      Date now = SystemTime.getCurrentTime();
      this.origin = subscriberEvaluationRequest.getJourneyNode().getNodeName();

      /*****************************************
      *
      *  request arguments
      *
      *****************************************/

      String voucherCode = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.voucher.code");
      String supplierDisplay = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.supplier");
      // special internal context variable set for supplier
      if(supplierDisplay!=null && subscriberEvaluationRequest.getJourneyState().getJourneyParameters()!=null) subscriberEvaluationRequest.getJourneyState().getJourneyParameters().put("variable."+INTERNAL_VARIABLE_SUPPLIER,supplierDisplay);

      String journeyID = subscriberEvaluationRequest.getJourneyState().getJourneyID();

      VoucherAction voucherActionEvent = new VoucherAction(subscriberProfile.getSubscriberID(), now, voucherCode, RESTAPIGenericReturnCodes.UNKNOWN.getGenericResponseMessage(), RESTAPIGenericReturnCodes.UNKNOWN.getGenericResponseCode(), operation.getExternalRepresentation());

      if (operation == Operation.Redeem)
        {
          try
            {
              VoucherProfileStored voucherProfileStored = getStoredVoucher(voucherCode, supplierDisplay, subscriberProfile);
              VoucherChange voucherChange = new VoucherChange(subscriberProfile.getSubscriberID(), now, null, evolutionEventContext.getEventID(), VoucherChangeAction.Redeem, voucherProfileStored.getVoucherCode(), voucherProfileStored.getVoucherID(), voucherProfileStored.getFileID(), moduleID, journeyID, origin, RESTAPIGenericReturnCodes.UNKNOWN, tenantID);
              for (VoucherProfileStored voucherStored : subscriberProfile.getVouchers())
                {
                  if (voucherStored.getVoucherCode().equals(voucherChange.getVoucherCode()) && voucherStored.getVoucherID().equals(voucherChange.getVoucherID()))
                    {
                      checkRedeemVoucher(voucherStored, voucherChange, true);
                      if (voucherChange.getReturnStatus() == RESTAPIGenericReturnCodes.SUCCESS) break;
                    }

                }
              evolutionEventContext.getSubscriberState().getVoucherChanges().add(voucherChange);
              subscriberEvaluationRequest.getJourneyState().getVoucherChanges().add(voucherChange);
              voucherActionEvent.setActionStatus(voucherChange.getReturnStatus().getGenericResponseMessage());
              voucherActionEvent.setActionStatusCode(voucherChange.getReturnStatus().getGenericResponseCode());
            }
          catch (ThirdPartyManagerException e)
            {
              voucherActionEvent.setActionStatus(e.getMessage());
              voucherActionEvent.setActionStatusCode(e.getResponseCode());
            }
          actions.add(voucherActionEvent);
        }
      else if (operation == Operation.Validate)
        {
          try
            {
              VoucherProfileStored voucherProfileStored = getStoredVoucher(voucherCode, supplierDisplay, subscriberProfile);
              VoucherChange voucherChange = new VoucherChange(subscriberProfile.getSubscriberID(), now, null, "", VoucherChangeAction.Unknown, voucherProfileStored.getVoucherCode(), voucherProfileStored.getVoucherID(), voucherProfileStored.getFileID(), moduleID, journeyID, origin, RESTAPIGenericReturnCodes.SUCCESS, tenantID);
              subscriberEvaluationRequest.getJourneyState().getVoucherChanges().add(voucherChange);
              voucherActionEvent.setActionStatus(voucherChange.getReturnStatus().getGenericResponseMessage());
              voucherActionEvent.setActionStatusCode(voucherChange.getReturnStatus().getGenericResponseCode());
            }
          catch (ThirdPartyManagerException e)
            {
              voucherActionEvent.setActionStatus(e.getMessage());
              voucherActionEvent.setActionStatusCode(e.getResponseCode());
            }
          actions.add(voucherActionEvent);
        }
      
      if (log.isDebugEnabled()) log.debug("VoucherActionManager - VoucherAction {}, journeyID {}, voucherActionEvent is {} and supplier is {}", operation, journeyID, voucherActionEvent, supplierDisplay);

      return actions;

    }
  }
  
  public static VoucherProfileStored getStoredVoucher(String voucherCode, String supplierDisplay, SubscriberProfile subscriberProfile) throws ThirdPartyManagerException
  {
    Date now = SystemTime.getCurrentTime();
    Supplier supplier = null;
    for (Supplier supplierConf : supplierService.getActiveSuppliers(now, subscriberProfile.getTenantID()))
      {
        if (supplierConf.getGUIManagedObjectDisplay().equals(supplierDisplay))
          {
            supplier = supplierConf;
            break;
          }
      }
    if (supplier == null)
      {
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.PARTNER_NOT_FOUND);
      }

    ThirdPartyManagerException errorException = null;
    VoucherProfileStored voucherStored = null;
    for (VoucherProfileStored profileVoucher : subscriberProfile.getVouchers())
      {
        Voucher voucher = voucherService.getActiveVoucher(profileVoucher.getVoucherID(), now);
        // a voucher in subscriber profile with no more voucher conf associated, very
        // likely to happen
        if (voucher == null)
          {
            if (errorException == null) errorException = new ThirdPartyManagerException(RESTAPIGenericReturnCodes.VOUCHER_CODE_NOT_FOUND);
            continue;
          }
        if (voucherCode.equals(profileVoucher.getVoucherCode()) && supplier.getSupplierID().equals(voucher.getSupplierID()))
          {
            if (profileVoucher.getVoucherStatus() == VoucherDelivery.VoucherStatus.Redeemed)
              {
                errorException = new ThirdPartyManagerException(RESTAPIGenericReturnCodes.VOUCHER_ALREADY_REDEEMED);
              } 
            else if (profileVoucher.getVoucherStatus() == VoucherDelivery.VoucherStatus.Expired || profileVoucher.getVoucherExpiryDate().before(now))
              {
                errorException = new ThirdPartyManagerException(RESTAPIGenericReturnCodes.VOUCHER_EXPIRED);
              } 
            else
              {
                voucherStored = profileVoucher;
                break;
              }
          }
      }

    if (voucherStored == null)
      {
        if (errorException != null) throw errorException;
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.VOUCHER_NOT_ASSIGNED);
      }
    return voucherStored;
  }
  
  private static boolean shoudGenerateEDR(SubscriberStreamEvent event)
  {
    boolean result = false;
    if (event instanceof EvolutionEngineEvent)
      {
        EvolutionEngineEvent engineEvent = (EvolutionEngineEvent) event;
        EvolutionEngineEventDeclaration declaration = Deployment.getEvolutionEngineEvents().get(engineEvent.getEventName());
        if (declaration != null && declaration.getEdrCriterionFieldsMapping() != null && !declaration.getEdrCriterionFieldsMapping().isEmpty())
          {
            result = true;
          }
      }
    return result;
  }
}
