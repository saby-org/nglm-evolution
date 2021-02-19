/*****************************************************************************
 *
 *  ThirdPartyManager.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.evolving.nglm.core.*;
import com.evolving.nglm.evolution.statistics.CounterStat;
import com.evolving.nglm.evolution.statistics.DurationStat;
import com.evolving.nglm.evolution.statistics.StatBuilder;
import com.evolving.nglm.evolution.statistics.StatsBuilders;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.LicenseChecker.LicenseState;
import com.evolving.nglm.core.SubscriberIDService.SubscriberIDServiceException;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryRequest;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.DeliveryRequest.ActivityType;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.EmptyFulfillmentManager.EmptyFulfillmentRequest;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.Journey.TargetingType;
import com.evolving.nglm.evolution.JourneyHistory.NodeHistory;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.LoyaltyProgramHistory.TierHistory;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.NotificationManager.NotificationManagerRequest;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.PushNotificationManager.PushNotificationManagerRequest;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;
import com.evolving.nglm.evolution.SubscriberProfile.ValidateUpdateProfileRequestException;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;
import com.evolving.nglm.evolution.Token.TokenStatus;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;
import com.evolving.nglm.evolution.offeroptimizer.DNBOMatrixAlgorithmParameters;
import com.evolving.nglm.evolution.offeroptimizer.GetOfferException;
import com.evolving.nglm.evolution.offeroptimizer.ProposedOfferDetails;
import com.evolving.nglm.evolution.reports.bdr.BDRReportDriver;
import com.evolving.nglm.evolution.reports.bdr.BDRReportMonoPhase;
import com.evolving.nglm.evolution.reports.journeycustomerstatistics.JourneyCustomerStatisticsReportDriver;
import com.evolving.nglm.evolution.reports.notification.NotificationReportDriver;
import com.evolving.nglm.evolution.reports.notification.NotificationReportMonoPhase;
import com.evolving.nglm.evolution.reports.odr.ODRReportDriver;
import com.evolving.nglm.evolution.reports.odr.ODRReportMonoPhase;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class ThirdPartyManager 
{
  private static final String DEFAULT_FEATURE_ID = "<anonymous>";
  private static final String CUSTOMER_ID = "customerID";
  private static final DeliveryRequest.DeliveryPriority DELIVERY_REQUEST_PRIORITY = DeliveryRequest.DeliveryPriority.High;
  private static String elasticSearchDateFormat = com.evolving.nglm.core.Deployment.getElasticsearchDateFormat();
  private static DateFormat esDateFormat = new SimpleDateFormat(elasticSearchDateFormat);

  /*****************************************
   *
   *  ProductID
   *
   *****************************************/

  public static String ProductID = "Evolution-ThirdPartyManager";

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ThirdPartyManager.class);

  /*****************************************
   *
   *  data
   *
   *****************************************/

  private KafkaProducer<byte[], byte[]> kafkaProducer;
  private OfferService offerService;
  private SubscriberProfileService subscriberProfileService;
  private SegmentationDimensionService segmentationDimensionService;
  private JourneyService journeyService;
  private JourneyObjectiveService journeyObjectiveService;
  private LoyaltyProgramService loyaltyProgramService;
  private PointService pointService;
  private PaymentMeanService paymentMeanService;
  private OfferObjectiveService offerObjectiveService;
  private SubscriberMessageTemplateService subscriberMessageTemplateService;
  private SalesChannelService salesChannelService;
  private ResellerService resellerService;
  private SubscriberIDService subscriberIDService;
  private ProductService productService;
  private VoucherService voucherService;
  private DeliverableService deliverableService;
  private TokenTypeService tokenTypeService;
  private ScoringStrategyService scoringStrategyService;
  private PresentationStrategyService presentationStrategyService;
  private ProductTypeService productTypeService;
  private VoucherTypeService voucherTypeService;
  private CatalogCharacteristicService catalogCharacteristicService;
  private DNBOMatrixService dnboMatrixService;
  private DynamicCriterionFieldService dynamicCriterionFieldService;
  private SupplierService supplierService;
  private CallingChannelService callingChannelService;
  private ExclusionInclusionTargetService exclusionInclusionTargetService;

  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private static final int RESTAPIVersion = 1;
  private HttpServer restServer;
  private ZookeeperUniqueKeyServer zuks;
  private ZookeeperUniqueKeyServer zuksVoucherChange;
  private ElasticsearchClientAPI elasticsearch;
  private KafkaResponseListenerService<StringKey,PurchaseFulfillmentRequest> purchaseResponseListenerService;
  private KafkaResponseListenerService<StringKey,VoucherChange> voucherChangeResponseListenerService;
  private static Map<String, ThirdPartyMethodAccessLevel> methodPermissionsMapper = new LinkedHashMap<String,ThirdPartyMethodAccessLevel>();
  private static Map<String, Constructor<? extends SubscriberStreamOutput>> JSON3rdPartyEventsConstructor = new HashMap<>();
  private static Integer authResponseCacheLifetimeInMinutes = null;
  private static final String GENERIC_RESPONSE_CODE = "responseCode";
  private static final String GENERIC_RESPONSE_MSG = "responseMessage";
  private static final String GENERIC_RESPONSE_DESCRIPTION = "description";
  private static final String GENERIC_RESPONSE_DETAILS = "responseDetails";
  private String getCustomerAlternateID;
  public static final String REQUEST_DATE_PATTERN = "\\d{4}-\\d{2}-\\d{2}"; //Represents exact yyyy-MM-dd
  public static final String REQUEST_DATE_FORMAT= "yyyy-MM-dd";
  // all this conf which should not makes no sense at then end :
  //private static final Class<?> PURCHASE_FULFILLMENT_REQUEST_CLASS = com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest.class;
  public static final String PURCHASE_FULFILLMENT_MANAGER_TYPE = "purchaseFulfillment";


  /*****************************************
   *
   *  configuration
   *
   *****************************************/

  private static final String ENV_CONF_THIRDPARTY_HTTP_CLIENT_TIMEOUT_MS = "THIRDPARTY_HTTP_CLIENT_TIMEOUT_MS";
  private static int httpTimeout = 10000;
  static{
    String timeoutConf = System.getenv().get(ENV_CONF_THIRDPARTY_HTTP_CLIENT_TIMEOUT_MS);
    if(timeoutConf!=null && !timeoutConf.isEmpty()){
      try{
        httpTimeout=Integer.parseInt(timeoutConf);
        log.info("loading env conf "+ENV_CONF_THIRDPARTY_HTTP_CLIENT_TIMEOUT_MS+" "+httpTimeout);
      }catch (NumberFormatException e){
        log.warn("bad env conf "+ENV_CONF_THIRDPARTY_HTTP_CLIENT_TIMEOUT_MS, e);
      }
    }
  }
  private String fwkServer = null;
  private String guimanagerHost = null;
  private int guimanagerPort;
  RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(httpTimeout).setSocketTimeout(httpTimeout).setConnectionRequestTimeout(httpTimeout).build();
  private HttpClient httpClient;

  /*****************************************
   *
   *  enum
   *
   *****************************************/

  enum API // package visible
  {
    ping(1),
    getCustomer(2),
    getCustomerBDRs(3),
    getCustomerODRs(4),
    getCustomerPoints(5),
    creditBonus(6),
    debitBonus(7),
    getCustomerMessages(8),
    getCustomerJourneys(9),
    getCustomerCampaigns(10),
    getCustomerLoyaltyPrograms(11),
    getLoyaltyProgram(12),
    getLoyaltyProgramList(13),
    getOffersList(14),
    getCustomerAvailableCampaigns(17),
    updateCustomer(18),
    updateCustomerParent(19),
    removeCustomerParent(20),
    getCustomerNBOs(21),
    getTokensCodesList(22),
    acceptOffer(23),
    purchaseOffer(24),
    triggerEvent(25),
    enterCampaign(26),
    loyaltyProgramOptIn(27),
    loyaltyProgramOptOut(28),
    validateVoucher(29),
    redeemVoucher(30),
    getCustomerTokenAndNBO(31),
    putSimpleOffer(32),
    getSimpleOfferList(33),
    removeSimpleOffer(34),
    getResellerDetails(35);
    private int methodIndex;
    private API(int methodIndex) { this.methodIndex = methodIndex; }
    public int getMethodIndex() { return methodIndex; }
  }

  //
  //  license
  //

  private LicenseChecker licenseChecker = null;

  //
  // stats
  //

  private StatBuilder<DurationStat> statsDuration = null;

  //
  //  authCache
  //

  TimebasedCache<ThirdPartyCredential, AuthenticatedResponse> authCache = null;

  /*****************************************
   *
   *  main
   *
   *****************************************/

  public static void main(String[] args)
  {
    NGLMRuntime.initialize(true);
    ThirdPartyManager thirdPartyManager = new ThirdPartyManager();
    new LoggerInitialization().initLogger();
    thirdPartyManager.start(args);
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

    String apiProcessKey = args[0];
    String bootstrapServers = args[1];
    int apiRestPort = parseInteger("apiRestPort", args[2]);
    String fwkServer = args[3];
    int threadPoolSize = parseInteger("threadPoolSize", args[4]);
    String elasticsearchServerHost = args[5];
    int elasticsearchServerPort = Integer.parseInt(args[6]);
    int connectTimeout = Deployment.getElasticsearchConnectionSettings().get("ThirdPartyManager").getConnectTimeout();
    int queryTimeout = Deployment.getElasticsearchConnectionSettings().get("ThirdPartyManager").getQueryTimeout();
    String userName = args[7];
    String userPassword = args[8];
    String guimanagerHost = args[9];
    int guimanagerPort = Integer.parseInt(args[10]);
    String nodeID = System.getProperty("nglm.license.nodeid");
    String offerTopic = Deployment.getOfferTopic();
    String subscriberGroupEpochTopic = Deployment.getSubscriberGroupEpochTopic();
    String journeyTopic = Deployment.getJourneyTopic();
    String journeyObjectiveTopic = Deployment.getJourneyObjectiveTopic();
    String loyaltyProgramTopic = Deployment.getLoyaltyProgramTopic();
    String pointTopic = Deployment.getPointTopic();
    String paymentMeanTopic = Deployment.getPaymentMeanTopic();
    String offerObjectiveTopic = Deployment.getOfferObjectiveTopic();
    String segmentationDimensionTopic = Deployment.getSegmentationDimensionTopic();
    String redisServer = Deployment.getRedisSentinels();
    String subscriberProfileEndpoints = Deployment.getSubscriberProfileEndpoints();
    methodPermissionsMapper = Deployment.getThirdPartyMethodPermissionsMap();
    authResponseCacheLifetimeInMinutes = Deployment.getAuthResponseCacheLifetimeInMinutes() == null ? new Integer(0) : Deployment.getAuthResponseCacheLifetimeInMinutes();

    //
    //  log
    //

    log.info("main START: {} {} {} {} {} {}", apiProcessKey, bootstrapServers, apiRestPort, fwkServer, threadPoolSize, authResponseCacheLifetimeInMinutes);

    /*****************************************
     *
     *  FWK Server
     *
     *****************************************/

    this.fwkServer = fwkServer;
    this.guimanagerHost = guimanagerHost;
    this.guimanagerPort = guimanagerPort;

    //
    //  license
    //

    licenseChecker = new LicenseChecker(ProductID, nodeID, Deployment.getZookeeperRoot(), Deployment.getZookeeperConnect());

    //
    //  statistics
    //

    statsDuration = StatsBuilders.getEvolutionDurationStatisticsBuilder("thirdpartyaccess","thirdpartymanager-" + apiProcessKey);

    //
    // authCache
    //

    authCache = TimebasedCache.getInstance(60000*authResponseCacheLifetimeInMinutes);
    
    //
    // ZookeeperUniqueKeyServer
    //
    
    zuks = new ZookeeperUniqueKeyServer("commoditydelivery");
    zuksVoucherChange = new ZookeeperUniqueKeyServer("voucherchange");

    //
    // elasticsearch
    //

    try
    {
      elasticsearch = new ElasticsearchClientAPI(elasticsearchServerHost, elasticsearchServerPort, connectTimeout, queryTimeout, userName, userPassword);
    }
    catch (ElasticsearchException e)
    {
      throw new ServerRuntimeException("could not initialize elasticsearch client", e);
    }
    
    /*****************************************
    *
    *  http
    *
    *****************************************/

    //
    //  default connections
    //

    PoolingHttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager();
    httpClientConnectionManager.setDefaultMaxPerRoute(threadPoolSize);
    httpClientConnectionManager.setMaxTotal(threadPoolSize);

    //
    //  httpClient
    //

    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    httpClientBuilder.setConnectionManager(httpClientConnectionManager);
    this.httpClient = httpClientBuilder.build();



    /*****************************************
     *
     *  kafka producer for the segmentationDimensionListener
     *
     *****************************************/

    Properties producerProperties = new Properties();
    producerProperties.put("bootstrap.servers", bootstrapServers);
    producerProperties.put("acks", "all");
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);

    /*****************************************
     *
     *  services
     *
     *****************************************/

    //
    //  construct & start
    //

    subscriberProfileService = new EngineSubscriberProfileService(subscriberProfileEndpoints, threadPoolSize);
    subscriberProfileService.start();

    dynamicCriterionFieldService = new DynamicCriterionFieldService(bootstrapServers, "thirdpartymanager-dynamiccriterionfieldservice-"+apiProcessKey, Deployment.getDynamicCriterionFieldTopic(), false);
    dynamicCriterionFieldService.start();
    CriterionContext.initialize(dynamicCriterionFieldService);

    // Services
    
    offerService = new OfferService(bootstrapServers, "thirdpartymanager-offerservice-" + apiProcessKey, offerTopic, false);
    offerService.start();

    segmentationDimensionService = new SegmentationDimensionService(bootstrapServers, "thirdpartymanager-segmentationDimensionservice-001", segmentationDimensionTopic, false);
    segmentationDimensionService.start();
    
    journeyService = new JourneyService(bootstrapServers, "thirdpartymanager-journeyservice-" + apiProcessKey, journeyTopic, false);
    journeyService.start();
    
    journeyObjectiveService = new JourneyObjectiveService(bootstrapServers, "thirdpartymanager-journeyObjectiveService-" + apiProcessKey, journeyObjectiveTopic, false);
    journeyObjectiveService.start();
    
    loyaltyProgramService = new LoyaltyProgramService(bootstrapServers, "thirdpartymanager-loyaltyprogramservice-" + apiProcessKey, loyaltyProgramTopic, false);
    loyaltyProgramService.start();
    
    pointService = new PointService(bootstrapServers, "thirdpartymanager-pointservice-" + apiProcessKey, pointTopic, false);
    pointService.start();
    
    paymentMeanService = new PaymentMeanService(bootstrapServers, "thirdpartymanager-paymentmeanservice-" + apiProcessKey, paymentMeanTopic, false);
    paymentMeanService.start();
    
    offerObjectiveService = new OfferObjectiveService(bootstrapServers, "thirdpartymanager-offerObjectiveService-" + apiProcessKey, offerObjectiveTopic, false);
    offerObjectiveService.start();
    
    subscriberMessageTemplateService = new SubscriberMessageTemplateService(bootstrapServers, "thirdpartymanager-subscribermessagetemplateservice-" + apiProcessKey, Deployment.getSubscriberMessageTemplateTopic(), false);
    subscriberMessageTemplateService.start();
    
    salesChannelService = new SalesChannelService(bootstrapServers, "thirdpartymanager-salesChannelService-" + apiProcessKey, Deployment.getSalesChannelTopic(), false);
    salesChannelService.start();
    
    resellerService = new ResellerService(bootstrapServers, "thirdpartymanager-resellerService-" + apiProcessKey, Deployment.getResellerTopic(), false);
    resellerService.start();
    
    tokenTypeService = new TokenTypeService(bootstrapServers, "thirdpartymanager-tokentypeservice-" + apiProcessKey, Deployment.getTokenTypeTopic(), false);
    tokenTypeService.start();
    
    productService = new ProductService(bootstrapServers, "thirdpartymanager-productservice-" + apiProcessKey, Deployment.getProductTopic(), false);
    productService.start();
    
    voucherService = new VoucherService(bootstrapServers, "thirdpartymanager-voucherservice-" + apiProcessKey, Deployment.getVoucherTopic());
    voucherService.start();

    deliverableService = new DeliverableService(bootstrapServers, "thirdpartymanager-deliverableservice-" + apiProcessKey, Deployment.getDeliverableTopic(), false);
    deliverableService.start();
    
    scoringStrategyService = new ScoringStrategyService(Deployment.getBrokerServers(), "thirdpartymanager-scoringstrategyservice-" + apiProcessKey, Deployment.getScoringStrategyTopic(), false);
    scoringStrategyService.start();
    
    presentationStrategyService = new PresentationStrategyService(bootstrapServers, "thirdpartymanager-presentationstrategyservice-" + apiProcessKey, Deployment.getPresentationStrategyTopic(), false);
    presentationStrategyService.start();
    
    productTypeService = new ProductTypeService(Deployment.getBrokerServers(), "thirdpartymanager-producttypeservice-" + apiProcessKey, Deployment.getProductTypeTopic(), false);
    productTypeService.start();
    
    voucherTypeService = new VoucherTypeService(Deployment.getBrokerServers(), "thirdpartymanager-vouchertypeservice-" + apiProcessKey, Deployment.getVoucherTypeTopic(), false);
    voucherTypeService.start();

    dnboMatrixService = new DNBOMatrixService(Deployment.getBrokerServers(),"thirdpartymanager-matrixservice"+apiProcessKey,Deployment.getDNBOMatrixTopic(),false);
    dnboMatrixService.start();
    
    catalogCharacteristicService = new CatalogCharacteristicService(Deployment.getBrokerServers(), "thirdpartymanager-catalogcharacteristicservice-" + apiProcessKey, Deployment.getCatalogCharacteristicTopic(), false);
    catalogCharacteristicService.start();
    
    supplierService = new SupplierService(Deployment.getBrokerServers(), "thirdpartymanager-supplierservice-" + apiProcessKey, Deployment.getSupplierTopic(), false);
    supplierService.start();
    
    callingChannelService = new CallingChannelService(Deployment.getBrokerServers(), "thirdpartymanager-callingchannelservice-" + apiProcessKey, Deployment.getCallingChannelTopic(), false);
    callingChannelService.start();
    
    exclusionInclusionTargetService = new ExclusionInclusionTargetService(Deployment.getBrokerServers(), "thirdpartymanager-exclusionInclusionTargetService-" + apiProcessKey, Deployment.getExclusionInclusionTargetTopic(), false);
    exclusionInclusionTargetService.start();

    subscriberIDService = new SubscriberIDService(redisServer, "thirdpartymanager-" + apiProcessKey);
    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("thirdpartymanager-subscribergroupepoch", bootstrapServers, subscriberGroupEpochTopic, SubscriberGroupEpoch::unpack);

    DeliveryManagerDeclaration dmd = Deployment.getDeliveryManagers().get(PURCHASE_FULFILLMENT_MANAGER_TYPE);
    purchaseResponseListenerService = new KafkaResponseListenerService<>(Deployment.getBrokerServers(),dmd.getResponseTopic(DELIVERY_REQUEST_PRIORITY),StringKey.serde(),PurchaseFulfillmentRequest.serde());
    purchaseResponseListenerService.start();

    voucherChangeResponseListenerService = new KafkaResponseListenerService<>(Deployment.getBrokerServers(),Deployment.getVoucherChangeResponseTopic(),StringKey.serde(),VoucherChange.serde());
    voucherChangeResponseListenerService.start();

    /*****************************************
     *
     *  REST interface -- server and handlers
     *
     *****************************************/

    try
    {
      InetSocketAddress addr = new InetSocketAddress(apiRestPort);
      restServer = HttpServer.create(addr, 0);
      restServer.createContext("/nglm-thirdpartymanager/ping", new APIHandler(API.ping));
      restServer.createContext("/nglm-thirdpartymanager/getCustomer", new APIHandler(API.getCustomer));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerBDRs", new APIHandler(API.getCustomerBDRs));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerODRs", new APIHandler(API.getCustomerODRs));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerPoints", new APIHandler(API.getCustomerPoints));
      restServer.createContext("/nglm-thirdpartymanager/creditBonus", new APIHandler(API.creditBonus));
      restServer.createContext("/nglm-thirdpartymanager/debitBonus", new APIHandler(API.debitBonus));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerMessages", new APIHandler(API.getCustomerMessages));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerJourneys", new APIHandler(API.getCustomerJourneys));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerCampaigns", new APIHandler(API.getCustomerCampaigns));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerLoyaltyPrograms", new APIHandler(API.getCustomerLoyaltyPrograms));
      restServer.createContext("/nglm-thirdpartymanager/getLoyaltyProgram", new APIHandler(API.getLoyaltyProgram));
      restServer.createContext("/nglm-thirdpartymanager/getLoyaltyProgramList", new APIHandler(API.getLoyaltyProgramList));
      restServer.createContext("/nglm-thirdpartymanager/getOffersList", new APIHandler(API.getOffersList));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerAvailableCampaigns", new APIHandler(API.getCustomerAvailableCampaigns));
      restServer.createContext("/nglm-thirdpartymanager/updateCustomer", new APIHandler(API.updateCustomer));
      restServer.createContext("/nglm-thirdpartymanager/updateCustomerParent", new APIHandler(API.updateCustomerParent));
      restServer.createContext("/nglm-thirdpartymanager/removeCustomerParent", new APIHandler(API.removeCustomerParent));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerNBOs", new APIHandler(API.getCustomerNBOs));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerTokenAndNBO", new APIHandler(API.getCustomerTokenAndNBO));
      restServer.createContext("/nglm-thirdpartymanager/getTokensCodesList", new APIHandler(API.getTokensCodesList));
      restServer.createContext("/nglm-thirdpartymanager/acceptOffer", new APIHandler(API.acceptOffer));
      restServer.createContext("/nglm-thirdpartymanager/purchaseOffer", new APIHandler(API.purchaseOffer));
      restServer.createContext("/nglm-thirdpartymanager/triggerEvent", new APIHandler(API.triggerEvent));
      restServer.createContext("/nglm-thirdpartymanager/enterCampaign", new APIHandler(API.enterCampaign));
      restServer.createContext("/nglm-thirdpartymanager/loyaltyProgramOptIn", new APIHandler(API.loyaltyProgramOptIn));
      restServer.createContext("/nglm-thirdpartymanager/loyaltyProgramOptOut", new APIHandler(API.loyaltyProgramOptOut));
      restServer.createContext("/nglm-thirdpartymanager/validateVoucher", new APIHandler(API.validateVoucher));
      restServer.createContext("/nglm-thirdpartymanager/redeemVoucher", new APIHandler(API.redeemVoucher));
      restServer.createContext("/nglm-thirdpartymanager/putSimpleOffer", new APIHandler(API.putSimpleOffer));
      restServer.createContext("/nglm-thirdpartymanager/getSimpleOfferList", new APIHandler(API.getSimpleOfferList));
      restServer.createContext("/nglm-thirdpartymanager/removeSimpleOffer", new APIHandler(API.removeSimpleOffer));
      restServer.createContext("/nglm-thirdpartymanager/getResellerDetails", new APIHandler(API.getResellerDetails));
      restServer.setExecutor(Executors.newFixedThreadPool(threadPoolSize));
      restServer.start();

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

    NGLMRuntime.addShutdownHook(new ShutdownHook(kafkaProducer, restServer, dynamicCriterionFieldService, offerService, subscriberProfileService, segmentationDimensionService, journeyService, journeyObjectiveService, loyaltyProgramService, pointService, paymentMeanService, offerObjectiveService, subscriberMessageTemplateService, salesChannelService, resellerService, subscriberIDService, subscriberGroupEpochReader, productService, deliverableService, callingChannelService, exclusionInclusionTargetService));

    /*****************************************
     *
     *  log restServerStarted
     *
     *****************************************/

    log.info("main restServerStarted");
    log.info("methodPermissionsMapper : {} ", methodPermissionsMapper);
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

    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private HttpServer restServer;
    private OfferService offerService;
    private SubscriberProfileService subscriberProfileService;
    private SegmentationDimensionService segmentationDimensionService;
    private JourneyService journeyService;
    private JourneyObjectiveService journeyObjectiveService;
    private LoyaltyProgramService loyaltyProgramService;
    private PointService pointService;
    private PaymentMeanService paymentMeanService;
    private OfferObjectiveService offerObjectiveService;
    private SubscriberMessageTemplateService subscriberMessageTemplateService;
    private SalesChannelService salesChannelService;
    private ResellerService resellerService;
    private SubscriberIDService subscriberIDService;
    private ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader;
    private ProductService productService;
    private DeliverableService deliverableService;
    private DynamicCriterionFieldService dynamicCriterionFieldService;
    private CallingChannelService callingChannelService;
    private ExclusionInclusionTargetService exclusionInclusionTargetService;    

    //
    //  constructor
    //

    private ShutdownHook(KafkaProducer<byte[], byte[]> kafkaProducer, HttpServer restServer, DynamicCriterionFieldService dynamicCriterionFieldService, OfferService offerService, SubscriberProfileService subscriberProfileService, SegmentationDimensionService segmentationDimensionService, JourneyService journeyService, JourneyObjectiveService journeyObjectiveService, LoyaltyProgramService loyaltyProgramService, PointService pointService, PaymentMeanService paymentMeanService, OfferObjectiveService offerObjectiveService, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, ResellerService resellerService, SubscriberIDService subscriberIDService, ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader, ProductService productService, DeliverableService deliverableService, CallingChannelService callingChannelService, ExclusionInclusionTargetService exclusionInclusionTargetService)
    {
      this.kafkaProducer = kafkaProducer;
      this.restServer = restServer;
      this.dynamicCriterionFieldService = dynamicCriterionFieldService;
      this.offerService = offerService;
      this.subscriberProfileService = subscriberProfileService;
      this.segmentationDimensionService = segmentationDimensionService;
      this.journeyService = journeyService;
      this.journeyObjectiveService = journeyObjectiveService;
      this.loyaltyProgramService = loyaltyProgramService;
      this.pointService = pointService;
      this.paymentMeanService = paymentMeanService;
      this.offerObjectiveService = offerObjectiveService;
      this.subscriberMessageTemplateService = subscriberMessageTemplateService;
      this.salesChannelService = salesChannelService;
      this.resellerService = resellerService;
      this.subscriberIDService = subscriberIDService;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
      this.productService = productService;
      this.deliverableService = deliverableService;
      this.callingChannelService = callingChannelService;
      this.exclusionInclusionTargetService = exclusionInclusionTargetService;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {

      //
      //  services
      //

      if (dynamicCriterionFieldService != null) dynamicCriterionFieldService.stop();
      if (offerService != null) offerService.stop();
      if (subscriberProfileService != null) subscriberProfileService.stop();
      if (segmentationDimensionService != null) segmentationDimensionService.stop();
      if (journeyService != null) journeyService.stop();
      if (journeyObjectiveService != null) journeyObjectiveService.stop();
      if (loyaltyProgramService != null) loyaltyProgramService.stop();
      if (pointService != null) pointService.stop();
      if (paymentMeanService != null) paymentMeanService.stop();
      if (offerObjectiveService != null ) offerObjectiveService.stop();
      if (subscriberMessageTemplateService != null) subscriberMessageTemplateService.stop();
      if (salesChannelService != null) salesChannelService.stop();
      if (resellerService != null) resellerService.stop();
      if (subscriberIDService != null) subscriberIDService.stop();
      if (productService != null) productService.stop();
      if (deliverableService != null) deliverableService.stop();
      if (callingChannelService != null) callingChannelService.stop();
      if (exclusionInclusionTargetService != null) exclusionInclusionTargetService.stop();
      
      //
      //  rest server
      //

      if (restServer != null) restServer.stop(1);

      //
      //  kafkaProducer
      //

      if (kafkaProducer != null) kafkaProducer.close();
    }
  }

  /*****************************************
   *
   *  parseInteger
   *
   *****************************************/

  private int parseInteger(String field, String stringValue)
  {
    int result = 0;
    try
    {
      result = Integer.parseInt(stringValue);
    }
    catch (NumberFormatException e)
    {
      throw new ServerRuntimeException("bad " + field + " argument", e);
    }
    return result;
  }

  /*****************************************
   *
   *  handleAPI
   *
   *****************************************/

  private void handleAPI(API api, HttpExchange exchange) throws IOException
  {
    // save start time for call latency stats
    long startTime = DurationStat.startTime();

    try
    {
      /*****************************************
      *
      *  validateURIandContext
      *
      *****************************************/
      
      validateURIandContext(exchange);
      
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
      if (log.isDebugEnabled()) log.debug("API (raw request): {} {}",api,requestBodyStringBuilder.toString());
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
      jsonRoot.remove("apiVersion");

      Boolean sync = JSONUtilities.decodeBoolean(jsonRoot, "sync",false);
      if(sync==null) sync = false;
      jsonRoot.remove("sync");

      /*****************************************
       *
       *  authenticate and accessCheck
       *
       *****************************************/

      if (! Deployment.getRegressionMode())
        {
          authenticateAndCheckAccess(jsonRoot, api.name());
        }

      /*****************************************
       *
       *  license state
       *
       *****************************************/

      LicenseState licenseState = licenseChecker.checkLicense();
      Alarm licenseAlarm = licenseState.getHighestAlarm();
      boolean allowAccess = true;
      switch (licenseAlarm.getLevel())
      {
        case None:
        case Alert:
        case Alarm:
          allowAccess = true;
          break;

        case Limit:
        case Block:
          allowAccess = false;
          break;
      }

      /*****************************************
       *
       *  process
       *
       *****************************************/

      JSONObject jsonResponse = null;
      if (licenseState.isValid() && allowAccess)
        {
          switch (api)
          {
            case ping:
              jsonResponse = processPing(jsonRoot);
              break;
            case getCustomer:
              jsonResponse = processGetCustomer(jsonRoot);
              break;
            case getCustomerBDRs:
              jsonResponse = processGetCustomerBDRs(jsonRoot);
              break;
            case getCustomerODRs:
              jsonResponse = processGetCustomerODRs(jsonRoot);
              break;
            case getCustomerPoints:
              jsonResponse = processGetCustomerPoints(jsonRoot);
              break;
            case creditBonus:
              jsonResponse = processCreditBonus(jsonRoot);
              break;
            case debitBonus:
              jsonResponse = processDebitBonus(jsonRoot);
              break;
            case getCustomerMessages:
              jsonResponse = processGetCustomerMessages(jsonRoot);
              break;
            case getCustomerJourneys:
              jsonResponse = processGetCustomerJourneys(jsonRoot);
              break;
            case getCustomerCampaigns:
              jsonResponse = processGetCustomerCampaigns(jsonRoot);
              break;
            case getCustomerLoyaltyPrograms:
              jsonResponse = processGetCustomerLoyaltyPrograms(jsonRoot);
              break;
            case getLoyaltyProgram:
              jsonResponse = processGetLoyaltyProgram(jsonRoot);
              break;
            case getLoyaltyProgramList:
              jsonResponse = processGetLoyaltyProgramList(jsonRoot);
              break;
            case getOffersList:
              jsonResponse = processGetOffersList(jsonRoot);
              break;
            case getCustomerAvailableCampaigns:
              jsonResponse = processGetCustomerAvailableCampaigns(jsonRoot);
              break;
            case updateCustomer:
              jsonResponse = processUpdateCustomer(jsonRoot);
              break;
            case updateCustomerParent:
              jsonResponse = processUpdateCustomerParent(jsonRoot);
              break;
            case removeCustomerParent:
              jsonResponse = processRemoveCustomerParent(jsonRoot);
              break;
            case getCustomerNBOs:
              jsonResponse = processGetCustomerNBOs(jsonRoot);
              break;
            case getCustomerTokenAndNBO:
              jsonResponse = processGetCustomerTokenAndNBO(jsonRoot);
              break;
            case getTokensCodesList:
              jsonResponse = processGetTokensCodesList(jsonRoot);
              break;
            case acceptOffer:
              jsonResponse = processAcceptOffer(jsonRoot);
              break;
            case purchaseOffer:
              jsonResponse = processPurchaseOffer(jsonRoot,sync);
              break;
            case triggerEvent:
              jsonResponse = processTriggerEvent(jsonRoot);
              break;
            case enterCampaign:
              jsonResponse = processEnterCampaign(jsonRoot);
              break;
            case loyaltyProgramOptIn:
              jsonResponse = processLoyaltyProgramOptInOut(jsonRoot, true);
              break;
            case loyaltyProgramOptOut:
              jsonResponse = processLoyaltyProgramOptInOut(jsonRoot, false);
              break;
            case validateVoucher:
              jsonResponse = processValidateVoucher(jsonRoot);
              break;
            case redeemVoucher:
              jsonResponse = processRedeemVoucher(jsonRoot,sync);
              break;              
            case getResellerDetails:
              jsonResponse = processGetResellerDetails(jsonRoot);
              break;
            case putSimpleOffer:
              jsonResponse = processPutSimpleOffer(jsonRoot);
              break;
            case getSimpleOfferList:
              jsonResponse = processGetSimpleOfferList(jsonRoot);
              break;
            case removeSimpleOffer:
              jsonResponse = processRemoveSimpleOffer(jsonRoot);
              break;
          }
        }
      else
        {
          jsonResponse = processFailedLicenseCheck(licenseState);
          log.warn("Failed license check {} ", licenseState);
        }

      //
      //  validate
      //

      if (jsonResponse == null)
        {
          addUnknownStats(api.name(),startTime);
          throw new ServerException("no handler for " + api);
        }

      /*****************************************
       *
       *  send response
       *
       *****************************************/

      //
      //  standard response fields
      //

      jsonResponse.put("apiVersion", RESTAPIVersion);

      //
      //  log
      //

      if (log.isDebugEnabled()) log.debug("API (raw response): {}", jsonResponse.toString());
      addOKStats(api.name(),startTime);

      //
      //  send
      //

      //
      // headers
      //

      exchange.sendResponseHeaders(200, 0);
      exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
      writer.write(jsonResponse.toString());
      writer.close();
      exchange.close();

    }
    catch (ThirdPartyManagerException ex )
    {

      log.warn("Exception processing REST api: ",ex);

      //
      //  statistics
      //

      addKOStats(api.name(),startTime);

      //
      //  send error response
      //

      HashMap<String,Object> response = new HashMap<String,Object>();
      response.put(GENERIC_RESPONSE_CODE, ex.getResponseCode());
      response.put(GENERIC_RESPONSE_MSG, ex.getMessage());
      if (ex.responseDetails != null && !(ex.responseDetails.isEmpty())) {
        response.put(GENERIC_RESPONSE_DETAILS, ex.responseDetails);
      }

      //
      //  standard response fields
      //

      response.put("apiVersion", RESTAPIVersion);
      JSONObject jsonResponse = JSONUtilities.encodeObject(response);
      
      int headerValue = 200;
      String result = jsonResponse.toString();
      
      if (ex.getResponseCode() == -404)
        {
          headerValue = 404;
          result = "<h1>404 Not Found</h1>No context found for request";
        }

      //
      // headers
      //

      exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
      exchange.sendResponseHeaders(headerValue, 0);

      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
      writer.write(result);
      writer.close();
      exchange.close();
    }
    catch (org.json.simple.parser.ParseException exception)
      {
        //
        //  log
        //

        log.warn("ParseException processing REST api: ", exception);

        //
        //  statistics
        //

        addKOStats(api.name(),startTime);

        //
        //  send error response
        //

        HashMap<String,Object> response = new HashMap<String,Object>();
        updateResponse(response, RESTAPIGenericReturnCodes.MALFORMED_REQUEST);

        //
        //  standard response fields
        //

        response.put("apiVersion", RESTAPIVersion);
        JSONObject jsonResponse = JSONUtilities.encodeObject(response);

        //
        // headers
        //

        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
    catch (IOException | ServerException | RuntimeException e )
      {

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  statistics
        //

        addKOStats(api.name(),startTime);

        //
        //  send error response
        //

        HashMap<String,Object> response = new HashMap<String,Object>();
        updateResponse(response, RESTAPIGenericReturnCodes.SYSTEM_ERROR);

        //
        //  standard response fields
        //

        response.put("apiVersion", RESTAPIVersion);
        JSONObject jsonResponse = JSONUtilities.encodeObject(response);

        //
        // headers
        //

        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
  }


  /*****************************************
  *
  *  validateURIandContext
  *
  *****************************************/
  
  private void validateURIandContext(HttpExchange exchange) throws ThirdPartyManagerException
  {
    String path = exchange.getRequestURI().getPath();
    if (path.endsWith("/")) path = path.substring(0, path.length()-1);
    
    //
    //  validate
    //
    
    if (! path.equals(exchange.getHttpContext().getPath()))
      {
        log.warn("invalid url {} should be {}", path, exchange.getHttpContext().getPath());
        throw new ThirdPartyManagerException("invalid URL", -404);
      }
      
  }

  /*****************************************
   *
   *  processFailedLicenseCheck
   *
   *****************************************/

  private JSONObject processFailedLicenseCheck(LicenseState licenseState)
  {
    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.LICENSE_RESTRICTION.getGenericResponseCode());
    response.put(GENERIC_RESPONSE_MSG, licenseState.getOutcome().name());
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processPing
   *
   *****************************************/

  private JSONObject processPing(JSONObject jsonRoot)
  {
    /*****************************************
     *
     *  ping
     *
     *****************************************/

    String responseStr = "success";

    /*****************************************
     *
     *  response
     *
     *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("ping", responseStr);
    return constructThirdPartyResponse(RESTAPIGenericReturnCodes.SUCCESS, response);
  }

  /*****************************************
   *
   *  processGetCustomer
   *
   *****************************************/

  private JSONObject processGetCustomer(JSONObject jsonRoot) throws ThirdPartyManagerException
  {

    /****************************************
     *
     *  response
     *
     ****************************************/

    Map<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String subscriberID = resolveSubscriberID(jsonRoot);

    //
    // process
    //

    try
    {
      SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true, false);
      if (baseSubscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
        }
      else
        {
          response = baseSubscriberProfile.getProfileMapForThirdPartyPresentation(segmentationDimensionService, subscriberGroupEpochReader, exclusionInclusionTargetService );
          response.putAll(resolveAllSubscriberIDs(baseSubscriberProfile));
          updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
        }
    } 
    catch (SubscriberProfileServiceException e)
    {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processGetCustomerBDRs
   *
   *****************************************/

  private JSONObject processGetCustomerBDRs(JSONObject jsonRoot) throws ThirdPartyManagerException
  {

    /****************************************
     *
     *  response
     *
     ****************************************/

    Map<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String startDateReq = readString(jsonRoot, "startDate", false);
    String moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", false);
    String featureID = JSONUtilities.decodeString(jsonRoot, "featureID", false);
    JSONArray deliverableIDs = JSONUtilities.decodeJSONArray(jsonRoot, "deliverableIDs", false);
    
    //
    //  filters
    //
    
    Collection<String> deliverableIDCollection = new ArrayList<String>();
    List<QueryBuilder> filters = new ArrayList<QueryBuilder>();
    if (moduleID != null && !moduleID.isEmpty()) filters.add(QueryBuilders.matchQuery("moduleID", moduleID));
    if (featureID != null && !featureID.isEmpty()) filters.add(QueryBuilders.matchQuery("featureID", featureID));
    if (deliverableIDs != null)
      {
        for(int i=0; i<deliverableIDs.size(); i++)
          {
            deliverableIDCollection.add(deliverableIDs.get(i).toString());
          }
        if (!deliverableIDCollection.isEmpty()) filters.add(QueryBuilders.termsQuery("deliverableID", deliverableIDCollection));
      }

    //
    // process
    //
    
    String subscriberID = resolveSubscriberID(jsonRoot);
    try
    {
      SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      if (baseSubscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
        }
      else
        {

          List<JSONObject> BDRsJson = new ArrayList<JSONObject>();
          
          //
          // read history
          //

          SearchRequest searchRequest = getSearchRequest(API.getCustomerBDRs, subscriberID, startDateReq == null ? null : getDateFromString(startDateReq, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN), filters);
          List<SearchHit> hits = getESHits(searchRequest);
          for (SearchHit hit : hits)
            {
              Map<String, Object> esFields = hit.getSourceAsMap();
              CommodityDeliveryRequest commodityDeliveryRequest = new CommodityDeliveryRequest(esFields);
              Map<String, Object> esbdrMap = commodityDeliveryRequest.getThirdPartyPresentationMap(subscriberMessageTemplateService, salesChannelService, journeyService, offerService, loyaltyProgramService, productService, voucherService, deliverableService, paymentMeanService, resellerService);
              BDRsJson.add(JSONUtilities.encodeObject(esbdrMap));
            }
          response.put("BDRs", JSONUtilities.encodeArray(BDRsJson));
          response.putAll(resolveAllSubscriberIDs(baseSubscriberProfile));
          updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
        }
    } 
    catch (SubscriberProfileServiceException e)
    {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getESHits
  *
  *****************************************/
  
  private List<SearchHit> getESHits(SearchRequest searchRequest) throws ThirdPartyManagerException
  {
    List<SearchHit> hits = new ArrayList<SearchHit>();
    Scroll scroll = new Scroll(TimeValue.timeValueSeconds(10L));
    searchRequest.scroll(scroll);
    searchRequest.source().size(1000);
    try
      {
        SearchResponse searchResponse = elasticsearch.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId(); // always null
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        while (searchHits != null && searchHits.length > 0)
          {
            //
            //  add
            //
            
            hits.addAll(new ArrayList<SearchHit>(Arrays.asList(searchHits)));
            
            //
            //  scroll
            //
            
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
            scrollRequest.scroll(scroll);
            searchResponse = elasticsearch.searchScroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
          }
      } 
    catch (IOException e)
      {
        log.error("IOException in ES qurery {}", e.getMessage());
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
      }
    
    //
    //  return
    //
    
    return hits;
  }

  /*****************************************
   *
   *  processGetCustomerODRs
   *
   *****************************************/

  private JSONObject processGetCustomerODRs(JSONObject jsonRoot) throws ThirdPartyManagerException
  {

    /****************************************
     *
     *  response
     *
     ****************************************/

    Map<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String startDateReq = readString(jsonRoot, "startDate", false);
    String moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", false);
    String featureID = JSONUtilities.decodeString(jsonRoot, "featureID", false);
    String offerID = JSONUtilities.decodeString(jsonRoot, "offerID", false);
    String salesChannelID = JSONUtilities.decodeString(jsonRoot, "salesChannelID", false);
    String paymentMeanID = JSONUtilities.decodeString(jsonRoot, "paymentMeanID", false);
    String subscriberID = resolveSubscriberID(jsonRoot);
    
    List<QueryBuilder> filters = new ArrayList<QueryBuilder>();
    if (moduleID != null && !moduleID.isEmpty()) filters.add(QueryBuilders.matchQuery("moduleID", moduleID));
    if (featureID != null && !featureID.isEmpty()) filters.add(QueryBuilders.matchQuery("featureID", featureID));
    if (offerID != null && !offerID.isEmpty()) filters.add(QueryBuilders.matchQuery("offerID", offerID));
    if (salesChannelID != null && !salesChannelID.isEmpty()) filters.add(QueryBuilders.matchQuery("salesChannelID", salesChannelID));

    //
    // process
    //

    try
    {
      SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      if (baseSubscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
        }
      else
        {

          //
          // read history
          //

          List<JSONObject> ODRsJson = new ArrayList<JSONObject>();
          List<DeliveryRequest> ODRs = new ArrayList<DeliveryRequest>();
          SearchRequest searchRequest = getSearchRequest(API.getCustomerODRs, subscriberID, startDateReq == null ? null : getDateFromString(startDateReq, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN), filters);
          List<SearchHit> hits = getESHits(searchRequest);
          for (SearchHit hit : hits)
            {
              PurchaseFulfillmentRequest purchaseFulfillmentRequest = new PurchaseFulfillmentRequest(hit.getSourceAsMap(), supplierService, offerService, productService, voucherService, resellerService);
              ODRs.add(purchaseFulfillmentRequest);
            }

          //
          // filter on paymentMeanID * NOT in ES SHOULD BE FILTER AS IT IS *
          //

          if (paymentMeanID != null)
            {
              List<DeliveryRequest> result = new ArrayList<DeliveryRequest>();
              for (DeliveryRequest request : ODRs)
                {
                  if (request instanceof PurchaseFulfillmentRequest)
                    {
                      PurchaseFulfillmentRequest odrRequest = (PurchaseFulfillmentRequest) request;
                      Offer offer = (Offer) offerService.getStoredGUIManagedObject(odrRequest.getOfferID());
                      if (offer != null)
                        {
                          if (offer.getOfferSalesChannelsAndPrices() != null)
                            {
                              for (OfferSalesChannelsAndPrice channel : offer.getOfferSalesChannelsAndPrices())
                                {
                                  if (channel.getPrice() != null && paymentMeanID.equals(channel.getPrice().getPaymentMeanID()))
                                    {
                                      result.add(request);
                                    }
                                }
                            }
                        }
                    }
                }
              ODRs = result;
            }

          //
          // filter
          //

          for (DeliveryRequest odr : ODRs)
            {
              Map<String, Object> presentationMap = odr.getThirdPartyPresentationMap(subscriberMessageTemplateService, salesChannelService, journeyService, offerService, loyaltyProgramService, productService, voucherService, deliverableService, paymentMeanService, resellerService);
              ODRsJson.add(JSONUtilities.encodeObject(presentationMap));
            }
          response.put("ODRs", JSONUtilities.encodeArray(ODRsJson));
          response.putAll(resolveAllSubscriberIDs(baseSubscriberProfile));
          updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
        }
    } 
    catch (SubscriberProfileServiceException e)
    {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  * processGetGetCustomerPoints
  *
  *****************************************/

  private JSONObject processGetCustomerPoints(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
     *
     *  now
     *
     ****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String bonusName = JSONUtilities.decodeString(jsonRoot, "bonusName", false);

    /*****************************************
     *
     *  resolve point
     *
     *****************************************/

    Point searchedPoint = null;
    if(bonusName != null && !bonusName.isEmpty())
      {
        for(GUIManagedObject storedPoint : pointService.getStoredPoints()){
          if(storedPoint instanceof Point && (((Point) storedPoint).getPointName().equals(bonusName))){
            searchedPoint = (Point)storedPoint;
          }
        }
        if(searchedPoint == null){
          log.info("bonus with name '"+bonusName+"' not found");
          updateResponse(response, RESTAPIGenericReturnCodes.BONUS_NOT_FOUND);
          return JSONUtilities.encodeObject(response);
        }
      }

    /*****************************************
     *
     *  resolve subscriberID
     *
     *****************************************/

    String subscriberID = resolveSubscriberID(jsonRoot);

    /*****************************************
     *
     *  getSubscriberProfile
     *
     *****************************************/

    try
    {
      SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true, false);
      if (baseSubscriberProfile == null)
        {
          response.put("responseCode", "CustomerNotFound");
        }
      else
        {
          ArrayList<JSONObject> pointsPresentation = new ArrayList<JSONObject>();
          Map<String, PointBalance> pointBalances = baseSubscriberProfile.getPointBalances();
          for (String pointID : pointBalances.keySet())
            {
              Point point = pointService.getActivePoint(pointID, now);
              if (point != null && (searchedPoint == null || searchedPoint.getPointID().equals(point.getPointID())))
                {
                  HashMap<String, Object> pointPresentation = new HashMap<String,Object>();
                  PointBalance pointBalance = pointBalances.get(pointID);
                  pointPresentation.put("pointName", point.getDisplay());
                  pointPresentation.put("balance", pointBalance.getBalance(now));
                  Set<Object> pointExpirations = new HashSet<Object>();
                  for(Date expirationDate : pointBalance.getBalances().keySet()){
                    HashMap<String, Object> expirationPresentation = new HashMap<String, Object>();
                    expirationPresentation.put("expirationDate", getDateString(expirationDate));
                    expirationPresentation.put("quantity", pointBalance.getBalances().get(expirationDate));
                    pointExpirations.add(JSONUtilities.encodeObject(expirationPresentation));
                  }
                  pointPresentation.put("expirations", pointExpirations);


                  pointsPresentation.add(JSONUtilities.encodeObject(pointPresentation));
                }
            }

          response.put("points", pointsPresentation);
          response.putAll(resolveAllSubscriberIDs(baseSubscriberProfile));
          updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
        }
    }
    catch (SubscriberProfileServiceException e)
    {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }

    /*****************************************
     *
     *  return
     *
     *****************************************/

    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processCreditBonus
   * @throws ParseException 
   * @throws IOException 
  *
  *****************************************/
  
  private JSONObject processCreditBonus(JSONObject jsonRoot) throws ThirdPartyManagerException, IOException, ParseException{
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String bonusName = JSONUtilities.decodeString(jsonRoot, "bonusName", true);
    Integer quantity = JSONUtilities.decodeInteger(jsonRoot, "quantity", true);
    String origin = JSONUtilities.decodeString(jsonRoot, "origin", true);
    AuthenticatedResponse authResponse = null;
    ThirdPartyCredential thirdPartyCredential = new ThirdPartyCredential(jsonRoot);
    if (!Deployment.getRegressionMode())
      {
        authResponse = authCache.get(thirdPartyCredential);
      }
    else
      {
        authResponse = authenticate(thirdPartyCredential);
      } 
    int user = (authResponse.getUserId());
    String userID = Integer.toString(user);
    String featureID = userID;
    
    /*****************************************
    *
    *  resolve subscriberID
    *
    *****************************************/
    
    String subscriberID = resolveSubscriberID(jsonRoot);
    
    /*****************************************
    *
    *  resolve bonus
    *
    *****************************************/

    Deliverable searchedBonus = null;
    for(GUIManagedObject storedDeliverable : deliverableService.getStoredDeliverables()){
      if(storedDeliverable instanceof Deliverable && (((Deliverable) storedDeliverable).getDeliverableName().equals(bonusName))){
        searchedBonus = (Deliverable)storedDeliverable;
      }
    }
    if(searchedBonus == null){
      log.info("bonus with name '"+bonusName+"' not found");
      response.put("responseCode", "BonusNotFound");
      return JSONUtilities.encodeObject(response);
    }
    
    /*****************************************
    *
    *  generate commodity delivery request
    *
    *****************************************/
    
    String deliveryRequestID = zuks.getStringKey();
    try {
      SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit validityPeriodType = null;
      int validityPeriod = 0;      
      String pointID = null;
      Date now = SystemTime.getCurrentTime();
      for (Point point : pointService.getActivePoints(now))
        {
          if (bonusName.equals(point.getGUIManagedObjectName()))
            {
              pointID = point.getGUIManagedObjectID();
              break;
            }
        }
      GUIManagedObject pointObject = pointService.getStoredPoint(pointID);
      if (pointObject != null && pointObject instanceof Point) {
        Point point = (Point) pointObject;
        validityPeriodType = point.getValidity().getPeriodType();
        validityPeriod = point.getValidity().getPeriodQuantity();
      }
     CommodityDeliveryManager.sendCommodityDeliveryRequest(subscriberProfile,subscriberGroupEpochReader,null, null, deliveryRequestID, null, true, deliveryRequestID, Module.Customer_Care.getExternalRepresentation(), featureID, subscriberID, searchedBonus.getFulfillmentProviderID(), searchedBonus.getDeliverableID(), CommodityDeliveryOperation.Credit, quantity, validityPeriodType, validityPeriod, DELIVERY_REQUEST_PRIORITY, origin);
    } catch (SubscriberProfileServiceException e) {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("deliveryRequestID", deliveryRequestID);
    response.put("responseCode", "ok");
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processDebitBonus
  *
  *****************************************/
  
  private JSONObject processDebitBonus(JSONObject jsonRoot) throws ThirdPartyManagerException, IOException, ParseException {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String bonusName = JSONUtilities.decodeString(jsonRoot, "bonusName", true);
    Integer quantity = JSONUtilities.decodeInteger(jsonRoot, "quantity", true);
    String origin = JSONUtilities.decodeString(jsonRoot, "origin", true);
    
    AuthenticatedResponse authResponse = null;
    ThirdPartyCredential thirdPartyCredential = new ThirdPartyCredential(jsonRoot);
    if (!Deployment.getRegressionMode())
      {
        authResponse = authCache.get(thirdPartyCredential);
      }
    else
      {
        authResponse = authenticate(thirdPartyCredential);
      } 
    int user = (authResponse.getUserId());
    String userID = Integer.toString(user);
    String featureID = userID;
    
    /*****************************************
    *
    *  resolve subscriberID
    *
    *****************************************/
    
    String subscriberID = resolveSubscriberID(jsonRoot);
    
    /*****************************************
    *
    *  resolve bonus
    *
    *****************************************/

    PaymentMean searchedBonus = null;
    for(GUIManagedObject storedPaymentMean : paymentMeanService.getStoredPaymentMeans()){
      if(storedPaymentMean instanceof PaymentMean && (((PaymentMean) storedPaymentMean).getPaymentMeanName().equals(bonusName))){
        searchedBonus = (PaymentMean)storedPaymentMean;
      }
    }
    if(searchedBonus == null){
      log.info("bonus with name '"+bonusName+"' not found");
      response.put("responseCode", "BonusNotFound");
      return JSONUtilities.encodeObject(response);
    }
    
    /*****************************************
    *
    *  generate commodity delivery request
    *
    *****************************************/
    
    String deliveryRequestID = zuks.getStringKey();
    try {
      SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      CommodityDeliveryManager.sendCommodityDeliveryRequest(subscriberProfile, subscriberGroupEpochReader,null, null, deliveryRequestID, null, true, deliveryRequestID, Module.REST_API.getExternalRepresentation(), featureID, subscriberID, searchedBonus.getFulfillmentProviderID(), searchedBonus.getPaymentMeanID(), CommodityDeliveryOperation.Debit, quantity, null, null, DELIVERY_REQUEST_PRIORITY, origin);
    } catch (SubscriberProfileServiceException e) {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("deliveryRequestID", deliveryRequestID);
    response.put("responseCode", "ok");
    return JSONUtilities.encodeObject(response);
  }
 
  /*****************************************
   *
   *  processGetCustomerMessages
   *
   *****************************************/

  private JSONObject processGetCustomerMessages(JSONObject jsonRoot) throws ThirdPartyManagerException
  {

    /****************************************
     *
     *  response
     *
     ****************************************/

    Map<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String startDateReq = readString(jsonRoot, "startDate", false);
    String moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", false);
    String featureID = JSONUtilities.decodeString(jsonRoot, "featureID", false);
    
    //
    //  filters
    //
    
    List<QueryBuilder> filters = new ArrayList<QueryBuilder>();
    if (moduleID != null && !moduleID.isEmpty()) filters.add(QueryBuilders.matchQuery("moduleID", moduleID));
    if (featureID != null && !featureID.isEmpty()) filters.add(QueryBuilders.matchQuery("featureID", featureID));

    //
    // process
    //
    
    String subscriberID = resolveSubscriberID(jsonRoot);
    try
    {
      SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      if (baseSubscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
        }
      else
        {

          //
          // read history
          //

          List<JSONObject> messagesJson = new ArrayList<JSONObject>();
          SearchRequest searchRequest = getSearchRequest(API.getCustomerMessages, subscriberID, startDateReq == null ? null : getDateFromString(startDateReq, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN), filters);
          List<SearchHit> hits = getESHits(searchRequest);
          for (SearchHit hit : hits)
            {
              String channelID = (String) hit.getSourceAsMap().get("channelID");
              if (channelID != null && !channelID.isEmpty())
                {
                  String deliveryType = null;
                  for (String deliveryTypeInMap : Deployment.getDeliveryTypeCommunicationChannelIDMap().keySet())
                    {
                      String chID = Deployment.getDeliveryTypeCommunicationChannelIDMap().get(deliveryTypeInMap);
                      if (channelID.equals(chID))
                        {
                          deliveryType = deliveryTypeInMap;
                          break;
                        }
                    }
                  if (deliveryType != null && Deployment.getDeliveryManagers().get(deliveryType) != null)
                    {
                      String requestClass = Deployment.getDeliveryManagers().get(deliveryType).getRequestClassName();
                      if (requestClass != null)
                        {
                          DeliveryRequest notification = getNotificationDeliveryRequest(requestClass, hit);
                          if (notification != null)
                            {
                              Map<String, Object> esNotificationMap = notification.getThirdPartyPresentationMap(subscriberMessageTemplateService, salesChannelService, journeyService, offerService, loyaltyProgramService, productService, voucherService, deliverableService, paymentMeanService, resellerService);
                              messagesJson.add(JSONUtilities.encodeObject(esNotificationMap));
                            }
                        }
                    }
                }
            }
          response.put("messages", JSONUtilities.encodeArray(messagesJson));
          response.putAll(resolveAllSubscriberIDs(baseSubscriberProfile));
          updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
        }
    } 
    catch (SubscriberProfileServiceException e)
    {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   * processGetCustomerJourneys
   *
   *****************************************/

  private JSONObject processGetCustomerJourneys(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    Date now = SystemTime.getCurrentTime();
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String subscriberID = resolveSubscriberID(jsonRoot);

    String journeyObjectiveName = readString(jsonRoot, "objective", false);
    String journeyState = readString(jsonRoot, "journeyState", false);
    String customerStatus = readString(jsonRoot, "customerStatus", false);
    String journeyStartDateStr = readString(jsonRoot, "journeyStartDate", false);
    String journeyEndDateStr = readString(jsonRoot, "journeyEndDate", false);

    Date journeyStartDate = prepareStartDate(getDateFromString(journeyStartDateStr, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN));
    Date journeyEndDate = prepareEndDate(getDateFromString(journeyEndDateStr, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN));
    
    List<QueryBuilder> filters = new ArrayList<QueryBuilder>();

    /*****************************************
     *
     *  getSubscriberProfile
     *
     *****************************************/
    try
    {
      SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      if (baseSubscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
          if (log.isDebugEnabled()) log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
        }
      else
        {
          List<JSONObject> journeysJsonES = new ArrayList<JSONObject>();
          SearchRequest searchRequest = getSearchRequest(API.getCustomerJourneys, subscriberID, journeyStartDate, filters);
          List<SearchHit> hits = getESHits(searchRequest);
          Map<String, JourneyHistory> journeyHistoryMap = new HashMap<String, JourneyHistory>(hits.size());
          for (SearchHit hit : hits)
            {
              Map<String, Object> esFields = hit.getSourceAsMap();
              JourneyHistory journeyHistory = new JourneyHistory(esFields);
              journeyHistoryMap.put(journeyHistory.getJourneyID(), journeyHistory);
            }

          //
          // read campaigns
          //

          Collection<GUIManagedObject> stroeRawJourneys = journeyService.getStoredJourneys();
          List<Journey> storeJourneys = new ArrayList<Journey>();
          for (GUIManagedObject storeJourney : stroeRawJourneys)
            {
              if (storeJourney instanceof Journey)
                storeJourneys.add((Journey) storeJourney);
            }

          //
          // filter Journeys
          //

          storeJourneys = storeJourneys.stream().filter(journey -> journey.getGUIManagedObjectType() == GUIManagedObjectType.Journey).collect(Collectors.toList());

          //
          // filter on journeyStartDate
          //

          if (journeyStartDate != null)
            {
              storeJourneys = storeJourneys.stream().filter(journey -> (journey.getEffectiveStartDate() == null || journey.getEffectiveStartDate().compareTo(journeyStartDate) >= 0)).collect(Collectors.toList());
            }

          //
          // filter on journeyEndDate
          //

          if (journeyEndDate != null)
            {
              storeJourneys = storeJourneys.stream().filter(journey -> (journey.getEffectiveEndDate() == null || journey.getEffectiveEndDate().compareTo(journeyEndDate) <= 0)).collect(Collectors.toList());
            }

          //
          // filter on journeyObjectiveName
          //

          if (journeyObjectiveName != null && !journeyObjectiveName.isEmpty())
            {

              //
              // read objective
              //

              Collection<JourneyObjective> activejourneyObjectives = journeyObjectiveService.getActiveJourneyObjectives(SystemTime.getCurrentTime());

              //
              // filter activejourneyObjective by name
              //

              List<JourneyObjective> journeyObjectives = activejourneyObjectives.stream().filter(journeyObj -> journeyObjectiveName.equals(journeyObj.getJSONRepresentation().get("display"))).collect(Collectors.toList());
              JourneyObjective exactJourneyObjective = journeyObjectives.size() > 0 ? journeyObjectives.get(0) : null;

              //
              // filter
              //
              if (exactJourneyObjective == null)
                storeJourneys = new ArrayList<Journey>();
              else
                storeJourneys = storeJourneys.stream().filter(journey -> (journey.getJourneyObjectiveInstances() != null && (journey.getJourneyObjectiveInstances().stream().filter(obj -> obj.getJourneyObjectiveID().equals(exactJourneyObjective.getJourneyObjectiveID())).count() > 0L))).collect(Collectors.toList());
            }

          for (Journey storeJourney : storeJourneys)
            {

              //
              // subsLatestStatistic
              //

              JourneyHistory subsLatestStatistic = journeyHistoryMap.get(storeJourney.getJourneyID());

              //
              // continue if not in stat
              //

              if (subsLatestStatistic == null)
                continue;

              //
              // filter on journeyState
              //

              if (journeyState != null && !journeyState.isEmpty())
                {
                  boolean criteriaSatisfied = false;
                  if (journeyService.getJourneyStatus(storeJourney).getExternalRepresentation().equalsIgnoreCase(journeyState))
                    {
                      criteriaSatisfied = true;
                    }
                  if (!criteriaSatisfied) continue;
                }

              //
              // filter on customerStatus
              //

              boolean statusNotified = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> SubscriberJourneyStatus.Notified.getExternalRepresentation().equals(campaignStat.getStatus())).count() > 0L;
              boolean statusConverted = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> campaignStat.isConverted()).count() > 0L;
              Boolean statusTargetGroup = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> SubscriberJourneyStatus.Targeted.getExternalRepresentation().equals(campaignStat.getStatus())).count() > 0L;
              Boolean statusControlGroup = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> SubscriberJourneyStatus.ControlGroup.getExternalRepresentation().equals(campaignStat.getStatus()) || SubscriberJourneyStatus.ControlGroupConverted.getExternalRepresentation().equals(campaignStat.getStatus())).count() > 0L;
              Boolean statusUniversalControlGroup = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> SubscriberJourneyStatus.UniversalControlGroup.getExternalRepresentation().equals(campaignStat.getStatus()) || SubscriberJourneyStatus.UniversalControlGroupConverted.getExternalRepresentation().equals(campaignStat.getStatus())).count() > 0L;
              boolean journeyComplete = subsLatestStatistic.getStatusHistory().stream().filter(journeyStat -> journeyStat.getJourneyComplete()).count() > 0L;

              SubscriberJourneyStatus customerStatusInJourney = Journey.getSubscriberJourneyStatus(statusConverted, statusNotified, statusTargetGroup, statusControlGroup, statusUniversalControlGroup);
              SubscriberJourneyStatus profilejourneyStatus = baseSubscriberProfile.getSubscriberJourneys().get(storeJourney.getJourneyID() + "");
              if (profilejourneyStatus.in(SubscriberJourneyStatus.NotEligible, SubscriberJourneyStatus.UniversalControlGroup, SubscriberJourneyStatus.Excluded, SubscriberJourneyStatus.ObjectiveLimitReached))
                customerStatusInJourney = profilejourneyStatus;

              if (customerStatus != null)
                {
                  SubscriberJourneyStatus customerStatusInReq = SubscriberJourneyStatus.fromExternalRepresentation(customerStatus);
                  boolean criteriaSatisfied = customerStatusInReq == customerStatusInJourney;
                  if (!criteriaSatisfied)
                    continue;
                }

              //
              // prepare response
              //

              Map<String, Object> journeyResponseMap = new HashMap<String, Object>();
              journeyResponseMap.put("journeyID", storeJourney.getJourneyID());
              journeyResponseMap.put("journeyName", journeyService.generateResponseJSON(storeJourney, true, SystemTime.getCurrentTime()).get("display"));
              journeyResponseMap.put("description", journeyService.generateResponseJSON(storeJourney, true, SystemTime.getCurrentTime()).get("description"));
              journeyResponseMap.put("startDate", getDateString(storeJourney.getEffectiveStartDate()));
              journeyResponseMap.put("endDate", getDateString(storeJourney.getEffectiveEndDate()));
              journeyResponseMap.put("entryDate", getDateString(subsLatestStatistic.getJourneyEntranceDate()));
              journeyResponseMap.put("exitDate", subsLatestStatistic.getJourneyExitDate(journeyService) != null ? getDateString(subsLatestStatistic.getJourneyExitDate(journeyService)) : "");
              journeyResponseMap.put("journeyState", journeyService.getJourneyStatus(storeJourney).getExternalRepresentation());
              List<JSONObject> resultObjectives = new ArrayList<JSONObject>();
              for (JourneyObjectiveInstance journeyObjectiveInstance : storeJourney.getJourneyObjectiveInstances())
                {
                  List<JSONObject> resultCharacteristics = new ArrayList<JSONObject>();
                  JSONObject result = new JSONObject();

                  JourneyObjective journeyObjective = journeyObjectiveService.getActiveJourneyObjective(journeyObjectiveInstance.getJourneyObjectiveID(), SystemTime.getCurrentTime());
                  result.put("active", journeyObjective.getActive());
                  result.put("parentJourneyObjectiveID", journeyObjective.getParentJourneyObjectiveID());
                  result.put("display", journeyObjective.getJSONRepresentation().get("display"));
                  result.put("readOnly", journeyObjective.getReadOnly());
                  result.put("name", journeyObjective.getGUIManagedObjectName());
                  result.put("contactPolicyID", journeyObjective.getContactPolicyID());
                  result.put("id", journeyObjective.getGUIManagedObjectID());

                  for (CatalogCharacteristicInstance catalogCharacteristicInstance : journeyObjectiveInstance.getCatalogCharacteristics())
                    {
                      JSONObject characteristics = new JSONObject();
                      CatalogCharacteristic catalogCharacteristic = catalogCharacteristicService.getActiveCatalogCharacteristic(catalogCharacteristicInstance.getCatalogCharacteristicID(), now);
                      characteristics.put("catalogCharacteristicID", catalogCharacteristic.getCatalogCharacteristicID());
                      characteristics.put("catalogCharacteristicName", catalogCharacteristic.getCatalogCharacteristicName());
                      characteristics.put("catalogCharacteristicDataType", catalogCharacteristic.getDataType().getExternalRepresentation());
                      characteristics.put("value", catalogCharacteristicInstance.getValue());
                      resultCharacteristics.add(characteristics);
                    }

                  result.put("catalogCharacteristics", JSONUtilities.encodeArray(resultCharacteristics));
                  resultObjectives.add(result);
                }

              journeyResponseMap.put("objectives", JSONUtilities.encodeArray(resultObjectives));

              Map<String, Object> currentState = new HashMap<String, Object>();
              NodeHistory nodeHistory = subsLatestStatistic.getLastNodeEntered();
              currentState.put("nodeID", nodeHistory.getToNodeID());
              currentState.put("nodeName", nodeHistory.getToNodeID() == null ? null : (storeJourney.getJourneyNode(nodeHistory.getToNodeID()) == null ? "node has been removed" : storeJourney.getJourneyNode(nodeHistory.getToNodeID()).getNodeName()));
              JSONObject currentStateJson = JSONUtilities.encodeObject(currentState);

              //
              // node history
              //

              List<JSONObject> nodeHistoriesJson = new ArrayList<JSONObject>();
              for (NodeHistory journeyHistories : subsLatestStatistic.getNodeHistory())
                {
                  Map<String, Object> nodeHistoriesMap = new HashMap<String, Object>();
                  nodeHistoriesMap.put("fromNodeID", journeyHistories.getFromNodeID());
                  nodeHistoriesMap.put("toNodeID", journeyHistories.getToNodeID());
                  nodeHistoriesMap.put("fromNode", journeyHistories.getFromNodeID() == null ? null : (storeJourney.getJourneyNode(journeyHistories.getFromNodeID()) == null ? "node has been removed" : storeJourney.getJourneyNode(journeyHistories.getFromNodeID()).getNodeName()));
                  nodeHistoriesMap.put("toNode", journeyHistories.getToNodeID() == null ? null : (storeJourney.getJourneyNode(journeyHistories.getToNodeID()) == null ? "node has been removed" : storeJourney.getJourneyNode(journeyHistories.getToNodeID()).getNodeName()));
                  nodeHistoriesMap.put("transitionDate", getDateString(journeyHistories.getTransitionDate()));
                  nodeHistoriesMap.put("linkID", journeyHistories.getLinkID());
                  nodeHistoriesMap.put("deliveryRequestID", journeyHistories.getDeliveryRequestID());
                  nodeHistoriesJson.add(JSONUtilities.encodeObject(nodeHistoriesMap));
                }
              journeyResponseMap.put("customerStatus", customerStatusInJourney.getExternalRepresentation());
              journeyResponseMap.put("journeyComplete", journeyComplete);
              journeyResponseMap.put("nodeHistories", JSONUtilities.encodeArray(nodeHistoriesJson));
              journeyResponseMap.put("currentState", currentStateJson);
              journeysJsonES.add(JSONUtilities.encodeObject(journeyResponseMap));
            }
          response.put("journeys", JSONUtilities.encodeArray(journeysJsonES));
          response.putAll(resolveAllSubscriberIDs(baseSubscriberProfile));
          updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
        }
    } 
    catch (SubscriberProfileServiceException e)
    {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }

    /*****************************************
     *
     *  return
     *
     *****************************************/

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   * processGetCustomerCampaigns
   *
   *****************************************/

  private JSONObject processGetCustomerCampaigns(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    Date now = SystemTime.getCurrentTime();
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String subscriberID = resolveSubscriberID(jsonRoot);

    String campaignObjectiveName = readString(jsonRoot, "objective", false);
    String campaignState = readString(jsonRoot, "campaignState", false);
    String customerStatus = readString(jsonRoot, "customerStatus", false);
    String campaignStartDateStr = readString(jsonRoot, "campaignStartDate", false);
    String campaignEndDateStr = readString(jsonRoot, "campaignEndDate", false);

    Date campaignStartDate = prepareStartDate(getDateFromString(campaignStartDateStr, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN));
    Date campaignEndDate = prepareEndDate(getDateFromString(campaignEndDateStr, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN));
    
    List<QueryBuilder> filters = new ArrayList<QueryBuilder>();

    /*****************************************
     *
     *  getSubscriberProfile
     *
     *****************************************/
    try
    {
      SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      if (baseSubscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
          if (log.isDebugEnabled()) log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
        }
      else
        {
          List<JSONObject> campaignsJsonES = new ArrayList<JSONObject>();
          
          SearchRequest searchRequest = getSearchRequest(API.getCustomerCampaigns, subscriberID, campaignStartDate, filters);
          List<SearchHit> hits = getESHits(searchRequest);
          Map<String, JourneyHistory> journeyHistoryMap = new HashMap<String, JourneyHistory>(hits.size());
          for (SearchHit hit : hits)
            {
              Map<String, Object> esFields = hit.getSourceAsMap();
              JourneyHistory journeyHistory = new JourneyHistory(esFields);
              journeyHistoryMap.put(journeyHistory.getJourneyID(), journeyHistory);
            }


          //
          // read campaigns
          //

          Collection<GUIManagedObject> storeRawCampaigns = journeyService.getStoredJourneys();
          List<Journey> storeCampaigns = new ArrayList<Journey>();
          for (GUIManagedObject storeCampaign : storeRawCampaigns)
            {
              if (storeCampaign instanceof Journey)
                storeCampaigns.add((Journey) storeCampaign);
            }

          //
          // filter campaigns
          //

          storeCampaigns = storeCampaigns.stream().filter(campaign -> (campaign.getGUIManagedObjectType() == GUIManagedObjectType.Campaign || campaign.getGUIManagedObjectType() == GUIManagedObjectType.BulkCampaign)).collect(Collectors.toList());

          //
          // filter on campaignStartDate
          //

          if (campaignStartDate != null)
            {
              storeCampaigns = storeCampaigns.stream().filter(campaign -> (campaign.getEffectiveStartDate() == null || campaign.getEffectiveStartDate().compareTo(campaignStartDate) >= 0)).collect(Collectors.toList());
            }

          //
          // filter on campaignEndDate
          //

          if (campaignEndDate != null)
            {
              storeCampaigns = storeCampaigns.stream().filter(campaign -> (campaign.getEffectiveEndDate() == null || campaign.getEffectiveEndDate().compareTo(campaignEndDate) <= 0)).collect(Collectors.toList());
            }

          //
          // filter on campaignObjectiveName
          //

          if (campaignObjectiveName != null && !campaignObjectiveName.isEmpty())
            {

              //
              // read objective
              //

              Collection<JourneyObjective> activecampaignObjectives = journeyObjectiveService.getActiveJourneyObjectives(SystemTime.getCurrentTime());

              //
              // lookup activecampaignObjective by name
              //

              List<JourneyObjective> campaignObjectives = activecampaignObjectives.stream().filter(journeyObj -> campaignObjectiveName.equals(journeyObj.getJSONRepresentation().get("display"))).collect(Collectors.toList());
              JourneyObjective exactCampaignObjective = campaignObjectives.size() > 0 ? campaignObjectives.get(0) : null;

              //
              // filter
              //

              if (exactCampaignObjective == null)
                storeCampaigns = new ArrayList<Journey>();
              else
                storeCampaigns = storeCampaigns.stream().filter(campaign -> (campaign.getJourneyObjectiveInstances() != null && (campaign.getJourneyObjectiveInstances().stream().filter(obj -> obj.getJourneyObjectiveID().equals(exactCampaignObjective.getJourneyObjectiveID())).count() > 0L))).collect(Collectors.toList());

            }

          for (Journey storeCampaign : storeCampaigns)
            {

              //
              // subsLatestStatistic
              //

              JourneyHistory subsLatestStatistic = journeyHistoryMap.get(storeCampaign.getJourneyID());

              //
              // continue if not in stat
              //

              if (subsLatestStatistic == null)
                continue;

              //
              // filter on campaignState
              //

              if (campaignState != null && !campaignState.isEmpty())
                {
                  boolean criteriaSatisfied = false;
                  if (journeyService.getJourneyStatus(storeCampaign).getExternalRepresentation().equalsIgnoreCase(campaignState))
                    {
                      criteriaSatisfied = true;
                    }
                  if (!criteriaSatisfied)
                    continue;
                }

              //
              // filter on customerStatus
              //

              boolean statusNotified = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> SubscriberJourneyStatus.Notified.getExternalRepresentation().equals(campaignStat.getStatus())).count() > 0L;
              boolean statusConverted = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> campaignStat.isConverted()).count() > 0L;
              Boolean statusTargetGroup = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> SubscriberJourneyStatus.Targeted.getExternalRepresentation().equals(campaignStat.getStatus())).count() > 0L;
              Boolean statusControlGroup = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> SubscriberJourneyStatus.ControlGroup.getExternalRepresentation().equals(campaignStat.getStatus()) || SubscriberJourneyStatus.ControlGroupConverted.getExternalRepresentation().equals(campaignStat.getStatus())).count() > 0L;
              Boolean statusUniversalControlGroup = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> SubscriberJourneyStatus.UniversalControlGroup.getExternalRepresentation().equals(campaignStat.getStatus()) || SubscriberJourneyStatus.UniversalControlGroupConverted.getExternalRepresentation().equals(campaignStat.getStatus())).count() > 0L;
              boolean campaignComplete = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> campaignStat.getJourneyComplete()).count() > 0L; // ??
                                                                                                                                                                 // ES

              SubscriberJourneyStatus customerStatusInJourney = Journey.getSubscriberJourneyStatus(statusConverted, statusNotified, statusTargetGroup, statusControlGroup, statusUniversalControlGroup);
              SubscriberJourneyStatus profilejourneyStatus = baseSubscriberProfile.getSubscriberJourneys().get(storeCampaign.getJourneyID() + "");
              if (profilejourneyStatus.in(SubscriberJourneyStatus.NotEligible, SubscriberJourneyStatus.UniversalControlGroup, SubscriberJourneyStatus.Excluded, SubscriberJourneyStatus.ObjectiveLimitReached))
                customerStatusInJourney = profilejourneyStatus;

              if (customerStatus != null)
                {
                  SubscriberJourneyStatus customerStatusInReq = SubscriberJourneyStatus.fromExternalRepresentation(customerStatus);
                  boolean criteriaSatisfied = customerStatusInReq == customerStatusInJourney;
                  if (!criteriaSatisfied)
                    continue;
                }

              //
              // prepare response
              //

              Map<String, Object> campaignResponseESMap = new HashMap<String, Object>();
              campaignResponseESMap.put("campaignID", storeCampaign.getJourneyID());
              campaignResponseESMap.put("campaignName", journeyService.generateResponseJSON(storeCampaign, true, SystemTime.getCurrentTime()).get("display"));
              campaignResponseESMap.put("description", journeyService.generateResponseJSON(storeCampaign, true, SystemTime.getCurrentTime()).get("description"));
              campaignResponseESMap.put("startDate", getDateString(storeCampaign.getEffectiveStartDate()));
              campaignResponseESMap.put("endDate", getDateString(storeCampaign.getEffectiveEndDate()));
              campaignResponseESMap.put("entryDate", getDateString(subsLatestStatistic.getJourneyEntranceDate()));
              campaignResponseESMap.put("exitDate", subsLatestStatistic.getJourneyExitDate(journeyService) != null ? getDateString(subsLatestStatistic.getJourneyExitDate(journeyService)) : "");
              campaignResponseESMap.put("campaignState", journeyService.getJourneyStatus(storeCampaign).getExternalRepresentation());
              List<JSONObject> resultObjectives = new ArrayList<JSONObject>();
              for (JourneyObjectiveInstance journeyObjectiveInstance : storeCampaign.getJourneyObjectiveInstances())
                {
                  List<JSONObject> resultCharacteristicsES = new ArrayList<JSONObject>();
                  JSONObject result = new JSONObject();

                  JourneyObjective journeyObjective = journeyObjectiveService.getActiveJourneyObjective(journeyObjectiveInstance.getJourneyObjectiveID(), SystemTime.getCurrentTime());
                  result.put("active", journeyObjective.getActive());
                  result.put("parentJourneyObjectiveID", journeyObjective.getParentJourneyObjectiveID());
                  result.put("display", journeyObjective.getJSONRepresentation().get("display"));
                  result.put("readOnly", journeyObjective.getReadOnly());
                  result.put("name", journeyObjective.getGUIManagedObjectName());
                  result.put("contactPolicyID", journeyObjective.getContactPolicyID());
                  result.put("id", journeyObjective.getGUIManagedObjectID());

                  for (CatalogCharacteristicInstance catalogCharacteristicInstance : journeyObjectiveInstance.getCatalogCharacteristics())
                    {
                      JSONObject characteristics = new JSONObject();
                      CatalogCharacteristic catalogCharacteristic = catalogCharacteristicService.getActiveCatalogCharacteristic(catalogCharacteristicInstance.getCatalogCharacteristicID(), now);
                      characteristics.put("catalogCharacteristicID", catalogCharacteristic.getCatalogCharacteristicID());
                      characteristics.put("catalogCharacteristicName", catalogCharacteristic.getCatalogCharacteristicName());
                      characteristics.put("catalogCharacteristicDataType", catalogCharacteristic.getDataType().getExternalRepresentation());
                      characteristics.put("value", catalogCharacteristicInstance.getValue());
                      resultCharacteristicsES.add(characteristics);
                    }

                  result.put("catalogCharacteristics", JSONUtilities.encodeArray(resultCharacteristicsES));
                  resultObjectives.add(result);
                }

              campaignResponseESMap.put("objectives", JSONUtilities.encodeArray(resultObjectives));

              NodeHistory nodeHistory = subsLatestStatistic.getLastNodeEntered();
              Map<String, Object> currentState = new HashMap<String, Object>();
              currentState.put("nodeID", nodeHistory.getToNodeID());
              currentState.put("nodeName", nodeHistory.getToNodeID() == null ? null : (storeCampaign.getJourneyNode(nodeHistory.getToNodeID()) == null ? "node has been removed" : storeCampaign.getJourneyNode(nodeHistory.getToNodeID()).getNodeName()));
              JSONObject currentStateJson = JSONUtilities.encodeObject(currentState);

              //
              // node history
              //

              List<JSONObject> nodeHistoriesJson = new ArrayList<JSONObject>();
              for (NodeHistory journeyHistories : subsLatestStatistic.getNodeHistory())
                {
                  Map<String, Object> nodeHistoriesMap = new HashMap<String, Object>();
                  nodeHistoriesMap.put("fromNodeID", journeyHistories.getFromNodeID());
                  nodeHistoriesMap.put("toNodeID", journeyHistories.getToNodeID());
                  nodeHistoriesMap.put("fromNode", journeyHistories.getFromNodeID() == null ? null : (storeCampaign.getJourneyNode(journeyHistories.getFromNodeID()) == null ? "node has been removed" : storeCampaign.getJourneyNode(journeyHistories.getFromNodeID()).getNodeName()));
                  nodeHistoriesMap.put("toNode", journeyHistories.getToNodeID() == null ? null : (storeCampaign.getJourneyNode(journeyHistories.getToNodeID()) == null ? "node has been removed" : storeCampaign.getJourneyNode(journeyHistories.getToNodeID()).getNodeName()));
                  nodeHistoriesMap.put("transitionDate", getDateString(journeyHistories.getTransitionDate()));
                  nodeHistoriesMap.put("linkID", journeyHistories.getLinkID());
                  nodeHistoriesMap.put("deliveryRequestID", journeyHistories.getDeliveryRequestID());
                  nodeHistoriesJson.add(JSONUtilities.encodeObject(nodeHistoriesMap));
                }
              campaignResponseESMap.put("customerStatus", customerStatusInJourney.getExternalRepresentation());
              campaignResponseESMap.put("campaignComplete", campaignComplete);
              campaignResponseESMap.put("nodeHistories", JSONUtilities.encodeArray(nodeHistoriesJson));
              campaignResponseESMap.put("currentState", currentStateJson);
              campaignsJsonES.add(JSONUtilities.encodeObject(campaignResponseESMap));
            }
          response.put("campaigns", JSONUtilities.encodeArray(campaignsJsonES));
          response.putAll(resolveAllSubscriberIDs(baseSubscriberProfile));
          updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
        }
    } 
    catch (SubscriberProfileServiceException e)
    {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }

    /*****************************************
     *
     *  return
     *
     *****************************************/

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  * processGetCustomerLoyaltyPrograms
  *
  *****************************************/

 private JSONObject processGetCustomerLoyaltyPrograms(JSONObject jsonRoot) throws ThirdPartyManagerException
 {

   Map<String, Object> response = new HashMap<String, Object>();

   /****************************************
    *
    *  argument
    *
    ****************************************/

   String subscriberID = resolveSubscriberID(jsonRoot);

   String searchedLoyaltyProgramID = readString(jsonRoot, "loyaltyProgramID", false);
   if(searchedLoyaltyProgramID != null && searchedLoyaltyProgramID.isEmpty()){ searchedLoyaltyProgramID = null; }

   /*****************************************
    *
    *  getSubscriberProfile
    *
    *****************************************/
   try
   {
     SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
     if (baseSubscriberProfile == null)
       {
         updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
         if (log.isDebugEnabled()) log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
       }
     else
       {

         Date now = SystemTime.getCurrentTime();
         Map<String,LoyaltyProgramState> loyaltyPrograms = baseSubscriberProfile.getLoyaltyPrograms();
         List<JSONObject> loyaltyProgramsPresentation = new ArrayList<JSONObject>();
         for (String loyaltyProgramID : loyaltyPrograms.keySet())
           {

             //
             //  check loyalty program still exist
             //

             LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramID, now);
             if (loyaltyProgram != null && (searchedLoyaltyProgramID == null || loyaltyProgramID.equals(searchedLoyaltyProgramID)))
               {

                 HashMap<String, Object> loyaltyProgramPresentation = new HashMap<String,Object>();

                 //
                 //  loyalty program informations
                 //

                 LoyaltyProgramState loyaltyProgramState = loyaltyPrograms.get(loyaltyProgramID);
                 loyaltyProgramPresentation.put("loyaltyProgramType", loyaltyProgram.getLoyaltyProgramType().getExternalRepresentation());
                 loyaltyProgramPresentation.put("loyaltyProgramName", loyaltyProgramState.getLoyaltyProgramName());
                 loyaltyProgramPresentation.put("loyaltyProgramEnrollmentDate", getDateString(loyaltyProgramState.getLoyaltyProgramEnrollmentDate()));
                 loyaltyProgramPresentation.put("loyaltyProgramExitDate", getDateString(loyaltyProgramState.getLoyaltyProgramExitDate()));


                 switch (loyaltyProgramState.getLoyaltyProgramType()) {
                   case POINTS:

                     LoyaltyProgramPointsState loyaltyProgramPointsState = (LoyaltyProgramPointsState) loyaltyProgramState;

                     //
                     //  current tier
                     //

                     if(loyaltyProgramPointsState.getTierName() != null){ loyaltyProgramPresentation.put("tierName", loyaltyProgramPointsState.getTierName()); }
                     if(loyaltyProgramPointsState.getTierEnrollmentDate() != null){ loyaltyProgramPresentation.put("tierEnrollmentDate", getDateString(loyaltyProgramPointsState.getTierEnrollmentDate())); }

                     //
                     //  status point
                     //
                     
                     LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints) loyaltyProgram;
                     String statusPointID = loyaltyProgramPoints.getStatusPointsID();
                     PointBalance pointBalance = baseSubscriberProfile.getPointBalances().get(statusPointID);
                     if(pointBalance != null)
                       {
                         loyaltyProgramPresentation.put("statusPointsBalance", pointBalance.getBalance(now));
                       }
                     else
                       {
                         loyaltyProgramPresentation.put("statusPointsBalance", 0);
                       }
                     
                     //
                     //  reward point informations
                     //

                     String rewardPointID = loyaltyProgramPoints.getRewardPointsID();
                     PointBalance rewardBalance = baseSubscriberProfile.getPointBalances().get(rewardPointID);
                     if(rewardBalance != null)
                       {
                         loyaltyProgramPresentation.put("rewardsPointsBalance", rewardBalance.getBalance(now));
                         loyaltyProgramPresentation.put("rewardsPointsEarned", rewardBalance.getEarnedHistory().getAllTimeBucket());
                         loyaltyProgramPresentation.put("rewardsPointsConsumed", rewardBalance.getConsumedHistory().getAllTimeBucket());
                         loyaltyProgramPresentation.put("rewardsPointsExpired", rewardBalance.getExpiredHistory().getAllTimeBucket());
                         Date firstExpirationDate = rewardBalance.getFirstExpirationDate(now);
                         if(firstExpirationDate != null)
                           {
                             int firstExpirationQty = rewardBalance.getBalance(firstExpirationDate);
                             loyaltyProgramPresentation.put("rewardsPointsEarliestexpirydate", getDateString(firstExpirationDate));
                             loyaltyProgramPresentation.put("rewardsPointsEarliestexpiryquantity", firstExpirationQty);
                           }
                         else
                           {
                             loyaltyProgramPresentation.put("rewardsPointsEarliestexpirydate", getDateString(now));
                             loyaltyProgramPresentation.put("rewardsPointsEarliestexpiryquantity", 0);
                           }
                       }
                     else
                       {
                         loyaltyProgramPresentation.put("rewardsPointsBalance", 0);
                         loyaltyProgramPresentation.put("rewardsPointsEarned", 0);
                         loyaltyProgramPresentation.put("rewardsPointsConsumed", 0);
                         loyaltyProgramPresentation.put("rewardsPointsExpired", 0);
                         loyaltyProgramPresentation.put("rewardsPointsEarliestexpirydate", getDateString(now));
                         loyaltyProgramPresentation.put("rewardsPointsEarliestexpiryquantity", 0);
                       }

                     //
                     //  history
                     //
                     ArrayList<JSONObject> loyaltyProgramHistoryJSON = new ArrayList<JSONObject>();
                     LoyaltyProgramHistory history = loyaltyProgramPointsState.getLoyaltyProgramHistory();
                     if(history != null && history.getTierHistory() != null && !history.getTierHistory().isEmpty()){
                       for(TierHistory tier : history.getTierHistory()){
                         HashMap<String, Object> tierHistoryJSON = new HashMap<String,Object>();
                         tierHistoryJSON.put("fromTier", tier.getFromTier());
                         tierHistoryJSON.put("toTier", tier.getToTier());
                         tierHistoryJSON.put("transitionDate", getDateString(tier.getTransitionDate()));
                         loyaltyProgramHistoryJSON.add(JSONUtilities.encodeObject(tierHistoryJSON));
                       }
                     }
                     loyaltyProgramPresentation.put("loyaltyProgramHistory", loyaltyProgramHistoryJSON);

                     break;

//                   case BADGES:
//                     // TODO
//                     break;

                   default:
                     break;
                 }

                 //
                 //  
                 //

                 loyaltyProgramsPresentation.add(JSONUtilities.encodeObject(loyaltyProgramPresentation));

               }
           }

         response.put("loyaltyPrograms", JSONUtilities.encodeArray(loyaltyProgramsPresentation));
         response.putAll(resolveAllSubscriberIDs(baseSubscriberProfile));
         updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
       }
   } 
   catch (SubscriberProfileServiceException e)
   {
     log.error("SubscriberProfileServiceException ", e.getMessage());
     throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
   }

   /*****************************************
    *
    *  return
    *
    *****************************************/

   return JSONUtilities.encodeObject(response);
 }

 /*****************************************
 *
 *  processGetActiveOffer
 *
 *****************************************/

 private JSONObject processGetLoyaltyProgram(JSONObject jsonRoot) throws ThirdPartyManagerException
 {
   /****************************************
    *
    *  response
    *
    ****************************************/

   HashMap<String,Object> response = new HashMap<String,Object>();

   /****************************************
    *
    *  argument
    *
    ****************************************/

   String loyaltyProgramID = readString(jsonRoot, "loyaltyProgramID", false);
   if (loyaltyProgramID == null) // this is mandatory, but we want to control the return code
     {
       response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
       response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage());
       return JSONUtilities.encodeObject(response);          
     }

   /*****************************************
    *
    *  retrieve loyaltyProgram
    *
    *****************************************/

   LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramID, SystemTime.getCurrentTime());

   /*****************************************
    *
    *  decorate and response
    *
    *****************************************/

   if (loyaltyProgram == null)
     {
       updateResponse(response, RESTAPIGenericReturnCodes.LOYALTY_PROJECT_NOT_FOUND);
       return JSONUtilities.encodeObject(response);
     }
   else 
     {
       response.put("loyaltyProgram", ThirdPartyJSONGenerator.generateLoyaltyProgramJSONForThirdParty(loyaltyProgram));
       updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
       return JSONUtilities.encodeObject(response);
     }
 }

  /*****************************************
  *
  *  processGetLoyaltyProgramList
  *
  *****************************************/

 private JSONObject processGetLoyaltyProgramList(JSONObject jsonRoot) throws ThirdPartyManagerException
 {
   /****************************************
   *
   *  response
   *
   ****************************************/

  HashMap<String,Object> response = new HashMap<String,Object>();

  /****************************************
  *
  *  argument
  *
  ****************************************/

  String type = readString(jsonRoot, "loyaltyProgramType", false);
  LoyaltyProgramType loyaltyProgramType = null;
  if(type != null){
    loyaltyProgramType = LoyaltyProgramType.fromExternalRepresentation(type);
    if(loyaltyProgramType.equals(LoyaltyProgramType.Unknown)){
      updateResponse(response, RESTAPIGenericReturnCodes.LOYALTY_TYPE_NOT_FOUND);
      return JSONUtilities.encodeObject(response);
    }
  }

   /*****************************************
   *
   *  retrieve loyalty programs
   *
   *****************************************/

  Collection<LoyaltyProgram> programs = loyaltyProgramService.getActiveLoyaltyPrograms(SystemTime.getCurrentTime());

   /*****************************************
   *
   *  decorate and response
   *
   *****************************************/

  List<JSONObject> loyaltyProgramsJson = new ArrayList<JSONObject>();
  for(LoyaltyProgram program : programs){
    if(loyaltyProgramType == null || program.getLoyaltyProgramType().equals(loyaltyProgramType)){
      loyaltyProgramsJson.add(ThirdPartyJSONGenerator.generateLoyaltyProgramJSONForThirdParty(program));
    }
  }
  
  response.put("loyaltyPrograms", JSONUtilities.encodeArray(loyaltyProgramsJson));
  updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
  return JSONUtilities.encodeObject(response);
}
 
   /*****************************************
   *
   *  processGetOffersList
   * @throws ParseException 
   * @throws IOException 
   *
   *****************************************/

  private JSONObject processGetOffersList(JSONObject jsonRoot) throws ThirdPartyManagerException, IOException, ParseException
  {

    /****************************************
     *
     *  response
     *
     ****************************************/

    Map<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/
    String subscriberID = null;
    boolean subscriberParameter = false;
    if (JSONUtilities.decodeString(jsonRoot, CUSTOMER_ID, false) == null)
      {
        for (String id : Deployment.getAlternateIDs().keySet())
          {
            if (JSONUtilities.decodeString(jsonRoot, id, false) != null)
              {
                subscriberParameter = true;
                break;
              }
          }
      }
    else
      {
        subscriberParameter = true;
      }
    if (subscriberParameter)
      {
        subscriberID = resolveSubscriberID(jsonRoot);
      }

    String offerState = readString(jsonRoot, "state", false);
    String startDateString = readString(jsonRoot, "startDate", false);
    String endDateString = readString(jsonRoot, "endDate", false);
    String offerObjective = readString(jsonRoot, "objective", false);
    String supplier = readString(jsonRoot, "supplier", false);
    //String userID = readString(jsonRoot, "loginName", true);
    String parentResellerID = "";
    Date now = SystemTime.getCurrentTime();
    AuthenticatedResponse authResponse = null;
    ThirdPartyCredential thirdPartyCredential = new ThirdPartyCredential(jsonRoot);
    if (!Deployment.getRegressionMode())
      {
        authResponse = authCache.get(thirdPartyCredential);
      }
    else
      {
        authResponse = authenticate(thirdPartyCredential);
      }
    int user = (authResponse.getUserId());
    String userID = Integer.toString(user);
    
    Date offerStartDate = prepareStartDate(getDateFromString(startDateString, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN));
    Date offerEndDate = prepareEndDate(getDateFromString(endDateString, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN));
    
    Map<String, List<String>> activeResellerAndSalesChannelIDs = activeResellerAndSalesChannelIDs(userID);    

    try
    {
      SubscriberProfile subscriberProfile = null;
      if (subscriberID != null)
        {
          subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID);
          if (subscriberProfile == null)
            {
              updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
              return JSONUtilities.encodeObject(response);
            }
        }
    
      if ((activeResellerAndSalesChannelIDs.containsKey("activeReseller")) && (activeResellerAndSalesChannelIDs.get("activeReseller")).size() == 0) {
        updateResponse(response, RESTAPIGenericReturnCodes.INACTIVE_RESELLER);
        return JSONUtilities.encodeObject(response);
      }
      
      if (activeResellerAndSalesChannelIDs.containsKey("salesChannelIDsList") && (activeResellerAndSalesChannelIDs.get("salesChannelIDsList")).size() == 0) {
        updateResponse(response, RESTAPIGenericReturnCodes.RESELLER_WITHOUT_SALESCHANNEL);
        return JSONUtilities.encodeObject(response);
      }
      if (offerState != null && !offerState.isEmpty() && !offerState.equalsIgnoreCase("ACTIVE"))
        {
          updateResponse(response, RESTAPIGenericReturnCodes.BAD_FIELD_VALUE, "-(state)");
        }
      else
        {
          Collection<Offer> offers = new ArrayList<>();
          if (offerState == null || offerState.isEmpty())
            {
              //
              // retrieve stored offers
              //

              for (GUIManagedObject ofr : offerService.getStoredOffers())
                {
                  if (ofr instanceof Offer) offers.add( (Offer) ofr);
                }
            }
          else
            {
              //
              // retrieve active offers
              //

              offers = offerService.getActiveOffers(SystemTime.getCurrentTime());
            }

          //
          // filter using subscriberID
          //

          if (subscriberID != null)
            {
              SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime());
              offers = offers.stream().filter(offer -> offer.evaluateProfileCriteria(evaluationRequest)).collect(Collectors.toList());
            }

          //
          // filter using startDate
          //

          if (offerStartDate != null)
            {
              offers = offers.stream().filter(offer -> (offer.getEffectiveStartDate() == null || offer.getEffectiveStartDate().compareTo(offerStartDate) >= 0)).collect(Collectors.toList()); 
            }

          //
          // filter using endDate
          //

          if (offerEndDate != null)
            {
              offers = offers.stream().filter(campaign -> (campaign.getEffectiveEndDate() == null || campaign.getEffectiveEndDate().compareTo(offerEndDate) <= 0)).collect(Collectors.toList());
            }


          if (offerObjective != null && !offerObjective.isEmpty())
            {

              //
              //  read objective
              //

              Collection<OfferObjective> activeOfferObjectives = offerObjectiveService.getActiveOfferObjectives(SystemTime.getCurrentTime());

              //
              //  filter activejourneyObjective by name
              //

              List<OfferObjective> offerObjectives = activeOfferObjectives.stream().filter(offerObj -> offerObj.getOfferObjectiveDisplay().equals(offerObjective)).collect(Collectors.toList());
              OfferObjective exactOfferObjectives = offerObjectives.size() > 0 ? offerObjectives.get(0) : null;

              //
              //  filter
              //

              if (exactOfferObjectives == null)
                offers = new ArrayList<Offer>();
              else
                offers = offers.stream().filter(offer -> (offer.getOfferObjectives() != null && (offer.getOfferObjectives().stream().filter(obj -> obj.getOfferObjectiveID().equals(exactOfferObjectives.getOfferObjectiveID())).count() > 0L))).collect(Collectors.toList());

            }
          
          //
          //filter using supplier
          //
          
          List<String> filteredBySupplierOfferIDs = new ArrayList<>();
          if (supplier != null && !supplier.isEmpty())
            {
              for (Offer offer : offers)
                {                    
                  Set<OfferProduct> offerProducts = offer.getOfferProducts();
                  Set<OfferVoucher> offerVouchers = offer.getOfferVouchers();
                  if (offerProducts != null && offerProducts.size() != 0)
                    {
                      for (OfferProduct offerproduct : offerProducts)
                        {
                          String productID = offerproduct.getProductID();
                          GUIManagedObject productObject = productService.getStoredProduct(productID);
                          if (productObject != null && productObject instanceof Product)
                            {
                              Product product = (Product) productObject;
                              GUIManagedObject SupplierObject = supplierService
                                  .getStoredSupplier(product.getSupplierID());
                              if (SupplierObject != null && SupplierObject instanceof Supplier)
                                {
                                  String supplierDisplay = ((Supplier) SupplierObject).getGUIManagedObjectDisplay();

                                  if (supplierDisplay.equals(supplier))
                                    {
                                      filteredBySupplierOfferIDs.add(offer.getOfferID());
                                      break;
                                    }

                                  else
                                    {
                                      if (log.isDebugEnabled())
                                        log.debug(productObject + "is not a complete product");
                                    }
                                }
                            }
                        }
                    }
                  if (offerVouchers != null && offerVouchers.size() != 0)
                    {
                      for (OfferVoucher offerVoucher : offerVouchers)
                        {
                          String voucherID = offerVoucher.getVoucherID();
                          GUIManagedObject voucherObject = voucherService.getStoredVoucher(voucherID);
                          if (voucherObject != null && voucherObject instanceof Voucher)
                            {
                              Voucher voucher = (Voucher) voucherObject;
                              GUIManagedObject SupplierObject = supplierService
                                  .getStoredSupplier(voucher.getSupplierID());
                              if (SupplierObject != null && SupplierObject instanceof Supplier)
                                {
                                  String supplierDisplay = ((Supplier) SupplierObject).getGUIManagedObjectDisplay();

                                  if (supplierDisplay.equals(supplier))
                                    {
                                      filteredBySupplierOfferIDs.add(offer.getOfferID());
                                      break;
                                    }

                                  else
                                    {
                                      if (log.isDebugEnabled())
                                        log.debug(voucherObject + "is not a complete voucher");
                                    }
                                }

                            }
                        }
                    }
                }
              offers = offers.stream()
                  .filter(offer -> (offer.getOfferID() != null) && (filteredBySupplierOfferIDs.contains(offer.getOfferID())))
                  .collect(Collectors.toList());

            }

          if (activeResellerAndSalesChannelIDs.containsKey("salesChannelIDsList") && (activeResellerAndSalesChannelIDs.get("salesChannelIDsList")).size() != 0)
            {
              List salesChannelIDList = activeResellerAndSalesChannelIDs.get("salesChannelIDsList");
              offers = offers.stream()
                  .filter(offer -> (offer.getOfferSalesChannelsAndPrices() != null && (offer
                      .getOfferSalesChannelsAndPrices().stream()
                      .filter(object -> (object.getSalesChannelIDs() != null
                          && (object.getSalesChannelIDs().stream().filter(obj -> salesChannelIDList.contains(obj)))
                              .count() > 0L))
                      .count() > 0L)))
                  .collect(Collectors.toList());

            } 
          //
          // filter the offers based on the product supplier parent ID
          //
          
          offers = offers.stream().filter(offer -> (offerValidation(offer.getOfferID()))).collect(Collectors.toList());
          

          /*****************************************
           *
           *  decorate offers response
           *
           *****************************************/

          List<JSONObject> offersJson = offers.stream().map(offer -> ThirdPartyJSONGenerator.generateOfferJSONForThirdParty(offer, offerService, offerObjectiveService, productService, voucherService, salesChannelService)).collect(Collectors.toList());
          response.put("offers", JSONUtilities.encodeArray(offersJson));
          updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
        }
    }
    catch(SubscriberProfileServiceException spe)
    {
      updateResponse(response, RESTAPIGenericReturnCodes.SYSTEM_ERROR);
      log.error("SubscriberProfileServiceException {}", spe);
    }

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processGetCustomerAvailableCampaigns
   *
   *****************************************/

  private JSONObject processGetCustomerAvailableCampaigns(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    Date now = SystemTime.getCurrentTime();
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
     *
     * argument
     *
     ****************************************/
    String subscriberID = resolveSubscriberID(jsonRoot);
    
    /*****************************************
     *
     * getSubscriberProfile - include history
     *
     *****************************************/
    try
    {
      SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, true);
      if (subscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
          if (log.isDebugEnabled()) log.debug("SubscriberProfile is null for subscriberID {}", subscriberID);
        } 
      else
        {
          SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, now);
          
          //
          //  journeys
          //
          
          List<String> enteredJourneysID = new ArrayList<String>();
          enteredJourneysID.addAll(subscriberProfile.getSubscriberJourneys().keySet());
          enteredJourneysID.addAll(subscriberProfile.getSubscriberJourneysEnded().keySet()); // double check
          
          //
          //  read the active journeys
          //

          Collection<Journey> activeCampaigns = journeyService.getActiveJourneys(now);

          //
          //  respect manual campaigns only
          //

          activeCampaigns = activeCampaigns.stream().filter(activeJourney -> TargetingType.Manual == activeJourney.getTargetingType()).collect(Collectors.toList());

          //
          // list the eligible campaigns
          //

          Collection<Journey> elgibleActiveCampaigns = activeCampaigns.stream().filter(activeJourney -> activeJourney.evaluateEligibilityCriteria(evaluationRequest)).collect(Collectors.toList());

          //
          //  consider if not enter
          //

          List<JSONObject> campaignsJson = new ArrayList<JSONObject>();
          for (Journey elgibleActiveCampaign : elgibleActiveCampaigns)
            {
              if (!enteredJourneysID.contains(elgibleActiveCampaign.getJourneyID()))
                {
                  //
                  // prepare and decorate response
                  //

                  Map<String, Object> campaignMap = new HashMap<String, Object>();
                  campaignMap.put("campaignID", elgibleActiveCampaign.getJourneyID());
                  campaignMap.put("campaignName", journeyService.generateResponseJSON(elgibleActiveCampaign, true, SystemTime.getCurrentTime()).get("display"));
                  campaignMap.put("description", journeyService.generateResponseJSON(elgibleActiveCampaign, true, now).get("description"));
                  campaignMap.put("startDate", getDateString(elgibleActiveCampaign.getEffectiveStartDate()));
                  campaignMap.put("endDate", getDateString(elgibleActiveCampaign.getEffectiveEndDate()));
                  List<JSONObject> resultObjectives = new ArrayList<JSONObject>();
                  for (JourneyObjectiveInstance journeyObjectiveInstance : elgibleActiveCampaign.getJourneyObjectiveInstances())
                    {
                      List<JSONObject> resultCharacteristics = new ArrayList<JSONObject>();
                      JSONObject result = new JSONObject();

                      JourneyObjective journeyObjective = journeyObjectiveService.getActiveJourneyObjective(journeyObjectiveInstance.getJourneyObjectiveID(), SystemTime.getCurrentTime());
                      result.put("active", journeyObjective.getActive());
                      result.put("parentJourneyObjectiveID", journeyObjective.getParentJourneyObjectiveID());
                      result.put("display", journeyObjective.getJSONRepresentation().get("display"));
                      result.put("readOnly", journeyObjective.getReadOnly());
                      result.put("name", journeyObjective.getGUIManagedObjectName());
                      result.put("contactPolicyID", journeyObjective.getContactPolicyID());
                      result.put("id", journeyObjective.getGUIManagedObjectID());

                      for (CatalogCharacteristicInstance catalogCharacteristicInstance : journeyObjectiveInstance.getCatalogCharacteristics())
                        {
                          JSONObject characteristics = new JSONObject();
                          CatalogCharacteristic catalogCharacteristic = catalogCharacteristicService.getActiveCatalogCharacteristic(catalogCharacteristicInstance.getCatalogCharacteristicID(), now);
                          characteristics.put("catalogCharacteristicID", catalogCharacteristic.getCatalogCharacteristicID());
                          characteristics.put("catalogCharacteristicName", catalogCharacteristic.getCatalogCharacteristicName());
                          characteristics.put("catalogCharacteristicDataType", catalogCharacteristic.getDataType().getExternalRepresentation());
                          characteristics.put("value", catalogCharacteristicInstance.getValue());
                          resultCharacteristics.add(characteristics);
                        }

                      result.put("catalogCharacteristics", JSONUtilities.encodeArray(resultCharacteristics));
                      resultObjectives.add(result);
                    }                   
                  campaignMap.put("objectives", JSONUtilities.encodeArray(resultObjectives));
                  campaignsJson.add(JSONUtilities.encodeObject(campaignMap));
                }
            }
          response.put("campaigns", JSONUtilities.encodeArray(campaignsJson));
          response.putAll(resolveAllSubscriberIDs(subscriberProfile));
          updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
        }
    }
    catch (SubscriberProfileServiceException e)
    {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }

    /*****************************************
     *
     * return
     *
     *****************************************/

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processUpdateCustomer
   *
   *****************************************/

  private JSONObject processUpdateCustomer(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
     *
     * argument
     *
     ****************************************/

    String subscriberID = resolveSubscriberID(jsonRoot);
    
    SubscriberProfile baseSubscriberProfile = null;
    try
    {
      baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true, false);
      if (baseSubscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
          return JSONUtilities.encodeObject(response);
        }
      baseSubscriberProfile.validateUpdateProfileRequest(jsonRoot);

      jsonRoot.put("subscriberID", subscriberID);
      SubscriberProfileForceUpdate subscriberProfileForceUpdate = new SubscriberProfileForceUpdate(jsonRoot);

      //
      //  submit to kafka
      //

      kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getSubscriberProfileForceUpdateTopic(), StringKey.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), new StringKey(subscriberProfileForceUpdate.getSubscriberID())), SubscriberProfileForceUpdate.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), subscriberProfileForceUpdate)));
      updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
    }
    catch (GUIManagerException | SubscriberProfileServiceException e) 
    {
      log.error("unable to process request updateCustomer {} ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    } 
    catch (ValidateUpdateProfileRequestException e)
    {
      throw new ThirdPartyManagerException(e.getMessage(), e.getResponseCode());
    }
    
    /*****************************************
     *
     * return
     *
     *****************************************/

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processUpdateCustomerParent
   *
   *****************************************/

  private JSONObject processUpdateCustomerParent(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    /****************************************
    *
    * /!\ this code is duplicated in GUImanager & ThirdPartyManager, do not forget to update both.
    *
    ****************************************/
    
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
    *
    * argument
    *
    ****************************************/

    String subscriberID = resolveSubscriberID(jsonRoot);

    String relationshipDisplay = JSONUtilities.decodeString(jsonRoot, "relationship", true);
    String newParentSubscriberID = resolveParentSubscriberID(jsonRoot);

    /*****************************************
    *
    * resolve relationship
    *
    *****************************************/
      
    boolean isRelationshipSupported = false;
    String relationshipID = null;
    for (SupportedRelationship supportedRelationship : Deployment.getSupportedRelationships().values())
      {
        if (supportedRelationship.getDisplay().equals(relationshipDisplay))
          {
            isRelationshipSupported = true;
            relationshipID = supportedRelationship.getID();
            break;
          }
      }
    
    if(!isRelationshipSupported)
      {
        updateResponse(response, RESTAPIGenericReturnCodes.RELATIONSHIP_NOT_FOUND);
        return JSONUtilities.encodeObject(response);
      }
    
    if (subscriberID == null)
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage()
            + "-{specified customerID do not relate to any customer}");
        return JSONUtilities.encodeObject(response);
      } 
    else if (newParentSubscriberID == null)
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage()
            + "-{specified newParentCustomerID do not relate to any customer}");
        return JSONUtilities.encodeObject(response);
      } 
    else if (subscriberID.equals(newParentSubscriberID))
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage()
            + "-{a customer cannot be its own parent}");
        return JSONUtilities.encodeObject(response);
      }

    try
      {
        SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID);
        String previousParentSubscriberID = null;
        SubscriberRelatives relatives = subscriberProfile.getRelations().get(relationshipID);
        if(relatives != null) 
          {
            previousParentSubscriberID = relatives.getParentSubscriberID(); // can still be null if undefined (no parent)
          }
        
        if(! newParentSubscriberID.equals(previousParentSubscriberID)) 
          {
            if(previousParentSubscriberID != null)
              {
                //
                // Delete child for the parent 
                // 
                
                jsonRoot.put("subscriberID", previousParentSubscriberID);
                SubscriberProfileForceUpdate previousParentProfileForceUpdate = new SubscriberProfileForceUpdate(jsonRoot);
                ParameterMap previousParentParameterMap = previousParentProfileForceUpdate.getParameterMap();
                previousParentParameterMap.put("subscriberRelationsUpdateMethod", SubscriberRelationsUpdateMethod.RemoveChild.getExternalRepresentation());
                previousParentParameterMap.put("relationshipID", relationshipID);
                previousParentParameterMap.put("relativeSubscriberID", subscriberID);
                
                //
                // submit to kafka 
                //
                  
                kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getSubscriberProfileForceUpdateTopic(), StringKey.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), new StringKey(previousParentProfileForceUpdate.getSubscriberID())), SubscriberProfileForceUpdate.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), previousParentProfileForceUpdate)));
                
              }
            

            //
            // Set child for the new parent 
            //
            
            jsonRoot.put("subscriberID", newParentSubscriberID);
            SubscriberProfileForceUpdate newParentProfileForceUpdate = new SubscriberProfileForceUpdate(jsonRoot);
            ParameterMap newParentParameterMap = newParentProfileForceUpdate.getParameterMap();
            newParentParameterMap.put("subscriberRelationsUpdateMethod", SubscriberRelationsUpdateMethod.AddChild.getExternalRepresentation());
            newParentParameterMap.put("relationshipID", relationshipID);
            newParentParameterMap.put("relativeSubscriberID", subscriberID);
              
            //
            // Set parent 
            //
            
            jsonRoot.put("subscriberID", subscriberID);
            SubscriberProfileForceUpdate subscriberProfileForceUpdate = new SubscriberProfileForceUpdate(jsonRoot);
            ParameterMap subscriberParameterMap = subscriberProfileForceUpdate.getParameterMap();
            subscriberParameterMap.put("subscriberRelationsUpdateMethod", SubscriberRelationsUpdateMethod.SetParent.getExternalRepresentation());
            subscriberParameterMap.put("relationshipID", relationshipID);
            subscriberParameterMap.put("relativeSubscriberID", newParentSubscriberID);
            
            //
            // submit to kafka 
            //
              
            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getSubscriberProfileForceUpdateTopic(), StringKey.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), new StringKey(newParentProfileForceUpdate.getSubscriberID())), SubscriberProfileForceUpdate.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), newParentProfileForceUpdate)));
            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getSubscriberProfileForceUpdateTopic(), StringKey.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), new StringKey(subscriberProfileForceUpdate.getSubscriberID())), SubscriberProfileForceUpdate.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), subscriberProfileForceUpdate)));
          }
        updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
      } 
    catch (GUIManagerException | SubscriberProfileServiceException e)
      {
        throw new ThirdPartyManagerException(e);
      }

    /*****************************************
    *
    * return
    *
    *****************************************/

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processRemoveCustomerParent
   *
   *****************************************/

  private JSONObject processRemoveCustomerParent(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    /****************************************
    *
    * /!\ this code is duplicated in GUImanager & ThirdPartyManager, do not forget to update both.
    *
    ****************************************/
    
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
    *
    * argument
    *
    ****************************************/

    String subscriberID = resolveSubscriberID(jsonRoot);

    String relationshipDisplay = JSONUtilities.decodeString(jsonRoot, "relationship", true);

    try
      {
        SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID);
        String previousParentSubscriberID = null;
        String relationshipID = null;
        for (SupportedRelationship supportedRelationship : Deployment.getSupportedRelationships().values())
          {
            if (supportedRelationship.getDisplay().equals(relationshipDisplay))
              {
                relationshipID = supportedRelationship.getID();
                break;
              }
          }
        SubscriberRelatives relatives = subscriberProfile.getRelations().get(relationshipID);
        if(relatives != null) 
          {
            previousParentSubscriberID = relatives.getParentSubscriberID(); // can still be null if undefined (no parent)
          }
        
        if(previousParentSubscriberID != null)
          {
            //
            // Delete child for the parent 
            // 
            
            jsonRoot.put("subscriberID", previousParentSubscriberID);
            SubscriberProfileForceUpdate parentProfileForceUpdate = new SubscriberProfileForceUpdate(jsonRoot);
            ParameterMap parentParameterMap = parentProfileForceUpdate.getParameterMap();
            parentParameterMap.put("subscriberRelationsUpdateMethod", SubscriberRelationsUpdateMethod.RemoveChild.getExternalRepresentation());
            parentParameterMap.put("relationshipID", relationshipID);
            parentParameterMap.put("relativeSubscriberID", subscriberID);
            
            
            //
            // Set parent null 
            //
            
            jsonRoot.put("subscriberID", subscriberID);
            SubscriberProfileForceUpdate subscriberProfileForceUpdate = new SubscriberProfileForceUpdate(jsonRoot);
            ParameterMap subscriberParameterMap = subscriberProfileForceUpdate.getParameterMap();
            subscriberParameterMap.put("subscriberRelationsUpdateMethod", SubscriberRelationsUpdateMethod.SetParent.getExternalRepresentation());
            subscriberParameterMap.put("relationshipID", relationshipID);
            // "relativeSubscriberID" must stay null
            
            //
            // submit to kafka 
            //
            
            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getSubscriberProfileForceUpdateTopic(), StringKey.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), new StringKey(parentProfileForceUpdate.getSubscriberID())), SubscriberProfileForceUpdate.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), parentProfileForceUpdate)));
            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getSubscriberProfileForceUpdateTopic(), StringKey.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), new StringKey(subscriberProfileForceUpdate.getSubscriberID())), SubscriberProfileForceUpdate.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), subscriberProfileForceUpdate)));
          }
        updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
      } 
    catch (GUIManagerException | SubscriberProfileServiceException e)
      {
        throw new ThirdPartyManagerException(e);
      }

    /*****************************************
    *
    * return
    *
    *****************************************/

    return JSONUtilities.encodeObject(response);
  }

  
  /*****************************************
  *
  *  processGetCustomerTokenAndNBO
  *
  *****************************************/

 private JSONObject processGetCustomerTokenAndNBO(JSONObject jsonRoot) throws ThirdPartyManagerException
 {
   /****************************************
    *
    *  response
    *
    ****************************************/

   HashMap<String,Object> response = new HashMap<String,Object>();

   /****************************************
    *
    *  argument
    *
    ****************************************/

   String subscriberID = resolveSubscriberID(jsonRoot);
   
   // one of the two must be there
   String presentationStrategyID = JSONUtilities.decodeString(jsonRoot, "presentationStrategyID", false);
   String presentationStrategyDisplay = JSONUtilities.decodeString(jsonRoot, "presentationStrategy", false);

   String tokenTypeDisplay = JSONUtilities.decodeString(jsonRoot, "tokenType", false);
   if (tokenTypeDisplay == null) // this is mandatory, but we want to control the return code
     {
       response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
       response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage());
       return JSONUtilities.encodeObject(response);          
     }

   // _allow_multiple_token_ indicates if the solution creates a new token if there is still one valid for this strategy
   // optional parameter default value: false (in that case, the existing valid token is returned)
   Boolean allowMultipleToken = JSONUtilities.decodeBoolean(jsonRoot, "allowMultipleToken", Boolean.FALSE);
   
   // _inbound_channel_ indicates which information to be returned for each presented offer
   // optional parameter keeping in mind that the method returns by default the _offerId_ and the _offerDisplay_
   String callingChannelDisplay = JSONUtilities.decodeString(jsonRoot, "inboundChannel", false);
   
   Date now = SystemTime.getCurrentTime();
   String supplier = JSONUtilities.decodeString(jsonRoot, "supplier", false);
   Supplier supplierFilter = null;

   /*****************************************
    *
    * getSupplier
    *
    *****************************************/
   if (supplier != null)
     {
       Collection<Supplier> suppliers = supplierService.getActiveSuppliers(SystemTime.getCurrentTime());
       for (Supplier supplierObject : suppliers) {
         if (supplierObject.getGUIManagedObjectDisplay().equals(supplier)) {
           supplierFilter = supplierObject;
           break;
         }
       }
     }
   
   /*****************************************
    *
    * getSubscriberProfile - no history
    *
    *****************************************/
   try
   {
     SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
     if (subscriberProfile == null)
       {
         response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
         response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
         if (log.isDebugEnabled()) log.debug("SubscriberProfile is null for subscriberID {}", subscriberID);
         return JSONUtilities.encodeObject(response);
       }

     if (presentationStrategyID != null)
       {
         if (presentationStrategyService.getActivePresentationStrategy(presentationStrategyID, now) == null)
           {
             response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericResponseCode());
             response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericResponseMessage());
             return JSONUtilities.encodeObject(response);          
           }
       }
     else if (presentationStrategyDisplay != null)
       {
         for (PresentationStrategy presentationStrategy : presentationStrategyService.getActivePresentationStrategies(now))
           {
             if (presentationStrategy.getGUIManagedObjectDisplay().equals(presentationStrategyDisplay))
               {
                 presentationStrategyID = presentationStrategy.getGUIManagedObjectID();
                 break;
               }
           }
         if (presentationStrategyID == null)
           {
             response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericResponseCode());
             response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericResponseMessage());
             return JSONUtilities.encodeObject(response);          
           }
       }
     else
       {
         response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
         response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage());
         return JSONUtilities.encodeObject(response);          
       }

     String tokenTypeID = null;
     for (TokenType tokenType : tokenTypeService.getActiveTokenTypes(now))
       {
         if (tokenType.getGUIManagedObjectDisplay().equals(tokenTypeDisplay))
           {
             tokenTypeID = tokenType.getGUIManagedObjectID();
             break;
           }
       }
     if (tokenTypeID == null)
       {
         response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.INVALID_TOKEN_TYPE.getGenericResponseCode());
         response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.INVALID_TOKEN_TYPE.getGenericResponseMessage());
         return JSONUtilities.encodeObject(response);          
       }

     CallingChannel callingChannel = null;
     if (callingChannelDisplay != null)
       {
         String callingChannelID = null;
         for (CallingChannel callingChannelLoop : callingChannelService.getActiveCallingChannels(now))
           {
             if (callingChannelLoop.getGUIManagedObjectDisplay().equals(callingChannelDisplay))
               {
                 callingChannelID = callingChannelLoop.getGUIManagedObjectID();
                 break;
               }
           }
         if (callingChannelID == null)
           {
             response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CHANNEL_NOT_FOUND.getGenericResponseCode());
             response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CHANNEL_NOT_FOUND.getGenericResponseMessage());
             return JSONUtilities.encodeObject(response);          
           }
         callingChannel = callingChannelService.getActiveCallingChannel(callingChannelID, now);
         if (callingChannel == null)
           {
             log.error(RESTAPIGenericReturnCodes.CHANNEL_NOT_FOUND.getGenericDescription()+" unknown id : "+callingChannelID);
             response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CHANNEL_NOT_FOUND.getGenericResponseCode());
             response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CHANNEL_NOT_FOUND.getGenericResponseMessage());
             return JSONUtilities.encodeObject(response);          
           }
       }

     PresentationStrategy presentationStrategy = presentationStrategyService.getActivePresentationStrategy(presentationStrategyID, now);
     if (presentationStrategy == null)
       {
         log.error(RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericDescription()+" unknown id : "+presentationStrategyID);
         response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericResponseCode());
         response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericResponseMessage());
         return JSONUtilities.encodeObject(response);          
       }

     TokenType tokenType = tokenTypeService.getActiveTokenType(tokenTypeID, now);
     if (tokenType == null)
       {
         log.error(RESTAPIGenericReturnCodes.INVALID_TOKEN_TYPE.getGenericDescription()+" unknown id : "+tokenTypeID);
         response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.INVALID_TOKEN_TYPE.getGenericResponseCode());
         response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.INVALID_TOKEN_TYPE.getGenericResponseMessage());
         return JSONUtilities.encodeObject(response);          
       }

     Token subscriberToken = null;
     if (!allowMultipleToken)
       {
         for (Token token : subscriberProfile.getTokens())
           {
             if (token != null && token instanceof DNBOToken)
               {
                 DNBOToken dnboToken = (DNBOToken) token;
                 if (presentationStrategyID.equals(dnboToken.getPresentationStrategyID()))
                   {
                     TokenStatus status = dnboToken.getTokenStatus();
                     if (status != null && (status.equals(TokenStatus.New) || status.equals(TokenStatus.Bound)) && !token.getTokenExpirationDate().before(now))
                       {
                         response = ThirdPartyJSONGenerator.generateTokenJSONForThirdParty(token, journeyService, offerService, scoringStrategyService, presentationStrategyService, offerObjectiveService, loyaltyProgramService, tokenTypeService, callingChannel, null, paymentMeanService);
                         response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
                         response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
                         return JSONUtilities.encodeObject(response);
                       }
                   }
               }
           }
         }

     DNBOToken newToken = TokenUtils.generateTokenCode(subscriberProfile, tokenType);
     if (newToken == null)
       {
         log.error(RESTAPIGenericReturnCodes.CANNOT_GENERATE_TOKEN_CODE.getGenericDescription());
         response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CANNOT_GENERATE_TOKEN_CODE.getGenericResponseCode());
         response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CANNOT_GENERATE_TOKEN_CODE.getGenericResponseMessage());
         return JSONUtilities.encodeObject(response);
       } 

     newToken.setModuleID(DeliveryRequest.Module.REST_API.getExternalRepresentation());
     String featureID = JSONUtilities.decodeString(jsonRoot, "loginName", DEFAULT_FEATURE_ID);
     newToken.setFeatureID(featureID);
     newToken.setPresentationStrategyID(presentationStrategy.getPresentationStrategyID());
     // TODO : which sales channel to use ?
     newToken.setPresentedOffersSalesChannel(presentationStrategy.getSalesChannelIDs().iterator().next());
     newToken.setCreationDate(now);

     StringBuffer returnedLog = new StringBuffer();
     double rangeValue = 0; // Not significant
     DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameters = new DNBOMatrixAlgorithmParameters(dnboMatrixService,rangeValue);
     SubscriberEvaluationRequest request = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, now);

     // Allocate offers for this subscriber, and associate them in the token
     // Here we have no saleschannel (we pass null), this means only the first salesChannelsAndPrices of the offer will be used and returned.  
     Collection<ProposedOfferDetails> presentedOffers = TokenUtils.getOffers(
         now, newToken, request,
         subscriberProfile, presentationStrategy,
         productService, productTypeService, voucherService, voucherTypeService,
         catalogCharacteristicService,
         scoringStrategyService,
         subscriberGroupEpochReader,
         segmentationDimensionService, dnboMatrixAlgorithmParameters, offerService, returnedLog, subscriberID, supplierFilter
         );
     String tokenCode = newToken.getTokenCode();
     if (presentedOffers.isEmpty())
       {
         generateTokenChange(subscriberID, now, tokenCode, TokenChange.ALLOCATE, "no offers presented", API.getCustomerTokenAndNBO, jsonRoot);
         log.error(returnedLog.toString()); // is not expected, trace errors
       }
     else
       {
         if (log.isTraceEnabled()) log.trace(returnedLog.toString()); 
         // Send a PresentationLog to EvolutionEngine

         String channelID = "channelID";
         String controlGroupState = "controlGroupState";
         String moduleID = DeliveryRequest.Module.Customer_Care.getExternalRepresentation(); 

         List<Integer> positions = new ArrayList<Integer>();
         List<Double> presentedOfferScores = new ArrayList<Double>();
         List<String> scoringStrategyIDs = new ArrayList<String>();
         int position = 0;
         ArrayList<String> presentedOfferIDs = new ArrayList<>();
         for (ProposedOfferDetails presentedOffer : presentedOffers)
           {
             presentedOfferIDs.add(presentedOffer.getOfferId());
             positions.add(new Integer(position));
             position++;
             presentedOfferScores.add(1.0);
             // scoring strategy not used anymore
             // scoringStrategyIDs.add(strategyID);
           }
         String salesChannelID = presentedOffers.iterator().next().getSalesChannelId(); // They all have the same one, set by TokenUtils.getOffers()
         int transactionDurationMs = 0; // TODO
         String callUniqueIdentifier = "";
         String userID = "0"; //TODO : fixthis

         PresentationLog presentationLog = new PresentationLog(
             subscriberID, subscriberID, now, 
             callUniqueIdentifier, channelID, salesChannelID, userID,
             newToken.getTokenCode(), 
             presentationStrategyID, transactionDurationMs, 
             presentedOfferIDs, presentedOfferScores, positions, 
             controlGroupState, scoringStrategyIDs, null, null, null,
             moduleID, featureID, newToken.getPresentationDates(), tokenTypeID, newToken
             );
         presentationLog.setToken(newToken);

         //
         //  submit to kafka
         //

         String topic = Deployment.getPresentationLogTopic();
         Serializer<StringKey> keySerializer = StringKey.serde().serializer();
         Serializer<PresentationLog> valueSerializer = PresentationLog.serde().serializer();
         kafkaProducer.send(new ProducerRecord<byte[],byte[]>(
             topic,
             keySerializer.serialize(topic, new StringKey(subscriberID)),
             valueSerializer.serialize(topic, presentationLog)
             ));
         keySerializer.close(); valueSerializer.close(); // to make Eclipse happy
         
         // Update token locally, so that it is correctly displayed in the response
         // For the real token stored in Kafka, this is done offline in EnvolutionEngine.

         newToken.setPresentedOfferIDs(presentedOfferIDs);
         newToken.setPresentedOffersSalesChannel(salesChannelID);
         newToken.setTokenStatus(TokenStatus.Bound);
         if (newToken.getCreationDate() == null)
           {
             newToken.setCreationDate(now);
           }
         newToken.setBoundDate(now);
         newToken.setBoundCount(newToken.getBoundCount()+1); // might not be accurate due to maxNumberofPlays
       }

     /*****************************************
      *
      *  decorate and response
      *
      *****************************************/
     response = ThirdPartyJSONGenerator.generateTokenJSONForThirdParty(newToken, journeyService, offerService, scoringStrategyService, presentationStrategyService, offerObjectiveService, loyaltyProgramService, tokenTypeService, callingChannel, presentedOffers, paymentMeanService);
     response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
     response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
   }
   catch (SubscriberProfileServiceException e) 
   {
     log.error("unable to process request updateCustomer {} ", e.getMessage());
     throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode()) ;
   } 
  catch (GetOfferException e) 
   {
     log.error(e.getLocalizedMessage());
     throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode()) ;
   }    

   /*****************************************
    *
    * return
    *
    *****************************************/

   return JSONUtilities.encodeObject(response);
 }




  /*****************************************
   *
   *  processGetCustomerNBOs
   *
   *****************************************/
 private JSONObject processGetCustomerNBOs(JSONObject jsonRoot) throws ThirdPartyManagerException
 {
   /****************************************
    *
    *  response
    *
    ****************************************/

   HashMap<String,Object> response = new HashMap<String,Object>();

   /****************************************
    *
    *  argument
    *
    ****************************************/

   String subscriberID = resolveSubscriberID(jsonRoot);
   
   String tokenCode = JSONUtilities.decodeString(jsonRoot, "tokenCode", false);
   Boolean viewOffersOnly = JSONUtilities.decodeBoolean(jsonRoot, "viewOffersOnly", Boolean.FALSE);
   Date now = SystemTime.getCurrentTime();
   String supplier = JSONUtilities.decodeString(jsonRoot, "supplier", false);
   Supplier supplierFilter = null;

   /*****************************************
    *
    * getSupplier
    *
    *****************************************/
   if (supplier != null)
     {
       Collection<Supplier> suppliers = supplierService.getActiveSuppliers(SystemTime.getCurrentTime());
       for (Supplier supplierObject : suppliers) {
         if (supplierObject.getGUIManagedObjectDisplay().equals(supplier)) {
           supplierFilter = supplierObject;
           break;
         }
       }
     }
   
   /*****************************************
    *
    * getSubscriberProfile - no history
    *
    *****************************************/
   try
   {
     SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
     if (subscriberProfile == null)
       {
         updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
         if (log.isDebugEnabled()) log.debug("SubscriberProfile is null for subscriberID {}", subscriberID);
         return JSONUtilities.encodeObject(response);
       }

      Token subscriberToken = null;
      if (tokenCode != null)
        {
          for (Token token : subscriberProfile.getTokens())
            {
              if (tokenCode.equals(token.getTokenCode()))
                {
                  subscriberToken = token;
                  break;
                }
            }
        }
      if (subscriberToken == null)
        {
          log.error(RESTAPIGenericReturnCodes.NO_TOKENS_RETURNED.getGenericDescription());
          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.NO_TOKENS_RETURNED.getGenericResponseCode());
          String str = RESTAPIGenericReturnCodes.NO_TOKENS_RETURNED.getGenericResponseMessage();
          response.put(GENERIC_RESPONSE_MSG, str);
          generateTokenChange(subscriberID, now, tokenCode, TokenChange.ALLOCATE, str, API.getCustomerNBOs, jsonRoot);
          return JSONUtilities.encodeObject(response);
        }
 
      if (!(subscriberToken instanceof DNBOToken))
        {
          // TODO can this really happen ?
          log.error(RESTAPIGenericReturnCodes.TOKEN_BAD_TYPE.getGenericDescription());
          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.TOKEN_BAD_TYPE.getGenericResponseCode());
          String str = RESTAPIGenericReturnCodes.TOKEN_BAD_TYPE.getGenericResponseMessage();
          response.put(GENERIC_RESPONSE_MSG, str);
          generateTokenChange(subscriberID, now, tokenCode, TokenChange.ALLOCATE, str, API.getCustomerNBOs, jsonRoot);
          return JSONUtilities.encodeObject(response);          
        }
      
      DNBOToken subscriberStoredToken = (DNBOToken) subscriberToken;
      String presentationStrategyID = subscriberStoredToken.getPresentationStrategyID();
      if (presentationStrategyID == null)
        {
          log.error(RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericDescription()+" null value");
          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericResponseCode());
          String str = RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericResponseMessage();
          response.put(GENERIC_RESPONSE_MSG, str);
          generateTokenChange(subscriberID, now, tokenCode, TokenChange.ALLOCATE, str, API.getCustomerNBOs, jsonRoot);
          return JSONUtilities.encodeObject(response);                    
        }
      PresentationStrategy presentationStrategy = (PresentationStrategy) presentationStrategyService.getStoredPresentationStrategy(presentationStrategyID);
      if (presentationStrategy == null)
        {
          log.error(RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericDescription()+" unknown id : "+presentationStrategyID);
          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericResponseCode());
          String str = RESTAPIGenericReturnCodes.INVALID_STRATEGY.getGenericResponseMessage();
          response.put(GENERIC_RESPONSE_MSG, str);
          generateTokenChange(subscriberID, now, tokenCode, TokenChange.ALLOCATE, str, API.getCustomerNBOs, jsonRoot);
          return JSONUtilities.encodeObject(response);          
        }
      
      if (!viewOffersOnly)
        {
          StringBuffer returnedLog = new StringBuffer();
          double rangeValue = 0; // Not significant
          DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameters = new DNBOMatrixAlgorithmParameters(dnboMatrixService,rangeValue);
          SubscriberEvaluationRequest request = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, now);

          // Allocate offers for this subscriber, and associate them in the token
          // Here we have no saleschannel (we pass null), this means only the first salesChannelsAndPrices of the offer will be used and returned.  
          Collection<ProposedOfferDetails> presentedOffers = TokenUtils.getOffers(
              now, subscriberStoredToken, request,
              subscriberProfile, presentationStrategy,
              productService, productTypeService,
              voucherService, voucherTypeService,
              catalogCharacteristicService,
              scoringStrategyService,
              subscriberGroupEpochReader,
              segmentationDimensionService, dnboMatrixAlgorithmParameters, offerService, returnedLog, subscriberID, supplierFilter
              );

          if (presentedOffers.isEmpty())
            {
              generateTokenChange(subscriberID, now, tokenCode, TokenChange.ALLOCATE, "no offers presented", API.getCustomerNBOs, jsonRoot);
              log.error(returnedLog.toString()); // is not expected, trace errors
            }
          else
            {
              if (log.isTraceEnabled()) log.trace(returnedLog.toString());
              // Send a PresentationLog to EvolutionEngine

              String channelID = "channelID";
              String userID = "0"; //TODO : fixthis
              String callUniqueIdentifier = "";
              String controlGroupState = "controlGroupState";
              String featureID = JSONUtilities.decodeString(jsonRoot, "loginName", DEFAULT_FEATURE_ID);
              String moduleID = DeliveryRequest.Module.REST_API.getExternalRepresentation(); 

              List<Integer> positions = new ArrayList<Integer>();
              List<Double> presentedOfferScores = new ArrayList<Double>();
              List<String> scoringStrategyIDs = new ArrayList<String>();
              int position = 0;
              ArrayList<String> presentedOfferIDs = new ArrayList<>();
              for (ProposedOfferDetails presentedOffer : presentedOffers)
                {
                  presentedOfferIDs.add(presentedOffer.getOfferId());
                  positions.add(new Integer(position));
                  position++;
                  presentedOfferScores.add(1.0);
                  // not used anymore
                  // scoringStrategyIDs.add(strategyID);
                }
               
              presentedOfferIDs = (ArrayList<String>) presentedOfferIDs.stream().filter(offerID -> (offerValidation(offerID))).collect(Collectors.toList());
              
              String salesChannelID = presentedOffers.iterator().next().getSalesChannelId(); // They all have the same one, set by TokenUtils.getOffers()
              String tokenTypeID = subscriberStoredToken.getTokenTypeID();

              int transactionDurationMs = 0; // TODO
              PresentationLog presentationLog = new PresentationLog(
                  subscriberID, subscriberID, now, 
                  callUniqueIdentifier, channelID, salesChannelID, userID,
                  tokenCode, 
                  presentationStrategyID, transactionDurationMs, 
                  presentedOfferIDs, presentedOfferScores, positions, 
                  controlGroupState, scoringStrategyIDs, null, null, null, moduleID, featureID, subscriberStoredToken.getPresentationDates(), tokenTypeID, null
                  );

             //
             //  submit to kafka
             //

             String topic = Deployment.getPresentationLogTopic();
             Serializer<StringKey> keySerializer = StringKey.serde().serializer();
             Serializer<PresentationLog> valueSerializer = PresentationLog.serde().serializer();
             kafkaProducer.send(new ProducerRecord<byte[],byte[]>(
                 topic,
                 keySerializer.serialize(topic, new StringKey(subscriberID)),
                 valueSerializer.serialize(topic, presentationLog)
                 ));
             keySerializer.close(); valueSerializer.close(); // to make Eclipse happy
             
             // Update token locally, so that it is correctly displayed in the response
             // For the real token stored in Kafka, this is done offline in EnvolutionEngine.

             subscriberStoredToken.setPresentedOfferIDs(presentedOfferIDs);
             subscriberStoredToken.setPresentedOffersSalesChannel(salesChannelID);
             subscriberStoredToken.setTokenStatus(TokenStatus.Bound);
             if (subscriberStoredToken.getCreationDate() == null)
               {
                 subscriberStoredToken.setCreationDate(now);
               }
             subscriberStoredToken.setBoundDate(now);
             subscriberStoredToken.setBoundCount(subscriberStoredToken.getBoundCount()+1); // might not be accurate due to maxNumberofPlays
           }
       }

     /*****************************************
      *
      *  decorate and response
      *
      *****************************************/
     response = ThirdPartyJSONGenerator.generateTokenJSONForThirdParty(subscriberStoredToken, journeyService, offerService, scoringStrategyService, presentationStrategyService, offerObjectiveService, loyaltyProgramService, tokenTypeService);
     response.putAll(resolveAllSubscriberIDs(subscriberProfile));
     updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
     return JSONUtilities.encodeObject(response);
   }
   catch (SubscriberProfileServiceException e) 
   {
     log.error("unable to process request updateCustomer {} ", e.getMessage());
     throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode()) ;
   } 
   catch (GetOfferException e) {
     log.error(e.getLocalizedMessage());
     throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode()) ;
   }
 }

  /*****************************************
   *
   *  processAcceptOffer
   *
   *****************************************/

  private JSONObject processAcceptOffer(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    /****************************************
     *
     *  response
     *
     ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    Date now = SystemTime.getCurrentTime();
    String resellerID = "";

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String subscriberID = resolveSubscriberID(jsonRoot);

    String tokenCode = JSONUtilities.decodeString(jsonRoot, "tokenCode", false);
    String offerID = JSONUtilities.decodeString(jsonRoot, "offerID", false);
    String origin = JSONUtilities.decodeString(jsonRoot, "origin", false);

    /*****************************************
     *
     * getSubscriberProfile - no history
     *
     *****************************************/
    String deliveryRequestID = "";
    try
    {
      SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      if (subscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
          if (log.isDebugEnabled()) log.debug("SubscriberProfile is null for subscriberID {}", subscriberID);
          return JSONUtilities.encodeObject(response);
        }

      Token subscriberToken = null;
      List<Token> tokens = subscriberProfile.getTokens();
      for (Token token : tokens)
        {
          if (token.getTokenCode().equals(tokenCode))
            {
              subscriberToken = token;
              break;
            }
        }
      if (subscriberToken == null)
        {
          // Token has not been assigned to this subscriber
          log.error(RESTAPIGenericReturnCodes.NO_TOKENS_RETURNED.getGenericDescription());
          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.NO_TOKENS_RETURNED.getGenericResponseCode());
          String str = RESTAPIGenericReturnCodes.NO_TOKENS_RETURNED.getGenericResponseMessage();
          response.put(GENERIC_RESPONSE_MSG, str);
          generateTokenChange(subscriberID, now, tokenCode, TokenChange.REDEEM, str, API.acceptOffer, jsonRoot);
          return JSONUtilities.encodeObject(response);
        }
      
      if (!(subscriberToken instanceof DNBOToken))
        {
          // TODO can this happen ?
          log.error(RESTAPIGenericReturnCodes.TOKEN_BAD_TYPE.getGenericDescription());
          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.TOKEN_BAD_TYPE.getGenericResponseCode());
          String str = RESTAPIGenericReturnCodes.TOKEN_BAD_TYPE.getGenericResponseMessage();
          response.put(GENERIC_RESPONSE_MSG, str);
          generateTokenChange(subscriberID, now, tokenCode, TokenChange.REDEEM, str, API.acceptOffer, jsonRoot);
          return JSONUtilities.encodeObject(response);          
        }
      
      DNBOToken subscriberStoredToken = (DNBOToken) subscriberToken;
      
      if (subscriberStoredToken.getTokenStatus() == TokenStatus.Redeemed)
        {
          log.error(RESTAPIGenericReturnCodes.CONCURRENT_ACCEPT.getGenericDescription());
          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CONCURRENT_ACCEPT.getGenericResponseCode());
          String str = RESTAPIGenericReturnCodes.CONCURRENT_ACCEPT.getGenericResponseMessage();
          response.put(GENERIC_RESPONSE_MSG, str);
          generateTokenChange(subscriberID, now, tokenCode, TokenChange.REDEEM, str, API.acceptOffer, jsonRoot);
          return JSONUtilities.encodeObject(response);
        }

      if (subscriberStoredToken.getTokenStatus() != TokenStatus.Bound)
        {
          log.error(RESTAPIGenericReturnCodes.NO_OFFER_ALLOCATED.getGenericDescription());
          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.NO_OFFER_ALLOCATED.getGenericResponseCode());
          String str = RESTAPIGenericReturnCodes.NO_OFFER_ALLOCATED.getGenericResponseMessage();
          response.put(GENERIC_RESPONSE_MSG, str);
          generateTokenChange(subscriberID, now, tokenCode, TokenChange.REDEEM, str, API.acceptOffer, jsonRoot);
          return JSONUtilities.encodeObject(response);
        }

      // Check that offer has been presented to customer
      
      List<String> offers = subscriberStoredToken.getPresentedOfferIDs();
      int position = 0;
      boolean found = false;
      for (String offID : offers)
      {
        if (offID.equals(offerID))
          {
            found = true;
            break;
          }
        position++;
      }
      if (!found)
        {
          log.error(RESTAPIGenericReturnCodes.OFFER_NOT_PRESENTED.getGenericDescription());
          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.OFFER_NOT_PRESENTED.getGenericResponseCode());
          String str = RESTAPIGenericReturnCodes.OFFER_NOT_PRESENTED.getGenericResponseMessage();
          response.put(GENERIC_RESPONSE_MSG, str);
          generateTokenChange(subscriberID, now, tokenCode, TokenChange.REDEEM, str, API.acceptOffer, jsonRoot);
          return JSONUtilities.encodeObject(response);          
        }
      String salesChannelID = subscriberStoredToken.getPresentedOffersSalesChannel();
      String featureID = JSONUtilities.decodeString(jsonRoot, "loginName", DEFAULT_FEATURE_ID);
      String moduleID = DeliveryRequest.Module.REST_API.getExternalRepresentation(); 
      Offer offer = offerService.getActiveOffer(offerID, now);
      if (offer != null)
        {
          Boolean validOffer = offerValidation(offer.getOfferID());
          if (!(validOffer))
            {
              response.put(GENERIC_RESPONSE_CODE,
                  RESTAPIGenericReturnCodes.PRODUCT_PARENT_SUPPLIER_INACTIVE.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG,
                  RESTAPIGenericReturnCodes.PRODUCT_PARENT_SUPPLIER_INACTIVE.getGenericResponseMessage());
              return JSONUtilities.encodeObject(response);
            }
        } 
      
       
      deliveryRequestID = purchaseOffer(subscriberProfile,false, subscriberID, offerID, salesChannelID, 1, moduleID, featureID, origin, resellerID, kafkaProducer).getDeliveryRequestID();

      
      // Redeem the token : Send an AcceptanceLog to EvolutionEngine

      String msisdn = subscriberID; // TODO check this
      String presentationStrategyID = subscriberStoredToken.getPresentationStrategyID();

      // TODO BEGIN Following fields are currently not used in EvolutionEngine, might need to be set later
      String callUniqueIdentifier = "";
      String controlGroupState = "controlGroupState";
      String channelID = "channelID";
      Integer actionCall = 1;
      int transactionDurationMs = 0;
      // TODO END
      
      Date fulfilledDate = now;
      String userID = JSONUtilities.decodeString(jsonRoot, "loginName", true);
      String tokenTypeID = subscriberStoredToken.getTokenTypeID();
      
      AcceptanceLog acceptanceLog = new AcceptanceLog(
          msisdn, subscriberID, now, 
          callUniqueIdentifier, channelID, salesChannelID,
          userID, tokenCode,
          presentationStrategyID, transactionDurationMs,
          controlGroupState, offerID, fulfilledDate, position, actionCall, moduleID, featureID, tokenTypeID);
      
      //
      //  submit to kafka
      //

      {
      String topic = Deployment.getAcceptanceLogTopic();
      Serializer<StringKey> keySerializer = StringKey.serde().serializer();
      Serializer<AcceptanceLog> valueSerializer = AcceptanceLog.serde().serializer();
      kafkaProducer.send(new ProducerRecord<byte[],byte[]>(
          topic,
          keySerializer.serialize(topic, new StringKey(subscriberID)),
          valueSerializer.serialize(topic, acceptanceLog)
          ));
      keySerializer.close(); valueSerializer.close(); // to make Eclipse happy
      }
      
      //
      // trigger event (for campaigns)
      //

      {
        TokenRedeemed tokenRedeemed = new TokenRedeemed(subscriberID, now, subscriberStoredToken.getTokenTypeID(), offerID);
        String topic = Deployment.getTokenRedeemedTopic();
        Serializer<StringKey> keySerializer = StringKey.serde().serializer();
        Serializer<TokenRedeemed> valueSerializer = TokenRedeemed.serde().serializer();
        kafkaProducer.send(new ProducerRecord<byte[],byte[]>(
            topic,
            keySerializer.serialize(topic, new StringKey(subscriberID)),
            valueSerializer.serialize(topic, tokenRedeemed)
            ));
        keySerializer.close(); valueSerializer.close(); // to make Eclipse happy
      }      
    }
    catch (SubscriberProfileServiceException e) 
    {
      log.error("unable to process request acceptOffer {} ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    } 

    /*****************************************
     *
     *  decorate and response
     *
     *****************************************/
    response.put("deliveryRequestID", deliveryRequestID);
    updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processPurchaseOffer
   * @throws ParseException 
   * @throws IOException 
   *
   *****************************************/

  private JSONObject processPurchaseOffer(JSONObject jsonRoot, boolean sync) throws ThirdPartyManagerException, IOException, ParseException
  {
    /****************************************
     *
     *  response
     *
     ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String subscriberID = resolveSubscriberID(jsonRoot);
    
    String offerID = JSONUtilities.decodeString(jsonRoot, "offerID", false);
    String offerDisplay = JSONUtilities.decodeString(jsonRoot, "offer", false);
    String salesChannel = JSONUtilities.decodeString(jsonRoot, "salesChannel", false);
    Date now = SystemTime.getCurrentTime();
    if (salesChannel == null) // this is mandatory, but we want to control the return code
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage());
        return JSONUtilities.encodeObject(response);          
      }

    Integer quantity = JSONUtilities.decodeInteger(jsonRoot, "quantity", true);
    if (quantity == null) // this is mandatory, but we want to control the return code
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage());
        return JSONUtilities.encodeObject(response);          
      }
    
    String origin = JSONUtilities.decodeString(jsonRoot, "origin", false);
    AuthenticatedResponse authResponse = null;
    ThirdPartyCredential thirdPartyCredential = new ThirdPartyCredential(jsonRoot);
    if (!Deployment.getRegressionMode())
      {
        authResponse = authCache.get(thirdPartyCredential);
      }
    else
      {
        authResponse = authenticate(thirdPartyCredential);
      } 
    int user = (authResponse.getUserId());
    String userID = Integer.toString(user); 
    Map<String, List<String>> activeResellerAndSalesChannelIDs = activeResellerAndSalesChannelIDs(userID);
    String resellerID = "";
    String parentResellerID = "";
    
    if ((activeResellerAndSalesChannelIDs.containsKey("activeReseller")) && (activeResellerAndSalesChannelIDs.get("activeReseller")).size() != 0) {
      resellerID = (activeResellerAndSalesChannelIDs.get("activeReseller")).get(0);
    }

    /*****************************************
     *
     * getSubscriberProfile - no history
     *
     *****************************************/
    PurchaseFulfillmentRequest purchaseResponse=null;
    try
    {
      SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
    
      if ((activeResellerAndSalesChannelIDs.containsKey("activeReseller")) && (activeResellerAndSalesChannelIDs.get("activeReseller")).size() == 0) {
        updateResponse(response, RESTAPIGenericReturnCodes.INACTIVE_RESELLER);
        return JSONUtilities.encodeObject(response);
      }
      if ((activeResellerAndSalesChannelIDs.containsKey("salesChannelIDsList")) && (activeResellerAndSalesChannelIDs.get("salesChannelIDsList")).size() == 0) {
        updateResponse(response, RESTAPIGenericReturnCodes.RESELLER_WITHOUT_SALESCHANNEL);
        return JSONUtilities.encodeObject(response);
      }
      if (subscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
          if (log.isDebugEnabled()) log.debug("SubscriberProfile is null for subscriberID {}", subscriberID);
          return JSONUtilities.encodeObject(response);
        }
      
      if (offerID != null)
        {
          if (offerService.getActiveOffer(offerID, now) == null)
            {
              updateResponse(response, RESTAPIGenericReturnCodes.OFFER_NOT_FOUND);
              return JSONUtilities.encodeObject(response);          
            }
        }
      else if (offerDisplay != null)
        {
          for (Offer offer : offerService.getActiveOffers(now))
            {
              if (offerDisplay.equals(offer.getDisplay()))
                {
                  offerID = offer.getGUIManagedObjectID();
                  break;
                }
            }
          if (offerID == null)
            {
              updateResponse(response, RESTAPIGenericReturnCodes.OFFER_NOT_FOUND);
              return JSONUtilities.encodeObject(response);          
            }
        }
      else
        {
          updateResponse(response, RESTAPIGenericReturnCodes.MISSING_PARAMETERS);
          return JSONUtilities.encodeObject(response);          
        }
      
        if (offerID != null)
          {
            Boolean validOffer = offerValidation(offerID);
            if (!(validOffer))
              {
                response.put(GENERIC_RESPONSE_CODE,
                    RESTAPIGenericReturnCodes.PRODUCT_PARENT_SUPPLIER_INACTIVE.getGenericResponseCode());
                response.put(GENERIC_RESPONSE_MSG,
                    RESTAPIGenericReturnCodes.PRODUCT_PARENT_SUPPLIER_INACTIVE.getGenericResponseMessage());
                return JSONUtilities.encodeObject(response);
              }
          }

      String salesChannelID = null;
      for (SalesChannel sc : salesChannelService.getActiveSalesChannels(now))
        {
          if (salesChannel.equals(sc.getGUIManagedObjectDisplay()))
            {
              salesChannelID = sc.getSalesChannelID();
              break;
            }
        }
      if (salesChannelID == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CHANNEL_DEACTIVATED);
          return JSONUtilities.encodeObject(response);          
        }
      
      String featureID = JSONUtilities.decodeString(jsonRoot, "loginName", DEFAULT_FEATURE_ID);
      String moduleID = DeliveryRequest.Module.REST_API.getExternalRepresentation();
      if (activeResellerAndSalesChannelIDs.containsKey("salesChannelIDsList") && (activeResellerAndSalesChannelIDs.get("salesChannelIDsList")).size() != 0)
        {
          List salesChannelIDList = activeResellerAndSalesChannelIDs.get("salesChannelIDsList");
          if (salesChannelIDList.contains(salesChannelID))
            {
              if(!sync)
                {
                  purchaseResponse = purchaseOffer(subscriberProfile,false,subscriberID, offerID, salesChannelID, quantity, moduleID, featureID,
                  origin, resellerID, kafkaProducer);
                }
              else
                {
                  purchaseResponse = purchaseOffer(subscriberProfile,true,subscriberID, offerID, salesChannelID, quantity, moduleID, featureID,
                          origin, resellerID, kafkaProducer);
                  response.put("offer",purchaseResponse.getThirdPartyPresentationMap(subscriberMessageTemplateService,salesChannelService,journeyService,offerService,loyaltyProgramService,productService,voucherService,deliverableService,paymentMeanService, resellerService));
                }
            }
          else
            {            
              updateResponse(response, RESTAPIGenericReturnCodes.SALESCHANNEL_RESELLER_MISMATCH);
              return JSONUtilities.encodeObject(response);
            }
        }
        else
          {

            if(!sync)
            {
              purchaseResponse = purchaseOffer(subscriberProfile,false,subscriberID, offerID, salesChannelID, quantity, moduleID, featureID,
                      origin, resellerID, kafkaProducer);
            }
            else
            {
              purchaseResponse = purchaseOffer(subscriberProfile,true,subscriberID, offerID, salesChannelID, quantity, moduleID, featureID,
                      origin, resellerID, kafkaProducer);
              response.put("offer",purchaseResponse.getThirdPartyPresentationMap(subscriberMessageTemplateService,salesChannelService,journeyService,offerService,loyaltyProgramService,productService,voucherService,deliverableService,paymentMeanService, resellerService));
            }
          }
      
      //
      // TODO how do we deal with the offline errors ? 
      //
      
      // TODO trigger event (for campaign) ?
      
    }
    catch (SubscriberProfileServiceException e) 
    {
      log.error("unable to process request purchaseOffer {} ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    } 

    /*****************************************
     *
     *  decorate and response
     *
     *****************************************/
    response.put("deliveryRequestID", purchaseResponse.getDeliveryRequestID());
    if(purchaseResponse==null){
      updateResponse(response, RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }else if(sync){
      response.put(GENERIC_RESPONSE_CODE, purchaseResponse.getStatus().getReturnCode());
      response.put(GENERIC_RESPONSE_MSG, purchaseResponse.getStatus().name());
    }else{
      updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
    }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processLoyaltyProgramOptInOut
   *
   *****************************************/

  private JSONObject processLoyaltyProgramOptInOut(JSONObject jsonRoot, boolean optIn) throws ThirdPartyManagerException
  {
    /****************************************
     *
     *  response
     *
     ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String subscriberID = resolveSubscriberID(jsonRoot);
    
    String loyaltyProgramName = JSONUtilities.decodeString(jsonRoot, "loyaltyProgram", false);
    if (loyaltyProgramName == null) // this is mandatory, but we want to control the return code
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.LOYALTY_PROJECT_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.LOYALTY_PROJECT_NOT_FOUND.getGenericResponseMessage());
        return JSONUtilities.encodeObject(response);          
      }
    String loyaltyProgramRequestID = "";

    /*****************************************
     *
     * getSubscriberProfile - no history
     *
     *****************************************/

    try
    {
      SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      if (subscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
          if (log.isDebugEnabled()) log.debug("SubscriberProfile is null for subscriberID {}", subscriberID);
          return JSONUtilities.encodeObject(response);
        }

      Date now = SystemTime.getCurrentTime();

      String loyaltyProgramID = null;
      for (LoyaltyProgram loyaltyProgram : loyaltyProgramService.getActiveLoyaltyPrograms(now))
        {
          if (loyaltyProgramName.equals(loyaltyProgram.getLoyaltyProgramDisplay()))
            {
              loyaltyProgramID = loyaltyProgram.getGUIManagedObjectID();
              break;
            }
        }
      if (loyaltyProgramID == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.LOYALTY_PROJECT_NOT_FOUND);
          return JSONUtilities.encodeObject(response);          
        }

      Serializer<StringKey> keySerializer = StringKey.serde().serializer();
      Serializer<LoyaltyProgramRequest> valueSerializer = LoyaltyProgramRequest.serde().serializer();
      
      String featureID = JSONUtilities.decodeString(jsonRoot, "loginName", DEFAULT_FEATURE_ID);
      String operation = optIn ? "opt-in" : "opt-out";
      String moduleID = DeliveryRequest.Module.REST_API.getExternalRepresentation();
      loyaltyProgramRequestID = zuks.getStringKey();

      /*****************************************
      *
      *  request
      *
      *****************************************/
            
      // Build a json doc to create the LoyaltyProgramRequest
      HashMap<String,Object> request = new HashMap<String,Object>();
      
      // Fields for LoyaltyProgramRequest
      request.put("operation", operation);
      request.put("loyaltyProgramRequestID", loyaltyProgramRequestID);
      request.put("loyaltyProgramID", loyaltyProgramID);
      request.put("eventDate", now);
      
      // Fields for DeliveryRequest
      request.put("deliveryRequestID", loyaltyProgramRequestID);
      request.put("subscriberID", subscriberID);
      request.put("eventID", "0"); // No event here
      request.put("moduleID", moduleID);
      request.put("featureID", featureID);
      request.put("deliveryType", "loyaltyProgramFulfillment");
      
      JSONObject valueRes = JSONUtilities.encodeObject(request);

      LoyaltyProgramRequest loyaltyProgramRequest = new LoyaltyProgramRequest(subscriberProfile,subscriberGroupEpochReader,valueRes, null);
      loyaltyProgramRequest.forceDeliveryPriority(DELIVERY_REQUEST_PRIORITY);
      String topic = Deployment.getDeliveryManagers().get(loyaltyProgramRequest.getDeliveryType()).getRequestTopic(loyaltyProgramRequest.getDeliveryPriority());

      // Write it to the right topic
      kafkaProducer.send(new ProducerRecord<byte[],byte[]>(
          topic,
          keySerializer.serialize(topic, new StringKey(subscriberID)),
          valueSerializer.serialize(topic, loyaltyProgramRequest)
          ));
      keySerializer.close(); valueSerializer.close(); // to make Eclipse happy
      
      //
      // TODO how do we deal with the offline errors ? 
      //
      
      // TODO trigger event (for campaign) ?
      
    }
    catch (SubscriberProfileServiceException e) 
    {
      log.error("unable to process request processLoyaltyProgramOptInOut {} ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    } 

    /*****************************************
     *
     *  decorate and response
     *
     *****************************************/
    response.put("deliveryRequestID", loyaltyProgramRequestID);
    updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processGetTokensCodesList
   *
   *****************************************/

  private JSONObject processGetTokensCodesList(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    /****************************************
     *
     *  response
     *
     ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  arguments
     *
     ****************************************/

    String subscriberID = resolveSubscriberID(jsonRoot);

    String tokenStatus = JSONUtilities.decodeString(jsonRoot, "tokenStatus", false);

    if (tokenStatus != null)
      {
        boolean found = false;
        for (TokenStatus enumeratedValue : TokenStatus.values())
          {
            if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(tokenStatus))
              {
                found = true;
                break; 
              }
          }
        if (!found)
          {
            log.error(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericDescription());
            updateResponse(response, RESTAPIGenericReturnCodes.BAD_FIELD_VALUE);
            return JSONUtilities.encodeObject(response);
          }
      }
    String tokenStatusForStreams = tokenStatus; // We need a 'final-like' variable to process streams later
    boolean hasFilter = (tokenStatusForStreams != null);

    /*****************************************
     *
     *  getSubscriberProfile
     *
     *****************************************/

    try
    {
      SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      if (baseSubscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
          return JSONUtilities.encodeObject(response);
        }
      List<JSONObject> tokensJson;
      List<Token> tokens = baseSubscriberProfile.getTokens();
      if (tokens == null)
        {
          tokensJson = new ArrayList<>();
        }
      else 
        {
          Stream<Token> tokenStream = tokens.stream();
          if (hasFilter)
            {
              if (log.isTraceEnabled()) log.trace("Filter provided : "+tokenStatus);
              tokenStream = tokenStream.filter(token -> tokenStatusForStreams.equalsIgnoreCase(token.getTokenStatus().getExternalRepresentation()));
            }
          tokensJson = tokenStream
              .map(token -> ThirdPartyJSONGenerator.generateTokenJSONForThirdParty(token, journeyService, offerService, scoringStrategyService, presentationStrategyService, offerObjectiveService, loyaltyProgramService, tokenTypeService))
              .collect(Collectors.toList());
        }

      /*****************************************
       *
       *  decorate and response
       *
       *****************************************/

      response.put("tokens", JSONUtilities.encodeArray(tokensJson));
      updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
    }
    catch (SubscriberProfileServiceException e)
    {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }

    /*****************************************
     *
     * return
     *
     *****************************************/

    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processTriggerEvent
  *
  *****************************************/

  private JSONObject processTriggerEvent(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
    *
    * argument
    *
    ****************************************/
    //
    //  eventName
    //

    String eventName = JSONUtilities.decodeString(jsonRoot, "eventName", false);
    if (eventName == null || eventName.isEmpty())
      {
        updateResponse(response, RESTAPIGenericReturnCodes.MISSING_PARAMETERS, "-{eventName is missing}");
        return JSONUtilities.encodeObject(response);
      }
    
    EvolutionEngineEventDeclaration eventDeclaration = Deployment.getEvolutionEngineEvents().get(eventName);
    AutoProvisionEvent autoProvisionEvent = com.evolving.nglm.core.Deployment.getAutoProvisionEvents().get(eventName);

    //
    // subscriberParameter contains by example customerID,<internalSubscriberID> or msisdn,<msisdn> coming from the request
    //

    Pair<String, String> subscriberParameter = resolveSubscriberAlternateID(jsonRoot);
    
    //
    //  eventBody
    //
    
    JSONObject eventBody = JSONUtilities.decodeJSONObject(jsonRoot, "eventBody");
    if (eventBody == null)
      {
        updateResponse(response, RESTAPIGenericReturnCodes.MISSING_PARAMETERS, "-{eventBody is missing}");
        return JSONUtilities.encodeObject(response);
      }
    eventBody.put(subscriberParameter.getFirstElement(), subscriberParameter.getSecondElement());

    //
    //  subscriberID
    //
    String subscriberID = null;
    try 
    {
      subscriberID = resolveSubscriberID(jsonRoot);
    }
    catch(ThirdPartyManagerException e)
    {
      // throw again the exception if the subscriberID is null and not an Autoprovision event
      if(autoProvisionEvent == null) {
        throw e;
      }
      else {
        subscriberID = null;
      }
    }
    
    if (eventDeclaration == null || eventDeclaration.getEventRule() == EvolutionEngineEventDeclaration.EventRule.Internal)
      {
        updateResponse(response, RESTAPIGenericReturnCodes.EVENT_NAME_UNKNOWN, "-{" + eventName + "}");
        return JSONUtilities.encodeObject(response);
      }
    else
      {
        Constructor<? extends SubscriberStreamOutput> constructor = JSON3rdPartyEventsConstructor.get(eventName);
        Class<? extends SubscriberStreamOutput> eventClass;
        if (constructor == null)
          {
            try
              {
                eventClass = (Class<? extends SubscriberStreamOutput>) Class.forName(eventDeclaration.getEventClassName());
                constructor = eventClass.getConstructor(new Class<?>[]{String.class, Date.class, JSONObject.class });
                JSON3rdPartyEventsConstructor.put(eventName, constructor);
              }
            catch (Exception e)
              {
                updateResponse(response, RESTAPIGenericReturnCodes.BAD_3RD_PARTY_EVENT_CLASS_DEFINITION, "-{" + eventDeclaration.getEventClassName() + "(1) Exception " + e.getClass().getName() + "}");
                return JSONUtilities.encodeObject(response);
              }
          }
        SubscriberStreamOutput eev = null;
        try
          {
            // 3 cases to create the Event object:
            // 1. subscriberID != null, it is given to the constructor,
            // 2. subscriberID == null and event is an autoprovision event, so in this case, we provide in the constructor, the alternateID coming from the REST request
            if(subscriberID != null)
              {
                eev = constructor.newInstance(new Object[]{subscriberID, SystemTime.getCurrentTime(), eventBody });
                eev.forceDeliveryPriority(DELIVERY_REQUEST_PRIORITY);
              }
            else if(autoProvisionEvent != null)
              {
                // means this is an autoprovision event, case 2...
                eev = constructor.newInstance(new Object[]{subscriberParameter.getSecondElement(), SystemTime.getCurrentTime(), eventBody });
                eev.forceDeliveryPriority(DELIVERY_REQUEST_PRIORITY);              
              }
          }
        catch (Exception e)
          {
            updateResponse(response, RESTAPIGenericReturnCodes.BAD_3RD_PARTY_EVENT_CLASS_DEFINITION, "-{" + eventDeclaration.getEventClassName() + "(2) Exception " + e.getClass().getName() + "}");
            return JSONUtilities.encodeObject(response);
          }

        // 2 cases to choose the topic
        // 1. subscriberID != null, normal event topic is used,
        // 2. subscriberID == null and event is an autoprovision event, autoprovision topic is used
        String topic = eventDeclaration.getEventTopic();
        if(subscriberID == null) { topic = autoProvisionEvent.getAutoProvisionTopic(); }
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(topic, StringKey.serde().serializer().serialize(topic, new StringKey(subscriberID != null ? subscriberID : subscriberParameter.getSecondElement())), eventDeclaration.getEventSerde().serializer().serialize(topic, (EvolutionEngineEvent)eev)));
        updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS, "{event triggered}");
      }

    /*****************************************
    *
    * return
    *
    *****************************************/

    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processEnterCampaign
   * @throws ThirdPartyManagerException 
  *
  *****************************************/
  
  private JSONObject processEnterCampaign(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    String responseCode = null;

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String subscriberID = resolveSubscriberID(jsonRoot);
    
    /*****************************************
    *
    *  getSubscriberProfile
    *
    *****************************************/

    SubscriberProfile baseSubscriberProfile = null;
    try
    {
      baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true, false);
      if (baseSubscriberProfile == null)
        {
          updateResponse(response, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
          return JSONUtilities.encodeObject(response);
        }
    }
    catch (SubscriberProfileServiceException e)
    {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }
    
    Journey journey = null;
    String campaignDisplay = JSONUtilities.decodeString(jsonRoot, "campaign", false);
    if (campaignDisplay == null)
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " (campaign)");
        return JSONUtilities.encodeObject(response);
      }
    Collection<Journey> allActiveJourneys = journeyService.getActiveJourneys(SystemTime.getCurrentTime());
    if(allActiveJourneys != null)
      {
        for(Journey activeJourney : allActiveJourneys)
          {
            if(activeJourney.getGUIManagedObjectDisplay().equals(campaignDisplay))
              {
                if(activeJourney.getTargetingType().equals(TargetingType.Manual))
                  {
                    journey = activeJourney;
                    responseCode = null;
                    break;
                  }
                else
                  {
                    responseCode = "Campaign is not manual targeting";
                  }
              }
            else
              {
                responseCode = "Campaign not found";
              }
          }
      }

    if(journey != null)
      {
        String uniqueKey = UUID.randomUUID().toString();
        JourneyRequest journeyRequest = new JourneyRequest(baseSubscriberProfile, subscriberGroupEpochReader, uniqueKey, subscriberID, journey.getJourneyID(), baseSubscriberProfile.getUniversalControlGroup());
        journeyRequest.forceDeliveryPriority(DELIVERY_REQUEST_PRIORITY);
        DeliveryManagerDeclaration journeyManagerDeclaration = Deployment.getDeliveryManagers().get(journeyRequest.getDeliveryType());
        String journeyRequestTopic = journeyManagerDeclaration.getRequestTopic(journeyRequest.getDeliveryPriority());
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(journeyRequestTopic, StringKey.serde().serializer().serialize(journeyRequestTopic, new StringKey(journeyRequest.getSubscriberID())), ((ConnectSerde<DeliveryRequest>)journeyManagerDeclaration.getRequestSerde()).serializer().serialize(journeyRequestTopic, journeyRequest)));
        responseCode = "ok";
      }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  private JSONObject processValidateVoucher(JSONObject jsonRoot) throws ThirdPartyManagerException {

    VoucherProfileStored voucherProfileStored = getStoredVoucher(jsonRoot).getSecondElement();

    Map<String,Object> response = new HashMap<String,Object>();
    response.put("code",voucherProfileStored.getVoucherCode());
    response.put("deliveryDate",getDateString(voucherProfileStored.getVoucherDeliveryDate()));
    response.put("expiryDate",getDateString(voucherProfileStored.getVoucherExpiryDate()));
    response.put("status",voucherProfileStored.getVoucherStatus().getExternalRepresentation());
    String offerID = voucherProfileStored.getOfferID();
    JSONObject offerJSON = new JSONObject();
    GUIManagedObject offerObject = offerService.getStoredOffer(offerID);
    if (offerObject instanceof Offer) {
      Offer offer = (Offer) offerObject;
      offerJSON = ThirdPartyJSONGenerator.generateOfferJSONForThirdParty(offer, offerService, offerObjectiveService, productService, voucherService, salesChannelService);
    }
    response.put("offerDetails",offerJSON);
    return constructThirdPartyResponse(RESTAPIGenericReturnCodes.SUCCESS,response);

  }

  private JSONObject processRedeemVoucher(JSONObject jsonRoot,boolean sync) throws ThirdPartyManagerException {

    String origin = readString(jsonRoot, "origin", true);
    Pair<String,VoucherProfileStored> voucherProfileStoredCheck = getStoredVoucher(jsonRoot);
    String subscriberID = voucherProfileStoredCheck.getFirstElement();
    VoucherProfileStored voucherProfileStored = voucherProfileStoredCheck.getSecondElement();

    String offerID = voucherProfileStored.getOfferID();
    JSONObject offerJSON = new JSONObject();
    GUIManagedObject offerObject = offerService.getStoredOffer(offerID);
    if (offerObject instanceof Offer) {
      Offer offer = (Offer) offerObject;
      offerJSON = ThirdPartyJSONGenerator.generateOfferJSONForThirdParty(offer, offerService, offerObjectiveService, productService, voucherService, salesChannelService);
    }
    Map<String,Object> offerResponse = new HashMap<>();
    offerResponse.put("offerDetails", offerJSON);
    
    //build the request to send
    VoucherChange request = new VoucherChange(
            subscriberID,
            SystemTime.getCurrentTime(),
            null,
            zuksVoucherChange.getStringKey(),
            VoucherChange.VoucherChangeAction.Redeem,
            voucherProfileStored.getVoucherCode(),
            voucherProfileStored.getVoucherID(),
            voucherProfileStored.getFileID(),
            voucherProfileStored.getModuleID(),
            voucherProfileStored.getFeatureID(),
            origin,
            RESTAPIGenericReturnCodes.UNKNOWN);

    Future<VoucherChange> waitingResponse=null;
    if(sync){
      waitingResponse = voucherChangeResponseListenerService.addWithOnValueFilter((value)->value.getEventID().equals(request.getEventID())&&value.getReturnStatus()!=RESTAPIGenericReturnCodes.UNKNOWN);
    }

    String requestTopic = Deployment.getVoucherChangeRequestTopic();
    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(
            requestTopic,
            StringKey.serde().serializer().serialize(requestTopic, new StringKey(subscriberID)),
            VoucherChange.serde().serializer().serialize(requestTopic, request)
    ));

    if(sync){
      VoucherChange response = handleWaitingResponse(waitingResponse);
      return constructThirdPartyResponse(response.getReturnStatus(),offerResponse);
    }
    return constructThirdPartyResponse(RESTAPIGenericReturnCodes.SUCCESS,offerResponse);
  }

  /*****************************************
   *
   * processGetResellerDetails
   * @throws ParseException 
   * @throws IOException 
   *
   *****************************************/

  private JSONObject processGetResellerDetails(JSONObject jsonRoot) throws ThirdPartyManagerException, IOException, ParseException
  {

    /****************************************
    *
    * response
    *
    ****************************************/

   Map<String, Object> response = new HashMap<String, Object>();
   boolean userIsReseller = false;
   AuthenticatedResponse authResponse = null;
   Reseller userReseller = null;    
   ThirdPartyCredential thirdPartyCredential = new ThirdPartyCredential(jsonRoot);
   JSONArray resellerJSONArray = new JSONArray();
   if (!Deployment.getRegressionMode())
     {
       authResponse = authCache.get(thirdPartyCredential);
     }
   else
     {
       authResponse = authenticate(thirdPartyCredential);
     }
   int userID = (authResponse.getUserId());
   String user = Integer.toString(userID);
   resellerloop:
   for (GUIManagedObject reseller : resellerService.getStoredResellers())
     {
       if (reseller instanceof Reseller)
         {
           List<String> userIDs = ((Reseller) reseller).getUserIDs();
           for (String userId : userIDs)
             {
               if (user.equals(userId))
                 {
                   userIsReseller = true;
                   userReseller = (Reseller) reseller;
                   break resellerloop;
                 }
             }
         }
     }
   if (userIsReseller == true)
     {

       JSONObject resellerJson = ThirdPartyJSONGenerator.generateResellerJSONForThirdParty((Reseller) userReseller,
           resellerService);
       response.put("resellerDetails", JSONUtilities.encodeObject(resellerJson));
       updateResponse(response, RESTAPIGenericReturnCodes.SUCCESS);
     }
   else
     {
       updateResponse(response, RESTAPIGenericReturnCodes.PARTNER_NOT_FOUND);
       return JSONUtilities.encodeObject(response);
     }

   return JSONUtilities.encodeObject(response);
  }
 
  private Pair<String,VoucherProfileStored> getStoredVoucher(JSONObject jsonRoot) throws ThirdPartyManagerException {

    Date now = SystemTime.getCurrentTime();

    String voucherCode = readString(jsonRoot, "voucherCode", true);
    String supplierDisplay = readString(jsonRoot, "supplier", true);

    Supplier supplier=null;
    for(Supplier supplierConf:supplierService.getActiveSuppliers(now)){
      if(supplierConf.getGUIManagedObjectDisplay().equals(supplierDisplay)){
        supplier=supplierConf;
        break;
      }
    }
    if(supplier==null){
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.PARTNER_NOT_FOUND);
    }

    String subscriberID=null;
    try
      {
        subscriberID = resolveSubscriberID(jsonRoot);
      }
    catch (ThirdPartyManagerException e)
      {
        if (e.getResponseCode() != RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode())
          {
            throw e;
          }
      }

    // OK if "not transferable"
    if(subscriberID == null){
      VoucherPersonalES voucherES = VoucherPersonalESService.getESVoucherFromVoucherCode(supplier.getSupplierID(),voucherCode,elasticsearch);
      if(voucherES==null) throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.VOUCHER_CODE_NOT_FOUND);
      subscriberID=voucherES.getSubscriberId();
    }
    
    SubscriberProfile subscriberProfile=null;
    try{
      subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
    } catch (SubscriberProfileServiceException e) {
      log.error("SubscriberProfileServiceException ", e.getMessage());
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }

    if (subscriberProfile == null) {
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
    }

    ThirdPartyManagerException errorException = null;
    VoucherProfileStored voucherStored = null;
    for(VoucherProfileStored profileVoucher:subscriberProfile.getVouchers()){
      Voucher voucher = voucherService.getActiveVoucher(profileVoucher.getVoucherID(),now);
      //a voucher in subscriber profile with no more voucher conf associated, very likely to happen
      if(voucher==null){
        if(errorException==null) errorException = new ThirdPartyManagerException(RESTAPIGenericReturnCodes.VOUCHER_CODE_NOT_FOUND);
        continue;
      }
      if(voucherCode.equals(profileVoucher.getVoucherCode()) && supplier.getSupplierID().equals(voucher.getSupplierID())){

            if (profileVoucher.getVoucherStatus() == VoucherDelivery.VoucherStatus.Redeemed)
              {
                Date redeemedDateToBeFormatted = profileVoucher.getVoucherRedeemDate();
                SimpleDateFormat dateFormat = new SimpleDateFormat(Deployment.getAPIresponseDateFormat());
                dateFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
                String redeemedDate = dateFormat.format(redeemedDateToBeFormatted);
                String redeemedSubscriberID = subscriberID;
                JSONObject additionalDetails = new JSONObject();
                Map<String, String> alternateIDs = new LinkedHashMap<>();
                SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile,
                    subscriberGroupEpochReader, SystemTime.getCurrentTime());
                for (Map.Entry<String, AlternateID> entry : Deployment.getAlternateIDs().entrySet())
                  {
                    AlternateID alternateID = entry.getValue();
                    if (alternateID.getProfileCriterionField() == null)
                      {
                        continue;
                      }
                    CriterionField criterionField = Deployment.getProfileCriterionFields()
                        .get(alternateID.getProfileCriterionField());
                    String criterionFieldValue = (String) criterionField.retrieveNormalized(evaluationRequest);                    
                    alternateIDs.put(entry.getKey(), criterionFieldValue);
                  }
                additionalDetails.put("RedeemedDate", redeemedDate);
                additionalDetails.put("customerID", redeemedSubscriberID);
                additionalDetails.put("AlternateIDs", alternateIDs);
                
                errorException = new ThirdPartyManagerException(
                    RESTAPIGenericReturnCodes.VOUCHER_ALREADY_REDEEMED.getGenericResponseMessage(),
                    RESTAPIGenericReturnCodes.VOUCHER_ALREADY_REDEEMED.getGenericResponseCode(), additionalDetails);
              }else if(profileVoucher.getVoucherStatus()==VoucherDelivery.VoucherStatus.Expired||profileVoucher.getVoucherExpiryDate().before(now)){
          errorException = new ThirdPartyManagerException(RESTAPIGenericReturnCodes.VOUCHER_EXPIRED);
        }else{
          //an OK one
          voucherStored=profileVoucher;
          break;
        }
      }
    }

    if(voucherStored==null){
      if(errorException!=null) throw errorException;
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.VOUCHER_NOT_ASSIGNED);
    }

    return new Pair<>(subscriberID,voucherStored);

  }
  

  /*****************************************
  *
  *  processPutSimpleOffer
  *
  *****************************************/
  
  private JSONObject processPutSimpleOffer(JSONObject jsonRoot) throws ThirdPartyManagerException, ParseException, IOException

  {
    try
      {
        //
        // create request
        //
        /*****************************************
        *
        *  request
        *
        *****************************************/

        HashMap<String,Object> request = new HashMap<String,Object>();
        jsonRoot.put("apiVersion", 1); 
        AuthenticatedResponse authResponse = null;
        ThirdPartyCredential thirdPartyCredential = new ThirdPartyCredential(jsonRoot);
        if (!Deployment.getRegressionMode())
          {
            authResponse = authCache.get(thirdPartyCredential);
          }
        else
          {
            authResponse = authenticate(thirdPartyCredential);
          }
        int user = (authResponse.getUserId());
        String userID = Integer.toString(user);
        jsonRoot.put("loginID", userID);
        
        JSONObject result;


        StringEntity stringEntity = new StringEntity(jsonRoot.toString(), ContentType.create("application/json"));
        HttpPost httpPost = new HttpPost("http://"+guimanagerHost +":"+ guimanagerPort+"/nglm-guimanager/putSimpleOfferThirdParty");
        httpPost.setEntity(stringEntity);

        //
        // submit request
        //

        HttpResponse httpResponse = httpClient.execute(httpPost);

        //
        // process response
        //

        if (httpResponse != null && httpResponse.getStatusLine() != null
            && httpResponse.getStatusLine().getStatusCode() == 200)
          {
            String jsonResponse = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
            log.info("GUIManager raw response : {}", jsonResponse);

            //
            // parse JSON response from GUI
            //

            result = (JSONObject) (new JSONParser()).parse(jsonResponse);

          }
        else if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 401)
          {
            log.error("GUI server HTTP reponse code {} message {} ", httpResponse.getStatusLine().getStatusCode(), EntityUtils.toString(httpResponse.getEntity(), "UTF-8"));
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.PUT_SUPPLIEROFFER_FAILED.getGenericResponseMessage(), RESTAPIGenericReturnCodes.PUT_SUPPLIEROFFER_FAILED.getGenericResponseCode());
          }
        else if (httpResponse != null && httpResponse.getStatusLine() != null)
          {
            log.error("GUI server HTTP reponse code is invalid {}", httpResponse.getStatusLine().getStatusCode());
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.PUT_SUPPLIEROFFER_FAILED.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
          }
        else
          {
            log.error("GUI server error httpResponse or httpResponse.getStatusLine() is null {} {} ", httpResponse, httpResponse.getStatusLine());
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.PUT_SUPPLIEROFFER_FAILED.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
          }
        return result;
      }
    catch (ParseException pe)
      {
        log.error("failed to Parse ParseException {} ", pe.getMessage());
        throw pe;
      }
    catch (IOException e)
      {        
        log.error("IOException: {}", e.getMessage());
        throw e;
      }

  }

  /*****************************************
  *
  *  processGetSimpleOfferList
  *
  *****************************************/
  
  private JSONObject processGetSimpleOfferList(JSONObject jsonRoot)  throws ThirdPartyManagerException, ParseException, IOException

  {
    try 
      {
        //
        // create request
        //
        /*****************************************
         *
         * request
         *
         *****************************************/

        HashMap<String, Object> request = new HashMap<String, Object>();
        jsonRoot.put("apiVersion", 1);
        AuthenticatedResponse authResponse = null;
        ThirdPartyCredential thirdPartyCredential = new ThirdPartyCredential(jsonRoot);
        if (!Deployment.getRegressionMode())
          {
            authResponse = authCache.get(thirdPartyCredential);
          }
        else
          {
            authResponse = authenticate(thirdPartyCredential);
          } 
        int user = (authResponse.getUserId());
        String userID = Integer.toString(user);
        jsonRoot.put("loginID", userID);
        JSONObject result;

        StringEntity stringEntity = new StringEntity(jsonRoot.toString(), ContentType.create("application/json"));
        HttpPost httpPost = new HttpPost("http://"+guimanagerHost +":"+ guimanagerPort+"/nglm-guimanager/getSimpleOfferListThirdParty");
        httpPost.setEntity(stringEntity);

        //
        // submit request
        //

        HttpResponse httpResponse = httpClient.execute(httpPost);

        //
        // process response
        //

        if (httpResponse != null && httpResponse.getStatusLine() != null
            && httpResponse.getStatusLine().getStatusCode() == 200)
          {
            String jsonResponse = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
            log.info("GUI response : {}", jsonResponse);

            //
            // parse JSON response from GUI
            //

            result = (JSONObject) (new JSONParser()).parse(jsonResponse);

          }
        else if (httpResponse != null && httpResponse.getStatusLine() != null
            && httpResponse.getStatusLine().getStatusCode() == 401)
          {
            log.error("GUI server HTTP reponse code {} message {} ", httpResponse.getStatusLine().getStatusCode(),
                EntityUtils.toString(httpResponse.getEntity(), "UTF-8"));
            throw new ThirdPartyManagerException(
                RESTAPIGenericReturnCodes.GET_SUPPLIEROFFERS_FAILED.getGenericResponseMessage(),
                RESTAPIGenericReturnCodes.GET_SUPPLIEROFFERS_FAILED.getGenericResponseCode());
          }
        else if (httpResponse != null && httpResponse.getStatusLine() != null)
          {
            log.error("GUI server HTTP reponse code is invalid {}", httpResponse.getStatusLine().getStatusCode());
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.GET_SUPPLIEROFFERS_FAILED.getGenericResponseMessage(),
                RESTAPIGenericReturnCodes.GET_SUPPLIEROFFERS_FAILED.getGenericResponseCode());
          }
        else
          {
            log.error("GUI server error httpResponse or httpResponse.getStatusLine() is null {} {} ", httpResponse,
                httpResponse.getStatusLine());
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.GET_SUPPLIEROFFERS_FAILED.getGenericResponseMessage(),
                RESTAPIGenericReturnCodes.GET_SUPPLIEROFFERS_FAILED.getGenericResponseCode());
          }
        return result;
      }
    catch (ParseException pe)
      {
        log.error("failed to Parse ParseException {} ", pe.getMessage());
        throw pe;
      }
    catch (IOException e)
      {
        log.error("IOException: {}", e.getMessage());
        throw e;
      }

  }

  /*****************************************
  *
  *  processRemoveSimpleOffer
  *
  *****************************************/
  
  private JSONObject processRemoveSimpleOffer(JSONObject jsonRoot) throws ThirdPartyManagerException, ParseException, IOException

  {
    try 
      {
        //
        // create request
        //
        /*****************************************
         *
         * request
         *
         *****************************************/

        HashMap<String, Object> request = new HashMap<String, Object>();
        jsonRoot.put("apiVersion", 1);
        AuthenticatedResponse authResponse = null;
        ThirdPartyCredential thirdPartyCredential = new ThirdPartyCredential(jsonRoot);
        if (!Deployment.getRegressionMode())
          {
            authResponse = authCache.get(thirdPartyCredential);
          }
        else
          {
            authResponse = authenticate(thirdPartyCredential);
          }
        int user = (authResponse.getUserId());
        String userID = Integer.toString(user);
        jsonRoot.put("loginID", userID);
        JSONObject result;

        StringEntity stringEntity = new StringEntity(jsonRoot.toString(), ContentType.create("application/json"));
        HttpPost httpPost = new HttpPost("http://"+guimanagerHost +":"+ guimanagerPort+"/nglm-guimanager/removeSimpleOfferThirdParty");
        httpPost.setEntity(stringEntity);

        //
        // submit request
        //

        HttpResponse httpResponse = httpClient.execute(httpPost);

        //
        // process response
        //

        if (httpResponse != null && httpResponse.getStatusLine() != null
            && httpResponse.getStatusLine().getStatusCode() == 200)
          {
            String jsonResponse = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
            log.info("GUImanager response : {}", jsonResponse);

            //
            // parse JSON response from GUI
            //

            result = (JSONObject) (new JSONParser()).parse(jsonResponse);

          }
        else if (httpResponse != null && httpResponse.getStatusLine() != null
            && httpResponse.getStatusLine().getStatusCode() == 401)
          {
            log.error("GUI server HTTP reponse code {} message {} ", httpResponse.getStatusLine().getStatusCode(),
                EntityUtils.toString(httpResponse.getEntity(), "UTF-8"));
            throw new ThirdPartyManagerException(
                RESTAPIGenericReturnCodes.REMOVE_SUPPLIEROFFER_FAILED.getGenericResponseMessage(),
                RESTAPIGenericReturnCodes.REMOVE_SUPPLIEROFFER_FAILED.getGenericResponseCode());
          }
        else if (httpResponse != null && httpResponse.getStatusLine() != null)
          {
            log.error("GUI server HTTP reponse code is invalid {}", httpResponse.getStatusLine().getStatusCode());
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.REMOVE_SUPPLIEROFFER_FAILED.getGenericResponseMessage(),
                RESTAPIGenericReturnCodes.REMOVE_SUPPLIEROFFER_FAILED.getGenericResponseCode());
          }
        else
          {
            log.error("GUI server error httpResponse or httpResponse.getStatusLine() is null {} {} ", httpResponse,
                httpResponse.getStatusLine());
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.REMOVE_SUPPLIEROFFER_FAILED.getGenericResponseMessage(),
                RESTAPIGenericReturnCodes.REMOVE_SUPPLIEROFFER_FAILED.getGenericResponseCode());
          }
        return result;
      }
    catch (ParseException pe)
      {
        log.error("failed to Parse ParseException {} ", pe.getMessage());
        throw pe;
      }
    catch (IOException e)
      {
        log.error("IOException: {}", e.getMessage());
        throw e;
      }
  }

  private JSONObject constructThirdPartyResponse(RESTAPIGenericReturnCodes genericCode, Map<String,Object> response){
    if(response==null) response=new HashMap<>();
    updateResponse(response, genericCode);
    return JSONUtilities.encodeObject(response);
  }

  private void updateResponse(Map<String,Object> response, RESTAPIGenericReturnCodes genericCode)
  {
    updateResponse(response, genericCode, "");
  }

  private void updateResponse(Map<String,Object> response, RESTAPIGenericReturnCodes genericCode, String descriptionSuffix)
  {
    if (response != null)
      {
        response.put(GENERIC_RESPONSE_CODE,        genericCode.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG,         genericCode.getGenericResponseMessage());
        response.put(GENERIC_RESPONSE_DESCRIPTION, genericCode.getGenericDescription() + descriptionSuffix);
      }
  }

  /*****************************************
   *
   *  getDateFromString
   *
   *****************************************/

  private Date getDateFromString(String dateString, String dateFormat, String pattern) throws ThirdPartyManagerException
  {
    Date result = null;
    if (dateString != null)
      {
        if (pattern != null && !dateString.matches(pattern))
          {
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage()+"(invalid date/format expected in "+dateFormat+" and found "+dateString+")", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
          }
        try 
          {
            result = RLMDateUtils.parseDate(dateString, dateFormat, Deployment.getBaseTimeZone(), false);
          }
        catch(Exception ex)
          {
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage()+"(invalid date/format expected in "+dateFormat+" and found "+dateString+")", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
          }
        
      }
    return result;
  }

  /*****************************************
  *
  *  prepareEndDate
  *
  *****************************************/

  public static Date prepareEndDate(Date endDate)
  {
    Date result = null;
    if (endDate != null)
      {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
        cal.setTime(endDate);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        result = cal.getTime();
      }
    return result;
  }
  
  /*****************************************
  *
  *  prepareStartDate
  *
  *****************************************/

  public static Date prepareStartDate(Date startDate)
  {
    Date result = null;
    if (startDate != null)
      {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
        cal.setTime(startDate);
        cal.set(Calendar.HOUR_OF_DAY, 00);
        cal.set(Calendar.MINUTE, 00);
        cal.set(Calendar.SECOND, 00);
        result = cal.getTime();
      }
    return result;
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
   *  authenticateAndCheckAccess
   *
   *****************************************/

  private void authenticateAndCheckAccess(JSONObject jsonRoot, String api) throws ThirdPartyManagerException, ParseException, IOException
  {

    ThirdPartyCredential thirdPartyCredential = new ThirdPartyCredential(jsonRoot);

    //
    // confirm thirdPartyCredential format is ok
    //

    if (thirdPartyCredential.getLoginName() == null || thirdPartyCredential.getPassword() == null || thirdPartyCredential.getLoginName().isEmpty() || thirdPartyCredential.getPassword().isEmpty())
      {
        log.error("invalid request {}", "credential is missing");
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage() + "-{credential is missing}", RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
      }

    //
    // look up methodAccess configuration from deployment
    //

    ThirdPartyMethodAccessLevel methodAccessLevel = methodPermissionsMapper.get(api);

    //
    // access hack(dev purpose)
    //

    if (methodAccessLevel != null && methodAccessLevel.isByPassAuth()) return ;

    //
    // lookup from authCache
    //

    AuthenticatedResponse authResponse = authCache.get(thirdPartyCredential);
    if(authResponse == null)
      {
        synchronized (authCache)
        {
          authResponse = authCache.get(thirdPartyCredential);
          
          //
          //  cache miss - reauthenticate
          //
    
          if (authResponse == null)
            {
              authResponse = authenticate(thirdPartyCredential);
              log.info("(Re)Authenticated: credential {} response {}", thirdPartyCredential, authResponse);
            }
        }
    }



    //
    //  hasAccess
    //

    if (! hasAccess(authResponse, methodAccessLevel, api))
      {
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.INSUFFICIENT_USER_RIGHTS);
      }

  }

  /*****************************************
   *
   *  authenticate
   *
   *****************************************/

  private AuthenticatedResponse authenticate(ThirdPartyCredential thirdPartyCredential) throws IOException, ParseException, ThirdPartyManagerException
  {
    CloseableHttpResponse httpResponse = null;
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build())
    {
      //
      // create request
      //

      StringEntity stringEntity = new StringEntity(thirdPartyCredential.getJSONString(), ContentType.create("application/json"));
      HttpPost httpPost = new HttpPost("http://" + fwkServer + "/api/account/login");
      httpPost.setEntity(stringEntity);

      //
      // submit request
      //

      httpResponse = httpClient.execute(httpPost);

      //
      // process response
      //

      if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 200)
        {
          String jsonResponse = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
          log.info("FWK raw response : {}", jsonResponse);

          //
          // parse JSON response from FWK
          //

          JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(jsonResponse);

          //
          // prepare response
          //

          AuthenticatedResponse authResponse = new AuthenticatedResponse(jsonRoot);


          //
          // update cache
          //

          synchronized (authCache)
          {
            authCache.put(thirdPartyCredential, authResponse);
          }

          //
          // return
          //

          return authResponse;
        }
      else if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 401)
        {
          log.error("FWK server HTTP reponse code {} message {} ", httpResponse.getStatusLine().getStatusCode(), EntityUtils.toString(httpResponse.getEntity(), "UTF-8"));
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.AUTHENTICATION_FAILURE);
        }
      else if (httpResponse != null && httpResponse.getStatusLine() != null)
        {
          log.error("FWK server HTTP reponse code is invalid {}", httpResponse.getStatusLine().getStatusCode());
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.AUTHENTICATION_FAILURE);
        }
      else
        {
          log.error("FWK server error httpResponse or httpResponse.getStatusLine() is null {} {} ", httpResponse, httpResponse.getStatusLine());
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.AUTHENTICATION_FAILURE);
        }
    }
    catch(ParseException pe) 
    {
      log.error("failed to Parse ParseException {} ", pe.getMessage());
      throw pe;
    }
    catch(IOException e) 
    {
      log.error("failed to authenticate in FWK server");
      log.error("IOException: {}", e.getMessage());
      throw e;
    }
    finally
    {
      if (httpResponse != null) httpResponse.close();
    }
  }

  /*****************************************
   *
   *  hasAccess
   *
   *****************************************/

  private boolean hasAccess(AuthenticatedResponse authResponse, ThirdPartyMethodAccessLevel methodAccessLevel, String api)
  {

    //
    // check method access
    //

    if (methodAccessLevel == null || (methodAccessLevel.getPermissions().isEmpty()))
      {
        log.warn("No permission/workgroup is configured for method {} ", api);
        return false;
      }

    //
    // check permissions
    //
    for (String userPermission : authResponse.getPermissions())
      {
        if (methodAccessLevel.getPermissions().contains(userPermission))
          {
            return true;
          }

      }
    return false;

  }
  
  /*****************************************
  *
  *  resolveSubscriberID
  *
  *****************************************/

  private String resolveSubscriberID(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    // "customerID" parameter is mapped internally to subscriberID 
    String subscriberID = JSONUtilities.decodeString(jsonRoot, CUSTOMER_ID, false);
    String alternateSubscriberID = null;
    
    // finds the first parameter in the input request that corresponds to an entry in alternateID[]
    for (String id : Deployment.getAlternateIDs().keySet())
      {
        String param = JSONUtilities.decodeString(jsonRoot, id, false);
        if (param != null)
          {
            try
            {
              alternateSubscriberID = subscriberIDService.getSubscriberID(id, param);
              if (alternateSubscriberID == null)
                {
                  throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
                }
              break;
            } catch (SubscriberIDServiceException e)
            {
              log.error("SubscriberIDServiceException can not resolve subscriberID for {} error is {}", id, e.getMessage());
            }
          }
      }
    
    if (subscriberID == null)
      {
        if (alternateSubscriberID == null)
          {
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.MISSING_PARAMETERS);
          }
        subscriberID = alternateSubscriberID;
      }
    else if (alternateSubscriberID != null)
      {
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE);
      }
    // Returns a value, or an exception
    return subscriberID;
  }
  
  /*****************************************
  *
  *  resolveSubscriberAlternateID
  *
  *****************************************/

  private Pair<String, String> resolveSubscriberAlternateID(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    // "customerID" parameter is mapped internally to subscriberID 
    String subscriberID = JSONUtilities.decodeString(jsonRoot, CUSTOMER_ID, false);
    String alternateSubscriberID = null;
    
    // finds the first parameter in the input request that corresponds to an entry in alternateID[]
    String subscriberkey = null;
    String subscriberValue = null;
    String alternateIDKey = null;
    String alternateIDValue = null;
    Map<String, String> result = new HashMap<>();
    for (String id : Deployment.getAlternateIDs().keySet())
      {
        String param = JSONUtilities.decodeString(jsonRoot, id, false);
        if (param != null)
          {
            alternateSubscriberID = param;
            alternateIDKey = id;
            alternateIDValue = param;
            break;

          }
      }
    
    if (subscriberID == null)
      {
        if (alternateSubscriberID == null)
          {
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.MISSING_PARAMETERS);
          }
        subscriberkey = alternateIDKey;
        subscriberValue = alternateIDValue;
      }
    else if (subscriberID != null && alternateSubscriberID != null)
      {
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE);
      }
    else {
      subscriberkey = CUSTOMER_ID;
      subscriberValue = subscriberID;
    }
    // Returns a value, or an exception
    return new Pair<String, String>(subscriberkey, subscriberValue);
  }


  /*****************************************
  *
  *  resolveParentSubscriberID
  *
  *****************************************/
  
  private String resolveParentSubscriberID(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    // "customerID" parameter is mapped internally to subscriberID 
    String subscriberID = JSONUtilities.decodeString(jsonRoot, "newParentCustomerID", false);
    String alternateSubscriberID = null;
    
    // finds the first parameter in the input request that corresponds to an entry in alternateID[]
    for (String id : Deployment.getAlternateIDs().keySet())
      {
        String param = JSONUtilities.decodeString(jsonRoot, "newParent"+id, false);
        if (param != null)
          {
            try
            {
              alternateSubscriberID = subscriberIDService.getSubscriberID(id, param);
              if (alternateSubscriberID == null)
                {
                  throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
                }
              break;
            } catch (SubscriberIDServiceException e)
            {
              log.error("SubscriberIDServiceException can not resolve subscriberID for {} error is {}", id, e.getMessage());
            }
          }
      }
    
    if (subscriberID == null)
      {
        if (alternateSubscriberID == null)
          {
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.MISSING_PARAMETERS);
          }
        subscriberID = alternateSubscriberID;
      }
    else if (alternateSubscriberID != null)
      {
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE);
      }
    // Returns a value, or an exception
    return subscriberID;
  }
  

  /*****************************************
  *
  *  decodeStringIgnoreCase
  *
  *****************************************/
  
  public String decodeStringIgnoreCase(JSONObject jobj, String key) {
    Iterator<String> iter = jobj.keySet().iterator();
    while (iter.hasNext()) {
        String key1 = iter.next();
        if (key1.equalsIgnoreCase(key)) {
          try
          {
            return (String) (jobj.get(key1));
          }
          catch (ClassCastException e)
          {
            throw new JSONUtilitiesException("value for key " + key + " is not a string", e);
          }
        }
    }
    return null;
}
  
  /****************************************
  *
  *  resolveAllSubscriberIDs
  *
  ****************************************/

  public Map<String, String> resolveAllSubscriberIDs(SubscriberProfile subscriberProfile)
  {
    Map<String, String> result = new HashMap<String, String>();
    try
    {
      SubscriberEvaluationRequest request = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime());
      for(AlternateID alternateID : Deployment.getAlternateIDs().values())
        {
          if(alternateID.getProfileCriterionField() != null)
            {
              CriterionField field = Deployment.getProfileCriterionFields().get(alternateID.getProfileCriterionField());
              if(field != null)
                {
                  String alternateIDValue = (String) field.retrieveNormalized(request);
                  result.put(alternateID.getID(), alternateIDValue);
                }
            }
        }
      result.put("subscriberID", subscriberProfile.getSubscriberID());
    }
    catch (Exception e)
    {
      log.error("Exception can not get alternateIDs for {} error is {}", subscriberProfile.getSubscriberID(), e.getMessage());
    }
    return result;
  }
  
  /*****************************************
  *
  *  activeReseller and the SalesChannel List based on the userID
  *
  *****************************************/

  public Map<String, List<String>> activeResellerAndSalesChannelIDs(String userID)
  {

    List<String> activeResellerID = new ArrayList<>();
    List<String> salesChannelIDList = new ArrayList<>();
    List<String> parentResellerID = new ArrayList<>();
    HashMap<String, List<String>> response = new HashMap<String, List<String>>();
    Date now = SystemTime.getCurrentTime();

    for (GUIManagedObject storedResellerObject : resellerService.getStoredResellers())
      {
        if (storedResellerObject instanceof Reseller)
          {
            Reseller storedReseller = (Reseller) storedResellerObject;

            if (storedReseller.getUserIDs() != null && !((storedReseller.getUserIDs()).isEmpty()))
              {

                if ((storedReseller.getUserIDs()).contains(userID))
                  {
                    if (resellerService.isActiveReseller(storedReseller, now))
                      {
                        activeResellerID.add(storedReseller.getResellerID());
                        response.put("activeReseller", activeResellerID);
                        parentResellerID.add(storedReseller.getParentResellerID());
                        response.put("parentResellerID", parentResellerID);
                        break;

                      }
                    else
                      {
                        response.put("activeReseller", activeResellerID);
                        response.put("parentResellerID", parentResellerID);
                        log.warn("The reseller is inactive" + storedReseller.getResellerID());
                        break;
                      }
                  }
              }

          }
      }

    if (activeResellerID.size() != 0 && activeResellerID != null)
      {
        for (GUIManagedObject storedSalesChannelObject : salesChannelService.getStoredSalesChannels())
          {

            if (storedSalesChannelObject instanceof SalesChannel)
              {
                SalesChannel storedSalesChannel = (SalesChannel) storedSalesChannelObject;
                if (storedSalesChannel.getResellerIDs() != null && !((storedSalesChannel.getResellerIDs()).isEmpty()))
                  {

                    if ((((SalesChannel) storedSalesChannel).getResellerIDs()).contains(activeResellerID.get(0)))

                      {
                        salesChannelIDList.add(storedSalesChannel.getSalesChannelID());
                      }
                  }
              }
          }
        response.put("salesChannelIDsList", salesChannelIDList);
      }

    return response;

  }
  
  /****************************************************
   *
   * validate the offers based on the parents partners
   *
   ******************************************************/

  public Boolean offerValidation(String offerID)
  {

    Date now = SystemTime.getCurrentTime();
    Offer offer = offerService.getActiveOffer(offerID, now);
    Set<OfferVoucher> offerVouchers = new HashSet<>();
    Set<OfferProduct> offerProducts = new HashSet<>();

    String voucherID = null;
    String productID = null;
    String supplierID = "";
    Boolean offerValid = true;

    if (offer != null)
      {
        if (offer.getOfferVouchers() != null && !(offer.getOfferVouchers().isEmpty()))
          {
            offerVouchers = offer.getOfferVouchers();
            for (OfferVoucher voucher : offerVouchers)
              {
                voucherID = voucher.getVoucherID();
                Voucher offerVoucher = voucherService.getActiveVoucher(voucherID, now);
                if (offerVoucher != null)
                  {
                    if (offerVoucher.getSupplierID() != null && !(offerVoucher.getSupplierID().isEmpty()))
                      {
                        supplierID = offerVoucher.getSupplierID();

                      }
                  }

                break;
              }
          }
        if (offer.getOfferProducts() != null && !(offer.getOfferProducts().isEmpty()))
          {
            offerProducts = offer.getOfferProducts();
            for (OfferProduct product : offerProducts)
              {
                productID = product.getProductID();
                Product offerproduct = productService.getActiveProduct(productID, now);
                if (offerproduct != null)
                  {
                    if (offerproduct.getSupplierID() != null && !(offerproduct.getSupplierID().isEmpty()))
                      {
                        supplierID = offerproduct.getSupplierID();

                      }
                  }
                break;
              }
          }

        Supplier offerSupplier = supplierService.getActiveSupplier(supplierID, now);
        if (offerSupplier != null)
          {

            if (supplierService.isActiveSupplier(offerSupplier, now))
              {
                String parentSupplierID = offerSupplier.getParentSupplierID();
                if (parentSupplierID != null)
                  {

                    Supplier parentSupplier = supplierService.getActiveSupplier(parentSupplierID, now);

                    if (parentSupplier != null)
                      {

                        offerValid = true;

                      }
                    else
                      {
                        offerValid = false;
                      }
                  }
              }
          }
      }

    return offerValid;
  }



  /*****************************************
   *
   *  purchaseOffer
   *
   *****************************************/
  
  public PurchaseFulfillmentRequest purchaseOffer(SubscriberProfile subscriberProfile, boolean sync, String subscriberID, String offerID, String salesChannelID, int quantity,
      String moduleID, String featureID, String origin, String resellerID, KafkaProducer<byte[],byte[]> kafkaProducer) throws ThirdPartyManagerException
  {
    DeliveryManagerDeclaration deliveryManagerDeclaration = Deployment.getDeliveryManagers().get(PURCHASE_FULFILLMENT_MANAGER_TYPE);
    if (deliveryManagerDeclaration == null)
      {
        log.error("Internal error, cannot find a deliveryManager with a RequestClassName as com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest");
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
      }

    Serializer<StringKey> keySerializer = StringKey.serde().serializer();
    Serializer<PurchaseFulfillmentRequest> valueSerializer = ((ConnectSerde<PurchaseFulfillmentRequest>) deliveryManagerDeclaration.getRequestSerde()).serializer();
    
    String deliveryRequestID = zuks.getStringKey();
    // Build a json doc to create the PurchaseFulfillmentRequest
    HashMap<String,Object> request = new HashMap<String,Object>();
    request.put("subscriberID", subscriberID);
    request.put("offerID", offerID);
    request.put("quantity", quantity);
    request.put("salesChannelID", salesChannelID); 
    request.put("deliveryRequestID", deliveryRequestID);
    request.put("eventID", "0"); // No event here
    request.put("moduleID", moduleID);
    request.put("featureID", featureID);
    request.put("origin", origin);
    request.put("resellerID", resellerID);
    request.put("deliveryType", deliveryManagerDeclaration.getDeliveryType());
    JSONObject valueRes = JSONUtilities.encodeObject(request);
    
    PurchaseFulfillmentRequest purchaseRequest = new PurchaseFulfillmentRequest(subscriberProfile,subscriberGroupEpochReader,valueRes, deliveryManagerDeclaration, offerService, paymentMeanService, resellerService, productService, supplierService, voucherService, SystemTime.getCurrentTime());
    purchaseRequest.forceDeliveryPriority(DELIVERY_REQUEST_PRIORITY);
    String topic = deliveryManagerDeclaration.getRequestTopic(purchaseRequest.getDeliveryPriority());

    Future<PurchaseFulfillmentRequest> waitingResponse=null;
    if(sync){
      waitingResponse = purchaseResponseListenerService.addWithOnValueFilter(purchaseResponse->!purchaseResponse.isPending()&&purchaseResponse.getDeliveryRequestID().equals(deliveryRequestID));
    }

    kafkaProducer.send(new ProducerRecord<byte[],byte[]>(
        topic,
        keySerializer.serialize(topic, new StringKey(deliveryRequestID)),
        valueSerializer.serialize(topic, purchaseRequest)
        ));
    keySerializer.close(); valueSerializer.close(); // to make Eclipse happy
    if (sync) {
      return handleWaitingResponse(waitingResponse);
    }
    return purchaseRequest;
  }

  private <T> T handleWaitingResponse(Future<T> waitingResponse) throws ThirdPartyManagerException {
    try {
      T response = waitingResponse.get(httpTimeout, TimeUnit.MILLISECONDS);
      if(log.isDebugEnabled()) log.debug("response processed : "+response);
      return response;
    } catch (InterruptedException|ExecutionException e) {
      log.warn("Error waiting purchase response");
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    } catch (TimeoutException e) {
      log.info("Timeout waiting purchase response");
      throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.TIMEOUT);
    }
  }

  /*****************************************
   *
   *  class ThirdPartyManagerException
   *
   *****************************************/

  public static class ThirdPartyManagerException extends Exception
  {
    /*****************************************
     *
     *  data
     *
     *****************************************/

    private int responseCode;
    private JSONObject responseDetails;

    /*****************************************
     *
     *  accessors
     *
     *****************************************/

    public int getResponseCode() { return responseCode; }
    public JSONObject getResponseDetails() { return responseDetails; }

    /*****************************************
     *
     *  constructor
     *
     *****************************************/

    public ThirdPartyManagerException(String responseMessage, int responseCode)
    {
      super(responseMessage);
      this.responseCode = responseCode;
    }
    
    public ThirdPartyManagerException(String responseMessage, int responseCode, JSONObject responseDetails)
    {
      super(responseMessage);
      this.responseCode = responseCode;
      this.responseDetails = responseDetails;
    }

    public ThirdPartyManagerException(RESTAPIGenericReturnCodes genericReturnCode)
    {
      this(genericReturnCode.getGenericResponseMessage(),genericReturnCode.getGenericResponseCode());
    }   
  
    /*****************************************
     *
     *  constructor - exception
     *
     *****************************************/

    public ThirdPartyManagerException(Throwable e)
    {
      super(e.getMessage(), e);
      this.responseCode = -1;
    }
  }

  /*****************************************
   *
   *  class ThirdPartyCredential
   *
   *****************************************/

  private class ThirdPartyCredential
  {

    //
    // data
    //

    private String loginName;
    private String password;
    private JSONObject jsonRepresentation;

    //
    //  accessors
    //

    public String getLoginName() { return loginName; }
    public String getPassword() { return password; }
    public String getJSONString() { return jsonRepresentation.toString(); }

    /****************************
     *
     *  constructor
     *
     *****************************/

    public ThirdPartyCredential(JSONObject jsonRoot) throws ThirdPartyManagerException
    {
      this.loginName = readString(jsonRoot, "loginName", true);
      this.password = readString(jsonRoot, "password", true);

      //
      //  jsonRepresentation
      //

      jsonRepresentation = new JSONObject();
      jsonRepresentation.put("LoginName", loginName);
      jsonRepresentation.put("Password", password);
    }

    /*****************************************
     *
     *  equals/hashCode
     *
     *****************************************/

    @Override public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof ThirdPartyCredential)
        {
          ThirdPartyCredential credential = (ThirdPartyCredential) obj;
          result = loginName.equals(credential.getLoginName()) && password.equals(credential.getPassword());
        }
      return result;
    }

    @Override public int hashCode()
    {
      return loginName.hashCode() + password.hashCode();
    }
  }

  /*****************************************
   *
   *  class AuthenticatedResponse
   *
   *****************************************/

  private class AuthenticatedResponse
  {
    //
    // data
    //

    private int userId;
    private String loginName;
    private String token;
    private String languageIso;
    private List<String> permissions;
    private String clientIP;
    private WorkgroupHierarchy workgroupHierarchy;
    private String tokenCreationDate;
    private String additionalInfo;

    //
    // accessors
    //

    public int getUserId() { return userId; }
    public String getLoginName() { return loginName; }  
    public String getToken() { return token; }  
    public String getLanguageIso() { return languageIso; }  
    public List<String> getPermissions() { return permissions; }  
    public String getClientIP() { return clientIP; }  
    public WorkgroupHierarchy getWorkgroupHierarchy() { return workgroupHierarchy; }  
    public String getTokenCreationDate() { return tokenCreationDate; }  
    public String getAdditionalInfo() { return additionalInfo; }

    /*****************************************
     *
     *  constructor
     *
     *****************************************/

    private AuthenticatedResponse(JSONObject jsonRoot)
    {
      this.userId = JSONUtilities.decodeInteger(jsonRoot, "UserId", true);
      this.loginName = JSONUtilities.decodeString(jsonRoot, "LoginName", true);
      this.token = JSONUtilities.decodeString(jsonRoot, "Token", true);
      this.languageIso = JSONUtilities.decodeString(jsonRoot, "LanguageIso", false);
      JSONArray userPermissionArray = JSONUtilities.decodeJSONArray(jsonRoot, "Permissions", true);
      permissions = new ArrayList<String>();
      for (int i=0; i<userPermissionArray.size(); i++)
        {
          permissions.add((String) userPermissionArray.get(i));
        }
      this.clientIP = JSONUtilities.decodeString(jsonRoot, "ClientIP", true);
      this.workgroupHierarchy = new WorkgroupHierarchy(JSONUtilities.decodeJSONObject(jsonRoot, "WorkgroupHierarchy", true));
      this.tokenCreationDate = JSONUtilities.decodeString(jsonRoot, "TokenCreationDate", true);
      this.additionalInfo = JSONUtilities.decodeString(jsonRoot, "AdditionalInfo", false);
    }

    /*****************************************
     *
     *  class WorkgroupHierarchy
     *
     *****************************************/

    private class WorkgroupHierarchy
    {
      //
      // data
      //

      private Workgroup workgroup;
      private String parents;
      private String children;

      //
      // accessors
      //

      public Workgroup getWorkgroup() { return workgroup; }
      public String getParents() { return parents; }
      public String getChildren() { return children; }

      /*****************************************
       *
       *  constructor
       *
       *****************************************/

      public WorkgroupHierarchy(JSONObject workgroupHierarchyJSONObject)
      {
        this.workgroup = new Workgroup(JSONUtilities.decodeJSONObject(workgroupHierarchyJSONObject, "Workgroup", true));
        this.parents = JSONUtilities.decodeString(workgroupHierarchyJSONObject, "Parents", false);
        this.children = JSONUtilities.decodeString(workgroupHierarchyJSONObject, "Children", false);
      }

      /*****************************************
       *
       *  class Workgroup
       *
       *****************************************/
      private class Workgroup
      {
        //
        // data
        //

        private int id;
        private String key;
        private String name;

        //
        // accessors
        //

        public int getId() { return id; }
        public String getKey() { return key; }
        public String getName() { return name; }

        /*****************************************
         *
         *  constructor
         *
         *****************************************/

        public Workgroup(JSONObject workgroupJSONObject)
        {
          this.id = JSONUtilities.decodeInteger(workgroupJSONObject, "Id", true);
          this.key = JSONUtilities.decodeString(workgroupJSONObject, "Key", true);
          this.name = JSONUtilities.decodeString(workgroupJSONObject, "Name", true);
        }

      }
    }
  }

  /*****************************************
   *
   *  getDateString
   *
   *****************************************/

  public static String getDateString(Date date)
  {
    String result = null;
    if (date == null) return result;
    try
    {
      SimpleDateFormat dateFormat = new SimpleDateFormat(Deployment.getAPIresponseDateFormat());
      dateFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
      result = dateFormat.format(date);
    }
    catch (Exception e)
    {
      log.warn(e.getMessage());
    }
    return result;
  }
  
  /*****************************************
  *
  *  readString
  *
  *****************************************/
  
  private String readString(JSONObject jsonRoot, String key, boolean validateNotEmpty) throws ThirdPartyManagerException
  {
    String result = readString(jsonRoot, key);
    if (validateNotEmpty && (result == null || result.trim().isEmpty()))
      {
        log.error("readString validation error");
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
      }
    return result;
  }
  
  /*****************************************
  *
  *  readString
  *
  *****************************************/
  
  private String readString(JSONObject jsonRoot, String key) throws ThirdPartyManagerException
  {
    String result = null;
    try 
      {
        result = JSONUtilities.decodeString(jsonRoot, key, false);
      }
    catch (JSONUtilitiesException e) 
      {
        log.error("readString JSONUtilitiesException "+e.getMessage());
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
      }
    return result;
  }
  
  /*****************************************
  *
  *  readInteger
  *
  *****************************************/
  
  private Integer readInteger(JSONObject jsonRoot, String key, boolean required) throws ThirdPartyManagerException
  {
    try 
      {
        return JSONUtilities.decodeInteger(jsonRoot, key, required);
      }
    catch (JSONUtilitiesException e) 
      {
        log.error("JSONUtilitiesException "+e.getMessage());
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
      }
  }

  /*****************************************
  *
  *  generateTokenChange
  *
  *****************************************/

  private void generateTokenChange(String subscriberID, Date now, String tokenCode, String action, String str, API api, JSONObject jsonRoot)
  {
    String topic = Deployment.getTokenChangeTopic();
    Serializer<StringKey> keySerializer = StringKey.serde().serializer();
    Serializer<TokenChange> valueSerializer = TokenChange.serde().serializer();
    String featureID = JSONUtilities.decodeString(jsonRoot, "loginName", DEFAULT_FEATURE_ID);
    String origin = JSONUtilities.decodeString(jsonRoot, "origin", false);
    TokenChange tokenChange = new TokenChange(subscriberID, now, "", tokenCode, action, str, origin, Module.REST_API, featureID);
    kafkaProducer.send(new ProducerRecord<byte[],byte[]>(
        topic,
        keySerializer.serialize(topic, new StringKey(subscriberID)),
        valueSerializer.serialize(topic, tokenChange)
        ));
    keySerializer.close(); valueSerializer.close(); // to make Eclipse happy
  }

  // helpers for stats
  private void addOKStats(String apiName, long startTime){
    addStats(apiName,startTime,StatsBuilders.STATUS.ok);
  }
  private void addKOStats(String apiName, long startTime){
    addStats(apiName,startTime,StatsBuilders.STATUS.ko);
  }
  private void addUnknownStats(String apiName, long startTime){
    addStats(apiName,startTime,StatsBuilders.STATUS.unknown);
  }
  private void addStats(String apiName, long startTime, StatsBuilders.STATUS status){
    if(statsDuration==null){
      log.warn("trying to add stats while not initialized "+apiName);
      return;
    }
    // exact same for duration
    statsDuration.withLabel(StatsBuilders.LABEL.name.name(),apiName)
         .withLabel(StatsBuilders.LABEL.status.name(),status.name())
         .getStats().add(startTime);
  }
  
  /*********************************
   * 
   * getSearchRequest
   * 
   ********************************/
  
  private SearchRequest getSearchRequest(API api, String subscriberId, Date startDate, List<QueryBuilder> filters)
  {
    SearchRequest searchRequest = null;
    String index = null;
    BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("subscriberID", subscriberId));
    Date indexFilterDate = RLMDateUtils.addDays(SystemTime.getCurrentTime(), -7, Deployment.getBaseTimeZone());
    
    //
    //  filters
    //
    
    for (QueryBuilder filter : filters)
      {
        query = query.filter(filter);
      }
    
    switch (api)
    {
      case getCustomerBDRs:
        if (startDate != null)
          {
            if (indexFilterDate.before(startDate))
              {
                List<String> esIndexDates = BDRReportMonoPhase.getEsIndexDates(startDate, SystemTime.getCurrentTime(), true);
                String indexCSV = BDRReportMonoPhase.getESIndices(BDRReportDriver.ES_INDEX_BDR_INITIAL, esIndexDates);
                index = getExistingIndices(indexCSV, BDRReportMonoPhase.getESAllIndices(BDRReportDriver.ES_INDEX_BDR_INITIAL));
              }
            else
              {
                index = BDRReportMonoPhase.getESAllIndices(BDRReportDriver.ES_INDEX_BDR_INITIAL);
              }
            query = query.filter(QueryBuilders.rangeQuery("eventDatetime").gte(esDateFormat.format(startDate)));
          }
        else
          {
            index = BDRReportMonoPhase.getESAllIndices(BDRReportDriver.ES_INDEX_BDR_INITIAL);
          }
        break;
        
      case getCustomerODRs:
        if (startDate != null)
          {
            if (indexFilterDate.before(startDate))
              {
                List<String> esIndexDates = ODRReportMonoPhase.getEsIndexDates(startDate, SystemTime.getCurrentTime(), true);
                String indexCSV = ODRReportMonoPhase.getESIndices(ODRReportDriver.ES_INDEX_ODR_INITIAL, esIndexDates);
                index = getExistingIndices(indexCSV, ODRReportMonoPhase.getESAllIndices(ODRReportDriver.ES_INDEX_ODR_INITIAL));
              }
            else
              {
                index = ODRReportMonoPhase.getESAllIndices(ODRReportDriver.ES_INDEX_ODR_INITIAL);
              }
            query = query.filter(QueryBuilders.rangeQuery("eventDatetime").gte(esDateFormat.format(startDate)));
          }
        else
          {
            index = ODRReportMonoPhase.getESAllIndices(ODRReportDriver.ES_INDEX_ODR_INITIAL);
          }
        break;
        
      case getCustomerMessages:
        if (startDate != null)
          {
            if (indexFilterDate.before(startDate))
              {
                List<String> esIndexDates = NotificationReportMonoPhase.getEsIndexDates(startDate, SystemTime.getCurrentTime(), true);
                String indexCSV = NotificationReportMonoPhase.getESIndices(NotificationReportDriver.ES_INDEX_NOTIFICATION_INITIAL, esIndexDates);
                index = getExistingIndices(indexCSV, NotificationReportMonoPhase.getESAllIndices(NotificationReportDriver.ES_INDEX_NOTIFICATION_INITIAL));
              }
            else
              {
                index = NotificationReportMonoPhase.getESAllIndices(NotificationReportDriver.ES_INDEX_NOTIFICATION_INITIAL);
              }
            query = query.filter(QueryBuilders.rangeQuery("creationDate").gte(esDateFormat.format(startDate)));
          }
        else
          {
            index = NotificationReportMonoPhase.getESAllIndices(NotificationReportDriver.ES_INDEX_NOTIFICATION_INITIAL);
          }
        break;
        
      case getCustomerCampaigns:
      case getCustomerJourneys:
        index = JourneyCustomerStatisticsReportDriver.JOURNEY_ES_INDEX + "*";
        break;
        
      default:
        break;
    }
    
    //
    //  searchRequest
    //
    
    searchRequest = new SearchRequest(index).source(new SearchSourceBuilder().query(query));
    
    //
    //  return
    //
    
    return searchRequest;
  }
  
  /*****************************************
  *
  * getNotificationDeliveryRequest
  *
  *****************************************/
  
  private DeliveryRequest getNotificationDeliveryRequest(String requestClass, SearchHit hit)
  {
    DeliveryRequest deliveryRequest = null;
    if (requestClass.equals(MailNotificationManagerRequest.class.getName()))
      {
        deliveryRequest = new MailNotificationManagerRequest(hit.getSourceAsMap());
      }
    else if(requestClass.equals(SMSNotificationManagerRequest.class.getName()))
      {
        deliveryRequest = new SMSNotificationManagerRequest(hit.getSourceAsMap());
      }
    else if (requestClass.equals(NotificationManagerRequest.class.getName()))
      {
        deliveryRequest = new NotificationManagerRequest(hit.getSourceAsMap());
      }
    else if (requestClass.equals(PushNotificationManagerRequest.class.getName()))
      {
        deliveryRequest = new PushNotificationManagerRequest(hit.getSourceAsMap());
      }
    else
      {
        if (log.isErrorEnabled()) log.error("invalid requestclass {}", requestClass);
      }
    return deliveryRequest;
  }
  
  /*****************************************
  *
  * getExistingIndices
  *
  *****************************************/
  
  private String getExistingIndices(String indexCSV, String defaulteValue)
  {
    String result = null;
    StringBuilder existingIndexes = new StringBuilder();
    boolean firstEntry = true;
    
    if (indexCSV != null)
      {
        for (String index : indexCSV.split(","))
          {
            if(index.endsWith("*")) 
              {
                if (!firstEntry) existingIndexes.append(",");
                existingIndexes.append(index); 
                firstEntry = false;
                continue;
              }
            else
              {
                GetIndexRequest request = new GetIndexRequest(index);
                request.local(false); 
                request.humanReadable(true); 
                request.includeDefaults(false); 
                try
                {
                  boolean exists = elasticsearch.indices().exists(request, RequestOptions.DEFAULT);
                  if (exists) 
                    {
                      if (!firstEntry) existingIndexes.append(",");
                      existingIndexes.append(index);
                      firstEntry = false;
                    }
                } 
              catch (IOException e)
                {
                  log.info("Exception " + e.getLocalizedMessage());
                }
              }
          }
        result = existingIndexes.toString();
      }
    result = result == null || result.trim().isEmpty() ? defaulteValue : result;
    if (log.isDebugEnabled()) log.debug("reading data from index {}", result);
    return result;
  }
}
