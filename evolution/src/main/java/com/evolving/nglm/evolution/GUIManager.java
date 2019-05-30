/*****************************************************************************
*
*  GUIManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUpload;
import org.apache.commons.fileupload.RequestContext;
import org.apache.commons.fileupload.util.Streams;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.EnhancedThrowableRenderer;
import org.apache.zookeeper.ZooKeeper;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Alarm;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.LicenseChecker;
import com.evolving.nglm.core.LicenseChecker.LicenseState;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.core.SubscriberIDService.SubscriberIDServiceException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.UniqueKeyServer;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityType;
import com.evolving.nglm.evolution.CriterionContext;
import com.evolving.nglm.evolution.DeliveryManagerAccount.Account;
import com.evolving.nglm.evolution.DeliveryRequest.ActivityType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionOperator;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.Journey.GUINode;
import com.evolving.nglm.evolution.Journey.TargetingType;
import com.evolving.nglm.evolution.Report.SchedulingInterval;
import com.evolving.nglm.evolution.SegmentationDimension.SegmentationDimensionTargetingType;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;
import com.evolving.nglm.evolution.TokenType.TokenTypeKind;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class GUIManager
{
  /*****************************************
  *
  *  ProductID
  *
  *****************************************/

  public static String ProductID = "Evolution-GUIManager";

  /*****************************************
  *
  *  enum
  *
  *****************************************/
  
  public enum CustomerStatusInJourney
  {
    ENTERED("ENTERED"), 
    NOTIFIED("NOTIFIED"), 
    CONVERTED("CONVERTED"),
    CONTROL("CONTROL"), 
    UCG("UCG"), 
    NOTIFIED_CONVERTED("NOTIFIED CONVERTED"), 
    CONTROL_CONVERTED("CONTROL CONVERTED"), 
    COMPLETED("COMPLETED"),
    UNKNOWN("UNKNOWN");
    private String externalRepresentation;
    private CustomerStatusInJourney(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CustomerStatusInJourney fromExternalRepresentation(String externalRepresentation) { for (CustomerStatusInJourney enumeratedValue : CustomerStatusInJourney.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
  }

  public enum API
  {
    getStaticConfiguration("getStaticConfiguration"),
    getSupportedLanguages("getSupportedLanguages"),
    getSupportedCurrencies("getSupportedCurrencies"),
    getSupportedTimeUnits("getSupportedTimeUnits"),
    getServiceTypes("getServiceTypes"),
    getTouchPoints("getTouchPoints"),
    getCallingChannelProperties("getCallingChannelProperties"),
    getCatalogCharacteristicUnits("getCatalogCharacteristicUnits"),
    getSupportedDataTypes("getSupportedDataTypes"),
    getSupportedEvents("getSupportedEvents"),
    getSupportedTargetingTypes("getSupportedTargetingTypes"),
    getProfileCriterionFields("getProfileCriterionFields"),
    getProfileCriterionFieldIDs("getProfileCriterionFieldIDs"),
    getProfileCriterionField("getProfileCriterionField"),
    getPresentationCriterionFields("getPresentationCriterionFields"),
    getPresentationCriterionFieldIDs("getPresentationCriterionFieldIDs"),
    getPresentationCriterionField("getPresentationCriterionField"),
    getJourneyCriterionFields("getJourneyCriterionFields"),
    getJourneyCriterionFieldIDs("getJourneyCriterionFieldIDs"),
    getJourneyCriterionField("getJourneyCriterionField"),
    getOfferCategories("getOfferCategories"),
    getOfferTypes("getOfferTypes"),
    getOfferOptimizationAlgorithms("getOfferOptimizationAlgorithms"),
    getNodeTypes("getNodeTypes"),
    getJourneyToolbox("getJourneyToolbox"),
    getJourneyList("getJourneyList"),
    getJourneySummaryList("getJourneySummaryList"),
    getJourney("getJourney"),
    putJourney("putJourney"),
    removeJourney("removeJourney"),
    startJourney("startJourney"),
    stopJourney("stopJourney"),
    getCampaignToolbox("getCampaignToolbox"),
    getCampaignList("getCampaignList"),
    getCampaignSummaryList("getCampaignSummaryList"),
    getCampaign("getCampaign"),
    putCampaign("putCampaign"),
    removeCampaign("removeCampaign"),
    startCampaign("startCampaign"),
    stopCampaign("stopCampaign"),
    getSegmentationDimensionList("getSegmentationDimensionList"),
    getSegmentationDimensionSummaryList("getSegmentationDimensionSummaryList"),
    getSegmentationDimension("getSegmentationDimension"),
    putSegmentationDimension("putSegmentationDimension"),
    removeSegmentationDimension("removeSegmentationDimension"),
    countBySegmentationRanges("countBySegmentationRanges"),
    evaluateProfileCriteria("evaluateProfileCriteria"),
    getUCGDimensionSummaryList("getUCGDimensionSummaryList"),
    getPointList("getPointList"),
    getPointSummaryList("getPointSummaryList"),
    getPoint("getPoint"),
    putPoint("putPoint"),
    removePoint("removePoint"),
    getOfferList("getOfferList"),
    getOfferSummaryList("getOfferSummaryList"),
    getOffer("getOffer"),
    putOffer("putOffer"),
    removeOffer("removeOffer"),
    getReportGlobalConfiguration("getReportGlobalConfiguration"),
    getReportList("getReportList"),
    putReport("putReport"),
    launchReport("launchReport"),
    downloadReport("downloadReport"),
    getPresentationStrategyList("getPresentationStrategyList"),
    getPresentationStrategySummaryList("getPresentationStrategySummaryList"),
    getPresentationStrategy("getPresentationStrategy"),
    putPresentationStrategy("putPresentationStrategy"),
    removePresentationStrategy("removePresentationStrategy"),
    getScoringStrategyList("getScoringStrategyList"),
    getScoringStrategySummaryList("getScoringStrategySummaryList"),
    getScoringStrategy("getScoringStrategy"),
    putScoringStrategy("putScoringStrategy"),
    removeScoringStrategy("removeScoringStrategy"),
    getCallingChannelList("getCallingChannelList"),
    getCallingChannelSummaryList("getCallingChannelSummaryList"),
    getCallingChannel("getCallingChannel"),
    putCallingChannel("putCallingChannel"),
    removeCallingChannel("removeCallingChannel"),
    getSalesChannelList("getSalesChannelList"),
    getSalesChannelSummaryList("getSalesChannelSummaryList"),
    getSalesChannel("getSalesChannel"),
    putSalesChannel("putSalesChannel"),
    removeSalesChannel("removeSalesChannel"),
    getSupplierList("getSupplierList"),
    getSupplierSummaryList("getSupplierSummaryList"),
    getSupplier("getSupplier"),
    putSupplier("putSupplier"),
    removeSupplier("removeSupplier"),
    getProductList("getProductList"),
    getProductSummaryList("getProductSummaryList"),
    getProduct("getProduct"),
    putProduct("putProduct"),
    removeProduct("removeProduct"),
    getCatalogCharacteristicList("getCatalogCharacteristicList"),
    getCatalogCharacteristicSummaryList("getCatalogCharacteristicSummaryList"),
    getCatalogCharacteristic("getCatalogCharacteristic"),
    putCatalogCharacteristic("putCatalogCharacteristic"),
    removeCatalogCharacteristic("removeCatalogCharacteristic"),
    getContactPolicyList("getContactPolicyList"),
    getContactPolicySummaryList("getContactPolicySummaryList"),
    getContactPolicy("getContactPolicy"),
    putContactPolicy("putContactPolicy"),
    removeContactPolicy("removeContactPolicy"),
    getJourneyObjectiveList("getJourneyObjectiveList"),
    getJourneyObjectiveSummaryList("getJourneyObjectiveSummaryList"),
    getJourneyObjective("getJourneyObjective"),
    putJourneyObjective("putJourneyObjective"),
    removeJourneyObjective("removeJourneyObjective"),
    getOfferObjectiveList("getOfferObjectiveList"),
    getOfferObjectiveSummaryList("getOfferObjectiveSummaryList"),
    getOfferObjective("getOfferObjective"),
    putOfferObjective("putOfferObjective"),
    removeOfferObjective("removeOfferObjective"),
    getProductTypeList("getProductTypeList"),
    getProductTypeSummaryList("getProductTypeSummaryList"),
    getProductType("getProductType"),
    putProductType("putProductType"),
    removeProductType("removeProductType"),
    getUCGRuleList("getUCGRuleList"),
    getUCGRuleSummaryList("getUCGRuleSummaryList"),
    getUCGRule("getUCGRule"),
    putUCGRule("putUCGRule"),
    removeUCGRule("removeUCGRule"),
    getDeliverableList("getDeliverableList"),
    getDeliverableSummaryList("getDeliverableSummaryList"),
    getDeliverable("getDeliverable"),
    getDeliverableByName("getDeliverableByName"),
    getTokenTypeList("getTokenTypeList"),
    getTokenTypeSummaryList("getTokenTypeSummaryList"),
    putTokenType("putTokenType"),
    getTokenType("getTokenType"),
    removeTokenType("removeTokenType"),
    getTokenCodesFormats("getTokenCodesFormats"),
    getMailTemplateList("getMailTemplateList"),
    getMailTemplateSummaryList("getMailTemplateSummaryList"),
    getMailTemplate("getMailTemplate"),
    putMailTemplate("putMailTemplate"),
    removeMailTemplate("removeMailTemplate"),
    getSMSTemplateList("getSMSTemplateList"),
    getSMSTemplateSummaryList("getSMSTemplateSummaryList"),
    getSMSTemplate("getSMSTemplate"),
    putSMSTemplate("putSMSTemplate"),
    removeSMSTemplate("removeSMSTemplate"),
    getFulfillmentProviders("getFulfillmentProviders"),
    getPaymentMeans("getPaymentMeans"),
    getPaymentMeanList("getPaymentMeanList"),
    getPaymentMeanSummaryList("getPaymentMeanSummaryList"),
    getPaymentMean("getPaymentMean"),
    putPaymentMean("putPaymentMean"),
    removePaymentMean("removePaymentMean"),
    getDashboardCounts("getDashboardCounts"),
    getCustomer("getCustomer"),
    getCustomerMetaData("getCustomerMetaData"),
    getCustomerActivityByDateRange("getCustomerActivityByDateRange"),
    getCustomerBDRs("getCustomerBDRs"),
    getCustomerODRs("getCustomerODRs"),
    getCustomerMessages("getCustomerMessages"),
    getCustomerJourneys("getCustomerJourneys"),
    getCustomerCampaigns("getCustomerCamapigns"),
    refreshUCG("refreshUCG"),
    putUploadedFile("putUploadedFile"),
    getUploadedFileList("getUploadedFileList"),
    getUploadedFileSummaryList("getUploadedFileSummaryList"),
    removeUploadedFile("removeUploadedFile"),
    getCustomerAlternateIDs("getCustomerAlternateIDs"),
    getCustomerAvailableCampaigns("getCustomerAvailableCampaigns"),
    getTargetList("getTargetList"),
    getTargetSummaryList("getTargetSummaryList"),
    putTarget("putTarget"),
    getTarget("getTarget"),
    removeTarget("removeTarget"),
    updateCustomer("updateCustomer"),
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

  private static final Logger log = LoggerFactory.getLogger(GUIManager.class);

  //
  //  license
  //

  private LicenseChecker licenseChecker = null;

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  static
  //

  private static final int RESTAPIVersion = 1;
  private static Method guiManagerExtensionEvaluateEnumeratedValuesMethod;

  //
  //  instance
  //

  private KafkaProducer<byte[], byte[]> kafkaProducer;
  private HttpServer restServer;
  private RestHighLevelClient elasticsearch;
  private JourneyService journeyService;
  private SegmentationDimensionService segmentationDimensionService;
  private PointService pointService;
  private OfferService offerService;
  private ReportService reportService;
  private PaymentMeanService paymentMeanService;
  private ScoringStrategyService scoringStrategyService;
  private PresentationStrategyService presentationStrategyService;
  private CallingChannelService callingChannelService;
  private SalesChannelService salesChannelService;
  private SupplierService supplierService;
  private ProductService productService;
  private CatalogCharacteristicService catalogCharacteristicService;
  private ContactPolicyService contactPolicyService;
  private JourneyObjectiveService journeyObjectiveService;
  private OfferObjectiveService offerObjectiveService;
  private ProductTypeService productTypeService;
  private UCGRuleService ucgRuleService;
  private DeliverableService deliverableService;
  private TokenTypeService tokenTypeService;
  private MailTemplateService mailTemplateService;
  private SMSTemplateService smsTemplateService;
  private SubscriberProfileService subscriberProfileService;
  private SubscriberIDService subscriberIDService;
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private DeliverableSourceService deliverableSourceService;
  private String getCustomerAlternateID;
  private UploadedFileService uploadedFileService;
  private TargetService targetService;
  
  private static final String MULTIPART_FORM_DATA = "multipart/form-data"; 
  private static final String FILE_REQUEST = "file"; 
  private static final String FILE_UPLOAD_META_DATA= "fileUploadMetaData"; 

  //
  //  context
  //

  private GUIManagerContext guiManagerContext;

  /*****************************************
  *
  *  epochServer
  *
  *****************************************/

  private static UniqueKeyServer epochServer = new UniqueKeyServer();

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    NGLMRuntime.initialize();
    GUIManager guiManager = new GUIManager();
    guiManager.start(args);
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
    String elasticsearchServerHost = args[3];
    int elasticsearchServerPort = parseInteger("elasticsearchServerPort", args[4]);
    String nodeID = System.getProperty("nglm.license.nodeid");

    String journeyTopic = Deployment.getJourneyTopic();
    String segmentationDimensionTopic = Deployment.getSegmentationDimensionTopic();
    String pointTopic = Deployment.getPointTopic();
    String offerTopic = Deployment.getOfferTopic();
    String reportTopic = Deployment.getReportTopic();
    String paymentMeanTopic = Deployment.getPaymentMeanTopic();
    String presentationStrategyTopic = Deployment.getPresentationStrategyTopic();
    String scoringStrategyTopic = Deployment.getScoringStrategyTopic();
    String callingChannelTopic = Deployment.getCallingChannelTopic();
    String salesChannelTopic = Deployment.getSalesChannelTopic();
    String supplierTopic = Deployment.getSupplierTopic();
    String productTopic = Deployment.getProductTopic();
    String catalogCharacteristicTopic = Deployment.getCatalogCharacteristicTopic();
    String contactPolicyTopic = Deployment.getContactPolicyTopic();
    String journeyObjectiveTopic = Deployment.getJourneyObjectiveTopic();
    String offerObjectiveTopic = Deployment.getOfferObjectiveTopic();
    String productTypeTopic = Deployment.getProductTypeTopic();
    String ucgRuleTopic = Deployment.getUCGRuleTopic();
    String deliverableTopic = Deployment.getDeliverableTopic();
    String tokenTypeTopic = Deployment.getTokenTypeTopic();
    String mailTemplateTopic = Deployment.getMailTemplateTopic();
    String smsTemplateTopic = Deployment.getSMSTemplateTopic();
    String subscriberGroupEpochTopic = Deployment.getSubscriberGroupEpochTopic();
    String deliverableSourceTopic = Deployment.getDeliverableSourceTopic();
    String redisServer = Deployment.getRedisSentinels();
    String subscriberProfileEndpoints = Deployment.getSubscriberProfileEndpoints();
    String uploadedFileTopic = Deployment.getUploadedFileTopic();
    String targetTopic = Deployment.getTargetTopic();
    getCustomerAlternateID = Deployment.getGetCustomerAlternateID();

    //
    //  log
    //

    log.info("main START: {} {} {} {} {} {} {} {} {} {} {} {} {}", apiProcessKey, bootstrapServers, apiRestPort, elasticsearchServerHost, elasticsearchServerPort, nodeID, journeyTopic, segmentationDimensionTopic, offerTopic, presentationStrategyTopic, scoringStrategyTopic, subscriberGroupEpochTopic, mailTemplateTopic, smsTemplateTopic);

    //
    //  license
    //

    licenseChecker = new LicenseChecker(ProductID, nodeID, Deployment.getZookeeperRoot(), Deployment.getZookeeperConnect());

    //
    //  guiManagerExtensionEvaluateEnumeratedValuesMethod
    //

    try
      {
        guiManagerExtensionEvaluateEnumeratedValuesMethod = (Deployment.getGUIManagerExtensionClass() != null) ? Deployment.getGUIManagerExtensionClass().getMethod("evaluateEnumeratedValues",GUIManagerContext.class,String.class,Date.class,boolean.class) : null;
      }
    catch (NoSuchMethodException e)
      {
        throw new RuntimeException(e);
      }

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
    *  services - construct
    *
    *****************************************/

    journeyService = new JourneyService(bootstrapServers, "guimanager-journeyservice-" + apiProcessKey, journeyTopic, true);
    segmentationDimensionService = new SegmentationDimensionService(bootstrapServers, "guimanager-segmentationDimensionservice-" + apiProcessKey, segmentationDimensionTopic, true);
    pointService = new PointService(bootstrapServers, "guimanager-pointservice-" + apiProcessKey, pointTopic, true);
    offerService = new OfferService(bootstrapServers, "guimanager-offerservice-" + apiProcessKey, offerTopic, true);
    reportService = new ReportService(bootstrapServers, "guimanager-reportservice-" + apiProcessKey, reportTopic, true);
    paymentMeanService = new PaymentMeanService(bootstrapServers, "guimanager-paymentmeanservice-" + apiProcessKey, paymentMeanTopic, true);
    scoringStrategyService = new ScoringStrategyService(bootstrapServers, "guimanager-scoringstrategyservice-" + apiProcessKey, scoringStrategyTopic, true);
    presentationStrategyService = new PresentationStrategyService(bootstrapServers, "guimanager-presentationstrategyservice-" + apiProcessKey, presentationStrategyTopic, true);
    callingChannelService = new CallingChannelService(bootstrapServers, "guimanager-callingchannelservice-" + apiProcessKey, callingChannelTopic, true);
    salesChannelService = new SalesChannelService(bootstrapServers, "guimanager-saleschannelservice-" + apiProcessKey, salesChannelTopic, true);
    supplierService = new SupplierService(bootstrapServers, "guimanager-supplierservice-" + apiProcessKey, supplierTopic, true);
    productService = new ProductService(bootstrapServers, "guimanager-productservice-" + apiProcessKey, productTopic, true);
    catalogCharacteristicService = new CatalogCharacteristicService(bootstrapServers, "guimanager-catalogcharacteristicservice-" + apiProcessKey, catalogCharacteristicTopic, true);
    contactPolicyService = new ContactPolicyService(bootstrapServers, "guimanager-contactpolicieservice-" + apiProcessKey, contactPolicyTopic, true);
    journeyObjectiveService = new JourneyObjectiveService(bootstrapServers, "guimanager-journeyobjectiveservice-" + apiProcessKey, journeyObjectiveTopic, true);
    offerObjectiveService = new OfferObjectiveService(bootstrapServers, "guimanager-offerobjectiveservice-" + apiProcessKey, offerObjectiveTopic, true);
    productTypeService = new ProductTypeService(bootstrapServers, "guimanager-producttypeservice-" + apiProcessKey, productTypeTopic, true);
    ucgRuleService = new UCGRuleService(bootstrapServers,"guimanager-ucgruleservice-"+apiProcessKey,ucgRuleTopic,true);
    deliverableService = new DeliverableService(bootstrapServers, "guimanager-deliverableservice-" + apiProcessKey, deliverableTopic, true);
    tokenTypeService = new TokenTypeService(bootstrapServers, "guimanager-tokentypeservice-" + apiProcessKey, tokenTypeTopic, true);
    mailTemplateService = new MailTemplateService(bootstrapServers, "guimanager-mailtemplateservice-" + apiProcessKey, mailTemplateTopic, true);
    smsTemplateService = new SMSTemplateService(bootstrapServers, "guimanager-smstemplateservice-" + apiProcessKey, smsTemplateTopic, true);
    subscriberProfileService = new EngineSubscriberProfileService(subscriberProfileEndpoints);
    subscriberIDService = new SubscriberIDService(redisServer);
    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("guimanager-subscribergroupepoch", apiProcessKey, bootstrapServers, subscriberGroupEpochTopic, SubscriberGroupEpoch::unpack);
    deliverableSourceService = new DeliverableSourceService(bootstrapServers, "guimanager-deliverablesourceservice-" + apiProcessKey, deliverableSourceTopic);
    uploadedFileService = new UploadedFileService(bootstrapServers, "guimanager-uploadfileservice-" + apiProcessKey, uploadedFileTopic, true);
    targetService = new TargetService(bootstrapServers, "guimanager-targetservice-" + apiProcessKey, targetTopic, true);

    /*****************************************
    *
    *  Elasticsearch -- client
    *
    *****************************************/

    try
      {
        elasticsearch = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticsearchServerHost, elasticsearchServerPort, "http")));
      }
    catch (ElasticsearchException e)
      {
        throw new ServerRuntimeException("could not initialize elasticsearch client", e);
      }

    /*****************************************
    *
    *  clean payment means and deliverables (need to be done before initialProducts, ...)
    *
    *****************************************/

    Map<String, DeliveryManagerDeclaration> providersMap = new HashMap<String, DeliveryManagerDeclaration>();
    for(DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values()){
      CommodityType commodityType = CommodityType.fromExternalRepresentation(deliveryManager.getRequestClassName());
      if(commodityType != null){
        switch (commodityType) {
        case IN:
        case EMPTY:
          
          JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
          String providerID = (String) deliveryManagerJSON.get("providerID");
          providersMap.put(providerID, deliveryManager);
          
          break;
        default:
          break;
        }
      }
    }
    
    for(DeliveryManagerAccount deliveryManagerAccount : Deployment.getDeliveryManagerAccounts().values()){
      String providerID = deliveryManagerAccount.getProviderID();
      DeliveryManagerDeclaration deliveryManagerDeclaration = providersMap.get(providerID);
      if(deliveryManagerDeclaration == null){
        throw new ServerRuntimeException("Delivery manager accounts : could not retrieve provider with ID "+providerID);
      }
      
      //
      // remove all paymentMeans related to this provider
      //
      
      Collection<GUIManagedObject> paymentMeanList = paymentMeanService.getStoredPaymentMeans();
      for(GUIManagedObject paymentMeanObject : paymentMeanList){
        PaymentMean paymentMean = (PaymentMean) paymentMeanObject;
        if(paymentMean.getFulfillmentProviderID().equals(providerID)){
          paymentMeanService.removePaymentMean(paymentMean.getPaymentMeanID(), "0");
        }
      }
      
      //
      // remove all deliverables related to this provider
      //
      
      Collection<GUIManagedObject> deliverableList = deliverableService.getStoredDeliverables();
      for(GUIManagedObject deliverableObject : deliverableList){
        Deliverable deliverable = (Deliverable) deliverableObject;
        if(deliverable.getFulfillmentProviderID().equals(providerID)){
          deliverableService.removeDeliverable(deliverable.getDeliverableID(), "0");
        }
      }
      
      //
      // add new paymentMeans and new deliverables
      //
      long epoch = epochServer.getKey();
      List<Account> accounts = deliveryManagerAccount.getAccounts();
      for (Account account : accounts) {
        if(account.getDebitable()){
          Map<String, Object> paymentMeanMap = new HashMap<String, Object>();
          paymentMeanMap.put("id", account.getAccountID());
          paymentMeanMap.put("fulfillmentProviderID", providerID);
          paymentMeanMap.put("commodityID", account.getAccountID());
          paymentMeanMap.put("name", account.getName());
          paymentMeanMap.put("display", account.getName());
          paymentMeanMap.put("active", true);
          paymentMeanMap.put("readOnly", true);
          try
          {
            PaymentMean paymentMean = new PaymentMean(JSONUtilities.encodeObject(paymentMeanMap), epoch, null);
            paymentMeanService.putPaymentMean(paymentMean, true, "0");
          } catch (GUIManagerException e)
          {
            throw new ServerRuntimeException("could not add paymentMean related to provider "+providerID+" (account "+account.getName()+")", e);
          }
        }
        if(account.getCreditable()){
          Map<String, Object> deliverableMap = new HashMap<String, Object>();
          deliverableMap.put("id", account.getAccountID());
          deliverableMap.put("fulfillmentProviderID", providerID);
          deliverableMap.put("commodityID", account.getAccountID());
          deliverableMap.put("name", account.getName());
          deliverableMap.put("display", account.getName());
          deliverableMap.put("active", true);
          deliverableMap.put("unitaryCost", 0);
          deliverableMap.put("readOnly", true);
          try
          {
            Deliverable deliverable = new Deliverable(JSONUtilities.encodeObject(deliverableMap), epoch, null);
            deliverableService.putDeliverable(deliverable, true, "0");
          } catch (GUIManagerException e)
          {
            throw new ServerRuntimeException("could not add deliverable related to provider "+providerID+" (account "+account.getName()+")", e);
          }
        }
      }
      
    }
    
    /*****************************************
    *
    *  services - initialize
    *
    *****************************************/

    //
    //  catalogCharacteristics
    //

    if (catalogCharacteristicService.getStoredCatalogCharacteristics().size() == 0)
      {
        try
          {
            JSONArray initialCatalogCharacteristicsJSONArray = Deployment.getInitialCatalogCharacteristicsJSONArray();
            for (int i=0; i<initialCatalogCharacteristicsJSONArray.size(); i++)
              {
                JSONObject catalogCharacteristicJSON = (JSONObject) initialCatalogCharacteristicsJSONArray.get(i);
                processPutCatalogCharacteristic("0", catalogCharacteristicJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }
      }

    //
    //  tokenTypes
    //

    if (tokenTypeService.getStoredTokenTypes().size() == 0)
      {
        try
          {
            JSONArray initialTokenTypesJSONArray = Deployment.getInitialTokenTypesJSONArray();
            for (int i=0; i<initialTokenTypesJSONArray.size(); i++)
              {
                JSONObject tokenTypeJSON = (JSONObject) initialTokenTypesJSONArray.get(i);
                processPutTokenType("0", tokenTypeJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }
      }
                
    //
    //  productTypes
    //

    if (productTypeService.getStoredProductTypes().size() == 0)
      {
        try
          {
            JSONArray initialProductTypesJSONArray = Deployment.getInitialProductTypesJSONArray();
            for (int i=0; i<initialProductTypesJSONArray.size(); i++)
              {
                JSONObject productTypeJSON = (JSONObject) initialProductTypesJSONArray.get(i);
                processPutProductType("0", productTypeJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }
      }
    
    //
    //  reports
    //

    if (reportService.getStoredReports().size() == 0)
      {
        try
          {
            JSONArray initialReportsJSONArray = Deployment.getInitialReportsJSONArray();
            for (int i=0; i<initialReportsJSONArray.size(); i++)
              {
                JSONObject reportJSON = (JSONObject) initialReportsJSONArray.get(i);
                processPutReport("0", reportJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }
      }

    //
    //  calling channels
    //

    if (callingChannelService.getStoredCallingChannels().size() == 0)
      {
        try
          {
            JSONArray initialCallingChannelsJSONArray = Deployment.getInitialCallingChannelsJSONArray();
            for (int i=0; i<initialCallingChannelsJSONArray.size(); i++)
              {
                JSONObject  callingChannelJSON = (JSONObject) initialCallingChannelsJSONArray.get(i);
                processPutCallingChannel("0", callingChannelJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }
      }

    //
    //  sales channels
    //

    if (salesChannelService.getStoredSalesChannels().size() == 0)
      {
        try
          {
            JSONArray initialSalesChannelsJSONArray = Deployment.getInitialSalesChannelsJSONArray();
            for (int i=0; i<initialSalesChannelsJSONArray.size(); i++)
              {
                JSONObject  salesChannelJSON = (JSONObject) initialSalesChannelsJSONArray.get(i);
                processPutSalesChannel("0", salesChannelJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }
      }

    //
    //  suppliers
    //

    if (supplierService.getStoredSuppliers().size() == 0)
      {
        try
          {
            JSONArray initialSuppliersJSONArray = Deployment.getInitialSuppliersJSONArray();
            for (int i=0; i<initialSuppliersJSONArray.size(); i++)
              {
                JSONObject supplierJSON = (JSONObject) initialSuppliersJSONArray.get(i);
                processPutSupplier("0", supplierJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }
      }

    //
    //  products
    //

    if (productService.getStoredProducts().size() == 0)
      {
        try
          {
            JSONArray initialProductsJSONArray = Deployment.getInitialProductsJSONArray();
            for (int i=0; i<initialProductsJSONArray.size(); i++)
              {
                JSONObject productJSON = (JSONObject) initialProductsJSONArray.get(i);
                processPutProduct("0", productJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }
      }

    //
    //  contactPolicies
    //

    if (contactPolicyService.getStoredContactPolicies().size() == 0)
      {
        try
          {
            JSONArray initialContactPoliciesJSONArray = Deployment.getInitialContactPoliciesJSONArray();
            for (int i=0; i<initialContactPoliciesJSONArray.size(); i++)
              {
                JSONObject contactPolicyJSON = (JSONObject) initialContactPoliciesJSONArray.get(i);
                processPutContactPolicy("0", contactPolicyJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }
      }

    //
    //  journeyObjectives
    //

    if (journeyObjectiveService.getStoredJourneyObjectives().size() == 0)
      {
        try
          {
            JSONArray initialJourneyObjectivesJSONArray = Deployment.getInitialJourneyObjectivesJSONArray();
            for (int i=0; i<initialJourneyObjectivesJSONArray.size(); i++)
              {
                JSONObject journeyObjectiveJSON = (JSONObject) initialJourneyObjectivesJSONArray.get(i);
                processPutJourneyObjective("0", journeyObjectiveJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }
      }

    //
    //  offerObjectives
    //

    if (offerObjectiveService.getStoredOfferObjectives().size() == 0)
      {
        try
          {
            JSONArray initialOfferObjectivesJSONArray = Deployment.getInitialOfferObjectivesJSONArray();
            for (int i=0; i<initialOfferObjectivesJSONArray.size(); i++)
              {
                JSONObject offerObjectiveJSON = (JSONObject) initialOfferObjectivesJSONArray.get(i);
                processPutOfferObjective("0", offerObjectiveJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }

      }

    //
    //  reports
    //

    if (reportService.getStoredReports().size() == 0)
      {
        try
          {
            JSONArray initialReportsJSONArray = Deployment.getInitialReportsJSONArray();
            for (int i=0; i<initialReportsJSONArray.size(); i++)
              {
                JSONObject reportJSON = (JSONObject) initialReportsJSONArray.get(i);
                processPutReport("0", reportJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }

      }

    //
    //  segmentationDimensions
    //

    if (segmentationDimensionService.getStoredSegmentationDimensions().size() == 0)
      {
        try
          {
            JSONArray initialSegmentationDimensionsJSONArray = Deployment.getInitialSegmentationDimensionsJSONArray();
            for (int i=0; i<initialSegmentationDimensionsJSONArray.size(); i++)
              {
                JSONObject segmentationDimensionJSON = (JSONObject) initialSegmentationDimensionsJSONArray.get(i);
                processPutSegmentationDimension("0", segmentationDimensionJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }
      }

    /*****************************************
    *
    *  simple profile dimensions
    *
    *****************************************/

    if (Deployment.getGenerateSimpleProfileDimensions())
      {
        //
        // remove all existing simple profile dimensions
        //

        for(GUIManagedObject dimensionObject : segmentationDimensionService.getStoredSegmentationDimensions())
          {
            if(dimensionObject instanceof SegmentationDimension)
              {
                SegmentationDimension dimension = (SegmentationDimension)dimensionObject;
                if(dimension.getIsSimpleProfileDimension())
                  {
                    log.debug("SimpleProfileDimension : removing dimension '" + dimension.getSegmentationDimensionName() + "'");
                    segmentationDimensionService.removeSegmentationDimension(dimension.getSegmentationDimensionID(), "0");
                  }
                else
                  {
                    log.debug("SimpleProfileDimension : dimension '"+ dimension.getSegmentationDimensionName() + "' is not a simpleProfile dimension, do Not remove");
                  }
              }
          }

        //
        // re-create simple profile dimensions (=> so we are sure that dimensions are in line with profile fields)
        //

        Date now = SystemTime.getCurrentTime();
        Map<String,CriterionField> profileCriterionFields = CriterionContext.Profile.getCriterionFields();
        for (CriterionField criterion : profileCriterionFields.values())
          {
            log.debug("SimpleProfileDimension : handling field '"+criterion.getName()+"' ...");
            List<JSONObject> availableValues = evaluateAvailableValues(criterion, now, false);
            if (availableValues != null && !availableValues.isEmpty())
              {
                //
                //  log
                //

                log.debug("     field '"+criterion.getName()+"' : field has availableValues => create a new dimension");

                //
                // create dimension
                //

                String dimensionID = "simple.subscriber." + criterion.getID();
                HashMap<String,Object> newSimpleProfileDimensionJSON = new HashMap<String,Object>();
                newSimpleProfileDimensionJSON.put("isSimpleProfileDimension", true);
                newSimpleProfileDimensionJSON.put("id", dimensionID);
                newSimpleProfileDimensionJSON.put("name", criterion.getName());
                newSimpleProfileDimensionJSON.put("display", criterion.getDisplay());
                newSimpleProfileDimensionJSON.put("description", "Simple profile criteria (from "+criterion.getName()+")");
                newSimpleProfileDimensionJSON.put("targetingType", SegmentationDimensionTargetingType.ELIGIBILITY.getExternalRepresentation());
                newSimpleProfileDimensionJSON.put("readOnly", Boolean.TRUE);

                //
                // create all segments of this dimension
                //

                ArrayList<Object> newSimpleProfileDimensionSegments = new ArrayList<Object>();
                for (JSONObject availableValue : availableValues)
                  {
                    HashMap<String,Object> segmentJSON = new HashMap<String,Object>();
                    ArrayList<Object> segmentProfileCriteriaList = new ArrayList<Object>();
                    switch (criterion.getFieldDataType())
                      {
                        case StringCriterion:

                          //
                          // create a segment
                          //

                          String stringValueID = JSONUtilities.decodeString(availableValue, "id", true);
                          String stringValueDisplay = JSONUtilities.decodeString(availableValue, "display", true);
                          segmentJSON.put("id", dimensionID + "." + stringValueID);
                          segmentJSON.put("name", normalizeSegmentName(criterion.getName() + "." + stringValueDisplay));
                          if (!newSimpleProfileDimensionSegments.isEmpty())
                            {
                              // first element is the default value => fill criteria for all values except the first
                              HashMap<String,Object> segmentProfileCriteria = new HashMap<String,Object> ();
                              segmentProfileCriteria.put("criterionField", criterion.getName());
                              segmentProfileCriteria.put("criterionOperator", CriterionOperator.EqualOperator.getExternalRepresentation());
                              HashMap<String,Object> argument = new HashMap<String,Object> ();
                              argument.put("expression", "'"+stringValueID+"'");
                              segmentProfileCriteria.put("argument", JSONUtilities.encodeObject(argument));
                              segmentProfileCriteriaList.add(JSONUtilities.encodeObject(segmentProfileCriteria));
                            }
                          segmentJSON.put("profileCriteria", JSONUtilities.encodeArray(segmentProfileCriteriaList));
                          newSimpleProfileDimensionSegments.add((newSimpleProfileDimensionSegments.isEmpty() ? 0 : newSimpleProfileDimensionSegments.size() - 1), JSONUtilities.encodeObject(segmentJSON)); // first element is the default value => need to be the last element of segments list
                          break;

                        case BooleanCriterion:

                          //
                          // create a segment
                          //

                          boolean booleanValueID = JSONUtilities.decodeBoolean(availableValue, "id", true);
                          String booleanValueDisplay = JSONUtilities.decodeString(availableValue, "display", true);
                          segmentJSON.put("id", dimensionID + "." + booleanValueID);
                          segmentJSON.put("name", normalizeSegmentName(criterion.getName() + "." + booleanValueDisplay));
                          if (!newSimpleProfileDimensionSegments.isEmpty())
                            {
                              // first element is the default value => fill criteria for all values except the first
                              HashMap<String,Object> segmentProfileCriteria = new HashMap<String,Object> ();
                              segmentProfileCriteria.put("criterionField", criterion.getName());
                              segmentProfileCriteria.put("criterionOperator", CriterionOperator.EqualOperator.getExternalRepresentation());
                              HashMap<String,Object> argument = new HashMap<String,Object> ();
                              argument.put("expression", Boolean.toString(booleanValueID));
                              segmentProfileCriteria.put("argument", JSONUtilities.encodeObject(argument));
                              segmentProfileCriteriaList.add(JSONUtilities.encodeObject(segmentProfileCriteria));
                            }
                          segmentJSON.put("profileCriteria", JSONUtilities.encodeArray(segmentProfileCriteriaList));
                          newSimpleProfileDimensionSegments.add((newSimpleProfileDimensionSegments.isEmpty() ? 0 : newSimpleProfileDimensionSegments.size() - 1), JSONUtilities.encodeObject(segmentJSON)); // first element is the default value => need to be the last element of segments list
                          break;

                        case IntegerCriterion:

                          //
                          // create a segment
                          //

                          int intValueID = JSONUtilities.decodeInteger(availableValue, "id", true);
                          String intValueDisplay = JSONUtilities.decodeString(availableValue, "display", true);
                          segmentJSON.put("id", dimensionID + "." + intValueID);
                          segmentJSON.put("name", normalizeSegmentName(criterion.getName() + "." + intValueDisplay));
                          if (!newSimpleProfileDimensionSegments.isEmpty())
                            {
                              // first element is the default value => fill criteria for all values except the first
                              HashMap<String,Object> segmentProfileCriteria = new HashMap<String,Object> ();
                              segmentProfileCriteria.put("criterionField", criterion.getName());
                              segmentProfileCriteria.put("criterionOperator", CriterionOperator.EqualOperator.getExternalRepresentation());
                              HashMap<String,Object> argument = new HashMap<String,Object> ();
                              argument.put("expression", ""+intValueID);
                              segmentProfileCriteria.put("argument", JSONUtilities.encodeObject(argument));
                              segmentProfileCriteriaList.add(JSONUtilities.encodeObject(segmentProfileCriteria));
                            }
                          segmentJSON.put("profileCriteria", JSONUtilities.encodeArray(segmentProfileCriteriaList));
                          newSimpleProfileDimensionSegments.add((newSimpleProfileDimensionSegments.isEmpty() ? 0 : newSimpleProfileDimensionSegments.size() - 1), JSONUtilities.encodeObject(segmentJSON)); // first element is the default value => need to be the last element of segments list
                          break;

                        default:
                          //DoubleCriterion
                          //DateCriterion
                          break;
                      }
                  }

                newSimpleProfileDimensionJSON.put("segments", JSONUtilities.encodeArray(newSimpleProfileDimensionSegments));

                JSONObject newSimpleProfileDimension = JSONUtilities.encodeObject(newSimpleProfileDimensionJSON);
                processPutSegmentationDimension("0", newSimpleProfileDimension);
                log.debug("     field '"+criterion.getName()+"' : field has availableValues => new dimension CREATED "+newSimpleProfileDimension);
              }
            else
              {
                log.debug("     field '"+criterion.getName()+"' : field DO NOT have availableValues => NO dimension created");
              }
          }
      }

    /*****************************************
    *
    *  services - start
    *
    *****************************************/

    journeyService.start();
    segmentationDimensionService.start();
    pointService.start();
    offerService.start();
    reportService.start();
    paymentMeanService.start();
    scoringStrategyService.start();
    presentationStrategyService.start();
    callingChannelService.start();
    salesChannelService.start();
    supplierService.start();
    productService.start();
    catalogCharacteristicService.start();
    contactPolicyService.start();
    journeyObjectiveService.start();
    offerObjectiveService.start();
    productTypeService.start();
    ucgRuleService.start();
    deliverableService.start();
    tokenTypeService.start();
    mailTemplateService.start();
    smsTemplateService.start();
    subscriberProfileService.start();
    deliverableSourceService.start();
    uploadedFileService.start();
    targetService.start();

    /*****************************************
    *
    *  REST interface -- server and handlers
    *
    *****************************************/

    try
      {
        InetSocketAddress addr = new InetSocketAddress(apiRestPort);
        restServer = HttpServer.create(addr, 0);
        restServer.createContext("/nglm-guimanager/getStaticConfiguration", new APISimpleHandler(API.getStaticConfiguration));
        restServer.createContext("/nglm-guimanager/getSupportedLanguages", new APISimpleHandler(API.getSupportedLanguages));
        restServer.createContext("/nglm-guimanager/getSupportedCurrencies", new APISimpleHandler(API.getSupportedCurrencies));
        restServer.createContext("/nglm-guimanager/getSupportedTimeUnits", new APISimpleHandler(API.getSupportedTimeUnits));
        restServer.createContext("/nglm-guimanager/getServiceTypes", new APISimpleHandler(API.getServiceTypes));
        restServer.createContext("/nglm-guimanager/getTouchPoints", new APISimpleHandler(API.getTouchPoints));
        restServer.createContext("/nglm-guimanager/getCallingChannelProperties", new APISimpleHandler(API.getCallingChannelProperties));
        restServer.createContext("/nglm-guimanager/getCatalogCharacteristicUnits", new APISimpleHandler(API.getCatalogCharacteristicUnits));
        restServer.createContext("/nglm-guimanager/getSupportedDataTypes", new APISimpleHandler(API.getSupportedDataTypes));
        restServer.createContext("/nglm-guimanager/getSupportedEvents", new APISimpleHandler(API.getSupportedEvents));
        restServer.createContext("/nglm-guimanager/getSupportedTargetingTypes", new APISimpleHandler(API.getSupportedTargetingTypes));
        restServer.createContext("/nglm-guimanager/getProfileCriterionFields", new APISimpleHandler(API.getProfileCriterionFields));
        restServer.createContext("/nglm-guimanager/getProfileCriterionFieldIDs", new APISimpleHandler(API.getProfileCriterionFieldIDs));
        restServer.createContext("/nglm-guimanager/getProfileCriterionField", new APISimpleHandler(API.getProfileCriterionField));
        restServer.createContext("/nglm-guimanager/getPresentationCriterionFields", new APISimpleHandler(API.getPresentationCriterionFields));
        restServer.createContext("/nglm-guimanager/getPresentationCriterionFieldIDs", new APISimpleHandler(API.getPresentationCriterionFieldIDs));
        restServer.createContext("/nglm-guimanager/getPresentationCriterionField", new APISimpleHandler(API.getPresentationCriterionField));
        restServer.createContext("/nglm-guimanager/getJourneyCriterionFields", new APISimpleHandler(API.getJourneyCriterionFields));
        restServer.createContext("/nglm-guimanager/getJourneyCriterionFieldIDs", new APISimpleHandler(API.getJourneyCriterionFieldIDs));
        restServer.createContext("/nglm-guimanager/getJourneyCriterionField", new APISimpleHandler(API.getJourneyCriterionField));
        restServer.createContext("/nglm-guimanager/getOfferCategories", new APISimpleHandler(API.getOfferCategories));
        restServer.createContext("/nglm-guimanager/getOfferTypes", new APISimpleHandler(API.getOfferTypes));
        restServer.createContext("/nglm-guimanager/getOfferOptimizationAlgorithms", new APISimpleHandler(API.getOfferOptimizationAlgorithms));
        restServer.createContext("/nglm-guimanager/getNodeTypes", new APISimpleHandler(API.getNodeTypes));
        restServer.createContext("/nglm-guimanager/getJourneyToolbox", new APISimpleHandler(API.getJourneyToolbox));
        restServer.createContext("/nglm-guimanager/getJourneyList", new APISimpleHandler(API.getJourneyList));
        restServer.createContext("/nglm-guimanager/getJourneySummaryList", new APISimpleHandler(API.getJourneySummaryList));
        restServer.createContext("/nglm-guimanager/getJourney", new APISimpleHandler(API.getJourney));
        restServer.createContext("/nglm-guimanager/putJourney", new APISimpleHandler(API.putJourney));
        restServer.createContext("/nglm-guimanager/removeJourney", new APISimpleHandler(API.removeJourney));
        restServer.createContext("/nglm-guimanager/startJourney", new APISimpleHandler(API.startJourney));
        restServer.createContext("/nglm-guimanager/stopJourney", new APISimpleHandler(API.stopJourney));
        restServer.createContext("/nglm-guimanager/getCampaignToolbox", new APISimpleHandler(API.getCampaignToolbox));
        restServer.createContext("/nglm-guimanager/getCampaignList", new APISimpleHandler(API.getCampaignList));
        restServer.createContext("/nglm-guimanager/getCampaignSummaryList", new APISimpleHandler(API.getCampaignSummaryList));
        restServer.createContext("/nglm-guimanager/getCampaign", new APISimpleHandler(API.getCampaign));
        restServer.createContext("/nglm-guimanager/putCampaign", new APISimpleHandler(API.putCampaign));
        restServer.createContext("/nglm-guimanager/removeCampaign", new APISimpleHandler(API.removeCampaign));
        restServer.createContext("/nglm-guimanager/startCampaign", new APISimpleHandler(API.startCampaign));
        restServer.createContext("/nglm-guimanager/stopCampaign", new APISimpleHandler(API.stopCampaign));
        restServer.createContext("/nglm-guimanager/getSegmentationDimensionList", new APISimpleHandler(API.getSegmentationDimensionList));
        restServer.createContext("/nglm-guimanager/getSegmentationDimensionSummaryList", new APISimpleHandler(API.getSegmentationDimensionSummaryList));
        restServer.createContext("/nglm-guimanager/getSegmentationDimension", new APISimpleHandler(API.getSegmentationDimension));
        restServer.createContext("/nglm-guimanager/putSegmentationDimension", new APISimpleHandler(API.putSegmentationDimension));
        restServer.createContext("/nglm-guimanager/removeSegmentationDimension", new APISimpleHandler(API.removeSegmentationDimension));
        restServer.createContext("/nglm-guimanager/countBySegmentationRanges", new APISimpleHandler(API.countBySegmentationRanges));
        restServer.createContext("/nglm-guimanager/evaluateProfileCriteria", new APISimpleHandler(API.evaluateProfileCriteria));
        restServer.createContext("/nglm-guimanager/getUCGDimensionSummaryList", new APISimpleHandler(API.getUCGDimensionSummaryList));
        restServer.createContext("/nglm-guimanager/getPointList", new APISimpleHandler(API.getPointList));
        restServer.createContext("/nglm-guimanager/getPointSummaryList", new APISimpleHandler(API.getPointSummaryList));
        restServer.createContext("/nglm-guimanager/getPoint", new APISimpleHandler(API.getPoint));
        restServer.createContext("/nglm-guimanager/putPoint", new APISimpleHandler(API.putPoint));
        restServer.createContext("/nglm-guimanager/removePoint", new APISimpleHandler(API.removePoint));
        restServer.createContext("/nglm-guimanager/getOfferList", new APISimpleHandler(API.getOfferList));
        restServer.createContext("/nglm-guimanager/getOfferSummaryList", new APISimpleHandler(API.getOfferSummaryList));
        restServer.createContext("/nglm-guimanager/getOffer", new APISimpleHandler(API.getOffer));
        restServer.createContext("/nglm-guimanager/putOffer", new APISimpleHandler(API.putOffer));
        restServer.createContext("/nglm-guimanager/removeOffer", new APISimpleHandler(API.removeOffer));
        restServer.createContext("/nglm-guimanager/getPresentationStrategyList", new APISimpleHandler(API.getPresentationStrategyList));
        restServer.createContext("/nglm-guimanager/getReportGlobalConfiguration", new APISimpleHandler(API.getReportGlobalConfiguration));
        restServer.createContext("/nglm-guimanager/getReportList", new APISimpleHandler(API.getReportList));
        restServer.createContext("/nglm-guimanager/putReport", new APISimpleHandler(API.putReport));
        restServer.createContext("/nglm-guimanager/launchReport", new APISimpleHandler(API.launchReport));
        restServer.createContext("/nglm-guimanager/downloadReport", new APIComplexHandler(API.downloadReport));
        restServer.createContext("/nglm-guimanager/getPresentationStrategySummaryList", new APISimpleHandler(API.getPresentationStrategySummaryList));
        restServer.createContext("/nglm-guimanager/getPresentationStrategy", new APISimpleHandler(API.getPresentationStrategy));
        restServer.createContext("/nglm-guimanager/putPresentationStrategy", new APISimpleHandler(API.putPresentationStrategy));
        restServer.createContext("/nglm-guimanager/removePresentationStrategy", new APISimpleHandler(API.removePresentationStrategy));
        restServer.createContext("/nglm-guimanager/getScoringStrategyList", new APISimpleHandler(API.getScoringStrategyList));
        restServer.createContext("/nglm-guimanager/getScoringStrategySummaryList", new APISimpleHandler(API.getScoringStrategySummaryList));
        restServer.createContext("/nglm-guimanager/getScoringStrategy", new APISimpleHandler(API.getScoringStrategy));
        restServer.createContext("/nglm-guimanager/putScoringStrategy", new APISimpleHandler(API.putScoringStrategy));
        restServer.createContext("/nglm-guimanager/removeScoringStrategy", new APISimpleHandler(API.removeScoringStrategy));
        restServer.createContext("/nglm-guimanager/getCallingChannelList", new APISimpleHandler(API.getCallingChannelList));
        restServer.createContext("/nglm-guimanager/getCallingChannelSummaryList", new APISimpleHandler(API.getCallingChannelSummaryList));
        restServer.createContext("/nglm-guimanager/getCallingChannel", new APISimpleHandler(API.getCallingChannel));
        restServer.createContext("/nglm-guimanager/putCallingChannel", new APISimpleHandler(API.putCallingChannel));
        restServer.createContext("/nglm-guimanager/removeCallingChannel", new APISimpleHandler(API.removeCallingChannel));
        restServer.createContext("/nglm-guimanager/getSalesChannelList", new APISimpleHandler(API.getSalesChannelList));
        restServer.createContext("/nglm-guimanager/getSalesChannelSummaryList", new APISimpleHandler(API.getSalesChannelSummaryList));
        restServer.createContext("/nglm-guimanager/getSalesChannel", new APISimpleHandler(API.getSalesChannel));
        restServer.createContext("/nglm-guimanager/putSalesChannel", new APISimpleHandler(API.putSalesChannel));
        restServer.createContext("/nglm-guimanager/removeSalesChannel", new APISimpleHandler(API.removeSalesChannel));
        restServer.createContext("/nglm-guimanager/getSupplierList", new APISimpleHandler(API.getSupplierList));
        restServer.createContext("/nglm-guimanager/getSupplierSummaryList", new APISimpleHandler(API.getSupplierSummaryList));
        restServer.createContext("/nglm-guimanager/getSupplier", new APISimpleHandler(API.getSupplier));
        restServer.createContext("/nglm-guimanager/putSupplier", new APISimpleHandler(API.putSupplier));
        restServer.createContext("/nglm-guimanager/removeSupplier", new APISimpleHandler(API.removeSupplier));
        restServer.createContext("/nglm-guimanager/getProductList", new APISimpleHandler(API.getProductList));
        restServer.createContext("/nglm-guimanager/getProductSummaryList", new APISimpleHandler(API.getProductSummaryList));
        restServer.createContext("/nglm-guimanager/getProduct", new APISimpleHandler(API.getProduct));
        restServer.createContext("/nglm-guimanager/putProduct", new APISimpleHandler(API.putProduct));
        restServer.createContext("/nglm-guimanager/removeProduct", new APISimpleHandler(API.removeProduct));
        restServer.createContext("/nglm-guimanager/getCatalogCharacteristicList", new APISimpleHandler(API.getCatalogCharacteristicList));
        restServer.createContext("/nglm-guimanager/getCatalogCharacteristicSummaryList", new APISimpleHandler(API.getCatalogCharacteristicSummaryList));
        restServer.createContext("/nglm-guimanager/getCatalogCharacteristic", new APISimpleHandler(API.getCatalogCharacteristic));
        restServer.createContext("/nglm-guimanager/putCatalogCharacteristic", new APISimpleHandler(API.putCatalogCharacteristic));
        restServer.createContext("/nglm-guimanager/removeCatalogCharacteristic", new APISimpleHandler(API.removeCatalogCharacteristic));
        restServer.createContext("/nglm-guimanager/getContactPolicyList", new APISimpleHandler(API.getContactPolicyList));
        restServer.createContext("/nglm-guimanager/getContactPolicySummaryList", new APISimpleHandler(API.getContactPolicySummaryList));
        restServer.createContext("/nglm-guimanager/getContactPolicy", new APISimpleHandler(API.getContactPolicy));
        restServer.createContext("/nglm-guimanager/putContactPolicy", new APISimpleHandler(API.putContactPolicy));
        restServer.createContext("/nglm-guimanager/removeContactPolicy", new APISimpleHandler(API.removeContactPolicy));
        restServer.createContext("/nglm-guimanager/getJourneyObjectiveList", new APISimpleHandler(API.getJourneyObjectiveList));
        restServer.createContext("/nglm-guimanager/getJourneyObjectiveSummaryList", new APISimpleHandler(API.getJourneyObjectiveSummaryList));
        restServer.createContext("/nglm-guimanager/getJourneyObjective", new APISimpleHandler(API.getJourneyObjective));
        restServer.createContext("/nglm-guimanager/putJourneyObjective", new APISimpleHandler(API.putJourneyObjective));
        restServer.createContext("/nglm-guimanager/removeJourneyObjective", new APISimpleHandler(API.removeJourneyObjective));
        restServer.createContext("/nglm-guimanager/getOfferObjectiveList", new APISimpleHandler(API.getOfferObjectiveList));
        restServer.createContext("/nglm-guimanager/getOfferObjectiveSummaryList", new APISimpleHandler(API.getOfferObjectiveSummaryList));
        restServer.createContext("/nglm-guimanager/getOfferObjective", new APISimpleHandler(API.getOfferObjective));
        restServer.createContext("/nglm-guimanager/putOfferObjective", new APISimpleHandler(API.putOfferObjective));
        restServer.createContext("/nglm-guimanager/removeOfferObjective", new APISimpleHandler(API.removeOfferObjective));
        restServer.createContext("/nglm-guimanager/getProductTypeList", new APISimpleHandler(API.getProductTypeList));
        restServer.createContext("/nglm-guimanager/getProductTypeSummaryList", new APISimpleHandler(API.getProductTypeSummaryList));
        restServer.createContext("/nglm-guimanager/getProductType", new APISimpleHandler(API.getProductType));
        restServer.createContext("/nglm-guimanager/putProductType", new APISimpleHandler(API.putProductType));
        restServer.createContext("/nglm-guimanager/removeProductType", new APISimpleHandler(API.removeProductType));
        restServer.createContext("/nglm-guimanager/getUCGRuleList", new APISimpleHandler(API.getUCGRuleList));
        restServer.createContext("/nglm-guimanager/getUCGRuleSummaryList",new APISimpleHandler(API.getUCGRuleSummaryList));
        restServer.createContext("/nglm-guimanager/getUCGRule", new APISimpleHandler(API.getUCGRule));
        restServer.createContext("/nglm-guimanager/putUCGRule", new APISimpleHandler(API.putUCGRule));
        restServer.createContext("/nglm-guimanager/removeUCGRule", new APISimpleHandler(API.removeUCGRule));
        restServer.createContext("/nglm-guimanager/getDeliverableList", new APISimpleHandler(API.getDeliverableList));
        restServer.createContext("/nglm-guimanager/getDeliverableSummaryList", new APISimpleHandler(API.getDeliverableSummaryList));
        restServer.createContext("/nglm-guimanager/getDeliverable", new APISimpleHandler(API.getDeliverable));
        restServer.createContext("/nglm-guimanager/getDeliverableByName", new APISimpleHandler(API.getDeliverableByName));
        restServer.createContext("/nglm-guimanager/getTokenTypeList", new APISimpleHandler(API.getTokenTypeList));
        restServer.createContext("/nglm-guimanager/getTokenTypeSummaryList", new APISimpleHandler(API.getTokenTypeSummaryList));
        restServer.createContext("/nglm-guimanager/putTokenType", new APISimpleHandler(API.putTokenType));
        restServer.createContext("/nglm-guimanager/getTokenType", new APISimpleHandler(API.getTokenType));
        restServer.createContext("/nglm-guimanager/removeTokenType", new APISimpleHandler(API.removeTokenType));
        restServer.createContext("/nglm-guimanager/getTokenCodesFormats", new APISimpleHandler(API.getTokenCodesFormats));
        restServer.createContext("/nglm-guimanager/getMailTemplateList", new APISimpleHandler(API.getMailTemplateList));
        restServer.createContext("/nglm-guimanager/getMailTemplateSummaryList", new APISimpleHandler(API.getMailTemplateSummaryList));
        restServer.createContext("/nglm-guimanager/getMailTemplate", new APISimpleHandler(API.getMailTemplate));
        restServer.createContext("/nglm-guimanager/putMailTemplate", new APISimpleHandler(API.putMailTemplate));
        restServer.createContext("/nglm-guimanager/removeMailTemplate", new APISimpleHandler(API.removeMailTemplate));
        restServer.createContext("/nglm-guimanager/getSMSTemplateList", new APISimpleHandler(API.getSMSTemplateList));
        restServer.createContext("/nglm-guimanager/getSMSTemplateSummaryList", new APISimpleHandler(API.getSMSTemplateSummaryList));
        restServer.createContext("/nglm-guimanager/getSMSTemplate", new APISimpleHandler(API.getSMSTemplate));
        restServer.createContext("/nglm-guimanager/putSMSTemplate", new APISimpleHandler(API.putSMSTemplate));
        restServer.createContext("/nglm-guimanager/removeSMSTemplate", new APISimpleHandler(API.removeSMSTemplate));
        restServer.createContext("/nglm-guimanager/getFulfillmentProviders", new APISimpleHandler(API.getFulfillmentProviders));
        restServer.createContext("/nglm-guimanager/getPaymentMeans", new APISimpleHandler(API.getPaymentMeans));
        restServer.createContext("/nglm-guimanager/getPaymentMeanList", new APISimpleHandler(API.getPaymentMeanList));
        restServer.createContext("/nglm-guimanager/getPaymentMeanSummaryList", new APISimpleHandler(API.getPaymentMeanSummaryList));
        restServer.createContext("/nglm-guimanager/getPaymentMean", new APISimpleHandler(API.getPaymentMean));
        restServer.createContext("/nglm-guimanager/putPaymentMean", new APISimpleHandler(API.putPaymentMean));
        restServer.createContext("/nglm-guimanager/removePaymentMean", new APISimpleHandler(API.removePaymentMean));
        restServer.createContext("/nglm-guimanager/getDashboardCounts", new APISimpleHandler(API.getDashboardCounts));
        restServer.createContext("/nglm-guimanager/getCustomer", new APISimpleHandler(API.getCustomer));
        restServer.createContext("/nglm-guimanager/getCustomerMetaData", new APISimpleHandler(API.getCustomerMetaData));
        restServer.createContext("/nglm-guimanager/getCustomerActivityByDateRange", new APISimpleHandler(API.getCustomerActivityByDateRange));
        restServer.createContext("/nglm-guimanager/getCustomerBDRs", new APISimpleHandler(API.getCustomerBDRs));
        restServer.createContext("/nglm-guimanager/getCustomerODRs", new APISimpleHandler(API.getCustomerODRs));
        restServer.createContext("/nglm-guimanager/getCustomerMessages", new APISimpleHandler(API.getCustomerMessages));
        restServer.createContext("/nglm-guimanager/getCustomerJourneys", new APISimpleHandler(API.getCustomerJourneys));
        restServer.createContext("/nglm-guimanager/getCustomerCampaigns", new APISimpleHandler(API.getCustomerCampaigns));
        restServer.createContext("/nglm-guimanager/refreshUCG", new APISimpleHandler(API.refreshUCG));
        restServer.createContext("/nglm-guimanager/getUploadedFileList", new APISimpleHandler(API.getUploadedFileList));
        restServer.createContext("/nglm-guimanager/getUploadedFileSummaryList", new APISimpleHandler(API.getUploadedFileSummaryList));
        restServer.createContext("/nglm-guimanager/removeUploadedFile", new APISimpleHandler(API.removeUploadedFile));
        restServer.createContext("/nglm-guimanager/putUploadedFile", new APIComplexHandler(API.putUploadedFile));
        restServer.createContext("/nglm-guimanager/getCustomerAlternateIDs", new APISimpleHandler(API.getCustomerAlternateIDs));
        restServer.createContext("/nglm-guimanager/getCustomerAvailableCampaigns", new APISimpleHandler(API.getCustomerAvailableCampaigns));
        restServer.createContext("/nglm-guimanager/getTargetList", new APISimpleHandler(API.getTargetList));
        restServer.createContext("/nglm-guimanager/getTargetSummaryList", new APISimpleHandler(API.getTargetSummaryList));
        restServer.createContext("/nglm-guimanager/putTarget", new APISimpleHandler(API.putTarget));
        restServer.createContext("/nglm-guimanager/getTarget", new APISimpleHandler(API.getTarget));
        restServer.createContext("/nglm-guimanager/removeTarget", new APISimpleHandler(API.removeTarget));
        restServer.createContext("/nglm-guimanager/updateCustomer", new APISimpleHandler(API.updateCustomer));
        restServer.setExecutor(Executors.newFixedThreadPool(10));
        restServer.start();
      }
    catch (IOException e)
      {
        throw new ServerRuntimeException("could not initialize REST server", e);
      }

    /*****************************************
    *
    *  context
    *
    *****************************************/

    guiManagerContext = new GUIManagerContext(journeyService, segmentationDimensionService, pointService, offerService, reportService, paymentMeanService, scoringStrategyService, presentationStrategyService, callingChannelService, salesChannelService, supplierService, productService, catalogCharacteristicService, contactPolicyService, journeyObjectiveService, offerObjectiveService, productTypeService, ucgRuleService, deliverableService, tokenTypeService, mailTemplateService, smsTemplateService, subscriberProfileService, subscriberIDService, deliverableSourceService, uploadedFileService, targetService);

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/

    NGLMRuntime.addShutdownHook(new ShutdownHook(kafkaProducer, restServer, journeyService, segmentationDimensionService, pointService, offerService, scoringStrategyService, presentationStrategyService, callingChannelService, salesChannelService, supplierService, productService, catalogCharacteristicService, contactPolicyService, journeyObjectiveService, offerObjectiveService, productTypeService, ucgRuleService, deliverableService, tokenTypeService, subscriberProfileService, subscriberIDService, subscriberGroupEpochReader, deliverableSourceService, reportService, mailTemplateService, smsTemplateService, uploadedFileService, targetService));

    /*****************************************
    *
    *  log restServerStarted
    *
    *****************************************/

    log.info("main restServerStarted");
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
    private JourneyService journeyService;
    private SegmentationDimensionService segmentationDimensionService;
    private PointService pointService;
    private OfferService offerService;
    private ReportService reportService;
    private ScoringStrategyService scoringStrategyService;
    private PresentationStrategyService presentationStrategyService;
    private CallingChannelService callingChannelService;
    private SalesChannelService salesChannelService;
    private SupplierService supplierService;
    private ProductService productService;
    private CatalogCharacteristicService catalogCharacteristicService;
    private ContactPolicyService contactPolicyService;
    private JourneyObjectiveService journeyObjectiveService;
    private OfferObjectiveService offerObjectiveService;
    private ProductTypeService productTypeService;
    private UCGRuleService ucgRuleService;
    private DeliverableService deliverableService;
    private TokenTypeService tokenTypeService;
    private MailTemplateService mailTemplateService;
    private SMSTemplateService smsTemplateService;
    private SubscriberProfileService subscriberProfileService;
    private SubscriberIDService subscriberIDService;
    private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
    private DeliverableSourceService deliverableSourceService;
    private UploadedFileService uploadedFileService;
    private TargetService targetService;

    //
    //  constructor
    //

    private ShutdownHook(KafkaProducer<byte[], byte[]> kafkaProducer, HttpServer restServer, JourneyService journeyService, SegmentationDimensionService segmentationDimensionService, PointService pointService, OfferService offerService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, CallingChannelService callingChannelService, SalesChannelService salesChannelService, SupplierService supplierService, ProductService productService, CatalogCharacteristicService catalogCharacteristicService, ContactPolicyService contactPolicyService, JourneyObjectiveService journeyObjectiveService, OfferObjectiveService offerObjectiveService, ProductTypeService productTypeService, UCGRuleService ucgRuleService, DeliverableService deliverableService, TokenTypeService tokenTypeService, SubscriberProfileService subscriberProfileService, SubscriberIDService subscriberIDService, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, DeliverableSourceService deliverableSourceService, ReportService reportService, MailTemplateService mailTemplateService, SMSTemplateService smsTemplateService, UploadedFileService uploadedFileService, TargetService targetService)
    {
      this.kafkaProducer = kafkaProducer;
      this.restServer = restServer;
      this.journeyService = journeyService;
      this.segmentationDimensionService = segmentationDimensionService;
      this.pointService = pointService;
      this.offerService = offerService;
      this.reportService = reportService;
      this.scoringStrategyService = scoringStrategyService;
      this.presentationStrategyService = presentationStrategyService;
      this.callingChannelService = callingChannelService;
      this.salesChannelService = salesChannelService;
      this.supplierService = supplierService;
      this.productService = productService;
      this.catalogCharacteristicService = catalogCharacteristicService;
      this.contactPolicyService = contactPolicyService;
      this.journeyObjectiveService = journeyObjectiveService;
      this.offerObjectiveService = offerObjectiveService;
      this.productTypeService = productTypeService;
      this.ucgRuleService = ucgRuleService;
      this.deliverableService = deliverableService;
      this.tokenTypeService = tokenTypeService;
      this.mailTemplateService = mailTemplateService;
      this.smsTemplateService = smsTemplateService;
      this.subscriberProfileService = subscriberProfileService;
      this.subscriberIDService = subscriberIDService;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
      this.deliverableSourceService = deliverableSourceService;
      this.uploadedFileService = uploadedFileService;
      this.targetService = targetService;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      //
      //  reference data reader
      //

      if (subscriberGroupEpochReader != null) subscriberGroupEpochReader.close();

      //
      //  services 
      //

      if (journeyService != null) journeyService.stop();
      if (segmentationDimensionService != null) segmentationDimensionService.stop();
      if (pointService != null) pointService.stop();
      if (offerService != null) offerService.stop();
      if (reportService != null) reportService.stop();
      if (scoringStrategyService != null) scoringStrategyService.stop();
      if (presentationStrategyService != null) presentationStrategyService.stop();
      if (callingChannelService != null) callingChannelService.stop();
      if (salesChannelService != null) salesChannelService.stop();
      if (supplierService != null) supplierService.stop();
      if (productService != null) productService.stop();
      if (catalogCharacteristicService != null) catalogCharacteristicService.stop();
      if (contactPolicyService != null) contactPolicyService.stop();
      if (journeyObjectiveService != null) journeyObjectiveService.stop();
      if (offerObjectiveService != null) offerObjectiveService.stop();
      if (productTypeService != null) productTypeService.stop();
      if (ucgRuleService != null) ucgRuleService.stop();
      if (deliverableService != null) deliverableService.stop();
      if (tokenTypeService != null) tokenTypeService.stop();
      if (mailTemplateService != null) mailTemplateService.stop();
      if (smsTemplateService != null) smsTemplateService.stop();
      if (subscriberProfileService != null) subscriberProfileService.stop();
      if (subscriberIDService != null) subscriberIDService.stop();
      if (deliverableSourceService != null) deliverableSourceService.stop();
      if (uploadedFileService != null) uploadedFileService.stop();
      if (targetService != null) targetService.stop();
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

  private synchronized void handleSimpleHandler(API api, HttpExchange exchange) throws IOException
  {
    try
      {
        /*****************************************
        *
        *  get the user
        *
        *****************************************/

        String userID = null;
        if (exchange.getRequestURI().getQuery() != null)
          {
            Pattern pattern = Pattern.compile("^(.*\\&user_id|user_id)=(.*?)(\\&.*$|$)");
            Matcher matcher = pattern.matcher(exchange.getRequestURI().getQuery());
            if (matcher.matches())
              {
                userID = matcher.group(2);
              }
          }

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
        jsonRoot.remove("apiVersion");

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
                case getStaticConfiguration:
                  jsonResponse = processGetStaticConfiguration(userID, jsonRoot);
                  break;

                case getSupportedLanguages:
                  jsonResponse = processGetSupportedLanguages(userID, jsonRoot);
                  break;

                case getSupportedCurrencies:
                  jsonResponse = processGetSupportedCurrencies(userID, jsonRoot);
                  break;

                case getSupportedTimeUnits:
                  jsonResponse = processGetSupportedTimeUnits(userID, jsonRoot);
                  break;

                case getServiceTypes:
                  jsonResponse = processGetServiceTypes(userID, jsonRoot);
                  break;

                case getTouchPoints:
                  jsonResponse = processGetTouchPoints(userID, jsonRoot);
                  break;

                case getCallingChannelProperties:
                  jsonResponse = processGetCallingChannelProperties(userID, jsonRoot);
                  break;

                case getCatalogCharacteristicUnits:
                  jsonResponse = processGetCatalogCharacteristicUnits(userID, jsonRoot);
                  break;

                case getSupportedDataTypes:
                  jsonResponse = processGetSupportedDataTypes(userID, jsonRoot);
                  break;

                case getSupportedEvents:
                  jsonResponse = processGetSupportedEvents(userID, jsonRoot);
                  break;

                case getSupportedTargetingTypes:
                  jsonResponse = processGetSupportedTargetingTypes(userID, jsonRoot);
                  break;

                case getProfileCriterionFields:
                  jsonResponse = processGetProfileCriterionFields(userID, jsonRoot);
                  break;

                case getProfileCriterionFieldIDs:
                  jsonResponse = processGetProfileCriterionFieldIDs(userID, jsonRoot);
                  break;

                case getProfileCriterionField:
                  jsonResponse = processGetProfileCriterionField(userID, jsonRoot);
                  break;

                case getPresentationCriterionFields:
                  jsonResponse = processGetPresentationCriterionFields(userID, jsonRoot);
                  break;

                case getPresentationCriterionFieldIDs:
                  jsonResponse = processGetPresentationCriterionFieldIDs(userID, jsonRoot);
                  break;

                case getPresentationCriterionField:
                  jsonResponse = processGetPresentationCriterionField(userID, jsonRoot);
                  break;

                case getJourneyCriterionFields:
                  jsonResponse = processGetJourneyCriterionFields(userID, jsonRoot);
                  break;

                case getJourneyCriterionFieldIDs:
                  jsonResponse = processGetJourneyCriterionFieldIDs(userID, jsonRoot);
                  break;

                case getJourneyCriterionField:
                  jsonResponse = processGetJourneyCriterionField(userID, jsonRoot);
                  break;

                case getOfferCategories:
                  jsonResponse = processGetOfferCategories(userID, jsonRoot);
                  break;

                case getOfferTypes:
                  jsonResponse = processGetOfferTypes(userID, jsonRoot);
                  break;

                case getOfferOptimizationAlgorithms:
                  jsonResponse = processGetOfferOptimizationAlgorithms(userID, jsonRoot);
                  break;

                case getNodeTypes:
                  jsonResponse = processGetNodeTypes(userID, jsonRoot);
                  break;

                case getJourneyToolbox:
                  jsonResponse = processGetJourneyToolbox(userID, jsonRoot);
                  break;

                case getJourneyList:
                  jsonResponse = processGetJourneyList(userID, jsonRoot, true);
                  break;

                case getJourneySummaryList:
                  jsonResponse = processGetJourneyList(userID, jsonRoot, false);
                  break;

                case getJourney:
                  jsonResponse = processGetJourney(userID, jsonRoot);
                  break;

                case putJourney:
                  jsonResponse = processPutJourney(userID, jsonRoot);
                  break;

                case removeJourney:
                  jsonResponse = processRemoveJourney(userID, jsonRoot);
                  break;

                case startJourney:
                  jsonResponse = processJourneySetActive(userID, jsonRoot, true);
                  break;

                case stopJourney:
                  jsonResponse = processJourneySetActive(userID, jsonRoot, false);
                  break;

                case getCampaignToolbox:
                  jsonResponse = processGetCampaignToolbox(userID, jsonRoot);
                  break;

                case getCampaignList:
                  jsonResponse = processGetCampaignList(userID, jsonRoot, true);
                  break;

                case getCampaignSummaryList:
                  jsonResponse = processGetCampaignList(userID, jsonRoot, false);
                  break;

                case getCampaign:
                  jsonResponse = processGetCampaign(userID, jsonRoot);
                  break;

                case putCampaign:
                  jsonResponse = processPutCampaign(userID, jsonRoot);
                  break;

                case removeCampaign:
                  jsonResponse = processRemoveCampaign(userID, jsonRoot);
                  break;

                case startCampaign:
                  jsonResponse = processCampaignSetActive(userID, jsonRoot, true);
                  break;

                case stopCampaign:
                  jsonResponse = processCampaignSetActive(userID, jsonRoot, false);
                  break;

                case getSegmentationDimensionList:
                  jsonResponse = processGetSegmentationDimensionList(userID, jsonRoot, true);
                  break;

                case getSegmentationDimensionSummaryList:
                  jsonResponse = processGetSegmentationDimensionList(userID, jsonRoot, false);
                  break;

                case getSegmentationDimension:
                  jsonResponse = processGetSegmentationDimension(userID, jsonRoot);
                  break;

                case putSegmentationDimension:
                  jsonResponse = processPutSegmentationDimension(userID, jsonRoot);
                  break;

                case removeSegmentationDimension:
                  jsonResponse = processRemoveSegmentationDimension(userID, jsonRoot);
                  break;

                case countBySegmentationRanges:
                  jsonResponse = processCountBySegmentationRanges(userID, jsonRoot);
                  break;

                case evaluateProfileCriteria:
                  jsonResponse = processEvaluateProfileCriteria(userID, jsonRoot);
                  break;

                case getUCGDimensionSummaryList:
                  jsonResponse = processGetUCGDimensionList(userID, jsonRoot, false);
                  break;

                case getPointList:
                  jsonResponse = processGetPointList(userID, jsonRoot, true);
                  break;

                case getPointSummaryList:
                  jsonResponse = processGetPointList(userID, jsonRoot, false);
                  break;

                case getPoint:
                  jsonResponse = processGetPoint(userID, jsonRoot);
                  break;

                case putPoint:
                  jsonResponse = processPutPoint(userID, jsonRoot);
                  break;

                case removePoint:
                  jsonResponse = processRemovePoint(userID, jsonRoot);
                  break;

                case getOfferList:
                  jsonResponse = processGetOfferList(userID, jsonRoot, true);
                  break;

                case getOfferSummaryList:
                  jsonResponse = processGetOfferList(userID, jsonRoot, false);
                  break;

                case getOffer:
                  jsonResponse = processGetOffer(userID, jsonRoot);
                  break;

                case putOffer:
                  jsonResponse = processPutOffer(userID, jsonRoot);
                  break;

                case removeOffer:
                  jsonResponse = processRemoveOffer(userID, jsonRoot);
                  break;

                case getReportGlobalConfiguration:
                  jsonResponse = processGetReportGlobalConfiguration(userID, jsonRoot);
                  break;

                case getReportList:
                  jsonResponse = processGetReportList(userID, jsonRoot);
                  break;

                case putReport:
                  jsonResponse = processPutReport(userID, jsonRoot);
                  break;

                case launchReport:
                  jsonResponse = processLaunchReport(userID, jsonRoot);
                  break;

                case getPresentationStrategyList:
                  jsonResponse = processGetPresentationStrategyList(userID, jsonRoot, true);
                  break;

                case getPresentationStrategySummaryList:
                  jsonResponse = processGetPresentationStrategyList(userID, jsonRoot, false);
                  break;

                case getPresentationStrategy:
                  jsonResponse = processGetPresentationStrategy(userID, jsonRoot);
                  break;

                case putPresentationStrategy:
                  jsonResponse = processPutPresentationStrategy(userID, jsonRoot);
                  break;

                case removePresentationStrategy:
                  jsonResponse = processRemovePresentationStrategy(userID, jsonRoot);
                  break;

                case getScoringStrategyList:
                  jsonResponse = processGetScoringStrategyList(userID, jsonRoot, true);
                  break;

                case getScoringStrategySummaryList:
                  jsonResponse = processGetScoringStrategyList(userID, jsonRoot, false);
                  break;

                case getScoringStrategy:
                  jsonResponse = processGetScoringStrategy(userID, jsonRoot);
                  break;

                case putScoringStrategy:
                  jsonResponse = processPutScoringStrategy(userID, jsonRoot);
                  break;

                case removeScoringStrategy:
                  jsonResponse = processRemoveScoringStrategy(userID, jsonRoot);
                  break;

                case getCallingChannelList:
                  jsonResponse = processGetCallingChannelList(userID, jsonRoot, true);
                  break;

                case getCallingChannelSummaryList:
                  jsonResponse = processGetCallingChannelList(userID, jsonRoot, false);
                  break;

                case getCallingChannel:
                  jsonResponse = processGetCallingChannel(userID, jsonRoot);
                  break;

                case putCallingChannel:
                  jsonResponse = processPutCallingChannel(userID, jsonRoot);
                  break;

                case removeCallingChannel:
                  jsonResponse = processRemoveCallingChannel(userID, jsonRoot);
                  break;

                case getSalesChannelList:
                  jsonResponse = processGetSalesChannelList(userID, jsonRoot, true);
                  break;

                case getSalesChannelSummaryList:
                  jsonResponse = processGetSalesChannelList(userID, jsonRoot, false);
                  break;

                case getSalesChannel:
                  jsonResponse = processGetSalesChannel(userID, jsonRoot);
                  break;

                case putSalesChannel:
                  jsonResponse = processPutSalesChannel(userID, jsonRoot);
                  break;

                case removeSalesChannel:
                  jsonResponse = processRemoveSalesChannel(userID, jsonRoot);
                  break;

                case getSupplierList:
                  jsonResponse = processGetSupplierList(userID, jsonRoot, true);
                  break;

                case getSupplierSummaryList:
                  jsonResponse = processGetSupplierList(userID, jsonRoot, false);
                  break;

                case getSupplier:
                  jsonResponse = processGetSupplier(userID, jsonRoot);
                  break;

                case putSupplier:
                  jsonResponse = processPutSupplier(userID, jsonRoot);
                  break;

                case removeSupplier:
                  jsonResponse = processRemoveSupplier(userID, jsonRoot);
                  break;

                case getProductList:
                  jsonResponse = processGetProductList(userID, jsonRoot, true);
                  break;

                case getProductSummaryList:
                  jsonResponse = processGetProductList(userID, jsonRoot, false);
                  break;

                case getProduct:
                  jsonResponse = processGetProduct(userID, jsonRoot);
                  break;

                case putProduct:
                  jsonResponse = processPutProduct(userID, jsonRoot);
                  break;

                case removeProduct:
                  jsonResponse = processRemoveProduct(userID, jsonRoot);
                  break;

                case getCatalogCharacteristicList:
                  jsonResponse = processGetCatalogCharacteristicList(userID, jsonRoot, true);
                  break;

                case getCatalogCharacteristicSummaryList:
                  jsonResponse = processGetCatalogCharacteristicList(userID, jsonRoot, false);
                  break;

                case getCatalogCharacteristic:
                  jsonResponse = processGetCatalogCharacteristic(userID, jsonRoot);
                  break;

                case putCatalogCharacteristic:
                  jsonResponse = processPutCatalogCharacteristic(userID, jsonRoot);
                  break;

                case removeCatalogCharacteristic:
                  jsonResponse = processRemoveCatalogCharacteristic(userID, jsonRoot);
                  break;

                case getContactPolicyList:
                  jsonResponse = processGetContactPolicyList(userID, jsonRoot, true);
                  break;

                case getContactPolicySummaryList:
                  jsonResponse = processGetContactPolicyList(userID, jsonRoot, false);
                  break;

                case getContactPolicy:
                  jsonResponse = processGetContactPolicy(userID, jsonRoot);
                  break;

                case putContactPolicy:
                  jsonResponse = processPutContactPolicy(userID, jsonRoot);
                  break;

                case removeContactPolicy:
                  jsonResponse = processRemoveContactPolicy(userID, jsonRoot);
                  break;

                case getJourneyObjectiveList:
                  jsonResponse = processGetJourneyObjectiveList(userID, jsonRoot, true);
                  break;

                case getJourneyObjectiveSummaryList:
                  jsonResponse = processGetJourneyObjectiveList(userID, jsonRoot, false);
                  break;

                case getJourneyObjective:
                  jsonResponse = processGetJourneyObjective(userID, jsonRoot);
                  break;

                case putJourneyObjective:
                  jsonResponse = processPutJourneyObjective(userID, jsonRoot);
                  break;

                case removeJourneyObjective:
                  jsonResponse = processRemoveJourneyObjective(userID, jsonRoot);
                  break;

                case getOfferObjectiveList:
                  jsonResponse = processGetOfferObjectiveList(userID, jsonRoot, true);
                  break;

                case getOfferObjectiveSummaryList:
                  jsonResponse = processGetOfferObjectiveList(userID, jsonRoot, false);
                  break;

                case getOfferObjective:
                  jsonResponse = processGetOfferObjective(userID, jsonRoot);
                  break;

                case putOfferObjective:
                  jsonResponse = processPutOfferObjective(userID, jsonRoot);
                  break;

                case removeOfferObjective:
                  jsonResponse = processRemoveOfferObjective(userID, jsonRoot);
                  break;

                case getProductTypeList:
                  jsonResponse = processGetProductTypeList(userID, jsonRoot, true);
                  break;

                case getProductTypeSummaryList:
                  jsonResponse = processGetProductTypeList(userID, jsonRoot, false);
                  break;

                case getProductType:
                  jsonResponse = processGetProductType(userID, jsonRoot);
                  break;

                case putProductType:
                  jsonResponse = processPutProductType(userID, jsonRoot);
                  break;

                case removeProductType:
                  jsonResponse = processRemoveProductType(userID, jsonRoot);
                  break;

                case getUCGRuleList:
                  jsonResponse = processGetUCGRuleList(userID, jsonRoot, true);
                  break;

                case getUCGRuleSummaryList:
                  jsonResponse = processGetUCGRuleList(userID, jsonRoot, false);
                  break;

                case getUCGRule:
                  jsonResponse = processGetUCGRule(userID, jsonRoot);
                  break;

                case putUCGRule:
                  jsonResponse = processPutUCGRule(userID, jsonRoot);
                  break;

                case removeUCGRule:
                  jsonResponse = processRemoveUCGRule(userID, jsonRoot);
                  break;

                case getDeliverableList:
                  jsonResponse = processGetDeliverableList(userID, jsonRoot, true);
                  break;

                case getDeliverableSummaryList:
                  jsonResponse = processGetDeliverableList(userID, jsonRoot, false);
                  break;

                case getDeliverable:
                  jsonResponse = processGetDeliverable(userID, jsonRoot);
                  break;

                case getDeliverableByName:
                  jsonResponse = processGetDeliverableByName(userID, jsonRoot);
                  break;

                case getTokenTypeList:
                  jsonResponse = processGetTokenTypeList(userID, jsonRoot, true);
                  break;

                case getTokenTypeSummaryList:
                  jsonResponse = processGetTokenTypeList(userID, jsonRoot, false);
                  break;

                case putTokenType:
                  jsonResponse = processPutTokenType(userID, jsonRoot);
                  break;

                case getTokenType:
                  jsonResponse = processGetTokenType(userID, jsonRoot);
                  break;

                case removeTokenType:
                  jsonResponse = processRemoveTokenType(userID, jsonRoot);
                  break;

                case getTokenCodesFormats:
                  jsonResponse = processGetTokenCodesFormats(userID, jsonRoot);
                  break;
                  
               case getMailTemplateList:
                 jsonResponse = processGetMailTemplateList(userID, jsonRoot, true);
                 break;

               case getMailTemplateSummaryList:
                 jsonResponse = processGetMailTemplateList(userID, jsonRoot, false);
                 break;

               case getMailTemplate:
                 jsonResponse = processGetMailTemplate(userID, jsonRoot);
                 break;

               case putMailTemplate:
                 jsonResponse = processPutMailTemplate(userID, jsonRoot);
                 break;

               case removeMailTemplate:
                 jsonResponse = processRemoveMailTemplate(userID, jsonRoot);
                 break;

               case getSMSTemplateList:
                 jsonResponse = processGetSMSTemplateList(userID, jsonRoot, true);
                 break;

               case getSMSTemplateSummaryList:
                 jsonResponse = processGetSMSTemplateList(userID, jsonRoot, false);
                 break;

               case getSMSTemplate:
                 jsonResponse = processGetSMSTemplate(userID, jsonRoot);
                 break;

               case putSMSTemplate:
                 jsonResponse = processPutSMSTemplate(userID, jsonRoot);
                 break;

               case removeSMSTemplate:
                 jsonResponse = processRemoveSMSTemplate(userID, jsonRoot);
                 break;

               case getFulfillmentProviders:
                 jsonResponse = processGetFulfillmentProviders(userID, jsonRoot);
                 break;

               case getPaymentMeans:
                 jsonResponse = processGetPaymentMeanList(userID, jsonRoot, true);
                 break;

               case getPaymentMeanList:
                  jsonResponse = processGetPaymentMeanList(userID, jsonRoot, true);
                  break;

                case getPaymentMeanSummaryList:
                  jsonResponse = processGetPaymentMeanList(userID, jsonRoot, false);
                  break;

                case getPaymentMean:
                  jsonResponse = processGetPaymentMean(userID, jsonRoot);
                  break;

                case putPaymentMean:
                  jsonResponse = processPutPaymentMean(userID, jsonRoot);
                  break;

                case removePaymentMean:
                  jsonResponse = processRemovePaymentMean(userID, jsonRoot);
                  break;

                case getDashboardCounts:
                  jsonResponse = processGetDashboardCounts(userID, jsonRoot);
                  break;

                case getCustomer:
                  jsonResponse = processGetCustomer(userID, jsonRoot);
                  break;

                case getCustomerMetaData:
                  jsonResponse = processGetCustomerMetaData(userID, jsonRoot);
                  break;

                case getCustomerActivityByDateRange:
                  jsonResponse = processGetCustomerActivityByDateRange(userID, jsonRoot);
                  break;

                case getCustomerBDRs:
                  jsonResponse = processGetCustomerBDRs(userID, jsonRoot);
                  break;

                case getCustomerODRs:
                  jsonResponse = processGetCustomerODRs(userID, jsonRoot);
                  break;

                case getCustomerMessages:
                  jsonResponse = processGetCustomerMessages(userID, jsonRoot);
                  break;

                case getCustomerJourneys:
                  jsonResponse = processGetCustomerJourneys(userID, jsonRoot);
                  break;

                case getCustomerCampaigns:
                  jsonResponse = processGetCustomerCampaigns(userID, jsonRoot);
                  break;

                case refreshUCG:
                  jsonResponse = processRefreshUCG(userID, jsonRoot);
                  break;
                  
                case getUploadedFileList:
                  jsonResponse = processGetFilesList(userID, jsonRoot, true);
                  break;

                case getUploadedFileSummaryList:
                  jsonResponse = processGetFilesList(userID, jsonRoot, false);
                  break;

                case removeUploadedFile:
                  jsonResponse = processRemoveUploadedFile(userID, jsonRoot);
                  break;

                case getCustomerAlternateIDs:
                  jsonResponse = processGetCustomerAlternateIDs(userID, jsonRoot);
                  break;

                case getCustomerAvailableCampaigns:
                  jsonResponse = processGetCustomerAvailableCampaigns(userID, jsonRoot);
                  break;

                case getTargetList:
                  jsonResponse = processGetTargetList(userID, jsonRoot, true);
                  break;

                case getTargetSummaryList:
                  jsonResponse = processGetTargetList(userID, jsonRoot, false);
                  break;

                case putTarget:
                  jsonResponse = processPutTarget(userID, jsonRoot);
                  break;

                case getTarget:
                  jsonResponse = processGetTarget(userID, jsonRoot);
                  break;

                case removeTarget:
                  jsonResponse = processRemoveTarget(userID, jsonRoot);
                  break;
                  
                case updateCustomer:
                  jsonResponse = processUpdateCustomer(userID, jsonRoot);
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
        jsonResponse.put("licenseCheck", licenseAlarm.getJSONRepresentation());

        //
        //  log
        //

        log.debug("API (raw response): {}", jsonResponse.toString());

        //
        //  send
        //

        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
    catch (org.json.simple.parser.ParseException | GUIManagerException | IOException | ServerException | RuntimeException e )
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  send error response
        //

        HashMap<String,Object> response = new HashMap<String,Object>();
        response.put("responseCode", "systemError");
        response.put("responseMessage", e.getMessage());
        JSONObject jsonResponse = JSONUtilities.encodeObject(response);
        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
  }
  
  /*****************************************
  *
  *  handleFileAPI
  *
  *****************************************/

  private synchronized void handleComplexAPI(API api, HttpExchange exchange) throws IOException
  {
    try
      {
        /*****************************************
        *
        *  get the user
        *
        *****************************************/

        String userID = null;
        if (exchange.getRequestURI().getQuery() != null)
          {
            Pattern pattern = Pattern.compile("^(.*\\&user_id|user_id)=(.*?)(\\&.*$|$)");
            Matcher matcher = pattern.matcher(exchange.getRequestURI().getQuery());
            if (matcher.matches())
              {
                userID = matcher.group(2);
              }
          }

        /*****************************************
        *
        *  get the body
        *
        *****************************************/

        JSONObject jsonRoot = null;
        if(!api.equals(API.putUploadedFile)){
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
          jsonRoot = (JSONObject) (new JSONParser()).parse(requestBodyStringBuilder.toString());

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

        //
        //  standard response fields
        //
        JSONObject jsonResponse = new JSONObject();
        jsonResponse.put("apiVersion", RESTAPIVersion);
        jsonResponse.put("licenseCheck", licenseAlarm.getJSONRepresentation());

        if (licenseState.isValid() && allowAccess)
          { 
            switch (api)
              {
                case putUploadedFile:
                  processPutFile(jsonResponse, exchange);
                  break;
                  
                case downloadReport:
                  processDownloadReport(userID, jsonRoot, jsonResponse, exchange);
                  break;
              }
          }
        else
          {
            log.warn("Failed license check {} ", licenseState);
            jsonResponse = processFailedLicenseCheck(licenseState);
            exchange.sendResponseHeaders(200, 0);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
            writer.write(jsonResponse.toString());
            writer.close();
            exchange.close();
          }

      }
    catch (org.json.simple.parser.ParseException | IOException | RuntimeException e )
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  send error response
        //

        HashMap<String,Object> response = new HashMap<String,Object>();
        response.put("responseCode", "systemError");
        response.put("responseMessage", e.getMessage());
        JSONObject jsonResponse = JSONUtilities.encodeObject(response);
        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
  }
  
  /*****************************************
  *
  *  processPutFile
  *
  *****************************************/

  private void processPutFile(JSONObject jsonResponse, HttpExchange exchange) throws IOException
  {
    /****************************************
    *
    *  response map and object
    *
    ****************************************/

    JSONObject jsonRoot = null;
    String fileID = null;
    String userID = null;
    String responseText = null;

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    
    /****************************************
    *
    *  check incoming request
    *
    ****************************************/

    //
    //  contentType
    //

    String contentType = exchange.getRequestHeaders().getFirst("Content-Type"); 
    if(contentType == null)
      { 
        responseText = "Content-Type is null";    
      }
    else if (!contentType.startsWith(MULTIPART_FORM_DATA))
      { 
        responseText = "Message is not multipart/form-data";
      } 

    //
    //  contentLength
    //

    String contentLengthString = exchange.getRequestHeaders().getFirst("Content-Length"); 
    if(contentLengthString == null)
      { 
        responseText = "Content of message is null";  
      } 

    /****************************************
    *
    *  apache FileUpload API
    *
    ****************************************/

    final InputStream requestBodyStream = exchange.getRequestBody(); 
    final String contentEncoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
    FileUpload upload = new FileUpload(); 
    FileItemIterator fileItemIterator; 
    try
      {
        fileItemIterator = upload.getItemIterator(new RequestContext()
        { 
          public String getCharacterEncoding() { return contentEncoding; } 
          public String getContentType() { return contentType; } 
          public int getContentLength() { return 0; } 
          public InputStream getInputStream() throws IOException { return requestBodyStream; }
        }); 

        if (!fileItemIterator.hasNext())
          { 
            responseText = "Body is empty";
          }

        //
        // here we will extract the meta data of the request and the file
        //

        if (responseText == null)
          {
            boolean uploadFile = false;
            while (fileItemIterator.hasNext())
              {
                FileItemStream fis = fileItemIterator.next();
                if (fis.getFieldName().equals(FILE_UPLOAD_META_DATA))
                  {
                    InputStream streams = fis.openStream();
                    String jsonAsString = Streams.asString(streams, "UTF-8");
                    jsonRoot = (JSONObject) (new JSONParser()).parse(jsonAsString);
                    userID = JSONUtilities.decodeString(jsonRoot, "userID", true);
                    if(!jsonRoot.containsKey("id"))
                      {
                        fileID = uploadedFileService.generateFileID();
                      }
                    else
                      {
                        fileID = JSONUtilities.decodeString(jsonRoot, "id", true);
                      }
                    jsonRoot.put("id", fileID);
                    uploadFile = true;
                  }
                if (fis.getFieldName().equals(FILE_REQUEST) && uploadFile)
                  {
                    // converted the meta data and now attempting to save the file locally
                    //

                    long epoch = epochServer.getKey();

                    /*****************************************
                    *
                    *  existing UploadedFile
                    *
                    *****************************************/

                    GUIManagedObject existingFileUpload = uploadedFileService.getStoredUploadedFile(fileID);
                    try
                      {
                        /****************************************
                        *
                        *  instantiate new UploadedFile
                        *
                        ****************************************/

                        UploadedFile uploadedFile = new UploadedFile(jsonRoot, epoch, existingFileUpload);

                        /*****************************************
                        *
                        *  store UploadedFile
                        *
                        *****************************************/

                        uploadedFileService.putUploadedFile(uploadedFile, fis.openStream(), uploadedFile.getDestinationFilename(), (uploadedFile == null), userID);

                        /*****************************************
                        *
                        *  revalidate dependent objects
                        *
                        *****************************************/

                        revalidateTargets(now);
                        
                        /*****************************************
                        *
                        *  response
                        *
                        *****************************************/

                        jsonResponse.put("fileID", fileID);
                        jsonResponse.put("id", fileID);
                        jsonResponse.put("accepted", true);
                        jsonResponse.put("valid", true);
                        jsonResponse.put("processing", true);
                        jsonResponse.put("responseCode", "ok");
                      }
                    catch (JSONUtilitiesException|GUIManagerException e)
                      {
                        //
                        //  incompleteObject
                        //

                        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

                        //
                        //  store
                        //

                        uploadedFileService.putIncompleteUploadedFile(incompleteObject, (existingFileUpload == null), userID);

                        //
                        //  revalidate dependent objects
                        //

                        revalidateTargets(now);

                        //
                        //  log
                        //

                        StringWriter stackTraceWriter = new StringWriter();
                        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

                        //
                        //  response
                        //

                        jsonResponse.put("id", incompleteObject.getGUIManagedObjectID());
                        jsonResponse.put("responseCode", "fileNotValid");
                        jsonResponse.put("responseMessage", e.getMessage());
                        jsonResponse.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
                      }
                  }
              }
          }
        else
          {
            jsonResponse.put("responseCode", "systemError");
            jsonResponse.put("responseMessage", responseText);
          }

        //
        //  log
        //

        log.debug("API (raw response): {}", jsonResponse.toString());

        //
        //  send
        //

        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
    catch (Exception e)
      { 
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Failed to write file REST api: {}", stackTraceWriter.toString());   
        jsonResponse.put("responseCode", "systemError");
        jsonResponse.put("responseMessage", e.getMessage());
        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      } 
  }

  /*****************************************
  *
  *  processGetFilesList
  *
  *****************************************/
  
  private JSONObject processGetFilesList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert UploadedFiles
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> uploadedFiles = new ArrayList<JSONObject>();
    String applicationID = JSONUtilities.decodeString(jsonRoot, "applicationID", true);
    for (GUIManagedObject uploaded : uploadedFileService.getStoredGUIManagedObjects())
      {
        String fileApplicationID = JSONUtilities.decodeString(uploaded.getJSONRepresentation(), "applicationID", false);
        if (Objects.equals(applicationID, fileApplicationID))
          {
            uploadedFiles.add(uploadedFileService.generateResponseJSON(uploaded, fullDetails, now));
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    //
    //  uploadedFiles
    //

    HashMap<String,Object> responseResult = new HashMap<String,Object>();
    responseResult.put("applicationID", applicationID);
    responseResult.put("uploadedFiles", JSONUtilities.encodeArray(uploadedFiles));

    //
    //  response
    //

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("uploadedFiles", JSONUtilities.encodeObject(responseResult));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processRemoveUploadedFile
  *
  *****************************************/
  
  public JSONObject processRemoveUploadedFile(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    
    /****************************************
    *
    *  argument
    *
    ****************************************/

    String uploadedFileID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject existingFileUpload = uploadedFileService.getStoredUploadedFile(uploadedFileID);
    if (existingFileUpload != null && !existingFileUpload.getReadOnly())
      {
        uploadedFileService.deleteUploadedFile(uploadedFileID, userID, (UploadedFile)existingFileUpload);
      }

    /*****************************************
    *
    *  revalidate dependent objects
    *
    *****************************************/

    revalidateTargets(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (existingFileUpload != null && ! existingFileUpload.getReadOnly())
      responseCode = "ok";
    else if (existingFileUpload != null) 
      responseCode = "failedReadOnly";
    else 
      responseCode = "uploadedFileNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getCustomerAlternateIDs
  *
  *****************************************/

  private JSONObject processGetCustomerAlternateIDs(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve alternateIDs
    *
    *****************************************/

    List<JSONObject> alternateIDs = new ArrayList<JSONObject>();
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
      {
        JSONObject json = new JSONObject();
        json.put("id", alternateID.getID());
        json.put("display", alternateID.getDisplay());
        alternateIDs.add(json);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("alternateIDs", JSONUtilities.encodeArray(alternateIDs));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processFailedLicenseCheck
  *
  *****************************************/

  private JSONObject processFailedLicenseCheck(LicenseState licenseState)
  {
    HashMap<String,Object> response = new HashMap<String,Object>();

    response.put("responseCode", "failedLicenseCheck");
    response.put("responseMessage", licenseState.getOutcome().name());

    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processGetTarget
  *
  *****************************************/
 
  private JSONObject processGetTarget(String userID, JSONObject jsonRoot)
  {
    log.info("GUIManager.processGetTarget("+userID+", "+jsonRoot+") called ...");
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

    String targetID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate target
    *
    *****************************************/

    GUIManagedObject target = targetService.getStoredTarget(targetID);
    JSONObject targetJSON = targetService.generateResponseJSON(target, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (target != null) ? "ok" : "targetNotFound");
    if (target != null) response.put("target", targetJSON);

    log.info("GUIManager.processGetTarget("+userID+", "+jsonRoot+") DONE");

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processPutTarget
   *
   *****************************************/

  private JSONObject processPutTarget(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  targetID
    *
    *****************************************/

    String targetID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (targetID == null)
      {
        targetID = targetService.generateTargetID();
        jsonRoot.put("id", targetID);
      }

    /*****************************************
    *
    *  existing target
    *
    *****************************************/

    GUIManagedObject existingTarget = targetService.getStoredTarget(targetID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingTarget != null && existingTarget.getReadOnly())
      {
        response.put("id", existingTarget.getGUIManagedObjectID());
        response.put("accepted", existingTarget.getAccepted());
        response.put("valid", existingTarget.getAccepted());
        response.put("processing", targetService.isActiveTarget(existingTarget, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process target
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
    {
      /****************************************
      *
      *  instantiate Target
      *
      ****************************************/

      Target target = new Target(jsonRoot, epoch, existingTarget);

      /*****************************************
      *
      *  store
      *
      *****************************************/

      targetService.putTarget(target, uploadedFileService, subscriberIDService, (existingTarget == null), userID);

      /*****************************************
      *
      *  revalidate dependent objects
      *
      *****************************************/

      revalidateJourneys(now);

      /*****************************************
      *
      *  response
      *
      *****************************************/

      response.put("id", target.getGUIManagedObjectID());
      response.put("accepted", target.getAccepted());
      response.put("valid", target.getAccepted());
      response.put("processing", targetService.isActiveTarget(target, now));
      response.put("responseCode", "ok");
      return JSONUtilities.encodeObject(response);
    }
    catch (JSONUtilitiesException|GUIManagerException e)
    {
      //
      //  incompleteObject
      //

      IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

      //
      //  store
      //

      targetService.putTarget(incompleteObject, uploadedFileService, subscriberIDService, (existingTarget == null), userID);

      //
      //  revalidate dependent objects
      //

      revalidateJourneys(now);
      
      //
      //  log
      //

      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

      //
      //  response
      //

      response.put("targetID", incompleteObject.getGUIManagedObjectID());
      response.put("responseCode", "targetNotValid");
      response.put("responseMessage", e.getMessage());
      response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
      return JSONUtilities.encodeObject(response);
    }
  }
  
  /*****************************************
  *
  *  processGetTargetList
  *
  *****************************************/

  private JSONObject processGetTargetList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {

    /*****************************************
    *
    *  retrieve target list
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> targetLists = new ArrayList<JSONObject>();
    for (GUIManagedObject targetList : targetService.getStoredTargets())
      {
        JSONObject targetResponse = targetService.generateResponseJSON(targetList, fullDetails, now);
        targetResponse.put("isRunning", targetService.isActiveTarget(targetList, now) ? targetService.isTargetFileBeingProcessed((Target) targetList) : false);
        targetLists.add(targetResponse);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("targets", JSONUtilities.encodeArray(targetLists));
    return JSONUtilities.encodeObject(response);
  }

  
  /*****************************************
  *
  *  processRemoveTarget
  *
  *****************************************/
  
  public JSONObject processRemoveTarget(String userID, JSONObject jsonRoot){
    
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

    String targetID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject existingTarget = targetService.getStoredTarget(targetID);
    if (existingTarget != null && !existingTarget.getReadOnly()) {
      targetService.removeTarget(targetID, userID);
    }

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (existingTarget != null && !existingTarget.getReadOnly()) {
      responseCode = "ok";
    }
    else if (existingTarget != null) {
      responseCode = "failedReadOnly";
    }
    else {
      responseCode = "targetNotFound";
    }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }
  

  /*****************************************
  *
  *  getStaticConfiguration
  *
  *****************************************/

  private JSONObject processGetStaticConfiguration(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supportedLanguages
    *
    *****************************************/

    List<JSONObject> supportedLanguages = new ArrayList<JSONObject>();
    for (SupportedLanguage supportedLanguage : Deployment.getSupportedLanguages().values())
      {
        JSONObject supportedLanguageJSON = supportedLanguage.getJSONRepresentation();
        supportedLanguages.add(supportedLanguageJSON);
      }

    /*****************************************
    *
    *  retrieve supportedCurrencies
    *
    *****************************************/

    List<JSONObject> supportedCurrencies = new ArrayList<JSONObject>();
    for (SupportedCurrency supportedCurrency : Deployment.getSupportedCurrencies().values())
      {
        JSONObject supportedCurrencyJSON = supportedCurrency.getJSONRepresentation();
        supportedCurrencies.add(supportedCurrencyJSON);
      }

    /*****************************************
    *
    *  retrieve supportedTimeUnits
    *
    *****************************************/

    List<JSONObject> supportedTimeUnits = new ArrayList<JSONObject>();
    for (SupportedTimeUnit supportedTimeUnit : Deployment.getSupportedTimeUnits().values())
      {
        JSONObject supportedTimeUnitJSON = supportedTimeUnit.getJSONRepresentation();
        supportedTimeUnits.add(supportedTimeUnitJSON);
      }

    /*****************************************
    *
    *  retrieve serviceTypes
    *
    *****************************************/

    List<JSONObject> serviceTypes = new ArrayList<JSONObject>();
    for (ServiceType serviceType : Deployment.getServiceTypes().values())
      {
        JSONObject serviceTypeJSON = serviceType.getJSONRepresentation();
        serviceTypes.add(serviceTypeJSON);
      }

    /*****************************************
    *
    *  retrieve touchPoints
    *
    *****************************************/

    List<JSONObject> touchPoints = new ArrayList<JSONObject>();
    for (TouchPoint touchPoint : Deployment.getTouchPoints().values())
      {
        JSONObject touchPointJSON = touchPoint.getJSONRepresentation();
        touchPoints.add(touchPointJSON);
      }

    /*****************************************
    *
    *  retrieve callingChannelProperties
    *
    *****************************************/

    List<JSONObject> callingChannelProperties = new ArrayList<JSONObject>();
    for (CallingChannelProperty callingChannelProperty : Deployment.getCallingChannelProperties().values())
      {
        JSONObject callingChannelPropertyJSON = callingChannelProperty.getJSONRepresentation();
        callingChannelProperties.add(callingChannelPropertyJSON);
      }

    /*****************************************
    *
    *  retrieve catalogCharacteristicUnits
    *
    *****************************************/

    List<JSONObject> catalogCharacteristicUnits = new ArrayList<JSONObject>();
    for (CatalogCharacteristicUnit catalogCharacteristicUnit : Deployment.getCatalogCharacteristicUnits().values())
      {
        JSONObject catalogCharacteristicUnitJSON = catalogCharacteristicUnit.getJSONRepresentation();
        catalogCharacteristicUnits.add(catalogCharacteristicUnitJSON);
      }

    /*****************************************
    *
    *  retrieve supported data types
    *
    *****************************************/

    List<JSONObject> supportedDataTypes = new ArrayList<JSONObject>();
    for (SupportedDataType supportedDataType : Deployment.getSupportedDataTypes().values())
      {
        JSONObject supportedDataTypeJSON = supportedDataType.getJSONRepresentation();
        supportedDataTypes.add(supportedDataTypeJSON);
      }

    /*****************************************
    *
    *  retrieve profile criterion fields
    *
    *****************************************/

    List<JSONObject> profileCriterionFields = processCriterionFields(CriterionContext.Profile.getCriterionFields(), false);

    /*****************************************
    *
    *  retrieve presentation criterion fields
    *
    *****************************************/

    List<JSONObject> presentationCriterionFields = processCriterionFields(CriterionContext.Presentation.getCriterionFields(), false);

    /*****************************************
    *
    *  retrieve offerCategories
    *
    *****************************************/

    List<JSONObject> offerCategories = new ArrayList<JSONObject>();
    for (OfferCategory offerCategory : Deployment.getOfferCategories().values())
      {
        JSONObject offerCategoryJSON = offerCategory.getJSONRepresentation();
        offerCategories.add(offerCategoryJSON);
      }

    /*****************************************
    *
    *  retrieve offerTypes
    *
    *****************************************/

    List<JSONObject> offerTypes = new ArrayList<JSONObject>();
    for (OfferType offerType : Deployment.getOfferTypes().values())
      {
        JSONObject offerTypeJSON = offerType.getJSONRepresentation();
        offerTypes.add(offerTypeJSON);
      }

    /*****************************************
    *
    *  retrieve offerOptimizationAlgorithms
    *
    *****************************************/

    List<JSONObject> offerOptimizationAlgorithms = new ArrayList<JSONObject>();
    for (OfferOptimizationAlgorithm offerOptimizationAlgorithm : Deployment.getOfferOptimizationAlgorithms().values())
      {
        JSONObject offerOptimizationAlgorithmJSON = offerOptimizationAlgorithm.getJSONRepresentation();
        offerOptimizationAlgorithms.add(offerOptimizationAlgorithmJSON);
      }

    /*****************************************
    *
    *  retrieve nodeTypes
    *
    *****************************************/

    List<JSONObject> nodeTypes = processNodeTypes(Deployment.getNodeTypes(), Collections.<String,CriterionField>emptyMap(), Collections.<String,CriterionField>emptyMap());

    /*****************************************
    *
    *  retrieve journeyToolboxSections
    *
    *****************************************/

    List<JSONObject> journeyToolboxSections = new ArrayList<JSONObject>();
    for (ToolboxSection journeyToolboxSection : Deployment.getJourneyToolbox().values())
      {
        JSONObject journeyToolboxSectionJSON = journeyToolboxSection.getJSONRepresentation();
        journeyToolboxSections.add(journeyToolboxSectionJSON);
      }

    /*****************************************
    *
    *  retrieve campaignToolboxSections
    *
    *****************************************/

    List<JSONObject> campaignToolboxSections = new ArrayList<JSONObject>();
    for (ToolboxSection campaignToolboxSection : Deployment.getCampaignToolbox().values())
      {
        JSONObject campaignToolboxSectionJSON = campaignToolboxSection.getJSONRepresentation();
        campaignToolboxSections.add(campaignToolboxSectionJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedLanguages", JSONUtilities.encodeArray(supportedLanguages));
    response.put("supportedCurrencies", JSONUtilities.encodeArray(supportedCurrencies));
    response.put("callingChannelProperties", JSONUtilities.encodeArray(callingChannelProperties));
    response.put("supportedDataTypes", JSONUtilities.encodeArray(supportedDataTypes));
    response.put("profileCriterionFields", JSONUtilities.encodeArray(profileCriterionFields));
    response.put("presentationCriterionFields", JSONUtilities.encodeArray(presentationCriterionFields));
    response.put("offerCategories", JSONUtilities.encodeArray(offerCategories));
    response.put("offerTypes", JSONUtilities.encodeArray(offerTypes));
    response.put("offerOptimizationAlgorithms", JSONUtilities.encodeArray(offerOptimizationAlgorithms));
    response.put("nodeTypes", JSONUtilities.encodeArray(nodeTypes));
    response.put("journeyToolbox", JSONUtilities.encodeArray(journeyToolboxSections));
    response.put("campaignToolbox", JSONUtilities.encodeArray(campaignToolboxSections));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedLanguages
  *
  *****************************************/

  private JSONObject processGetSupportedLanguages(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supportedLanguages
    *
    *****************************************/

    List<JSONObject> supportedLanguages = new ArrayList<JSONObject>();
    for (SupportedLanguage supportedLanguage : Deployment.getSupportedLanguages().values())
      {
        JSONObject supportedLanguageJSON = supportedLanguage.getJSONRepresentation();
        supportedLanguages.add(supportedLanguageJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedLanguages", JSONUtilities.encodeArray(supportedLanguages));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedCurrencies
  *
  *****************************************/

  private JSONObject processGetSupportedCurrencies(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supportedCurrencies
    *
    *****************************************/

    List<JSONObject> supportedCurrencies = new ArrayList<JSONObject>();
    for (SupportedCurrency supportedCurrency : Deployment.getSupportedCurrencies().values())
      {
        JSONObject supportedCurrencyJSON = supportedCurrency.getJSONRepresentation();
        supportedCurrencies.add(supportedCurrencyJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedCurrencies", JSONUtilities.encodeArray(supportedCurrencies));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedTimeUnits
  *
  *****************************************/

  private JSONObject processGetSupportedTimeUnits(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supportedTimeUnits
    *
    *****************************************/

    List<JSONObject> supportedTimeUnits = new ArrayList<JSONObject>();
    for (SupportedTimeUnit supportedTimeUnit : Deployment.getSupportedTimeUnits().values())
      {
        JSONObject supportedTimeUnitJSON = supportedTimeUnit.getJSONRepresentation();
        supportedTimeUnits.add(supportedTimeUnitJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedTimeUnits", JSONUtilities.encodeArray(supportedTimeUnits));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getServiceTypes
  *
  *****************************************/

  private JSONObject processGetServiceTypes(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve serviceTypes
    *
    *****************************************/

    List<JSONObject> serviceTypes = new ArrayList<JSONObject>();
    for (ServiceType serviceType : Deployment.getServiceTypes().values())
      {
        JSONObject serviceTypeJSON = serviceType.getJSONRepresentation();
        serviceTypes.add(serviceTypeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("serviceTypes", JSONUtilities.encodeArray(serviceTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getTouchPoints
  *
  *****************************************/

  private JSONObject processGetTouchPoints(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve touchPoints
    *
    *****************************************/

    List<JSONObject> touchPoints = new ArrayList<JSONObject>();
    for (TouchPoint touchPoint : Deployment.getTouchPoints().values())
      {
        JSONObject touchPointJSON = touchPoint.getJSONRepresentation();
        touchPoints.add(touchPointJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("touchPoints", JSONUtilities.encodeArray(touchPoints));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getCallingChannelProperties
  *
  *****************************************/

  private JSONObject processGetCallingChannelProperties(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve callingChannelProperties
    *
    *****************************************/

    List<JSONObject> callingChannelProperties = new ArrayList<JSONObject>();
    for (CallingChannelProperty callingChannelProperty : Deployment.getCallingChannelProperties().values())
      {
        JSONObject callingChannelPropertyJSON = callingChannelProperty.getJSONRepresentation();
        callingChannelProperties.add(callingChannelPropertyJSON);
      }

    /*****************************************et
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("callingChannelProperties", JSONUtilities.encodeArray(callingChannelProperties));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getCatalogCharacteristicUnits
  *
  *****************************************/

  private JSONObject processGetCatalogCharacteristicUnits(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve catalogCharacteristicUnits
    *
    *****************************************/

    List<JSONObject> catalogCharacteristicUnits = new ArrayList<JSONObject>();
    for (CatalogCharacteristicUnit catalogCharacteristicUnit : Deployment.getCatalogCharacteristicUnits().values())
      {
        JSONObject catalogCharacteristicUnitJSON = catalogCharacteristicUnit.getJSONRepresentation();
        catalogCharacteristicUnits.add(catalogCharacteristicUnitJSON);
      }

    /*****************************************et
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("catalogCharacteristicUnits", JSONUtilities.encodeArray(catalogCharacteristicUnits));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedDataTypes
  *
  *****************************************/

  private JSONObject processGetSupportedDataTypes(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supported data types
    *
    *****************************************/

    List<JSONObject> supportedDataTypes = new ArrayList<JSONObject>();
    for (SupportedDataType supportedDataType : Deployment.getSupportedDataTypes().values())
      {
        JSONObject supportedDataTypeJSON = supportedDataType.getJSONRepresentation();
        supportedDataTypes.add(supportedDataTypeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedDataTypes", JSONUtilities.encodeArray(supportedDataTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedEvents
  *
  *****************************************/

  private JSONObject processGetSupportedEvents(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve events
    *
    *****************************************/

    List<JSONObject> events = evaluateEnumeratedValues("eventNames", SystemTime.getCurrentTime(), true);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("events", JSONUtilities.encodeArray(events));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedTargetingTypes
  *
  *****************************************/

  private JSONObject processGetSupportedTargetingTypes(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve events
    *
    *****************************************/

    List<JSONObject> targetingTypes = new ArrayList<JSONObject>();
    for (TargetingType targetingType : TargetingType.values())
      {
        if (targetingType != TargetingType.Unknown)
          {
            JSONObject json = new JSONObject();
            json.put("id", targetingType.getExternalRepresentation());
            json.put("display", targetingType.getDisplay());
            targetingTypes.add(json);
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("targetingTypes", JSONUtilities.encodeArray(targetingTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getProfileCriterionFields
  *
  *****************************************/

  private JSONObject processGetProfileCriterionFields(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve profile criterion fields
    *
    *****************************************/

    List<JSONObject> profileCriterionFields = processCriterionFields(CriterionContext.Profile.getCriterionFields(), false);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("profileCriterionFields", JSONUtilities.encodeArray(profileCriterionFields));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getProfileCriterionFieldIDs
  *
  *****************************************/

  private JSONObject processGetProfileCriterionFieldIDs(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve profile criterion fields
    *
    *****************************************/

    List<JSONObject> profileCriterionFields = processCriterionFields(CriterionContext.Profile.getCriterionFields(), false);

    /*****************************************
    *
    *  strip out everything but id/display
    *
    *****************************************/

    List<JSONObject> profileCriterionFieldIDs = new ArrayList<JSONObject>();
    for (JSONObject profileCriterionField : profileCriterionFields)
      {
        HashMap<String,Object> profileCriterionFieldID = new HashMap<String,Object>();
        profileCriterionFieldID.put("id", profileCriterionField.get("id"));
        profileCriterionFieldID.put("display", profileCriterionField.get("display"));
        profileCriterionFieldIDs.add(JSONUtilities.encodeObject(profileCriterionFieldID));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("profileCriterionFieldIDs", JSONUtilities.encodeArray(profileCriterionFieldIDs));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getProfileCriterionField
  *
  *****************************************/

  private JSONObject processGetProfileCriterionField(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve field id (setting it to null if blank)
    *
    *****************************************/

    String id = JSONUtilities.decodeString(jsonRoot, "id", true);
    id = (id != null && id.trim().length() == 0) ? null : id;

    /*****************************************
    *
    *  retrieve field with id
    *
    *****************************************/

    JSONObject requestedProfileCriterionField = null;
    if (id != null)
      {
        //
        //  retrieve profile criterion fields
        //

        List<JSONObject> profileCriterionFields = processCriterionFields(CriterionContext.Profile.getCriterionFields(), false);

        //
        //  find requested field
        //

        for (JSONObject profileCriterionField : profileCriterionFields)
          {
            if (Objects.equals(id, profileCriterionField.get("id")))
              {
                requestedProfileCriterionField = profileCriterionField;
                break;
              }
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    if (requestedProfileCriterionField != null)
      {
        response.put("responseCode", "ok");
        response.put("profileCriterionField", requestedProfileCriterionField);
      }
    else if (id == null)
      {
        response.put("responseCode", "invalidRequest");
        response.put("responseMessage", "id argument not provided");
      }
    else
      {
        response.put("responseCode", "fieldNotFound");
        response.put("responseMessage", "could not find profile criterion field with id " + id);
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getPresentationCriterionFields
  *
  *****************************************/

  private JSONObject processGetPresentationCriterionFields(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve presentation criterion fields
    *
    *****************************************/

    List<JSONObject> presentationCriterionFields = processCriterionFields(CriterionContext.Presentation.getCriterionFields(), false);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("presentationCriterionFields", JSONUtilities.encodeArray(presentationCriterionFields));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetPresentationCriterionFieldIDs
  *
  *****************************************/

  private JSONObject processGetPresentationCriterionFieldIDs(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve presentation criterion fields
    *
    *****************************************/

    List<JSONObject> presentationCriterionFields = processCriterionFields(CriterionContext.Presentation.getCriterionFields(), false);

    /*****************************************
    *
    *  strip out everything but id/display
    *
    *****************************************/

    List<JSONObject> presentationCriterionFieldIDs = new ArrayList<JSONObject>();
    for (JSONObject presentationCriterionField : presentationCriterionFields)
      {
        HashMap<String,Object> presentationCriterionFieldID = new HashMap<String,Object>();
        presentationCriterionFieldID.put("id", presentationCriterionField.get("id"));
        presentationCriterionFieldID.put("display", presentationCriterionField.get("display"));
        presentationCriterionFieldIDs.add(JSONUtilities.encodeObject(presentationCriterionFieldID));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("presentationCriterionFieldIDs", JSONUtilities.encodeArray(presentationCriterionFieldIDs));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getPresentationCriterionField
  *
  *****************************************/

  private JSONObject processGetPresentationCriterionField(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve field id (setting it to null if blank)
    *
    *****************************************/

    String id = JSONUtilities.decodeString(jsonRoot, "id", true);
    id = (id != null && id.trim().length() == 0) ? null : id;

    /*****************************************
    *
    *  retrieve field with id
    *
    *****************************************/

    JSONObject requestedPresentationCriterionField = null;
    if (id != null)
      {
        //
        //  retrieve presentation criterion fields
        //

        List<JSONObject> presentationCriterionFields = processCriterionFields(CriterionContext.Presentation.getCriterionFields(), false);

        //
        //  find requested field
        //

        for (JSONObject presentationCriterionField : presentationCriterionFields)
          {
            if (Objects.equals(id, presentationCriterionField.get("id")))
              {
                requestedPresentationCriterionField = presentationCriterionField;
                break;
              }
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    if (requestedPresentationCriterionField != null)
      {
        response.put("responseCode", "ok");
        response.put("presentationCriterionField", requestedPresentationCriterionField);
      }
    else if (id == null)
      {
        response.put("responseCode", "invalidRequest");
        response.put("responseMessage", "id argument not provided");
      }
    else
      {
        response.put("responseCode", "fieldNotFound");
        response.put("responseMessage", "could not find presentation criterion field with id " + id);
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getJourneyCriterionFields
  *
  *****************************************/

  private JSONObject processGetJourneyCriterionFields(String userID, JSONObject jsonRoot) throws GUIManagerException
  {
    /*****************************************
    *
    *  arguments
    *
    *****************************************/

    Map<String,CriterionField> journeyParameters = Journey.decodeJourneyParameters(JSONUtilities.decodeJSONArray(jsonRoot,"journeyParameters", false));
    Map<String,GUINode> contextVariableNodes = Journey.decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot,"contextVariableNodes", false), journeyParameters, Collections.<String,CriterionField>emptyMap(), false);
    NodeType journeyNodeType = Deployment.getNodeTypes().get(JSONUtilities.decodeString(jsonRoot, "nodeTypeID", true));
    EvolutionEngineEventDeclaration journeyNodeEvent = (JSONUtilities.decodeString(jsonRoot, "eventName", false) != null) ? Deployment.getEvolutionEngineEvents().get(JSONUtilities.decodeString(jsonRoot, "eventName", true)) : null;
    boolean tagsOnly = JSONUtilities.decodeBoolean(jsonRoot, "tagsOnly", Boolean.FALSE);

    /*****************************************
    *
    *  retrieve journey criterion fields
    *
    *****************************************/

    List<JSONObject> journeyCriterionFields = Collections.<JSONObject>emptyList();
    if (journeyNodeType != null)
      {
        CriterionContext criterionContext = new CriterionContext(journeyParameters, Journey.processContextVariableNodes(contextVariableNodes, journeyParameters), journeyNodeType, journeyNodeEvent, false);
        journeyCriterionFields = processCriterionFields(criterionContext.getCriterionFields(), tagsOnly);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    if (journeyNodeType != null)
      {
        response.put("responseCode", "ok");
        response.put("journeyCriterionFields", JSONUtilities.encodeArray(journeyCriterionFields));
      }
    else
      {
        response.put("responseCode", "invalidRequest");
        response.put("responseMessage", "could not find nodeType with id " + JSONUtilities.decodeString(jsonRoot, "nodeTypeID", true));
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getJourneyCriterionFieldIDs
  *
  *****************************************/

  private JSONObject processGetJourneyCriterionFieldIDs(String userID, JSONObject jsonRoot) throws GUIManagerException
  {
    /*****************************************
    *
    *  arguments
    *
    *****************************************/

    Map<String,CriterionField> journeyParameters = Journey.decodeJourneyParameters(JSONUtilities.decodeJSONArray(jsonRoot,"journeyParameters", false));
    Map<String,GUINode> contextVariableNodes = Journey.decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot,"contextVariableNodes", false), journeyParameters, Collections.<String,CriterionField>emptyMap(), false);
    NodeType journeyNodeType = Deployment.getNodeTypes().get(JSONUtilities.decodeString(jsonRoot, "nodeTypeID", true));
    EvolutionEngineEventDeclaration journeyNodeEvent = (JSONUtilities.decodeString(jsonRoot, "eventName", false) != null) ? Deployment.getEvolutionEngineEvents().get(JSONUtilities.decodeString(jsonRoot, "eventName", true)) : null;
    boolean tagsOnly = JSONUtilities.decodeBoolean(jsonRoot, "tagsOnly", Boolean.FALSE);

    /*****************************************
    *
    *  retrieve journey criterion fields
    *
    *****************************************/

    List<JSONObject> journeyCriterionFields = Collections.<JSONObject>emptyList();
    if (journeyNodeType != null)
      {
        CriterionContext criterionContext = new CriterionContext(journeyParameters, Journey.processContextVariableNodes(contextVariableNodes, journeyParameters), journeyNodeType, journeyNodeEvent, false);
        journeyCriterionFields = processCriterionFields(criterionContext.getCriterionFields(), tagsOnly);
      }
    
    /*****************************************
    *
    *  strip out everything but id/display
    *
    *****************************************/

    List<JSONObject> journeyCriterionFieldIDs = new ArrayList<JSONObject>();
    for (JSONObject journeyCriterionField : journeyCriterionFields)
      {
        HashMap<String,Object> journeyCriterionFieldID = new HashMap<String,Object>();
        journeyCriterionFieldID.put("id", journeyCriterionField.get("id"));
        journeyCriterionFieldID.put("display", journeyCriterionField.get("display"));
        journeyCriterionFieldIDs.add(JSONUtilities.encodeObject(journeyCriterionFieldID));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    if (journeyNodeType != null)
      {
        response.put("responseCode", "ok");
        response.put("journeyCriterionFieldIDs", JSONUtilities.encodeArray(journeyCriterionFieldIDs));
      }
    else
      {
        response.put("responseCode", "invalidRequest");
        response.put("responseMessage", "could not find nodeType with id " + JSONUtilities.decodeString(jsonRoot, "nodeTypeID", true));
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getJourneyCriterionField
  *
  *****************************************/

  private JSONObject processGetJourneyCriterionField(String userID, JSONObject jsonRoot) throws GUIManagerException
  {
    /*****************************************
    *
    *  retrieve field id (setting it to null if blank)
    *
    *****************************************/

    Map<String,CriterionField> journeyParameters = Journey.decodeJourneyParameters(JSONUtilities.decodeJSONArray(jsonRoot,"journeyParameters", false));
    Map<String,GUINode> contextVariableNodes = Journey.decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot,"contextVariableNodes", false), journeyParameters, Collections.<String,CriterionField>emptyMap(), false);
    NodeType journeyNodeType = Deployment.getNodeTypes().get(JSONUtilities.decodeString(jsonRoot, "nodeTypeID", true));
    EvolutionEngineEventDeclaration journeyNodeEvent = (JSONUtilities.decodeString(jsonRoot, "eventName", false) != null) ? Deployment.getEvolutionEngineEvents().get(JSONUtilities.decodeString(jsonRoot, "eventName", true)) : null;
    String id = JSONUtilities.decodeString(jsonRoot, "id", true);
    id = (id != null && id.trim().length() == 0) ? null : id;

    /*****************************************
    *
    *  retrieve field with id
    *
    *****************************************/

    JSONObject requestedJourneyCriterionField = null;
    if (id != null)
      {
        /*****************************************
        *
        *  retrieve journey criterion fields
        *
        *****************************************/

        List<JSONObject> journeyCriterionFields = Collections.<JSONObject>emptyList();
        if (journeyNodeType != null)
          {
            CriterionContext criterionContext = new CriterionContext(journeyParameters, Journey.processContextVariableNodes(contextVariableNodes, journeyParameters), journeyNodeType, journeyNodeEvent, false);
            journeyCriterionFields = processCriterionFields(criterionContext.getCriterionFields(), false);
          }

        /*****************************************
        *
        *  find requested field
        *
        *****************************************/

        for (JSONObject journeyCriterionField : journeyCriterionFields)
          {
            if (Objects.equals(id, journeyCriterionField.get("id")))
              {
                requestedJourneyCriterionField = journeyCriterionField;
                break;
              }
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    if (requestedJourneyCriterionField != null)
      {
        response.put("responseCode", "ok");
        response.put("journeyCriterionField", requestedJourneyCriterionField);
      }
    else if (journeyNodeType == null)
      {
        response.put("responseCode", "invalidRequest");
        response.put("responseMessage", "could not find nodeType with id " + JSONUtilities.decodeString(jsonRoot, "nodeTypeID", true));
      }
    else if (id == null)
      {
        response.put("responseCode", "invalidRequest");
        response.put("responseMessage", "id argument not provided");
      }
    else
      {
        response.put("responseCode", "fieldNotFound");
        response.put("responseMessage", "could not find journey criterion field with id " + id);
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getOfferCategories
  *
  *****************************************/

  private JSONObject processGetOfferCategories(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve offerCategories
    *
    *****************************************/

    List<JSONObject> offerCategories = new ArrayList<JSONObject>();
    for (OfferCategory offerCategory : Deployment.getOfferCategories().values())
      {
        JSONObject offerCategoryJSON = offerCategory.getJSONRepresentation();
        offerCategories.add(offerCategoryJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("offerCategories", JSONUtilities.encodeArray(offerCategories));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getOfferTypes
  *
  *****************************************/

  private JSONObject processGetOfferTypes(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve offerTypes
    *
    *****************************************/

    List<JSONObject> offerTypes = new ArrayList<JSONObject>();
    for (OfferType offerType : Deployment.getOfferTypes().values())
      {
        JSONObject offerTypeJSON = offerType.getJSONRepresentation();
        offerTypes.add(offerTypeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("offerTypes", JSONUtilities.encodeArray(offerTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getOfferOptimizationAlgorithms
  *
  *****************************************/

  private JSONObject processGetOfferOptimizationAlgorithms(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve offerOptimizationAlgorithms
    *
    *****************************************/

    List<JSONObject> offerOptimizationAlgorithms = new ArrayList<JSONObject>();
    for (OfferOptimizationAlgorithm offerOptimizationAlgorithm : Deployment.getOfferOptimizationAlgorithms().values())
      {
        JSONObject offerOptimizationAlgorithmJSON = offerOptimizationAlgorithm.getJSONRepresentation();
        offerOptimizationAlgorithms.add(offerOptimizationAlgorithmJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("offerOptimizationAlgorithms", JSONUtilities.encodeArray(offerOptimizationAlgorithms));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getNodeTypes
  *
  *****************************************/

  private JSONObject processGetNodeTypes(String userID, JSONObject jsonRoot) throws GUIManagerException
  {
    /*****************************************
    *
    *  arguments
    *
    *****************************************/

    Map<String,CriterionField> journeyParameters = Journey.decodeJourneyParameters(JSONUtilities.decodeJSONArray(jsonRoot,"journeyParameters", false));
    Map<String,GUINode> contextVariableNodes = Journey.decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot,"contextVariableNodes", false), journeyParameters, Collections.<String,CriterionField>emptyMap(), false);
    Map<String,CriterionField> contextVariables = Journey.processContextVariableNodes(contextVariableNodes, journeyParameters);

    /*****************************************
    *
    *  retrieve nodeTypes
    *
    *****************************************/

    List<JSONObject> nodeTypes = processNodeTypes(Deployment.getNodeTypes(), journeyParameters, contextVariables);
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("nodeTypes", JSONUtilities.encodeArray(nodeTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getJourneyToolbox
  *
  *****************************************/

  private JSONObject processGetJourneyToolbox(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve journeyToolboxSections
    *
    *****************************************/

    List<JSONObject> journeyToolboxSections = new ArrayList<JSONObject>();
    for (ToolboxSection journeyToolboxSection : Deployment.getJourneyToolbox().values())
      {
        JSONObject journeyToolboxSectionJSON = journeyToolboxSection.getJSONRepresentation();
        journeyToolboxSections.add(journeyToolboxSectionJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("journeyToolbox", JSONUtilities.encodeArray(journeyToolboxSections));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getCampaignToolbox
  *
  *****************************************/

  private JSONObject processGetCampaignToolbox(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve campaignToolboxSections
    *
    *****************************************/

    List<JSONObject> campaignToolboxSections = new ArrayList<JSONObject>();
    for (ToolboxSection campaignToolboxSection : Deployment.getCampaignToolbox().values())
      {
        JSONObject campaignToolboxSectionJSON = campaignToolboxSection.getJSONRepresentation();
        campaignToolboxSections.add(campaignToolboxSectionJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("campaignToolbox", JSONUtilities.encodeArray(campaignToolboxSections));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processCriterionFields
  *
  *****************************************/

  private List<JSONObject> processCriterionFields(Map<String,CriterionField> baseCriterionFields, boolean tagsOnly)
  {
    /*****************************************
    *
    *  filter out parameter-only and tag-only data types
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    Map<String,CriterionField> criterionFields = new LinkedHashMap<String,CriterionField>();
    for (CriterionField criterionField : baseCriterionFields.values())
      {
        switch (criterionField.getFieldDataType())
          {
            case IntegerCriterion:
            case DoubleCriterion:
            case StringCriterion:
            case BooleanCriterion:
            case DateCriterion:
              criterionFields.put(criterionField.getID(), criterionField);
              break;

            case StringSetCriterion:
              if (! tagsOnly) criterionFields.put(criterionField.getID(), criterionField);
              break;
          }
      }

    /****************************************
    *
    *  resolve field data types
    *
    ****************************************/

    Map<String, ResolvedFieldType> resolvedFieldTypes = new LinkedHashMap<String, ResolvedFieldType>();
    Map<String, List<JSONObject>> resolvedAvailableValues = new LinkedHashMap<String, List<JSONObject>>();
    for (CriterionField criterionField : criterionFields.values())
      {
        List<JSONObject> availableValues = evaluateAvailableValues(criterionField, now, true);
        resolvedFieldTypes.put(criterionField.getID(), new ResolvedFieldType(criterionField.getFieldDataType(), availableValues));
        resolvedAvailableValues.put(criterionField.getID(), availableValues);
      }

    /****************************************
    *
    *  default list of fields for each field data type
    *
    ****************************************/

    Map<ResolvedFieldType, List<CriterionField>> defaultFieldsForResolvedType = new LinkedHashMap<ResolvedFieldType, List<CriterionField>>();
    for (CriterionField criterionField : criterionFields.values())
      {
        if (! criterionField.getID().equals(CriterionField.EvaluationDateField))
          {
            ResolvedFieldType resolvedFieldType = resolvedFieldTypes.get(criterionField.getID());
            List<CriterionField> fields = defaultFieldsForResolvedType.get(resolvedFieldType);
            if (fields == null)
              {
                fields = new ArrayList<CriterionField>();
                defaultFieldsForResolvedType.put(resolvedFieldType, fields);
              }
            fields.add(criterionField);
          }
      }

    /****************************************
    *
    *  process
    *
    ****************************************/

    List<JSONObject> result = new ArrayList<JSONObject>();
    for (CriterionField criterionField : criterionFields.values())
      {
        if (! criterionField.getInternalOnly())
          {
            //
            //  remove server-side fields
            //

            JSONObject criterionFieldJSON = (JSONObject) criterionField.getJSONRepresentation().clone();
            criterionFieldJSON.remove("esField");
            criterionFieldJSON.remove("retriever");

            //
            //  evaluate operators
            //

            List<JSONObject> fieldAvailableValues = resolvedAvailableValues.get(criterionField.getID());
            List<JSONObject> operators = evaluateOperators(criterionFieldJSON, fieldAvailableValues);
            criterionFieldJSON.put("operators", operators);
            criterionFieldJSON.remove("includedOperators");
            criterionFieldJSON.remove("excludedOperators");

            //
            //  evaluate comparable fields
            //

            List<CriterionField> defaultComparableFields = defaultFieldsForResolvedType.get(resolvedFieldTypes.get(criterionField.getID()));
            criterionFieldJSON.put("singletonComparableFields", evaluateComparableFields(criterionField.getID(), criterionFieldJSON, defaultComparableFields, true));
            criterionFieldJSON.put("setValuedComparableFields", evaluateComparableFields(criterionField.getID(), criterionFieldJSON, defaultComparableFields, false));
            criterionFieldJSON.remove("includedComparableFields");
            criterionFieldJSON.remove("excludedComparableFields");

            //
            //  evaluate available values for reference data
            //

            criterionFieldJSON.put("availableValues", resolvedAvailableValues.get(criterionField.getID()));

            //
            //  resolve maxTagLength
            //

            criterionFieldJSON.put("tagMaxLength", criterionField.resolveTagMaxLength());

            //
            //  normalize set-valued dataTypes to singleton-valued dataTypes (required by GUI)
            //

            switch (criterionField.getFieldDataType())
              {
                case StringSetCriterion:
                  criterionFieldJSON.put("dataType", CriterionDataType.StringCriterion.getExternalRepresentation());
                  break;
              }

            //
            //  add
            //

            result.add(criterionFieldJSON);
          }
      }

    //
    //  return
    //

    return result;
  }

  /****************************************
  *
  *  evaluateOperators
  *
  ****************************************/

  private List<JSONObject> evaluateOperators(JSONObject criterionFieldJSON, List<JSONObject> fieldAvailableValues)
  {
    //
    //  all operators
    //

    Map<String,SupportedOperator> supportedOperatorsForType = Deployment.getSupportedDataTypes().get(criterionFieldJSON.get("dataType")).getOperators();

    //
    //  remove set operators for non-enumerated types
    //

    List<String> supportedOperators = new ArrayList<String>();
    for (String supportedOperatorID : supportedOperatorsForType.keySet())
      {
        SupportedOperator supportedOperator = supportedOperatorsForType.get(supportedOperatorID);
        if (! supportedOperator.getArgumentSet())
          supportedOperators.add(supportedOperatorID);
        else if (supportedOperator.getArgumentSet() && fieldAvailableValues != null)
          supportedOperators.add(supportedOperatorID);
      }

    //
    //  find list of explicitly included operators
    //

    List<String> requestedIncludedOperatorIDs = null;
    if (criterionFieldJSON.get("includedOperators") != null)
      {
        requestedIncludedOperatorIDs = new ArrayList<String>();
        for (String operator : supportedOperators)
          {
            for (String operatorRegex : (List<String>) criterionFieldJSON.get("includedOperators"))
              {
                Pattern pattern = Pattern.compile("^" + operatorRegex + "$");
                if (pattern.matcher(operator).matches())
                  {
                    requestedIncludedOperatorIDs.add(operator);
                    break;
                  }
              }
          }
      }

    //
    //  find list of explicitly excluded operators
    //

    List<String> requestedExcludedOperatorIDs = null;
    if (criterionFieldJSON.get("excludedOperators") != null)
      {
        requestedExcludedOperatorIDs = new ArrayList<String>();
        for (String operator : supportedOperators)
          {
            for (String operatorRegex : (List<String>) criterionFieldJSON.get("excludedOperators"))
              {
                Pattern pattern = Pattern.compile("^" + operatorRegex + "$");
                if (pattern.matcher(operator).matches())
                  {
                    requestedExcludedOperatorIDs.add(operator);
                    break;
                  }
              }
          }
      }

    //
    //  resolve included/excluded operators
    //

    List<String> includedOperatorIDs = requestedIncludedOperatorIDs != null ? requestedIncludedOperatorIDs : supportedOperators;
    Set<String> excludedOperatorIDs = requestedExcludedOperatorIDs != null ? new LinkedHashSet<String>(requestedExcludedOperatorIDs) : Collections.<String>emptySet();

    //
    //  evaluate
    //

    List<JSONObject> result = new ArrayList<JSONObject>();
    for (String operatorID : includedOperatorIDs)
      {
        SupportedOperator operator = supportedOperatorsForType.get(operatorID);
        if (! excludedOperatorIDs.contains(operatorID))
          {
            result.add(operator.getJSONRepresentation());
          }
      }

    //
    //  return
    //

    return result;
  }

  /****************************************
  *
  *  evaluateComparableFields
  *
  ****************************************/

  private List<JSONObject> evaluateComparableFields(String criterionFieldID, JSONObject criterionFieldJSON, Collection<CriterionField> allFields, boolean singleton)
  {
    //
    //  all fields
    //

    Map<String, CriterionField> comparableFields = new LinkedHashMap<String, CriterionField>();
    for (CriterionField criterionField : allFields)
      {
        comparableFields.put(criterionField.getID(), criterionField);
      }

    //
    //  find list of explicitly included fields
    //

    List<String> requestedIncludedComparableFieldIDs = null;
    if (criterionFieldJSON.get("includedComparableFields") != null)
      {
        requestedIncludedComparableFieldIDs = new ArrayList<String>();
        for (String comparableField : comparableFields.keySet())
          {
            for (String fieldRegex : (List<String>) criterionFieldJSON.get("includedComparableFields"))
              {
                Pattern pattern = Pattern.compile("^" + fieldRegex + "$");
                if (pattern.matcher(comparableField).matches())
                  {
                    requestedIncludedComparableFieldIDs.add(comparableField);
                    break;
                  }
              }
          }
      }

    //
    //  find list of explicitly excluded fields
    //

    List<String> requestedExcludedComparableFieldIDs = null;
    if (criterionFieldJSON.get("excludedComparableFields") != null)
      {
        requestedExcludedComparableFieldIDs = new ArrayList<String>();
        for (String comparableField : comparableFields.keySet())
          {
            for (String fieldRegex : (List<String>) criterionFieldJSON.get("excludedComparableFields"))
              {
                Pattern pattern = Pattern.compile("^" + fieldRegex + "$");
                if (pattern.matcher(comparableField).matches())
                  {
                    requestedExcludedComparableFieldIDs.add(comparableField);
                    break;
                  }
              }
          }
      }

    //
    //  resolve included/excluded fields
    //

    List<String> includedComparableFieldIDs = requestedIncludedComparableFieldIDs != null ? requestedIncludedComparableFieldIDs : new ArrayList<String>(comparableFields.keySet());
    Set<String> excludedComparableFieldIDs = requestedExcludedComparableFieldIDs != null ? new LinkedHashSet<String>(requestedExcludedComparableFieldIDs) : new HashSet<String>();

    //
    //  always exclude internal-only fields
    //

    for (CriterionField criterionField : comparableFields.values())
      {
        if (criterionField.getInternalOnly())
          {
            excludedComparableFieldIDs.add(criterionField.getID());
          }
      }

    //
    //  evaluate
    //

    List<JSONObject> result = new ArrayList<JSONObject>();
    for (String comparableFieldID : includedComparableFieldIDs)
      {
        CriterionField criterionField = comparableFields.get(comparableFieldID);
        if ((! excludedComparableFieldIDs.contains(comparableFieldID)) && (singleton == criterionField.getFieldDataType().getSingletonType()) && (! comparableFieldID.equals(criterionFieldID)))
          {
            HashMap<String,Object> comparableFieldJSON = new HashMap<String,Object>();
            comparableFieldJSON.put("id", criterionField.getID());
            comparableFieldJSON.put("display", criterionField.getDisplay());
            result.add(JSONUtilities.encodeObject(comparableFieldJSON));
          }
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  processNodeTypes
  *
  *****************************************/

  private List<JSONObject> processNodeTypes(Map<String,NodeType> nodeTypes, Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables)
  {
    Date now = SystemTime.getCurrentTime();
    List<JSONObject> result = new ArrayList<JSONObject>();
    for (NodeType nodeType : nodeTypes.values())
      {
        //
        //  clone
        //

        JSONObject resolvedNodeTypeJSON = (JSONObject) nodeType.getJSONRepresentation().clone();

        //
        //  parameters
        //

        List<JSONObject>  resolvedParameters = new ArrayList<JSONObject>();
        JSONArray parameters = JSONUtilities.decodeJSONArray(resolvedNodeTypeJSON, "parameters", true);
        for (int i=0; i<parameters.size(); i++)
          {
            //
            //  clone (so we can modify the result)
            //

            JSONObject parameterJSON = (JSONObject) ((JSONObject) parameters.get(i)).clone();

            //
            //  availableValues
            //
            
            List<JSONObject> availableValues = evaluateAvailableValues(JSONUtilities.decodeJSONArray(parameterJSON, "availableValues", false), now);
            parameterJSON.put("availableValues", (availableValues != null) ? JSONUtilities.encodeArray(availableValues) : null);

            //
            //  expressionFields
            //

            CriterionField parameter = nodeType.getParameters().get(JSONUtilities.decodeString(parameterJSON, "id", true));
            if (parameter != null && parameter.getExpressionValuedParameter())
              {
                //
                //  default list of fields for parameter data type
                //

                CriterionContext criterionContext = new CriterionContext(journeyParameters, contextVariables, nodeType, (EvolutionEngineEventDeclaration) null, false);
                List<CriterionField> defaultFields = new ArrayList<CriterionField>();
                for (CriterionField criterionField : criterionContext.getCriterionFields().values())
                  {
                    if (! criterionField.getID().equals(CriterionField.EvaluationDateField) && criterionField.getFieldDataType() == parameter.getFieldDataType())
                      {
                        defaultFields.add(criterionField);
                      }
                  }
                
                //
                //  evaluate comparable fields
                //

                List<JSONObject> expressionFields = evaluateComparableFields(parameter.getID(), parameter.getJSONRepresentation(), defaultFields, true);
                parameterJSON.put("expressionFields", JSONUtilities.encodeArray(expressionFields));
              }

            //
            //  result
            //

            resolvedParameters.add(parameterJSON);
          }
        resolvedNodeTypeJSON.put("parameters", JSONUtilities.encodeArray(resolvedParameters));

        //
        //  dynamic output connector
        //

        JSONObject dynamicOutputConnectorJSON = JSONUtilities.decodeJSONObject(resolvedNodeTypeJSON, "dynamicOutputConnector", false);
        if (dynamicOutputConnectorJSON != null)
          {
            JSONObject resolvedDynamicOutputConnectorJSON = (JSONObject) dynamicOutputConnectorJSON.clone();
            List<JSONObject>  resolvedDynamicOutputConnectorParameters = new ArrayList<JSONObject>();
            JSONArray dynamicOutputConnectorParameters = JSONUtilities.decodeJSONArray(resolvedDynamicOutputConnectorJSON, "parameters", true);
            for (int i=0; i<dynamicOutputConnectorParameters.size(); i++)
              {
                JSONObject parameterJSON = (JSONObject) ((JSONObject) dynamicOutputConnectorParameters.get(i)).clone();
                List<JSONObject> availableValues = evaluateAvailableValues(JSONUtilities.decodeJSONArray(parameterJSON, "availableValues", false), now);
                parameterJSON.put("availableValues", (availableValues != null) ? JSONUtilities.encodeArray(availableValues) : null);
                resolvedDynamicOutputConnectorParameters.add(parameterJSON);
              }
            resolvedDynamicOutputConnectorJSON.put("parameters", JSONUtilities.encodeArray(resolvedDynamicOutputConnectorParameters));
            resolvedNodeTypeJSON.put("dynamicOutputConnector", resolvedDynamicOutputConnectorJSON);
          }

        //
        //  result
        //

        result.add(resolvedNodeTypeJSON);
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  evaluateAvailableValues
  *
  *****************************************/

  private List<JSONObject> evaluateAvailableValues(CriterionField criterionField, Date now, boolean includeDynamic)
  {
    JSONObject criterionFieldJSON = (JSONObject) criterionField.getJSONRepresentation();
    List<JSONObject> availableValues = evaluateAvailableValues(JSONUtilities.decodeJSONArray(criterionFieldJSON, "availableValues", false), now, includeDynamic);
    return availableValues;
  }

  /*****************************************
  *
  *  evaluateAvailableValues
  *
  *****************************************/

  private List<JSONObject> evaluateAvailableValues(JSONArray availableValues, Date now)
  {
    return evaluateAvailableValues(availableValues, now, true);
  }

  /****************************************
  *
  *  evaluateAvailableValues
  *
  ****************************************/

  private List<JSONObject> evaluateAvailableValues(JSONArray availableValues, Date now, boolean includeDynamic)
  {
    List<JSONObject> result = null;
    if (availableValues != null)
      {
        result = new ArrayList<JSONObject>();
        Pattern enumeratedValuesPattern = Pattern.compile("^#(.*?)#$");
        for (Object availableValueUnchecked : availableValues)
          {
            if (availableValueUnchecked instanceof String)
              {
                String availableValue = (String) availableValueUnchecked;
                Matcher matcher = enumeratedValuesPattern.matcher(availableValue);
                if (matcher.matches())
                  {
                    result.addAll(evaluateEnumeratedValues(matcher.group(1), now, includeDynamic));
                  }
                else
                  {
                    HashMap<String,Object> availableValueJSON = new HashMap<String,Object>();
                    availableValueJSON.put("id", availableValue);
                    availableValueJSON.put("display", availableValue);
                    result.add(JSONUtilities.encodeObject(availableValueJSON));
                  }
              }
            else if (availableValueUnchecked instanceof JSONObject)
              {
                JSONObject availableValue = (JSONObject) availableValueUnchecked;
                result.add(availableValue);
              }
            else
              {
                Object availableValue = (Object) availableValueUnchecked;
                HashMap<String,Object> availableValueJSON = new HashMap<String,Object>();
                availableValueJSON.put("id", availableValue);
                availableValueJSON.put("display", availableValue.toString());
                result.add(JSONUtilities.encodeObject(availableValueJSON));
              }
          }
      }
    return result;
  }

  /****************************************
  *
  *  evaluateEnumeratedValues
  *
  ****************************************/

  private List<JSONObject> evaluateEnumeratedValues(String reference, Date now, boolean includeDynamic)
  {
    List<JSONObject> result = new ArrayList<JSONObject>();
    switch (reference)
      {
        case "supportedLanguages":
          for (SupportedLanguage supportedLanguage : Deployment.getSupportedLanguages().values())
            {
              HashMap<String,Object> availableValue = new HashMap<String,Object>();
              availableValue.put("id", supportedLanguage.getID());
              availableValue.put("display", supportedLanguage.getDisplay());
              result.add(JSONUtilities.encodeObject(availableValue));
            }
          break;

        case "segments":
          if (includeDynamic)
            {
              for (SegmentationDimension dimension : segmentationDimensionService.getActiveSegmentationDimensions(now))
                {
                  for (Segment segment : dimension.getSegments())
                    {
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", segment.getID());
                      availableValue.put("display", segment.getName());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "eventNames":
          for (EvolutionEngineEventDeclaration evolutionEngineEventDeclaration : Deployment.getEvolutionEngineEvents().values())
            {
              HashMap<String,Object> availableValue = new HashMap<String,Object>();
              availableValue.put("id", evolutionEngineEventDeclaration.getName());
              availableValue.put("display", evolutionEngineEventDeclaration.getName());
              result.add(JSONUtilities.encodeObject(availableValue));
            }
          break;

        case "paymentMeans":
          if (includeDynamic)
            {
              for (GUIManagedObject paymentMeanUnchecked : paymentMeanService.getStoredPaymentMeans())
                {
                  if (paymentMeanUnchecked.getAccepted())
                    {
                      PaymentMean paymentMean = (PaymentMean) paymentMeanUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", paymentMean.getPaymentMeanID());
                      availableValue.put("display", paymentMean.getDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "offers":
          if (includeDynamic)
            {
              for (GUIManagedObject offerUnchecked : offerService.getStoredOffers())
                {
                  if (offerUnchecked.getAccepted())
                    {
                      Offer offer = (Offer) offerUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", offer.getOfferID());
                      availableValue.put("display", offer.getDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "pointDeliverables":
          if (includeDynamic)
            {
              DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("pointFulfillment");
              JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
              String providerID = (String) deliveryManagerJSON.get("providerID");
              for (GUIManagedObject deliverableUnchecked : deliverableService.getStoredDeliverables())
                {
                  if (deliverableUnchecked.getAccepted())
                    {
                      Deliverable deliverable = (Deliverable) deliverableUnchecked;
                      if(deliverable.getFulfillmentProviderID().equals(providerID)){
                        HashMap<String,Object> availableValue = new HashMap<String,Object>();
                        availableValue.put("id", deliverable.getDeliverableID());
                        availableValue.put("display", deliverable.getDeliverableName());
                        result.add(JSONUtilities.encodeObject(availableValue));
                      }
                    }
                }
            }
          break;

        case "products":
          if (includeDynamic)
            {
              for (GUIManagedObject productUnchecked : productService.getStoredProducts())
                {
                  if (productUnchecked.getAccepted())
                    {
                      Product product = (Product) productUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", product.getProductID());
                      availableValue.put("display", product.getDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "productTypes":
          if (includeDynamic)
            {
              for (GUIManagedObject productTypeUnchecked : productTypeService.getStoredProductTypes())
                {
                  if (productTypeUnchecked.getAccepted())
                    {
                      ProductType productType = (ProductType) productTypeUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", productType.getProductTypeID());
                      availableValue.put("display", productType.getProductTypeName());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "callableCampaigns":
          if (includeDynamic)
            {
              for (GUIManagedObject campaignUnchecked : journeyService.getStoredJourneys())
                {
                  if (campaignUnchecked.getAccepted())
                    {
                      Journey campaign = (Journey) campaignUnchecked;
                      switch (campaign.getTargetingType())
                        {
                          case Manual:
                            switch (campaign.getGUIManagedObjectType())
                              {
                                case Campaign:
                                  HashMap<String,Object> availableValue = new HashMap<String,Object>();
                                  availableValue.put("id", campaign.getJourneyID());
                                  availableValue.put("display", campaign.getJourneyName());
                                  result.add(JSONUtilities.encodeObject(availableValue));
                                  break;
                              }
                            break;
                        }
                    }
                }
            }
          break;

        case "callableJourneys":
          if (includeDynamic)
            {
              for (GUIManagedObject journeyUnchecked : journeyService.getStoredJourneys())
                {
                  if (journeyUnchecked.getAccepted())
                    {
                      Journey journey = (Journey) journeyUnchecked;
                      switch (journey.getTargetingType())
                        {
                          case Manual:
                            switch (journey.getGUIManagedObjectType())
                              {
                                case Journey:
                                  HashMap<String,Object> availableValue = new HashMap<String,Object>();
                                  availableValue.put("id", journey.getJourneyID());
                                  availableValue.put("display", journey.getJourneyName());
                                  result.add(JSONUtilities.encodeObject(availableValue));
                                  break;
                              }
                            break;
                        }
                    }
                }
            }
          break;

        case "presentationStrategies":
          if (includeDynamic)
            {
              for (GUIManagedObject presentationStrategyUnchecked : presentationStrategyService.getStoredPresentationStrategies())
                {
                  if (presentationStrategyUnchecked.getAccepted())
                    {
                      PresentationStrategy presentationStrategy = (PresentationStrategy) presentationStrategyUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", presentationStrategy.getPresentationStrategyID());
                      availableValue.put("display", presentationStrategy.getGUIManagedObjectName());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "offersPresentationTokenTypes":
          if (includeDynamic)
            {
              for (GUIManagedObject tokenTypeUnchecked : tokenTypeService.getStoredTokenTypes())
                {
                  if (tokenTypeUnchecked.getAccepted())
                    {
                      TokenType tokenType = (TokenType) tokenTypeUnchecked;
                      switch (tokenType.getTokenTypeKind())
                        {
                          case OffersPresentation:
                            HashMap<String,Object> availableValue = new HashMap<String,Object>();
                            availableValue.put("id", tokenType.getTokenTypeID());
                            availableValue.put("display", tokenType.getTokenTypeName());
                            result.add(JSONUtilities.encodeObject(availableValue));
                            break;
                        }
                    }
                }
            }
          break;

        case "supportedShortCodes":
          for (SupportedShortCode supportedShortCode : Deployment.getSupportedShortCodes().values())
            {
              HashMap<String,Object> availableValue = new HashMap<String,Object>();
              availableValue.put("id", supportedShortCode.getID());
              availableValue.put("display", supportedShortCode.getDisplay());
              result.add(JSONUtilities.encodeObject(availableValue));
            }
          break;

        case "supportedEmailAddresses":
          for (SupportedEmailAddress supportedEmailAddress : Deployment.getSupportedEmailAddresses().values())
            {
              HashMap<String,Object> availableValue = new HashMap<String,Object>();
              availableValue.put("id", supportedEmailAddress.getID());
              availableValue.put("display", supportedEmailAddress.getDisplay());
              result.add(JSONUtilities.encodeObject(availableValue));
            }
          break;

        default:
          if (guiManagerExtensionEvaluateEnumeratedValuesMethod != null)
            {
              try
                {
                  result.addAll((List<JSONObject>) guiManagerExtensionEvaluateEnumeratedValuesMethod.invoke(null, guiManagerContext, reference, now, includeDynamic));
                }
              catch (IllegalAccessException|InvocationTargetException e)
                {
                  throw new RuntimeException(e);
                }
            }
          break;
      }
    return result;
  }

  /*****************************************
  *
  *  processGetJourneyList
  *
  *****************************************/

  private JSONObject processGetJourneyList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert journeys
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> journeys = new ArrayList<JSONObject>();
    for (GUIManagedObject journey : journeyService.getStoredJourneys())
      {
        switch (journey.getGUIManagedObjectType())
          {
            case Journey:
              journeys.add(journeyService.generateResponseJSON(journey, fullDetails, now));
              break;
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("journeys", JSONUtilities.encodeArray(journeys));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetJourney
  *
  *****************************************/

  private JSONObject processGetJourney(String userID, JSONObject jsonRoot)
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

    String journeyID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate journey
    *
    *****************************************/

    GUIManagedObject journey = journeyService.getStoredJourney(journeyID);
    journey = (journey != null && journey.getGUIManagedObjectType() == GUIManagedObjectType.Journey) ? journey : null;
    JSONObject journeyJSON = journeyService.generateResponseJSON(journey, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (journey != null) ? "ok" : "journeyNotFound");
    if (journey != null) response.put("journey", journeyJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutJourney
  *
  *****************************************/

  private JSONObject processPutJourney(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  journeyID
    *
    *****************************************/

    String journeyID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (journeyID == null)
      {
        journeyID = journeyService.generateJourneyID();
        jsonRoot.put("id", journeyID);
      }

    /*****************************************
    *
    *  existing journey
    *
    *****************************************/

    GUIManagedObject existingJourney = journeyService.getStoredJourney(journeyID);
    existingJourney = (existingJourney != null && existingJourney.getGUIManagedObjectType() == GUIManagedObjectType.Journey) ? existingJourney : null;

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingJourney != null && existingJourney.getReadOnly())
      {
        response.put("id", existingJourney.getGUIManagedObjectID());
        response.put("accepted", existingJourney.getAccepted());
        response.put("valid", existingJourney.getAccepted());
        response.put("processing", journeyService.isActiveJourney(existingJourney, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process journey
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate journey
        *
        ****************************************/

        Journey journey = new Journey(jsonRoot, GUIManagedObjectType.Journey, epoch, existingJourney, catalogCharacteristicService);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyService.putJourney(journey, journeyObjectiveService, catalogCharacteristicService, targetService, (existingJourney == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", journey.getJourneyID());
        response.put("accepted", journey.getAccepted());
        response.put("valid", journey.getAccepted());
        response.put("processing", journeyService.isActiveJourney(journey, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, GUIManagedObjectType.Journey, epoch);

        //
        //  store
        //

        journeyService.putJourney(incompleteObject, journeyObjectiveService, catalogCharacteristicService, targetService, (existingJourney == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("journeyID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "journeyNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveJourney
  *
  *****************************************/

  private JSONObject processRemoveJourney(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String journeyID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject journey = journeyService.getStoredJourney(journeyID);
    journey = (journey != null && journey.getGUIManagedObjectType() == GUIManagedObjectType.Journey) ? journey : null;
    if (journey != null && ! journey.getReadOnly()) journeyService.removeJourney(journeyID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (journey != null && ! journey.getReadOnly())
      responseCode = "ok";
    else if (journey != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "journeyNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processJourneySetActive
  *
  *****************************************/

  private JSONObject processJourneySetActive(String userID, JSONObject jsonRoot, boolean active)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String journeyID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  validate existing journey
    *  - exists
    *  - valid
    *  - not read-only
    *
    *****************************************/

    GUIManagedObject existingJourney = journeyService.getStoredJourney(journeyID);
    String responseCode = null;
    responseCode = (responseCode == null && existingJourney == null) ? "journeyNotFound" : responseCode;
    responseCode = (responseCode == null && existingJourney.getGUIManagedObjectType() != GUIManagedObjectType.Journey) ? "journeyNotFound" : responseCode;
    responseCode = (responseCode == null && ! existingJourney.getAccepted()) ? "journeyNotValid" : responseCode;
    responseCode = (responseCode == null && existingJourney.getReadOnly()) ? "failedReadOnly" : responseCode;
    if (responseCode != null)
      {
        response.put("id", journeyID);
        if (existingJourney != null) response.put("accepted", existingJourney.getAccepted());
        if (existingJourney != null) response.put("valid", existingJourney.getAccepted());
        if (existingJourney != null) response.put("processing", journeyService.isActiveJourney(existingJourney, now));
        response.put("responseCode", responseCode);
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process journey
    *
    *****************************************/

    JSONObject journeyRoot = (JSONObject) existingJourney.getJSONRepresentation().clone();
    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  set active
        *
        ****************************************/

        journeyRoot.put("active", active);

        /****************************************
        *
        *  instantiate journey
        *
        ****************************************/

        Journey journey = new Journey(journeyRoot, GUIManagedObjectType.Journey, epoch, existingJourney, catalogCharacteristicService);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyService.putJourney(journey, journeyObjectiveService, catalogCharacteristicService, targetService, (existingJourney == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", journey.getJourneyID());
        response.put("accepted", journey.getAccepted());
        response.put("valid", journey.getAccepted());
        response.put("processing", journeyService.isActiveJourney(journey, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(journeyRoot, GUIManagedObjectType.Journey, epoch);

        //
        //  store
        //

        journeyService.putJourney(incompleteObject, journeyObjectiveService, catalogCharacteristicService, targetService, (existingJourney == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("journeyID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "journeyNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processGetCampaignList
  *
  *****************************************/

  private JSONObject processGetCampaignList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert campaigns
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> campaigns = new ArrayList<JSONObject>();
    for (GUIManagedObject campaign : journeyService.getStoredJourneys())
      {
        switch (campaign.getGUIManagedObjectType())
          {
            case Campaign:
              campaigns.add(journeyService.generateResponseJSON(campaign, fullDetails, now));
              break;
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("campaigns", JSONUtilities.encodeArray(campaigns));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetCampaign
  *
  *****************************************/

  private JSONObject processGetCampaign(String userID, JSONObject jsonRoot)
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

    String campaignID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate campaign
    *
    *****************************************/

    GUIManagedObject campaign = journeyService.getStoredJourney(campaignID);
    campaign = (campaign != null && campaign.getGUIManagedObjectType() == GUIManagedObjectType.Campaign) ? campaign : null;
    JSONObject campaignJSON = journeyService.generateResponseJSON(campaign, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (campaign != null) ? "ok" : "campaignNotFound");
    if (campaign != null) response.put("campaign", campaignJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutCampaign
  *
  *****************************************/

  private JSONObject processPutCampaign(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  campaignID
    *
    *****************************************/

    String campaignID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (campaignID == null)
      {
        campaignID = journeyService.generateJourneyID();
        jsonRoot.put("id", campaignID);
      }

    /*****************************************
    *
    *  existing campaign
    *
    *****************************************/

    GUIManagedObject existingCampaign = journeyService.getStoredJourney(campaignID);
    existingCampaign = (existingCampaign != null && existingCampaign.getGUIManagedObjectType() == GUIManagedObjectType.Campaign) ? existingCampaign : null;

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingCampaign != null && existingCampaign.getReadOnly())
      {
        response.put("id", existingCampaign.getGUIManagedObjectID());
        response.put("accepted", existingCampaign.getAccepted());
        response.put("valid", existingCampaign.getAccepted());
        response.put("processing", journeyService.isActiveJourney(existingCampaign, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process campaign
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate campaign
        *
        ****************************************/

        Journey campaign = new Journey(jsonRoot, GUIManagedObjectType.Campaign, epoch, existingCampaign, catalogCharacteristicService);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyService.putJourney(campaign, journeyObjectiveService, catalogCharacteristicService, targetService, (existingCampaign == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", campaign.getJourneyID());
        response.put("accepted", campaign.getAccepted());
        response.put("valid", campaign.getAccepted());
        response.put("processing", journeyService.isActiveJourney(campaign, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, GUIManagedObjectType.Campaign, epoch);

        //
        //  store
        //

        journeyService.putJourney(incompleteObject, journeyObjectiveService, catalogCharacteristicService, targetService, (existingCampaign == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("campaignID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "campaignNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveCampaign
  *
  *****************************************/

  private JSONObject processRemoveCampaign(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String campaignID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject campaign = journeyService.getStoredJourney(campaignID);
    campaign = (campaign != null && campaign.getGUIManagedObjectType() == GUIManagedObjectType.Campaign) ? campaign : null;
    if (campaign != null && ! campaign.getReadOnly()) journeyService.removeJourney(campaignID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (campaign != null && ! campaign.getReadOnly())
      responseCode = "ok";
    else if (campaign != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "campaignNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processCampaignSetActive
  *
  *****************************************/

  private JSONObject processCampaignSetActive(String userID, JSONObject jsonRoot, boolean active)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String campaignID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  validate existing campaign
    *  - exists
    *  - valid
    *  - not read-only
    *
    *****************************************/

    GUIManagedObject existingCampaign = journeyService.getStoredJourney(campaignID);
    String responseCode = null;
    responseCode = (responseCode == null && existingCampaign == null) ? "campaignNotFound" : responseCode;
    responseCode = (responseCode == null && existingCampaign.getGUIManagedObjectType() != GUIManagedObjectType.Campaign) ? "campaignNotFound" : responseCode;
    responseCode = (responseCode == null && ! existingCampaign.getAccepted()) ? "campaignNotValid" : responseCode;
    responseCode = (responseCode == null && existingCampaign.getReadOnly()) ? "failedReadOnly" : responseCode;
    if (responseCode != null)
      {
        response.put("id", campaignID);
        if (existingCampaign != null) response.put("accepted", existingCampaign.getAccepted());
        if (existingCampaign != null) response.put("valid", existingCampaign.getAccepted());
        if (existingCampaign != null) response.put("processing", journeyService.isActiveJourney(existingCampaign, now));
        response.put("responseCode", responseCode);
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process campaign
    *
    *****************************************/

    JSONObject campaignRoot = (JSONObject) existingCampaign.getJSONRepresentation().clone();
    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  set active
        *
        ****************************************/

        campaignRoot.put("active", active);

        /****************************************
        *
        *  instantiate campaign
        *
        ****************************************/

        Journey campaign = new Journey(campaignRoot, GUIManagedObjectType.Campaign, epoch, existingCampaign, catalogCharacteristicService);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyService.putJourney(campaign, journeyObjectiveService, catalogCharacteristicService, targetService, (existingCampaign == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", campaign.getJourneyID());
        response.put("accepted", campaign.getAccepted());
        response.put("valid", campaign.getAccepted());
        response.put("processing", journeyService.isActiveJourney(campaign, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(campaignRoot, GUIManagedObjectType.Campaign, epoch);

        //
        //  store
        //

        journeyService.putJourney(incompleteObject, journeyObjectiveService, catalogCharacteristicService, targetService, (existingCampaign == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("campaignID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "campaignNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processGetSegmentationDimensionList
  *
  *****************************************/

  private JSONObject processGetSegmentationDimensionList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert segmentationDimensions
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> segmentationDimensions = new ArrayList<JSONObject>();
    for (GUIManagedObject segmentationDimension : segmentationDimensionService.getStoredSegmentationDimensions())
      {
        
//        SegmentationDimensionTargetingType targetingType = ((SegmentationDimension)segmentationDimension).getTargetingType();
//        
//        switch(targetingType) {
//          case ELIGIBILITY:
//
//            break;
//          case FILE_IMPORT:
//            
//            break;
//          case RANGES:
//            if(((SegmentationDimensionRanges)segmentationDimension).get != null) {
//              ContactPolicy contactPolicy = contactPolicyService.getActiveContactPolicy(((SegmentationDimensionRanges)segmentationDimension).getContactPolicyID(), now);
//              segmentationDimensions.add(contactPolicy.getJSONRepresentation());
//            }
//            break;
//        }
        
        segmentationDimensions.add(segmentationDimensionService.generateResponseJSON(segmentationDimension, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("segmentationDimensions", JSONUtilities.encodeArray(segmentationDimensions));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetSegmentationDimension
  *
  *****************************************/

  private JSONObject processGetSegmentationDimension(String userID, JSONObject jsonRoot)
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

    String segmentationDimensionID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate segmentationDimension
    *
    *****************************************/

    GUIManagedObject segmentationDimension = segmentationDimensionService.getStoredSegmentationDimension(segmentationDimensionID);
    JSONObject segmentationDimensionJSON = segmentationDimensionService.generateResponseJSON(segmentationDimension, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (segmentationDimension != null) ? "ok" : "segmentationDimensionNotFound");
    if (segmentationDimension != null) response.put("segmentationDimension", segmentationDimensionJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutSegmentationDimension
  *
  *****************************************/

  private JSONObject processPutSegmentationDimension(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  segmentationDimensionID
    *
    *****************************************/

    String segmentationDimensionID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (segmentationDimensionID == null)
      {
        segmentationDimensionID = segmentationDimensionService.generateSegmentationDimensionID();
        jsonRoot.put("id", segmentationDimensionID);
      }

    /*****************************************
    *
    *  existing segmentationDimension
    *
    *****************************************/

    GUIManagedObject existingSegmentationDimension = segmentationDimensionService.getStoredSegmentationDimension(segmentationDimensionID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingSegmentationDimension != null && existingSegmentationDimension.getReadOnly())
      {
        response.put("id", existingSegmentationDimension.getGUIManagedObjectID());
        response.put("accepted", existingSegmentationDimension.getAccepted());
        response.put("valid", existingSegmentationDimension.getAccepted());
        response.put("processing", segmentationDimensionService.isActiveSegmentationDimension(existingSegmentationDimension, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process segmentationDimension
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate segmentationDimension
        *
        ****************************************/

        SegmentationDimension segmentationDimension = null;
        switch (SegmentationDimensionTargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", true)))
          {
            case ELIGIBILITY:
              segmentationDimension = new SegmentationDimensionEligibility(segmentationDimensionService, jsonRoot, epoch, existingSegmentationDimension);
              break;

            case RANGES:
              segmentationDimension = new SegmentationDimensionRanges(segmentationDimensionService, jsonRoot, epoch, existingSegmentationDimension);
              break;

            case FILE_IMPORT:
              segmentationDimension = new SegmentationDimensionFileImport(segmentationDimensionService, jsonRoot, epoch, existingSegmentationDimension);
              break;

            case Unknown:
              throw new GUIManagerException("unsupported dimension type", JSONUtilities.decodeString(jsonRoot, "targetingType", false));
          }

        /*****************************************
        *
        *  initialize/update subscriber group
        *
        *****************************************/

        //
        //  open zookeeper and lock dimension
        //

        ZooKeeper zookeeper = SubscriberGroupEpochService.openZooKeeperAndLockGroup(segmentationDimension.getSegmentationDimensionID());

        //
        //  create or ensure subscriberGroupEpoch exists
        //

        SubscriberGroupEpoch existingSubscriberGroupEpoch = SubscriberGroupEpochService.retrieveSubscriberGroupEpoch(zookeeper, segmentationDimension.getSegmentationDimensionID());

        //
        //  submit new subscriberGroupEpoch
        //

        SubscriberGroupEpoch subscriberGroupEpoch = SubscriberGroupEpochService.updateSubscriberGroupEpoch(zookeeper, segmentationDimension.getSegmentationDimensionID(), existingSubscriberGroupEpoch, kafkaProducer, Deployment.getSubscriberGroupEpochTopic());

        //
        //  update segmentationDimension
        //

        segmentationDimension.setSubscriberGroupEpoch(subscriberGroupEpoch);

        //
        //  close zookeeper and release dimension
        //

        SubscriberGroupEpochService.closeZooKeeperAndReleaseGroup(zookeeper, segmentationDimension.getSegmentationDimensionID());

        /*****************************************
        *
        *  store
        *
        *****************************************/

        segmentationDimensionService.putSegmentationDimension(segmentationDimension, (existingSegmentationDimension == null), userID);

        /*****************************************
        *
        *  revalidate
        *
        *****************************************/

        revalidateUCGRules(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", segmentationDimension.getSegmentationDimensionID());
        response.put("accepted", segmentationDimension.getAccepted());
        response.put("valid", segmentationDimension.getAccepted());
        response.put("processing", segmentationDimensionService.isActiveSegmentationDimension(segmentationDimension, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        segmentationDimensionService.putIncompleteSegmentationDimension(incompleteObject, (existingSegmentationDimension == null), userID);

        //
        //  revalidate
        //

        revalidateUCGRules(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("segmentationDimensionID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "segmentationDimensionNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveSegmentationDimension
  *
  *****************************************/

  private JSONObject processRemoveSegmentationDimension(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String segmentationDimensionID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject segmentationDimension = segmentationDimensionService.getStoredSegmentationDimension(segmentationDimensionID);
    if (segmentationDimension != null && ! segmentationDimension.getReadOnly()) segmentationDimensionService.removeSegmentationDimension(segmentationDimensionID, userID);

    /*****************************************
    *
    *  revalidate
    *
    *****************************************/

    revalidateUCGRules(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (segmentationDimension != null && ! segmentationDimension.getReadOnly())
      responseCode = "ok";
    else if (segmentationDimension != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "segmentationDimensionNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processCountBySegmentationRanges
   *
   *****************************************/

  private JSONObject processCountBySegmentationRanges(String userID, JSONObject jsonRoot)
  {
    /****************************************
     *
     *  response
     *
     ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
     *
     *  parse input (segmentationDimension)
     *
     *****************************************/

    jsonRoot.put("id", "fake-id"); // fill segmentationDimensionID with anything
    SegmentationDimensionRanges segmentationDimensionRanges = null;
    try
    {
      switch (SegmentationDimensionTargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", true)))
      {
      case RANGES:
        segmentationDimensionRanges = new SegmentationDimensionRanges(segmentationDimensionService, jsonRoot, epochServer.getKey(), null);
        break;

      case Unknown:
        throw new GUIManagerException("unsupported dimension type", JSONUtilities.decodeString(jsonRoot, "targetingType", false));
      }
    }
    catch (JSONUtilitiesException|GUIManagerException e)
    {
      //
      //  log
      //

      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

      //
      //  response
      //

      response.put("responseCode", "segmentationDimensionNotValid");
      response.put("responseMessage", e.getMessage());
      response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
      return JSONUtilities.encodeObject(response);
    }

    // Extract BaseSplits
    List<BaseSplit> baseSplits = segmentationDimensionRanges.getBaseSplit();
    int nbBaseSplits = baseSplits.size();

    /*****************************************
     *
     *  construct query
     *
     *****************************************/

    final String MAIN_AGG_NAME = "MAIN";
    final String RANGE_AGG_PREFIX = "RANGE-";

    //
    //  Main aggregation query: BaseSplit
    //

    List<BoolQueryBuilder> baseSplitQueries = new ArrayList<BoolQueryBuilder>();
    try
    {
      // BaseSplit query creation
      for(int i = 0; i < nbBaseSplits; i++) {
        baseSplitQueries.add(QueryBuilders.boolQuery());
      }

      for(int i = 0; i < nbBaseSplits; i++) {
        BoolQueryBuilder query = baseSplitQueries.get(i);
        BaseSplit baseSplit = baseSplits.get(i);

        // Filter this bucket with this BaseSplit criteria
        if(baseSplit.getProfileCriteria().isEmpty()) {
          // If there is not any profile criteria, just filter with a match_all query.
          query = query.filter(QueryBuilders.matchAllQuery());
        } else {
          for (EvaluationCriterion evaluationCriterion : baseSplit.getProfileCriteria())
          {
            query = query.filter(evaluationCriterion.esQuery());
          }
        }

        // Must_not for all following buckets (reminder : bucket must be disjointed, if not, some customer could be counted in several buckets)
        for(int j = i+1; j < nbBaseSplits; j++) {
          BoolQueryBuilder nextQuery = baseSplitQueries.get(j);

          if(baseSplit.getProfileCriteria().isEmpty()) {
            // If there is not any profile criteria, just filter with a match_all query.
            nextQuery = nextQuery.mustNot(QueryBuilders.matchAllQuery());
          } else {
            for (EvaluationCriterion evaluationCriterion : baseSplit.getProfileCriteria())
            {
              nextQuery = nextQuery.mustNot(evaluationCriterion.esQuery());
            }
          }
        }
      }
    }
    catch (CriterionException e)
    {
      //
      //  log
      //

      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

      //
      //  response
      //

      response.put("responseCode", "argumentError");
      response.put("responseMessage", e.getMessage());
      response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
      return JSONUtilities.encodeObject(response);
    }

    // The main aggregation is a filter aggregation. Each filter query will constitute a bucket representing a BaseSplit.
    List<KeyedFilter> queries = new ArrayList<KeyedFilter>();
    for(int i = 0; i < nbBaseSplits; i++) {
      BoolQueryBuilder query = baseSplitQueries.get(i);
      String bucketName = baseSplits.get(i).getSplitName(); // Warning: input must ensure that all BaseSplit names are different. ( TODO )
      queries.add(new FiltersAggregator.KeyedFilter(bucketName, query));
    }

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    AggregationBuilder aggregation = AggregationBuilders
        .filters(MAIN_AGG_NAME, queries.toArray(new KeyedFilter[queries.size()]));
    // @DEBUG: *otherBucket* can be activated for debug purpose: .otherBucket(true).otherBucketKey("OTH_BUCK")

    //
    //  Sub-aggregation query: Segments
    //

    // Sub-aggregations corresponding to all ranges-aggregation are added to the query
    for(int i = 0; i < nbBaseSplits; i++) {
      BaseSplit baseSplit = baseSplits.get(i);

      if(baseSplit.getVariableName() == null) {
        // This means that it should be the default segment, ranges-aggregation does not make sense here.

        QueryBuilder match_all = QueryBuilders.matchAllQuery();
        AggregationBuilder other = AggregationBuilders.filter(RANGE_AGG_PREFIX+baseSplit.getSplitName(), match_all);
        aggregation.subAggregation(other);
      } else {
        // Warning: input must ensure that all BaseSplit names are different. ( TODO )
        RangeAggregationBuilder range = AggregationBuilders.range(RANGE_AGG_PREFIX+baseSplit.getSplitName());

        // Retrieving the ElasticSearch field from the Criterion field.
        CriterionField criterionField = CriterionContext.Profile.getCriterionFields().get(baseSplit.getVariableName());
        if(criterionField.getESField() == null) {
          // If this Criterion field does not correspond to any field from Deployment.json, raise an error

          log.warn("Unknown criterion field {}", baseSplit.getVariableName());

          //
          //  response
          //

          response.put("responseCode", "systemError");
          response.put("responseMessage", "Unknown criterion field "+baseSplit.getVariableName()); // TODO security issue ?
          response.put("responseParameter", null);
          return JSONUtilities.encodeObject(response);
        }

        range = range.field(criterionField.getESField());

        for(SegmentRanges segment : baseSplit.getSegments()) {
          // Warning: input must ensure that all segment names are different. ( TODO )
          range = range.addRange(new Range(segment.getName(),
              (segment.getRangeMin() != null)? new Double (segment.getRangeMin()) : null,
              (segment.getRangeMax() != null)? new Double (segment.getRangeMax()) : null));
        }
        aggregation.subAggregation(range);
      }
    }

    searchSourceBuilder.aggregation(aggregation);
    // @DEBUG log.info(searchSourceBuilder.toString());

    /*****************************************
     *
     *  construct response (JSON object)
     *
     *****************************************/

    JSONObject responseJSON = new JSONObject();
    List<JSONObject> responseBaseSplits = new ArrayList<JSONObject>();
    for(int i = 0; i < nbBaseSplits; i++) {
      BaseSplit baseSplit = baseSplits.get(i);

      JSONObject responseBaseSplit = new JSONObject();
      List<JSONObject> responseSegments = new ArrayList<JSONObject>();
      responseBaseSplit.put("splitName", baseSplit.getSplitName());

      // Ranges
      for(SegmentRanges segment : baseSplit.getSegments()) {
        JSONObject responseSegment = new JSONObject();
        responseSegment.put("name", segment.getName());
        responseSegments.add(responseSegment);
        // The "count" field will be filled with the result of the ElasticSearch query
      }
      responseBaseSplit.put("segments", responseSegments);
      responseBaseSplits.add(responseBaseSplit);
    }

    /*****************************************
     *
     *  execute query
     *
     *****************************************/

    SearchRequest searchRequest = new SearchRequest("subscriberprofile").source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).aggregation(aggregation).size(0));
    SearchResponse searchResponse = null;

    try
    {
      searchResponse = elasticsearch.search(searchRequest);
      // @DEBUG log.info(searchResponse.toString());
    }
    catch (IOException e)
    {
      //
      //  log
      //

      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

      //
      //  response
      //

      response.put("responseCode", "systemError");
      response.put("responseMessage", e.getMessage());
      response.put("responseParameter", null);
      return JSONUtilities.encodeObject(response);
    }

    /*****************************************
     *
     *  retrieve result and fill the response JSON object
     *
     *****************************************/

    Filters mainAggregationResult = searchResponse.getAggregations().get(MAIN_AGG_NAME);
    // @DEBUG log.info(mainAggregationResult.toString());

    // Fill response JSON object with counts for each segments from ElasticSearch result
    for(JSONObject responseBaseSplit : responseBaseSplits) {
      Filters.Bucket bucket = mainAggregationResult.getBucketByKey((String) responseBaseSplit.get("splitName"));
      ParsedAggregation segmentAggregationResult = bucket.getAggregations().get(RANGE_AGG_PREFIX+bucket.getKeyAsString());

      if (segmentAggregationResult instanceof ParsedFilter) {
        // This specific segment aggregation is corresponding to the "default" BaseSplit (without any variableName)
        ParsedFilter other = (ParsedFilter) segmentAggregationResult;

        // Fill the "count" field of the response JSON object (for each segments)
        for(JSONObject responseSegment : (List<JSONObject>) responseBaseSplit.get("segments")) {
          responseSegment.put("count", other.getDocCount());
        }
      } else {
        // Segment aggregation is a range-aggregation.
        ParsedRange ranges = (ParsedRange) segmentAggregationResult;
        List<ParsedRange.ParsedBucket> segmentBuckets = (List<ParsedRange.ParsedBucket>) ranges.getBuckets();

        // bucketMap is an hash map for caching purpose
        Map<String, ParsedRange.ParsedBucket> bucketMap = new HashMap<>(segmentBuckets.size());
        for (ParsedRange.ParsedBucket segmentBucket : segmentBuckets) {
          bucketMap.put(segmentBucket.getKey(), segmentBucket);
        }

        // Fill the "count" field of the response JSON object (for each segments)
        for(JSONObject responseSegment : (List<JSONObject>) responseBaseSplit.get("segments")) {
          responseSegment.put("count", bucketMap.get(responseSegment.get("name")).getDocCount());
        }
      }
    }
    responseJSON.put("baseSplit", responseBaseSplits);
    // @DEBUG log.info(responseJSON.toJSONString());

    /*****************************************
     *
     *  response
     *
     *****************************************/

    response.put("result", responseJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processEvaluateProfileCriteria
  *
  *****************************************/

  private JSONObject processEvaluateProfileCriteria(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  parse
    *
    *****************************************/

    List<EvaluationCriterion> criteriaList = new ArrayList<EvaluationCriterion>();
    try
      {
        JSONArray jsonCriteriaList = JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", true);
        for (int i=0; i<jsonCriteriaList.size(); i++)
          {
            criteriaList.add(new EvaluationCriterion((JSONObject) jsonCriteriaList.get(i), CriterionContext.Profile));
          }
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("responseCode", "argumentError");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  construct query
    *
    *****************************************/

    BoolQueryBuilder query;
    try
      {
        query = QueryBuilders.boolQuery();
        for (EvaluationCriterion evaluationCriterion : criteriaList)
          {
            query = query.filter(evaluationCriterion.esQuery());
          }
      }
    catch (CriterionException e)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("responseCode", "argumentError");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  excecute query
    *
    *****************************************/

    long result;
    try
      {
        SearchRequest searchRequest = new SearchRequest("subscriberprofile").source(new SearchSourceBuilder().sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).query(query).size(0));
        SearchResponse searchResponse = elasticsearch.search(searchRequest);
        result = searchResponse.getHits().getTotalHits();
      }
    catch (IOException e)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("responseCode", "systemError");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", null);
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("result", result);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetUCGDimensionList
  *
  *****************************************/

  private JSONObject processGetUCGDimensionList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert segmentationDimensions
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> segmentationDimensions = new ArrayList<JSONObject>();
    for (GUIManagedObject segmentationDimension : segmentationDimensionService.getStoredSegmentationDimensions())
      {
        SegmentationDimension dimension = (SegmentationDimension) segmentationDimension;
        if (dimension.getHasDefaultSegment()){
          segmentationDimensions.add(segmentationDimensionService.generateResponseJSON(segmentationDimension, fullDetails, now));
        }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("segmentationDimensions", JSONUtilities.encodeArray(segmentationDimensions));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetPointList
  *
  *****************************************/

  private JSONObject processGetPointList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert Points
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> points = new ArrayList<JSONObject>();
    for (GUIManagedObject point : pointService.getStoredPoints())
      {
        points.add(pointService.generateResponseJSON(point, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("points", JSONUtilities.encodeArray(points));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetPoint
  *
  *****************************************/

  private JSONObject processGetPoint(String userID, JSONObject jsonRoot)
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

    String pointID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate point
    *
    *****************************************/

    GUIManagedObject point = pointService.getStoredPoint(pointID);
    JSONObject pointJSON = pointService.generateResponseJSON(point, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (point != null) ? "ok" : "pointNotFound");
    if (point != null) response.put("point", pointJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutPoint
  *
  *****************************************/

  private JSONObject processPutPoint(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  pointID
    *
    *****************************************/

    String pointID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (pointID == null)
      {
        //little hack here :
        //   since pointID = deliverableID (if creditable) = paymentMeanID (if debitable), we need to be sure that 
        //   deliverableID and paymentMeanID are unique, so point IDs start at position 10001
        //   NOTE : we will be in trouble when we will have more than 10000 deliverables/paymentMeans ...
        String idString = pointService.generatePointID();
        try
          {
            int id = Integer.parseInt(idString);
            pointID = String.valueOf(id > 10000 ? id : (10000 + id));
          } catch (NumberFormatException e)
          {
            throw new ServerRuntimeException("ProcessPutPoint : could not generate new ID");
          }
        jsonRoot.put("id", pointID);
      }

    /*****************************************
    *
    *  existing point
    *
    *****************************************/

    GUIManagedObject existingPoint = pointService.getStoredPoint(pointID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingPoint != null && existingPoint.getReadOnly())
      {
        response.put("id", existingPoint.getGUIManagedObjectID());
        response.put("accepted", existingPoint.getAccepted());
        response.put("valid", existingPoint.getAccepted());
        response.put("processing", pointService.isActivePoint(existingPoint, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process point
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate Point
        *
        ****************************************/

        Point point = new Point(jsonRoot, epoch, existingPoint);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        pointService.putPoint(point, (existingPoint == null), userID);

        /*****************************************
        *
        *  create related deliverable and related paymentMean
        *
        *****************************************/

        DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("pointFulfillment");
        JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
        String providerID = (String) deliveryManagerJSON.get("providerID");
        
        //
        // deliverable
        //
        if(point.getCreditable()){
          Map<String, Object> deliverableMap = new HashMap<String, Object>();
          deliverableMap.put("id", point.getPointID());
          deliverableMap.put("fulfillmentProviderID", providerID);
          deliverableMap.put("commodityID", point.getPointID());
          deliverableMap.put("name", point.getPointName());
          deliverableMap.put("display", point.getDisplay());
          deliverableMap.put("active", true);
          deliverableMap.put("unitaryCost", 0);
          Deliverable deliverable = new Deliverable(JSONUtilities.encodeObject(deliverableMap), epoch, null);
          deliverableService.putDeliverable(deliverable, true, userID);
        }
        
        //
        // paymentMean
        //
        
        if(point.getDebitable()){
          Map<String, Object> paymentMeanMap = new HashMap<String, Object>();
          paymentMeanMap.put("id", point.getPointID());
          paymentMeanMap.put("fulfillmentProviderID", providerID);
          paymentMeanMap.put("commodityID", point.getPointID());
          paymentMeanMap.put("name", point.getPointName());
          paymentMeanMap.put("display", point.getDisplay());
          paymentMeanMap.put("active", true);
          PaymentMean paymentMean = new PaymentMean(JSONUtilities.encodeObject(paymentMeanMap), epoch, null);
          paymentMeanService.putPaymentMean(paymentMean, true, userID);
        }
        
        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", point.getPointID());
        response.put("accepted", point.getAccepted());
        response.put("valid", point.getAccepted());
        response.put("processing", pointService.isActivePoint(point, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        pointService.putIncompletePoint(incompleteObject, (existingPoint == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("pointID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "pointNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemovePoint
  *
  *****************************************/

  private JSONObject processRemovePoint(String userID, JSONObject jsonRoot)
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

    String pointID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove related deliverable and related paymentMean
    *
    *****************************************/

    DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("pointFulfillment");
    JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
    String providerID = (String) deliveryManagerJSON.get("providerID");

    Collection<GUIManagedObject> deliverableObjects = deliverableService.getStoredDeliverables();
    for(GUIManagedObject deliverableObject : deliverableObjects){
      if(deliverableObject instanceof Deliverable){
        Deliverable deliverable = (Deliverable) deliverableObject;
        if(deliverable.getFulfillmentProviderID().equals(providerID) && deliverable.getCommodityID().equals(pointID)){
          deliverableService.removeDeliverable(deliverable.getDeliverableID(), "0");
        }
      }
    }
    
    Collection<GUIManagedObject> paymentMeanObjects = paymentMeanService.getStoredPaymentMeans();
    for(GUIManagedObject paymentMeanObject : paymentMeanObjects){
      if(paymentMeanObject instanceof PaymentMean){
        PaymentMean paymentMean = (PaymentMean) paymentMeanObject;
        if(paymentMean.getFulfillmentProviderID().equals(providerID) && paymentMean.getCommodityID().equals(pointID)){
          paymentMeanService.removePaymentMean(paymentMean.getPaymentMeanID(), "0");
        }
      }
    }

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject point = pointService.getStoredPoint(pointID);
    if (point != null && ! point.getReadOnly()) pointService.removePoint(pointID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (point != null && ! point.getReadOnly())
      responseCode = "ok";
    else if (point != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "pointNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetOfferList
  *
  *****************************************/

  private JSONObject processGetOfferList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert offers
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> offers = new ArrayList<JSONObject>();
    for (GUIManagedObject offer : offerService.getStoredOffers())
      {
        offers.add(offerService.generateResponseJSON(offer, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("offers", JSONUtilities.encodeArray(offers));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetOffer
  *
  *****************************************/

  private JSONObject processGetOffer(String userID, JSONObject jsonRoot)
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

    String offerID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate offer
    *
    *****************************************/

    GUIManagedObject offer = offerService.getStoredOffer(offerID);
    JSONObject offerJSON = offerService.generateResponseJSON(offer, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (offer != null) ? "ok" : "offerNotFound");
    if (offer != null) response.put("offer", offerJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutOffer
  *
  *****************************************/

  private JSONObject processPutOffer(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  offerID
    *
    *****************************************/

    String offerID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (offerID == null)
      {
        offerID = offerService.generateOfferID();
        jsonRoot.put("id", offerID);
      }

    /*****************************************
    *
    *  existing offer
    *
    *****************************************/

    GUIManagedObject existingOffer = offerService.getStoredOffer(offerID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingOffer != null && existingOffer.getReadOnly())
      {
        response.put("id", existingOffer.getGUIManagedObjectID());
        response.put("accepted", existingOffer.getAccepted());
        response.put("valid", existingOffer.getAccepted());
        response.put("processing", offerService.isActiveOffer(existingOffer, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process offer
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate offer
        *
        ****************************************/

        Offer offer = new Offer(jsonRoot, epoch, existingOffer, catalogCharacteristicService);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        offerService.putOffer(offer, callingChannelService, salesChannelService, productService, (existingOffer == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", offer.getOfferID());
        response.put("accepted", offer.getAccepted());
        response.put("valid", offer.getAccepted());
        response.put("processing", offerService.isActiveOffer(offer, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        offerService.putOffer(incompleteObject, callingChannelService, salesChannelService, productService, (existingOffer == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "offerNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveOffer
  *
  *****************************************/

  private JSONObject processRemoveOffer(String userID, JSONObject jsonRoot)
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

    String offerID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject offer = offerService.getStoredOffer(offerID);
    if (offer != null && ! offer.getReadOnly()) offerService.removeOffer(offerID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (offer != null && ! offer.getReadOnly())
      responseCode = "ok";
    else if (offer != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "offerNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetReportConfig
  *
  *****************************************/

  private JSONObject processGetReportGlobalConfiguration(String userID, JSONObject jsonRoot)
  {
    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    HashMap<String,Object> globalConfig = new HashMap<String,Object>();
    globalConfig.put("reportManagerZookeeperDir",   Deployment.getReportManagerZookeeperDir());
    globalConfig.put("reportManagerOutputPath",     Deployment.getReportManagerOutputPath());
    globalConfig.put("reportManagerDateFormat",     Deployment.getReportManagerDateFormat());
    globalConfig.put("reportManagerFileExtension",  Deployment.getReportManagerFileExtension());
    globalConfig.put("reportManagerCsvSeparator",   Deployment.getReportManagerCsvSeparator());
    globalConfig.put("reportManagerStreamsTempDir", Deployment.getReportManagerStreamsTempDir());
    response.put("reportGlobalConfiguration", JSONUtilities.encodeObject(globalConfig));
    response.put("responseCode", "ok");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetReportList
  *
  *****************************************/

  private JSONObject processGetReportList(String userID, JSONObject jsonRoot)
  {
    log.trace("In processGetReportList : "+jsonRoot);
    Date now = SystemTime.getCurrentTime();
    List<JSONObject> reports = new ArrayList<JSONObject>();
    for (GUIManagedObject report : reportService.getStoredReports())
      {
        log.trace("In processGetReportList, adding : "+report);
        JSONObject reportResponse = reportService.generateResponseJSON(report, true, now);
        reportResponse.put("isRunning", reportService.isReportRunning(((Report)report).getName()));
        reports.add(reportResponse);
      }
    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("reports", JSONUtilities.encodeArray(reports));
    log.trace("res : "+response.get("reports"));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processLaunchReport
  *
  *****************************************/

  private JSONObject processLaunchReport(String userID, JSONObject jsonRoot)
  {
    log.trace("In processLaunchReport : "+jsonRoot);
    HashMap<String,Object> response = new HashMap<String,Object>();
    String reportID = JSONUtilities.decodeString(jsonRoot, "id", true);
    Report report = (Report) reportService.getStoredReport(reportID);
    log.trace("Looking for "+reportID+" and got "+report);
    String responseCode;
    if (report == null)
      {
        responseCode = "reportNotFound";
      }
    else
      {
        if(!reportService.isReportRunning(report.getName())) {
          reportService.launchReport(report.getName());
          responseCode = "ok";
        }else {
          responseCode = "reportIsAlreadyRunning";
        }
      }
    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processDownloadReport
  *
  *****************************************/

  private void processDownloadReport(String userID, JSONObject jsonRoot, JSONObject jsonResponse, HttpExchange exchange)
  {
    String reportID = JSONUtilities.decodeString(jsonRoot, "id", true);
    GUIManagedObject report1 = reportService.getStoredReport(reportID);
    log.trace("Looking for "+reportID+" and got "+report1);
    String responseCode = null;
    
    if (report1 == null)
      {
        responseCode = "reportNotFound";
      }
    else
      {
        try
          {
            Report report = new Report(report1.getJSONRepresentation(), epochServer.getKey(), null);
            String reportName = report.getName();
            
            String outputPath = Deployment.getReportManagerOutputPath()+File.separator;
            String fileExtension = Deployment.getReportManagerFileExtension();

            File folder = new File(outputPath);
            String csvFilenameRegex = reportName+ "_"+ ".*"+ "\\."+ fileExtension;
            
            File[] listOfFiles = folder.listFiles(new FileFilter(){
              @Override
              public boolean accept(File f) {
                return Pattern.compile(csvFilenameRegex).matcher(f.getName()).matches();
              }});

            File reportFile = null;

            long lastMod = Long.MIN_VALUE;
            if(listOfFiles != null && listOfFiles.length != 0) {
              for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                  if(listOfFiles[i].lastModified() > lastMod) {
                    reportFile = listOfFiles[i];
                    lastMod = reportFile.lastModified();
                  }
                }
              }
            }else {
              responseCode = "Cant find report with that name";
            }

            if(reportFile != null && reportFile.length() != 0) {
              try {
                FileInputStream fis = new FileInputStream(reportFile);
                exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                exchange.getResponseHeaders().add("Content-Disposition", "attachment; filename=" + reportFile.getName());
                exchange.sendResponseHeaders(200, reportFile.length());
                OutputStream os = exchange.getResponseBody();
                int c;
                while ((c = fis.read()) != -1) {
                  os.write(c);
                }
                fis.close();
                os.flush();
                os.close();
              } catch (Exception excp) {
                StringWriter stackTraceWriter = new StringWriter();
                excp.printStackTrace(new PrintWriter(stackTraceWriter, true));
                log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
              }
            }else {
              responseCode = "Report is empty";
            }
          }
        catch (GUIManagerException e)
          {
            log.info("Exception when building report from "+report1+" : "+e.getLocalizedMessage());
            responseCode = "internalError";
          }
      }
    if(responseCode != null) {
      try {
        jsonResponse.put("responseCode", responseCode);
        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }catch(Exception e) {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
      }
    }
  }

  /*****************************************
  *
  *  processPutReport
  *
  *****************************************/

  private JSONObject processPutReport(String userID, JSONObject jsonRoot)
  {
    log.trace("In processPutReport : "+jsonRoot);
    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    String reportID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (reportID == null)
      {
        reportID = reportService.generateReportID();
        jsonRoot.put("id", reportID);
      }
    log.trace("ID : "+reportID);
    GUIManagedObject existingReport = reportService.getStoredReport(reportID);
    if (existingReport != null && existingReport.getReadOnly())
    {
      log.trace("existingReport : "+existingReport);
      response.put("id", existingReport.getGUIManagedObjectID());
      response.put("accepted", existingReport.getAccepted());
      response.put("valid", existingReport.getAccepted());
      response.put("processing", reportService.isActiveReport(existingReport, now));
      response.put("responseCode", "failedReadOnly");
      return JSONUtilities.encodeObject(response);
    }
    if (existingReport != null) {
      Report existingRept = (Report) existingReport;
      // Is the new effective scheduling valid ?
      List<SchedulingInterval> availableScheduling = existingRept.getAvailableScheduling();
      if (availableScheduling != null) {
        // Check if the effectiveScheduling is valid
        JSONArray effectiveSchedulingJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, Report.EFFECTIVE_SCHEDULING, false);
        if (effectiveSchedulingJSONArray != null) { 
          for (int i=0; i<effectiveSchedulingJSONArray.size(); i++) {
            String schedulingIntervalStr = (String) effectiveSchedulingJSONArray.get(i);
            SchedulingInterval eSchedule = SchedulingInterval.fromExternalRepresentation(schedulingIntervalStr);
            log.trace("Checking that "+eSchedule+" is allowed");
            if (! availableScheduling.contains(eSchedule)) {
              response.put("id", jsonRoot.get("id"));
              response.put("responseCode", "reportNotValid");
              response.put("responseMessage", "scheduling "+eSchedule+" is not valid");
              StringBuffer respMsg = new StringBuffer("scheduling "+eSchedule+" should be part of [ ");
              for (SchedulingInterval aSched : availableScheduling) {
                respMsg.append(aSched+" ");
              }
              respMsg.append("]");
              response.put("responseParameter", respMsg.toString());
              return JSONUtilities.encodeObject(response);
            }
          }
        }
      }
    }
    long epoch = epochServer.getKey();
    try
      {
        Report report = new Report(jsonRoot, epoch, null);
        log.trace("new report : "+report);
        reportService.putReport(report, true, userID);
        response.put("id", report.getReportID());
        response.put("accepted", report.getAccepted());
        response.put("valid", report.getAccepted());
        response.put("processing", reportService.isActiveReport(report, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
        response.put("id", jsonRoot.get("id"));
        response.put("responseCode", "reportNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processGetPresentationStrategyList
  *
  *****************************************/

  private JSONObject processGetPresentationStrategyList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert presentationStrategies
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> presentationStrategies = new ArrayList<JSONObject>();
    for (GUIManagedObject presentationStrategy : presentationStrategyService.getStoredPresentationStrategies())
      {
        presentationStrategies.add(presentationStrategyService.generateResponseJSON(presentationStrategy, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("presentationStrategies", JSONUtilities.encodeArray(presentationStrategies));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetPresentationStrategy
  *
  *****************************************/

  private JSONObject processGetPresentationStrategy(String userID, JSONObject jsonRoot)
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

    String presentationStrategyID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate presentation strategy
    *
    *****************************************/

    GUIManagedObject presentationStrategy = presentationStrategyService.getStoredPresentationStrategy(presentationStrategyID);
    JSONObject presentationStrategyJSON = presentationStrategyService.generateResponseJSON(presentationStrategy, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (presentationStrategy != null) ? "ok" : "presentationStrategyNotFound");
    if (presentationStrategy != null) response.put("presentationStrategy", presentationStrategyJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutPresentationStrategy
  *
  *****************************************/

  private JSONObject processPutPresentationStrategy(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  presentationStrategyID
    *
    *****************************************/

    String presentationStrategyID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (presentationStrategyID == null)
      {
        presentationStrategyID = presentationStrategyService.generatePresentationStrategyID();
        jsonRoot.put("id", presentationStrategyID);
      }

    /*****************************************
    *
    *  existing presentationStrategy
    *
    *****************************************/

    GUIManagedObject existingPresentationStrategy = presentationStrategyService.getStoredPresentationStrategy(presentationStrategyID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingPresentationStrategy != null && existingPresentationStrategy.getReadOnly())
      {
        response.put("id", existingPresentationStrategy.getGUIManagedObjectID());
        response.put("accepted", existingPresentationStrategy.getAccepted());
        response.put("valid", existingPresentationStrategy.getAccepted());
        response.put("processing", presentationStrategyService.isActivePresentationStrategy(existingPresentationStrategy, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process presentationStrategy
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate presentationStrategy
        *
        ****************************************/

        PresentationStrategy presentationStrategy = new PresentationStrategy(jsonRoot, epoch, existingPresentationStrategy);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        presentationStrategyService.putPresentationStrategy(presentationStrategy, scoringStrategyService, (existingPresentationStrategy == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", presentationStrategy.getPresentationStrategyID());
        response.put("accepted", presentationStrategy.getAccepted());
        response.put("valid", presentationStrategy.getAccepted());
        response.put("processing", presentationStrategyService.isActivePresentationStrategy(presentationStrategy, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        presentationStrategyService.putPresentationStrategy(incompleteObject, scoringStrategyService, (existingPresentationStrategy == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "presentationStrategyNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemovePresentationStrategy
  *
  *****************************************/

  private JSONObject processRemovePresentationStrategy(String userID, JSONObject jsonRoot)
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

    String presentationStrategyID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject presentationStrategy = presentationStrategyService.getStoredPresentationStrategy(presentationStrategyID);
    if (presentationStrategy != null && ! presentationStrategy.getReadOnly()) presentationStrategyService.removePresentationStrategy(presentationStrategyID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (presentationStrategy != null && ! presentationStrategy.getReadOnly())
      responseCode = "ok";
    else if (presentationStrategy != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "presentationStrategyNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetScoringStrategyList
  *
  *****************************************/

  private JSONObject processGetScoringStrategyList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert scoringStrategies
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> scoringStrategies = new ArrayList<JSONObject>();
    for (GUIManagedObject scoringStrategy : scoringStrategyService.getStoredScoringStrategies())
      {
        scoringStrategies.add(scoringStrategyService.generateResponseJSON(scoringStrategy, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("scoringStrategies", JSONUtilities.encodeArray(scoringStrategies));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetScoringStrategy
  *
  *****************************************/

  private JSONObject processGetScoringStrategy(String userID, JSONObject jsonRoot)
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

    String scoringStrategyID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject scoringStrategy = scoringStrategyService.getStoredScoringStrategy(scoringStrategyID);
    JSONObject scoringStrategyJSON = scoringStrategyService.generateResponseJSON(scoringStrategy, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (scoringStrategy != null) ? "ok" : "scoringStrategyNotFound");
    if (scoringStrategy != null) response.put("scoringStrategy", scoringStrategyJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutScoringStrategy
  *
  *****************************************/

  private JSONObject processPutScoringStrategy(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  scoringStrategyID
    *
    *****************************************/

    String scoringStrategyID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (scoringStrategyID == null)
      {
        scoringStrategyID = scoringStrategyService.generateScoringStrategyID();
        jsonRoot.put("id", scoringStrategyID);
      }

    /*****************************************
    *
    *  existing scoringStrategy
    *
    *****************************************/

    GUIManagedObject existingScoringStrategy = scoringStrategyService.getStoredScoringStrategy(scoringStrategyID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingScoringStrategy != null && existingScoringStrategy.getReadOnly())
      {
        response.put("id", existingScoringStrategy.getGUIManagedObjectID());
        response.put("accepted", existingScoringStrategy.getAccepted());
        response.put("valid", existingScoringStrategy.getAccepted());
        response.put("processing", scoringStrategyService.isActiveScoringStrategy(existingScoringStrategy, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process scoringStrategy
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate scoringStrategy
        *
        ****************************************/

        ScoringStrategy scoringStrategy = new ScoringStrategy(jsonRoot, epoch, existingScoringStrategy);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        scoringStrategyService.putScoringStrategy(scoringStrategy, (existingScoringStrategy == null), userID);

        /*****************************************
        *
        *  revalidatePresentationStrategies
        *
        *****************************************/

        revalidatePresentationStrategies(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", scoringStrategy.getScoringStrategyID());
        response.put("accepted", scoringStrategy.getAccepted());
        response.put("valid", scoringStrategy.getAccepted());
        response.put("processing", scoringStrategyService.isActiveScoringStrategy(scoringStrategy, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        scoringStrategyService.putScoringStrategy(incompleteObject, (existingScoringStrategy == null), userID);

        //
        //  revalidatePresentationStrategies
        //

        revalidatePresentationStrategies(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "scoringStrategyNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveScoringStrategy
  *
  *****************************************/

  private JSONObject processRemoveScoringStrategy(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String scoringStrategyID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject scoringStrategy = scoringStrategyService.getStoredScoringStrategy(scoringStrategyID);
    if (scoringStrategy != null && ! scoringStrategy.getReadOnly()) scoringStrategyService.removeScoringStrategy(scoringStrategyID, userID);

    /*****************************************
    *
    *  revalidatePresentationStrategies
    *
    *****************************************/

    revalidatePresentationStrategies(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (scoringStrategy != null && ! scoringStrategy.getReadOnly())
      responseCode = "ok";
    else if (scoringStrategy != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "scoringStrategyNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetCallingChannelList
  *
  *****************************************/

  private JSONObject processGetCallingChannelList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert callingChannels
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> callingChannels = new ArrayList<JSONObject>();
    for (GUIManagedObject callingChannel : callingChannelService.getStoredCallingChannels())
      {
        callingChannels.add(callingChannelService.generateResponseJSON(callingChannel, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("callingChannels", JSONUtilities.encodeArray(callingChannels));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetCallingChannel
  *
  *****************************************/

  private JSONObject processGetCallingChannel(String userID, JSONObject jsonRoot)
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

    String callingChannelID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject callingChannel = callingChannelService.getStoredCallingChannel(callingChannelID);
    JSONObject callingChannelJSON = callingChannelService.generateResponseJSON(callingChannel, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (callingChannel != null) ? "ok" : "callingChannelNotFound");
    if (callingChannel != null) response.put("callingChannel", callingChannelJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutCallingChannel
  *
  *****************************************/

  private JSONObject processPutCallingChannel(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  callingChannelID
    *
    *****************************************/

    String callingChannelID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (callingChannelID == null)
      {
        callingChannelID = callingChannelService.generateCallingChannelID();
        jsonRoot.put("id", callingChannelID);
      }

    /*****************************************
    *
    *  existing callingChannel
    *
    *****************************************/

    GUIManagedObject existingCallingChannel = callingChannelService.getStoredCallingChannel(callingChannelID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingCallingChannel != null && existingCallingChannel.getReadOnly())
      {
        response.put("id", existingCallingChannel.getGUIManagedObjectID());
        response.put("accepted", existingCallingChannel.getAccepted());
        response.put("valid", existingCallingChannel.getAccepted());
        response.put("processing", callingChannelService.isActiveCallingChannel(existingCallingChannel, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process callingChannel
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate callingChannel
        *
        ****************************************/

        CallingChannel callingChannel = new CallingChannel(jsonRoot, epoch, existingCallingChannel);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        callingChannelService.putCallingChannel(callingChannel, (existingCallingChannel == null), userID);

        /*****************************************
        *
        *  revalidateOffers
        *
        *****************************************/

        revalidateSalesChannels(now);
        revalidateOffers(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", callingChannel.getCallingChannelID());
        response.put("accepted", callingChannel.getAccepted());
        response.put("valid", callingChannel.getAccepted());
        response.put("processing", callingChannelService.isActiveCallingChannel(callingChannel, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        callingChannelService.putCallingChannel(incompleteObject, (existingCallingChannel == null), userID);

        //
        //  revalidateOffers
        //

        revalidateSalesChannels(now);
        revalidateOffers(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "callingChannelNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveCallingChannel
  *
  *****************************************/

  private JSONObject processRemoveCallingChannel(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String callingChannelID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject callingChannel = callingChannelService.getStoredCallingChannel(callingChannelID);
    if (callingChannel != null && ! callingChannel.getReadOnly()) callingChannelService.removeCallingChannel(callingChannelID, userID);

    /*****************************************
    *
    *  revalidateOffers
    *
    *****************************************/

    revalidateSalesChannels(now);
    revalidateOffers(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (callingChannel != null && ! callingChannel.getReadOnly())
      responseCode = "ok";
    else if (callingChannel != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "callingChannelNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetSalesChannelList
  *
  *****************************************/

  private JSONObject processGetSalesChannelList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert salesChannels
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> salesChannels = new ArrayList<JSONObject>();
    for (GUIManagedObject salesChannel : salesChannelService.getStoredSalesChannels())
      {
        salesChannels.add(salesChannelService.generateResponseJSON(salesChannel, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("salesChannels", JSONUtilities.encodeArray(salesChannels));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetSalesChannel
  *
  *****************************************/

  private JSONObject processGetSalesChannel(String userID, JSONObject jsonRoot)
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

    String salesChannelID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject salesChannel = salesChannelService.getStoredSalesChannel(salesChannelID);
    JSONObject salesChannelJSON = salesChannelService.generateResponseJSON(salesChannel, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (salesChannel != null) ? "ok" : "salesChannelNotFound");
    if (salesChannel != null) response.put("salesChannel", salesChannelJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutSalesChannel
  *
  *****************************************/

  private JSONObject processPutSalesChannel(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  salesChannelID
    *
    *****************************************/

    String salesChannelID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (salesChannelID == null)
      {
        salesChannelID = salesChannelService.generateSalesChannelID();
        jsonRoot.put("id", salesChannelID);
      }

    /*****************************************
    *
    *  existing salesChannel
    *
    *****************************************/

    GUIManagedObject existingSalesChannel = salesChannelService.getStoredSalesChannel(salesChannelID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingSalesChannel != null && existingSalesChannel.getReadOnly())
      {
        response.put("id", existingSalesChannel.getGUIManagedObjectID());
        response.put("accepted", existingSalesChannel.getAccepted());
        response.put("valid", existingSalesChannel.getAccepted());
        response.put("processing", salesChannelService.isActiveSalesChannel(existingSalesChannel, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process salesChannel
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate salesChannel
        *
        ****************************************/

        SalesChannel salesChannel = new SalesChannel(jsonRoot, epoch, existingSalesChannel);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        salesChannelService.putSalesChannel(salesChannel, callingChannelService, (existingSalesChannel == null), userID);

        /*****************************************
        *
        *  revalidateOffers
        *
        *****************************************/

        revalidateOffers(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", salesChannel.getSalesChannelID());
        response.put("accepted", salesChannel.getAccepted());
        response.put("valid", salesChannel.getAccepted());
        response.put("processing", salesChannelService.isActiveSalesChannel(salesChannel, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        salesChannelService.putSalesChannel(incompleteObject, callingChannelService, (existingSalesChannel == null), userID);

        //
        //  revalidateOffers
        //

        revalidateOffers(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "salesChannelNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveSalesChannel
  *
  *****************************************/

  private JSONObject processRemoveSalesChannel(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String salesChannelID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject salesChannel = salesChannelService.getStoredSalesChannel(salesChannelID);
    if (salesChannel != null && ! salesChannel.getReadOnly()) salesChannelService.removeSalesChannel(salesChannelID, userID);

    /*****************************************
    *
    *  revalidateOffers
    *
    *****************************************/

    revalidateOffers(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (salesChannel != null && ! salesChannel.getReadOnly())
      responseCode = "ok";
    else if (salesChannel != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "salesChannelNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetSupplierList
  *
  *****************************************/

  private JSONObject processGetSupplierList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert suppliers
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> suppliers = new ArrayList<JSONObject>();
    for (GUIManagedObject supplier : supplierService.getStoredSuppliers())
      {
        suppliers.add(supplierService.generateResponseJSON(supplier, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("suppliers", JSONUtilities.encodeArray(suppliers));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetSupplier
  *
  *****************************************/

  private JSONObject processGetSupplier(String userID, JSONObject jsonRoot)
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

    String supplierID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject supplier = supplierService.getStoredSupplier(supplierID);
    JSONObject supplierJSON = supplierService.generateResponseJSON(supplier, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (supplier != null) ? "ok" : "supplierNotFound");
    if (supplier != null) response.put("supplier", supplierJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutSupplier
  *
  *****************************************/

  private JSONObject processPutSupplier(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  supplierID
    *
    *****************************************/

    String supplierID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (supplierID == null)
      {
        supplierID = supplierService.generateSupplierID();
        jsonRoot.put("id", supplierID);
      }

    /*****************************************
    *
    *  existing supplier
    *
    *****************************************/

    GUIManagedObject existingSupplier = supplierService.getStoredSupplier(supplierID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingSupplier != null && existingSupplier.getReadOnly())
      {
        response.put("id", existingSupplier.getGUIManagedObjectID());
        response.put("accepted", existingSupplier.getAccepted());
        response.put("valid", existingSupplier.getAccepted());
        response.put("processing", supplierService.isActiveSupplier(existingSupplier, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process supplier
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate supplier
        *
        ****************************************/

        Supplier supplier = new Supplier(jsonRoot, epoch, existingSupplier);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        supplierService.putSupplier(supplier, (existingSupplier == null), userID);

        /*****************************************
        *
        *  revalidateProducts
        *
        *****************************************/

        revalidateProducts(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", supplier.getSupplierID());
        response.put("accepted", supplier.getAccepted());
        response.put("valid", supplier.getAccepted());
        response.put("processing", supplierService.isActiveSupplier(supplier, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        supplierService.putSupplier(incompleteObject, (existingSupplier == null), userID);

        //
        //  revalidateProducts
        //

        revalidateProducts(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "supplierNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveSupplier
  *
  *****************************************/

  private JSONObject processRemoveSupplier(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String supplierID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject supplier = supplierService.getStoredSupplier(supplierID);
    if (supplier != null && ! supplier.getReadOnly()) supplierService.removeSupplier(supplierID, userID);

    /*****************************************
    *
    *  revalidateProducts
    *
    *****************************************/

    revalidateProducts(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (supplier != null && ! supplier.getReadOnly())
      responseCode = "ok";
    else if (supplier != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "supplierNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetProductList
  *
  *****************************************/

  private JSONObject processGetProductList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert products
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> products = new ArrayList<JSONObject>();
    for (GUIManagedObject product : productService.getStoredProducts())
      {
        products.add(productService.generateResponseJSON(product, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("products", JSONUtilities.encodeArray(products));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetProduct
  *
  *****************************************/

  private JSONObject processGetProduct(String userID, JSONObject jsonRoot)
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

    String productID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject product = productService.getStoredProduct(productID);
    JSONObject productJSON = productService.generateResponseJSON(product, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (product != null) ? "ok" : "productNotFound");
    if (product != null) response.put("product", productJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutProduct
  *
  *****************************************/

  private JSONObject processPutProduct(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  productID
    *
    *****************************************/

    String productID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (productID == null)
      {
        productID = productService.generateProductID();
        jsonRoot.put("id", productID);
      }

    /*****************************************
    *
    *  existing product
    *
    *****************************************/

    GUIManagedObject existingProduct = productService.getStoredProduct(productID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingProduct != null && existingProduct.getReadOnly())
      {
        response.put("id", existingProduct.getGUIManagedObjectID());
        response.put("accepted", existingProduct.getAccepted());
        response.put("valid", existingProduct.getAccepted());
        response.put("processing", productService.isActiveProduct(existingProduct, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process product
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate product
        *
        ****************************************/

        Product product = new Product(jsonRoot, epoch, existingProduct, catalogCharacteristicService);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        productService.putProduct(product, supplierService, productTypeService, deliverableService, (existingProduct == null), userID);

        /*****************************************
        *
        *  revalidateOffers
        *
        *****************************************/

        revalidateOffers(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", product.getProductID());
        response.put("accepted", product.getAccepted());
        response.put("valid", product.getAccepted());
        response.put("processing", productService.isActiveProduct(product, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        productService.putProduct(incompleteObject, supplierService, productTypeService, deliverableService, (existingProduct == null), userID);

        //
        //  revalidateOffers
        //

        revalidateOffers(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "productNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveProduct
  *
  *****************************************/

  private JSONObject processRemoveProduct(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String productID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject product = productService.getStoredProduct(productID);
    if (product != null && ! product.getReadOnly()) productService.removeProduct(productID, userID);

    /*****************************************
    *
    *  revalidateOffers
    *
    *****************************************/

    revalidateOffers(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (product != null && ! product.getReadOnly())
      responseCode = "ok";
    else if (product != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "productNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetCatalogCharacteristicList
  *
  *****************************************/

  private JSONObject processGetCatalogCharacteristicList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert catalogCharacteristics
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> catalogCharacteristics = new ArrayList<JSONObject>();
    for (GUIManagedObject catalogCharacteristic : catalogCharacteristicService.getStoredCatalogCharacteristics())
      {
        catalogCharacteristics.add(catalogCharacteristicService.generateResponseJSON(catalogCharacteristic, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("catalogCharacteristics", JSONUtilities.encodeArray(catalogCharacteristics));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetCatalogCharacteristic
  *
  *****************************************/

  private JSONObject processGetCatalogCharacteristic(String userID, JSONObject jsonRoot)
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

    String catalogCharacteristicID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject catalogCharacteristic = catalogCharacteristicService.getStoredCatalogCharacteristic(catalogCharacteristicID);
    JSONObject catalogCharacteristicJSON = catalogCharacteristicService.generateResponseJSON(catalogCharacteristic, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (catalogCharacteristic != null) ? "ok" : "catalogCharacteristicNotFound");
    if (catalogCharacteristic != null) response.put("catalogCharacteristic", catalogCharacteristicJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutCatalogCharacteristic
  *
  *****************************************/

  private JSONObject processPutCatalogCharacteristic(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  catalogCharacteristicID
    *
    *****************************************/

    String catalogCharacteristicID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (catalogCharacteristicID == null)
      {
        catalogCharacteristicID = catalogCharacteristicService.generateCatalogCharacteristicID();
        jsonRoot.put("id", catalogCharacteristicID);
      }

    /*****************************************
    *
    *  existing catalogCharacteristic
    *
    *****************************************/

    GUIManagedObject existingCatalogCharacteristic = catalogCharacteristicService.getStoredCatalogCharacteristic(catalogCharacteristicID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingCatalogCharacteristic != null && existingCatalogCharacteristic.getReadOnly())
      {
        response.put("id", existingCatalogCharacteristic.getGUIManagedObjectID());
        response.put("accepted", existingCatalogCharacteristic.getAccepted());
        response.put("valid", existingCatalogCharacteristic.getAccepted());
        response.put("processing", catalogCharacteristicService.isActiveCatalogCharacteristic(existingCatalogCharacteristic, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process catalogCharacteristic
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate catalogCharacteristic
        *
        ****************************************/

        CatalogCharacteristic catalogCharacteristic = new CatalogCharacteristic(jsonRoot, epoch, existingCatalogCharacteristic);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        catalogCharacteristicService.putCatalogCharacteristic(catalogCharacteristic, (existingCatalogCharacteristic == null), userID);

        /*****************************************
        *
        *  revalidate dependent objects
        *
        *****************************************/

        revalidateOffers(now);
        revalidateJourneyObjectives(now);
        revalidateOfferObjectives(now);
        revalidateProductTypes(now);
        revalidateProducts(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", catalogCharacteristic.getCatalogCharacteristicID());
        response.put("accepted", catalogCharacteristic.getAccepted());
        response.put("valid", catalogCharacteristic.getAccepted());
        response.put("processing", catalogCharacteristicService.isActiveCatalogCharacteristic(catalogCharacteristic, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        catalogCharacteristicService.putCatalogCharacteristic(incompleteObject, (existingCatalogCharacteristic == null), userID);

        //
        //  revalidate dependent objects
        //

        revalidateOffers(now);
        revalidateJourneyObjectives(now);
        revalidateOfferObjectives(now);
        revalidateProductTypes(now);
        revalidateProducts(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "catalogCharacteristicNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveCatalogCharacteristic
  *
  *****************************************/

  private JSONObject processRemoveCatalogCharacteristic(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String catalogCharacteristicID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject catalogCharacteristic = catalogCharacteristicService.getStoredCatalogCharacteristic(catalogCharacteristicID);
    if (catalogCharacteristic != null && ! catalogCharacteristic.getReadOnly()) catalogCharacteristicService.removeCatalogCharacteristic(catalogCharacteristicID, userID);

    /*****************************************
    *
    *  revalidate dependent objects
    *
    *****************************************/

    revalidateOffers(now);
    revalidateJourneyObjectives(now);
    revalidateOfferObjectives(now);
    revalidateProductTypes(now);
    revalidateProducts(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (catalogCharacteristic != null && ! catalogCharacteristic.getReadOnly())
      responseCode = "ok";
    else if (catalogCharacteristic != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "catalogCharacteristicNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetContactPolicyList
  *
  *****************************************/

  private JSONObject processGetContactPolicyList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert contactPolicies
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> contactPolicies = new ArrayList<JSONObject>();
    for (GUIManagedObject contactPolicy : contactPolicyService.getStoredContactPolicies())
      {
        contactPolicies.add(contactPolicyService.generateResponseJSON(contactPolicy, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("contactPolicies", JSONUtilities.encodeArray(contactPolicies));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetContactPolicy
  *
  *****************************************/

  private JSONObject processGetContactPolicy(String userID, JSONObject jsonRoot)
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

    String contactPolicyID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject contactPolicy = contactPolicyService.getStoredContactPolicy(contactPolicyID);
    JSONObject contactPolicyJSON = contactPolicyService.generateResponseJSON(contactPolicy, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (contactPolicy != null) ? "ok" : "contactPolicyNotFound");
    if (contactPolicy != null) response.put("contactPolicy", contactPolicyJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutContactPolicy
  *
  *****************************************/

  private JSONObject processPutContactPolicy(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  contactPolicyID
    *
    *****************************************/

    String contactPolicyID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (contactPolicyID == null)
      {
        contactPolicyID = contactPolicyService.generateContactPolicyID();
        jsonRoot.put("id", contactPolicyID);
      }

    /*****************************************
    *
    *  existing contactPolicy
    *
    *****************************************/

    GUIManagedObject existingContactPolicy = contactPolicyService.getStoredContactPolicy(contactPolicyID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingContactPolicy != null && existingContactPolicy.getReadOnly())
      {
        response.put("id", existingContactPolicy.getGUIManagedObjectID());
        response.put("accepted", existingContactPolicy.getAccepted());
        response.put("valid", existingContactPolicy.getAccepted());
        response.put("processing", contactPolicyService.isActiveContactPolicy(existingContactPolicy, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process contactPolicy
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate contactPolicy
        *
        ****************************************/

        ContactPolicy contactPolicy = new ContactPolicy(jsonRoot, epoch, existingContactPolicy);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        contactPolicyService.putContactPolicy(contactPolicy, (existingContactPolicy == null), userID);

        /*****************************************
        *
        *  revalidate dependent objects
        *
        *****************************************/

        revalidateJourneyObjectives(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", contactPolicy.getContactPolicyID());
        response.put("accepted", contactPolicy.getAccepted());
        response.put("valid", contactPolicy.getAccepted());
        response.put("processing", contactPolicyService.isActiveContactPolicy(contactPolicy, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        contactPolicyService.putContactPolicy(incompleteObject, (existingContactPolicy == null), userID);

        //
        //  revalidate dependent objects
        //

        revalidateJourneyObjectives(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "contactPolicyNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveContactPolicy
  *
  *****************************************/

  private JSONObject processRemoveContactPolicy(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String contactPolicyID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject contactPolicy = contactPolicyService.getStoredContactPolicy(contactPolicyID);
    if (contactPolicy != null && ! contactPolicy.getReadOnly()) contactPolicyService.removeContactPolicy(contactPolicyID, userID);

    /*****************************************
    *
    *  revalidate dependent objects
    *
    *****************************************/

    revalidateJourneyObjectives(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (contactPolicy != null && ! contactPolicy.getReadOnly())
      responseCode = "ok";
    else if (contactPolicy != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "contactPolicyNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetJourneyObjectiveList
  *
  *****************************************/

  private JSONObject processGetJourneyObjectiveList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert journeyObjectives
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> journeyObjectives = new ArrayList<JSONObject>();
    for (GUIManagedObject journeyObjective : journeyObjectiveService.getStoredJourneyObjectives())
      {
        journeyObjectives.add(journeyObjectiveService.generateResponseJSON(journeyObjective, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("journeyObjectives", JSONUtilities.encodeArray(journeyObjectives));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetJourneyObjective
  *
  *****************************************/

  private JSONObject processGetJourneyObjective(String userID, JSONObject jsonRoot)
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

    String journeyObjectiveID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/
    
    Date now = SystemTime.getCurrentTime();
    GUIManagedObject journeyObjective = journeyObjectiveService.getStoredJourneyObjective(journeyObjectiveID);
    JSONObject journeyObjectiveJSON = journeyObjectiveService.generateResponseJSON(journeyObjective, true, now);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (journeyObjective != null) ? "ok" : "journeyObjectiveNotFound");
    if (journeyObjective != null) response.put("journeyObjective", journeyObjectiveJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutJourneyObjective
  *
  *****************************************/

  private JSONObject processPutJourneyObjective(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  journeyObjectiveID
    *
    *****************************************/

    String journeyObjectiveID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (journeyObjectiveID == null)
      {
        journeyObjectiveID = journeyObjectiveService.generateJourneyObjectiveID();
        jsonRoot.put("id", journeyObjectiveID);
      }

    /*****************************************
    *
    *  existing journeyObjective
    *
    *****************************************/

    GUIManagedObject existingJourneyObjective = journeyObjectiveService.getStoredJourneyObjective(journeyObjectiveID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingJourneyObjective != null && existingJourneyObjective.getReadOnly())
      {
        response.put("id", existingJourneyObjective.getGUIManagedObjectID());
        response.put("accepted", existingJourneyObjective.getAccepted());
        response.put("valid", existingJourneyObjective.getAccepted());
        response.put("processing", journeyObjectiveService.isActiveJourneyObjective(existingJourneyObjective, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process journeyObjective
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate journeyObjective
        *
        ****************************************/

        JourneyObjective journeyObjective = new JourneyObjective(jsonRoot, epoch, existingJourneyObjective);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyObjectiveService.putJourneyObjective(journeyObjective, journeyObjectiveService, contactPolicyService, catalogCharacteristicService, (existingJourneyObjective == null), userID);

        /*****************************************
        *
        *  revalidate dependent objects
        *
        *****************************************/

        revalidateJourneys(now);
        revalidateJourneyObjectives(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", journeyObjective.getJourneyObjectiveID());
        response.put("accepted", journeyObjective.getAccepted());
        response.put("valid", journeyObjective.getAccepted());
        response.put("processing", journeyObjectiveService.isActiveJourneyObjective(journeyObjective, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        journeyObjectiveService.putJourneyObjective(incompleteObject, journeyObjectiveService, contactPolicyService, catalogCharacteristicService, (existingJourneyObjective == null), userID);

        //
        //  revalidate dependent objects
        //

        revalidateJourneys(now);
        revalidateJourneyObjectives(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "journeyObjectiveNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveJourneyObjective
  *
  *****************************************/

  private JSONObject processRemoveJourneyObjective(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String journeyObjectiveID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject journeyObjective = journeyObjectiveService.getStoredJourneyObjective(journeyObjectiveID);
    if (journeyObjective != null && ! journeyObjective.getReadOnly()) journeyObjectiveService.removeJourneyObjective(journeyObjectiveID, userID);

    /*****************************************
    *
    *  revalidate dependent objects
    *
    *****************************************/

    revalidateJourneys(now);
    revalidateJourneyObjectives(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (journeyObjective != null && ! journeyObjective.getReadOnly())
      responseCode = "ok";
    else if (journeyObjective != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "journeyObjectiveNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetOfferObjectiveList
  *
  *****************************************/

  private JSONObject processGetOfferObjectiveList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert offerObjectives
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> offerObjectives = new ArrayList<JSONObject>();
    for (GUIManagedObject offerObjective : offerObjectiveService.getStoredOfferObjectives())
      {
        offerObjectives.add(offerObjectiveService.generateResponseJSON(offerObjective, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("offerObjectives", JSONUtilities.encodeArray(offerObjectives));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetOfferObjective
  *
  *****************************************/

  private JSONObject processGetOfferObjective(String userID, JSONObject jsonRoot)
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

    String offerObjectiveID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject offerObjective = offerObjectiveService.getStoredOfferObjective(offerObjectiveID);
    JSONObject offerObjectiveJSON = offerObjectiveService.generateResponseJSON(offerObjective, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (offerObjective != null) ? "ok" : "offerObjectiveNotFound");
    if (offerObjective != null) response.put("offerObjective", offerObjectiveJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutOfferObjective
  *
  *****************************************/

  private JSONObject processPutOfferObjective(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  offerObjectiveID
    *
    *****************************************/

    String offerObjectiveID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (offerObjectiveID == null)
      {
        offerObjectiveID = offerObjectiveService.generateOfferObjectiveID();
        jsonRoot.put("id", offerObjectiveID);
      }

    /*****************************************
    *
    *  existing offerObjective
    *
    *****************************************/

    GUIManagedObject existingOfferObjective = offerObjectiveService.getStoredOfferObjective(offerObjectiveID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingOfferObjective != null && existingOfferObjective.getReadOnly())
      {
        response.put("id", existingOfferObjective.getGUIManagedObjectID());
        response.put("accepted", existingOfferObjective.getAccepted());
        response.put("valid", existingOfferObjective.getAccepted());
        response.put("processing", offerObjectiveService.isActiveOfferObjective(existingOfferObjective, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process offerObjective
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate offerObjective
        *
        ****************************************/

        OfferObjective offerObjective = new OfferObjective(jsonRoot, epoch, existingOfferObjective);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        offerObjectiveService.putOfferObjective(offerObjective, (existingOfferObjective == null), userID);

        /*****************************************
        *
        *  revalidate dependent objects
        *
        *****************************************/

        revalidateOffers(now);
        revalidateScoringStrategies(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", offerObjective.getOfferObjectiveID());
        response.put("accepted", offerObjective.getAccepted());
        response.put("valid", offerObjective.getAccepted());
        response.put("processing", offerObjectiveService.isActiveOfferObjective(offerObjective, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        offerObjectiveService.putOfferObjective(incompleteObject, (existingOfferObjective == null), userID);

        //
        //  revalidate dependent objects
        //

        revalidateOffers(now);
        revalidateScoringStrategies(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "offerObjectiveNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveOfferObjective
  *
  *****************************************/

  private JSONObject processRemoveOfferObjective(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String offerObjectiveID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject offerObjective = offerObjectiveService.getStoredOfferObjective(offerObjectiveID);
    if (offerObjective != null && ! offerObjective.getReadOnly()) offerObjectiveService.removeOfferObjective(offerObjectiveID, userID);

    /*****************************************
    *
    *  revalidate dependent objects
    *
    *****************************************/

    revalidateOffers(now);
    revalidateScoringStrategies(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (offerObjective != null && ! offerObjective.getReadOnly())
      responseCode = "ok";
    else if (offerObjective != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "offerObjectiveNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetProductTypeList
  *
  *****************************************/

  private JSONObject processGetProductTypeList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert productTypes
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> productTypes = new ArrayList<JSONObject>();
    for (GUIManagedObject productType : productTypeService.getStoredProductTypes())
      {
        productTypes.add(productTypeService.generateResponseJSON(productType, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("productTypes", JSONUtilities.encodeArray(productTypes));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processGetFulfillmentProviders
  *
  *****************************************/

  private JSONObject processGetFulfillmentProviders(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve fulfillment providers
    *
    *****************************************/
    
    List<JSONObject> fulfillmentProviders = new ArrayList<JSONObject>();
    for(DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values()){
      CommodityType commodityType = CommodityType.fromExternalRepresentation(deliveryManager.getRequestClassName());
      if(commodityType != null){
        JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
        Map<String, String> providerJSON = new HashMap<String, String>();
        providerJSON.put("id", (String) deliveryManagerJSON.get("providerID"));
        providerJSON.put("name", (String) deliveryManagerJSON.get("providerName"));
        providerJSON.put("providerType", commodityType.toString());
        providerJSON.put("deliveryType", deliveryManager.getDeliveryType());
        providerJSON.put("url", (String) deliveryManagerJSON.get("url"));
        FulfillmentProvider provider = new FulfillmentProvider(JSONUtilities.encodeObject(providerJSON));
        fulfillmentProviders.add(provider.getJSONRepresentation());
      } 
    }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("fulfillmentProviders", JSONUtilities.encodeArray(fulfillmentProviders));
    return JSONUtilities.encodeObject(response);
  }  
  
  /*****************************************
  *
  *  processGetPaymentMeanList
  *
  *****************************************/

  private JSONObject processGetPaymentMeanList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {

    /*****************************************
    *
    *  retrieve payment means
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> paymentMeans = new ArrayList<JSONObject>();
    for (GUIManagedObject paymentMean : paymentMeanService.getStoredPaymentMeans())
      {
        paymentMeans.add(paymentMeanService.generateResponseJSON(paymentMean, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("paymentMeans", JSONUtilities.encodeArray(paymentMeans));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetPaymentMean
  *
  *****************************************/

  private JSONObject processGetPaymentMean(String userID, JSONObject jsonRoot)
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

    String paymentMeanID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate payment mean
    *
    *****************************************/

    GUIManagedObject paymentMean = paymentMeanService.getStoredPaymentMean(paymentMeanID);
    JSONObject paymentMeanJSON = paymentMeanService.generateResponseJSON(paymentMean, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (paymentMean != null) ? "ok" : "paymentMeanNotFound");
    if (paymentMean != null) response.put("paymentMean", paymentMeanJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutPaymentMean
  *
  *****************************************/

  private JSONObject processPutPaymentMean(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();
    
    /*****************************************
    *
    *  paymentMeanID
    *
    *****************************************/
    
    String paymentMeanID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (paymentMeanID == null)
      {
        paymentMeanID = paymentMeanService.generatePaymentMeanID();
        jsonRoot.put("id", paymentMeanID);
      }
    
    /*****************************************
    *
    *  existing paymentMean
    *
    *****************************************/

    GUIManagedObject existingPaymentMean = paymentMeanService.getStoredPaymentMean(paymentMeanID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingPaymentMean != null && existingPaymentMean.getReadOnly())
      {
        response.put("id", existingPaymentMean.getGUIManagedObjectID());
        response.put("accepted", existingPaymentMean.getAccepted());
        response.put("processing", paymentMeanService.isActivePaymentMean(existingPaymentMean, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process paymentMean
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate paymentMean
        *
        ****************************************/

        PaymentMean paymentMean = new PaymentMean(jsonRoot, epoch, existingPaymentMean);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        paymentMeanService.putPaymentMean(paymentMean, (existingPaymentMean == null), userID);

        /*****************************************
        *
        *  revalidateProducts
        *
        *****************************************/

        revalidateProducts(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", paymentMean.getPaymentMeanID());
        response.put("accepted", paymentMean.getAccepted());
        response.put("processing", paymentMeanService.isActivePaymentMean(paymentMean, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        paymentMeanService.putIncompletePaymentMean(incompleteObject, (existingPaymentMean == null), userID);

        //
        //  revalidateProducts
        //

        revalidateProducts(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
        
        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "paymentMeanNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemovePaymentMean
  *
  *****************************************/

  private JSONObject processRemovePaymentMean(String userID, JSONObject jsonRoot)
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
    
    String paymentMeanID = JSONUtilities.decodeString(jsonRoot, "id", true);
    
    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject paymentMean = paymentMeanService.getStoredPaymentMean(paymentMeanID);
    if (paymentMean != null && ! paymentMean.getReadOnly()) paymentMeanService.removePaymentMean(paymentMeanID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (paymentMean != null && ! paymentMean.getReadOnly())
      responseCode = "ok";
    else if (paymentMean != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "paymentMeanNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processGetProductType
  *
  *****************************************/

  private JSONObject processGetProductType(String userID, JSONObject jsonRoot)
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

    String productTypeID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate product type
    *
    *****************************************/

    GUIManagedObject productType = productTypeService.getStoredProductType(productTypeID);
    JSONObject productTypeJSON = productTypeService.generateResponseJSON(productType, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (productType != null) ? "ok" : "productTypeNotFound");
    if (productType != null) response.put("productType", productTypeJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutProductType
  *
  *****************************************/

  private JSONObject processPutProductType(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  productTypeID
    *
    *****************************************/

    String productTypeID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (productTypeID == null)
      {
        productTypeID = productTypeService.generateProductTypeID();
        jsonRoot.put("id", productTypeID);
      }

    /*****************************************
    *
    *  existing productType
    *
    *****************************************/

    GUIManagedObject existingProductType = productTypeService.getStoredProductType(productTypeID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingProductType != null && existingProductType.getReadOnly())
      {
        response.put("id", existingProductType.getGUIManagedObjectID());
        response.put("accepted", existingProductType.getAccepted());
        response.put("valid", existingProductType.getAccepted());
        response.put("processing", productTypeService.isActiveProductType(existingProductType, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process productType
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate productType
        *
        ****************************************/

        ProductType productType = new ProductType(jsonRoot, epoch, existingProductType);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        productTypeService.putProductType(productType, (existingProductType == null), userID);

        /*****************************************
        *
        *  revalidateProducts
        *
        *****************************************/

        revalidateProducts(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", productType.getProductTypeID());
        response.put("accepted", productType.getAccepted());
        response.put("valid", productType.getAccepted());
        response.put("processing", productTypeService.isActiveProductType(productType, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        productTypeService.putProductType(incompleteObject, (existingProductType == null), userID);

        //
        //  revalidateProducts
        //

        revalidateProducts(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "productTypeNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveProductType
  *
  *****************************************/

  private JSONObject processRemoveProductType(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String productTypeID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject productType = productTypeService.getStoredProductType(productTypeID);
    if (productType != null && ! productType.getReadOnly()) productTypeService.removeProductType(productTypeID, userID);

    /*****************************************
    *
    *  revalidateProducts
    *
    *****************************************/

    revalidateProducts(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (productType != null && ! productType.getReadOnly())
      responseCode = "ok";
    else if (productType != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "productTypeNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetUCGRuleList
  *
  *****************************************/

  private JSONObject processGetUCGRuleList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert ucg rules
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> ucgRules = new ArrayList<JSONObject>();
    for (GUIManagedObject ucgRule : ucgRuleService.getStoredUCGRules())
      {
        ucgRules.add(ucgRuleService.generateResponseJSON(ucgRule, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("ucgRules", JSONUtilities.encodeArray(ucgRules));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetUCGRule
  *
  *****************************************/

  private JSONObject processGetUCGRule(String userID, JSONObject jsonRoot)
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

    String ucgRuleID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate ucg rule
    *
    *****************************************/

    GUIManagedObject ucgRule = ucgRuleService.getStoredUCGRule(ucgRuleID);
    JSONObject productJSON = ucgRuleService.generateResponseJSON(ucgRule, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (ucgRule != null) ? "ok" : "ucgRuleNotFound");
    if (ucgRule != null) response.put("ucgRule", productJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutUCGRule
  *
  *****************************************/

  private JSONObject processPutUCGRule(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  productID
    *
    *****************************************/

    String ucgRuleID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (ucgRuleID == null)
      {
        ucgRuleID = ucgRuleService.generateUCGRuleID();
        jsonRoot.put("id", ucgRuleID);
      }

    /*****************************************
    *
    *  existing product
    *
    *****************************************/

    GUIManagedObject existingUCGRule = ucgRuleService.getStoredUCGRule(ucgRuleID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingUCGRule != null && existingUCGRule.getReadOnly())
      {
        response.put("id", existingUCGRule.getGUIManagedObjectID());
        response.put("accepted", existingUCGRule.getAccepted());
        response.put("valid", existingUCGRule.getAccepted());
        response.put("processing", ucgRuleService.isActiveUCGRule(existingUCGRule, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process product
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate product
        *
        ****************************************/

        UCGRule ucgRule = new UCGRule(jsonRoot, epoch, existingUCGRule);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        ucgRuleService.putUCGRule(ucgRule,segmentationDimensionService,(existingUCGRule == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", ucgRule.getUCGRuleID());
        response.put("accepted", ucgRule.getAccepted());
        response.put("valid", ucgRule.getAccepted());
        response.put("processing", ucgRuleService.isActiveUCGRule(ucgRule, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        ucgRuleService.putUCGRule(incompleteObject, segmentationDimensionService, (existingUCGRule == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "ucgRuleNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveProduct
  *
  *****************************************/

  private JSONObject processRemoveUCGRule(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String ucgRuleID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject ucgRule = ucgRuleService.getStoredUCGRule(ucgRuleID);
    if (ucgRule != null && ! ucgRule.getReadOnly()) ucgRuleService.removeUCGRule(ucgRuleID, userID);


    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (ucgRule != null && ! ucgRule.getReadOnly())
      responseCode = "ok";
    else if (ucgRule != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "ucgRuleNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetDeliverableList
  *
  *****************************************/

  private JSONObject processGetDeliverableList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert deliverables
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> deliverables = new ArrayList<JSONObject>();
    for (GUIManagedObject deliverable : deliverableService.getStoredDeliverables())
      {
        deliverables.add(deliverableService.generateResponseJSON(deliverable, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("deliverables", JSONUtilities.encodeArray(deliverables));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetDeliverable
  *
  *****************************************/

  private JSONObject processGetDeliverable(String userID, JSONObject jsonRoot)
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

    String deliverableID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject deliverable = deliverableService.getStoredDeliverable(deliverableID);
    JSONObject deliverableJSON = deliverableService.generateResponseJSON(deliverable, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (deliverable != null) ? "ok" : "deliverableNotFound");
    if (deliverable != null) response.put("deliverable", deliverableJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetDeliverableByName
  *
  *****************************************/

  private JSONObject processGetDeliverableByName(String userID, JSONObject jsonRoot)
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

    String deliverableName = JSONUtilities.decodeString(jsonRoot, "name", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject deliverable = deliverableService.getStoredDeliverableByName(deliverableName);
    JSONObject deliverableJSON = deliverableService.generateResponseJSON(deliverable, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (deliverable != null) ? "ok" : "deliverableNotFound");
    if (deliverable != null) response.put("deliverable", deliverableJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutDeliverable
  *
  *****************************************/

  private JSONObject processPutDeliverable(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  deliverableID
    *
    *****************************************/

    String deliverableID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (deliverableID == null)
      {
        deliverableID = deliverableService.generateDeliverableID();
        jsonRoot.put("id", deliverableID);
      }

    /*****************************************
    *
    *  existing deliverable
    *
    *****************************************/

    GUIManagedObject existingDeliverable = deliverableService.getStoredDeliverable(deliverableID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingDeliverable != null && existingDeliverable.getReadOnly())
      {
        response.put("id", existingDeliverable.getGUIManagedObjectID());
        response.put("accepted", existingDeliverable.getAccepted());
        response.put("valid", existingDeliverable.getAccepted());
        response.put("processing", deliverableService.isActiveDeliverable(existingDeliverable, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process deliverable
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate deliverable
        *
        ****************************************/

        Deliverable deliverable = new Deliverable(jsonRoot, epoch, existingDeliverable);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        deliverableService.putDeliverable(deliverable, (existingDeliverable == null), userID);

        /*****************************************
        *
        *  revalidateProducts
        *
        *****************************************/

        revalidateProducts(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", deliverable.getDeliverableID());
        response.put("accepted", deliverable.getAccepted());
        response.put("valid", deliverable.getAccepted());
        response.put("processing", deliverableService.isActiveDeliverable(deliverable, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        deliverableService.putDeliverable(incompleteObject, (existingDeliverable == null), userID);

        //
        //  revalidateProducts
        //

        revalidateProducts(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "deliverableNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveDeliverable
  *
  *****************************************/

  private JSONObject processRemoveDeliverable(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String deliverableID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject deliverable = deliverableService.getStoredDeliverable(deliverableID);
    if (deliverable != null && ! deliverable.getReadOnly()) deliverableService.removeDeliverable(deliverableID, userID);

    /*****************************************
    *
    *  revalidateProducts
    *
    *****************************************/

    revalidateProducts(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (deliverable != null && ! deliverable.getReadOnly())
      responseCode = "ok";
    else if (deliverable != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "deliverableNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetTokenTypeList
  *
  *****************************************/

  private JSONObject processGetTokenTypeList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert tokenTypes
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> tokenTypes = new ArrayList<JSONObject>();
    for (GUIManagedObject tokenType : tokenTypeService.getStoredTokenTypes())
      {
        tokenTypes.add(tokenTypeService.generateResponseJSON(tokenType, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("tokenTypes", JSONUtilities.encodeArray(tokenTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetTokenType
  *
  *****************************************/

  private JSONObject processGetTokenType(String userID, JSONObject jsonRoot)
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

    String tokenTypeID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate tokenType
    *
    *****************************************/

    GUIManagedObject tokenType = tokenTypeService.getStoredTokenType(tokenTypeID);
    JSONObject tokenTypeJSON = tokenTypeService.generateResponseJSON(tokenType, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (tokenType != null) ? "ok" : "tokenTypeNotFound");
    if (tokenType != null) response.put("tokenType", tokenTypeJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutTokenType
  *
  *****************************************/

  private JSONObject processPutTokenType(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  tokenTypeID
    *
    *****************************************/

    String tokenTypeID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (tokenTypeID == null)
      {
        tokenTypeID = tokenTypeService.generateTokenTypeID();
        jsonRoot.put("id", tokenTypeID);
      }

    /*****************************************
    *
    *  existing tokenType
    *
    *****************************************/

    GUIManagedObject existingTokenType = tokenTypeService.getStoredTokenType(tokenTypeID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingTokenType != null && existingTokenType.getReadOnly())
      {
        response.put("id", existingTokenType.getGUIManagedObjectID());
        response.put("accepted", existingTokenType.getAccepted());
        response.put("valid", existingTokenType.getAccepted());
        response.put("processing", tokenTypeService.isActiveTokenType(existingTokenType, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process tokenType
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate tokenType
        *
        ****************************************/

        TokenType tokenType = new TokenType(jsonRoot, epoch, existingTokenType);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        tokenTypeService.putTokenType(tokenType, (existingTokenType == null), userID);

        /*****************************************
        *
        *  revalidateProducts
        *
        *****************************************/

        revalidateProducts(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", tokenType.getTokenTypeID());
        response.put("accepted", tokenType.getAccepted());
        response.put("valid", tokenType.getAccepted());
        response.put("processing", tokenTypeService.isActiveTokenType(tokenType, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        tokenTypeService.putTokenType(incompleteObject, (existingTokenType == null), userID);

        //
        //  revalidateProducts
        //

        revalidateProducts(now);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "tokenTypeNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveTokenType
  *
  *****************************************/

  private JSONObject processRemoveTokenType(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  now
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String tokenTypeID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject tokenType = tokenTypeService.getStoredTokenType(tokenTypeID);
    if (tokenType != null && ! tokenType.getReadOnly()) tokenTypeService.removeTokenType(tokenTypeID, userID);

    /*****************************************
    *
    *  revalidateProducts
    *
    *****************************************/

    revalidateProducts(now);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (tokenType != null && ! tokenType.getReadOnly())
      responseCode = "ok";
    else if (tokenType != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "tokenTypeNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processGetTokenCodesFormats
  *
  *****************************************/
  private JSONObject processGetTokenCodesFormats(String userID, JSONObject jsonRoot)
  {

    /*****************************************
     *
     *  retrieve tokenCodesFormats
     *
     *****************************************/
    
    List<JSONObject> supportedTokenCodesFormats = new ArrayList<JSONObject>();
    for (SupportedTokenCodesFormat supportedTokenCodesFormat : Deployment.getSupportedTokenCodesFormats().values())
      {
        JSONObject supportedTokenCodesFormatJSON = supportedTokenCodesFormat.getJSONRepresentation();
        supportedTokenCodesFormats.add(supportedTokenCodesFormatJSON);
      }

    /*****************************************
     *
     *  response
     *
     *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedTokenCodesFormats", JSONUtilities.encodeArray(supportedTokenCodesFormats));
    return JSONUtilities.encodeObject(response);
  }


  
  /*****************************************
  *
  *  processGetMailTemplateList
  *
  *****************************************/

  private JSONObject processGetMailTemplateList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert templates
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> templates = new ArrayList<JSONObject>();
    for (GUIManagedObject template : mailTemplateService.getStoredMailTemplates())
      {
        templates.add(mailTemplateService.generateResponseJSON(template, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("templates", JSONUtilities.encodeArray(templates));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetMailTemplate
  *
  *****************************************/

  private JSONObject processGetMailTemplate(String userID, JSONObject jsonRoot)
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

    String templateID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate template
    *
    *****************************************/

    GUIManagedObject template = mailTemplateService.getStoredMailTemplate(templateID);
    JSONObject templateJSON = mailTemplateService.generateResponseJSON(template, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (template != null) ? "ok" : "templateNotFound");
    if (template != null) response.put("template", templateJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutMailTemplate
  *
  *****************************************/

  private JSONObject processPutMailTemplate(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  templateID
    *
    *****************************************/

    String templateID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (templateID == null)
      {
        templateID = mailTemplateService.generateMailTemplateID();
        jsonRoot.put("id", templateID);
      }

    /*****************************************
    *
    *  existing template
    *
    *****************************************/

    GUIManagedObject existingTemplate = mailTemplateService.getStoredMailTemplate(templateID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingTemplate != null && existingTemplate.getReadOnly())
      {
        response.put("id", existingTemplate.getGUIManagedObjectID());
        response.put("accepted", existingTemplate.getAccepted());
        response.put("processing", mailTemplateService.isActiveMailTemplate(existingTemplate, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process template
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate template
        *
        ****************************************/

        MailTemplate mailTemplate = new MailTemplate(jsonRoot, epoch, existingTemplate);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        mailTemplateService.putMailTemplate(mailTemplate, (existingTemplate == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", mailTemplate.getMailTemplateID());
        response.put("accepted", mailTemplate.getAccepted());
        response.put("processing", mailTemplateService.isActiveMailTemplate(mailTemplate, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        mailTemplateService.putIncompleteMailTemplate(incompleteObject, (existingTemplate == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "mailTemplateNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveMailTemplate
  *
  *****************************************/

  private JSONObject processRemoveMailTemplate(String userID, JSONObject jsonRoot)
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

    String templateID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject template = mailTemplateService.getStoredMailTemplate(templateID);
    if (template != null && ! template.getReadOnly()) mailTemplateService.removeMailTemplate(templateID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (template != null && ! template.getReadOnly())
      responseCode = "ok";
    else if (template != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "templateNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetSMSTemplateList
  *
  *****************************************/

  private JSONObject processGetSMSTemplateList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    log.info("GUIManager.processGetSMSTemplateList("+userID+", "+jsonRoot+", "+fullDetails+") called ...");

    /*****************************************
    *
    *  retrieve and convert templates
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> templates = new ArrayList<JSONObject>();
    for (GUIManagedObject template : smsTemplateService.getStoredSMSTemplates())
      {
        templates.add(smsTemplateService.generateResponseJSON(template, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("templates", JSONUtilities.encodeArray(templates));

    log.info("GUIManager.processGetSMSTemplateList("+userID+", "+jsonRoot+", "+fullDetails+") Done");

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetSMSTemplate
  *
  *****************************************/

  private JSONObject processGetSMSTemplate(String userID, JSONObject jsonRoot)
  {
    log.info("GUIManager.processGetSMSTemplate("+userID+", "+jsonRoot+") called ...");
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

    String templateID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate template
    *
    *****************************************/

    GUIManagedObject template = smsTemplateService.getStoredSMSTemplate(templateID);
    JSONObject templateJSON = smsTemplateService.generateResponseJSON(template, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (template != null) ? "ok" : "templateNotFound");
    if (template != null) response.put("template", templateJSON);

    log.info("GUIManager.processGetSMSTemplate("+userID+", "+jsonRoot+") DONE");

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutSMSTemplate
  *
  *****************************************/

  private JSONObject processPutSMSTemplate(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  templateID
    *
    *****************************************/

    String templateID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (templateID == null)
      {
        templateID = smsTemplateService.generateSMSTemplateID();
        jsonRoot.put("id", templateID);
      }

    /*****************************************
    *
    *  existing template
    *
    *****************************************/

    GUIManagedObject existingTemplate = smsTemplateService.getStoredSMSTemplate(templateID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingTemplate != null && existingTemplate.getReadOnly())
      {
        response.put("id", existingTemplate.getGUIManagedObjectID());
        response.put("accepted", existingTemplate.getAccepted());
        response.put("processing", smsTemplateService.isActiveSMSTemplate(existingTemplate, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process template
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate template
        *
        ****************************************/

        SMSTemplate smsTemplate = new SMSTemplate(jsonRoot, epoch, existingTemplate);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        smsTemplateService.putSMSTemplate(smsTemplate, (existingTemplate == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", smsTemplate.getSMSTemplateID());
        response.put("accepted", smsTemplate.getAccepted());
        response.put("processing", smsTemplateService.isActiveSMSTemplate(smsTemplate, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        smsTemplateService.putIncompleteSMSTemplate(incompleteObject, (existingTemplate == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("id", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "smsTemplateNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveSMSTemplate
  *
  *****************************************/

  private JSONObject processRemoveSMSTemplate(String userID, JSONObject jsonRoot)
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

    String templateID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject template = smsTemplateService.getStoredSMSTemplate(templateID);
    if (template != null && ! template.getReadOnly()) smsTemplateService.removeSMSTemplate(templateID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (template != null && ! template.getReadOnly())
      responseCode = "ok";
    else if (template != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "templateNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  revalidateTargets
  *
  *****************************************/

  private void revalidateTargets(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedTargets = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingTarget : targetService.getStoredTargets())
      {
        //
        //  modifiedScoringStrategy
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedTarget;
        try
          {
            Target target = new Target(existingTarget.getJSONRepresentation(), epoch, existingTarget);
            target.validate(uploadedFileService, date);
            modifiedTarget = target;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedTarget = new IncompleteObject(existingTarget.getJSONRepresentation(), epoch);
          }

        //
        //  changed?
        //

        if (existingTarget.getAccepted() != modifiedTarget.getAccepted())
          {
            modifiedTargets.add(modifiedTarget);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedTarget : modifiedTargets)
      {
        targetService.putGUIManagedObject(modifiedTarget, date, false, null);
      }

    /****************************************
    *
    *  revalidate journeys
    *
    ****************************************/

    revalidateJourneys(date);
  }

  /*****************************************
  *
  *  revalidateScoringStrategies
  *
  *****************************************/

  private void revalidateScoringStrategies(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedScoringStrategies = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingScoringStrategy : scoringStrategyService.getStoredScoringStrategies())
      {
        //
        //  modifiedScoringStrategy
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedScoringStrategy;
        try
          {
            ScoringStrategy scoringStrategy = new ScoringStrategy(existingScoringStrategy.getJSONRepresentation(), epoch, existingScoringStrategy);
            scoringStrategy.validate(offerObjectiveService, date);
            modifiedScoringStrategy = scoringStrategy;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedScoringStrategy = new IncompleteObject(existingScoringStrategy.getJSONRepresentation(), epoch);
          }

        //
        //  changed?
        //

        if (existingScoringStrategy.getAccepted() != modifiedScoringStrategy.getAccepted())
          {
            modifiedScoringStrategies.add(modifiedScoringStrategy);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedScoringStrategy : modifiedScoringStrategies)
      {
        scoringStrategyService.putGUIManagedObject(modifiedScoringStrategy, date, false, null);
      }

    /****************************************
    *
    *  revalidate presentation strategies
    *
    ****************************************/

    revalidatePresentationStrategies(date);
  }

  /*****************************************
  *
  *  revalidatePresentationStrategies
  *
  *****************************************/

  private void revalidatePresentationStrategies(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedPresentationStrategies = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingPresentationStrategy : presentationStrategyService.getStoredPresentationStrategies())
      {
        //
        //  modifiedPresentationStrategy
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedPresentationStrategy;
        try
          {
            PresentationStrategy presentationStrategy = new PresentationStrategy(existingPresentationStrategy.getJSONRepresentation(), epoch, existingPresentationStrategy);
            presentationStrategy.validate(scoringStrategyService, date);
            modifiedPresentationStrategy = presentationStrategy;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedPresentationStrategy = new IncompleteObject(existingPresentationStrategy.getJSONRepresentation(), epoch);
          }

        //
        //  changed?
        //

        if (existingPresentationStrategy.getAccepted() != modifiedPresentationStrategy.getAccepted())
          {
            modifiedPresentationStrategies.add(modifiedPresentationStrategy);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedPresentationStrategy : modifiedPresentationStrategies)
      {
        presentationStrategyService.putGUIManagedObject(modifiedPresentationStrategy, date, false, null);
      }
  }

  /*****************************************
  *
  *  revalidateJourneys
  *
  *****************************************/

  private void revalidateJourneys(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedJourneys = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingJourney : journeyService.getStoredJourneys())
      {
        //
        //  modifiedJourney
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedJourney;
        try
          {
            Journey journey = new Journey(existingJourney.getJSONRepresentation(), existingJourney.getGUIManagedObjectType(), epoch, existingJourney, catalogCharacteristicService);
            journey.validate(journeyObjectiveService, catalogCharacteristicService, targetService, date);
            modifiedJourney = journey;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedJourney = new IncompleteObject(existingJourney.getJSONRepresentation(), existingJourney.getGUIManagedObjectType(), epoch);
          }

        //
        //  changed?
        //

        if (existingJourney.getAccepted() != modifiedJourney.getAccepted())
          {
            modifiedJourneys.add(modifiedJourney);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedJourney : modifiedJourneys)
      {
        journeyService.putGUIManagedObject(modifiedJourney, date, false, null);
      }
  }

  /*****************************************
  *
  *  revalidateOffers
  *
  *****************************************/

  private void revalidateOffers(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedOffers = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingOffer : offerService.getStoredOffers())
      {
        //
        //  modifiedOffer
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedOffer;
        try
          {
            Offer offer = new Offer(existingOffer.getJSONRepresentation(), epoch, existingOffer, catalogCharacteristicService);
            offer.validate(callingChannelService, salesChannelService, productService, date);
            modifiedOffer = offer;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedOffer = new IncompleteObject(existingOffer.getJSONRepresentation(), epoch);
          }

        //
        //  changed?
        //

        if (existingOffer.getAccepted() != modifiedOffer.getAccepted())
          {
            modifiedOffers.add(modifiedOffer);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedOffer : modifiedOffers)
      {
        offerService.putGUIManagedObject(modifiedOffer, date, false, null);
      }
  }

  /*****************************************
  *
  *  revalidateProducts
  *
  *****************************************/

  private void revalidateProducts(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedProducts = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingProduct : productService.getStoredProducts())
      {
        //
        //  modifiedProduct
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedProduct;
        try
          {
            Product product = new Product(existingProduct.getJSONRepresentation(), epoch, existingProduct, catalogCharacteristicService);
            product.validate(supplierService, productTypeService, deliverableService, date);
            modifiedProduct = product;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedProduct = new IncompleteObject(existingProduct.getJSONRepresentation(), epoch);
          }

        //
        //  changed?
        //

        if (existingProduct.getAccepted() != modifiedProduct.getAccepted())
          {
            modifiedProducts.add(modifiedProduct);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedProduct : modifiedProducts)
      {
        productService.putGUIManagedObject(modifiedProduct, date, false, null);
      }

    /****************************************
    *
    *  revalidate offers
    *
    ****************************************/

    revalidateOffers(date);
  }

  /*****************************************
  *
  *  revalidateCatalogCharacteristics
  *
  *****************************************/

  private void revalidateCatalogCharacteristics(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedCatalogCharacteristics = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingCatalogCharacteristic : catalogCharacteristicService.getStoredCatalogCharacteristics())
      {
        //
        //  modifiedCatalogCharacteristic
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedCatalogCharacteristic;
        try
          {
            CatalogCharacteristic catalogCharacteristic = new CatalogCharacteristic(existingCatalogCharacteristic.getJSONRepresentation(), epoch, existingCatalogCharacteristic);
            modifiedCatalogCharacteristic = catalogCharacteristic;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedCatalogCharacteristic = new IncompleteObject(existingCatalogCharacteristic.getJSONRepresentation(), epoch);
          }

        //
        //  changed?
        //

        if (existingCatalogCharacteristic.getAccepted() != modifiedCatalogCharacteristic.getAccepted())
          {
            modifiedCatalogCharacteristics.add(modifiedCatalogCharacteristic);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedCatalogCharacteristic : modifiedCatalogCharacteristics)
      {
        catalogCharacteristicService.putGUIManagedObject(modifiedCatalogCharacteristic, date, false, null);
      }

    /****************************************
    *
    *  revalidate dependent objects
    *
    ****************************************/

    revalidateOffers(date);
    revalidateJourneyObjectives(date);
    revalidateOfferObjectives(date);
    revalidateProductTypes(date);
    revalidateProducts(date);
  }

  /*****************************************
  *
  *  revalidateJourneyObjectives
  *
  *****************************************/

  private void revalidateJourneyObjectives(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedJourneyObjectives = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingJourneyObjective : journeyObjectiveService.getStoredJourneyObjectives())
      {
        //
        //  modifiedJourneyObjective
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedJourneyObjective;
        try
          {
            JourneyObjective journeyObjective = new JourneyObjective(existingJourneyObjective.getJSONRepresentation(), epoch, existingJourneyObjective);
            journeyObjective.validate(journeyObjectiveService, contactPolicyService, catalogCharacteristicService, date);
            modifiedJourneyObjective = journeyObjective;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedJourneyObjective = new IncompleteObject(existingJourneyObjective.getJSONRepresentation(), epoch);
          }

        //
        //  changed?
        //

        if (existingJourneyObjective.getAccepted() != modifiedJourneyObjective.getAccepted())
          {
            modifiedJourneyObjectives.add(modifiedJourneyObjective);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedJourneyObjective : modifiedJourneyObjectives)
      {
        journeyObjectiveService.putGUIManagedObject(modifiedJourneyObjective, date, false, null);
      }

    /****************************************
    *
    *  revalidate dependent objects
    *
    ****************************************/

    revalidateJourneys(date);
    if (modifiedJourneyObjectives.size() > 0)
      {
        revalidateJourneyObjectives(date);
      }
  }

  /*****************************************
  *
  *  revalidateSalesChannels
  *
  *****************************************/

  private void revalidateSalesChannels(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedSalesChannels = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingSalesChannel : salesChannelService.getStoredSalesChannels())
      {
        //
        //  modifiedsalesChannel
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedSalesChannel;
        try
          {
            SalesChannel salesChannel = new SalesChannel(existingSalesChannel.getJSONRepresentation(), epoch, existingSalesChannel);
            salesChannel.validate(callingChannelService, date);
            modifiedSalesChannel = salesChannel;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedSalesChannel = new IncompleteObject(existingSalesChannel.getJSONRepresentation(), epoch);
          }

        //
        //  changed?
        //

        if (existingSalesChannel.getAccepted() != modifiedSalesChannel.getAccepted())
          {
            modifiedSalesChannels.add(modifiedSalesChannel);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedSalesChannel : modifiedSalesChannels)
      {
        salesChannelService.putGUIManagedObject(modifiedSalesChannel, date, false, null);
      }

    /****************************************
    *
    *  revalidate dependent objects
    *
    ****************************************/

    revalidateOffers(date);
  }

  /*****************************************
  *
  *  revalidateOfferObjectives
  *
  *****************************************/

  private void revalidateOfferObjectives(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedOfferObjectives = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingOfferObjective : offerObjectiveService.getStoredOfferObjectives())
      {
        //
        //  modifiedOfferObjective
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedOfferObjective;
        try
          {
            OfferObjective offerObjective = new OfferObjective(existingOfferObjective.getJSONRepresentation(), epoch, existingOfferObjective);
            offerObjective.validate(catalogCharacteristicService, date);
            modifiedOfferObjective = offerObjective;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedOfferObjective = new IncompleteObject(existingOfferObjective.getJSONRepresentation(), epoch);
          }

        //
        //  changed?
        //

        if (existingOfferObjective.getAccepted() != modifiedOfferObjective.getAccepted())
          {
            modifiedOfferObjectives.add(modifiedOfferObjective);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedOfferObjective : modifiedOfferObjectives)
      {
        offerObjectiveService.putGUIManagedObject(modifiedOfferObjective, date, false, null);
      }

    /****************************************
    *
    *  revalidate dependent objects
    *
    ****************************************/

    revalidateOffers(date);
    revalidateScoringStrategies(date);
  }

  /*****************************************
  *
  *  revalidateProductTypes
  *
  *****************************************/

  private void revalidateProductTypes(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedProductTypes = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingProductType : productTypeService.getStoredProductTypes())
      {
        //
        //  modifiedProductType
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedProductType;
        try
          {
            ProductType productType = new ProductType(existingProductType.getJSONRepresentation(), epoch, existingProductType);
            productType.validate(catalogCharacteristicService, date);
            modifiedProductType = productType;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedProductType = new IncompleteObject(existingProductType.getJSONRepresentation(), epoch);
          }

        //
        //  changed?
        //

        if (existingProductType.getAccepted() != modifiedProductType.getAccepted())
          {
            modifiedProductTypes.add(modifiedProductType);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedProductType : modifiedProductTypes)
      {
        productTypeService.putGUIManagedObject(modifiedProductType, date, false, null);
      }

    /****************************************
    *
    *  revalidate dependent objects
    *
    ****************************************/

    revalidateProducts(date);
  }

  /*****************************************
  *
  *  revalidateUCGRules
  *
  *****************************************/

  private void revalidateUCGRules(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedUCGRules = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingUCGRule : ucgRuleService.getStoredUCGRules())
      {
        //
        //  modifiedUCGRule
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedUCGRule;
        try
          {
            UCGRule ucgRule = new UCGRule(existingUCGRule.getJSONRepresentation(), epoch, existingUCGRule);
            ucgRule.validate(ucgRuleService, segmentationDimensionService, date);
            modifiedUCGRule = ucgRule;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedUCGRule = new IncompleteObject(existingUCGRule.getJSONRepresentation(), epoch);
          }

        //
        //  changed?
        //

        if (existingUCGRule.getAccepted() != modifiedUCGRule.getAccepted())
          {
            modifiedUCGRules.add(modifiedUCGRule);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedUCGRule : modifiedUCGRules)
      {
        ucgRuleService.putGUIManagedObject(modifiedUCGRule, date, false, null);
      }
  }

  /*****************************************
  *
  *  processGetDashboardCounts
  *
  *****************************************/

  private JSONObject processGetDashboardCounts(String userID, JSONObject jsonRoot)
  {
    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("journeyCount", journeyCount(GUIManagedObjectType.Journey));
    response.put("campaignCount", journeyCount(GUIManagedObjectType.Campaign));
    response.put("segmentationDimensionCount", segmentationDimensionService.getStoredSegmentationDimensions().size());
    response.put("pointCount", pointService.getStoredPoints().size());
    response.put("offerCount", offerService.getStoredOffers().size());
    response.put("scoringStrategyCount", scoringStrategyService.getStoredScoringStrategies().size());
    response.put("presentationStrategyCount", presentationStrategyService.getStoredPresentationStrategies().size());
    response.put("callingChannelCount", callingChannelService.getStoredCallingChannels().size());
    response.put("salesChannelCount", salesChannelService.getStoredSalesChannels().size());
    response.put("supplierCount", supplierService.getStoredSuppliers().size());
    response.put("productCount", productService.getStoredProducts().size());
    response.put("catalogCharacteristicCount", catalogCharacteristicService.getStoredCatalogCharacteristics().size());
    response.put("journeyObjectiveCount", journeyObjectiveService.getStoredJourneyObjectives().size());
    response.put("offerObjectiveCount", offerObjectiveService.getStoredOfferObjectives().size());
    response.put("productTypeCount", productTypeService.getStoredProductTypes().size());
    response.put("deliverableCount", deliverableService.getStoredDeliverables().size());
    response.put("mailTemplateCount", mailTemplateService.getStoredMailTemplates().size());
    response.put("smsTemplateCount", smsTemplateService.getStoredSMSTemplates().size());
    response.put("reportsCount", reportService.getStoredReports().size());
    response.put("walletsCount", pointService.getStoredPoints().size() + tokenTypeService.getStoredTokenTypes().size());
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetCustomer
  * @throws GUIManagerException
  *
  *****************************************/

  private JSONObject processGetCustomer(String userID, JSONObject jsonRoot) throws GUIManagerException
  {

    Map<String, Object> response = new LinkedHashMap<String, Object>();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);

    /*****************************************
    *
    *  resolve subscriberID
    *
    *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        log.info("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID {}", getCustomerAlternateID, customerID);
        response.put("responseCode", "CustomerNotFound");
      }

    /*****************************************
    *
    *  getSubscriberProfile
    *
    *****************************************/

    if (subscriberID != null)
      {
        try
          {
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false);
            if (baseSubscriberProfile == null)
              {
                response.put("responseCode", "CustomerNotFound");
              }
            else
              {
                response = baseSubscriberProfile.getProfileMapForGUIPresentation(segmentationDimensionService, subscriberGroupEpochReader);
                response.put("responseCode", "ok");
              }
          }
        catch (SubscriberProfileServiceException e)
          {
            throw new GUIManagerException(e);
          }
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
  *  processGetCustomerMetaData
  *
  *****************************************/

  private JSONObject processGetCustomerMetaData(String userID, JSONObject jsonRoot) throws GUIManagerException
  {
    Map<String, Object> response = new HashMap<String, Object>();

    /***************************************
    *
    *  argument
    *
    ****************************************/

    //
    //  no args
    //

    /*****************************************
    *
    *  retrieve CustomerMetaData
    *
    *****************************************/

    List<JSONObject> generalDetailsMetaDataList = Deployment.getCustomerMetaData().getGeneralDetailsMetaData().stream().map(generalDetailsMetaData -> generalDetailsMetaData.getJSONRepresentation()).collect(Collectors.toList());
    List<JSONObject> kpisMetaDataList = Deployment.getCustomerMetaData().getKpiMetaData().stream().map(kpisMetaData -> kpisMetaData.getJSONRepresentation()).collect(Collectors.toList());
    response.put("generalDetailsMetaData", JSONUtilities.encodeArray(generalDetailsMetaDataList));
    response.put("kpisMetaData", JSONUtilities.encodeArray(kpisMetaDataList));

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", "ok");

    /****************************************
    *
    *  return
    *
    *****************************************/

    return JSONUtilities.encodeObject(response);
  }

  /****************************************
  *
  *  processGetCustomerActivityByDateRange
  *
  *****************************************/

  private JSONObject processGetCustomerActivityByDateRange(String userID, JSONObject jsonRoot) throws GUIManagerException
  {

    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String fromDateReq = JSONUtilities.decodeString(jsonRoot, "fromDate", false);
    String toDateReq = JSONUtilities.decodeString(jsonRoot, "toDate", false);

    //
    // yyyy-MM-dd -- date format
    //

    String dateFormat = "yyyy-MM-dd";

    /*****************************************
    *
    *  resolve subscriberID
    *
    *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        log.info("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID ", getCustomerAlternateID, customerID);
        response.put("responseCode", "CustomerNotFound");
      }
    else
      {
        /*****************************************
        *
        *  getSubscriberProfile - include history
        *
        *****************************************/
        try
          {
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true);
            if (baseSubscriberProfile == null)
              {
                response.put("responseCode", "CustomerNotFound");
                log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
              }
            else
              {
                List<JSONObject> deliveryRequestsJson = new ArrayList<JSONObject>();
                SubscriberHistory subscriberHistory = baseSubscriberProfile.getSubscriberHistory();
                if (subscriberHistory != null && subscriberHistory.getDeliveryRequests() != null)
                  {
                    List<DeliveryRequest> activities = subscriberHistory.getDeliveryRequests();

                    //
                    // prepare dates
                    //

                    Date fromDate = null;
                    Date toDate = null;
                    Date now = SystemTime.getCurrentTime();

                    if (fromDateReq == null || fromDateReq.isEmpty() || toDateReq == null || toDateReq.isEmpty())
                      {
                        toDate = now;
                        fromDate = RLMDateUtils.addDays(toDate, -7, Deployment.getBaseTimeZone());
                      }
                    else if (toDateReq == null || toDateReq.isEmpty())
                      {
                        toDate = now;
                        fromDate = RLMDateUtils.parseDate(fromDateReq, dateFormat, Deployment.getBaseTimeZone());
                      }
                    else
                      {
                        toDate = RLMDateUtils.parseDate(toDateReq, dateFormat, Deployment.getBaseTimeZone());
                        fromDate = RLMDateUtils.addDays(toDate, -7, Deployment.getBaseTimeZone());
                      }

                    //
                    // filter
                    //

                    List<DeliveryRequest> result = new ArrayList<DeliveryRequest>();
                    for (DeliveryRequest activity : activities)
                      {
                        if ( (activity.getEventDate().before(toDate) && activity.getEventDate().after(fromDate)) || (activity.getEventDate().equals(toDate) || activity.getEventDate().equals(fromDate)) )
                          {
                            result.add(activity);
                          }
                      }

                    //
                    // prepare json
                    //

                    deliveryRequestsJson = result.stream().map(deliveryRequest -> JSONUtilities.encodeObject(deliveryRequest.getGUIPresentationMap(salesChannelService))).collect(Collectors.toList());
                  }

                //
                // prepare response
                //

                response.put("activities", JSONUtilities.encodeArray(deliveryRequestsJson));
                response.put("responseCode", "ok");
              }
          }
        catch (SubscriberProfileServiceException e)
          {
            throw new GUIManagerException(e);
          }
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
  * processGetCustomerBDRs
  *
  *****************************************/

  private JSONObject processGetCustomerBDRs(String userID, JSONObject jsonRoot) throws GUIManagerException
  {

    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String startDateReq = JSONUtilities.decodeString(jsonRoot, "startDate", false);

    //
    // yyyy-MM-dd -- date format
    //

    String dateFormat = "yyyy-MM-dd";

    /*****************************************
    *
    *  resolve subscriberID
    *
    *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        log.info("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID ", getCustomerAlternateID, customerID);
        response.put("responseCode", "CustomerNotFound");
      }
    else
      {
        /*****************************************
        *
        *  getSubscriberProfile - include history
        *
        *****************************************/
        try
          {
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true);
            if (baseSubscriberProfile == null)
              {
                response.put("responseCode", "CustomerNotFound");
                log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
              }
            else
              {
                List<JSONObject> BDRsJson = new ArrayList<JSONObject>();
                SubscriberHistory subscriberHistory = baseSubscriberProfile.getSubscriberHistory();
                if (subscriberHistory != null && subscriberHistory.getDeliveryRequests() != null)
                  {
                    List<DeliveryRequest> activities = subscriberHistory.getDeliveryRequests();

                    //
                    // filterBDRs
                    //

                    List<DeliveryRequest> BDRs = activities.stream().filter(activity -> activity.getActivityType().equals(ActivityType.BDR.getExternalRepresentation())).collect(Collectors.toList());

                    //
                    // prepare dates
                    //

                    Date startDate = null;
                    Date now = SystemTime.getCurrentTime();

                    if (startDateReq == null || startDateReq.isEmpty())
                      {
                        startDate = RLMDateUtils.addDays(now, -7, Deployment.getBaseTimeZone());
                      }
                    else
                      {
                        startDate = RLMDateUtils.parseDate(startDateReq, dateFormat, Deployment.getBaseTimeZone());
                      }

                    //
                    // filter and prepare json
                    //

                    List<DeliveryRequest> result = new ArrayList<DeliveryRequest>();
                    for (DeliveryRequest bdr : BDRs)
                      {
                        if (bdr.getEventDate().after(startDate) || bdr.getEventDate().equals(startDate))
                          {
                            Map<String, Object> bdrMap = bdr.getGUIPresentationMap(salesChannelService);
                            DeliveryRequest.Module deliveryModule = DeliveryRequest.Module.fromExternalRepresentation(String.valueOf(bdrMap.get(DeliveryRequest.MODULEID)));
                            if (bdrMap.get(DeliveryRequest.FEATUREID) != null)
                              {
                                bdrMap.put(DeliveryRequest.FEATURENAME, getFeatureName(deliveryModule, String.valueOf(bdrMap.get(DeliveryRequest.FEATUREID))));
                              }
                            else
                              {
                                bdrMap.put(DeliveryRequest.FEATURENAME, null);
                              }
                            BDRsJson.add(JSONUtilities.encodeObject(bdrMap));
                          }
                      }
                  }

                //
                // prepare response
                //

                response.put("BDRs", JSONUtilities.encodeArray(BDRsJson));
                response.put("responseCode", "ok");
              }
          }
        catch (SubscriberProfileServiceException e)
          {
            throw new GUIManagerException(e);
          }
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
  * processGetCustomerODRs
  *
  *****************************************/

  private JSONObject processGetCustomerODRs(String userID, JSONObject jsonRoot) throws GUIManagerException
  {

    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String startDateReq = JSONUtilities.decodeString(jsonRoot, "startDate", false);

    //
    // yyyy-MM-dd -- date format
    //

    String dateFormat = "yyyy-MM-dd";

    /*****************************************
    *
    *  resolve subscriberID
    *
    *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        log.info("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID ", getCustomerAlternateID, customerID);
        response.put("responseCode", "CustomerNotFound");
      }
    else
      {
        /*****************************************
        *
        *  getSubscriberProfile - include history
        *
        *****************************************/
        try
          {
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true);
            if (baseSubscriberProfile == null)
              {
                response.put("responseCode", "CustomerNotFound");
                log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
              }
            else
              {
                List<JSONObject> ODRsJson = new ArrayList<JSONObject>();
                SubscriberHistory subscriberHistory = baseSubscriberProfile.getSubscriberHistory();
                if (subscriberHistory != null && subscriberHistory.getDeliveryRequests() != null)
                  {
                    List<DeliveryRequest> activities = subscriberHistory.getDeliveryRequests();

                    //
                    // filter ODRs
                    //

                    List<DeliveryRequest> ODRs = activities.stream().filter(activity -> activity.getActivityType().equals(ActivityType.ODR.getExternalRepresentation())).collect(Collectors.toList());

                    //
                    // prepare dates
                    //

                    Date startDate = null;
                    Date now = SystemTime.getCurrentTime();

                    if (startDateReq == null || startDateReq.isEmpty())
                      {
                        startDate = RLMDateUtils.addDays(now, -7, Deployment.getBaseTimeZone());
                      }
                    else
                      {
                        startDate = RLMDateUtils.parseDate(startDateReq, dateFormat, Deployment.getBaseTimeZone());
                      }

                    //
                    // filter using dates and prepare json
                    //

                    for (DeliveryRequest odr : ODRs)
                      {
                        if (odr.getEventDate().after(startDate) || odr.getEventDate().equals(startDate))
                          {
                            Map<String, Object> presentationMap =  odr.getGUIPresentationMap(salesChannelService);
                            String offerID = presentationMap.get(DeliveryRequest.OFFERID) == null ? null : presentationMap.get(DeliveryRequest.OFFERID).toString();
                            if (offerID != null)
                              {
                                Offer offer = (Offer) offerService.getStoredOffer(offerID);
                                if (offer != null)
                                  {
                                    if (offer.getOfferSalesChannelsAndPrices() != null)
                                      {
                                        for (OfferSalesChannelsAndPrice channel : offer.getOfferSalesChannelsAndPrices())
                                          {
                                            presentationMap.put(DeliveryRequest.SALESCHANNELID, channel.getSalesChannelIDs());
                                            presentationMap.put(DeliveryRequest.OFFERPRICE, channel.getPrice().getAmount());
                                          }
                                      }
                                    presentationMap.put(DeliveryRequest.OFFERNAME, offer.getGUIManagedObjectName());
                                    presentationMap.put(DeliveryRequest.OFFERSTOCK, "");
                                    presentationMap.put(DeliveryRequest.OFFERCONTENT, offer.getOfferProducts().toString());
                                  }
                              }
                            ODRsJson.add(JSONUtilities.encodeObject(presentationMap));
                          }
                      }
                  }

                //
                // prepare response
                //

                response.put("ODRs", JSONUtilities.encodeArray(ODRsJson));
                response.put("responseCode", "ok");
              }
          }
        catch (SubscriberProfileServiceException e)
          {
            throw new GUIManagerException(e);
          }
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
  * processGetCustomerMessages
  *
  *****************************************/

  private JSONObject processGetCustomerMessages(String userID, JSONObject jsonRoot) throws GUIManagerException
  {

    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String startDateReq = JSONUtilities.decodeString(jsonRoot, "startDate", false);

    //
    // yyyy-MM-dd -- date format
    //

    String dateFormat = "yyyy-MM-dd";

    /*****************************************
    *
    *  resolve subscriberID
    *
    *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        log.info("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID ", getCustomerAlternateID, customerID);
        response.put("responseCode", "CustomerNotFound");
      }
    else
      {
        /*****************************************
        *
        *  getSubscriberProfile - include history
        *
        *****************************************/
        try
          {
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true);
            if (baseSubscriberProfile == null)
              {
                response.put("responseCode", "CustomerNotFound");
                log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
              }
            else
              {
                List<JSONObject> messagesJson = new ArrayList<JSONObject>();
                SubscriberHistory subscriberHistory = baseSubscriberProfile.getSubscriberHistory();
                if (subscriberHistory != null && subscriberHistory.getDeliveryRequests() != null)
                  {
                    List<DeliveryRequest> activities = subscriberHistory.getDeliveryRequests();

                    //
                    // filter ODRs
                    //

                    List<DeliveryRequest> messages = activities.stream().filter(activity -> activity.getActivityType().equals(ActivityType.Messages.getExternalRepresentation())).collect(Collectors.toList());

                    //
                    // prepare dates
                    //

                    Date startDate = null;
                    Date now = SystemTime.getCurrentTime();

                    if (startDateReq == null || startDateReq.isEmpty())
                      {
                        startDate = RLMDateUtils.addDays(now, -7, Deployment.getBaseTimeZone());
                      }
                    else
                      {
                        startDate = RLMDateUtils.parseDate(startDateReq, dateFormat, Deployment.getBaseTimeZone());
                      }

                    //
                    // filter using dates and prepare json
                    //

                    List<DeliveryRequest> result = new ArrayList<DeliveryRequest>();
                    for (DeliveryRequest message : messages)
                      {
                        if (message.getEventDate().after(startDate) || message.getEventDate().equals(startDate))
                          {
                            messagesJson.add(JSONUtilities.encodeObject(message.getGUIPresentationMap(salesChannelService)));
                          }
                      }
                  }

                //
                // prepare response
                //

                response.put("messages", JSONUtilities.encodeArray(messagesJson));
                response.put("responseCode", "ok");
              }
          }
        catch (SubscriberProfileServiceException e)
          {
            throw new GUIManagerException(e);
          }
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
  * processGetCustomerJourneys
  *
  *****************************************/

  private JSONObject processGetCustomerJourneys(String userID, JSONObject jsonRoot) throws GUIManagerException
  {
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String journeyObjectiveName = JSONUtilities.decodeString(jsonRoot, "objectiveName", false);
    String journeyState = JSONUtilities.decodeString(jsonRoot, "journeyState", false);
    String customerStatus = JSONUtilities.decodeString(jsonRoot, "customerStatus", false);
    String journeyStartDateStr = JSONUtilities.decodeString(jsonRoot, "journeyStartDate", false);
    String journeyEndDateStr = JSONUtilities.decodeString(jsonRoot, "journeyEndDate", false);


    //
    // yyyy-MM-dd -- date format
    //

    String dateFormat = "yyyy-MM-dd";
    Date journeyStartDate = getDateFromString(journeyStartDateStr, dateFormat);
    Date journeyEndDate = getDateFromString(journeyEndDateStr, dateFormat);

    /*****************************************
    *
    *  resolve subscriberID
    *
    *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        log.info("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID ", getCustomerAlternateID, customerID);
        response.put("responseCode", "CustomerNotFound");
      }
    else
      {
        /*****************************************
        *
        *  getSubscriberProfile - include history
        *
        *****************************************/
        try
          {
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true);
            if (baseSubscriberProfile == null)
              {
                response.put("responseCode", "CustomerNotFound");
                log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
              }
            else
              {
                List<JSONObject> journeysJson = new ArrayList<JSONObject>();
                SubscriberHistory subscriberHistory = baseSubscriberProfile.getSubscriberHistory();
                if (subscriberHistory != null && subscriberHistory.getJourneyStatistics() != null)
                  {
                    
                    //
                    //  read campaigns
                    //
                    
                    Collection<GUIManagedObject> stroeRawJourneys = journeyService.getStoredJourneys();
                    List<Journey> storeJourneys = new ArrayList<Journey>();
                    for (GUIManagedObject storeJourney : stroeRawJourneys)
                      {
                        if (storeJourney instanceof Journey ) storeJourneys.add( (Journey) storeJourney);
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
                        //  read objective
                        //
                        
                        Collection<JourneyObjective> activejourneyObjectives = journeyObjectiveService.getActiveJourneyObjectives(SystemTime.getCurrentTime());
                       
                        //
                        //  filter activejourneyObjective by name
                        //
                        
                        List<JourneyObjective> journeyObjectives = activejourneyObjectives.stream().filter(journeyObj -> journeyObj.getJourneyObjectiveName().equals(journeyObjectiveName)).collect(Collectors.toList());
                        JourneyObjective exactJourneyObjective = journeyObjectives.size() > 0 ? journeyObjectives.get(0) : null;

                        //
                        //  filter
                        //
                        if (exactJourneyObjective == null)
                          storeJourneys = new ArrayList<Journey>();
                        else
                          storeJourneys = storeJourneys.stream().filter(journey -> (journey.getJourneyObjectiveInstances() != null && (journey.getJourneyObjectiveInstances().stream().filter(obj -> obj.getJourneyObjectiveID().equals(exactJourneyObjective.getJourneyObjectiveID())).count() > 0L))).collect(Collectors.toList());
                       
                      }
                    
                    //
                    //  read campaign statistics 
                    //
                    
                    List<JourneyStatistic> journeyStatistics = subscriberHistory.getJourneyStatistics();
                    
                    //
                    // change data structure to map
                    //
                    
                    Map<String, List<JourneyStatistic>> journeyStatisticsMap = journeyStatistics.stream().collect(Collectors.groupingBy(JourneyStatistic::getJourneyID));
                    
                    for (Journey storeJourney : storeJourneys)
                      {
                        
                        //
                        //  thisJourneyStatistics
                        //
                        
                        List<JourneyStatistic> thisJourneyStatistics = journeyStatisticsMap.get(storeJourney.getJourneyID());
                        
                        //
                        //  continue if not in stat
                        //
                        
                        if (thisJourneyStatistics == null || thisJourneyStatistics.isEmpty()) continue;
                        
                        //
                        // filter on journeyState
                        //

                        if (journeyState != null && !journeyState.isEmpty())
                          {
                            boolean criteriaSatisfied = false;
                            switch (journeyState)
                              {
                                case "active":
                                  criteriaSatisfied = storeJourney.getActive();
                                  break;
                                case "inactive":
                                  criteriaSatisfied = !storeJourney.getActive();
                                  break;
                              }
                            if (! criteriaSatisfied) continue;
                          }
                        
                        //
                        // filter on customerStatus
                        //
                        
                        boolean statusNotified = thisJourneyStatistics.stream().filter(journeyStat -> journeyStat.getStatusNotified()).count() > 0L ;
                        boolean statusConverted = thisJourneyStatistics.stream().filter(journeyStat -> journeyStat.getStatusConverted()).count() > 0L ;
                        boolean statusControlGroup = thisJourneyStatistics.stream().filter(journeyStat -> journeyStat.getStatusControlGroup()).count() > 0L ;
                        boolean statusUniversalControlGroup = thisJourneyStatistics.stream().filter(journeyStat -> journeyStat.getStatusUniversalControlGroup()).count() > 0L ;
                        boolean journeyComplete = thisJourneyStatistics.stream().filter(journeyStat -> journeyStat.getJourneyComplete()).count() > 0L ;
                        if (customerStatus != null)
                          {
                            CustomerStatusInJourney customerStatusInJourney = CustomerStatusInJourney.fromExternalRepresentation(customerStatus);
                            boolean criteriaSatisfied = false;
                            switch (customerStatusInJourney)
                              {
                                case ENTERED:
                                  criteriaSatisfied = !journeyComplete;
                                  break;
                                case NOTIFIED:
                                  criteriaSatisfied = statusNotified && !statusConverted && !journeyComplete;
                                  break;
                                case CONVERTED:
                                  criteriaSatisfied = statusConverted && !journeyComplete;
                                  break;
                                case CONTROL:
                                  criteriaSatisfied = statusControlGroup && !journeyComplete;
                                  break;
                                case UCG:
                                  criteriaSatisfied = statusUniversalControlGroup && !journeyComplete;
                                  break;
                                case NOTIFIED_CONVERTED:
                                  criteriaSatisfied = statusNotified && statusConverted && !journeyComplete;
                                  break;
                                case CONTROL_CONVERTED:
                                  criteriaSatisfied = statusControlGroup && statusConverted && !journeyComplete;
                                  break;
                                case COMPLETED:
                                  criteriaSatisfied = journeyComplete;
                                  break;
                                case UNKNOWN:
                                  break;
                              }
                            if (!criteriaSatisfied) continue;
                          }
                        
                        //
                        // prepare response
                        //
                        
                        Map<String, Object> journeyResponseMap = new HashMap<String, Object>();
                        journeyResponseMap.put("journeyID", storeJourney.getJourneyID());
                        journeyResponseMap.put("journeyName", storeJourney.getGUIManagedObjectName());
                        journeyResponseMap.put("startDate", getDateString(storeJourney.getEffectiveStartDate()));
                        journeyResponseMap.put("endDate", getDateString(storeJourney.getEffectiveEndDate()));
                        
                        //
                        // reverse sort
                        //

                        Collections.sort(thisJourneyStatistics, Collections.reverseOrder());

                        //
                        // prepare current node
                        //

                        JourneyStatistic subsLatestStatistic = thisJourneyStatistics.get(0);
                        Map<String, Object> currentState = new HashMap<String, Object>();
                        currentState.put("nodeID", subsLatestStatistic.getToNodeID());
                        currentState.put("nodeName", subsLatestStatistic.getToNodeID() == null ? null : storeJourney.getJourneyNode(subsLatestStatistic.getToNodeID()).getNodeName());
                        JSONObject currentStateJson = JSONUtilities.encodeObject(currentState);
                        
                        //
                        //  node history
                        //
                        
                        List<JSONObject> nodeHistoriesJson = new ArrayList<JSONObject>();
                        for (JourneyStatistic journeyStatistic : thisJourneyStatistics)
                          {
                            Map<String, Object> nodeHistoriesMap = new HashMap<String, Object>();
                            nodeHistoriesMap.put("fromNodeID", journeyStatistic.getFromNodeID());
                            nodeHistoriesMap.put("toNodeID", journeyStatistic.getToNodeID());
                            nodeHistoriesMap.put("fromNode", journeyStatistic.getFromNodeID() == null ? null : storeJourney.getJourneyNode(journeyStatistic.getFromNodeID()).getNodeName());
                            nodeHistoriesMap.put("toNode", journeyStatistic.getToNodeID() == null  ? null : storeJourney.getJourneyNode(journeyStatistic.getToNodeID()).getNodeName());
                            nodeHistoriesMap.put("transitionDate", getDateString(journeyStatistic.getTransitionDate()));
                            nodeHistoriesMap.put("linkID", journeyStatistic.getLinkID());
                            nodeHistoriesMap.put("deliveryRequestID", journeyStatistic.getDeliveryRequestID());
                            nodeHistoriesJson.add(JSONUtilities.encodeObject(nodeHistoriesMap));
                          }
                        
                        journeyResponseMap.put("statusNotified", statusNotified);
                        journeyResponseMap.put("statusConverted", statusConverted);
                        journeyResponseMap.put("statusControlGroup", statusControlGroup);
                        journeyResponseMap.put("statusUniversalControlGroup", statusUniversalControlGroup);
                        journeyResponseMap.put("journeyComplete", journeyComplete);
                        journeyResponseMap.put("nodeHistories", JSONUtilities.encodeArray(nodeHistoriesJson));
                        journeyResponseMap.put("currentState", currentStateJson);
                        journeysJson.add(JSONUtilities.encodeObject(journeyResponseMap));
                      }
                  }
                response.put("journeys", JSONUtilities.encodeArray(journeysJson));
                response.put("responseCode", "ok");
              }
          }
        catch (SubscriberProfileServiceException e)
          {
            throw new GUIManagerException(e);
          }
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
  * processGetGetCustomerCampaigns
  *
  *****************************************/

  private JSONObject processGetCustomerCampaigns(String userID, JSONObject jsonRoot) throws GUIManagerException
  {
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String campaignObjectiveName = JSONUtilities.decodeString(jsonRoot, "objectiveName", false);
    String campaignState = JSONUtilities.decodeString(jsonRoot, "campaignState", false);
    String customerStatus = JSONUtilities.decodeString(jsonRoot, "customerStatus", false);
    String campaignStartDateStr = JSONUtilities.decodeString(jsonRoot, "campaignStartDate", false);
    String campaignEndDateStr = JSONUtilities.decodeString(jsonRoot, "campaignEndDate", false);


    //
    // yyyy-MM-dd -- date format
    //

    String dateFormat = "yyyy-MM-dd";
    Date campaignStartDate = getDateFromString(campaignStartDateStr, dateFormat);
    Date campaignEndDate = getDateFromString(campaignEndDateStr, dateFormat);

    /*****************************************
    *
    *  resolve subscriberID
    *
    *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        log.info("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID ", getCustomerAlternateID, customerID);
        response.put("responseCode", "CustomerNotFound");
      }
    else
      {
        /*****************************************
        *
        *  getSubscriberProfile - include history
        *
        *****************************************/
        try
          {
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true);
            if (baseSubscriberProfile == null)
              {
                response.put("responseCode", "CustomerNotFound");
                log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
              }
            else
              {
                List<JSONObject> campaignsJson = new ArrayList<JSONObject>();
                SubscriberHistory subscriberHistory = baseSubscriberProfile.getSubscriberHistory();
                if (subscriberHistory != null && subscriberHistory.getJourneyStatistics() != null)
                  {
                    
                    //
                    //  read campaigns
                    //
                    
                    Collection<GUIManagedObject> storeRawCampaigns = journeyService.getStoredJourneys();
                    List<Journey> storeCampaigns = new ArrayList<Journey>();
                    for (GUIManagedObject storeCampaign : storeRawCampaigns)
                      {
                        if (storeCampaign instanceof Journey) storeCampaigns.add( (Journey) storeCampaign);
                      }
                    
                    //
                    // filter campaigns
                    //
                    
                    storeCampaigns = storeCampaigns.stream().filter(campaign -> campaign.getGUIManagedObjectType() == GUIManagedObjectType.Campaign).collect(Collectors.toList()); 
                    
                    //
                    // filter on campaignStartDate
                    //
                    
                    if (campaignStartDate != null )
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
                        //  read objective
                        //
                        
                        Collection<JourneyObjective> activecampaignObjectives = journeyObjectiveService.getActiveJourneyObjectives(SystemTime.getCurrentTime());
                       
                        //
                        //  lookup activecampaignObjective by name
                        //
                        
                        List<JourneyObjective> campaignObjectives = activecampaignObjectives.stream().filter(journeyObj -> journeyObj.getJourneyObjectiveName().equals(campaignObjectiveName)).collect(Collectors.toList());
                        JourneyObjective exactCampaignObjective = campaignObjectives.size() > 0 ? campaignObjectives.get(0) : null;
                        
                        //
                        //  filter
                        //
                        
                        if (exactCampaignObjective == null)
                          storeCampaigns = new ArrayList<Journey>();
                        else
                          storeCampaigns = storeCampaigns.stream().filter(campaign -> (campaign.getJourneyObjectiveInstances() != null && (campaign.getJourneyObjectiveInstances().stream().filter(obj -> obj.getJourneyObjectiveID().equals(exactCampaignObjective.getJourneyObjectiveID())).count() > 0L))).collect(Collectors.toList());
                       
                      }
                    
                    //
                    //  read campaign statistics 
                    //
                    
                    List<JourneyStatistic> subscribersCampaignStatistics = subscriberHistory.getJourneyStatistics();
                    
                    //
                    // change data structure to map
                    //
                    
                    Map<String, List<JourneyStatistic>> campaignStatisticsMap = subscribersCampaignStatistics.stream().collect(Collectors.groupingBy(JourneyStatistic::getJourneyID));
                    
                    for (Journey storeCampaign : storeCampaigns)
                      {
                        
                        //
                        //  thisCampaignStatistics
                        //
                        
                        List<JourneyStatistic> thisCampaignStatistics = campaignStatisticsMap.get(storeCampaign.getJourneyID());
                        
                        //
                        //  continue if not in stat
                        //
                        
                        if (thisCampaignStatistics == null || thisCampaignStatistics.isEmpty()) continue;
                        
                        //
                        // filter on campaignState
                        //

                        if (campaignState != null && !campaignState.isEmpty())
                          {
                            boolean criteriaSatisfied = false;
                            switch (campaignState)
                              {
                                case "active":
                                  criteriaSatisfied = storeCampaign.getActive();
                                  break;
                                case "inactive":
                                  criteriaSatisfied = !storeCampaign.getActive();
                                  break;
                              }
                            if (! criteriaSatisfied) continue;
                          }
                        
                        //
                        // filter on customerStatus
                        //
                        
                        boolean statusNotified = thisCampaignStatistics.stream().filter(campaignStat -> campaignStat.getStatusNotified()).count() > 0L ;
                        boolean statusConverted = thisCampaignStatistics.stream().filter(campaignStat -> campaignStat.getStatusConverted()).count() > 0L ;
                        boolean statusControlGroup = thisCampaignStatistics.stream().filter(campaignStat -> campaignStat.getStatusControlGroup()).count() > 0L ;
                        boolean statusUniversalControlGroup = thisCampaignStatistics.stream().filter(campaignStat -> campaignStat.getStatusUniversalControlGroup()).count() > 0L ;
                        boolean campaignComplete = thisCampaignStatistics.stream().filter(campaignStat -> campaignStat.getJourneyComplete()).count() > 0L ;
                        
                        if (customerStatus != null)
                          {
                            CustomerStatusInJourney customerStatusInJourney = CustomerStatusInJourney.fromExternalRepresentation(customerStatus);
                            boolean criteriaSatisfied = false;
                            switch (customerStatusInJourney)
                              {
                                case ENTERED:
                                  criteriaSatisfied = !campaignComplete;
                                  break;
                                case NOTIFIED:
                                  criteriaSatisfied = statusNotified && !statusConverted && !campaignComplete;
                                  break;
                                case CONVERTED:
                                  criteriaSatisfied = statusConverted && !campaignComplete;
                                  break;
                                case CONTROL:
                                  criteriaSatisfied = statusControlGroup && !campaignComplete;
                                  break;
                                case UCG:
                                  criteriaSatisfied = statusUniversalControlGroup && !campaignComplete;
                                  break;
                                case NOTIFIED_CONVERTED:
                                  criteriaSatisfied = statusNotified && statusConverted && !campaignComplete;
                                  break;
                                case CONTROL_CONVERTED:
                                  criteriaSatisfied = statusControlGroup && statusConverted && !campaignComplete;
                                  break;
                                case COMPLETED:
                                  criteriaSatisfied = campaignComplete;
                                  break;
                                case UNKNOWN:
                                  break;
                              }
                            if (! criteriaSatisfied) continue;
                          }
                        
                        //
                        // prepare response
                        //
                        
                        Map<String, Object> campaignResponseMap = new HashMap<String, Object>();
                        campaignResponseMap.put("campaignID", storeCampaign.getJourneyID());
                        campaignResponseMap.put("campaignName", storeCampaign.getGUIManagedObjectName());
                        campaignResponseMap.put("startDate", getDateString(storeCampaign.getEffectiveStartDate()));
                        campaignResponseMap.put("endDate", getDateString(storeCampaign.getEffectiveEndDate()));
                        
                        //
                        // reverse sort
                        //

                        Collections.sort(thisCampaignStatistics, Collections.reverseOrder());

                        //
                        // prepare current node
                        //

                        JourneyStatistic subsLatestStatistic = thisCampaignStatistics.get(0);
                        Map<String, Object> currentState = new HashMap<String, Object>();
                        currentState.put("nodeID", subsLatestStatistic.getToNodeID());
                        currentState.put("nodeName", subsLatestStatistic.getToNodeID() == null ? null : storeCampaign.getJourneyNode(subsLatestStatistic.getToNodeID()).getNodeName());
                        JSONObject currentStateJson = JSONUtilities.encodeObject(currentState);

                        //
                        //  node history
                        //
                        
                        List<JSONObject> nodeHistoriesJson = new ArrayList<JSONObject>();
                        for (JourneyStatistic campaignStatistic : thisCampaignStatistics)
                          {
                            Map<String, Object> nodeHistoriesMap = new HashMap<String, Object>();
                            nodeHistoriesMap.put("fromNodeID", campaignStatistic.getFromNodeID());
                            nodeHistoriesMap.put("toNodeID", campaignStatistic.getToNodeID());
                            nodeHistoriesMap.put("fromNode", campaignStatistic.getFromNodeID() == null ? null : storeCampaign.getJourneyNode(campaignStatistic.getFromNodeID()).getNodeName());
                            nodeHistoriesMap.put("toNode", campaignStatistic.getToNodeID() == null ? null : storeCampaign.getJourneyNode(campaignStatistic.getToNodeID()).getNodeName());
                            nodeHistoriesMap.put("transitionDate", getDateString(campaignStatistic.getTransitionDate()));
                            nodeHistoriesMap.put("linkID", campaignStatistic.getLinkID());
                            nodeHistoriesMap.put("deliveryRequestID", campaignStatistic.getDeliveryRequestID());
                            nodeHistoriesJson.add(JSONUtilities.encodeObject(nodeHistoriesMap));
                          }
                        
                        campaignResponseMap.put("statusNotified", statusNotified);
                        campaignResponseMap.put("statusConverted", statusConverted);
                        campaignResponseMap.put("statusControlGroup", statusControlGroup);
                        campaignResponseMap.put("statusUniversalControlGroup", statusUniversalControlGroup);
                        campaignResponseMap.put("campaignComplete", campaignComplete);
                        campaignResponseMap.put("nodeHistories", JSONUtilities.encodeArray(nodeHistoriesJson));
                        campaignResponseMap.put("currentState", currentStateJson);
                        campaignsJson.add(JSONUtilities.encodeObject(campaignResponseMap));
                      }
                  }
                response.put("campaigns", JSONUtilities.encodeArray(campaignsJson));
                response.put("responseCode", "ok");
              }
          }
        catch (SubscriberProfileServiceException e)
          {
            throw new GUIManagerException(e);
          }
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
  * refreshUCG
  *
  *****************************************/

  private JSONObject processRefreshUCG(String userID, JSONObject jsonRoot) throws GUIManagerException
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    Date now = SystemTime.getCurrentTime();
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  identify active UCGRule (if any)
    *
    *****************************************/

    UCGRule activeUCGRule = null;
    for (UCGRule ucgRule : ucgRuleService.getActiveUCGRules(now))
      {
        activeUCGRule = ucgRule;
      }

    /*****************************************
    *
    *  refresh
    *
    *****************************************/

    if (activeUCGRule != null)
      {
        activeUCGRule.setRefreshEpoch(activeUCGRule.getRefreshEpoch() != null ? activeUCGRule.getRefreshEpoch() + 1 : 1);
        ucgRuleService.putUCGRule(activeUCGRule,segmentationDimensionService,false, userID);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (activeUCGRule != null) ? "ok" : "noActiveUCGRule");
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processGetCustomerAvailableCampaigns
  *
  *****************************************/

  private JSONObject processGetCustomerAvailableCampaigns(String userID, JSONObject jsonRoot) throws GUIManagerException
  {
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
     *
     * argument
     *
     ****************************************/

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);

    /*****************************************
     *
     * resolve subscriberID
     *
     *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        response.put("responseCode", "CustomerNotFound");
      } 
    else
      {
        /*****************************************
         *
         * getSubscriberProfile - include history
         *
         *****************************************/
        try
          {
            SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true);
            if (subscriberProfile == null)
              {
                response.put("responseCode", "CustomerNotFound");
              } 
            else
              {
                Date now = SystemTime.getCurrentTime();
                SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, now);
                SubscriberHistory subscriberHistory = subscriberProfile.getSubscriberHistory();
                Map<String, List<JourneyStatistic>> campaignStatisticsMap = new HashMap<String, List<JourneyStatistic>>();
                
                //
                //  journey statistics
                //
                
                if (subscriberHistory != null && subscriberHistory.getJourneyStatistics() != null)
                  {
                    campaignStatisticsMap = subscriberHistory.getJourneyStatistics().stream().collect(Collectors.groupingBy(JourneyStatistic::getJourneyID));
                  }
                
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
                    if (campaignStatisticsMap.get(elgibleActiveCampaign.getJourneyID()) == null || campaignStatisticsMap.get(elgibleActiveCampaign.getJourneyID()).isEmpty())
                      {
                        //
                        // prepare and decorate response
                        //
                        
                        Map<String, Object> campaignMap = new HashMap<String, Object>();
                        campaignMap.put("campaignID", elgibleActiveCampaign.getJourneyID());
                        campaignMap.put("campaignName", elgibleActiveCampaign.getJourneyName());
                        campaignMap.put("description", journeyService.generateResponseJSON(elgibleActiveCampaign, true, now).get("description"));
                        campaignMap.put("startDate", getDateString(elgibleActiveCampaign.getEffectiveStartDate()));
                        campaignMap.put("endDate", getDateString(elgibleActiveCampaign.getEffectiveEndDate()));
                        campaignsJson.add(JSONUtilities.encodeObject(campaignMap));
                      }
                  }
                response.put("campaigns", JSONUtilities.encodeArray(campaignsJson));
                response.put("responseCode", "ok");
              }
          }
        catch (SubscriberProfileServiceException e)
          {
            log.error("SubscriberProfileServiceException ", e.getMessage());
            throw new GUIManagerException(e);
          }
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

  private JSONObject processUpdateCustomer(String userID, JSONObject jsonRoot) throws GUIManagerException
  {
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
     *
     * argument
     *
     ****************************************/

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);

    /*****************************************
     *
     * resolve subscriberID
     *
     *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        response.put("responseCode", "CustomerNotFound");
      } 
    else
      {
        jsonRoot.put("subscriberID", subscriberID);
        SubscriberProfileForceUpdate subscriberProfileForceUpdate = new SubscriberProfileForceUpdate(jsonRoot);
        
        //
        //  submit to kafka
        //

        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getSubscriberProfileForceUpdateTopic(), StringKey.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), new StringKey(subscriberProfileForceUpdate.getSubscriberID())), SubscriberProfileForceUpdate.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), subscriberProfileForceUpdate)));
        
        response.put("responseCode", "ok");
      }

    /*****************************************
     *
     * return
     *
     *****************************************/

    return JSONUtilities.encodeObject(response);
  }

  /****************************************
  *
  *  resolveSubscriberID
  *
  ****************************************/

  private String resolveSubscriberID(String customerID)
  {
    String result = null;
    try
      {
        result = subscriberIDService.getSubscriberID(getCustomerAlternateID, customerID);
      }
    catch (SubscriberIDServiceException e)
      {
        log.error("SubscriberIDServiceException can not resolve subscriberID for {} error is {}", customerID, e.getMessage());
      }
    return result;
  }

  /*****************************************
  *
  *  journeyCount
  *
  *****************************************/

  private int journeyCount(GUIManagedObjectType journeyType)
  {
    int result = 0;
    for (GUIManagedObject journey : journeyService.getStoredJourneys())
      {
        if (journey.getGUIManagedObjectType() == journeyType)
          {
            result += 1;
          }
      }
    return result;
  }

  /*****************************************
  *
  *  getDateFromString
  *
  *****************************************/

  private Date getDateFromString(String dateString, String dateFormat)
  {
    Date result = null;
    if (dateString != null)
      {
        result = RLMDateUtils.parseDate(dateString, dateFormat, Deployment.getBaseTimeZone());
      }
    return result;
  }

  /*****************************************
  *
  *  class APISimpleHandler
  *
  *****************************************/

  private class APISimpleHandler implements HttpHandler
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

    private APISimpleHandler(API api)
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
      handleSimpleHandler(api, exchange);
    }
  }
  
  /*****************************************
  *
  *  class APIComplexHandler
  *
  *****************************************/
  
  private class APIComplexHandler implements HttpHandler
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

    private APIComplexHandler(API api)
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
      handleComplexAPI(api, exchange);
    }
  }
  

  /*****************************************
  *
  *  class ResolvedFieldType
  *
  *****************************************/

  private class ResolvedFieldType
  {
    //
    //  attributes
    //

    private CriterionDataType dataType;
    private Set<JSONObject> availableValues;

    //
    //  accessors
    //

    CriterionDataType getDataType() { return dataType; }
    Set<JSONObject> getAvailableValues() { return availableValues; }

    //
    //  constructor
    //

    ResolvedFieldType(CriterionDataType dataType, List<JSONObject> availableValues)
    {
      this.dataType = dataType.getBaseType();
      this.availableValues = availableValues != null ? new HashSet<JSONObject>(availableValues) : null;
    }

    /*****************************************
    *
    *  equals
    *
    *****************************************/

    public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof ResolvedFieldType)
        {
          ResolvedFieldType resolvedFieldType = (ResolvedFieldType) obj;
          result = true;
          result = result && Objects.equals(dataType, resolvedFieldType.getDataType());
          result = result && Objects.equals(availableValues, resolvedFieldType.getAvailableValues());
        }
      return result;
    }

    /*****************************************
    *
    *  hashCode
    *
    *****************************************/

    public int hashCode()
    {
      return dataType.hashCode() + (availableValues != null ? availableValues.hashCode() : 0);
    }
  }

  /*****************************************
  *
  *  class DeliverableSourceService
  *
  *****************************************/

  private class DeliverableSourceService
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private volatile boolean stopRequested = false;
    private String deliverableSourceTopic;
    private KafkaConsumer<byte[], byte[]> deliverableSourceConsumer;
    Thread deliverableSourceReaderThread = null;

    //
    //  serdes
    //

    private ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
    private ConnectSerde<DeliverableSource> deliverableSourceSerde = DeliverableSource.serde();

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public DeliverableSourceService(String bootstrapServers, String groupID, String deliverableSourceTopic)
    {
      //
      // set up consumer
      //

      Properties consumerProperties = new Properties();
      consumerProperties.put("bootstrap.servers", bootstrapServers);
      consumerProperties.put("group.id", groupID);
      consumerProperties.put("auto.offset.reset", "earliest");
      consumerProperties.put("enable.auto.commit", "false");
      consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      deliverableSourceConsumer = new KafkaConsumer<>(consumerProperties);

      //
      //  subscribe to topic
      //

      deliverableSourceConsumer.subscribe(Arrays.asList(deliverableSourceTopic));
    }

    /*****************************************
    *
    *  start
    *
    *****************************************/

    public void start()
    {
      Runnable deliverableSourceReader = new Runnable() { @Override public void run() { readDeliverableSource(deliverableSourceConsumer); } };
      deliverableSourceReaderThread = new Thread(deliverableSourceReader, "DeliverableSourceReader");
      deliverableSourceReaderThread.start();
    }

    /*****************************************
    *
    *  stop
    *
    *****************************************/

    public synchronized void stop()
    {
      //
      //  mark stopRequested
      //

      stopRequested = true;

      //
      //  wake sleeping polls (if necessary)
      //

      if (deliverableSourceConsumer != null) deliverableSourceConsumer.wakeup();

      //
      //  wait for threads to finish
      //

      try
        {
          if (deliverableSourceReaderThread != null) deliverableSourceReaderThread.join();
        }
      catch (InterruptedException e)
        {
          // nothing
        }

      //
      //  close
      //

      if (deliverableSourceConsumer != null) deliverableSourceConsumer.close();
    }

    /****************************************
    *
    *  readDeliverableSource
    *
    ****************************************/

    private void readDeliverableSource(KafkaConsumer<byte[], byte[]> consumer)
    {
      do
        {
          //
          // poll
          //

          ConsumerRecords<byte[], byte[]> deliverableSourceRecords;
          try
            {
              deliverableSourceRecords = consumer.poll(5000);
            }
          catch (WakeupException e)
            {
              deliverableSourceRecords = ConsumerRecords.<byte[], byte[]>empty();
            }

          //
          //  processing?
          //

          if (stopRequested) continue;

          //
          //  process
          //

          Date now = SystemTime.getCurrentTime();
          for (ConsumerRecord<byte[], byte[]> deliverableSourceRecord : deliverableSourceRecords)
            {
              //
              //  parse
              //

              DeliverableSource deliverableSource = null;
              try
                {
                  deliverableSource = deliverableSourceSerde.deserializer().deserialize(deliverableSourceRecord.topic(), deliverableSourceRecord.value());
                }
              catch (SerializationException e)
                {
                  log.info("error reading deliverableSource: {}", e.getMessage());
                }
              if (deliverableSource != null) log.info("read deliverableSource {}", deliverableSource);

              //
              //  process
              //

              if (deliverableSource != null)
                {
                  GUIManagedObject existingGUIManagedObject = deliverableService.getStoredDeliverableByName(deliverableSource.getName());
                  if (existingGUIManagedObject != null)
                    {
                      deliverableSource.setID(existingGUIManagedObject.getGUIManagedObjectID());
                    }
                  processPutDeliverable("0", deliverableSource.getDeliverableJSON());
                }
            }
        }
      while (!stopRequested);
    }
  }

  /*****************************************
  *
  *  getFeatureName
  *
  *****************************************/

  private String getFeatureName(DeliveryRequest.Module module, String featureId)
  {
    String featureName = null;

    switch (module)
      {
        case Campaign_Manager:
          GUIManagedObject campaign = journeyService.getStoredJourney(featureId);
          campaign = (campaign != null && campaign.getGUIManagedObjectType() == GUIManagedObjectType.Campaign) ? campaign : null;
          featureName = campaign == null ? null : campaign.getGUIManagedObjectName();
          break;

        case Journey_Manager:
          GUIManagedObject journey = journeyService.getStoredJourney(featureId);
          journey = (journey != null && journey.getGUIManagedObjectType() == GUIManagedObjectType.Journey) ? journey : null;
          featureName = journey == null ? null : journey.getGUIManagedObjectName();
          break;

        case Offer_Catalog:
          featureName = offerService.getStoredOffer(featureId).getGUIManagedObjectName();
          break;

        case Delivery_Manager:
          featureName = "Delivery_Manager-its temp"; //To DO
          break;

        case REST_API:
          featureName = "REST_API-its temp"; //To DO
          break;

        case Unknown:
          featureName = "Unknown";
          break;
      }
    return featureName;
  }

  /*****************************************
  *
  *  class GUIManagerContext
  *
  *****************************************/

  public static class GUIManagerContext
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private JourneyService journeyService;
    private SegmentationDimensionService segmentationDimensionService;
    private PointService pointService;
    private OfferService offerService;
    private ReportService reportService;
    private PaymentMeanService paymentMeanService;
    private ScoringStrategyService scoringStrategyService;
    private PresentationStrategyService presentationStrategyService;
    private CallingChannelService callingChannelService;
    private SalesChannelService salesChannelService;
    private SupplierService supplierService;
    private ProductService productService;
    private CatalogCharacteristicService catalogCharacteristicService;
    private ContactPolicyService contactPolicyService;
    private JourneyObjectiveService journeyObjectiveService;
    private OfferObjectiveService offerObjectiveService;
    private ProductTypeService productTypeService;
    private UCGRuleService ucgRuleService;
    private DeliverableService deliverableService;
    private TokenTypeService tokenTypeService;
    private MailTemplateService mailTemplateService;
    private SMSTemplateService smsTemplateService;
    private SubscriberProfileService subscriberProfileService;
    private SubscriberIDService subscriberIDService;
    private DeliverableSourceService deliverableSourceService;
    private UploadedFileService uploadedFileService;
    private TargetService targetService;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public JourneyService getJourneyService() { return journeyService; }
    public SegmentationDimensionService getSegmentationDimensionService() { return segmentationDimensionService; }
    public PointService getPointService() { return pointService; }
    public OfferService getOfferService() { return offerService; }
    public ReportService getReportService() { return reportService; }
    public PaymentMeanService getPaymentMeanService() { return paymentMeanService; }
    public ScoringStrategyService getScoringStrategyService() { return scoringStrategyService; }
    public PresentationStrategyService getPresentationStrategyService() { return presentationStrategyService; }
    public CallingChannelService getCallingChannelService() { return callingChannelService; }
    public SalesChannelService getSalesChannelService() { return salesChannelService; }
    public SupplierService getSupplierService() { return supplierService; }
    public ProductService getProductService() { return productService; }
    public CatalogCharacteristicService getCatalogCharacteristicService() { return catalogCharacteristicService; }
    public ContactPolicyService getContactPolicyService() { return contactPolicyService; }
    public JourneyObjectiveService getJourneyObjectiveService() { return journeyObjectiveService; }
    public OfferObjectiveService getOfferObjectiveService() { return offerObjectiveService; }
    public ProductTypeService getProductTypeService() { return productTypeService; }
    public UCGRuleService getUcgRuleService() { return ucgRuleService; }
    public DeliverableService getDeliverableService() { return deliverableService; }
    public TokenTypeService getTokenTypeService() { return tokenTypeService; }
    public MailTemplateService getMailTemplateService() { return mailTemplateService; }
    public SMSTemplateService getSmsTemplateService() { return smsTemplateService; }
    public SubscriberProfileService getSubscriberProfileService() { return subscriberProfileService; }
    public SubscriberIDService getSubscriberIDService() { return subscriberIDService; }
    public DeliverableSourceService getDeliverableSourceService() { return deliverableSourceService; }
    public UploadedFileService getUploadFileService() { return uploadedFileService; }
    public TargetService getTargetService() { return targetService; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public GUIManagerContext(JourneyService journeyService, SegmentationDimensionService segmentationDimensionService, PointService pointService, OfferService offerService, ReportService reportService, PaymentMeanService paymentMeanService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, CallingChannelService callingChannelService, SalesChannelService salesChannelService, SupplierService supplierService, ProductService productService, CatalogCharacteristicService catalogCharacteristicService, ContactPolicyService contactPolicyService, JourneyObjectiveService journeyObjectiveService, OfferObjectiveService offerObjectiveService, ProductTypeService productTypeService, UCGRuleService ucgRuleService, DeliverableService deliverableService, TokenTypeService tokenTypeService, MailTemplateService mailTemplateService, SMSTemplateService smsTemplateService, SubscriberProfileService subscriberProfileService, SubscriberIDService subscriberIDService, DeliverableSourceService deliverableSourceService, UploadedFileService uploadedFileService, TargetService targetService)
    {
      this.journeyService = journeyService;
      this.segmentationDimensionService = segmentationDimensionService;
      this.pointService = pointService;
      this.offerService = offerService;
      this.reportService = reportService;
      this.paymentMeanService = paymentMeanService;
      this.scoringStrategyService = scoringStrategyService;
      this.presentationStrategyService = presentationStrategyService;
      this.callingChannelService = callingChannelService;
      this.salesChannelService = salesChannelService;
      this.supplierService = supplierService;
      this.productService = productService;
      this.catalogCharacteristicService = catalogCharacteristicService;
      this.contactPolicyService = contactPolicyService;
      this.journeyObjectiveService = journeyObjectiveService;
      this.offerObjectiveService = offerObjectiveService;
      this.productTypeService = productTypeService;
      this.ucgRuleService = ucgRuleService;
      this.deliverableService = deliverableService;
      this.tokenTypeService = tokenTypeService;
      this.mailTemplateService = mailTemplateService;
      this.smsTemplateService = smsTemplateService;
      this.subscriberProfileService = subscriberProfileService;
      this.subscriberIDService = subscriberIDService;
      this.deliverableSourceService = deliverableSourceService;
      this.uploadedFileService = uploadedFileService;
      this.targetService = targetService;
    }
  }

  /*****************************************
  *
  *  class GUIManagerException
  *
  *****************************************/

  public static class GUIManagerException extends Exception
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String responseParameter;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getResponseParameter() { return responseParameter; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public GUIManagerException(String responseMessage, String responseParameter)
    {
      super(responseMessage);
      this.responseParameter = responseParameter;
    }

    /*****************************************
    *
    *  constructor - exception
    *
    *****************************************/

    public GUIManagerException(Throwable e)
    {
      super(e.getMessage(), e);
      this.responseParameter = null;
    }

    /*****************************************
    *
    *  toString
    *
    *****************************************/

    public String toString()
    {
      return super.toString() + "(" + responseParameter + ")";
    }
  }

  /*****************************************
  *
  *  normalizeSegmentName
  *
  *****************************************/

  private String normalizeSegmentName(String segmentName)
  {
    return segmentName.replace(" ",".");
  }

  /*****************************************
  *
  *  getDateString
  *
  *****************************************/

  private String getDateString(Date date)
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
}
