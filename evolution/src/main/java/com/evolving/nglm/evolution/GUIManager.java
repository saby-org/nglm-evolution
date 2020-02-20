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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.SwitchPoint;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUpload;
import org.apache.commons.fileupload.RequestContext;
import org.apache.commons.fileupload.util.Streams;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.zookeeper.ZooKeeper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
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
import org.json.simple.parser.ParseException;
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
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ReferenceDataValue;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.StringValue;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.core.SubscriberIDService.SubscriberIDServiceException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.UniqueKeyServer;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryRequest;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.DeliveryManagerAccount.Account;
import com.evolving.nglm.evolution.DeliveryRequest.ActivityType;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.EmptyFulfillmentManager.EmptyFulfillmentRequest;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionOperator;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIService.GUIManagedObjectListener;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;
import com.evolving.nglm.evolution.Journey.BulkType;
import com.evolving.nglm.evolution.Journey.GUINode;
import com.evolving.nglm.evolution.Journey.JourneyStatus;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.Journey.TargetingType;
import com.evolving.nglm.evolution.JourneyHistory.NodeHistory;
import com.evolving.nglm.evolution.JourneyService.JourneyListener;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.LoyaltyProgramHistory.TierHistory;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.Report.SchedulingInterval;
import com.evolving.nglm.evolution.SegmentationDimension.SegmentationDimensionTargetingType;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;
import com.evolving.nglm.evolution.Token.TokenStatus;
import com.evolving.nglm.evolution.offeroptimizer.DNBOMatrixAlgorithmParameters;
import com.evolving.nglm.evolution.offeroptimizer.GetOfferException;
import com.evolving.nglm.evolution.offeroptimizer.ProposedOfferDetails;
import com.evolving.nglm.evolution.reports.ReportUtils;
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

  public enum API
  {
    //
    //  GUI APIs
    //  

    getStaticConfiguration("getStaticConfiguration"),
    getSupportedLanguages("getSupportedLanguages"),
    getSupportedCurrencies("getSupportedCurrencies"),
    getSupportedTimeUnits("getSupportedTimeUnits"),
    getSupportedRelationships("getSupportedRelationships"),
    getServiceTypes("getServiceTypes"),
    getCallingChannelProperties("getCallingChannelProperties"),
    getCatalogCharacteristicUnits("getCatalogCharacteristicUnits"),
    getSupportedDataTypes("getSupportedDataTypes"),
    getSupportedEvents("getSupportedEvents"),
    getLoyaltyProgramPointsEvents("getLoyaltyProgramPointsEvents"),
    getSupportedTargetingTypes("getSupportedTargetingTypes"),
    getProfileCriterionFields("getProfileCriterionFields"),
    getProfileCriterionFieldIDs("getProfileCriterionFieldIDs"),
    getProfileCriterionField("getProfileCriterionField"),
    getFullProfileCriterionFields("getFullProfileCriterionFields"),
    getFullProfileCriterionFieldIDs("getFullProfileCriterionFieldIDs"),
    getFullProfileCriterionField("getFullProfileCriterionField"),
    getPresentationCriterionFields("getPresentationCriterionFields"),
    getPresentationCriterionFieldIDs("getPresentationCriterionFieldIDs"),
    getPresentationCriterionField("getPresentationCriterionField"),
    getJourneyCriterionFields("getJourneyCriterionFields"),
    getJourneyCriterionFieldIDs("getJourneyCriterionFieldIDs"),
    getJourneyCriterionField("getJourneyCriterionField"),
    getOfferCategories("getOfferCategories"),
    getOfferProperties("getOfferProperties"),
    getScoringEngines("scoringEngines"),
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
    getWorkflowToolbox("getWorkflowToolbox"),
    getFullWorkflowList("getFullWorkflowList"),
    getWorkflowList("getWorkflowList"),
    getWorkflowSummaryList("getWorkflowSummaryList"),
    getWorkflow("getWorkflow"),
    putWorkflow("putWorkflow"),
    removeWorkflow("removeWorkflow"),
    getBulkCampaignList("getBulkCampaignList"),
    getBulkCampaignSummaryList("getBulkCampaignSummaryList"),
    getBulkCampaign("getBulkCampaign"),
    putBulkCampaign("putBulkCampaign"),
    removeBulkCampaign("removeBulkCampaign"),
    startBulkCampaign("startBulkCampaign"),
    stopBulkCampaign("stopBulkCampaign"),
    getJourneyTemplateList("getJourneyTemplateList"),
    getJourneyTemplateSummaryList("getJourneyTemplateSummaryList"),
    getJourneyTemplate("getJourneyTemplate"),
    putJourneyTemplate("putJourneyTemplate"),
    removeJourneyTemplate("removeJourneyTemplate"),
    getJourneyNodeCount("getJourneyNodeCount"),
    getSegmentationDimensionList("getSegmentationDimensionList"),
    getSegmentationDimensionSummaryList("getSegmentationDimensionSummaryList"),
    getSegmentationDimension("getSegmentationDimension"),
    putSegmentationDimension("putSegmentationDimension"),
    removeSegmentationDimension("removeSegmentationDimension"),
    getCountBySegmentationRanges("getCountBySegmentationRanges"),
    getCountBySegmentationEligibility("getCountBySegmentationEligibility"),
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
    getDNBOMatrixList("getDNBOMatrixList"),
    getDNBOMatrixSummaryList("getDNBOMatrixSummaryList"),
    getDNBOMatrix("getDNBOMatrix"),
    putDNBOMatrix("putDNBOMatrix"),
    removeDNBOMatrix("removeDNBOMatrix"),
    getScoringTypesList("getScoringTypesList"),
    getDNBOMatrixVariablesList("getDNBOMatrixVariablesList"),
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
    
    getVoucherTypeList("getVoucherTypeList"),
    getVoucherTypeSummaryList("getVoucherTypeSummaryList"),
    putVoucherType("putVoucherType"),
    getVoucherType("getVoucherType"),
    removeVoucherType("removeVoucherType"),

    getVoucherCodeFormatList("getVoucherCodeFormatList"),
    
    getVoucherList("getVoucherList"),
    getVoucherSummaryList("getVoucherSummaryList"),
    putVoucher("putVoucher"),
    getVoucher("getVoucher"),
    removeVoucher("removeVoucher"),

    getMailTemplateList("getMailTemplateList"),
    getFullMailTemplateList("getFullMailTemplateList"),
    getMailTemplateSummaryList("getMailTemplateSummaryList"),
    getMailTemplate("getMailTemplate"),
    putMailTemplate("putMailTemplate"),
    removeMailTemplate("removeMailTemplate"),
    getSMSTemplateList("getSMSTemplateList"),
    getFullSMSTemplateList("getFullSMSTemplateList"),
    getSMSTemplateSummaryList("getSMSTemplateSummaryList"),
    getSMSTemplate("getSMSTemplate"),
    putSMSTemplate("putSMSTemplate"),
    removeSMSTemplate("removeSMSTemplate"),
    getPushTemplateList("getPushTemplateList"),
    getFullPushTemplateList("getFullPushTemplateList"),
    getPushTemplateSummaryList("getPushTemplateSummaryList"),
    getPushTemplate("getPushTemplate"),
    putPushTemplate("putPushTemplate"),
    removePushTemplate("removePushTemplate"),
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
    getCustomerPoints("getCustomerPoints"),
    getCustomerLoyaltyPrograms("getCustomerLoyaltyPrograms"),
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
    updateCustomerParent("updateCustomerParent"),
    removeCustomerParent("removeCustomerParent"),
    getCommunicationChannelList("getCommunicationChannelList"),
    getCommunicationChannelSummaryList("getCommunicationChannelSummaryList"),
    getCommunicationChannel("getCommunicationChannel"),
    putCommunicationChannel("putCommunicationChannel"),
    removeCommunicationChannel("removeCommunicationChannel"),
    getBlackoutPeriodsList("getBlackoutPeriodsList"),
    getBlackoutPeriodsSummaryList("getBlackoutPeriodsSummaryList"),
    getBlackoutPeriods("getBlackoutPeriods"),
    putBlackoutPeriods("putBlackoutPeriods"),
    removeBlackoutPeriods("removeBlackoutPeriods"),
    getLoyaltyProgramTypeList("getLoyaltyProgramTypeList"),
    getLoyaltyProgramList("getLoyaltyProgramList"),
    getLoyaltyProgramSummaryList("getLoyaltyProgramSummaryList"),
    getLoyaltyProgram("getLoyaltyProgram"),
    putLoyaltyProgram("putLoyaltyProgram"),
    removeLoyaltyProgram("removeLoyaltyProgram"),
    getResellerList("getResellerList"),
    getResellerSummaryList("getResellerSummaryList"),
    getReseller("getReseller"),
    putReseller("putReseller"),
    removeReseller("removeReseller"),
    enterCampaign("enterCampaign"),
    creditBonus("creditBonus"),
    debitBonus("debitBonus"),
    getExclusionInclusionTargetList("getExclusionInclusionTargetList"),
    getExclusionInclusionTargetSummaryList("getExclusionInclusionTargetSummaryList"),
    putExclusionInclusionTarget("putExclusionInclusionTarget"),
    getExclusionInclusionTarget("getExclusionInclusionTarget"),
    removeExclusionInclusionTarget("removeExclusionInclusionTarget"),
    getSegmentContactPolicyList("getSegmentContactPolicyList"),
    getSegmentContactPolicySummaryList("getSegmentContactPolicySummaryList"),
    putSegmentContactPolicy("putSegmentContactPolicy"),
    getSegmentContactPolicy("getSegmentContactPolicy"),
    removeSegmentContactPolicy("removeSegmentContactPolicy"),
    getBillingModes("getBillingModes"),
    getPartnerTypes("getPartnerTypes"),
    getCriterionFieldAvailableValuesList("getCriterionFieldAvailableValuesList"),
    getCriterionFieldAvailableValuesSummaryList("getCriterionFieldAvailableValuesSummaryList"),
    getCriterionFieldAvailableValues("getCriterionFieldAvailableValues"),
    putCriterionFieldAvailableValues("putCriterionFieldAvailableValues"),
    removeCriterionFieldAvailableValues("removeCriterionFieldAvailableValues"),
    getEffectiveSystemTime("getEffectiveSystemTime"),
    getCustomerNBOs("getCustomerNBOs"),
    getTokensCodesList("getTokensCodesList"),
    acceptOffer("acceptOffer"),
    getTokenEventDetails("getTokenEventDetails"),
    getTenantList("getTenantList"),

    //
    //  configAdaptor APIs
    //

    configAdaptorSupportedLanguages("configAdaptorSupportedLanguages"),
    configAdaptorSubscriberMessageTemplate("configAdaptorSubscriberMessageTemplate"),
    configAdaptorOffer("configAdaptorOffer"),
    configAdaptorProduct("configAdaptorProduct"),
    configAdaptorPresentationStrategy("configAdaptorPresentationStrategy"),
    configAdaptorScoringStrategy("configAdaptorScoringStrategy"),
    configAdaptorCallingChannel("configAdaptorCallingChannel"),
    configAdaptorSalesChannel("configAdaptorSalesChannel"),
    configAdaptorCommunicationChannel("configAdaptorCommunicationChannel"),
    configAdaptorBlackoutPeriods("configAdaptorBlackoutPeriods"),
    configAdaptorContactPolicy("configAdaptorContactPolicy"),
    configAdaptorSegmentationDimension("configAdaptorSegmentationDimension"),
    configAdaptorCampaign("configAdaptorCampaign"),
    configAdaptorJourneyObjective("configAdaptorJourneyObjective"),
    configAdaptorProductType("configAdaptorProductType"),
    configAdaptorOfferObjective("configAdaptorOfferObjective"),
    configAdaptorScoringEngines("configAdaptorScoringEngines"),
    configAdaptorPresentationCriterionFields("configAdaptorPresentationCriterionFields"),
    configAdaptorDefaultNoftificationDailyWindows("configAdaptorDefaultNoftificationDailyWindows"),
    configAdaptorDeliverable("configAdaptorDeliverable"),
    
    getSourceAddressList("getSourceAddressList"),
    getSourceAddressSummaryList("getSourceAddressSummaryList"),
    getSourceAddress("getSourceAddress"),
    putSourceAddress("putSourceAddress"),
    removeSourceAddress("removeSourceAddress"),
    
    //
    //  structor
    //
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
  private DynamicCriterionFieldService dynamicCriterionFieldService;
  private JourneyService journeyService;
  private JourneyTemplateService journeyTemplateService;
  private SegmentationDimensionService segmentationDimensionService;
  private PointService pointService;
  private OfferService offerService;
  private ReportService reportService;
  private PaymentMeanService paymentMeanService;
  private ScoringStrategyService scoringStrategyService;
  private PresentationStrategyService presentationStrategyService;
  private DNBOMatrixService dnboMatrixService;
  private CallingChannelService callingChannelService;
  private SalesChannelService salesChannelService;
  private SourceAddressService sourceAddressService;
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
  private VoucherTypeService voucherTypeService;
  private VoucherService voucherService;
  private SubscriberMessageTemplateService subscriberMessageTemplateService;
  private SubscriberProfileService subscriberProfileService;
  private SubscriberIDService subscriberIDService;
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private ReferenceDataReader<String,JourneyTrafficHistory> journeyTrafficReader;
  private ReferenceDataReader<String,RenamedProfileCriterionField> renamedProfileCriterionFieldReader;
  private ReferenceDataReader<PropensityKey, PropensityState> propensityDataReader;
  private DeliverableSourceService deliverableSourceService;
  private String getCustomerAlternateID;
  private UploadedFileService uploadedFileService;
  private TargetService targetService;
  private CommunicationChannelService communicationChannelService;
  private CommunicationChannelBlackoutService communicationChannelBlackoutService;
  private LoyaltyProgramService loyaltyProgramService;
  private ExclusionInclusionTargetService exclusionInclusionTargetService;
  private ResellerService resellerService;
  private SegmentContactPolicyService segmentContactPolicyService;
  private SharedIDService subscriberGroupSharedIDService;
  private DynamicEventDeclarationsService dynamicEventDeclarationsService;
  private CriterionFieldAvailableValuesService criterionFieldAvailableValuesService;
  private static Method externalAPIMethodJourneyActivated;
  private static Method externalAPIMethodJourneyDeactivated;
  private ZookeeperUniqueKeyServer zuks;

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
    NGLMRuntime.initialize(true);
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

    String dynamicCriterionFieldTopic = Deployment.getDynamicCriterionFieldTopic();
    String journeyTopic = Deployment.getJourneyTopic();
    String journeyTemplateTopic = Deployment.getJourneyTemplateTopic();
    String segmentationDimensionTopic = Deployment.getSegmentationDimensionTopic();
    String pointTopic = Deployment.getPointTopic();
    String offerTopic = Deployment.getOfferTopic();
    String reportTopic = Deployment.getReportTopic();
    String paymentMeanTopic = Deployment.getPaymentMeanTopic();
    String presentationStrategyTopic = Deployment.getPresentationStrategyTopic();
    String dnboMatrixTopic = Deployment.getDNBOMatrixTopic();
    String scoringStrategyTopic = Deployment.getScoringStrategyTopic();
    String callingChannelTopic = Deployment.getCallingChannelTopic();
    String salesChannelTopic = Deployment.getSalesChannelTopic();
    String sourceAddresTopic = Deployment.getSourceAddressTopic();
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
    String voucherTypeTopic = Deployment.getVoucherTypeTopic();
    String voucherTopic = Deployment.getVoucherTopic();
    String subscriberMessageTemplateTopic = Deployment.getSubscriberMessageTemplateTopic();
    String subscriberGroupEpochTopic = Deployment.getSubscriberGroupEpochTopic();
    String journeyTrafficChangeLogTopic = Deployment.getJourneyTrafficChangeLogTopic();
    String renamedProfileCriterionFieldTopic = Deployment.getRenamedProfileCriterionFieldTopic();
    String deliverableSourceTopic = Deployment.getDeliverableSourceTopic();
    String redisServer = Deployment.getRedisSentinels();
    String subscriberProfileEndpoints = Deployment.getSubscriberProfileEndpoints();
    String uploadedFileTopic = Deployment.getUploadedFileTopic();
    String targetTopic = Deployment.getTargetTopic();
    String communicationChannelTopic = Deployment.getCommunicationChannelTopic();
    String communicationChannelBlackoutTopic = Deployment.getCommunicationChannelBlackoutTopic();
    String loyaltyProgramTopic = Deployment.getLoyaltyProgramTopic();
    String exclusionInclusionTargetTopic = Deployment.getExclusionInclusionTargetTopic();
    String resellerTopic = Deployment.getResellerTopic();
    String segmentContactPolicyTopic = Deployment.getSegmentContactPolicyTopic();
    String dynamicEventDeclarationsTopic = Deployment.getDynamicEventDeclarationsTopic();
    String criterionFieldAvailableValuesTopic = Deployment.getCriterionFieldAvailableValuesTopic();
    getCustomerAlternateID = Deployment.getGetCustomerAlternateID();

    //
    //  log
    //

    log.info("main START: {} {} {} {} {} {} {} {} {} {} {} {}", apiProcessKey, bootstrapServers, apiRestPort, elasticsearchServerHost, elasticsearchServerPort, nodeID, journeyTopic, segmentationDimensionTopic, offerTopic, presentationStrategyTopic, scoringStrategyTopic, subscriberGroupEpochTopic, subscriberMessageTemplateTopic);

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

    //
    //  externalAPIMethodJourneyActivated
    //

    try
      {
        externalAPIMethodJourneyActivated = (Deployment.getEvolutionEngineExternalAPIClass() != null) ? Deployment.getEvolutionEngineExternalAPIClass().getMethod("processDataJourneyActivated", Journey.class) : null;
        externalAPIMethodJourneyDeactivated = (Deployment.getEvolutionEngineExternalAPIClass() != null) ? Deployment.getEvolutionEngineExternalAPIClass().getMethod("processDataJourneyDeactivated", String.class, JourneyService.class) : null;
      }
    catch (NoSuchMethodException e)
      {
        throw new ServerRuntimeException(e);
      }
    
    //
    // ZookeeperUniqueKeyServer
    //
 
    zuks = new ZookeeperUniqueKeyServer("commoditydelivery");

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

    JourneyListener journeyListener = new JourneyListener()
    {
      @Override public void journeyActivated(Journey journey) {
          log.debug("journeyActivated: " + journey.getJourneyID()+" "+journey.getJourneyName());
          if (externalAPIMethodJourneyActivated != null)
            {
              try
              {
                Pair<String, JSONObject> result = (Pair<String,JSONObject>) externalAPIMethodJourneyActivated.invoke(null, journey);
                JSONObject json = result.getSecondElement();
                if (json != null)
                  {
                    String topicID = result.getFirstElement();
                    ExternalAPITopic apiTopic = Deployment.getExternalAPITopics().get(topicID);
                    if (apiTopic != null)
                      {
                        String topic = apiTopic.getName();
                        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(
                            topic,
                            new Serdes.StringSerde().serializer().serialize(topic, journey.getJourneyID()),
                            new Serdes.StringSerde().serializer().serialize(topic, json.toJSONString())));
                      }
                    else
                      {
                        log.info("journeyActivated: unknown topicID" + topicID);
                      }
                  }
              }
              catch (IllegalAccessException|InvocationTargetException e)
              {
                throw new RuntimeException(e);
              }
            }
        }
      @Override public void journeyDeactivated(String guiManagedObjectID)
      {
        log.debug("journeyDeactivated: " + guiManagedObjectID);
        if (externalAPIMethodJourneyDeactivated != null)
          {
            try
            {
              Pair<String, JSONObject> result = (Pair<String,JSONObject>) externalAPIMethodJourneyDeactivated.invoke(null, guiManagedObjectID, journeyService);
              JSONObject json = result.getSecondElement();
              if (json != null)
                {
                  String topicID = result.getFirstElement();
                  ExternalAPITopic apiTopic = Deployment.getExternalAPITopics().get(topicID);
                  if (apiTopic != null)
                    {
                      String topic = apiTopic.getName();
                      kafkaProducer.send(new ProducerRecord<byte[], byte[]>(
                          topic,
                          new Serdes.StringSerde().serializer().serialize(topic, guiManagedObjectID),
                          new Serdes.StringSerde().serializer().serialize(topic, json.toJSONString())));
                    }
                  else
                    {
                      log.info("journeyDeactivated: unknown topicID" + topicID);
                    }
                }
            }
            catch (IllegalAccessException|InvocationTargetException e)
            {
              throw new RuntimeException(e);
            }
          }
      }
    };
    dynamicCriterionFieldService = new DynamicCriterionFieldService(bootstrapServers, "guimanager-dynamiccriterionfieldservice-"+apiProcessKey, dynamicCriterionFieldTopic, true);
    CriterionContext.initialize(dynamicCriterionFieldService);
    journeyService = new JourneyService(bootstrapServers, "guimanager-journeyservice-" + apiProcessKey, journeyTopic, true, journeyListener);
    journeyTemplateService = new JourneyTemplateService(bootstrapServers, "guimanager-journeytemplateservice-" + apiProcessKey, journeyTemplateTopic, true);
    dynamicEventDeclarationsService = new DynamicEventDeclarationsService(bootstrapServers, "guimanager-dynamiceventdeclarationsservice-"+apiProcessKey, dynamicEventDeclarationsTopic, true);
    segmentationDimensionService = new SegmentationDimensionService(bootstrapServers, "guimanager-segmentationDimensionservice-" + apiProcessKey, segmentationDimensionTopic, true);
    pointService = new PointService(bootstrapServers, "guimanager-pointservice-" + apiProcessKey, pointTopic, true);
    offerService = new OfferService(bootstrapServers, "guimanager-offerservice-" + apiProcessKey, offerTopic, true);
    reportService = new ReportService(bootstrapServers, "guimanager-reportservice-" + apiProcessKey, reportTopic, true);
    paymentMeanService = new PaymentMeanService(bootstrapServers, "guimanager-paymentmeanservice-" + apiProcessKey, paymentMeanTopic, true);
    scoringStrategyService = new ScoringStrategyService(bootstrapServers, "guimanager-scoringstrategyservice-" + apiProcessKey, scoringStrategyTopic, true);
    presentationStrategyService = new PresentationStrategyService(bootstrapServers, "guimanager-presentationstrategyservice-" + apiProcessKey, presentationStrategyTopic, true);
    dnboMatrixService = new DNBOMatrixService(bootstrapServers, "guimanager-dnbomatrixservice-" + apiProcessKey, dnboMatrixTopic, true);
    callingChannelService = new CallingChannelService(bootstrapServers, "guimanager-callingchannelservice-" + apiProcessKey, callingChannelTopic, true);
    salesChannelService = new SalesChannelService(bootstrapServers, "guimanager-saleschannelservice-" + apiProcessKey, salesChannelTopic, true);
    sourceAddressService = new SourceAddressService(bootstrapServers, "guimanager-sourceaddressservice-" + apiProcessKey, sourceAddresTopic, true);
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
    voucherTypeService = new VoucherTypeService(bootstrapServers, "guimanager-vouchertypeservice-" + apiProcessKey, voucherTypeTopic, true);
    voucherService = new VoucherService(bootstrapServers, "guimanager-voucherservice-" + apiProcessKey, voucherTopic, true);
    subscriberMessageTemplateService = new SubscriberMessageTemplateService(bootstrapServers, "guimanager-subscribermessagetemplateservice-" + apiProcessKey, subscriberMessageTemplateTopic, true);
    subscriberProfileService = new EngineSubscriberProfileService(subscriberProfileEndpoints);
    subscriberIDService = new SubscriberIDService(redisServer, "guimanager-" + apiProcessKey);
    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("guimanager-subscribergroupepoch", apiProcessKey, bootstrapServers, subscriberGroupEpochTopic, SubscriberGroupEpoch::unpack);
    journeyTrafficReader = ReferenceDataReader.<String,JourneyTrafficHistory>startReader("guimanager-journeytrafficservice", apiProcessKey, bootstrapServers, journeyTrafficChangeLogTopic, JourneyTrafficHistory::unpack);
    renamedProfileCriterionFieldReader = ReferenceDataReader.<String,RenamedProfileCriterionField>startReader("guimanager-renamedprofilecriterionfield", apiProcessKey, bootstrapServers, renamedProfileCriterionFieldTopic, RenamedProfileCriterionField::unpack);
    propensityDataReader = ReferenceDataReader.<PropensityKey, PropensityState>startReader("guimanager-propensitystate", "guimanager-propensityreader-"+apiProcessKey, bootstrapServers, Deployment.getPropensityLogTopic(), PropensityState::unpack);
    deliverableSourceService = new DeliverableSourceService(bootstrapServers, "guimanager-deliverablesourceservice-" + apiProcessKey, deliverableSourceTopic);
    uploadedFileService = new UploadedFileService(bootstrapServers, "guimanager-uploadfileservice-" + apiProcessKey, uploadedFileTopic, true);
    targetService = new TargetService(bootstrapServers, "guimanager-targetservice-" + apiProcessKey, targetTopic, true);
    communicationChannelService = new CommunicationChannelService(bootstrapServers, "guimanager-communicationchannelservice-" + apiProcessKey, communicationChannelTopic, true);
    communicationChannelBlackoutService = new CommunicationChannelBlackoutService(bootstrapServers, "guimanager-blackoutservice-" + apiProcessKey, communicationChannelBlackoutTopic, true);
    loyaltyProgramService = new LoyaltyProgramService(bootstrapServers, "guimanager-loyaltyprogramservice-"+apiProcessKey, loyaltyProgramTopic, true);
    exclusionInclusionTargetService = new ExclusionInclusionTargetService(bootstrapServers, "guimanager-exclusioninclusiontargetservice-" + apiProcessKey, exclusionInclusionTargetTopic, true);
    resellerService = new ResellerService(bootstrapServers, "guimanager-resellerservice-"+apiProcessKey, resellerTopic, true);
    segmentContactPolicyService = new SegmentContactPolicyService(bootstrapServers, "guimanager-segmentcontactpolicyservice-"+apiProcessKey, segmentContactPolicyTopic, true);
    subscriberGroupSharedIDService = new SharedIDService(segmentationDimensionService, targetService, exclusionInclusionTargetService);
    criterionFieldAvailableValuesService = new CriterionFieldAvailableValuesService(bootstrapServers, "guimanager-criterionfieldavailablevaluesservice-"+apiProcessKey, criterionFieldAvailableValuesTopic, true);

    /*****************************************
    *
    *  Register Service Listener
    *
    *****************************************/
    GUIManagedObjectListener dynamicEventDeclarationsListener = new GUIManagedObjectListener() {

      @Override
      public void guiManagedObjectActivated(GUIManagedObject guiManagedObject)
      {
        dynamicEventDeclarationsService.refreshSegmentationChangeEvent(segmentationDimensionService);         
      }

      @Override
      public void guiManagedObjectDeactivated(String objectID)
      {
        dynamicEventDeclarationsService.refreshSegmentationChangeEvent(segmentationDimensionService);         
      }      
    };
    segmentationDimensionService.registerListener(dynamicEventDeclarationsListener);
    
    dynamicEventDeclarationsListener = new GUIManagedObjectListener() {

      @Override
      public void guiManagedObjectActivated(GUIManagedObject guiManagedObject)
      {
        dynamicEventDeclarationsService.refreshLoyaltyProgramChangeEvent(loyaltyProgramService);         
      }

      @Override
      public void guiManagedObjectDeactivated(String objectID)
      {
        dynamicEventDeclarationsService.refreshLoyaltyProgramChangeEvent(loyaltyProgramService);         
      }      
    };
    loyaltyProgramService.registerListener(dynamicEventDeclarationsListener);

    /*****************************************
    *
    *  elasticsearch -- client
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

    long providerEpoch = epochServer.getKey();
    for (DeliveryManagerAccount deliveryManagerAccount : Deployment.getDeliveryManagerAccounts().values())
      {
        /*****************************************
        *
        *  provider
        *
        *****************************************/

        String providerID = deliveryManagerAccount.getProviderID();
        DeliveryManagerDeclaration provider = Deployment.getFulfillmentProviders().get(providerID);
        if (provider == null)
          {
            throw new ServerRuntimeException("Delivery manager accounts : could not retrieve provider with ID " + providerID);
          }

        /*****************************************
        *
        *  accounts
        *
        *****************************************/

        Set<String> configuredDeliverableIDs = new HashSet<String>();
        Set<String> configuredPaymentMeanIDs = new HashSet<String>();
        for (Account account : deliveryManagerAccount.getAccounts())
          {
            /*****************************************
            *
            *  create/update deliverable 
            *
            *****************************************/

            if (account.getCreditable())
              {
                //
                //  find existing deliverable (by name) and use/generate deliverableID
                //

                GUIManagedObject existingDeliverable = deliverableService.getStoredDeliverableByName(account.getName());
                String deliverableID = (existingDeliverable != null) ? existingDeliverable.getGUIManagedObjectID() : deliverableService.generateDeliverableID();
                configuredDeliverableIDs.add(deliverableID);

                //
                //  deliverable
                //

                Deliverable deliverable = null;
                try
                  {
                    Map<String, Object> deliverableMap = new HashMap<String, Object>();
                    deliverableMap.put("id", deliverableID);
                    deliverableMap.put("fulfillmentProviderID", providerID);
                    deliverableMap.put("externalAccountID", account.getExternalAccountID());
                    deliverableMap.put("name", account.getName());
                    deliverableMap.put("display", account.getName());
                    deliverableMap.put("active", true);
                    deliverableMap.put("unitaryCost", 0);
                    deliverableMap.put("readOnly", true);
                    deliverableMap.put("generatedFromAccount", true);
                    deliverable = new Deliverable(JSONUtilities.encodeObject(deliverableMap), providerEpoch, null);
                  }
                catch (GUIManagerException e)
                  {
                    throw new ServerRuntimeException("could not add deliverable related to provider "+providerID+" (account "+account.getName()+")", e);
                  }

                //
                //  create/update deliverable if necessary)
                //

                if (existingDeliverable == null || ! Objects.equals(existingDeliverable, deliverable))
                  {
                    //
                    //  log
                    //

                    log.info("provider deliverable {} {}", deliverableID, (existingDeliverable == null) ? "create" : "update");

                    //
                    //  create/update deliverable
                    //

                    deliverableService.putDeliverable(deliverable, (existingDeliverable == null), "0");
                  }
              }

            /*****************************************
            *
            *  create/update paymentMean
            *
            *****************************************/

            if (account.getDebitable())
              {
                //
                //  find existing paymentMean (by name) and use/generate paymentMeanID
                //

                GUIManagedObject existingPaymentMean = paymentMeanService.getStoredPaymentMeanByName(account.getName());
                String paymentMeanID = (existingPaymentMean != null) ? existingPaymentMean.getGUIManagedObjectID() : paymentMeanService.generatePaymentMeanID();
                configuredPaymentMeanIDs.add(paymentMeanID);

                //
                //  paymentMean
                //

                PaymentMean paymentMean = null;
                try
                  {
                    Map<String, Object> paymentMeanMap = new HashMap<String, Object>();
                    paymentMeanMap.put("id", paymentMeanID);
                    paymentMeanMap.put("fulfillmentProviderID", providerID);
                    paymentMeanMap.put("externalAccountID", account.getExternalAccountID());
                    paymentMeanMap.put("name", account.getName());
                    paymentMeanMap.put("display", account.getName());
                    paymentMeanMap.put("active", true);
                    paymentMeanMap.put("readOnly", true);
                    paymentMeanMap.put("generatedFromAccount", true);
                    paymentMean = new PaymentMean(JSONUtilities.encodeObject(paymentMeanMap), providerEpoch, null);
                  }
                catch (GUIManagerException e)
                  {
                    throw new ServerRuntimeException("could not add paymentMean related to provider "+providerID+" (account "+account.getName()+")", e);
                  }

                // 
                //  create/update paymentMean (if ncessary)
                //

                if (existingPaymentMean == null || ! Objects.equals(existingPaymentMean, paymentMean))
                  {
                    //
                    //  log
                    //

                    log.info("provider paymentMean {} {}", paymentMeanID, (existingPaymentMean == null) ? "create" : "update");

                    //
                    //  create/update paymentMean
                    //

                    paymentMeanService.putPaymentMean(paymentMean, (existingPaymentMean == null), "0");
                  }
              }
          }

        /*****************************************
        *
        *  remove unused deliverables
        *
        *****************************************/

        for (Deliverable deliverable : deliverableService.getActiveDeliverables(SystemTime.getCurrentTime()))
          {
            if (Objects.equals(providerID, deliverable.getFulfillmentProviderID()) && deliverable.getGeneratedFromAccount() && ! configuredDeliverableIDs.contains(deliverable.getDeliverableID()))
              {
                deliverableService.removeDeliverable(deliverable.getDeliverableID(), "0");
                log.info("provider deliverable {} {}", deliverable.getDeliverableID(), "remove");
              }
          }
        
        /*****************************************
        *
        *  remove unused paymentMeans
        *
        *****************************************/

        for (PaymentMean paymentMean : paymentMeanService.getActivePaymentMeans(SystemTime.getCurrentTime()))
          {
            if (Objects.equals(providerID, paymentMean.getFulfillmentProviderID()) && paymentMean.getGeneratedFromAccount() && ! configuredPaymentMeanIDs.contains(paymentMean.getPaymentMeanID()))
              {
                paymentMeanService.removePaymentMean(paymentMean.getPaymentMeanID(), "0");
                log.info("provider paymentMean {} {}", paymentMean.getPaymentMeanID(), "remove");
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
    //  communicationChannels
    //

    try
      {
        JSONArray initialCommunicationChannelsJSONArray = Deployment.getInitialCommunicationChannelsJSONArray();
        for (int i=0; i<initialCommunicationChannelsJSONArray.size(); i++)
          {
            JSONObject communicationChannelJSON = (JSONObject) initialCommunicationChannelsJSONArray.get(i);
            processPutCommunicationChannel("0", communicationChannelJSON);
          }
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
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
    //  journeyTemplates
    //
    
    if (journeyTemplateService.getStoredJourneyTemplates().size() == 0)
      {
        try
        {
          JSONArray initialJourneyTemplatesJSONArray = Deployment.getInitialJourneyTemplatesJSONArray();
          for (int i=0; i<initialJourneyTemplatesJSONArray.size(); i++)
            {
              JSONObject journeyTemplateJSON = (JSONObject) initialJourneyTemplatesJSONArray.get(i);
              processPutJourneyTemplate("0", journeyTemplateJSON);
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

    //
    // remove all existing simple profile dimensions
    //

    for (GUIManagedObject dimensionObject : segmentationDimensionService.getStoredSegmentationDimensions())
      {
        if (dimensionObject instanceof SegmentationDimension)
          {
            SegmentationDimension dimension = (SegmentationDimension)dimensionObject;
            if (dimension.getIsSimpleProfileDimension())
              {
                segmentationDimensionService.removeSegmentationDimension(dimension.getSegmentationDimensionID(), "0");
              }
          }
      }

    //
    // re-create simple profile dimensions (=> so we are sure that dimensions are in line with profile fields)
    //

    Date now = SystemTime.getCurrentTime();
    Map<String,CriterionField> profileCriterionFields = CriterionContext.FullProfile.getCriterionFields();
    for (CriterionField criterion : profileCriterionFields.values())
      {
        if (Deployment.getGenerateSimpleProfileDimensions() || criterion.getGenerateDimension())
          {
            List<JSONObject> availableValues = evaluateAvailableValues(criterion, now, false);
            if (availableValues != null && availableValues.size() > 0)
              {
                //
                // create dimension
                //

                String dimensionID = "simple.subscriber." + criterion.getID();
                HashMap<String,Object> newSimpleProfileDimensionJSON = new HashMap<String,Object>();
                newSimpleProfileDimensionJSON.put("isSimpleProfileDimension", true);
                newSimpleProfileDimensionJSON.put("id", dimensionID);
                newSimpleProfileDimensionJSON.put("name", normalizeSegmentName(criterion.getName()));
                newSimpleProfileDimensionJSON.put("display", criterion.getDisplay());
                newSimpleProfileDimensionJSON.put("description", "Simple profile criteria (from "+criterion.getName()+")");
                newSimpleProfileDimensionJSON.put("targetingType", SegmentationDimensionTargetingType.ELIGIBILITY.getExternalRepresentation());
                newSimpleProfileDimensionJSON.put("active", Boolean.TRUE);
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
                          segmentJSON.put("name", normalizeSegmentName(stringValueDisplay));
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
                          segmentJSON.put("name", normalizeSegmentName(booleanValueDisplay));
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
                          segmentJSON.put("name", normalizeSegmentName(intValueDisplay));
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
              }
          }
      }

    /*****************************************
    *
    *  services - start
    *
    *****************************************/

    dynamicCriterionFieldService.start();
    journeyService.start();
    segmentationDimensionService.start();
    pointService.start();
    offerService.start();
    reportService.start();
    paymentMeanService.start();
    scoringStrategyService.start();
    presentationStrategyService.start();
    dnboMatrixService.start();
    callingChannelService.start();
    salesChannelService.start();
    sourceAddressService.start();
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
    voucherTypeService.start();
    voucherService.start();
    subscriberMessageTemplateService.start();
    subscriberProfileService.start();
    deliverableSourceService.start();
    uploadedFileService.start();
    targetService.start();
    communicationChannelService.start();
    communicationChannelBlackoutService.start();
    loyaltyProgramService.start();
    exclusionInclusionTargetService.start();
    resellerService.start();
    segmentContactPolicyService.start();
    dynamicEventDeclarationsService.start();
    dynamicEventDeclarationsService.refreshSegmentationChangeEvent(segmentationDimensionService);
    dynamicEventDeclarationsService.refreshLoyaltyProgramChangeEvent(loyaltyProgramService);
    criterionFieldAvailableValuesService.start();
    
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
        restServer.createContext("/nglm-guimanager/getSupportedRelationships", new APISimpleHandler(API.getSupportedRelationships));
        restServer.createContext("/nglm-guimanager/getServiceTypes", new APISimpleHandler(API.getServiceTypes));
        restServer.createContext("/nglm-guimanager/getCallingChannelProperties", new APISimpleHandler(API.getCallingChannelProperties));
        restServer.createContext("/nglm-guimanager/getCatalogCharacteristicUnits", new APISimpleHandler(API.getCatalogCharacteristicUnits));
        restServer.createContext("/nglm-guimanager/getSupportedDataTypes", new APISimpleHandler(API.getSupportedDataTypes));
        restServer.createContext("/nglm-guimanager/getSupportedEvents", new APISimpleHandler(API.getSupportedEvents));
        restServer.createContext("/nglm-guimanager/getLoyaltyProgramPointsEvents", new APISimpleHandler(API.getLoyaltyProgramPointsEvents));
        restServer.createContext("/nglm-guimanager/getSupportedTargetingTypes", new APISimpleHandler(API.getSupportedTargetingTypes));
        restServer.createContext("/nglm-guimanager/getProfileCriterionFields", new APISimpleHandler(API.getProfileCriterionFields));
        restServer.createContext("/nglm-guimanager/getProfileCriterionFieldIDs", new APISimpleHandler(API.getProfileCriterionFieldIDs));
        restServer.createContext("/nglm-guimanager/getProfileCriterionField", new APISimpleHandler(API.getProfileCriterionField));
        restServer.createContext("/nglm-guimanager/getFullProfileCriterionFields", new APISimpleHandler(API.getFullProfileCriterionFields));
        restServer.createContext("/nglm-guimanager/getFullProfileCriterionFieldIDs", new APISimpleHandler(API.getFullProfileCriterionFieldIDs));
        restServer.createContext("/nglm-guimanager/getFullProfileCriterionField", new APISimpleHandler(API.getFullProfileCriterionField));
        restServer.createContext("/nglm-guimanager/getPresentationCriterionFields", new APISimpleHandler(API.getPresentationCriterionFields));
        restServer.createContext("/nglm-guimanager/getPresentationCriterionFieldIDs", new APISimpleHandler(API.getPresentationCriterionFieldIDs));
        restServer.createContext("/nglm-guimanager/getPresentationCriterionField", new APISimpleHandler(API.getPresentationCriterionField));
        restServer.createContext("/nglm-guimanager/getJourneyCriterionFields", new APISimpleHandler(API.getJourneyCriterionFields));
        restServer.createContext("/nglm-guimanager/getJourneyCriterionFieldIDs", new APISimpleHandler(API.getJourneyCriterionFieldIDs));
        restServer.createContext("/nglm-guimanager/getJourneyCriterionField", new APISimpleHandler(API.getJourneyCriterionField));
        restServer.createContext("/nglm-guimanager/getOfferCategories", new APISimpleHandler(API.getOfferCategories));
        restServer.createContext("/nglm-guimanager/getOfferProperties", new APISimpleHandler(API.getOfferProperties));
        restServer.createContext("/nglm-guimanager/getScoringEngines", new APISimpleHandler(API.getScoringEngines));
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
        restServer.createContext("/nglm-guimanager/getWorkflowToolbox", new APISimpleHandler(API.getWorkflowToolbox));
        restServer.createContext("/nglm-guimanager/getWorkflowList", new APISimpleHandler(API.getWorkflowList));
        restServer.createContext("/nglm-guimanager/getWorkflowSummaryList", new APISimpleHandler(API.getWorkflowSummaryList));
        restServer.createContext("/nglm-guimanager/getWorkflow", new APISimpleHandler(API.getWorkflow));
        restServer.createContext("/nglm-guimanager/putWorkflow", new APISimpleHandler(API.putWorkflow));
        restServer.createContext("/nglm-guimanager/removeWorkflow", new APISimpleHandler(API.removeWorkflow));
        restServer.createContext("/nglm-guimanager/getBulkCampaignList", new APISimpleHandler(API.getBulkCampaignList));
        restServer.createContext("/nglm-guimanager/getBulkCampaignSummaryList", new APISimpleHandler(API.getBulkCampaignSummaryList));
        restServer.createContext("/nglm-guimanager/getBulkCampaign", new APISimpleHandler(API.getBulkCampaign));
        restServer.createContext("/nglm-guimanager/putBulkCampaign", new APISimpleHandler(API.putBulkCampaign));
        restServer.createContext("/nglm-guimanager/removeBulkCampaign", new APISimpleHandler(API.removeBulkCampaign));
        restServer.createContext("/nglm-guimanager/startBulkCampaign", new APISimpleHandler(API.startBulkCampaign));
        restServer.createContext("/nglm-guimanager/stopBulkCampaign", new APISimpleHandler(API.stopBulkCampaign));
        restServer.createContext("/nglm-guimanager/getJourneyTemplateList", new APISimpleHandler(API.getJourneyTemplateList));
        restServer.createContext("/nglm-guimanager/getJourneyTemplateSummaryList", new APISimpleHandler(API.getJourneyTemplateSummaryList));
        restServer.createContext("/nglm-guimanager/getJourneyTemplate", new APISimpleHandler(API.getJourneyTemplate));
        restServer.createContext("/nglm-guimanager/putJourneyTemplate", new APISimpleHandler(API.putJourneyTemplate));
        restServer.createContext("/nglm-guimanager/removeJourneyTemplate", new APISimpleHandler(API.removeJourneyTemplate));
        restServer.createContext("/nglm-guimanager/getJourneyNodeCount", new APISimpleHandler(API.getJourneyNodeCount));
        restServer.createContext("/nglm-guimanager/getSegmentationDimensionList", new APISimpleHandler(API.getSegmentationDimensionList));
        restServer.createContext("/nglm-guimanager/getSegmentationDimensionSummaryList", new APISimpleHandler(API.getSegmentationDimensionSummaryList));
        restServer.createContext("/nglm-guimanager/getSegmentationDimension", new APISimpleHandler(API.getSegmentationDimension));
        restServer.createContext("/nglm-guimanager/putSegmentationDimension", new APISimpleHandler(API.putSegmentationDimension));
        restServer.createContext("/nglm-guimanager/removeSegmentationDimension", new APISimpleHandler(API.removeSegmentationDimension));
        restServer.createContext("/nglm-guimanager/getCountBySegmentationRanges", new APISimpleHandler(API.getCountBySegmentationRanges));
        restServer.createContext("/nglm-guimanager/getCountBySegmentationEligibility", new APISimpleHandler(API.getCountBySegmentationEligibility));
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
        restServer.createContext("/nglm-guimanager/getDNBOMatrixList", new APISimpleHandler(API.getDNBOMatrixList));
        restServer.createContext("/nglm-guimanager/getDNBOMatrixSummaryList", new APISimpleHandler(API.getDNBOMatrixSummaryList));
        restServer.createContext("/nglm-guimanager/getDNBOMatrix", new APISimpleHandler(API.getDNBOMatrix));
        restServer.createContext("/nglm-guimanager/putDNBOMatrix", new APISimpleHandler(API.putDNBOMatrix));
        restServer.createContext("/nglm-guimanager/removeDNBOMatrix", new APISimpleHandler(API.removeDNBOMatrix));
        restServer.createContext("/nglm-guimanager/getScoringTypesList", new APISimpleHandler(API.getScoringTypesList));
        restServer.createContext("/nglm-guimanager/getDNBOMatrixVariablesList", new APISimpleHandler(API.getDNBOMatrixVariablesList));
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
        
        restServer.createContext("/nglm-guimanager/getVoucherTypeList", new APISimpleHandler(API.getVoucherTypeList));
        restServer.createContext("/nglm-guimanager/getVoucherTypeSummaryList", new APISimpleHandler(API.getVoucherTypeSummaryList));
        restServer.createContext("/nglm-guimanager/putVoucherType", new APISimpleHandler(API.putVoucherType));
        restServer.createContext("/nglm-guimanager/getVoucherType", new APISimpleHandler(API.getVoucherType));
        restServer.createContext("/nglm-guimanager/removeVoucherType", new APISimpleHandler(API.removeVoucherType));

        restServer.createContext("/nglm-guimanager/getVoucherCodeFormatList", new APISimpleHandler(API.getVoucherCodeFormatList));
        
        restServer.createContext("/nglm-guimanager/getVoucherList", new APISimpleHandler(API.getVoucherList));
        restServer.createContext("/nglm-guimanager/getVoucherSummaryList", new APISimpleHandler(API.getVoucherSummaryList));
        restServer.createContext("/nglm-guimanager/putVoucher", new APISimpleHandler(API.putVoucher));
        restServer.createContext("/nglm-guimanager/getVoucher", new APISimpleHandler(API.getVoucher));
        restServer.createContext("/nglm-guimanager/removeVoucher", new APISimpleHandler(API.removeVoucher));

        restServer.createContext("/nglm-guimanager/getMailTemplateList", new APISimpleHandler(API.getMailTemplateList));
        restServer.createContext("/nglm-guimanager/getFullMailTemplateList", new APISimpleHandler(API.getFullMailTemplateList));
        restServer.createContext("/nglm-guimanager/getMailTemplateSummaryList", new APISimpleHandler(API.getMailTemplateSummaryList));
        restServer.createContext("/nglm-guimanager/getMailTemplate", new APISimpleHandler(API.getMailTemplate));
        restServer.createContext("/nglm-guimanager/putMailTemplate", new APISimpleHandler(API.putMailTemplate));
        restServer.createContext("/nglm-guimanager/removeMailTemplate", new APISimpleHandler(API.removeMailTemplate));
        restServer.createContext("/nglm-guimanager/getSMSTemplateList", new APISimpleHandler(API.getSMSTemplateList));
        restServer.createContext("/nglm-guimanager/getFullSMSTemplateList", new APISimpleHandler(API.getFullSMSTemplateList));
        restServer.createContext("/nglm-guimanager/getSMSTemplateSummaryList", new APISimpleHandler(API.getSMSTemplateSummaryList));
        restServer.createContext("/nglm-guimanager/getSMSTemplate", new APISimpleHandler(API.getSMSTemplate));
        restServer.createContext("/nglm-guimanager/putSMSTemplate", new APISimpleHandler(API.putSMSTemplate));
        restServer.createContext("/nglm-guimanager/removeSMSTemplate", new APISimpleHandler(API.removeSMSTemplate));
        restServer.createContext("/nglm-guimanager/getPushTemplateList", new APISimpleHandler(API.getPushTemplateList));
        restServer.createContext("/nglm-guimanager/getPushTemplateSummaryList", new APISimpleHandler(API.getPushTemplateSummaryList));
        restServer.createContext("/nglm-guimanager/getPushTemplate", new APISimpleHandler(API.getPushTemplate));
        restServer.createContext("/nglm-guimanager/putPushTemplate", new APISimpleHandler(API.putPushTemplate));
        restServer.createContext("/nglm-guimanager/removePushTemplate", new APISimpleHandler(API.removePushTemplate));
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
        restServer.createContext("/nglm-guimanager/getCustomerPoints", new APISimpleHandler(API.getCustomerPoints));
        restServer.createContext("/nglm-guimanager/getCustomerLoyaltyPrograms", new APISimpleHandler(API.getCustomerLoyaltyPrograms));
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
        restServer.createContext("/nglm-guimanager/updateCustomerParent", new APISimpleHandler(API.updateCustomerParent));
        restServer.createContext("/nglm-guimanager/removeCustomerParent", new APISimpleHandler(API.removeCustomerParent));
        restServer.createContext("/nglm-guimanager/getCommunicationChannelList", new APISimpleHandler(API.getCommunicationChannelList));
        restServer.createContext("/nglm-guimanager/getCommunicationChannelSummaryList", new APISimpleHandler(API.getCommunicationChannelSummaryList));
        restServer.createContext("/nglm-guimanager/getCommunicationChannel", new APISimpleHandler(API.getCommunicationChannel));
        restServer.createContext("/nglm-guimanager/putCommunicationChannel", new APISimpleHandler(API.putCommunicationChannel));
        restServer.createContext("/nglm-guimanager/removeCommunicationChannel", new APISimpleHandler(API.removeCommunicationChannel));
        restServer.createContext("/nglm-guimanager/getBlackoutPeriodsList", new APISimpleHandler(API.getBlackoutPeriodsList));
        restServer.createContext("/nglm-guimanager/getBlackoutPeriodsSummaryList", new APISimpleHandler(API.getBlackoutPeriodsSummaryList));
        restServer.createContext("/nglm-guimanager/getBlackoutPeriods", new APISimpleHandler(API.getBlackoutPeriods));
        restServer.createContext("/nglm-guimanager/putBlackoutPeriods", new APISimpleHandler(API.putBlackoutPeriods));
        restServer.createContext("/nglm-guimanager/removeBlackoutPeriods", new APISimpleHandler(API.removeBlackoutPeriods));
        restServer.createContext("/nglm-guimanager/getLoyaltyProgramTypeList", new APISimpleHandler(API.getLoyaltyProgramTypeList));
        restServer.createContext("/nglm-guimanager/getLoyaltyProgramList", new APISimpleHandler(API.getLoyaltyProgramList));
        restServer.createContext("/nglm-guimanager/getLoyaltyProgramSummaryList", new APISimpleHandler(API.getLoyaltyProgramSummaryList));
        restServer.createContext("/nglm-guimanager/getLoyaltyProgram", new APISimpleHandler(API.getLoyaltyProgram));
        restServer.createContext("/nglm-guimanager/putLoyaltyProgram", new APISimpleHandler(API.putLoyaltyProgram));
        restServer.createContext("/nglm-guimanager/removeLoyaltyProgram", new APISimpleHandler(API.removeLoyaltyProgram));
        restServer.createContext("/nglm-guimanager/getResellerList", new APISimpleHandler(API.getResellerList));
        restServer.createContext("/nglm-guimanager/getResellerSummaryList", new APISimpleHandler(API.getResellerSummaryList));
        restServer.createContext("/nglm-guimanager/getReseller", new APISimpleHandler(API.getReseller));
        restServer.createContext("/nglm-guimanager/putReseller", new APISimpleHandler(API.putReseller));
        restServer.createContext("/nglm-guimanager/removeReseller", new APISimpleHandler(API.removeReseller));
        restServer.createContext("/nglm-guimanager/enterCampaign", new APISimpleHandler(API.enterCampaign));
        restServer.createContext("/nglm-guimanager/creditBonus", new APISimpleHandler(API.creditBonus));
        restServer.createContext("/nglm-guimanager/debitBonus", new APISimpleHandler(API.debitBonus));
        restServer.createContext("/nglm-guimanager/getExclusionInclusionTargetList", new APISimpleHandler(API.getExclusionInclusionTargetList));
        restServer.createContext("/nglm-guimanager/getExclusionInclusionTargetSummaryList", new APISimpleHandler(API.getExclusionInclusionTargetSummaryList));
        restServer.createContext("/nglm-guimanager/putExclusionInclusionTarget", new APISimpleHandler(API.putExclusionInclusionTarget));
        restServer.createContext("/nglm-guimanager/getExclusionInclusionTarget", new APISimpleHandler(API.getExclusionInclusionTarget));
        restServer.createContext("/nglm-guimanager/removeExclusionInclusionTarget", new APISimpleHandler(API.removeExclusionInclusionTarget));
        restServer.createContext("/nglm-guimanager/getSegmentContactPolicyList", new APISimpleHandler(API.getSegmentContactPolicyList));
        restServer.createContext("/nglm-guimanager/getSegmentContactPolicySummaryList", new APISimpleHandler(API.getSegmentContactPolicySummaryList));
        restServer.createContext("/nglm-guimanager/putSegmentContactPolicy", new APISimpleHandler(API.putSegmentContactPolicy));
        restServer.createContext("/nglm-guimanager/getSegmentContactPolicy", new APISimpleHandler(API.getSegmentContactPolicy));
        restServer.createContext("/nglm-guimanager/removeSegmentContactPolicy", new APISimpleHandler(API.removeSegmentContactPolicy));     
        restServer.createContext("/nglm-configadaptor/getSupportedLanguages", new APISimpleHandler(API.configAdaptorSupportedLanguages));
        restServer.createContext("/nglm-configadaptor/getSubscriberMessageTemplate", new APISimpleHandler(API.configAdaptorSubscriberMessageTemplate));
        restServer.createContext("/nglm-configadaptor/getOffer", new APISimpleHandler(API.configAdaptorOffer));
        restServer.createContext("/nglm-configadaptor/getProduct", new APISimpleHandler(API.configAdaptorProduct));
        restServer.createContext("/nglm-configadaptor/getPresentationStrategy", new APISimpleHandler(API.configAdaptorPresentationStrategy));
        restServer.createContext("/nglm-configadaptor/getScoringStrategy", new APISimpleHandler(API.configAdaptorScoringStrategy));
        restServer.createContext("/nglm-configadaptor/getCallingChannel", new APISimpleHandler(API.configAdaptorCallingChannel));
        restServer.createContext("/nglm-configadaptor/getSalesChannel", new APISimpleHandler(API.configAdaptorSalesChannel));
        restServer.createContext("/nglm-configadaptor/getCommunicationChannel", new APISimpleHandler(API.configAdaptorCommunicationChannel));
        restServer.createContext("/nglm-configadaptor/getBlackoutPeriods", new APISimpleHandler(API.configAdaptorBlackoutPeriods));
        restServer.createContext("/nglm-configadaptor/getContactPolicy", new APISimpleHandler(API.configAdaptorContactPolicy));
        restServer.createContext("/nglm-configadaptor/getSegmentationDimension", new APISimpleHandler(API.configAdaptorSegmentationDimension));
        restServer.createContext("/nglm-configadaptor/getCampaign", new APISimpleHandler(API.configAdaptorCampaign));
        restServer.createContext("/nglm-configadaptor/getJourneyObjective", new APISimpleHandler(API.configAdaptorJourneyObjective));
        restServer.createContext("/nglm-configadaptor/getProductType", new APISimpleHandler(API.configAdaptorProductType));
        restServer.createContext("/nglm-configadaptor/getOfferObjective", new APISimpleHandler(API.configAdaptorOfferObjective));
        restServer.createContext("/nglm-configadaptor/getScoringEngines", new APISimpleHandler(API.configAdaptorScoringEngines));
        restServer.createContext("/nglm-configadaptor/getPresentationCriterionFields", new APISimpleHandler(API.configAdaptorPresentationCriterionFields));
        restServer.createContext("/nglm-configadaptor/getDefaultNoftificationDailyWindows", new APISimpleHandler(API.configAdaptorDefaultNoftificationDailyWindows));
        restServer.createContext("/nglm-configadaptor/getDeliverable", new APISimpleHandler(API.configAdaptorDeliverable));
        restServer.createContext("/nglm-guimanager/getBillingModes", new APISimpleHandler(API.getBillingModes));
        restServer.createContext("/nglm-guimanager/getPartnerTypes", new APISimpleHandler(API.getPartnerTypes));
        restServer.createContext("/nglm-guimanager/getCriterionFieldAvailableValuesList", new APISimpleHandler(API.getCriterionFieldAvailableValuesList));
        restServer.createContext("/nglm-guimanager/getCriterionFieldAvailableValuesSummaryList", new APISimpleHandler(API.getCriterionFieldAvailableValuesSummaryList));
        restServer.createContext("/nglm-guimanager/getCriterionFieldAvailableValues", new APISimpleHandler(API.getCriterionFieldAvailableValues));
        restServer.createContext("/nglm-guimanager/putCriterionFieldAvailableValues", new APISimpleHandler(API.putCriterionFieldAvailableValues));
        restServer.createContext("/nglm-guimanager/removeCriterionFieldAvailableValues", new APISimpleHandler(API.removeCriterionFieldAvailableValues));
        restServer.createContext("/nglm-guimanager/getEffectiveSystemTime", new APISimpleHandler(API.getEffectiveSystemTime));
        restServer.createContext("/nglm-guimanager/getCustomerNBOs", new APISimpleHandler(API.getCustomerNBOs));
        restServer.createContext("/nglm-guimanager/getTokensCodesList", new APISimpleHandler(API.getTokensCodesList));
        restServer.createContext("/nglm-guimanager/acceptOffer", new APISimpleHandler(API.acceptOffer));
        restServer.createContext("/nglm-guimanager/getTokenEventDetails", new APISimpleHandler(API.getTokenEventDetails));
        restServer.createContext("/nglm-guimanager/getTenantList", new APISimpleHandler(API.getTenantList));
        
        restServer.createContext("/nglm-guimanager/getSourceAddressList", new APISimpleHandler(API.getSourceAddressList));
        restServer.createContext("/nglm-guimanager/getSourceAddressSummaryList", new APISimpleHandler(API.getSourceAddressSummaryList));
        restServer.createContext("/nglm-guimanager/getSourceAddress", new APISimpleHandler(API.getSourceAddress));
        restServer.createContext("/nglm-guimanager/putSourceAddress", new APISimpleHandler(API.putSourceAddress));
        restServer.createContext("/nglm-guimanager/removeSourceAddress", new APISimpleHandler(API.removeSourceAddress));
        
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

    guiManagerContext = new GUIManagerContext(journeyService, segmentationDimensionService, pointService, offerService, reportService, paymentMeanService, scoringStrategyService, presentationStrategyService, callingChannelService, salesChannelService, sourceAddressService, supplierService, productService, catalogCharacteristicService, contactPolicyService, journeyObjectiveService, offerObjectiveService, productTypeService, ucgRuleService, deliverableService, tokenTypeService, voucherTypeService, voucherService, subscriberMessageTemplateService, subscriberProfileService, subscriberIDService, deliverableSourceService, uploadedFileService, targetService, communicationChannelService, communicationChannelBlackoutService, loyaltyProgramService, resellerService, exclusionInclusionTargetService, segmentContactPolicyService, criterionFieldAvailableValuesService);

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/

    NGLMRuntime.addShutdownHook(new ShutdownHook(kafkaProducer, restServer, dynamicCriterionFieldService, journeyService, segmentationDimensionService, pointService, offerService, scoringStrategyService, presentationStrategyService, callingChannelService, salesChannelService, sourceAddressService, supplierService, productService, catalogCharacteristicService, contactPolicyService, journeyObjectiveService, offerObjectiveService, productTypeService, ucgRuleService, deliverableService, tokenTypeService, voucherTypeService, voucherService, subscriberProfileService, subscriberIDService, subscriberGroupEpochReader, journeyTrafficReader, renamedProfileCriterionFieldReader, deliverableSourceService, reportService, subscriberMessageTemplateService, uploadedFileService, targetService, communicationChannelService, communicationChannelBlackoutService, loyaltyProgramService, resellerService, exclusionInclusionTargetService, dnboMatrixService, segmentContactPolicyService, criterionFieldAvailableValuesService));

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
    private DynamicCriterionFieldService dynamicCriterionFieldService;
    private JourneyService journeyService;
    private SegmentationDimensionService segmentationDimensionService;
    private DNBOMatrixService dnboMatrixService;
    private PointService pointService;
    private OfferService offerService;
    private ReportService reportService;
    private ScoringStrategyService scoringStrategyService;
    private PresentationStrategyService presentationStrategyService;
    private CallingChannelService callingChannelService;
    private SalesChannelService salesChannelService;
    private SourceAddressService sourceAddressService;
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
    private VoucherTypeService voucherTypeService;
    private VoucherService voucherService;
    private SubscriberMessageTemplateService subscriberMessageTemplateService;
    private SubscriberProfileService subscriberProfileService;
    private SubscriberIDService subscriberIDService;
    private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
    private ReferenceDataReader<String,JourneyTrafficHistory> journeyTrafficReader;
    private ReferenceDataReader<String,RenamedProfileCriterionField> renamedProfileCriterionFieldReader;
    private DeliverableSourceService deliverableSourceService;
    private UploadedFileService uploadedFileService;
    private TargetService targetService;
    private CommunicationChannelService communicationChannelService;
    private CommunicationChannelBlackoutService communicationChannelBlackoutService;
    private LoyaltyProgramService loyaltyProgramService;
    private ExclusionInclusionTargetService exclusionInclusionTargetService;
    private ResellerService resellerService;
    private SegmentContactPolicyService segmentContactPolicyService;
    private CriterionFieldAvailableValuesService criterionFieldAvailableValuesService;

    //
    //  constructor
    //
    
    private ShutdownHook(KafkaProducer<byte[], byte[]> kafkaProducer, HttpServer restServer, DynamicCriterionFieldService dynamicCriterionFieldService, JourneyService journeyService, SegmentationDimensionService segmentationDimensionService, PointService pointService, OfferService offerService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, CallingChannelService callingChannelService, SalesChannelService salesChannelService, SourceAddressService sourceAddressService, SupplierService supplierService, ProductService productService, CatalogCharacteristicService catalogCharacteristicService, ContactPolicyService contactPolicyService, JourneyObjectiveService journeyObjectiveService, OfferObjectiveService offerObjectiveService, ProductTypeService productTypeService, UCGRuleService ucgRuleService, DeliverableService deliverableService, TokenTypeService tokenTypeService, VoucherTypeService voucherTypeService, VoucherService voucherService, SubscriberProfileService subscriberProfileService, SubscriberIDService subscriberIDService, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,JourneyTrafficHistory> journeyTrafficReader, ReferenceDataReader<String,RenamedProfileCriterionField> renamedProfileCriterionFieldReader, DeliverableSourceService deliverableSourceService, ReportService reportService, SubscriberMessageTemplateService subscriberMessageTemplateService, UploadedFileService uploadedFileService, TargetService targetService, CommunicationChannelService communicationChannelService, CommunicationChannelBlackoutService communicationChannelBlackoutService, LoyaltyProgramService loyaltyProgramService, ResellerService resellerService, ExclusionInclusionTargetService exclusionInclusionTargetService, DNBOMatrixService dnboMatrixService, SegmentContactPolicyService segmentContactPolicyService, CriterionFieldAvailableValuesService criterionFieldAvailableValuesService)
    {
      this.kafkaProducer = kafkaProducer;
      this.restServer = restServer;
      this.dynamicCriterionFieldService = dynamicCriterionFieldService;
      this.journeyService = journeyService;
      this.segmentationDimensionService = segmentationDimensionService;
      this.pointService = pointService;
      this.offerService = offerService;
      this.reportService = reportService;
      this.scoringStrategyService = scoringStrategyService;
      this.presentationStrategyService = presentationStrategyService;
      this.callingChannelService = callingChannelService;
      this.salesChannelService = salesChannelService;
      this.sourceAddressService = sourceAddressService;
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
      this.voucherTypeService = voucherTypeService;
      this.voucherService = voucherService;
      this.subscriberMessageTemplateService = subscriberMessageTemplateService;
      this.subscriberProfileService = subscriberProfileService;
      this.subscriberIDService = subscriberIDService;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
      this.journeyTrafficReader = journeyTrafficReader;
      this.renamedProfileCriterionFieldReader = renamedProfileCriterionFieldReader;
      this.deliverableSourceService = deliverableSourceService;
      this.uploadedFileService = uploadedFileService;
      this.targetService = targetService;
      this.communicationChannelService = communicationChannelService;
      this.communicationChannelBlackoutService = communicationChannelBlackoutService;
      this.loyaltyProgramService = loyaltyProgramService;
      this.exclusionInclusionTargetService = exclusionInclusionTargetService;
      this.resellerService = resellerService;
      this.dnboMatrixService = dnboMatrixService;
      this.segmentContactPolicyService = segmentContactPolicyService;
      this.criterionFieldAvailableValuesService = criterionFieldAvailableValuesService;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      //
      //  reference data readers
      //

      if (subscriberGroupEpochReader != null) subscriberGroupEpochReader.close();
      if (journeyTrafficReader != null) journeyTrafficReader.close();
      if (renamedProfileCriterionFieldReader != null) renamedProfileCriterionFieldReader.close();
      //
      //  services 
      //

      if (dynamicCriterionFieldService != null) dynamicCriterionFieldService.stop();
      if (journeyService != null) journeyService.stop();
      if (segmentationDimensionService != null) segmentationDimensionService.stop();
      if (pointService != null) pointService.stop();
      if (offerService != null) offerService.stop();
      if (reportService != null) reportService.stop();
      if (scoringStrategyService != null) scoringStrategyService.stop();
      if (presentationStrategyService != null) presentationStrategyService.stop();
      if (callingChannelService != null) callingChannelService.stop();
      if (salesChannelService != null) salesChannelService.stop();
      if (sourceAddressService != null) sourceAddressService.stop();
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
      if (voucherTypeService != null) voucherTypeService.stop();
      if (voucherService != null) voucherService.stop();
      if (subscriberMessageTemplateService != null) subscriberMessageTemplateService.stop();
      if (subscriberProfileService != null) subscriberProfileService.stop();
      if (subscriberIDService != null) subscriberIDService.stop();
      if (deliverableSourceService != null) deliverableSourceService.stop();
      if (uploadedFileService != null) uploadedFileService.stop();
      if (targetService != null) targetService.stop();
      if (communicationChannelService != null) communicationChannelService.stop();
      if (communicationChannelBlackoutService != null) communicationChannelBlackoutService.stop();
      if (loyaltyProgramService != null) loyaltyProgramService.stop();
      if (exclusionInclusionTargetService != null) exclusionInclusionTargetService.stop();
      if (resellerService != null) resellerService.stop();
      if (dnboMatrixService != null) dnboMatrixService.stop();
      if (segmentContactPolicyService != null) segmentContactPolicyService.stop();
      if (criterionFieldAvailableValuesService != null) criterionFieldAvailableValuesService.stop();

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
        *  userID
        *
        *****************************************/

        String jsonUserID = JSONUtilities.decodeString(jsonRoot, "userID", false);
        if (jsonUserID == null && userID != null)
          {
            jsonRoot.put("userID", userID);
          }

        /*****************************************
        *
        *  includeArchived
        *
        *****************************************/

        boolean includeArchived = JSONUtilities.decodeBoolean(jsonRoot, "includeArchived", Boolean.FALSE);

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
                  jsonResponse = processGetStaticConfiguration(userID, jsonRoot, includeArchived);
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

                case getSupportedRelationships:
                  jsonResponse = processGetSupportedRelationships(userID, jsonRoot);
                  break;

                case getServiceTypes:
                  jsonResponse = processGetServiceTypes(userID, jsonRoot);
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

                case getLoyaltyProgramPointsEvents:
                  jsonResponse = processGetLoyaltyProgramPointsEvents(userID, jsonRoot);
                  break;

                case getSupportedTargetingTypes:
                  jsonResponse = processGetSupportedTargetingTypes(userID, jsonRoot);
                  break;

                case getProfileCriterionFields:
                  jsonResponse = processGetProfileCriterionFields(userID, jsonRoot, getIncludeDynamicParameter(jsonRoot) ? CriterionContext.DynamicProfile : CriterionContext.Profile);
                  break;

                case getProfileCriterionFieldIDs:
                  jsonResponse = processGetProfileCriterionFieldIDs(userID, jsonRoot, getIncludeDynamicParameter(jsonRoot) ? CriterionContext.DynamicProfile : CriterionContext.Profile);
                  break;

                case getProfileCriterionField:
                  jsonResponse = processGetProfileCriterionField(userID, jsonRoot, getIncludeDynamicParameter(jsonRoot) ? CriterionContext.DynamicProfile : CriterionContext.Profile);
                  break;

                case getFullProfileCriterionFields:
                  jsonResponse = processGetProfileCriterionFields(userID, jsonRoot, getIncludeDynamicParameter(jsonRoot) ? CriterionContext.FullDynamicProfile : CriterionContext.FullProfile);
                  break;

                case getFullProfileCriterionFieldIDs:
                  jsonResponse = processGetProfileCriterionFieldIDs(userID, jsonRoot, getIncludeDynamicParameter(jsonRoot) ? CriterionContext.FullDynamicProfile : CriterionContext.FullProfile);
                  break;

                case getFullProfileCriterionField:
                  jsonResponse = processGetProfileCriterionField(userID, jsonRoot, getIncludeDynamicParameter(jsonRoot) ? CriterionContext.FullDynamicProfile : CriterionContext.FullProfile);
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

                case getOfferProperties:
                  jsonResponse = processGetOfferProperties(userID, jsonRoot);
                  break;

                case getScoringEngines:
                  jsonResponse = processGetScoringEngines(userID, jsonRoot);
                  break;

                case getOfferOptimizationAlgorithms:
                  jsonResponse = processGetOfferOptimizationAlgorithms(userID, jsonRoot, includeArchived);
                  break;

                case getNodeTypes:
                  jsonResponse = processGetNodeTypes(userID, jsonRoot);
                  break;

                case getJourneyToolbox:
                  jsonResponse = processGetJourneyToolbox(userID, jsonRoot);
                  break;

                case getJourneyList:
                  jsonResponse = processGetJourneyList(userID, jsonRoot, GUIManagedObjectType.Journey, true, true, includeArchived);
                  break;

                case getJourneySummaryList:
                  jsonResponse = processGetJourneyList(userID, jsonRoot, GUIManagedObjectType.Journey, false, true, includeArchived);
                  break;

                case getJourney:
                  jsonResponse = processGetJourney(userID, jsonRoot, GUIManagedObjectType.Journey, true, includeArchived);
                  break;

                case putJourney:
                  jsonResponse = processPutJourney(userID, jsonRoot, GUIManagedObjectType.Journey);
                  break;

                case removeJourney:
                  jsonResponse = processRemoveJourney(userID, jsonRoot, GUIManagedObjectType.Journey);
                  break;

                case startJourney:
                  jsonResponse = processSetActive(userID, jsonRoot, GUIManagedObjectType.Journey, true);
                  break;

                case stopJourney:
                  jsonResponse = processSetActive(userID, jsonRoot, GUIManagedObjectType.Journey, false);
                  break;

                case getCampaignToolbox:
                  jsonResponse = processGetCampaignToolbox(userID, jsonRoot);
                  break;

                case getCampaignList:
                  jsonResponse = processGetJourneyList(userID, jsonRoot, GUIManagedObjectType.Campaign, true, true, includeArchived);
                  break;

                case getCampaignSummaryList:
                  jsonResponse = processGetJourneyList(userID, jsonRoot, GUIManagedObjectType.Campaign, false, true, includeArchived);
                  break;

                case getCampaign:
                  jsonResponse = processGetJourney(userID, jsonRoot, GUIManagedObjectType.Campaign, true, includeArchived);
                  break;

                case putCampaign:
                  jsonResponse = processPutJourney(userID, jsonRoot, GUIManagedObjectType.Campaign);
                  break;

                case removeCampaign:
                  jsonResponse = processRemoveJourney(userID, jsonRoot, GUIManagedObjectType.Campaign);
                  break;

                case startCampaign:
                  jsonResponse = processSetActive(userID, jsonRoot, GUIManagedObjectType.Campaign, true);
                  break;

                case stopCampaign:
                  jsonResponse = processSetActive(userID, jsonRoot, GUIManagedObjectType.Campaign, false);
                  break;

                case getWorkflowToolbox:
                  jsonResponse = processGetWorkflowToolbox(userID, jsonRoot);
                  break;

                case getWorkflowList:
                  jsonResponse = processGetJourneyList(userID, jsonRoot, GUIManagedObjectType.Workflow, true, true, includeArchived);
                  break;

                case getFullWorkflowList:
                  jsonResponse = processGetJourneyList(userID, jsonRoot, GUIManagedObjectType.Workflow, true, false, includeArchived);
                  break;

                case getWorkflowSummaryList:
                  jsonResponse = processGetJourneyList(userID, jsonRoot, GUIManagedObjectType.Workflow, false, true, includeArchived);
                  break;

                case getWorkflow:
                  jsonResponse = processGetJourney(userID, jsonRoot, GUIManagedObjectType.Workflow, true, includeArchived);
                  break;

                case putWorkflow:
                  jsonResponse = processPutJourney(userID, jsonRoot, GUIManagedObjectType.Workflow);
                  break;

                case removeWorkflow:
                  jsonResponse = processRemoveJourney(userID, jsonRoot, GUIManagedObjectType.Workflow);
                  break;

                case getBulkCampaignList:
                  jsonResponse = processGetJourneyList(userID, jsonRoot, GUIManagedObjectType.BulkCampaign, true, true, includeArchived);
                  break;

                case getBulkCampaignSummaryList:
                  jsonResponse = processGetJourneyList(userID, jsonRoot, GUIManagedObjectType.BulkCampaign, false, true, includeArchived);
                  break;

                case getBulkCampaign:
                  jsonResponse = processGetJourney(userID, jsonRoot, GUIManagedObjectType.BulkCampaign, true, includeArchived);
                  break;

                case putBulkCampaign:
                  jsonResponse = processPutBulkCampaign(userID, jsonRoot);
                  break;

                case removeBulkCampaign:
                  jsonResponse = processRemoveJourney(userID, jsonRoot, GUIManagedObjectType.BulkCampaign);
                  break;

                case startBulkCampaign:
                  jsonResponse = processSetActive(userID, jsonRoot, GUIManagedObjectType.BulkCampaign, true);
                  break;

                case stopBulkCampaign:
                  jsonResponse = processSetActive(userID, jsonRoot, GUIManagedObjectType.BulkCampaign, false);
                  break;

                case getJourneyTemplateList:
                  jsonResponse = processGetJourneyTemplateList(userID, jsonRoot, true, includeArchived);
                  break;

                case getJourneyTemplateSummaryList:
                  jsonResponse = processGetJourneyTemplateList(userID, jsonRoot, false, includeArchived);
                  break;

                case getJourneyTemplate:
                  jsonResponse = processGetJourneyTemplate(userID, jsonRoot, includeArchived);
                  break;

                case putJourneyTemplate:
                  jsonResponse = processPutJourneyTemplate(userID, jsonRoot);
                  break;

                case removeJourneyTemplate:
                  jsonResponse = processRemoveJourneyTemplate(userID, jsonRoot);
                  break;

                case getJourneyNodeCount:
                  jsonResponse = processGetJourneyNodeCount(userID, jsonRoot);
                  break;
                  
                case getSegmentationDimensionList:
                  jsonResponse = processGetSegmentationDimensionList(userID, jsonRoot, true, includeArchived);
                  break;

                case getSegmentationDimensionSummaryList:
                  jsonResponse = processGetSegmentationDimensionList(userID, jsonRoot, false, includeArchived);
                  break;

                case getSegmentationDimension:
                  jsonResponse = processGetSegmentationDimension(userID, jsonRoot, includeArchived);
                  break;

                case putSegmentationDimension:
                  jsonResponse = processPutSegmentationDimension(userID, jsonRoot);
                  break;

                case removeSegmentationDimension:
                  jsonResponse = processRemoveSegmentationDimension(userID, jsonRoot);
                  break;

                case getCountBySegmentationRanges:
                  jsonResponse = processGetCountBySegmentationRanges(userID, jsonRoot);
                  break;

                case getCountBySegmentationEligibility:
                  jsonResponse = processGetCountBySegmentationEligibility(userID, jsonRoot);
                  break;

                case evaluateProfileCriteria:
                  jsonResponse = processEvaluateProfileCriteria(userID, jsonRoot);
                  break;

                case getUCGDimensionSummaryList:
                  jsonResponse = processGetUCGDimensionList(userID, jsonRoot, false, includeArchived);
                  break;

                case getPointList:
                  jsonResponse = processGetPointList(userID, jsonRoot, true, includeArchived);
                  break;

                case getPointSummaryList:
                  jsonResponse = processGetPointList(userID, jsonRoot, false, includeArchived);
                  break;

                case getPoint:
                  jsonResponse = processGetPoint(userID, jsonRoot, includeArchived);
                  break;

                case putPoint:
                  jsonResponse = processPutPoint(userID, jsonRoot);
                  break;

                case removePoint:
                  jsonResponse = processRemovePoint(userID, jsonRoot);
                  break;

                case getOfferList:
                  jsonResponse = processGetOfferList(userID, jsonRoot, true, includeArchived);
                  break;

                case getOfferSummaryList:
                  jsonResponse = processGetOfferList(userID, jsonRoot, false, includeArchived);
                  break;

                case getOffer:
                  jsonResponse = processGetOffer(userID, jsonRoot, includeArchived);
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
                  jsonResponse = processGetReportList(userID, jsonRoot, includeArchived);
                  break;

                case putReport:
                  jsonResponse = processPutReport(userID, jsonRoot);
                  break;

                case launchReport:
                  jsonResponse = processLaunchReport(userID, jsonRoot);
                  break;

                case getPresentationStrategyList:
                  jsonResponse = processGetPresentationStrategyList(userID, jsonRoot, true, includeArchived);
                  break;

                case getPresentationStrategySummaryList:
                  jsonResponse = processGetPresentationStrategyList(userID, jsonRoot, false, includeArchived);
                  break;

                case getPresentationStrategy:
                  jsonResponse = processGetPresentationStrategy(userID, jsonRoot, includeArchived);
                  break;

                case putPresentationStrategy:
                  jsonResponse = processPutPresentationStrategy(userID, jsonRoot);
                  break;

                case removePresentationStrategy:
                  jsonResponse = processRemovePresentationStrategy(userID, jsonRoot);
                  break;

                case getDNBOMatrixList:
                  jsonResponse = processGetDNBOMatrixList(userID, jsonRoot, true, includeArchived);
                  break;

                case getDNBOMatrixSummaryList:
                  jsonResponse = processGetDNBOMatrixList(userID, jsonRoot, false, includeArchived);
                  break;

                case getDNBOMatrix:
                  jsonResponse = processGetDNBOMatrix(userID, jsonRoot, includeArchived);
                  break;

                case putDNBOMatrix:
                  jsonResponse = processPutDNBOMatrix(userID, jsonRoot);
                  break;

                case removeDNBOMatrix:
                  jsonResponse = processRemoveDNBOMatrix(userID, jsonRoot);
                  break;
                  
                case getScoringTypesList:
                  jsonResponse = processGetScoringTypesList(userID, jsonRoot);
                  break;
                  
                case getDNBOMatrixVariablesList:
                  jsonResponse = processGetDNBOMatrixVariablesList(userID, jsonRoot);
                  break;
                  
                case getScoringStrategyList:
                  jsonResponse = processGetScoringStrategyList(userID, jsonRoot, true, includeArchived);
                  break;

                case getScoringStrategySummaryList:
                  jsonResponse = processGetScoringStrategyList(userID, jsonRoot, false, includeArchived);
                  break;

                case getScoringStrategy:
                  jsonResponse = processGetScoringStrategy(userID, jsonRoot, includeArchived);
                  break;

                case putScoringStrategy:
                  jsonResponse = processPutScoringStrategy(userID, jsonRoot);
                  break;

                case removeScoringStrategy:
                  jsonResponse = processRemoveScoringStrategy(userID, jsonRoot);
                  break;

                case getCallingChannelList:
                  jsonResponse = processGetCallingChannelList(userID, jsonRoot, true, includeArchived);
                  break;

                case getCallingChannelSummaryList:
                  jsonResponse = processGetCallingChannelList(userID, jsonRoot, false, includeArchived);
                  break;

                case getCallingChannel:
                  jsonResponse = processGetCallingChannel(userID, jsonRoot, includeArchived);
                  break;

                case putCallingChannel:
                  jsonResponse = processPutCallingChannel(userID, jsonRoot);
                  break;

                case removeCallingChannel:
                  jsonResponse = processRemoveCallingChannel(userID, jsonRoot);
                  break;

                case getSalesChannelList:
                  jsonResponse = processGetSalesChannelList(userID, jsonRoot, true, includeArchived);
                  break;

                case getSalesChannelSummaryList:
                  jsonResponse = processGetSalesChannelList(userID, jsonRoot, false, includeArchived);
                  break;

                case getSalesChannel:
                  jsonResponse = processGetSalesChannel(userID, jsonRoot, includeArchived);
                  break;

                case putSalesChannel:
                  jsonResponse = processPutSalesChannel(userID, jsonRoot);
                  break;

                case removeSalesChannel:
                  jsonResponse = processRemoveSalesChannel(userID, jsonRoot);
                  break;

                case getSupplierList:
                  jsonResponse = processGetSupplierList(userID, jsonRoot, true, includeArchived);
                  break;

                case getSupplierSummaryList:
                  jsonResponse = processGetSupplierList(userID, jsonRoot, false, includeArchived);
                  break;

                case getSupplier:
                  jsonResponse = processGetSupplier(userID, jsonRoot, includeArchived);
                  break;

                case putSupplier:
                  jsonResponse = processPutSupplier(userID, jsonRoot);
                  break;

                case removeSupplier:
                  jsonResponse = processRemoveSupplier(userID, jsonRoot);
                  break;

                case getProductList:
                  jsonResponse = processGetProductList(userID, jsonRoot, true, includeArchived);
                  break;

                case getProductSummaryList:
                  jsonResponse = processGetProductList(userID, jsonRoot, false, includeArchived);
                  break;

                case getProduct:
                  jsonResponse = processGetProduct(userID, jsonRoot, includeArchived);
                  break;

                case putProduct:
                  jsonResponse = processPutProduct(userID, jsonRoot);
                  break;

                case removeProduct:
                  jsonResponse = processRemoveProduct(userID, jsonRoot);
                  break;

                case getCatalogCharacteristicList:
                  jsonResponse = processGetCatalogCharacteristicList(userID, jsonRoot, true, includeArchived);
                  break;

                case getCatalogCharacteristicSummaryList:
                  jsonResponse = processGetCatalogCharacteristicList(userID, jsonRoot, false, includeArchived);
                  break;

                case getCatalogCharacteristic:
                  jsonResponse = processGetCatalogCharacteristic(userID, jsonRoot, includeArchived);
                  break;

                case putCatalogCharacteristic:
                  jsonResponse = processPutCatalogCharacteristic(userID, jsonRoot);
                  break;

                case removeCatalogCharacteristic:
                  jsonResponse = processRemoveCatalogCharacteristic(userID, jsonRoot);
                  break;

                case getContactPolicyList:
                  jsonResponse = processGetContactPolicyList(userID, jsonRoot, true, includeArchived);
                  break;

                case getContactPolicySummaryList:
                  jsonResponse = processGetContactPolicyList(userID, jsonRoot, false, includeArchived);
                  break;

                case getContactPolicy:
                  jsonResponse = processGetContactPolicy(userID, jsonRoot, includeArchived);
                  break;

                case putContactPolicy:
                  jsonResponse = processPutContactPolicy(userID, jsonRoot);
                  break;

                case removeContactPolicy:
                  jsonResponse = processRemoveContactPolicy(userID, jsonRoot);
                  break;

                case getJourneyObjectiveList:
                  jsonResponse = processGetJourneyObjectiveList(userID, jsonRoot, true, includeArchived);
                  break;

                case getJourneyObjectiveSummaryList:
                  jsonResponse = processGetJourneyObjectiveList(userID, jsonRoot, false, includeArchived);
                  break;

                case getJourneyObjective:
                  jsonResponse = processGetJourneyObjective(userID, jsonRoot, includeArchived);
                  break;

                case putJourneyObjective:
                  jsonResponse = processPutJourneyObjective(userID, jsonRoot);
                  break;

                case removeJourneyObjective:
                  jsonResponse = processRemoveJourneyObjective(userID, jsonRoot);
                  break;

                case getOfferObjectiveList:
                  jsonResponse = processGetOfferObjectiveList(userID, jsonRoot, true, includeArchived);
                  break;

                case getOfferObjectiveSummaryList:
                  jsonResponse = processGetOfferObjectiveList(userID, jsonRoot, false, includeArchived);
                  break;

                case getOfferObjective:
                  jsonResponse = processGetOfferObjective(userID, jsonRoot, includeArchived);
                  break;

                case putOfferObjective:
                  jsonResponse = processPutOfferObjective(userID, jsonRoot);
                  break;

                case removeOfferObjective:
                  jsonResponse = processRemoveOfferObjective(userID, jsonRoot);
                  break;

                case getProductTypeList:
                  jsonResponse = processGetProductTypeList(userID, jsonRoot, true, includeArchived);
                  break;

                case getProductTypeSummaryList:
                  jsonResponse = processGetProductTypeList(userID, jsonRoot, false, includeArchived);
                  break;

                case getProductType:
                  jsonResponse = processGetProductType(userID, jsonRoot, includeArchived);
                  break;

                case putProductType:
                  jsonResponse = processPutProductType(userID, jsonRoot);
                  break;

                case removeProductType:
                  jsonResponse = processRemoveProductType(userID, jsonRoot);
                  break;

                case getUCGRuleList:
                  jsonResponse = processGetUCGRuleList(userID, jsonRoot, true, includeArchived);
                  break;

                case getUCGRuleSummaryList:
                  jsonResponse = processGetUCGRuleList(userID, jsonRoot, false, includeArchived);
                  break;

                case getUCGRule:
                  jsonResponse = processGetUCGRule(userID, jsonRoot, includeArchived);
                  break;

                case putUCGRule:
                  jsonResponse = processPutUCGRule(userID, jsonRoot);
                  break;

                case removeUCGRule:
                  jsonResponse = processRemoveUCGRule(userID, jsonRoot);
                  break;

                case getDeliverableList:
                  jsonResponse = processGetDeliverableList(userID, jsonRoot, true, includeArchived);
                  break;

                case getDeliverableSummaryList:
                  jsonResponse = processGetDeliverableList(userID, jsonRoot, false, includeArchived);
                  break;

                case getDeliverable:
                  jsonResponse = processGetDeliverable(userID, jsonRoot, includeArchived);
                  break;

                case getDeliverableByName:
                  jsonResponse = processGetDeliverableByName(userID, jsonRoot, includeArchived);
                  break;

                case getTokenTypeList:
                  jsonResponse = processGetTokenTypeList(userID, jsonRoot, true, includeArchived);
                  break;

                case getTokenTypeSummaryList:
                  jsonResponse = processGetTokenTypeList(userID, jsonRoot, false, includeArchived);
                  break;

                case putTokenType:
                  jsonResponse = processPutTokenType(userID, jsonRoot);
                  break;

                case getTokenType:
                  jsonResponse = processGetTokenType(userID, jsonRoot, includeArchived);
                  break;

                case removeTokenType:
                  jsonResponse = processRemoveTokenType(userID, jsonRoot);
                  break;

                case getTokenCodesFormats:
                  jsonResponse = processGetTokenCodesFormats(userID, jsonRoot);
                  break;

                case getVoucherTypeList:
                  jsonResponse = processGetVoucherTypeList(userID, jsonRoot, true, includeArchived);
                  break;

                case getVoucherTypeSummaryList:
                  jsonResponse = processGetVoucherTypeList(userID, jsonRoot, false, includeArchived);
                  break;

                case putVoucherType:
                  jsonResponse = processPutVoucherType(userID, jsonRoot);
                  break;

                case getVoucherType:
                  jsonResponse = processGetVoucherType(userID, jsonRoot, includeArchived);
                  break;

                case removeVoucherType:
                  jsonResponse = processRemoveVoucherType(userID, jsonRoot);
                  break;

                case getVoucherCodeFormatList:
                  jsonResponse = processGetVoucherCodeFormatList(userID, jsonRoot);
                  break;

                case getVoucherList:
                  jsonResponse = processGetVoucherList(userID, jsonRoot, true, includeArchived);
                  break;

                case getVoucherSummaryList:
                  jsonResponse = processGetVoucherList(userID, jsonRoot, false, includeArchived);
                  break;

                case putVoucher:
                  jsonResponse = processPutVoucher(userID, jsonRoot);
                  break;

                case getVoucher:
                  jsonResponse = processGetVoucher(userID, jsonRoot, includeArchived);
                  break;

                case removeVoucher:
                  jsonResponse = processRemoveVoucher(userID, jsonRoot);
                  break;

                case getMailTemplateList:
                  jsonResponse = processGetMailTemplateList(userID, jsonRoot, true, true, includeArchived);
                  break;

                case getFullMailTemplateList:
                  jsonResponse = processGetMailTemplateList(userID, jsonRoot, true, false, includeArchived);
                  break;

                case getMailTemplateSummaryList:
                  jsonResponse = processGetMailTemplateList(userID, jsonRoot, false, true, includeArchived);
                  break;

                case getMailTemplate:
                  jsonResponse = processGetMailTemplate(userID, jsonRoot, includeArchived);
                  break;

                case putMailTemplate:
                  jsonResponse = processPutMailTemplate(userID, jsonRoot);
                  break;

                case removeMailTemplate:
                  jsonResponse = processRemoveMailTemplate(userID, jsonRoot);
                  break;

                case getSMSTemplateList:
                  jsonResponse = processGetSMSTemplateList(userID, jsonRoot, true, true, includeArchived);
                  break;

                case getFullSMSTemplateList:
                  jsonResponse = processGetSMSTemplateList(userID, jsonRoot, true, false, includeArchived);
                  break;

                case getSMSTemplateSummaryList:
                  jsonResponse = processGetSMSTemplateList(userID, jsonRoot, false, true, includeArchived);
                  break;

                case getSMSTemplate:
                  jsonResponse = processGetSMSTemplate(userID, jsonRoot, includeArchived);
                  break;

                case putSMSTemplate:
                  jsonResponse = processPutSMSTemplate(userID, jsonRoot);
                  break;

                case removeSMSTemplate:
                  jsonResponse = processRemoveSMSTemplate(userID, jsonRoot);
                  break;

                case getPushTemplateList:
                  jsonResponse = processGetPushTemplateList(userID, jsonRoot, true, true, includeArchived);
                  break;

                case getFullPushTemplateList:
                  jsonResponse = processGetPushTemplateList(userID, jsonRoot, true, false, includeArchived);
                  break;

                case getPushTemplateSummaryList:
                  jsonResponse = processGetPushTemplateList(userID, jsonRoot, false, true, includeArchived);
                  break;

                case getPushTemplate:
                  jsonResponse = processGetPushTemplate(userID, jsonRoot, includeArchived);
                  break;

                case putPushTemplate:
                  jsonResponse = processPutPushTemplate(userID, jsonRoot);
                  break;

                case removePushTemplate:
                  jsonResponse = processRemovePushTemplate(userID, jsonRoot);
                  break;

                case getFulfillmentProviders:
                  jsonResponse = processGetFulfillmentProviders(userID, jsonRoot);
                  break;

                case getPaymentMeans:
                  jsonResponse = processGetPaymentMeanList(userID, jsonRoot, true, includeArchived);
                  break;

                case getPaymentMeanList:
                  jsonResponse = processGetPaymentMeanList(userID, jsonRoot, true, includeArchived);
                  break;

                case getPaymentMeanSummaryList:
                  jsonResponse = processGetPaymentMeanList(userID, jsonRoot, false, includeArchived);
                  break;

                case getPaymentMean:
                  jsonResponse = processGetPaymentMean(userID, jsonRoot, includeArchived);
                  break;

                case putPaymentMean:
                  jsonResponse = processPutPaymentMean(userID, jsonRoot);
                  break;

                case removePaymentMean:
                  jsonResponse = processRemovePaymentMean(userID, jsonRoot);
                  break;

                case getDashboardCounts:
                  jsonResponse = processGetDashboardCounts(userID, jsonRoot, includeArchived);
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

                case getCustomerPoints:
                  jsonResponse = processGetCustomerPoints(userID, jsonRoot);
                  break;

                case getCustomerLoyaltyPrograms:
                  jsonResponse = processGetCustomerLoyaltyPrograms(userID, jsonRoot);
                  break;

                case refreshUCG:
                  jsonResponse = processRefreshUCG(userID, jsonRoot);
                  break;

                case getUploadedFileList:
                  jsonResponse = processGetFilesList(userID, jsonRoot, true, includeArchived);
                  break;

                case getUploadedFileSummaryList:
                  jsonResponse = processGetFilesList(userID, jsonRoot, false, includeArchived);
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
                  jsonResponse = processGetTargetList(userID, jsonRoot, true, includeArchived);
                  break;

                case getTargetSummaryList:
                  jsonResponse = processGetTargetList(userID, jsonRoot, false, includeArchived);
                  break;

                case getTarget:
                  jsonResponse = processGetTarget(userID, jsonRoot, includeArchived);
                  break;

                case putTarget:
                  jsonResponse = processPutTarget(userID, jsonRoot);
                  break;

                case removeTarget:
                  jsonResponse = processRemoveTarget(userID, jsonRoot);
                  break;

                case updateCustomer:
                  jsonResponse = processUpdateCustomer(userID, jsonRoot);
                  break;

                case updateCustomerParent:
                  jsonResponse = processUpdateCustomerParent(userID, jsonRoot);
                  break;

                case removeCustomerParent:
                  jsonResponse = processRemoveCustomerParent(userID, jsonRoot);
                  break;
                  
                case getCommunicationChannelList:
                  jsonResponse = processgetCommunicationChannelList(userID, jsonRoot, true, includeArchived);
                  break;

                case getCommunicationChannelSummaryList:
                  jsonResponse = processgetCommunicationChannelList(userID, jsonRoot, false, includeArchived);
                  break;

                case getCommunicationChannel:
                  jsonResponse = processGetCommunicationChannel(userID, jsonRoot, includeArchived);
                  break;

                case putCommunicationChannel:
                  jsonResponse = processPutCommunicationChannel(userID, jsonRoot);
                  break;

                case removeCommunicationChannel:
                  jsonResponse = processRemoveCommunicationChannel(userID, jsonRoot);
                  break;

                case getBlackoutPeriodsList:
                  jsonResponse = processGetBlackoutPeriodsList(userID, jsonRoot, true, includeArchived);
                  break;

                case getBlackoutPeriodsSummaryList:
                  jsonResponse = processGetBlackoutPeriodsList(userID, jsonRoot, false, includeArchived);
                  break;

                case getBlackoutPeriods:
                  jsonResponse = processGetBlackoutPeriods(userID, jsonRoot, includeArchived);
                  break;

                case putBlackoutPeriods:
                  jsonResponse = processPutBlackoutPeriods(userID, jsonRoot);
                  break;

                case removeBlackoutPeriods:
                  jsonResponse = processRemoveBlackoutPeriods(userID, jsonRoot);
                  break;

                case getLoyaltyProgramTypeList:
                  jsonResponse = processGetLoyaltyProgramTypeList(userID, jsonRoot);
                  break;

                case getLoyaltyProgramList:
                  jsonResponse = processGetLoyaltyProgramList(userID, jsonRoot, true, includeArchived);
                  break;

                case getLoyaltyProgramSummaryList:
                  jsonResponse = processGetLoyaltyProgramList(userID, jsonRoot, false, includeArchived);
                  break;

                case getLoyaltyProgram:
                  jsonResponse = processGetLoyaltyProgram(userID, jsonRoot, includeArchived);
                  break;

                case putLoyaltyProgram:
                  jsonResponse = processPutLoyaltyProgram(userID, jsonRoot);
                  break;

                case removeLoyaltyProgram:
                  jsonResponse = processRemoveLoyaltyProgram(userID, jsonRoot);
                  break;

                case getResellerList:
                  jsonResponse = processGetResellerList(userID, jsonRoot, true, includeArchived);
                  break;

                case getResellerSummaryList:
                  jsonResponse = processGetResellerList(userID, jsonRoot, false, includeArchived);
                  break;

                case getReseller:
                  jsonResponse = processGetReseller(userID, jsonRoot, includeArchived);
                  break;

                case putReseller:
                  jsonResponse = processPutReseller(userID, jsonRoot);
                  break;

                case removeReseller:
                  jsonResponse = processRemoveReseller(userID, jsonRoot);
                  break;

                case enterCampaign:
                  jsonResponse = processEnterCampaign(userID, jsonRoot);
                  break;
                  
                case creditBonus:
                  jsonResponse = processCreditBonus(userID, jsonRoot);
                  break;
                  
                case debitBonus:
                  jsonResponse = processDebitBonus(userID, jsonRoot);
                  break;
                  
                case getExclusionInclusionTargetList:  
                  jsonResponse = processGetExclusionInclusionTargetList(userID, jsonRoot, true, includeArchived);
                  break;
                  
                case getExclusionInclusionTargetSummaryList:
                  jsonResponse = processGetExclusionInclusionTargetList(userID, jsonRoot, false, includeArchived);
                  break;
                  
                case getExclusionInclusionTarget:
                  jsonResponse = processGetExclusionInclusionTarget(userID, jsonRoot, includeArchived);
                  break;

                case putExclusionInclusionTarget:
                  jsonResponse = processPutExclusionInclusionTarget(userID, jsonRoot);
                  break;
                  
                case removeExclusionInclusionTarget:
                  jsonResponse = processRemoveExclusionInclusionTarget(userID, jsonRoot);
                  break;
                  
                case getSegmentContactPolicyList:  
                  jsonResponse = processGetSegmentContactPolicyList(userID, jsonRoot, true, includeArchived);
                  break;
                  
                case getSegmentContactPolicySummaryList:
                  jsonResponse = processGetSegmentContactPolicyList(userID, jsonRoot, false, includeArchived);
                  break;

                case getSegmentContactPolicy:
                  jsonResponse = processGetSegmentContactPolicy(userID, jsonRoot, includeArchived);
                  break;
                  
                case putSegmentContactPolicy:
                  jsonResponse = processPutSegmentContactPolicy(userID, jsonRoot);
                  break;
                  
                case removeSegmentContactPolicy:
                  jsonResponse = processRemoveSegmentContactPolicy(userID, jsonRoot);
                  break;
                  
                case getBillingModes:
                  jsonResponse = processGetBillingModes(userID, jsonRoot);
                  break;

                case getPartnerTypes:
                  jsonResponse = processGetPartnerTypes(userID, jsonRoot);
                  break;

                case configAdaptorSupportedLanguages:
                  jsonResponse = processConfigAdaptorSupportedLanguages(jsonRoot);
                  break;

                case configAdaptorSubscriberMessageTemplate:
                  jsonResponse = processConfigAdaptorSubscriberMessageTemplate(jsonRoot);
                  break;

                case configAdaptorOffer:
                  jsonResponse = processConfigAdaptorOffer(jsonRoot);
                  break;

                case configAdaptorProduct:
                  jsonResponse = processConfigAdaptorProduct(jsonRoot);
                  break;

                case configAdaptorPresentationStrategy:
                  jsonResponse = processConfigAdaptorPresentationStrategy(jsonRoot);
                  break;

                case configAdaptorScoringStrategy:
                  jsonResponse = processConfigAdaptorScoringStrategy(jsonRoot);
                  break;

                case configAdaptorCallingChannel:
                  jsonResponse = processConfigAdaptorCallingChannel(jsonRoot);
                  break;

                case configAdaptorSalesChannel:
                  jsonResponse = processConfigAdaptorSalesChannel(jsonRoot);
                  break;

                case configAdaptorCommunicationChannel:
                  jsonResponse = processConfigAdaptorCommunicationChannel(jsonRoot);
                  break;

                case configAdaptorBlackoutPeriods:
                  jsonResponse = processConfigAdaptorBlackoutPeriods(jsonRoot);
                  break;

                case configAdaptorContactPolicy:
                  jsonResponse = processConfigAdaptorContactPolicy(jsonRoot);
                  break;
                  
                case configAdaptorSegmentationDimension:
                  jsonResponse = processConfigAdaptorSegmentationDimension(jsonRoot);
                  break;

                case configAdaptorCampaign:
                  jsonResponse = processConfigAdaptorCampaign(jsonRoot);
                  break;

                case configAdaptorJourneyObjective:
                  jsonResponse = processConfigAdaptorJourneyObjective(jsonRoot);
                  break;

                case configAdaptorProductType:
                  jsonResponse = processConfigAdaptorProductType(jsonRoot);
                  break;

                case configAdaptorOfferObjective:
                  jsonResponse = processConfigAdaptorOfferObjective(jsonRoot);
                  break;

                case configAdaptorScoringEngines:
                  jsonResponse = processConfigAdaptorScoringEngines(jsonRoot);
                  break;

                case configAdaptorPresentationCriterionFields:
                  jsonResponse = processConfigAdaptorPresentationCriterionFields(jsonRoot);
                  break;

                case configAdaptorDefaultNoftificationDailyWindows:
                  jsonResponse = processConfigAdaptorDefaultNoftificationDailyWindows(jsonRoot);
                  break;

                case configAdaptorDeliverable:
                  jsonResponse = processConfigAdaptorDeliverable(jsonRoot);
                  break;

                case getCriterionFieldAvailableValuesList:
                  jsonResponse = processGetCriterionFieldAvailableValuesList(userID, jsonRoot, true, includeArchived);
                  break;

                case getCriterionFieldAvailableValuesSummaryList:
                  jsonResponse = processGetCriterionFieldAvailableValuesList(userID, jsonRoot, false, includeArchived);
                  break;

                case getCriterionFieldAvailableValues:
                  jsonResponse = processGetCriterionFieldAvailableValues(userID, jsonRoot, includeArchived);
                  break;

                case putCriterionFieldAvailableValues:
                  jsonResponse = processPutCriterionFieldAvailableValues(userID, jsonRoot);
                  break;

                case removeCriterionFieldAvailableValues:
                  jsonResponse = processRemoveCriterionFieldAvailableValues(userID, jsonRoot);
                  break;

                case getEffectiveSystemTime:
                  jsonResponse = processGetEffectiveSystemTime(userID, jsonRoot);
                  break;
                  
                case getCustomerNBOs:
                  jsonResponse = processGetCustomerNBOs(userID, jsonRoot);
                  break;

                case getTokensCodesList:
                  jsonResponse = processGetTokensCodesList(userID, jsonRoot);
                  break;

                case acceptOffer:
                  jsonResponse = processAcceptOffer(userID, jsonRoot);
                  break;
                  
                case getTokenEventDetails:
                  jsonResponse = processGetTokenEventDetails(userID, jsonRoot);
                  break;
                  
                case getSourceAddressList:
                  jsonResponse = processGetSourceAddressList(userID, jsonRoot, true, includeArchived);
                  break;

                case getSourceAddressSummaryList:
                  jsonResponse = processGetSourceAddressList(userID, jsonRoot, false, includeArchived);
                  break;

                case getSourceAddress:
                  jsonResponse = processGetSourceAddress(userID, jsonRoot, includeArchived);
                  break;

                case putSourceAddress:
                  jsonResponse = processPutSourceAddress(userID, jsonRoot);
                  break;

                case removeSourceAddress:
                  jsonResponse = processRemoveSourceAddress(userID, jsonRoot);
                  break;

                case getTenantList:
                  jsonResponse = processGetTenantList(userID, jsonRoot, true, includeArchived);
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
        switch (api)
          {
            case putUploadedFile:
              break;

            default:
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

              /*****************************************
              *
              *  userID
              *
              *****************************************/

              String jsonUserID = JSONUtilities.decodeString(jsonRoot, "userID", false);
              if (jsonUserID == null && userID != null)
                {
                  jsonRoot.put("userID", userID);
                }

              /*****************************************
              *
              *  break
              *
              *****************************************/

              break;
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
  *  getIncludeDynamicParameter(jsonRoot)
  *
  *****************************************/

  private boolean getIncludeDynamicParameter(JSONObject jsonRoot)
  {
    return JSONUtilities.decodeBoolean(jsonRoot, "includeDynamic", Boolean.FALSE);
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
  *  getStaticConfiguration
  *
  *****************************************/

  private JSONObject processGetStaticConfiguration(String userID, JSONObject jsonRoot, boolean includeArchived)
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
    *  retrieve supportedRelationships
    *
    *****************************************/

    List<JSONObject> supportedRelationships = new ArrayList<JSONObject>();
    for (SupportedRelationship supportedRelationship : Deployment.getSupportedRelationships().values())
      {
        JSONObject supportedRelationshipJSON = supportedRelationship.getJSONRepresentation();
        supportedRelationships.add(supportedRelationshipJSON);
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
    *  retrieve communicationChannels
    *
    *****************************************/

    List<JSONObject> communicationChannels = new ArrayList<JSONObject>();
    for (GUIManagedObject communicationChannel : communicationChannelService.getStoredCommunicationChannels(includeArchived))
      {
        JSONObject communicationChannelJSON = communicationChannel.getJSONRepresentation();
        communicationChannels.add(communicationChannelJSON);
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
    *  retrieve offerProperties
    *
    *****************************************/

    List<JSONObject> offerProperties = new ArrayList<JSONObject>();
    for (OfferProperty offerProperty : Deployment.getOfferProperties().values())
      {
        JSONObject offerPropertyJSON = offerProperty.getJSONRepresentation();
        offerProperties.add(offerPropertyJSON);
      }

    /*****************************************
    *
    *  retrieve scoringEngines
    *
    *****************************************/

    List<JSONObject> scoringEngines = new ArrayList<JSONObject>();
    for (ScoringEngine scoringEngine : Deployment.getScoringEngines().values())
      {
        JSONObject scoringEngineJSON = scoringEngine.getJSONRepresentation();
        scoringEngines.add(scoringEngineJSON);
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

    List<JSONObject> nodeTypes;
    try
      {
        nodeTypes = processNodeTypes(Deployment.getNodeTypes(), Collections.<String,CriterionField>emptyMap(), Collections.<String,CriterionField>emptyMap());
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

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
    *  retrieve workflowToolboxSections
    *
    *****************************************/

    List<JSONObject> workflowToolboxSections = new ArrayList<JSONObject>();
    for (ToolboxSection workflowToolboxSection : Deployment.getWorkflowToolbox().values())
      {
        JSONObject workflowToolboxSectionJSON = workflowToolboxSection.getJSONRepresentation();
        workflowToolboxSections.add(workflowToolboxSectionJSON);
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
    response.put("supportedRelationships", JSONUtilities.encodeArray(supportedRelationships));
    response.put("callingChannelProperties", JSONUtilities.encodeArray(callingChannelProperties));
    response.put("supportedDataTypes", JSONUtilities.encodeArray(supportedDataTypes));
    response.put("profileCriterionFields", JSONUtilities.encodeArray(profileCriterionFields));
    response.put("presentationCriterionFields", JSONUtilities.encodeArray(presentationCriterionFields));
    response.put("offerCategories", JSONUtilities.encodeArray(offerCategories));
    response.put("offerProperties", JSONUtilities.encodeArray(offerProperties));
    response.put("scoringEngines", JSONUtilities.encodeArray(scoringEngines));
    response.put("offerOptimizationAlgorithms", JSONUtilities.encodeArray(offerOptimizationAlgorithms));
    response.put("nodeTypes", JSONUtilities.encodeArray(nodeTypes));
    response.put("journeyToolbox", JSONUtilities.encodeArray(journeyToolboxSections));
    response.put("campaignToolbox", JSONUtilities.encodeArray(campaignToolboxSections));
    response.put("workflowToolbox", JSONUtilities.encodeArray(workflowToolboxSections));
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
  *  getBillingModes
  *
  *****************************************/

  private JSONObject processGetBillingModes(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve BillingModes
    *
    *****************************************/

    List<JSONObject> billingModes = new ArrayList<JSONObject>();
    for (BillingMode billingMode : Deployment.getBillingModes().values())
      {
        JSONObject billingModeJSON = billingMode.getJSONRepresentation();
        billingModes.add(billingModeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("billingModes", JSONUtilities.encodeArray(billingModes));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  getPartnerTypes
  *
  *****************************************/

  private JSONObject processGetPartnerTypes(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve PartnerTypes
    *
    *****************************************/

    List<JSONObject> partnerTypes = new ArrayList<JSONObject>();
    for (PartnerType partnerType : Deployment.getPartnerTypes().values())
      {
        JSONObject partnerTypeJSON = partnerType.getJSONRepresentation();
        partnerTypes.add(partnerTypeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("partnerTypes", JSONUtilities.encodeArray(partnerTypes));
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
  *  getSupportedRelationships
  *
  *****************************************/

  private JSONObject processGetSupportedRelationships(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve supportedRelationships
    *
    *****************************************/

    List<JSONObject> supportedRelationships = new ArrayList<JSONObject>();
    for (SupportedRelationship supportedRelationship : Deployment.getSupportedRelationships().values())
      {
        JSONObject supportedRelationshipJSON = supportedRelationship.getJSONRepresentation();
        supportedRelationships.add(supportedRelationshipJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedRelationships", JSONUtilities.encodeArray(supportedRelationships));
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
  *  getLoyaltyProgramPointsEvents
  *
  *****************************************/

  private JSONObject processGetLoyaltyProgramPointsEvents(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve events
    *
    *****************************************/

    List<JSONObject> events = evaluateEnumeratedValues("loyaltyProgramPointsEventNames", SystemTime.getCurrentTime(), true);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("loyaltyProgramPointsEvents", JSONUtilities.encodeArray(events));
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

  private JSONObject processGetProfileCriterionFields(String userID, JSONObject jsonRoot, CriterionContext profileContext)
  {
    /*****************************************
    *
    *  retrieve profile criterion fields
    *
    *****************************************/

    Map<String,List<JSONObject>> currentGroups = new HashMap<>();
    List<JSONObject> profileCriterionFields = processCriterionFields(profileContext.getCriterionFields(), false, currentGroups);

    List<JSONObject> groups = new ArrayList<>();
    for (String id : currentGroups.keySet())
      {
        List<JSONObject> group = currentGroups.get(id);
        HashMap<String,Object> groupJSON = new HashMap<String,Object>();
        groupJSON.put("id", id);
        groupJSON.put("value", JSONUtilities.encodeArray(group));
        groups.add(JSONUtilities.encodeObject(groupJSON));
      }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("profileCriterionFields", JSONUtilities.encodeArray(profileCriterionFields));
    response.put("groups", JSONUtilities.encodeArray(groups));
    return JSONUtilities.encodeObject(response);
  } 

  /*****************************************
  *
  *  getProfileCriterionFieldIDs
  *
  *****************************************/

  private JSONObject processGetProfileCriterionFieldIDs(String userID, JSONObject jsonRoot, CriterionContext profileContext)
  {
    /*****************************************
    *
    *  retrieve profile criterion fields
    *
    *****************************************/

    List<JSONObject> profileCriterionFields = processCriterionFields(profileContext.getCriterionFields(), false);

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

  private JSONObject processGetProfileCriterionField(String userID, JSONObject jsonRoot, CriterionContext profileContext)
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

        List<JSONObject> profileCriterionFields = processCriterionFields(profileContext.getCriterionFields(), false);

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

    Map<String,List<JSONObject>> currentGroups = new HashMap<>();
    List<JSONObject> presentationCriterionFields = processCriterionFields(CriterionContext.Presentation.getCriterionFields(), false, currentGroups);
    
    List<JSONObject> groups = new ArrayList<>();
    for (String id : currentGroups.keySet())
      {
        List<JSONObject> group = currentGroups.get(id);
        HashMap<String,Object> groupJSON = new HashMap<String,Object>();
        groupJSON.put("id", id);
        groupJSON.put("value", JSONUtilities.encodeArray(group));
        groups.add(JSONUtilities.encodeObject(groupJSON));
      }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("presentationCriterionFields", JSONUtilities.encodeArray(presentationCriterionFields));
    response.put("groups", JSONUtilities.encodeArray(groups));
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
    Map<String,GUINode> contextVariableNodes = Journey.decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot,"contextVariableNodes", false), journeyParameters, Collections.<String,CriterionField>emptyMap(), true, journeyService, subscriberMessageTemplateService, dynamicEventDeclarationsService);
    NodeType journeyNodeType = Deployment.getNodeTypes().get(JSONUtilities.decodeString(jsonRoot, "nodeTypeID", true));
    EvolutionEngineEventDeclaration journeyNodeEvent = (JSONUtilities.decodeString(jsonRoot, "eventName", false) != null) ? dynamicEventDeclarationsService.getStaticAndDynamicEvolutionEventDeclarations().get(JSONUtilities.decodeString(jsonRoot, "eventName", true)) : null;
    Journey selectedJourney = (JSONUtilities.decodeString(jsonRoot, "selectedJourneyID", false) != null) ? journeyService.getActiveJourney(JSONUtilities.decodeString(jsonRoot, "selectedJourneyID", true), SystemTime.getCurrentTime()) : null;
    boolean tagsOnly = JSONUtilities.decodeBoolean(jsonRoot, "tagsOnly", Boolean.FALSE);

    /*****************************************
    *
    *  retrieve journey criterion fields
    *
    *****************************************/

    List<JSONObject> journeyCriterionFields = Collections.<JSONObject>emptyList();
    List<JSONObject> groups = new ArrayList<>();
    if (journeyNodeType != null)
      {
        CriterionContext criterionContext = new CriterionContext(journeyParameters, Journey.processContextVariableNodes(contextVariableNodes, journeyParameters), journeyNodeType, journeyNodeEvent, selectedJourney);
        Map<String,List<JSONObject>> currentGroups = new HashMap<>();
        journeyCriterionFields = processCriterionFields(criterionContext.getCriterionFields(), tagsOnly, currentGroups);
        for (String id : currentGroups.keySet())
          {
            List<JSONObject> group = currentGroups.get(id);
            HashMap<String,Object> groupJSON = new HashMap<String,Object>();
            groupJSON.put("id", id);
            groupJSON.put("value", JSONUtilities.encodeArray(group));
            groups.add(JSONUtilities.encodeObject(groupJSON));
          }
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
        response.put("groups", JSONUtilities.encodeArray(groups));
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
    Map<String,GUINode> contextVariableNodes = Journey.decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot,"contextVariableNodes", false), journeyParameters, Collections.<String,CriterionField>emptyMap(), false, journeyService, subscriberMessageTemplateService, dynamicEventDeclarationsService);
    NodeType journeyNodeType = Deployment.getNodeTypes().get(JSONUtilities.decodeString(jsonRoot, "nodeTypeID", true));
    EvolutionEngineEventDeclaration journeyNodeEvent = (JSONUtilities.decodeString(jsonRoot, "eventName", false) != null) ? dynamicEventDeclarationsService.getStaticAndDynamicEvolutionEventDeclarations().get(JSONUtilities.decodeString(jsonRoot, "eventName", true)) : null;
    Journey selectedJourney = (JSONUtilities.decodeString(jsonRoot, "selectedJourneyID", false) != null) ? journeyService.getActiveJourney(JSONUtilities.decodeString(jsonRoot, "selectedJourneyID", true), SystemTime.getCurrentTime()) : null;
    boolean tagsOnly = JSONUtilities.decodeBoolean(jsonRoot, "tagsOnly", Boolean.FALSE);

    /*****************************************
    *
    *  retrieve journey criterion fields
    *
    *****************************************/

    List<JSONObject> journeyCriterionFields = Collections.<JSONObject>emptyList();
    if (journeyNodeType != null)
      {
        CriterionContext criterionContext = new CriterionContext(journeyParameters, Journey.processContextVariableNodes(contextVariableNodes, journeyParameters), journeyNodeType, journeyNodeEvent, selectedJourney);
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
    Map<String,GUINode> contextVariableNodes = Journey.decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot,"contextVariableNodes", false), journeyParameters, Collections.<String,CriterionField>emptyMap(), false, journeyService, subscriberMessageTemplateService, dynamicEventDeclarationsService);
    NodeType journeyNodeType = Deployment.getNodeTypes().get(JSONUtilities.decodeString(jsonRoot, "nodeTypeID", true));
    EvolutionEngineEventDeclaration journeyNodeEvent = (JSONUtilities.decodeString(jsonRoot, "eventName", false) != null) ? dynamicEventDeclarationsService.getStaticAndDynamicEvolutionEventDeclarations().get(JSONUtilities.decodeString(jsonRoot, "eventName", true)) : null;
    Journey selectedJourney = (JSONUtilities.decodeString(jsonRoot, "selectedJourneyID", false) != null) ? journeyService.getActiveJourney(JSONUtilities.decodeString(jsonRoot, "selectedJourneyID", true), SystemTime.getCurrentTime()) : null;
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
            CriterionContext criterionContext = new CriterionContext(journeyParameters, Journey.processContextVariableNodes(contextVariableNodes, journeyParameters), journeyNodeType, journeyNodeEvent, selectedJourney);
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
  *  getOfferProperties
  *
  *****************************************/

  private JSONObject processGetOfferProperties(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve offerProperties
    *
    *****************************************/

    List<JSONObject> offerProperties = new ArrayList<JSONObject>();
    for (OfferProperty offerProperty : Deployment.getOfferProperties().values())
      {
        JSONObject offerPropertyJSON = offerProperty.getJSONRepresentation();
        offerProperties.add(offerPropertyJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("offerProperties", JSONUtilities.encodeArray(offerProperties));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getScoringEngines
  *
  *****************************************/

  private JSONObject processGetScoringEngines(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve scoringEngines
    *
    *****************************************/

    List<JSONObject> scoringEngines = new ArrayList<JSONObject>();
    for (ScoringEngine scoringEngine : Deployment.getScoringEngines().values())
      {
        JSONObject scoringEngineJSON = scoringEngine.getJSONRepresentation();
        scoringEngines.add(scoringEngineJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("scoringEngines", JSONUtilities.encodeArray(scoringEngines));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getOfferOptimizationAlgorithms
  *
  *****************************************/

  private JSONObject processGetOfferOptimizationAlgorithms(String userID, JSONObject jsonRoot, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve offerOptimizationAlgorithms
    *
    *****************************************/

    List<JSONObject> offerOptimizationAlgorithms = new ArrayList<JSONObject>();
    for (OfferOptimizationAlgorithm offerOptimizationAlgorithm : Deployment.getOfferOptimizationAlgorithms().values())
    {
      if(!offerOptimizationAlgorithm.getID().equals("matrix-algorithm"))
      {
        JSONObject offerOptimizationAlgorithmJSON = offerOptimizationAlgorithm.getJSONRepresentation();
        offerOptimizationAlgorithms.add(offerOptimizationAlgorithmJSON);
      }
    }
    
    // Add DNBOMatrix Algorithm for gui
    Date now = SystemTime.getCurrentTime();
    for (GUIManagedObject dnboMatrix : dnboMatrixService.getStoredDNBOMatrixes(includeArchived))
    {
      JSONObject matrixObject = presentationStrategyService.generateResponseJSON(dnboMatrix, false, now);
      matrixObject.replace("id", "DNBO" + JSONUtilities.decodeString(matrixObject, "id", true));
      offerOptimizationAlgorithms.add(matrixObject);
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
  *  processGetScoringTypesList
  *
  *****************************************/

  private JSONObject processGetScoringTypesList(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve offerOptimizationAlgorithms
    *
    *****************************************/

    List<JSONObject> scoringTypes = new ArrayList<JSONObject>();
    for (ScoringType scoringType : Deployment.getScoringTypes().values())
      {
        JSONObject scoringTypeJSON = scoringType.getJSONRepresentation();
        scoringTypes.add(scoringTypeJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("scoringTypes", JSONUtilities.encodeArray(scoringTypes));
    return JSONUtilities.encodeObject(response);
  }

  
  /*****************************************
  *
  *  processGetDNBOMatrixVariablesList
  *
  *****************************************/

  private JSONObject processGetDNBOMatrixVariablesList(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve dnboMatrixVariables
    *
    *****************************************/

    List<JSONObject> dnboMatrixVariables = new ArrayList<JSONObject>();
    for (DNBOMatrixVariable dnboMatrixVariable : Deployment.getDNBOMatrixVariables().values())
      {
        JSONObject dnboMatrixVariableJSON = dnboMatrixVariable.getJSONRepresentation();
        dnboMatrixVariables.add(dnboMatrixVariableJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("dnboMatrixVariables", JSONUtilities.encodeArray(dnboMatrixVariables));
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
    Map<String,GUINode> contextVariableNodes = Journey.decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot,"contextVariableNodes", false), journeyParameters, Collections.<String,CriterionField>emptyMap(), false, journeyService, subscriberMessageTemplateService, dynamicEventDeclarationsService);
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
  *  processGetJourneyList
  *
  *****************************************/

  private JSONObject processGetJourneyList(String userID, JSONObject jsonRoot, GUIManagedObjectType objectType, boolean fullDetails, boolean externalOnly, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert journeys
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> journeys = new ArrayList<JSONObject>();
    for (GUIManagedObject journey : journeyService.getStoredJourneys(includeArchived))
      {
        if (journey.getGUIManagedObjectType().equals(objectType) && (! externalOnly || ! journey.getInternalOnly()))
          {
            JSONObject journeyInfo = journeyService.generateResponseJSON(journey, fullDetails, now);
            int subscriberCount = 0;
            JourneyTrafficHistory journeyTrafficHistory = journeyTrafficReader.get(journey.getGUIManagedObjectID());
            if (journeyTrafficHistory != null && journeyTrafficHistory.getCurrentData() != null && journeyTrafficHistory.getCurrentData().getGlobal() != null)
              {
                subscriberCount = journeyTrafficHistory.getCurrentData().getGlobal().getSubscriberCount();
              }
            journeyInfo.put("journeySubscriberCount", subscriberCount);
            journeys.add(journeyInfo);
          }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    switch (objectType)
      {
        case Journey:
          response.put("journeys", JSONUtilities.encodeArray(journeys));
          break;

        case Campaign:
          response.put("campaigns", JSONUtilities.encodeArray(journeys));
          break;

        case Workflow:
          response.put("workflows", JSONUtilities.encodeArray(journeys));
          break;

        case BulkCampaign:
          response.put("bulkCampaigns", JSONUtilities.encodeArray(journeys));
          break;

        default:
          response.put("journeys", JSONUtilities.encodeArray(journeys));
          break;
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetJourney
  *
  *****************************************/

  private JSONObject processGetJourney(String userID, JSONObject jsonRoot, GUIManagedObjectType objectType, boolean externalOnly, boolean includeArchived)
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
    *  retrieve and decorate campaign
    *
    *****************************************/

    GUIManagedObject journey = journeyService.getStoredJourney(journeyID, includeArchived);
    journey = (journey != null && journey.getGUIManagedObjectType() == objectType && (! externalOnly || ! journey.getInternalOnly())) ? journey : null;
    JSONObject journeyJSON = journeyService.generateResponseJSON(journey, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    switch (objectType)
      {
        case Journey:
          response.put("responseCode", (journey != null) ? "ok" : "journeyNotFound");
          if (journey != null) response.put("journey", journeyJSON);
          break;

        case Campaign:
          response.put("responseCode", (journey != null) ? "ok" : "campaignNotFound");
          if (journey != null) response.put("campaign", journeyJSON);
          break;

        case Workflow:
          response.put("responseCode", (journey != null) ? "ok" : "workflowNotFound");
          if (journey != null) response.put("workflow", journeyJSON);
          break;

        case BulkCampaign:
          response.put("responseCode", (journey != null) ? "ok" : "bulkCampaignNotFound");
          if (journey != null) response.put("bulkCampaign", journeyJSON);
          break;

        default:
          response.put("responseCode", (journey != null) ? "ok" : "notFound");
          if (journey != null) response.put("journey", journeyJSON);
          break;
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutJourney
  *
  *****************************************/

  private JSONObject processPutJourney(String userID, JSONObject jsonRoot, GUIManagedObjectType objectType)
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
    
    //
    // initial approval
    //
    
    JourneyStatus approval = JourneyStatus.Pending;

    /*****************************************
    *
    *  existing journey
    *
    *****************************************/

    GUIManagedObject existingJourney = journeyService.getStoredJourney(journeyID);
    existingJourney = (existingJourney != null && existingJourney.getGUIManagedObjectType() == objectType) ? existingJourney : null;

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
        //
        // change approval if existingJourney
        //
        
        if (existingJourney != null && existingJourney.getAccepted())
          {
            approval = ((Journey) existingJourney).getApproval();
          }
        
        /****************************************
        *
        *  instantiate journey
        *
        ****************************************/

        Journey journey = new Journey(jsonRoot, objectType, epoch, existingJourney, journeyService, catalogCharacteristicService, subscriberMessageTemplateService, dynamicEventDeclarationsService, communicationChannelService, approval);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyService.putJourney(journey, journeyObjectiveService, catalogCharacteristicService, targetService, (existingJourney == null), userID);

        /*****************************************
        *
        *  handle related deliverable
        *
        *****************************************/

        if (GUIManagedObjectType.Campaign.equals(objectType))
          {
            DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("journeyFulfillment");
            JSONObject deliveryManagerJSON = (deliveryManager != null) ? deliveryManager.getJSONRepresentation() : null;
            String providerID = (deliveryManagerJSON != null) ? (String) deliveryManagerJSON.get("providerID") : null;
            if (providerID != null)
              {
                if (journey.getTargetingType().equals(TargetingType.Manual))
                  {

                    //
                    // create deliverable  --  only if campaign has "manual provisioning"
                    //

                    Map<String, Object> deliverableMap = new HashMap<String, Object>();
                    deliverableMap.put("id", "journey-" + journey.getJourneyID());
                    deliverableMap.put("fulfillmentProviderID", providerID);
                    deliverableMap.put("externalAccountID", journey.getJourneyID());
                    deliverableMap.put("name", journey.getJourneyName());
                    deliverableMap.put("display", journey.getJourneyName());
                    deliverableMap.put("active", true);
                    deliverableMap.put("unitaryCost", 0);
                    Deliverable deliverable = new Deliverable(JSONUtilities.encodeObject(deliverableMap), epoch, null);
                    deliverableService.putDeliverable(deliverable, true, userID);
                  }
                else
                  {

                    //
                    // delete deliverable  --  only if campaign does NOT have "manual provisioning"
                    //

                    // TODO SCH : may need to check that deliverable is not used in any offer

                    for (GUIManagedObject deliverableObject : deliverableService.getStoredDeliverables())
                      {
                        Deliverable deliverable = (Deliverable) deliverableObject;
                        if (deliverable.getFulfillmentProviderID().equals(providerID) && deliverable.getExternalAccountID().equals(journey.getJourneyID()))
                          {
                            deliverableService.removeDeliverable(deliverable.getDeliverableID(), userID);
                          }
                      }

                  }
              }
          }

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

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, objectType, epoch);

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

        response.put("id", incompleteObject.getGUIManagedObjectID());
        switch (objectType) 
          {
            case Journey:
              response.put("responseCode", "journeyNotValid");
              break;

            case Campaign:
              response.put("responseCode", "campaignNotValid");
              break;

            case Workflow:
              response.put("responseCode", "workflowNotValid");
              break;

            case BulkCampaign:
              response.put("responseCode", "bulkCampaignNotValid");
              break;

            default:
              response.put("responseCode", "notValid");
              break;
          }
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

  private JSONObject processRemoveJourney(String userID, JSONObject jsonRoot, GUIManagedObjectType objectType)
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject journey = journeyService.getStoredJourney(journeyID);
    journey = (journey != null && journey.getGUIManagedObjectType() == objectType) ? journey : null;
    if (journey != null && (force || !journey.getReadOnly())) 
      {

        //
        // remove journey
        //

        journeyService.removeJourney(journeyID, userID);

        //
        // remove related deliverable
        //

        if (GUIManagedObjectType.Campaign.equals(objectType))
          {
            //
            // TODO SCH : what if deliverable is used in an offer ?
            //
            
            DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("journeyFulfillment");
            JSONObject deliveryManagerJSON = (deliveryManager != null) ? deliveryManager.getJSONRepresentation() : null;
            String providerID = (deliveryManagerJSON != null) ? (String) deliveryManagerJSON.get("providerID") : null;
            if (providerID != null)
              {
                for(GUIManagedObject deliverableOgbject : deliverableService.getStoredDeliverables())
                  {
                    Deliverable deliverable = (Deliverable)deliverableOgbject;
                    if (deliverable.getFulfillmentProviderID().equals(providerID) && deliverable.getExternalAccountID().equals(journeyID))
                      {
                        deliverableService.removeDeliverable(deliverable.getDeliverableID(), userID);
                      }
                  }
              }
          }
      }

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (journey != null && (force || !journey.getReadOnly()))
      responseCode = "ok";
    else if (journey != null)
      responseCode = "failedReadOnly";
    else {
      switch (objectType) {
          case Journey:
            responseCode = "journeyNotFound";
            break;

          case Campaign:
            responseCode = "campaignNotFound";
            break;

          case Workflow:
            responseCode = "workflowNotFound";
            break;

          case BulkCampaign:
            responseCode = "bulkCampaignNotFound";
            break;

          default:
            responseCode = "notFound";
            break;
      }
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
  *  processSetActive
  *
  *****************************************/

  private JSONObject processSetActive(String userID, JSONObject jsonRoot, GUIManagedObjectType type, boolean active)
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

    String elementID = JSONUtilities.decodeString(jsonRoot, "id", true);
    String approvalStatusString = JSONUtilities.decodeString(jsonRoot, "status", false);
    JourneyStatus approval = JourneyStatus.fromExternalRepresentation(approvalStatusString);

    /*****************************************
    *
    *  validate existing object (journey, campaign or bulk campaign)
    *  - exists
    *  - valid
    *  - not read-only
    *
    *****************************************/

    GUIManagedObject existingElement = journeyService.getStoredJourney(elementID);
    String responseCode = null;
    switch (type)
      {
        case Journey:
          responseCode = (responseCode == null && existingElement == null) ? "journeyNotFound" : responseCode;
          responseCode = (responseCode == null && existingElement.getGUIManagedObjectType() != GUIManagedObjectType.Journey) ? "journeyNotFound" : responseCode;
          responseCode = (responseCode == null && ! existingElement.getAccepted()) ? "journeyNotValid" : responseCode;
          responseCode = (responseCode == null && existingElement.getReadOnly()) ? "failedReadOnly" : responseCode;
          break;

        case Campaign:
          responseCode = (responseCode == null && existingElement == null) ? "campaignNotFound" : responseCode;
          responseCode = (responseCode == null && existingElement.getGUIManagedObjectType() != GUIManagedObjectType.Campaign) ? "campaignNotFound" : responseCode;
          responseCode = (responseCode == null && ! existingElement.getAccepted()) ? "campaignNotValid" : responseCode;
          responseCode = (responseCode == null && existingElement.getReadOnly()) ? "failedReadOnly" : responseCode;
          break;

        case BulkCampaign:
          responseCode = (responseCode == null && existingElement == null) ? "bulkCampaignNotFound" : responseCode;
          responseCode = (responseCode == null && existingElement.getGUIManagedObjectType() != GUIManagedObjectType.BulkCampaign) ? "bulkCampaignNotFound" : responseCode;
          responseCode = (responseCode == null && ! existingElement.getAccepted()) ? "bulkCampaignNotValid" : responseCode;
          responseCode = (responseCode == null && existingElement.getReadOnly()) ? "failedReadOnly" : responseCode;
          break;

        default:
          throw new ServerRuntimeException("invalid guimanagedobject type");
      }
    
    if (responseCode != null)
      {
        response.put("id", elementID);
        if (existingElement != null) response.put("accepted", existingElement.getAccepted());
        if (existingElement != null) response.put("valid", existingElement.getAccepted());
        if (existingElement != null) response.put("processing", journeyService.isActiveJourney(existingElement, now));
        response.put("responseCode", responseCode);
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process element (journey, campaign or bulk campaign)
    *
    *****************************************/

    JSONObject elementRoot = (JSONObject) existingElement.getJSONRepresentation().clone();
    long epoch = epochServer.getKey();
    try
      {
        
        switch (approval)
          {
            case Unknown:
            case StartedApproved:
              elementRoot.put("active", active);
              approval = JourneyStatus.StartedApproved;
              break;
              
            case WaitingForApproval:
            case PendingNotApproved:
              elementRoot.put("active", false);
              break;
              
            default:
              throw new ServerRuntimeException("invalid approvalStatus " + approval);
        }

        /****************************************
        *
        *  instantiate element (journey, campaign or bulk campaign)
        *
        ****************************************/

        Journey element = new Journey(elementRoot, type, epoch, existingElement, journeyService, catalogCharacteristicService, subscriberMessageTemplateService, dynamicEventDeclarationsService, communicationChannelService, approval);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyService.putJourney(element, journeyObjectiveService, catalogCharacteristicService, targetService, (existingElement == null), userID);

        /*****************************************
        *
        *  evaluate targets
        *
        *****************************************/

        if (active && element.getTargetID() != null)
          {
            EvaluateTargets evaluateTargets = new EvaluateTargets(Collections.<String>singleton(element.getJourneyID()), element.getTargetID());
            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getEvaluateTargetsTopic(), EvaluateTargets.serde().serializer().serialize(Deployment.getEvaluateTargetsTopic(), evaluateTargets)));
          }
        
        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", element.getJourneyID());
        response.put("accepted", element.getAccepted());
        response.put("valid", element.getAccepted());
        response.put("processing", journeyService.isActiveJourney(element, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(elementRoot, type, epoch);

        //
        //  store
        //

        journeyService.putJourney(incompleteObject, journeyObjectiveService, catalogCharacteristicService, targetService, (existingElement == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        switch (type) {
        case Journey:
          response.put("journeyID", incompleteObject.getGUIManagedObjectID());
          response.put("responseCode", "journeyNotValid");
          break;

        case Campaign:
          response.put("campaignID", incompleteObject.getGUIManagedObjectID());
          response.put("responseCode", "campaignNotValid");
          break;

        case BulkCampaign:
          response.put("bulkCampaignID", incompleteObject.getGUIManagedObjectID());
          response.put("responseCode", "bulkCampaignNotValid");
          break;
        }
        
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
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
  *  getWorkflowToolbox
  *
  *****************************************/

  private JSONObject processGetWorkflowToolbox(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve workflowToolboxSections
    *
    *****************************************/

    List<JSONObject> workflowToolboxSections = new ArrayList<JSONObject>();
    for (ToolboxSection workflowToolboxSection : Deployment.getWorkflowToolbox().values())
      {
        JSONObject workflowToolboxSectionJSON = workflowToolboxSection.getJSONRepresentation();
        workflowToolboxSections.add(workflowToolboxSectionJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("workflowToolbox", JSONUtilities.encodeArray(workflowToolboxSections));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutBulkCampaign
  *
  *****************************************/  

  private JSONObject processPutBulkCampaign(String userID, JSONObject jsonRoot)
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
    *  retrieve campaign informations
    *
    *****************************************/

    //
    //  get journey template
    //
    
    BulkType bulkType = BulkType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "bulkType", false));
    String journeyTemplateID = null;
    if(bulkType != BulkType.Unknown){
      switch (bulkType) {
      case Bulk_SMS:
        journeyTemplateID = "Bulk_SMS";
        break;

      case Bulk_Bonus:
        journeyTemplateID = "Bulk_Bonus";
        break;

      default:
        break;
      }
    }else{
      journeyTemplateID = JSONUtilities.decodeString(jsonRoot, "journeyTemplateID", true);
    }
    if(journeyTemplateID == null || journeyTemplateID.isEmpty()){
      response.put("responseCode", "missingJourneyTemplate");
      return JSONUtilities.encodeObject(response);
    }
    Journey journeyTemplate = journeyTemplateService.getActiveJourneyTemplate(journeyTemplateID, now);
    if(journeyTemplate == null){
      response.put("responseCode", "journeyTemplateNotFound");
      return JSONUtilities.encodeObject(response);
    }
    
    //
    //  get campaign parameters
    //
    
    String bulkCampaignID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (bulkCampaignID == null)
      {
        bulkCampaignID = journeyService.generateJourneyID();
        jsonRoot.put("id", bulkCampaignID);
      }
    String bulkCampaignName = JSONUtilities.decodeString(jsonRoot, "name", true);
    String bulkCampaignDisplay = JSONUtilities.decodeString(jsonRoot, "display", true);
    String bulkCampaignDescription = JSONUtilities.decodeString(jsonRoot, "description", false);
    String bulkCampaignEffectiveStartDate = JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", true);
    String bulkCampaignEffectiveEndDate = JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", true);
    JSONArray bulkCampaignTargetIDs = JSONUtilities.decodeJSONArray(jsonRoot, "targetID", true);
    JSONArray bulkCampaignBoundParameters = JSONUtilities.decodeJSONArray(jsonRoot, "boundParameters", true);
    boolean appendUCG = JSONUtilities.decodeBoolean(jsonRoot, "appendUCG", true);
    boolean appendInclusionLists = JSONUtilities.decodeBoolean(jsonRoot, "appendInclusionLists", true);
    boolean appendExclusionLists = JSONUtilities.decodeBoolean(jsonRoot, "appendExclusionLists", true);
    String userIdentifier = JSONUtilities.decodeString(jsonRoot, "userID", "");
    String userName = JSONUtilities.decodeString(jsonRoot, "userName", "");
    boolean active = JSONUtilities.decodeBoolean(jsonRoot, "active", Boolean.FALSE);
    
    /*****************************************
    *
    *  existing journey
    *
    *****************************************/

    GUIManagedObject existingBulkCampaign = journeyService.getStoredJourney(bulkCampaignID);
    existingBulkCampaign = (existingBulkCampaign != null && existingBulkCampaign.getGUIManagedObjectType() == GUIManagedObjectType.BulkCampaign) ? existingBulkCampaign : null;

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingBulkCampaign != null && existingBulkCampaign.getReadOnly())
      {
        response.put("id", existingBulkCampaign.getGUIManagedObjectID());
        response.put("accepted", existingBulkCampaign.getAccepted());
        response.put("valid", existingBulkCampaign.getAccepted());
        response.put("processing", journeyService.isActiveJourney(existingBulkCampaign, now));
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
        /*****************************************
        *
        *  templateJSONRepresentation
        *
        *****************************************/

        JSONObject templateJSONRepresentation = journeyTemplate.getJSONRepresentation();

        /*****************************************
        *
        *  generate JSON representation of the bulk campaign
        *
        *****************************************/

        Map<String,Object> campaignJSONRepresentation = new HashMap<String,Object>(templateJSONRepresentation);

        //
        //  override with bulkCampaign attributes
        //

        campaignJSONRepresentation.put("bulkType", bulkType.getExternalRepresentation());
        campaignJSONRepresentation.put("journeyTemplateID", journeyTemplateID);
        campaignJSONRepresentation.put("id", bulkCampaignID);
        campaignJSONRepresentation.put("name", bulkCampaignName);
        campaignJSONRepresentation.put("display", bulkCampaignDisplay);
        campaignJSONRepresentation.put("description", bulkCampaignDescription);
        campaignJSONRepresentation.put("effectiveStartDate", bulkCampaignEffectiveStartDate);
        campaignJSONRepresentation.put("effectiveEndDate", bulkCampaignEffectiveEndDate);
        campaignJSONRepresentation.put("targetingType", "criteria");
        campaignJSONRepresentation.put("targetID", bulkCampaignTargetIDs);
        campaignJSONRepresentation.put("boundParameters", bulkCampaignBoundParameters);
        campaignJSONRepresentation.put("appendUCG", appendUCG);
        campaignJSONRepresentation.put("appendInclusionLists", appendInclusionLists);
        campaignJSONRepresentation.put("appendExclusionLists", appendExclusionLists);
        campaignJSONRepresentation.put("userID", userIdentifier);
        campaignJSONRepresentation.put("userName", userName);
        campaignJSONRepresentation.put("active", active);

        //
        //  campaignJSON
        //

        JSONObject campaignJSON = JSONUtilities.encodeObject(campaignJSONRepresentation);

        /****************************************
        *
        *  instantiate bulk campaign
        *
        ****************************************/

        Journey bulkCampaign = new Journey(campaignJSON, GUIManagedObjectType.BulkCampaign, epoch, existingBulkCampaign, journeyService, catalogCharacteristicService, subscriberMessageTemplateService, dynamicEventDeclarationsService, communicationChannelService);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyService.putJourney(bulkCampaign, journeyObjectiveService, catalogCharacteristicService, targetService, (existingBulkCampaign == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", bulkCampaign.getJourneyID());
        response.put("accepted", bulkCampaign.getAccepted());
        response.put("valid", bulkCampaign.getAccepted());
        response.put("processing", journeyService.isActiveJourney(bulkCampaign, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, GUIManagedObjectType.BulkCampaign, epoch);

        //
        //  store
        //

        journeyService.putJourney(incompleteObject, journeyObjectiveService, catalogCharacteristicService, targetService, (existingBulkCampaign == null), userID);

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
        response.put("responseCode", "bulkCampaignNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processGetJourneyTemplateList
  *
  *****************************************/

  private JSONObject processGetJourneyTemplateList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert journeyTemplates
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> journeyTemplates = new ArrayList<JSONObject>();
    for (GUIManagedObject journeyTemplate : journeyTemplateService.getStoredJourneyTemplates(includeArchived))
      {
        switch (journeyTemplate.getGUIManagedObjectType())
          {
            case JourneyTemplate:
              journeyTemplates.add(resolveJourneyParameters(journeyTemplateService.generateResponseJSON(journeyTemplate, fullDetails, now), now));
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
    response.put("journeyTemplates", JSONUtilities.encodeArray(journeyTemplates));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetJourneyTemplate
  *
  *****************************************/

  private JSONObject processGetJourneyTemplate(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String journeyTemplateID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate journeyTemplate
    *
    *****************************************/

    GUIManagedObject journeyTemplate = journeyTemplateService.getStoredJourneyTemplate(journeyTemplateID, includeArchived);
    journeyTemplate = (journeyTemplate != null && journeyTemplate.getGUIManagedObjectType() == GUIManagedObjectType.JourneyTemplate) ? journeyTemplate : null;
    JSONObject journeyTemplateJSON = resolveJourneyParameters(journeyTemplateService.generateResponseJSON(journeyTemplate, true, now), now);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (journeyTemplate != null) ? "ok" : "journeyTemplateNotFound");
    if (journeyTemplate != null) response.put("journeyTemplate", journeyTemplateJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutJourneyTemplate
  *
  *****************************************/

  private JSONObject processPutJourneyTemplate(String userID, JSONObject jsonRoot)
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
    *  journeyTemplateID
    *
    *****************************************/

    String journeyTemplateID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (journeyTemplateID == null)
      {
        journeyTemplateID = journeyTemplateService.generateJourneyTemplateID();
        jsonRoot.put("id", journeyTemplateID);
      }

    /*****************************************
    *
    *  existing journeyTemplate
    *
    *****************************************/

    GUIManagedObject existingJourneyTemplate = journeyTemplateService.getStoredJourneyTemplate(journeyTemplateID);
    existingJourneyTemplate = (existingJourneyTemplate != null && existingJourneyTemplate.getGUIManagedObjectType() == GUIManagedObjectType.Journey) ? existingJourneyTemplate : null;

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingJourneyTemplate != null && existingJourneyTemplate.getReadOnly())
      {
        response.put("id", existingJourneyTemplate.getGUIManagedObjectID());
        response.put("accepted", existingJourneyTemplate.getAccepted());
        response.put("valid", existingJourneyTemplate.getAccepted());
        response.put("processing", journeyTemplateService.isActiveJourneyTemplate(existingJourneyTemplate, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process journeyTemplate
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate journeyTemplate
        *
        ****************************************/

        Journey journeyTemplate = new Journey(jsonRoot, GUIManagedObjectType.JourneyTemplate, epoch, existingJourneyTemplate, journeyService, catalogCharacteristicService, subscriberMessageTemplateService, dynamicEventDeclarationsService, communicationChannelService);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyTemplateService.putJourneyTemplate(journeyTemplate, journeyObjectiveService, catalogCharacteristicService, targetService, (existingJourneyTemplate == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", journeyTemplate.getJourneyID());
        response.put("accepted", journeyTemplate.getAccepted());
        response.put("valid", journeyTemplate.getAccepted());
        response.put("processing", journeyTemplateService.isActiveJourneyTemplate(journeyTemplate, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, GUIManagedObjectType.JourneyTemplate, epoch);

        //
        //  store
        //

        journeyTemplateService.putJourneyTemplate(incompleteObject, journeyObjectiveService, catalogCharacteristicService, targetService, (existingJourneyTemplate == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("journeyTemplateID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "journeyTemplateNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveJourneyTemplate
  *
  *****************************************/

  private JSONObject processRemoveJourneyTemplate(String userID, JSONObject jsonRoot)
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

    String journeyTemplateID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject journeyTemplate = journeyTemplateService.getStoredJourneyTemplate(journeyTemplateID);
    journeyTemplate = (journeyTemplate != null && journeyTemplate.getGUIManagedObjectType() == GUIManagedObjectType.JourneyTemplate) ? journeyTemplate: null;
    if (journeyTemplate != null && (force || !journeyTemplate.getReadOnly())) journeyTemplateService.removeJourneyTemplate(journeyTemplateID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (journeyTemplate != null && (force || !journeyTemplate.getReadOnly()))
      responseCode = "ok";
    else if (journeyTemplate != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "journeyTemplateNotFound";

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
  *  processGetJourneyNodeCount
  *
  *****************************************/

  private JSONObject processGetJourneyNodeCount(String userID, JSONObject jsonRoot)
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
    
    String journeyID = null;

    // 
    // We support "campaignID" as an entry for "journeyID" but both can not be filled at the same time
    //
    
    String campaignID = JSONUtilities.decodeString(jsonRoot, "campaignID", false);
    if (campaignID != null && campaignID != "")
      {
        journeyID = campaignID;
        Object previousValue = jsonRoot.put("journeyID", campaignID);
        if (previousValue != null) 
          {
            response.put("responseCode", RESTAPIGenericReturnCodes.MALFORMED_REQUEST.getGenericResponseCode());
            response.put("responseMessage", RESTAPIGenericReturnCodes.MALFORMED_REQUEST.getGenericResponseMessage() 
                + "-{both fields campaignID and journeyID must not be filled at the same time}");
            return JSONUtilities.encodeObject(response);
          }
      }
    else
      {
        journeyID = JSONUtilities.decodeString(jsonRoot, "journeyID", true);
      }
    

    /*****************************************
    *
    *  retrieve corresponding Journey & JourneyTrafficHistory
    *
    *****************************************/
    Map<String,Object> result = new HashMap<String,Object>();

    //
    // only return KPIs if the journey still exist.
    //

    GUIManagedObject journey = journeyService.getStoredJourney(journeyID);
    if (journey instanceof Journey) 
      {
        Set<String> nodeIDs = ((Journey) journey).getJourneyNodes().keySet();
        JourneyTrafficHistory journeyTrafficHistory = journeyTrafficReader.get(journeyID);
        for (String key : nodeIDs)
        {
          int subscriberCount = 0;
          
          if(journeyTrafficHistory != null)
            {
              Map<String, SubscriberTraffic> byNodeMap = journeyTrafficHistory.getCurrentData().getByNode();
              if(byNodeMap.get(key) != null)
                {
                  subscriberCount = byNodeMap.get(key).getSubscriberCount();
                }
            }
            
          result.put(key, subscriberCount);
        }
      }
    else
      {
        response.put("responseCode", RESTAPIGenericReturnCodes.CAMPAIGN_NOT_FOUND.getGenericResponseCode());
        response.put("responseMessage", RESTAPIGenericReturnCodes.CAMPAIGN_NOT_FOUND.getGenericResponseMessage() 
            + "-{could not find any journey (campaign) with the specified journeyID (campaignID)}");
        return JSONUtilities.encodeObject(response);
      }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/
    
    response.put("responseCode", RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
    response.put("responseMessage", RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
    response.put("journeyNodeCount", JSONUtilities.encodeObject(result));
    return JSONUtilities.encodeObject(response);
  }  
  
  /*****************************************
  *
  *  processGetSegmentationDimensionList
  *
  *****************************************/

  private JSONObject processGetSegmentationDimensionList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert segmentationDimensions
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> segmentationDimensions = new ArrayList<JSONObject>();
    for (GUIManagedObject segmentationDimension : segmentationDimensionService.getStoredSegmentationDimensions(includeArchived))
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

  private JSONObject processGetSegmentationDimension(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject segmentationDimension = segmentationDimensionService.getStoredSegmentationDimension(segmentationDimensionID, includeArchived);
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
    boolean resetSegmentIDs = false;
    if (segmentationDimensionID == null)
      {
        segmentationDimensionID = subscriberGroupSharedIDService.generateID();
        jsonRoot.put("id", segmentationDimensionID);
        resetSegmentIDs = true;
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
              segmentationDimension = new SegmentationDimensionEligibility(segmentationDimensionService, jsonRoot, epoch, existingSegmentationDimension, resetSegmentIDs);
              break;

            case RANGES:
              segmentationDimension = new SegmentationDimensionRanges(segmentationDimensionService, jsonRoot, epoch, existingSegmentationDimension, resetSegmentIDs);
              break;

            case FILE:
              segmentationDimension = new SegmentationDimensionFileImport(segmentationDimensionService, jsonRoot, epoch, existingSegmentationDimension, resetSegmentIDs);
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

        segmentationDimensionService.putSegmentationDimension(segmentationDimension, uploadedFileService, subscriberIDService, (existingSegmentationDimension == null), userID);

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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject segmentationDimensionUnchecked = segmentationDimensionService.getStoredSegmentationDimension(segmentationDimensionID);
    if (segmentationDimensionUnchecked != null && (force || !segmentationDimensionUnchecked.getReadOnly()))
      {
        /*****************************************
        *
        *  initialize/update subscriber group
        *
        *****************************************/

        if (segmentationDimensionUnchecked.getAccepted())
          {
            //
            //  segmentationDimension
            //

            SegmentationDimension segmentationDimension = (SegmentationDimension) segmentationDimensionUnchecked;

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

          }

        /*****************************************
        *
        *  remove
        *
        *****************************************/

        segmentationDimensionService.removeSegmentationDimension(segmentationDimensionID, userID);
      }

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
    if (segmentationDimensionUnchecked != null && (force || !segmentationDimensionUnchecked.getReadOnly()))
      responseCode = "ok";
    else if (segmentationDimensionUnchecked != null)
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
  *  processGetCountBySegmentationRanges
  *
  *****************************************/

  private JSONObject processGetCountBySegmentationRanges(String userID, JSONObject jsonRoot)
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
              segmentationDimensionRanges = new SegmentationDimensionRanges(segmentationDimensionService, jsonRoot, epochServer.getKey(), null, false);
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

    /*****************************************
    *
    *  extract BaseSplits
    *
    *****************************************/

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
        //
        // BaseSplit query creation
        //

        for(int i = 0; i < nbBaseSplits; i++)
          {
            baseSplitQueries.add(QueryBuilders.boolQuery());
          }

        for(int i = 0; i < nbBaseSplits; i++)
          {
            BoolQueryBuilder query = baseSplitQueries.get(i);
            BaseSplit baseSplit = baseSplits.get(i);

            //
            // Filter this bucket with this BaseSplit criteria
            //

            if(baseSplit.getProfileCriteria().isEmpty())
              {
                //
                // If there is not any profile criteria, just filter with a match_all query.
                //

                query = query.filter(QueryBuilders.matchAllQuery());
              }
            else
              {
                for (EvaluationCriterion evaluationCriterion : baseSplit.getProfileCriteria())
                  {
                    query = query.filter(evaluationCriterion.esQuery());
                  }
              }

            //
            //  Must_not for all following buckets (reminder : bucket must be disjointed, if not, some customer could be counted in several buckets)
            //

            for(int j = i+1; j < nbBaseSplits; j++)
              {
                BoolQueryBuilder nextQuery = baseSplitQueries.get(j);
                if(baseSplit.getProfileCriteria().isEmpty())
                  {
                    //
                    // If there is not any profile criteria, just filter with a match_all query.
                    //
                    nextQuery = nextQuery.mustNot(QueryBuilders.matchAllQuery());
                  }
                else
                  {
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

    /*****************************************
    *
    *  the main aggregation is a filter aggregation.Each filter query will constitute a bucket representing a BaseSplit.
    *
    *****************************************/

    List<KeyedFilter> queries = new ArrayList<KeyedFilter>();
    for(int i = 0; i < nbBaseSplits; i++)
      {
        BoolQueryBuilder query = baseSplitQueries.get(i);
        String bucketName = baseSplits.get(i).getSplitName(); // Warning: input must ensure that all BaseSplit names are different. ( TODO )
        queries.add(new FiltersAggregator.KeyedFilter(bucketName, query));
      }

    //
    // @DEBUG: *otherBucket* can be activated for debug purpose: .otherBucket(true).otherBucketKey("OTH_BUCK")
    //

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    AggregationBuilder aggregation = AggregationBuilders.filters(MAIN_AGG_NAME, queries.toArray(new KeyedFilter[queries.size()]));

    //
    //  Sub-aggregation query: Segments
    //    sub-aggregations corresponding to all ranges-aggregation are added to the query
    //

    for(int i = 0; i < nbBaseSplits; i++)
      {
        BaseSplit baseSplit = baseSplits.get(i);
        if(baseSplit.getVariableName() == null)
          {
            //
            // This means that it should be the default segment, ranges-aggregation does not make sense here.
            //

            QueryBuilder match_all = QueryBuilders.matchAllQuery();
            AggregationBuilder other = AggregationBuilders.filter(RANGE_AGG_PREFIX+baseSplit.getSplitName(), match_all);
            aggregation.subAggregation(other);
          }
        else
          {
            //
            // Warning: input must ensure that all BaseSplit names are different. ( TODO )
            //

            RangeAggregationBuilder range = AggregationBuilders.range(RANGE_AGG_PREFIX+baseSplit.getSplitName());

            //
            // Retrieving the ElasticSearch field from the Criterion field.
            //

            CriterionField criterionField = CriterionContext.FullProfile.getCriterionFields().get(baseSplit.getVariableName());
            if(criterionField.getESField() == null)
              {
                //
                // If this Criterion field does not correspond to any field from Deployment.json, raise an error
                //

                log.warn("Unknown criterion field {}", baseSplit.getVariableName());

                //
                //  response
                //

                response.put("responseCode", "systemError");
                response.put("responseMessage", "Unknown criterion field "+baseSplit.getVariableName()); // TODO security issue ?
                response.put("responseParameter", null);
                return JSONUtilities.encodeObject(response);
              }

            //
            //
            //

            range = range.field(criterionField.getESField());
            for(SegmentRanges segment : baseSplit.getSegments())
              {
                //
                // Warning: input must ensure that all segment names are different. ( TODO )
                //

                range = range.addRange(new Range(segment.getName(), (segment.getRangeMin() != null)? new Double (segment.getRangeMin()) : null, (segment.getRangeMax() != null)? new Double (segment.getRangeMax()) : null));
              }
            aggregation.subAggregation(range);
            //add no value aggrebation for null values for range field
            BoolQueryBuilder builder = QueryBuilders.boolQuery();
            ExistsQueryBuilder existsQueryBuilder = QueryBuilders.existsQuery(criterionField.getESField());
            builder.mustNot().add(existsQueryBuilder);
            AggregationBuilder noValueAgg = AggregationBuilders.filter(RANGE_AGG_PREFIX+baseSplit.getSplitName()+"NOVALUE",builder);
            aggregation.subAggregation(noValueAgg);
          }
      }

    searchSourceBuilder.aggregation(aggregation);

    /*****************************************
    *
    *  construct response (JSON object)
    *
    *****************************************/
//trebuie inteles aici cum se conpun array-urile pentru citirea raspunsului
    JSONObject responseJSON = new JSONObject();
    List<JSONObject> responseBaseSplits = new ArrayList<JSONObject>();
    for(int i = 0; i < nbBaseSplits; i++)
      {
        BaseSplit baseSplit = baseSplits.get(i);
        JSONObject responseBaseSplit = new JSONObject();
        List<JSONObject> responseSegments = new ArrayList<JSONObject>();
        responseBaseSplit.put("splitName", baseSplit.getSplitName());

        //
        //  ranges
        //   the "count" field will be filled with the result of the ElasticSearch query
        //

        for(SegmentRanges segment : baseSplit.getSegments())
          {
            JSONObject responseSegment = new JSONObject();
            responseSegment.put("name", segment.getName());
            responseSegments.add(responseSegment);
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
        searchResponse = elasticsearch.search(searchRequest, RequestOptions.DEFAULT);
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

    //
    //  fill response JSON object with counts for each segments from ElasticSearch result
    //

    for(JSONObject responseBaseSplit : responseBaseSplits)
      {
        Filters.Bucket bucket = mainAggregationResult.getBucketByKey((String) responseBaseSplit.get("splitName"));
        ParsedAggregation segmentAggregationResult = bucket.getAggregations().get(RANGE_AGG_PREFIX+bucket.getKeyAsString());
        ParsedAggregation noValueAggregationResult = bucket.getAggregations().get(RANGE_AGG_PREFIX+bucket.getKeyAsString()+"NOVALUE");
        if (segmentAggregationResult instanceof ParsedFilter)
          {
            //
            // This specific segment aggregation is corresponding to the "default" BaseSplit (without any variableName)
            //

            ParsedFilter other = (ParsedFilter) segmentAggregationResult;

            //
            //  fill the "count" field of the response JSON object (for each segments)
            List<JSONObject> responseSegments = (List<JSONObject>)responseBaseSplit.get("segments");
            for(JSONObject responseSegment : responseSegments)
              {
                responseSegment.put("count", other.getDocCount());
              }
            JSONObject noValueSegment = new JSONObject();
            noValueSegment.put("name", "no value");
            noValueSegment.put("count",((ParsedFilter)noValueAggregationResult).getDocCount());
            responseSegments.add(noValueSegment);
          }
        else
          {
            //
            // Segment aggregation is a range-aggregation.
            //

            ParsedRange ranges = (ParsedRange) segmentAggregationResult;
            List<ParsedRange.ParsedBucket> segmentBuckets = (List<ParsedRange.ParsedBucket>) ranges.getBuckets();

            //
            // bucketMap is an hash map for caching purpose
            //

            Map<String, ParsedRange.ParsedBucket> bucketMap = new HashMap<>(segmentBuckets.size());
            for (ParsedRange.ParsedBucket segmentBucket : segmentBuckets)
              {
                bucketMap.put(segmentBucket.getKey(), segmentBucket);
              }

            //
            //  fill the "count" field of the response JSON object (for each segments)
            //
            List<JSONObject> responseSegments = (List<JSONObject>)responseBaseSplit.get("segments");
            for(JSONObject responseSegment : responseSegments)
              {
                responseSegment.put("count", bucketMap.get(responseSegment.get("name")).getDocCount());
              }
            //read no value aggregation values and add for each segment
            JSONObject noValueSegment = new JSONObject();
            noValueSegment.put("name", "no available values");
            noValueSegment.put("count",((ParsedFilter)noValueAggregationResult).getDocCount());
            responseSegments.add(noValueSegment);
          }
      }
    responseJSON.put("baseSplit", responseBaseSplits);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("result", responseJSON);
    response.put("responseCode", "ok");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetCountBySegmentationEligibility
  *
  *****************************************/

  private JSONObject processGetCountBySegmentationEligibility(String userID,JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  populate segmentationDimensionID with "nothing"
    *
    *****************************************/

    jsonRoot.put("id", "(not used)"); 

    /*****************************************
    *
    *  validate targetingType
    *
    *****************************************/

    SegmentationDimensionTargetingType targetingType = SegmentationDimensionTargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", true));
    if(targetingType != SegmentationDimensionTargetingType.ELIGIBILITY)
      {
        //
        //  log
        //

        log.warn("Invalid dimension targeting type for processGetCountBySegmentationEligibility. Targeting type: {}",targetingType.getExternalRepresentation());

        //
        //  response
        //

        response.put("responseCode", "segmentationDimensionNotValid");
        response.put("responseMessage", "Segmentation dimension not ELIGIBILITY");
        response.put("responseParameter", null);
        return JSONUtilities.encodeObject(response);
      }

    /****************************************
    *
    *  response
    *
    ****************************************/

    List<JSONObject> aggregationResult = new ArrayList<>();
    List<QueryBuilder> processedQueries = new ArrayList<>();
    try
      {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).query(QueryBuilders.matchAllQuery()).size(0);
        List<FiltersAggregator.KeyedFilter> aggFilters = new ArrayList<>();
        SegmentationDimensionEligibility segmentationDimensionEligibility = new SegmentationDimensionEligibility(segmentationDimensionService, jsonRoot, epochServer.getKey(), null, false);
        for(SegmentEligibility segmentEligibility :segmentationDimensionEligibility.getSegments())
          {
            BoolQueryBuilder query = QueryBuilders.boolQuery();
            for(QueryBuilder processedQuery : processedQueries)
              {
                query = query.mustNot(processedQuery);
              }
            BoolQueryBuilder segmentQuery = QueryBuilders.boolQuery();
            for(EvaluationCriterion evaluationCriterion:segmentEligibility.getProfileCriteria())
              {
                segmentQuery = segmentQuery.filter(evaluationCriterion.esQuery());
                processedQueries.add(segmentQuery);
              }
            query = query.filter(segmentQuery);
            //use name as key, even if normally should use id, to make simpler to use this count in chart
            aggFilters.add(new FiltersAggregator.KeyedFilter(segmentEligibility.getName(),query));
          }
        AggregationBuilder aggregation = null;
        FiltersAggregator.KeyedFilter [] filterArray = new FiltersAggregator.KeyedFilter [aggFilters.size()];
        filterArray = aggFilters.toArray(filterArray);
        aggregation = AggregationBuilders.filters("SegmentEligibility",filterArray);
        ((FiltersAggregationBuilder) aggregation).otherBucket(true);
        ((FiltersAggregationBuilder) aggregation).otherBucketKey("other_key");
        searchSourceBuilder.aggregation(aggregation);

        //
        //  search in ES
        //

        SearchRequest searchRequest = new SearchRequest("subscriberprofile").source(searchSourceBuilder);
        SearchResponse searchResponse = elasticsearch.search(searchRequest, RequestOptions.DEFAULT);
        Filters aggResultFilters = searchResponse.getAggregations().get("SegmentEligibility");
        for (Filters.Bucket entry : aggResultFilters.getBuckets())
          {
            HashMap<String,Object> aggItem = new HashMap<String,Object>();
            String key = entry.getKeyAsString();            // bucket key
            long docCount = entry.getDocCount();            // Doc count
            aggItem.put("name",key);
            aggItem.put("count",docCount);
            aggregationResult.add(JSONUtilities.encodeObject(aggItem));
          }
      }
    catch(Exception ex)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        ex.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("responseCode", "systemError");
        response.put("responseMessage", ex.getMessage());
        response.put("responseParameter", (ex instanceof GUIManagerException) ? ((GUIManagerException) ex).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
    response.put("responseCode", "ok");
    response.put("result",aggregationResult);
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
            criteriaList.add(new EvaluationCriterion((JSONObject) jsonCriteriaList.get(i), CriterionContext.FullDynamicProfile));
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

    Boolean returnQuery = JSONUtilities.decodeBoolean(jsonRoot, "returnQuery", Boolean.FALSE);

    /*****************************************
    *
    *  construct query
    *
    *****************************************/

    BoolQueryBuilder query = null;
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
    *  execute query
    *
    *****************************************/

    long result;
    try
      {
        SearchRequest searchRequest = new SearchRequest("subscriberprofile").source(new SearchSourceBuilder().sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).query(query).size(0));
        SearchResponse searchResponse = elasticsearch.search(searchRequest, RequestOptions.DEFAULT);
        result = searchResponse.getHits().getTotalHits().value;
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

    response.put("responseCode", "ok");
    response.put("result", result);
    if (returnQuery && (query != null))
      {
        try
          {
            JSONObject queryJSON = (JSONObject) (new JSONParser()).parse(query.toString());
            response.put("query", JSONUtilities.encodeObject(queryJSON));
          }
        catch (ParseException e)
          {
            log.debug("Cannot parse query string {} : {}", query.toString(), e.getLocalizedMessage());
          }
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetUCGDimensionList
  *
  *****************************************/

  private JSONObject processGetUCGDimensionList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert segmentationDimensions
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> segmentationDimensions = new ArrayList<JSONObject>();
    for (GUIManagedObject segmentationDimension : segmentationDimensionService.getStoredSegmentationDimensions(includeArchived))
      {
        SegmentationDimension dimension = (SegmentationDimension) segmentationDimension;
        if (dimension.getDefaultSegmentID() != null)
          {
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

  private JSONObject processGetPointList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert Points
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> points = new ArrayList<JSONObject>();
    for (GUIManagedObject point : pointService.getStoredPoints(includeArchived))
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

  private JSONObject processGetPoint(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject point = pointService.getStoredPoint(pointID, includeArchived);
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
        *  add dynamic criterion fields)
        *
        *****************************************/

        dynamicCriterionFieldService.addPointCriterionFields(point, (existingPoint == null));

        /*****************************************
        *
        *  create related deliverable and related paymentMean
        *
        *****************************************/

        DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("pointFulfillment");
        JSONObject deliveryManagerJSON = (deliveryManager != null) ? deliveryManager.getJSONRepresentation() : null;
        String providerID = (deliveryManagerJSON != null) ? (String) deliveryManagerJSON.get("providerID") : null;

        //
        // deliverable
        //

        if (providerID != null && point.getCreditable())
          {
            Map<String, Object> deliverableMap = new HashMap<String, Object>();
            deliverableMap.put("id", "point-" + point.getPointID());
            deliverableMap.put("fulfillmentProviderID", providerID);
            deliverableMap.put("externalAccountID", point.getPointID());
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

        if (providerID != null && point.getDebitable())
          {
            Map<String, Object> paymentMeanMap = new HashMap<String, Object>();
            paymentMeanMap.put("id", "point-" + point.getPointID());
            paymentMeanMap.put("fulfillmentProviderID", providerID);
            paymentMeanMap.put("externalAccountID", point.getPointID());
            paymentMeanMap.put("name", point.getPointName());
            paymentMeanMap.put("display", point.getDisplay());
            paymentMeanMap.put("active", true);
            PaymentMean paymentMean = new PaymentMean(JSONUtilities.encodeObject(paymentMeanMap), epoch, null);
            paymentMeanService.putPaymentMean(paymentMean, true, userID);
          }

        /*****************************************
        *
        *  revalidate
        *
        *****************************************/

        revalidateSubscriberMessageTemplates(now);
        revalidateTargets(now);
        revalidateJourneys(now);

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

    String pointID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove related deliverable and related paymentMean
    *
    *****************************************/

    DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("pointFulfillment");
    JSONObject deliveryManagerJSON = (deliveryManager != null) ? deliveryManager.getJSONRepresentation() : null;
    String providerID = (deliveryManagerJSON != null) ? (String) deliveryManagerJSON.get("providerID") : null;
    if (providerID != null)
      {
        //
        //  deliverable
        //

        Collection<GUIManagedObject> deliverableObjects = deliverableService.getStoredDeliverables();
        for (GUIManagedObject deliverableObject : deliverableObjects)
          {
            if(deliverableObject instanceof Deliverable)
              {
                Deliverable deliverable = (Deliverable) deliverableObject;
                if (deliverable.getFulfillmentProviderID().equals(providerID) && deliverable.getExternalAccountID().equals(pointID))
                  {
                    deliverableService.removeDeliverable(deliverable.getDeliverableID(), "0");
                  }
              }
          }

        //
        //  paymentMean
        //

        Collection<GUIManagedObject> paymentMeanObjects = paymentMeanService.getStoredPaymentMeans();
        for(GUIManagedObject paymentMeanObject : paymentMeanObjects)
          {
            if(paymentMeanObject instanceof PaymentMean)
              {
                PaymentMean paymentMean = (PaymentMean) paymentMeanObject;
                if(paymentMean.getFulfillmentProviderID().equals(providerID) && paymentMean.getExternalAccountID().equals(pointID))
                  {
                    paymentMeanService.removePaymentMean(paymentMean.getPaymentMeanID(), "0");
                  }
              }
          }
      }

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject point = pointService.getStoredPoint(pointID);
    if (point != null && (force || !point.getReadOnly()))
      {
        //
        //  remove point
        //

        pointService.removePoint(pointID, userID);

        //
        //  remove dynamic criterion fields
        //

        dynamicCriterionFieldService.removePointCriterionFields(point);

        //
        //  revalidate
        //

        revalidateSubscriberMessageTemplates(now);
        revalidateTargets(now);
        revalidateJourneys(now);
      }

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (point != null && (force || !point.getReadOnly()))
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

  private JSONObject processGetOfferList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert offers
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> offers = new ArrayList<JSONObject>();
    for (GUIManagedObject offer : offerService.getStoredOffers(includeArchived))
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

  private JSONObject processGetOffer(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject offer = offerService.getStoredOffer(offerID, includeArchived);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject offer = offerService.getStoredOffer(offerID);
    if (offer != null && (force || !offer.getReadOnly())) offerService.removeOffer(offerID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (offer != null && (force || !offer.getReadOnly()))
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
  *  processGetReportGlobalConfiguration
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

  private JSONObject processGetReportList(String userID, JSONObject jsonRoot, boolean includeArchived)
  {
    log.trace("In processGetReportList : "+jsonRoot);
    Date now = SystemTime.getCurrentTime();
    List<JSONObject> reports = new ArrayList<JSONObject>();
    for (GUIManagedObject report : reportService.getStoredReports(includeArchived))
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
  *  processGetPresentationStrategyList
  *
  *****************************************/

  private JSONObject processGetPresentationStrategyList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert presentationStrategies
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> presentationStrategies = new ArrayList<JSONObject>();
    for (GUIManagedObject presentationStrategy : presentationStrategyService.getStoredPresentationStrategies(includeArchived))
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

  private JSONObject processGetPresentationStrategy(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject presentationStrategy = presentationStrategyService.getStoredPresentationStrategy(presentationStrategyID, includeArchived);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject presentationStrategy = presentationStrategyService.getStoredPresentationStrategy(presentationStrategyID);
    if (presentationStrategy != null && (force || !presentationStrategy.getReadOnly())) presentationStrategyService.removePresentationStrategy(presentationStrategyID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (presentationStrategy != null && (force || !presentationStrategy.getReadOnly()))
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
  *  processGetDNBOMatrixList
  *
  *****************************************/

  private JSONObject processGetDNBOMatrixList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert DNBOMatrixList
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> dnboMatrixes = new ArrayList<JSONObject>();
    for (GUIManagedObject dnboMatrix : dnboMatrixService.getStoredDNBOMatrixes(includeArchived))
      {
        dnboMatrixes.add(presentationStrategyService.generateResponseJSON(dnboMatrix, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("dnboMatrixes", JSONUtilities.encodeArray(dnboMatrixes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetDNBOMatrix
  *
  *****************************************/

  private JSONObject processGetDNBOMatrix(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String dnboMatrixID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate DNBOMatrix
    *
    *****************************************/

    GUIManagedObject dnboMatrix = dnboMatrixService.getStoredDNBOMatrix(dnboMatrixID, includeArchived);
    JSONObject dnboMatrixJSON = dnboMatrixService.generateResponseJSON(dnboMatrix, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (dnboMatrix != null) ? "ok" : "dnboMatrixNotFound");
    if (dnboMatrix != null) response.put("dnboMatrix", dnboMatrixJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutDNBOMatrix
  *
  *****************************************/

  private JSONObject processPutDNBOMatrix(String userID, JSONObject jsonRoot)
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
    *  dnboMatrixID
    *
    *****************************************/

    String dnboMatrixID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (dnboMatrixID == null)
      {
        dnboMatrixID = dnboMatrixService.generateDNBOMatrixID();
        jsonRoot.put("id", dnboMatrixID);
      }

    /*****************************************
    *
    *  existing dnboMatrix
    *
    *****************************************/

    GUIManagedObject existingDNBOMatrix = dnboMatrixService.getStoredDNBOMatrix(dnboMatrixID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingDNBOMatrix != null && existingDNBOMatrix.getReadOnly())
      {
        response.put("id", existingDNBOMatrix.getGUIManagedObjectID());
        response.put("accepted", existingDNBOMatrix.getAccepted());
        response.put("valid", existingDNBOMatrix.getAccepted());
        response.put("processing", dnboMatrixService.isActiveDNBOMatrix(existingDNBOMatrix, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process dnboMatrix
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate DNBOMatrix
        *
        ****************************************/

        DNBOMatrix dnboMatrix = new DNBOMatrix(jsonRoot, epoch, existingDNBOMatrix);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        dnboMatrixService.putDNBOMatrix(dnboMatrix, (existingDNBOMatrix == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", dnboMatrix.getGUIManagedObjectID());
        response.put("accepted", dnboMatrix.getAccepted());
        response.put("valid", dnboMatrix.getAccepted());
        response.put("processing", dnboMatrixService.isActiveDNBOMatrix(dnboMatrix, now));
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

        dnboMatrixService.putDNBOMatrix(incompleteObject, (existingDNBOMatrix == null), userID);

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
        response.put("responseCode", "dnboMatrixNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveDNBOMatrix
  *
  *****************************************/

  private JSONObject processRemoveDNBOMatrix(String userID, JSONObject jsonRoot)
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

    String dnboMatrixID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject dnboMatrix = dnboMatrixService.getStoredDNBOMatrix(dnboMatrixID);
    if (dnboMatrix != null && (force || !dnboMatrix.getReadOnly())) dnboMatrixService.removeDNBOMatrix(dnboMatrixID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (dnboMatrix != null && (force || !dnboMatrix.getReadOnly()))
      responseCode = "ok";
    else if (dnboMatrix != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "dnboMatrixNotFound";

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

  private JSONObject processGetScoringStrategyList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert scoringStrategies
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> scoringStrategies = new ArrayList<JSONObject>();
    for (GUIManagedObject scoringStrategy : scoringStrategyService.getStoredScoringStrategies(includeArchived))
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

  private JSONObject processGetScoringStrategy(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject scoringStrategy = scoringStrategyService.getStoredScoringStrategy(scoringStrategyID, includeArchived);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject scoringStrategy = scoringStrategyService.getStoredScoringStrategy(scoringStrategyID);
    if (scoringStrategy != null && (force || !scoringStrategy.getReadOnly())) scoringStrategyService.removeScoringStrategy(scoringStrategyID, userID);

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
    if (scoringStrategy != null && (force || !scoringStrategy.getReadOnly()))
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

  private JSONObject processGetCallingChannelList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert callingChannels
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> callingChannels = new ArrayList<JSONObject>();
    for (GUIManagedObject callingChannel : callingChannelService.getStoredCallingChannels(includeArchived))
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

  private JSONObject processGetCallingChannel(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject callingChannel = callingChannelService.getStoredCallingChannel(callingChannelID, includeArchived);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject callingChannel = callingChannelService.getStoredCallingChannel(callingChannelID);
    if (callingChannel != null && (force || !callingChannel.getReadOnly()) )callingChannelService.removeCallingChannel(callingChannelID, userID);

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
    if (callingChannel != null && (force || !callingChannel.getReadOnly()))
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
  
  /*********************************************
  *
  *  processGetCriterionFieldAvailableValuesList
  *
  *********************************************/

  private JSONObject processGetCriterionFieldAvailableValuesList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert CriterionFieldAvailableValues
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> criterionFieldAvailableValuesList = new ArrayList<JSONObject>();
    for (GUIManagedObject criterionFieldAvailableValues : criterionFieldAvailableValuesService.getStoredCriterionFieldAvailableValuesList(includeArchived))
      {
        criterionFieldAvailableValuesList.add(criterionFieldAvailableValuesService.generateResponseJSON(criterionFieldAvailableValues, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("criterionFieldAvailableValues", JSONUtilities.encodeArray(criterionFieldAvailableValuesList));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processRemoveCriterionFieldAvailableValues
  *
  *****************************************/

  private JSONObject processRemoveCriterionFieldAvailableValues(String userID, JSONObject jsonRoot)
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

    String criterionFieldAvailableValuesID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject criterionFieldAvailableValues = criterionFieldAvailableValuesService.getStoredCriterionFieldAvailableValues(criterionFieldAvailableValuesID);
    if (criterionFieldAvailableValues != null && (force || !criterionFieldAvailableValues.getReadOnly())) criterionFieldAvailableValuesService.removeCriterionFieldAvailableValues(criterionFieldAvailableValuesID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (criterionFieldAvailableValues != null && (force || !criterionFieldAvailableValues.getReadOnly()))
      responseCode = "ok";
    else if (criterionFieldAvailableValues != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "criterionFieldAvailableValuesNotFound";

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
  *  processPutCriterionFieldAvailableValues
  *
  *****************************************/

  private JSONObject processPutCriterionFieldAvailableValues(String userID, JSONObject jsonRoot)
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
    *  CriterionFieldAvailableValuesID
    *
    *****************************************/

    String criterionFieldAvailableValuesID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (criterionFieldAvailableValuesID == null)
      {
        criterionFieldAvailableValuesID = communicationChannelBlackoutService.generateCommunicationChannelBlackoutID();
        jsonRoot.put("id", criterionFieldAvailableValuesID);
      }

    /*****************************************
    *
    *  existing CriterionFieldAvailableValues
    *
    *****************************************/

    GUIManagedObject existingCriterionFieldAvailableValues = criterionFieldAvailableValuesService.getStoredCriterionFieldAvailableValues(criterionFieldAvailableValuesID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingCriterionFieldAvailableValues != null && existingCriterionFieldAvailableValues.getReadOnly())
      {
        response.put("id", existingCriterionFieldAvailableValues.getGUIManagedObjectID());
        response.put("accepted", existingCriterionFieldAvailableValues.getAccepted());
        response.put("valid", existingCriterionFieldAvailableValues.getAccepted());
        response.put("processing", criterionFieldAvailableValuesService.isActiveCriterionFieldAvailableValues(existingCriterionFieldAvailableValues, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process CriterionFieldAvailableValues
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate CriterionFieldAvailableValues
        *
        ****************************************/

        CriterionFieldAvailableValues criterionFieldAvailableValues = new CriterionFieldAvailableValues(jsonRoot, epoch, existingCriterionFieldAvailableValues);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        criterionFieldAvailableValuesService.putCriterionFieldAvailableValues(criterionFieldAvailableValues, (existingCriterionFieldAvailableValues == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", criterionFieldAvailableValues.getGUIManagedObjectID());
        response.put("accepted", criterionFieldAvailableValues.getAccepted());
        response.put("valid", criterionFieldAvailableValues.getAccepted());
        response.put("processing", criterionFieldAvailableValuesService.isActiveCriterionFieldAvailableValues(criterionFieldAvailableValues, now));
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

        criterionFieldAvailableValuesService.putCriterionFieldAvailableValues(incompleteObject, (existingCriterionFieldAvailableValues == null), userID);

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
        response.put("responseCode", "criterionFieldAvailableValuesNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  *  processGetCriterionFieldAvailableValues
  *
  *****************************************/

  private JSONObject processGetCriterionFieldAvailableValues(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String criterionFieldAvailableValuesID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject criterionFieldAvailableValues = salesChannelService.getStoredSalesChannel(criterionFieldAvailableValuesID, includeArchived);
    JSONObject criterionFieldAvailableValuesJSON = criterionFieldAvailableValuesService.generateResponseJSON(criterionFieldAvailableValues, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (criterionFieldAvailableValues != null) ? "ok" : "criterionFieldAvailableValuesNotFound");
    if (criterionFieldAvailableValues != null) response.put("criterionFieldAvailableValues", criterionFieldAvailableValuesJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getEffectiveSystemTime
  *
  *****************************************/

  private JSONObject processGetEffectiveSystemTime(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("effectiveSystemTime", SystemTime.getCurrentTime());
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetSalesChannelList
  *
  *****************************************/

  private JSONObject processGetSalesChannelList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert salesChannels
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> salesChannels = new ArrayList<JSONObject>(); 
    HashMap<String,Object> response = new HashMap<String,Object>(); 
    
    for (GUIManagedObject salesChannel : salesChannelService.getStoredSalesChannels(includeArchived))
      {       
        JSONObject salesChannelJSON = salesChannelService.generateResponseJSON(salesChannel, fullDetails, now);       
        
        
        /*****************************************
        *
        *  To display resellers in the summary list
        *
        *****************************************/
        
        if (!fullDetails) {
          if (salesChannel.getJSONRepresentation().get("resellerIDs")!= null) {
           salesChannelJSON.put("resellerIDs", salesChannel.getJSONRepresentation().get("resellerIDs"));
          }
          else {
            salesChannelJSON.put("resellerIDs", new ArrayList<>());
          } 
          
        }
        
        salesChannels.add(salesChannelJSON);
        
      }    
    
    

    /*****************************************
    *
    *  response
    *
    *****************************************/
    
    
    response.put("ResponseCode", "ok" );    
    response.put("salesChannels", JSONUtilities.encodeArray(salesChannels));    
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetSalesChannel
  *
  *****************************************/

  private JSONObject processGetSalesChannel(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject salesChannel = salesChannelService.getStoredSalesChannel(salesChannelID, includeArchived);
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

        salesChannelService.putSalesChannel(salesChannel, callingChannelService, resellerService, (existingSalesChannel == null), userID);

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

        salesChannelService.putSalesChannel(incompleteObject, callingChannelService, resellerService, (existingSalesChannel == null), userID);

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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject salesChannel = salesChannelService.getStoredSalesChannel(salesChannelID);
    if (salesChannel != null && (force || !salesChannel.getReadOnly())) salesChannelService.removeSalesChannel(salesChannelID, userID);

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
    if (salesChannel != null && (force || !salesChannel.getReadOnly()))
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

  private JSONObject processGetSupplierList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert suppliers
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> suppliers = new ArrayList<JSONObject>();
    for (GUIManagedObject supplier : supplierService.getStoredSuppliers(includeArchived))
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

  private JSONObject processGetSupplier(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject supplier = supplierService.getStoredSupplier(supplierID, includeArchived);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject supplier = supplierService.getStoredSupplier(supplierID);
    if (supplier != null && (force || !supplier.getReadOnly())) supplierService.removeSupplier(supplierID, userID);

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
    if (supplier != null && (force || !supplier.getReadOnly()))
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

  private JSONObject processGetProductList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert products
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> products = new ArrayList<JSONObject>();
    for (GUIManagedObject product : productService.getStoredProducts(includeArchived))
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

  private JSONObject processGetProduct(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject product = productService.getStoredProduct(productID, includeArchived);
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

        Product product = new Product(jsonRoot, epoch, existingProduct, deliverableService, catalogCharacteristicService);

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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject product = productService.getStoredProduct(productID);
    if (product != null && (force || !product.getReadOnly())) productService.removeProduct(productID, userID);

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
    if (product != null && (force || !product.getReadOnly()))
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

  private JSONObject processGetCatalogCharacteristicList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert catalogCharacteristics
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> catalogCharacteristics = new ArrayList<JSONObject>();
    for (GUIManagedObject catalogCharacteristic : catalogCharacteristicService.getStoredCatalogCharacteristics(includeArchived))
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

  private JSONObject processGetCatalogCharacteristic(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject catalogCharacteristic = catalogCharacteristicService.getStoredCatalogCharacteristic(catalogCharacteristicID, includeArchived);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject catalogCharacteristic = catalogCharacteristicService.getStoredCatalogCharacteristic(catalogCharacteristicID);
    if (catalogCharacteristic != null && (force || !catalogCharacteristic.getReadOnly())) catalogCharacteristicService.removeCatalogCharacteristic(catalogCharacteristicID, userID);

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
    if (catalogCharacteristic != null && (force || !catalogCharacteristic.getReadOnly()))
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

  private JSONObject processGetContactPolicyList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert contactPolicies
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> contactPolicies = new ArrayList<JSONObject>();
    for (GUIManagedObject contactPolicy : contactPolicyService.getStoredContactPolicies(includeArchived))
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

  private JSONObject processGetContactPolicy(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject contactPolicy = contactPolicyService.getStoredContactPolicy(contactPolicyID, includeArchived);
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

        contactPolicyService.putContactPolicy(contactPolicy, communicationChannelService, (existingContactPolicy == null), userID);

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

        contactPolicyService.putContactPolicy(incompleteObject, communicationChannelService, (existingContactPolicy == null), userID);

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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject contactPolicy = contactPolicyService.getStoredContactPolicy(contactPolicyID);
    if (contactPolicy != null && (force || !contactPolicy.getReadOnly())) contactPolicyService.removeContactPolicy(contactPolicyID, userID);

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
    if (contactPolicy != null && (force || !contactPolicy.getReadOnly()))
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

  private JSONObject processGetJourneyObjectiveList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert journeyObjectives
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> journeyObjectives = new ArrayList<JSONObject>();
    for (GUIManagedObject journeyObjective : journeyObjectiveService.getStoredJourneyObjectives(includeArchived))
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

  private JSONObject processGetJourneyObjective(String userID, JSONObject jsonRoot, boolean includeArchived)
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
    GUIManagedObject journeyObjective = journeyObjectiveService.getStoredJourneyObjective(journeyObjectiveID, includeArchived);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject journeyObjective = journeyObjectiveService.getStoredJourneyObjective(journeyObjectiveID);
    if (journeyObjective != null && (force || !journeyObjective.getReadOnly())) journeyObjectiveService.removeJourneyObjective(journeyObjectiveID, userID);

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
    if (journeyObjective != null && (force || !journeyObjective.getReadOnly()))
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

  private JSONObject processGetOfferObjectiveList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert offerObjectives
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> offerObjectives = new ArrayList<JSONObject>();
    for (GUIManagedObject offerObjective : offerObjectiveService.getStoredOfferObjectives(includeArchived))
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

  private JSONObject processGetOfferObjective(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject offerObjective = offerObjectiveService.getStoredOfferObjective(offerObjectiveID, includeArchived);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject offerObjective = offerObjectiveService.getStoredOfferObjective(offerObjectiveID);
    if (offerObjective != null && (force || !offerObjective.getReadOnly())) offerObjectiveService.removeOfferObjective(offerObjectiveID, userID);

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
    if (offerObjective != null && (force || !offerObjective.getReadOnly()))
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

  private JSONObject processGetProductTypeList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert productTypes
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> productTypes = new ArrayList<JSONObject>();
    for (GUIManagedObject productType : productTypeService.getStoredProductTypes(includeArchived))
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
  *  processGetProductType
  *
  *****************************************/

  private JSONObject processGetProductType(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject productType = productTypeService.getStoredProductType(productTypeID, includeArchived);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject productType = productTypeService.getStoredProductType(productTypeID);
    if (productType != null && (force || !productType.getReadOnly())) productTypeService.removeProductType(productTypeID, userID);

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
    if (productType != null && (force || !productType.getReadOnly()))
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

  private JSONObject processGetUCGRuleList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert ucg rules
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> ucgRules = new ArrayList<JSONObject>();
    for (GUIManagedObject ucgRule : ucgRuleService.getStoredUCGRules(includeArchived))
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

  private JSONObject processGetUCGRule(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject ucgRule = ucgRuleService.getStoredUCGRule(ucgRuleID, includeArchived);
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
        *  deactivate any active rules except current one active
        *  this replace one UCGRule active sequence from UCGRule.validate()
        *
        *****************************************/

        if(ucgRule.getActive()) {
          deactivateOtherUCGRules(ucgRule, now);
        }

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
  *  processRemoveUCGRule
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject ucgRule = ucgRuleService.getStoredUCGRule(ucgRuleID);
    if (ucgRule != null && (force || !ucgRule.getReadOnly())) ucgRuleService.removeUCGRule(ucgRuleID, userID);


    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (ucgRule != null && (force || !ucgRule.getReadOnly()))
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
  
  /*********************************************
  *
  *  processGetTenantList
  *
  *********************************************/

  private JSONObject processGetTenantList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert Tenants
    *
    *****************************************/
    Date now = SystemTime.getCurrentTime();
    List<JSONObject> tenantList = new ArrayList<JSONObject>();

    // TODO move this to be a regular GUIManagedObject
    {
      JSONObject result = new JSONObject();
      result.put("id", "0");
      result.put("name", "global");
      result.put("description", "Global");
      result.put("display", "Global");
      result.put("isDefault", false);
      result.put("language", "1");
      result.put("active", true);
      result.put("readOnly", true);
      tenantList.add(result);
    }
    {
      JSONObject result = new JSONObject();
      result.put("id", "1");
      result.put("name", "default");
      result.put("description", "Default");
      result.put("display", "Default");
      result.put("isDefault", true);
      result.put("language", "1");
      result.put("active", true);
      result.put("readOnly", true);
      tenantList.add(result);
    }
    
    /*****************************************
    *
    *  response
    *
    *****************************************/
    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("tenants", JSONUtilities.encodeArray(tenantList));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetDeliverableList
  *
  *****************************************/

  private JSONObject processGetDeliverableList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert deliverables
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> deliverables = new ArrayList<JSONObject>();
    for (GUIManagedObject deliverable : deliverableService.getStoredDeliverables(includeArchived))
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

  private JSONObject processGetDeliverable(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject deliverable = deliverableService.getStoredDeliverable(deliverableID, includeArchived);
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

  private JSONObject processGetDeliverableByName(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject deliverable = deliverableService.getStoredDeliverableByName(deliverableName, includeArchived);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject deliverable = deliverableService.getStoredDeliverable(deliverableID);
    if (deliverable != null && (force || !deliverable.getReadOnly())) deliverableService.removeDeliverable(deliverableID, userID);

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
    if (deliverable != null && (force || !deliverable.getReadOnly()))
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

  private JSONObject processGetTokenTypeList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert tokenTypes
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> tokenTypes = new ArrayList<JSONObject>();
    for (GUIManagedObject tokenType : tokenTypeService.getStoredTokenTypes(includeArchived))
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

  private JSONObject processGetTokenType(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject tokenType = tokenTypeService.getStoredTokenType(tokenTypeID, includeArchived);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject tokenType = tokenTypeService.getStoredTokenType(tokenTypeID);
    if (tokenType != null && (force || !tokenType.getReadOnly())) tokenTypeService.removeTokenType(tokenTypeID, userID);

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
    if (tokenType != null && (force || !tokenType.getReadOnly()))
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
  *  processGetVoucherTypeList
  *
  *****************************************/

  private JSONObject processGetVoucherTypeList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert voucherTypes
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> voucherTypes = new ArrayList<JSONObject>();
    for (GUIManagedObject voucherType : voucherTypeService.getStoredVoucherTypes(includeArchived))
      {
        voucherTypes.add(voucherTypeService.generateResponseJSON(voucherType, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("voucherTypes", JSONUtilities.encodeArray(voucherTypes));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetVoucherType
  *
  *****************************************/

  private JSONObject processGetVoucherType(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String voucherTypeID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate voucherType
    *
    *****************************************/

    GUIManagedObject voucherType = voucherTypeService.getStoredVoucherType(voucherTypeID, includeArchived);
    JSONObject voucherTypeJSON = voucherTypeService.generateResponseJSON(voucherType, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (voucherType != null) ? "ok" : "voucherTypeNotFound");
    if (voucherType != null) response.put("voucherType", voucherTypeJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutVoucherType
  *
  *****************************************/

  private JSONObject processPutVoucherType(String userID, JSONObject jsonRoot)
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
    *  voucherTypeID
    *
    *****************************************/

    String voucherTypeID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (voucherTypeID == null)
      {
        voucherTypeID = voucherTypeService.generateVoucherTypeID();
        jsonRoot.put("id", voucherTypeID);
      }

    /*****************************************
    *
    *  existing voucherType
    *
    *****************************************/

    GUIManagedObject existingVoucherType = voucherTypeService.getStoredVoucherType(voucherTypeID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingVoucherType != null && existingVoucherType.getReadOnly())
      {
        response.put("id", existingVoucherType.getGUIManagedObjectID());
        response.put("accepted", existingVoucherType.getAccepted());
        response.put("valid", existingVoucherType.getAccepted());
        response.put("processing", voucherTypeService.isActiveVoucherType(existingVoucherType, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process voucherType
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate voucherType
        *
        ****************************************/

        VoucherType voucherType = new VoucherType(jsonRoot, epoch, existingVoucherType);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        voucherTypeService.putVoucherType(voucherType, (existingVoucherType == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", voucherType.getVoucherTypeID());
        response.put("accepted", voucherType.getAccepted());
        response.put("valid", voucherType.getAccepted());
        response.put("processing", voucherTypeService.isActiveVoucherType(voucherType, now));
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

        voucherTypeService.putVoucherType(incompleteObject, (existingVoucherType == null), userID);

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
        response.put("responseCode", "voucherTypeNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveVoucherType
  *
  *****************************************/

  private JSONObject processRemoveVoucherType(String userID, JSONObject jsonRoot)
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

    String voucherTypeID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject voucherType = voucherTypeService.getStoredVoucherType(voucherTypeID);
    if (voucherType != null && (force || !voucherType.getReadOnly())) voucherTypeService.removeVoucherType(voucherTypeID, userID);

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
    if (voucherType != null && (force || !voucherType.getReadOnly()))
      responseCode = "ok";
    else if (voucherType != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "voucherTypeNotFound";

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
  *  processGetVoucherCodeFormatList
  *
  *****************************************/

  private JSONObject processGetVoucherCodeFormatList(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve vouchers code format list
    *
    *****************************************/

    JSONArray voucherCodeFormatJSONArray = Deployment.getInitialVoucherCodeFormatsJSONArray();

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("voucherCodeFormats", JSONUtilities.encodeArray(voucherCodeFormatJSONArray));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetVoucherList
  *
  *****************************************/

  private JSONObject processGetVoucherList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert vouchers
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> vouchers = new ArrayList<JSONObject>();
    for (GUIManagedObject voucher : voucherService.getStoredVouchers(includeArchived))
      {
        vouchers.add(voucherService.generateResponseJSON(voucher, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("vouchers", JSONUtilities.encodeArray(vouchers));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetVoucher
  *
  *****************************************/

  private JSONObject processGetVoucher(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String voucherID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate voucher
    *
    *****************************************/

    GUIManagedObject voucher = voucherService.getStoredVoucher(voucherID, includeArchived);
    JSONObject voucherJSON = voucherService.generateResponseJSON(voucher, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (voucher != null) ? "ok" : "voucherNotFound");
    if (voucher != null) response.put("voucher", voucherJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutVoucher
  *
  *****************************************/

  private JSONObject processPutVoucher(String userID, JSONObject jsonRoot)
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
    *  voucherID
    *
    *****************************************/

    String voucherID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (voucherID == null)
      {
        voucherID = voucherService.generateVoucherID();
        jsonRoot.put("id", voucherID);
      }

    /*****************************************
    *
    *  existing voucherType
    *
    *****************************************/

    GUIManagedObject existingVoucher = voucherService.getStoredVoucher(voucherID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingVoucher != null && existingVoucher.getReadOnly())
      {
        response.put("id", existingVoucher.getGUIManagedObjectID());
        response.put("accepted", existingVoucher.getAccepted());
        response.put("valid", existingVoucher.getAccepted());
        response.put("processing", voucherService.isActiveVoucher(existingVoucher, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process voucherType
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate voucherType
        *
        ****************************************/

        Voucher voucher = new Voucher(jsonRoot, epoch, existingVoucher);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        voucherService.putVoucher(voucher, (existingVoucher == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", voucher.getVoucherID());
        response.put("accepted", voucher.getAccepted());
        response.put("processing", voucherService.isActiveVoucher(voucher, now));
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

        voucherService.putVoucher(incompleteObject, (existingVoucher == null), userID);

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
        response.put("responseCode", "voucherNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveVoucher
  *
  *****************************************/

  private JSONObject processRemoveVoucher(String userID, JSONObject jsonRoot)
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

    String voucherID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject voucher = voucherService.getStoredVoucher(voucherID);
    if (voucher != null && (force || !voucher.getReadOnly())) voucherService.removeVoucher(voucherID, userID);

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
    if (voucher != null && (force || !voucher.getReadOnly()))
      responseCode = "ok";
    else if (voucher != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "voucherNotFound";

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
  *  processGetMailTemplateList
  *
  *****************************************/

  private JSONObject processGetMailTemplateList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean externalOnly, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert templates
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> templates = new ArrayList<JSONObject>();
    for (GUIManagedObject template : subscriberMessageTemplateService.getStoredMailTemplates(externalOnly, includeArchived))
      {
        templates.add(subscriberMessageTemplateService.generateResponseJSON(template, fullDetails, now));
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

  private JSONObject processGetMailTemplate(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject template = subscriberMessageTemplateService.getStoredSubscriberMessageTemplate(templateID, includeArchived);
    template = (template != null && template.getGUIManagedObjectType() == GUIManagedObjectType.MailMessageTemplate) ? template : null;
    JSONObject templateJSON = subscriberMessageTemplateService.generateResponseJSON(template, true, SystemTime.getCurrentTime());

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
        templateID = subscriberMessageTemplateService.generateSubscriberMessageTemplateID();
        jsonRoot.put("id", templateID);
      }

    /*****************************************
    *
    *  existing template
    *
    *****************************************/

    GUIManagedObject existingTemplate = subscriberMessageTemplateService.getStoredSubscriberMessageTemplate(templateID);
    existingTemplate = (existingTemplate != null && existingTemplate.getGUIManagedObjectType() == GUIManagedObjectType.MailMessageTemplate) ? existingTemplate : null;

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingTemplate != null && existingTemplate.getReadOnly())
      {
        response.put("id", existingTemplate.getGUIManagedObjectID());
        response.put("accepted", existingTemplate.getAccepted());
        response.put("processing", subscriberMessageTemplateService.isActiveSubscriberMessageTemplate(existingTemplate, now));
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

        MailTemplate mailTemplate = new MailTemplate(communicationChannelService, jsonRoot, epoch, existingTemplate);

        /*****************************************
        *
        *  enhance with parameterTags
        *
        *****************************************/

        if (mailTemplate.getParameterTags().size() > 0)
          {
            List<JSONObject> parameterTags = new ArrayList<JSONObject>();
            for (CriterionField parameterTag : mailTemplate.getParameterTags())
              {
                parameterTags.add(parameterTag.getJSONRepresentation());
              }
            mailTemplate.getJSONRepresentation().put("parameterTags", JSONUtilities.encodeArray(parameterTags));
          }

        /*****************************************
        *
        *  store read-only copy
        *
        *****************************************/

        if (existingTemplate == null || mailTemplate.getEpoch() != existingTemplate.getEpoch())
          {
            if (! mailTemplate.getReadOnly())
              {
                MailTemplate readOnlyCopy = (MailTemplate) SubscriberMessageTemplate.newReadOnlyCopy(mailTemplate, subscriberMessageTemplateService, communicationChannelService);
                mailTemplate.setReadOnlyCopyID(readOnlyCopy.getMailTemplateID());
                subscriberMessageTemplateService.putSubscriberMessageTemplate(readOnlyCopy, true, null);
              }
          }
        else if (existingTemplate.getAccepted())
          {
            mailTemplate.setReadOnlyCopyID(((MailTemplate) existingTemplate).getReadOnlyCopyID());
          }

        /*****************************************
        *
        *  store
        *
        *****************************************/

        subscriberMessageTemplateService.putSubscriberMessageTemplate(mailTemplate, (existingTemplate == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", mailTemplate.getMailTemplateID());
        response.put("accepted", mailTemplate.getAccepted());
        response.put("processing", subscriberMessageTemplateService.isActiveSubscriberMessageTemplate(mailTemplate, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, GUIManagedObjectType.MailMessageTemplate, epoch);

        //
        //  store
        //

        subscriberMessageTemplateService.putIncompleteSubscriberMessageTemplate(incompleteObject, (existingTemplate == null), userID);

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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject template = subscriberMessageTemplateService.getStoredSubscriberMessageTemplate(templateID);
    template = (template != null && template.getGUIManagedObjectType() == GUIManagedObjectType.MailMessageTemplate) ? template : null;
    if (template != null && (force || !template.getReadOnly())) subscriberMessageTemplateService.removeSubscriberMessageTemplate(templateID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (template != null && (force || !template.getReadOnly()))
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

  private JSONObject processGetSMSTemplateList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean externalOnly, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert templates
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> templates = new ArrayList<JSONObject>();
    for (GUIManagedObject template : subscriberMessageTemplateService.getStoredSMSTemplates(externalOnly, includeArchived))
      {
        templates.add(subscriberMessageTemplateService.generateResponseJSON(template, fullDetails, now));
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
  *  processGetSMSTemplate
  *
  *****************************************/

  private JSONObject processGetSMSTemplate(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject template = subscriberMessageTemplateService.getStoredSubscriberMessageTemplate(templateID, includeArchived);
    template = (template != null && template.getGUIManagedObjectType() == GUIManagedObjectType.SMSMessageTemplate) ? template : null;
    JSONObject templateJSON = subscriberMessageTemplateService.generateResponseJSON(template, true, SystemTime.getCurrentTime());

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
        templateID = subscriberMessageTemplateService.generateSubscriberMessageTemplateID();
        jsonRoot.put("id", templateID);
      }

    /*****************************************
    *
    *  existing template
    *
    *****************************************/

    GUIManagedObject existingTemplate = subscriberMessageTemplateService.getStoredSubscriberMessageTemplate(templateID);
    existingTemplate = (existingTemplate != null && existingTemplate.getGUIManagedObjectType() == GUIManagedObjectType.SMSMessageTemplate) ? existingTemplate : null;

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingTemplate != null && existingTemplate.getReadOnly())
      {
        response.put("id", existingTemplate.getGUIManagedObjectID());
        response.put("accepted", existingTemplate.getAccepted());
        response.put("processing", subscriberMessageTemplateService.isActiveSubscriberMessageTemplate(existingTemplate, now));
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

        SMSTemplate smsTemplate = new SMSTemplate(communicationChannelService, jsonRoot, epoch, existingTemplate);

        /*****************************************
        *
        *  enhance with parameterTags
        *
        *****************************************/

        if (smsTemplate.getParameterTags().size() > 0)
          {
            List<JSONObject> parameterTags = new ArrayList<JSONObject>();
            for (CriterionField parameterTag : smsTemplate.getParameterTags())
              {
                parameterTags.add(parameterTag.getJSONRepresentation());
              }
            smsTemplate.getJSONRepresentation().put("parameterTags", JSONUtilities.encodeArray(parameterTags));
          }

        /*****************************************
        *
        *  store read-only copy
        *
        *****************************************/

        if (existingTemplate == null || smsTemplate.getEpoch() != existingTemplate.getEpoch())
          {
            if (! smsTemplate.getReadOnly())
              {
                SMSTemplate readOnlyCopy = (SMSTemplate) SubscriberMessageTemplate.newReadOnlyCopy(smsTemplate, subscriberMessageTemplateService, communicationChannelService);
                smsTemplate.setReadOnlyCopyID(readOnlyCopy.getSMSTemplateID());
                subscriberMessageTemplateService.putSubscriberMessageTemplate(readOnlyCopy, true, null);
              }
          }
        else if (existingTemplate.getAccepted())
          {
            smsTemplate.setReadOnlyCopyID(((SMSTemplate) existingTemplate).getReadOnlyCopyID());
          }

        /*****************************************
        *
        *  store
        *
        *****************************************/

        subscriberMessageTemplateService.putSubscriberMessageTemplate(smsTemplate, (existingTemplate == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", smsTemplate.getSMSTemplateID());
        response.put("accepted", smsTemplate.getAccepted());
        response.put("processing", subscriberMessageTemplateService.isActiveSubscriberMessageTemplate(smsTemplate, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, GUIManagedObjectType.SMSMessageTemplate, epoch);

        //
        //  store
        //

        subscriberMessageTemplateService.putIncompleteSubscriberMessageTemplate(incompleteObject, (existingTemplate == null), userID);

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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject template = subscriberMessageTemplateService.getStoredSubscriberMessageTemplate(templateID);
    template = (template != null && template.getGUIManagedObjectType() == GUIManagedObjectType.SMSMessageTemplate) ? template : null;
    if (template != null && (force || !template.getReadOnly())) subscriberMessageTemplateService.removeSubscriberMessageTemplate(templateID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (template != null && (force || !template.getReadOnly()))
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
  *  processGetPushTemplateList
  *
  *****************************************/

  private JSONObject processGetPushTemplateList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean externalOnly, boolean includeArchived)
  {
    
    /****************************************
    *
    *  argument
    *
    ****************************************/

    String communicationChannelID = JSONUtilities.decodeString(jsonRoot, "communicationChannelID", false);
    
    /*****************************************
    *
    *  retrieve and convert templates
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> templates = new ArrayList<JSONObject>();
    for (GUIManagedObject template : subscriberMessageTemplateService.getStoredPushTemplates(externalOnly, includeArchived))
      {
        String templateCommunicationChannelID = (String) template.getJSONRepresentation().get("communicationChannelID");
        if(communicationChannelID == null || communicationChannelID.isEmpty() || communicationChannelID.equals(templateCommunicationChannelID)){
          templates.add(subscriberMessageTemplateService.generateResponseJSON(template, fullDetails, now));
        }
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
  *  processGetPushTemplate
  *
  *****************************************/

  private JSONObject processGetPushTemplate(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject template = subscriberMessageTemplateService.getStoredSubscriberMessageTemplate(templateID, includeArchived);
    template = (template != null && template.getGUIManagedObjectType() == GUIManagedObjectType.PushMessageTemplate) ? template : null;
    JSONObject templateJSON = subscriberMessageTemplateService.generateResponseJSON(template, true, SystemTime.getCurrentTime());

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
  *  processPutPushTemplate
  *
  *****************************************/

  private JSONObject processPutPushTemplate(String userID, JSONObject jsonRoot)
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
        templateID = subscriberMessageTemplateService.generateSubscriberMessageTemplateID();
        jsonRoot.put("id", templateID);
      }

    /*****************************************
    *
    *  existing template
    *
    *****************************************/

    GUIManagedObject existingTemplate = subscriberMessageTemplateService.getStoredSubscriberMessageTemplate(templateID);
    existingTemplate = (existingTemplate != null && existingTemplate.getGUIManagedObjectType() == GUIManagedObjectType.PushMessageTemplate) ? existingTemplate : null;

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingTemplate != null && existingTemplate.getReadOnly())
      {
        response.put("id", existingTemplate.getGUIManagedObjectID());
        response.put("accepted", existingTemplate.getAccepted());
        response.put("processing", subscriberMessageTemplateService.isActiveSubscriberMessageTemplate(existingTemplate, now));
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

        PushTemplate pushTemplate = new PushTemplate(communicationChannelService, jsonRoot, epoch, existingTemplate);

        /*****************************************
        *
        *  enhance with parameterTags
        *
        *****************************************/

        if (pushTemplate.getParameterTags().size() > 0)
          {
            List<JSONObject> parameterTags = new ArrayList<JSONObject>();
            for (CriterionField parameterTag : pushTemplate.getParameterTags())
              {
                parameterTags.add(parameterTag.getJSONRepresentation());
              }
            pushTemplate.getJSONRepresentation().put("parameterTags", JSONUtilities.encodeArray(parameterTags));
          }

        /*****************************************
        *
        *  store read-only copy
        *
        *****************************************/

        if (existingTemplate == null || pushTemplate.getEpoch() != existingTemplate.getEpoch())
          {
            if (! pushTemplate.getReadOnly())
              {
                PushTemplate readOnlyCopy = (PushTemplate) SubscriberMessageTemplate.newReadOnlyCopy(pushTemplate, subscriberMessageTemplateService, communicationChannelService);
                pushTemplate.setReadOnlyCopyID(readOnlyCopy.getPushTemplateID());
                subscriberMessageTemplateService.putSubscriberMessageTemplate(readOnlyCopy, true, null);
              }
          }
        else if (existingTemplate.getAccepted())
          {
            pushTemplate.setReadOnlyCopyID(((PushTemplate) existingTemplate).getReadOnlyCopyID());
          }

        /*****************************************
        *
        *  store
        *
        *****************************************/

        subscriberMessageTemplateService.putSubscriberMessageTemplate(pushTemplate, (existingTemplate == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", pushTemplate.getPushTemplateID());
        response.put("accepted", pushTemplate.getAccepted());
        response.put("processing", subscriberMessageTemplateService.isActiveSubscriberMessageTemplate(pushTemplate, now));
        response.put("responseCode", "ok");
        return JSONUtilities.encodeObject(response);
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  incompleteObject
        //

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, GUIManagedObjectType.PushMessageTemplate, epoch);

        //
        //  store
        //

        subscriberMessageTemplateService.putIncompleteSubscriberMessageTemplate(incompleteObject, (existingTemplate == null), userID);

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
        response.put("responseCode", "pushTemplateNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemovePushTemplate
  *
  *****************************************/

  private JSONObject processRemovePushTemplate(String userID, JSONObject jsonRoot)
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject template = subscriberMessageTemplateService.getStoredSubscriberMessageTemplate(templateID);
    template = (template != null && template.getGUIManagedObjectType() == GUIManagedObjectType.PushMessageTemplate) ? template : null;
    if (template != null && (force || !template.getReadOnly())) subscriberMessageTemplateService.removeSubscriberMessageTemplate(templateID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (template != null && (force || !template.getReadOnly()))
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
    for(DeliveryManagerDeclaration deliveryManager : Deployment.getFulfillmentProviders().values())
      {
        Map<String, String> providerJSON = new HashMap<String, String>();
        providerJSON.put("id", deliveryManager.getProviderID());
        providerJSON.put("name", deliveryManager.getProviderName());
        providerJSON.put("providerType", (deliveryManager.getProviderType() != null) ? deliveryManager.getProviderType().toString() : null);
        providerJSON.put("deliveryType", deliveryManager.getDeliveryType());
        providerJSON.put("url", (String) deliveryManager.getJSONRepresentation().get("url"));
        fulfillmentProviders.add(JSONUtilities.encodeObject(providerJSON));
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

  private JSONObject processGetPaymentMeanList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {

    /*****************************************
    *
    *  retrieve payment means
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> paymentMeans = new ArrayList<JSONObject>();
    for (GUIManagedObject paymentMean : paymentMeanService.getStoredPaymentMeans(includeArchived))
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

  private JSONObject processGetPaymentMean(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    GUIManagedObject paymentMean = paymentMeanService.getStoredPaymentMean(paymentMeanID, includeArchived);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject paymentMean = paymentMeanService.getStoredPaymentMean(paymentMeanID);
    if (paymentMean != null && (force || !paymentMean.getReadOnly())) paymentMeanService.removePaymentMean(paymentMeanID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (paymentMean != null && (force || !paymentMean.getReadOnly()))
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
  *  processGetDashboardCounts
  *
  *****************************************/

  private JSONObject processGetDashboardCounts(String userID, JSONObject jsonRoot, boolean includeArchived)
  {
    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("journeyCount", journeyCount(GUIManagedObjectType.Journey));
    response.put("campaignCount", journeyCount(GUIManagedObjectType.Campaign));
    response.put("workflowCount", journeyCount(GUIManagedObjectType.Workflow));
    response.put("bulkCampaignCount", journeyCount(GUIManagedObjectType.BulkCampaign));
    response.put("segmentationDimensionCount", segmentationDimensionService.getStoredSegmentationDimensions(includeArchived).size());
    response.put("pointCount", pointService.getStoredPoints(includeArchived).size());
    response.put("offerCount", offerService.getStoredOffers(includeArchived).size());
    response.put("loyaltyProgramCount", loyaltyProgramService.getStoredLoyaltyPrograms(includeArchived).size());
    response.put("scoringStrategyCount", scoringStrategyService.getStoredScoringStrategies(includeArchived).size());
    response.put("presentationStrategyCount", presentationStrategyService.getStoredPresentationStrategies(includeArchived).size());
    response.put("dnboMatrixCount", dnboMatrixService.getStoredDNBOMatrixes(includeArchived).size());
    response.put("callingChannelCount", callingChannelService.getStoredCallingChannels(includeArchived).size());
    response.put("salesChannelCount", salesChannelService.getStoredSalesChannels(includeArchived).size());
    response.put("sourceAddressCount", sourceAddressService.getStoredSourceAddresses(includeArchived).size());
    response.put("supplierCount", supplierService.getStoredSuppliers(includeArchived).size());
    response.put("productCount", productService.getStoredProducts(includeArchived).size());
    response.put("voucherTypeCount", voucherTypeService.getStoredVoucherTypes(includeArchived).size());
    response.put("voucherCount", voucherService.getStoredVouchers(includeArchived).size());
    response.put("catalogCharacteristicCount", catalogCharacteristicService.getStoredCatalogCharacteristics(includeArchived).size());
    response.put("journeyObjectiveCount", journeyObjectiveService.getStoredJourneyObjectives(includeArchived).size());
    response.put("offerObjectiveCount", offerObjectiveService.getStoredOfferObjectives(includeArchived).size());
    response.put("productTypeCount", productTypeService.getStoredProductTypes(includeArchived).size());
    response.put("deliverableCount", deliverableService.getStoredDeliverables(includeArchived).size());
    response.put("mailTemplateCount", subscriberMessageTemplateService.getStoredMailTemplates(true, includeArchived).size());
    response.put("smsTemplateCount", subscriberMessageTemplateService.getStoredSMSTemplates(true, includeArchived).size());
    response.put("pushTemplateCount", subscriberMessageTemplateService.getStoredPushTemplates(true, includeArchived).size());
    response.put("reportsCount", reportService.getStoredReports(includeArchived).size());
    response.put("walletsCount", pointService.getStoredPoints(includeArchived).size() + tokenTypeService.getStoredTokenTypes(includeArchived).size() + voucherTypeService.getStoredVoucherTypes(includeArchived).size());
    response.put("ucgRuleCount", ucgRuleService.getStoredUCGRules(includeArchived).size());
    response.put("targetCount", targetService.getStoredTargets(includeArchived).size());
    response.put("exclusionInclusionCount", exclusionInclusionTargetService.getStoredExclusionInclusionTargets(includeArchived).size());
    response.put("segmentContactPolicies",segmentContactPolicyService.getStoredSegmentContactPolicys(includeArchived).size());
    response.put("contactPolicyCount", contactPolicyService.getStoredContactPolicies(includeArchived).size());
    response.put("communicationChannelCount", communicationChannelService.getStoredCommunicationChannels(includeArchived).size());
    response.put("communicationChannelBlackoutCount", communicationChannelBlackoutService.getStoredCommunicationChannelBlackouts(includeArchived).size());
    response.put("partnerCount", resellerService.getStoredResellers(includeArchived).size());
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
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true, false);
            if (baseSubscriberProfile == null)
              {
                response.put("responseCode", "CustomerNotFound");
              }
            else
              {
                response = baseSubscriberProfile.getProfileMapForGUIPresentation(loyaltyProgramService, segmentationDimensionService, targetService, pointService, exclusionInclusionTargetService, subscriberGroupEpochReader);
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
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, true);
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

                    deliveryRequestsJson = result.stream().map(deliveryRequest -> JSONUtilities.encodeObject(deliveryRequest.getGUIPresentationMap(subscriberMessageTemplateService, salesChannelService, journeyService, offerService, loyaltyProgramService, productService, deliverableService, paymentMeanService))).collect(Collectors.toList());
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
    String moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", false);
    String featureID = JSONUtilities.decodeString(jsonRoot, "featureID", false);
    JSONArray deliverableIDs = JSONUtilities.decodeJSONArray(jsonRoot, "deliverableIDs", false);

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
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, true);
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

                    List<DeliveryRequest> BDRs = activities.stream().filter(activity -> activity.getActivityType() == ActivityType.BDR).collect(Collectors.toList());

                    //
                    // prepare dates
                    //

                    Date startDate = null;

                    if (startDateReq == null || startDateReq.isEmpty())
                      {
                        startDate = new Date(0L);
                      }
                    else
                      {
                        startDate = RLMDateUtils.parseDate(startDateReq, dateFormat, Deployment.getBaseTimeZone());
                      }
                    
                    //
                    // filter on moduleID
                    //

                    if (moduleID != null)
                      {
                        BDRs = BDRs.stream().filter(activity -> activity.getModuleID().equals(moduleID)).collect(Collectors.toList());
                      }
                    
                    //
                    // filter on featureID
                    //

                    if (featureID != null)
                      {
                        BDRs = BDRs.stream().filter(activity -> activity.getFeatureID().equals(featureID)).collect(Collectors.toList());
                      }
                    
                    //
                    // filter on deliverableIDs
                    //
                    
                    if(deliverableIDs != null)
                      {
                        List<DeliveryRequest> result = new ArrayList<DeliveryRequest>();
                        for(DeliveryRequest deliveryRequest : BDRs)
                          {
                            if(deliveryRequest instanceof CommodityDeliveryRequest)
                              {
                                CommodityDeliveryRequest request = (CommodityDeliveryRequest) deliveryRequest;
                                if(checkDeliverableIDs(deliverableIDs, request.getCommodityID()))
                                  {
                                    result.add(deliveryRequest);
                                  }
                              }
                            else if(deliveryRequest instanceof EmptyFulfillmentRequest) 
                              {
                                EmptyFulfillmentRequest request = (EmptyFulfillmentRequest) deliveryRequest;
                                if(checkDeliverableIDs(deliverableIDs, request.getCommodityID()))
                                  {
                                    result.add(deliveryRequest);
                                  }
                              }
                            else if(deliveryRequest instanceof INFulfillmentRequest) 
                              {
                                INFulfillmentRequest request = (INFulfillmentRequest) deliveryRequest;
                                if(checkDeliverableIDs(deliverableIDs, request.getCommodityID()))
                                  {
                                    result.add(deliveryRequest);
                                  }
                              }
                            else if(deliveryRequest instanceof PointFulfillmentRequest) 
                              {
                                PointFulfillmentRequest request = (PointFulfillmentRequest) deliveryRequest;
                                if(checkDeliverableIDs(deliverableIDs, request.getPointID()))
                                  {
                                    result.add(deliveryRequest);
                                  }
                              }
                          }
                        BDRs = result;
                      }

                    //
                    // filter and prepare json
                    //

                    for (DeliveryRequest bdr : BDRs)
                      {
                        if (bdr.getEventDate().after(startDate) || bdr.getEventDate().equals(startDate))
                          {
                            Map<String, Object> bdrMap = bdr.getGUIPresentationMap(subscriberMessageTemplateService, salesChannelService, journeyService, offerService, loyaltyProgramService, productService, deliverableService, paymentMeanService);
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
    String moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", false);
    String featureID = JSONUtilities.decodeString(jsonRoot, "featureID", false);
    String offerID = JSONUtilities.decodeString(jsonRoot, "offerID", false);
    String salesChannelID = JSONUtilities.decodeString(jsonRoot, "salesChannelID", false);
    String paymentMeanID = JSONUtilities.decodeString(jsonRoot, "paymentMeanID", false);

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
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, true);
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

                    List<DeliveryRequest> ODRs = activities.stream().filter(activity -> activity.getActivityType() == ActivityType.ODR).collect(Collectors.toList());

                    //
                    // prepare dates
                    //

                    Date startDate = null;

                    if (startDateReq == null || startDateReq.isEmpty())
                      {
                        startDate = new Date(0L);
                      }
                    else
                      {
                        startDate = RLMDateUtils.parseDate(startDateReq, dateFormat, Deployment.getBaseTimeZone());
                      }
                    
                    //
                    // filter on moduleID
                    //

                    if (moduleID != null)
                      {
                        ODRs = ODRs.stream().filter(activity -> activity.getModuleID().equals(moduleID)).collect(Collectors.toList());
                      }
                    
                    //
                    // filter on featureID
                    //

                    if (featureID != null)
                      {
                        ODRs = ODRs.stream().filter(activity -> activity.getFeatureID().equals(featureID)).collect(Collectors.toList());
                      }
                    
                    //
                    // filter on offerID
                    //

                    if (offerID != null)
                      {
                        List<DeliveryRequest> result = new ArrayList<DeliveryRequest>();
                        for (DeliveryRequest request : ODRs)
                          {
                            if(request instanceof PurchaseFulfillmentRequest)
                              {
                                if(((PurchaseFulfillmentRequest)request).getOfferID().equals(offerID))
                                  {
                                    result.add(request);
                                  }
                              }
                          }
                        ODRs = result;
                      }
                    
                    //
                    // filter on salesChannelID
                    //

                    if (salesChannelID != null)
                      {
                        List<DeliveryRequest> result = new ArrayList<DeliveryRequest>();
                        for (DeliveryRequest request : ODRs)
                          {
                            if(request instanceof PurchaseFulfillmentRequest)
                              {
                                if(((PurchaseFulfillmentRequest)request).getSalesChannelID().equals(salesChannelID))
                                  {
                                    result.add(request);
                                  }
                              }
                          }
                        ODRs = result;
                      }
                    
                    //
                    // filter on paymentMeanID
                    //

                    if (paymentMeanID != null)
                      {
                        List<DeliveryRequest> result = new ArrayList<DeliveryRequest>();
                        for (DeliveryRequest request : ODRs)
                          {
                            if(request instanceof PurchaseFulfillmentRequest)
                              {
                                PurchaseFulfillmentRequest odrRequest = (PurchaseFulfillmentRequest) request;
                                Offer offer = (Offer) offerService.getStoredGUIManagedObject(odrRequest.getOfferID());
                                if(offer != null)
                                  {
                                    if(offer.getOfferSalesChannelsAndPrices() != null)
                                      {
                                        for(OfferSalesChannelsAndPrice channel : offer.getOfferSalesChannelsAndPrices())
                                          {
                                            if(channel.getPrice() != null && channel.getPrice().getPaymentMeanID().equals(paymentMeanID))
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
                    // filter using dates and prepare json
                    //
                    
                    for (DeliveryRequest odr : ODRs)
                      {
                        if (odr.getEventDate().after(startDate) || odr.getEventDate().equals(startDate))
                          {
                            Map<String, Object> presentationMap =  odr.getGUIPresentationMap(subscriberMessageTemplateService, salesChannelService, journeyService, offerService, loyaltyProgramService, productService, deliverableService, paymentMeanService);
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
    String moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", false);
    String featureID = JSONUtilities.decodeString(jsonRoot, "featureID", false);

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
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, true);
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

                    List<DeliveryRequest> messages = activities.stream().filter(activity -> activity.getActivityType() == ActivityType.Messages).collect(Collectors.toList());

                    //
                    // prepare dates
                    //

                    Date startDate = null;

                    if (startDateReq == null || startDateReq.isEmpty())
                      {
                        startDate = new Date(0L);
                      }
                    else
                      {
                        startDate = RLMDateUtils.parseDate(startDateReq, dateFormat, Deployment.getBaseTimeZone());
                      }
                    
                    //
                    // filter on moduleID
                    //

                    if (moduleID != null)
                      {
                        messages = messages.stream().filter(activity -> activity.getModuleID().equals(moduleID)).collect(Collectors.toList());
                      }
                    
                    //
                    // filter on featureID
                    //

                    if (featureID != null)
                      {
                        messages = messages.stream().filter(activity -> activity.getFeatureID().equals(featureID)).collect(Collectors.toList());
                      }

                    //
                    // filter using dates and prepare json
                    //

                    for (DeliveryRequest message : messages)
                      {
                        if (message.getEventDate().after(startDate) || message.getEventDate().equals(startDate))
                          {
                            Map<String, Object> messageMap = message.getGUIPresentationMap(subscriberMessageTemplateService, salesChannelService, journeyService, offerService, loyaltyProgramService, productService, deliverableService, paymentMeanService);
                            messagesJson.add(JSONUtilities.encodeObject(messageMap));
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
    String journeyObjectiveName = JSONUtilities.decodeString(jsonRoot, "objective", false);
    String journeyState = JSONUtilities.decodeString(jsonRoot, "journeyState", false);
    String customerStatus = JSONUtilities.decodeString(jsonRoot, "customerStatus", false);
    String journeyStartDateStr = JSONUtilities.decodeString(jsonRoot, "journeyStartDate", false);
    String journeyEndDateStr = JSONUtilities.decodeString(jsonRoot, "journeyEndDate", false);


    //
    // yyyy-MM-dd -- date format
    //

    String dateFormat = "yyyy-MM-dd";
    Date journeyStartDate = prepareStartDate(getDateFromString(journeyStartDateStr, dateFormat));
    Date journeyEndDate = prepareEndDate(getDateFromString(journeyEndDateStr, dateFormat));

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
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, true);
            if (baseSubscriberProfile == null)
              {
                response.put("responseCode", "CustomerNotFound");
                log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
              }
            else
              {
                List<JSONObject> journeysJson = new ArrayList<JSONObject>();
                SubscriberHistory subscriberHistory = baseSubscriberProfile.getSubscriberHistory();
                if (subscriberHistory != null && subscriberHistory.getJourneyHistory() != null)
                  {

                    //
                    //  read campaigns
                    //

                    Collection<GUIManagedObject> stroeRawJourneys = journeyService.getStoredJourneys(true);
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

                        List<JourneyObjective> journeyObjectives = activejourneyObjectives.stream().filter(journeyObj -> journeyObjectiveName.equals(journeyObj.getJSONRepresentation().get("display"))).collect(Collectors.toList());
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
                    // filter on journeyState
                    //
                    
                    if (journeyState != null)
                      {
                        storeJourneys = storeJourneys.stream().filter(journey -> journeyService.getJourneyStatus(journey).getExternalRepresentation().equalsIgnoreCase(journeyState)).collect(Collectors.toList()); 
                      }

                    //
                    //  read campaign statistics 
                    //

                    List<JourneyHistory> journeyHistory = subscriberHistory.getJourneyHistory();

                    //
                    // change data structure to map
                    //

                    Map<String, List<JourneyHistory>> journeyStatisticsMap = journeyHistory.stream().collect(Collectors.groupingBy(JourneyHistory::getJourneyID));

                    for (Journey storeJourney : storeJourneys)
                      {

                        //
                        //  thisJourneyStatistics
                        //

                        List<JourneyHistory> thisJourneyStatistics = journeyStatisticsMap.get(storeJourney.getJourneyID());

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
                            if(journeyService.getJourneyStatus(storeJourney).getExternalRepresentation().equalsIgnoreCase(journeyState))
                              {
                                criteriaSatisfied = true;
                              }
                            if (! criteriaSatisfied) continue;
                          }

                        //
                        // reverse sort
                        //

                        Collections.sort(thisJourneyStatistics, Collections.reverseOrder());

                        //
                        // prepare current node
                        //

                        JourneyHistory subsLatestStatistic = thisJourneyStatistics.get(0);
                        
                        //
                        // filter on customerStatus
                        //
                        
                        boolean statusNotified = subsLatestStatistic.getStatusHistory().stream().filter(journeyStat -> journeyStat.getStatusNotified()).count() > 0L ;
                        boolean statusConverted = subsLatestStatistic.getStatusHistory().stream().filter(journeyStat -> journeyStat.getStatusConverted()).count() > 0L ;
                        boolean statusControlGroup = subsLatestStatistic.getStatusHistory().stream().filter(journeyStat -> journeyStat.getStatusControlGroup()).count() > 0L ;
                        boolean statusUniversalControlGroup = subsLatestStatistic.getStatusHistory().stream().filter(journeyStat -> journeyStat.getStatusUniversalControlGroup()).count() > 0L ;
                        boolean journeyComplete = subsLatestStatistic.getStatusHistory().stream().filter(journeyStat -> journeyStat.getJourneyComplete()).count() > 0L ;

                        if (customerStatus != null)
                          {
                            SubscriberJourneyStatus customerStatusInJourney = SubscriberJourneyStatus.fromExternalRepresentation(customerStatus);
                            boolean criteriaSatisfied = false;
                            switch (customerStatusInJourney)
                              {
                                case Entered:
                                  criteriaSatisfied = ! statusControlGroup && ! statusNotified && ! statusConverted;
                                  break;
                                case ConvertedNotNotified:
                                  criteriaSatisfied = ! statusControlGroup && ! statusNotified && statusConverted;
                                  break;
                                case Notified:
                                  criteriaSatisfied = ! statusControlGroup && statusNotified && ! statusConverted;
                                  break;
                                case ConvertedNotified:
                                  criteriaSatisfied = ! statusControlGroup && statusNotified && statusConverted;
                                  break;
                                case ControlGroupEntered:
                                  criteriaSatisfied = statusControlGroup && ! statusConverted;
                                  break;
                                case ControlGroupConverted:
                                  criteriaSatisfied = statusControlGroup && statusConverted;
                                  break;
                                case Unknown:
                                  break;
                              }
                            if (!criteriaSatisfied) continue;
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
                        journeyResponseMap.put("exitDate", subsLatestStatistic.getJourneyExitDate()!=null?getDateString(subsLatestStatistic.getJourneyExitDate()):"");
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
                                characteristics.put("catalogCharacteristicID", catalogCharacteristicInstance.getCatalogCharacteristicID());
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
                        //  node history
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

                        journeyResponseMap.put("customerStatus", Journey.getSubscriberJourneyStatus(journeyComplete, statusConverted, statusNotified, statusControlGroup).getExternalRepresentation());
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
    String campaignObjectiveName = JSONUtilities.decodeString(jsonRoot, "objective", false);
    String campaignState = JSONUtilities.decodeString(jsonRoot, "campaignState", false);
    String customerStatus = JSONUtilities.decodeString(jsonRoot, "customerStatus", false);
    String campaignStartDateStr = JSONUtilities.decodeString(jsonRoot, "campaignStartDate", false);
    String campaignEndDateStr = JSONUtilities.decodeString(jsonRoot, "campaignEndDate", false);


    //
    // yyyy-MM-dd -- date format
    //

    String dateFormat = "yyyy-MM-dd";
    Date campaignStartDate = prepareStartDate(getDateFromString(campaignStartDateStr, dateFormat));
    Date campaignEndDate = prepareEndDate(getDateFromString(campaignEndDateStr, dateFormat));

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
            SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, true);
            if (baseSubscriberProfile == null)
              {
                response.put("responseCode", "CustomerNotFound");
                log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
              }
            else
              {
                List<JSONObject> campaignsJson = new ArrayList<JSONObject>();
                SubscriberHistory subscriberHistory = baseSubscriberProfile.getSubscriberHistory();
                if (subscriberHistory != null && subscriberHistory.getJourneyHistory() != null)
                  {

                    //
                    //  read campaigns
                    //

                    Collection<GUIManagedObject> storeRawCampaigns = journeyService.getStoredJourneys(true);
                    List<Journey> storeCampaigns = new ArrayList<Journey>();
                    for (GUIManagedObject storeCampaign : storeRawCampaigns)
                      {
                        if (storeCampaign instanceof Journey) storeCampaigns.add( (Journey) storeCampaign);
                      }

                    //
                    // filter campaigns
                    //

                    storeCampaigns = storeCampaigns.stream().filter(campaign -> (campaign.getGUIManagedObjectType() == GUIManagedObjectType.Campaign || campaign.getGUIManagedObjectType() == GUIManagedObjectType.BulkCampaign)).collect(Collectors.toList()); 

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

                        List<JourneyObjective> campaignObjectives = activecampaignObjectives.stream().filter(journeyObj -> campaignObjectiveName.equals(journeyObj.getJSONRepresentation().get("display"))).collect(Collectors.toList());
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

                    List<JourneyHistory> subscribersCampaignHistory = subscriberHistory.getJourneyHistory();

                    //
                    // change data structure to map
                    //

                    Map<String, List<JourneyHistory>> campaignStatisticsMap = subscribersCampaignHistory.stream().collect(Collectors.groupingBy(JourneyHistory::getJourneyID));

                    for (Journey storeCampaign : storeCampaigns)
                      {

                        //
                        //  thisCampaignStatistics
                        //

                        List<JourneyHistory> thisCampaignHistory = campaignStatisticsMap.get(storeCampaign.getJourneyID());

                        //
                        //  continue if not in stat
                        //

                        if (thisCampaignHistory == null || thisCampaignHistory.isEmpty()) continue;

                        //
                        // filter on campaignState
                        //

                        if (campaignState != null && !campaignState.isEmpty())
                          {
                            boolean criteriaSatisfied = false;
                            if(journeyService.getJourneyStatus(storeCampaign).getExternalRepresentation().equalsIgnoreCase(campaignState))
                              {
                                criteriaSatisfied = true;
                              }
                            if (! criteriaSatisfied) continue;
                          }

                        //
                        // reverse sort
                        //

                        Collections.sort(thisCampaignHistory, Collections.reverseOrder());

                        //
                        // prepare current node
                        //

                        JourneyHistory subsLatestStatistic = thisCampaignHistory.get(0);

                        //
                        // filter on customerStatus
                        //
                        
                        boolean statusNotified = subsLatestStatistic.getStatusHistory().stream().filter(journeyStat -> journeyStat.getStatusNotified()).count() > 0L ;
                        boolean statusConverted = subsLatestStatistic.getStatusHistory().stream().filter(journeyStat -> journeyStat.getStatusConverted()).count() > 0L ;
                        boolean statusControlGroup = subsLatestStatistic.getStatusHistory().stream().filter(journeyStat -> journeyStat.getStatusControlGroup()).count() > 0L ;
                        boolean statusUniversalControlGroup = subsLatestStatistic.getStatusHistory().stream().filter(journeyStat -> journeyStat.getStatusUniversalControlGroup()).count() > 0L ;
                        boolean journeyComplete = subsLatestStatistic.getStatusHistory().stream().filter(journeyStat -> journeyStat.getJourneyComplete()).count() > 0L ;

                        if (customerStatus != null)
                          {
                            SubscriberJourneyStatus customerStatusInJourney = SubscriberJourneyStatus.fromExternalRepresentation(customerStatus);
                            boolean criteriaSatisfied = false;
                            switch (customerStatusInJourney)
                              {
                                case Entered:
                                  criteriaSatisfied = ! statusControlGroup && ! statusNotified && ! statusConverted;
                                  break;
                                case ConvertedNotNotified:
                                  criteriaSatisfied = ! statusControlGroup && ! statusNotified && statusConverted;
                                  break;
                                case Notified:
                                  criteriaSatisfied = ! statusControlGroup && statusNotified && ! statusConverted;
                                  break;
                                case ConvertedNotified:
                                  criteriaSatisfied = ! statusControlGroup && statusNotified && statusConverted;
                                  break;
                                case ControlGroupEntered:
                                  criteriaSatisfied = statusControlGroup && ! statusConverted;
                                  break;
                                case ControlGroupConverted:
                                  criteriaSatisfied = statusControlGroup && statusConverted;
                                  break;
                                case Unknown:
                                  break;
                              }
                            if (!criteriaSatisfied) continue;
                          }

                        //
                        // prepare response
                        //

                        Map<String, Object> campaignResponseMap = new HashMap<String, Object>();
                        campaignResponseMap.put("campaignID", storeCampaign.getJourneyID());
                        campaignResponseMap.put("campaignName", journeyService.generateResponseJSON(storeCampaign, true, SystemTime.getCurrentTime()).get("display"));
                        campaignResponseMap.put("description", journeyService.generateResponseJSON(storeCampaign, true, SystemTime.getCurrentTime()).get("description"));
                        campaignResponseMap.put("startDate", getDateString(storeCampaign.getEffectiveStartDate()));
                        campaignResponseMap.put("endDate", getDateString(storeCampaign.getEffectiveEndDate()));
                        campaignResponseMap.put("entryDate", getDateString(subsLatestStatistic.getJourneyEntranceDate()));
                        campaignResponseMap.put("exitDate", subsLatestStatistic.getJourneyExitDate()!=null?getDateString(subsLatestStatistic.getJourneyExitDate()):"");
                        campaignResponseMap.put("campaignState", journeyService.getJourneyStatus(storeCampaign).getExternalRepresentation());
                        
                        List<JSONObject> resultObjectives = new ArrayList<JSONObject>();
                        for (JourneyObjectiveInstance journeyObjectiveInstance : storeCampaign.getJourneyObjectiveInstances())
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
                                characteristics.put("catalogCharacteristicID", catalogCharacteristicInstance.getCatalogCharacteristicID());
                                characteristics.put("value", catalogCharacteristicInstance.getValue());
                                resultCharacteristics.add(characteristics);
                              }
                            
                            result.put("catalogCharacteristics", JSONUtilities.encodeArray(resultCharacteristics));
                            resultObjectives.add(result);
                          }
                        
                        campaignResponseMap.put("objectives", JSONUtilities.encodeArray(resultObjectives));

                        Map<String, Object> currentState = new HashMap<String, Object>();
                        NodeHistory nodeHistory = subsLatestStatistic.getLastNodeEntered();
                        currentState.put("nodeID", nodeHistory.getToNodeID());
                        currentState.put("nodeName", nodeHistory.getToNodeID() == null ? null : (storeCampaign.getJourneyNode(nodeHistory.getToNodeID()) == null ? "node has been removed" : storeCampaign.getJourneyNode(nodeHistory.getToNodeID()).getNodeName()));
                        JSONObject currentStateJson = JSONUtilities.encodeObject(currentState);

                        //
                        //  node history
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
                        campaignResponseMap.put("customerStatus", Journey.getSubscriberJourneyStatus(journeyComplete, statusConverted, statusNotified, statusControlGroup).getExternalRepresentation());
                        campaignResponseMap.put("journeyComplete", journeyComplete);
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
  * processGetGetCustomerPoints
  *
  *****************************************/

  private JSONObject processGetCustomerPoints(String userID, JSONObject jsonRoot) throws GUIManagerException
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

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
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
          response.put("responseCode", "BonusNotFound");
          return JSONUtilities.encodeObject(response);
        }
      }

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
        return JSONUtilities.encodeObject(response);
      }

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
                  pointPresentation.put("earned", pointBalance.getEarnedHistory().getAllTimeBucket());
                  pointPresentation.put("expired", pointBalance.getExpiredHistory().getAllTimeBucket());
                  pointPresentation.put("consumed", pointBalance.getConsumedHistory().getAllTimeBucket());
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
          response.put("responseCode", "ok");
        }
    }
    catch (SubscriberProfileServiceException e)
    {
      throw new GUIManagerException(e);
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

  private JSONObject processGetCustomerLoyaltyPrograms(String userID, JSONObject jsonRoot) throws GUIManagerException
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

   String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
   String searchedLoyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "loyaltyProgramID", false);
   if(searchedLoyaltyProgramID != null && searchedLoyaltyProgramID.isEmpty()){ searchedLoyaltyProgramID = null; }

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
       return JSONUtilities.encodeObject(response);
     }

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
                  loyaltyProgramPresentation.put("loyaltyProgramName", loyaltyProgram.getLoyaltyProgramName());
                  loyaltyProgramPresentation.put("loyaltyProgramDisplay", loyaltyProgram.getLoyaltyProgramDisplay());
                  loyaltyProgramPresentation.put("loyaltyProgramEnrollmentDate", getDateString(loyaltyProgramState.getLoyaltyProgramEnrollmentDate()));
                  loyaltyProgramPresentation.put("loyaltyProgramExitDate", getDateString(loyaltyProgramState.getLoyaltyProgramExitDate()));
                  loyaltyProgramPresentation.put("active", loyaltyProgram.getActive());


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
                      Point statusPoint = pointService.getActivePoint(statusPointID, now);
                      if (statusPoint != null)
                        {
                          loyaltyProgramPresentation.put("statusPointID", statusPoint.getPointID());
                          loyaltyProgramPresentation.put("statusPointName", statusPoint.getPointName());
                          loyaltyProgramPresentation.put("statusPointDisplay", statusPoint.getDisplay());
                        }
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
                      Point rewardPoint = pointService.getActivePoint(rewardPointID, now);
                      if (rewardPoint != null)
                        {
                          loyaltyProgramPresentation.put("rewardsPointID", rewardPoint.getPointID());
                          loyaltyProgramPresentation.put("rewardsPointName", rewardPoint.getPointName());
                          loyaltyProgramPresentation.put("rewardsPointDisplay", rewardPoint.getDisplay());
                        }
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

//                    case BADGES:
//                      // TODO
//                      break;

                    default:
                      break;
                  }

                  //
                  //  
                  //

                  loyaltyProgramsPresentation.add(JSONUtilities.encodeObject(loyaltyProgramPresentation));

                }
            }

          response.put("loyaltyPrograms", loyaltyProgramsPresentation);
          response.put("responseCode", "ok");
        }
    } 
    catch (SubscriberProfileServiceException e)
    {
      throw new GUIManagerException(e);
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
  *  processGetFilesList
  *
  *****************************************/

  private JSONObject processGetFilesList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert UploadedFiles
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> uploadedFiles = new ArrayList<JSONObject>();
    String applicationID = JSONUtilities.decodeString(jsonRoot, "applicationID", true);
    for (GUIManagedObject uploaded : uploadedFileService.getStoredGUIManagedObjects(includeArchived))
      {
        String fileApplicationID = JSONUtilities.decodeString(uploaded.getJSONRepresentation(), "applicationID", false);
        if (Objects.equals(applicationID, fileApplicationID))
          {
            JSONObject jsonObject = uploadedFileService.generateResponseJSON(uploaded, fullDetails, now);
            if(fullDetails && applicationID.equals(UploadedFileService.basemanagementApplicationID))
              {
                jsonObject.put("segmentCounts", ((UploadedFile)uploaded).getMetaData().get("segmentCounts"));
              }
            uploadedFiles.add(jsonObject);
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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject existingFileUpload = uploadedFileService.getStoredUploadedFile(uploadedFileID);
    if (existingFileUpload != null && (force || !existingFileUpload.getReadOnly()))
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
    if (existingFileUpload != null && (force || !existingFileUpload.getReadOnly()))
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
            SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, true);
            if (subscriberProfile == null)
              {
                response.put("responseCode", "CustomerNotFound");
              } 
            else
              {
                Date now = SystemTime.getCurrentTime();
                SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, now);
                SubscriberHistory subscriberHistory = subscriberProfile.getSubscriberHistory();
                Map<String, List<JourneyHistory>> campaignStatisticsMap = new HashMap<String, List<JourneyHistory>>();

                //
                //  journey statistics
                //

                if (subscriberHistory != null && subscriberHistory.getJourneyHistory() != null)
                  {
                    campaignStatisticsMap = subscriberHistory.getJourneyHistory().stream().collect(Collectors.groupingBy(JourneyHistory::getJourneyID));
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
                        campaignMap.put("campaignName", journeyService.generateResponseJSON(elgibleActiveCampaign, true, now).get("display"));
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
                                characteristics.put("catalogCharacteristicID", catalogCharacteristicInstance.getCatalogCharacteristicID());
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
  *  processGetTargetList
  *
  *****************************************/

  private JSONObject processGetTargetList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {

    /*****************************************
    *
    *  retrieve target list
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> targetLists = new ArrayList<JSONObject>();
    for (GUIManagedObject targetList : targetService.getStoredTargets(includeArchived))
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
        targetID = subscriberGroupSharedIDService.generateID();
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
  *  processGetTarget
  *
  *****************************************/

  private JSONObject processGetTarget(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String targetID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate target
    *
    *****************************************/

    GUIManagedObject target = targetService.getStoredTarget(targetID, includeArchived);
    JSONObject targetJSON = targetService.generateResponseJSON(target, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (target != null) ? "ok" : "targetNotFound");
    if (target != null) response.put("target", targetJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processRemoveTarget
  *
  *****************************************/

  private JSONObject processRemoveTarget(String userID, JSONObject jsonRoot){

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
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject existingTarget = targetService.getStoredTarget(targetID);
    if (existingTarget != null && (force || !existingTarget.getReadOnly()) ){
      targetService.removeTarget(targetID, userID);
    }

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (existingTarget != null && (force || !existingTarget.getReadOnly())) {
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
  *  processUpdateCustomerParent
  *
  *****************************************/

  private JSONObject processUpdateCustomerParent(String userID, JSONObject jsonRoot) throws GUIManagerException
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

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String relationshipID = JSONUtilities.decodeString(jsonRoot, "relationshipID", true);
    String newParentCustomerID = JSONUtilities.decodeString(jsonRoot, "newParentCustomerID", true);

    /*****************************************
    *
    * resolve relationship
    *
    *****************************************/
      
    boolean isRelationshipSupported = false;
    for (SupportedRelationship supportedRelationship : Deployment.getSupportedRelationships().values())
      {
        if (supportedRelationship.getID().equals(relationshipID))
          {
            isRelationshipSupported = true;
            break;
          }
      }
    
    if(!isRelationshipSupported)
      {
        response.put("responseCode", RESTAPIGenericReturnCodes.RELATIONSHIP_NOT_FOUND.getGenericResponseCode());
        response.put("responseMessage", RESTAPIGenericReturnCodes.RELATIONSHIP_NOT_FOUND.getGenericResponseMessage());
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    * resolve subscriberID
    *
    *****************************************/
    
    String subscriberID = resolveSubscriberID(customerID);
    String newParentSubscriberID = resolveSubscriberID(newParentCustomerID);
    if (subscriberID == null)
      {
        response.put("responseCode", RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put("responseMessage", RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage()
            + "-{specified customerID do not relate to any customer}");
        return JSONUtilities.encodeObject(response);
      } 
    else if (newParentSubscriberID == null)
      {
        response.put("responseCode", RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put("responseMessage", RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage()
            + "-{specified newParentCustomerID do not relate to any customer}");
        return JSONUtilities.encodeObject(response);
      } 
    else if (subscriberID.equals(newParentSubscriberID))
      {
        response.put("responseCode", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
        response.put("responseMessage", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage()
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

        response.put("responseCode", RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
        response.put("responseMessage", RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
      } 
    catch (SubscriberProfileServiceException e)
      {
        throw new GUIManagerException(e);
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
  *  processDeleteCustomerParent
  *
  *****************************************/

  private JSONObject processRemoveCustomerParent(String userID, JSONObject jsonRoot) throws GUIManagerException
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

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String relationshipID = JSONUtilities.decodeString(jsonRoot, "relationshipID", true);

    /*****************************************
    *
    * resolve subscriberID
    *
    *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        response.put("responseCode", RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put("responseMessage", RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage()
            + "-{specified customerID do not relate to any customer}");
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


        response.put("responseCode", RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
        response.put("responseMessage", RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
      } 
    catch (SubscriberProfileServiceException e)
      {
        throw new GUIManagerException(e);
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

  /*****************************************
  *
  *  processgetCommunicationChannelList
  *
  *****************************************/

  private JSONObject processgetCommunicationChannelList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve communication channel list
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> communicationChannelList = new ArrayList<JSONObject>();
    for (GUIManagedObject communicationChannel : communicationChannelService.getStoredCommunicationChannels(includeArchived))
      {
        JSONObject channel = communicationChannelService.generateResponseJSON(communicationChannel, fullDetails, now);
        communicationChannelList.add(channel);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("communicationChannels", JSONUtilities.encodeArray(communicationChannelList));
    if(fullDetails) {
      NotificationDailyWindows notifWindows = Deployment.getNotificationDailyWindows().get("0");
      response.put("defaultNoftificationDailyWindows", notifWindows.getJSONRepresentation());
    }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetCommunicationChannel
  *
  *****************************************/

  private JSONObject processGetCommunicationChannel(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String communicationChannelID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate communication channel
    *
    *****************************************/

    GUIManagedObject communicationChannel = communicationChannelService.getStoredCommunicationChannel(communicationChannelID, includeArchived);
    JSONObject communicationChannelJSON = communicationChannelService.generateResponseJSON(communicationChannel, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (communicationChannel != null) ? "ok" : "communicationChannelNotFound");
    if (communicationChannel != null) response.put("communicationChannel", communicationChannelJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutCommunicationChannel
  *
  *****************************************/

  private JSONObject processPutCommunicationChannel(String userID, JSONObject jsonRoot)
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
    *  communicationChannelID
    *
    *****************************************/

    String communicationChannelID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (communicationChannelID == null)
      {
        communicationChannelID = communicationChannelService.generateCommunicationChannelID();
        jsonRoot.put("id", communicationChannelID);
      }

    /*****************************************
    *
    *  existing CommunicationChannel
    *
    *****************************************/

    GUIManagedObject existingCommunicationChannel = communicationChannelService.getStoredCommunicationChannel(communicationChannelID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingCommunicationChannel != null && existingCommunicationChannel.getReadOnly())
      {
        response.put("id", existingCommunicationChannel.getGUIManagedObjectID());
        response.put("accepted", existingCommunicationChannel.getAccepted());
        response.put("valid", existingCommunicationChannel.getAccepted());
        response.put("processing", communicationChannelService.isActiveCommunicationChannel(existingCommunicationChannel, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process CommunicationChannel
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate CommunicationChannel
        *
        ****************************************/

        CommunicationChannel communicationChannel = new CommunicationChannel(jsonRoot, epoch, existingCommunicationChannel);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        communicationChannelService.putCommunicationChannel(communicationChannel, (existingCommunicationChannel == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", communicationChannel.getGUIManagedObjectID());
        response.put("accepted", communicationChannel.getAccepted());
        response.put("valid", communicationChannel.getAccepted());
        response.put("processing", communicationChannelService.isActiveCommunicationChannel(communicationChannel, now));
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

        communicationChannelService.putCommunicationChannel(incompleteObject, (existingCommunicationChannel == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  response
        //

        response.put("communicationChannelID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "communicationChannelNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveCommunicationChannel
  *
  *****************************************/

  private JSONObject processRemoveCommunicationChannel(String userID, JSONObject jsonRoot){

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

    String communicationChannelID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject existingCommunicationChannel = communicationChannelService.getStoredCommunicationChannel(communicationChannelID);
    if (existingCommunicationChannel != null && (force || !existingCommunicationChannel.getReadOnly())) {
      communicationChannelService.removeCommunicationChannel(communicationChannelID, userID);
    }

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (existingCommunicationChannel != null && (force || !existingCommunicationChannel.getReadOnly())) {
      responseCode = "ok";
    }
    else if (existingCommunicationChannel != null) {
      responseCode = "failedReadOnly";
    }
    else {
      responseCode = "communicationChannelNotFound";
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
  *  processGetBlackoutPeriodsList
  *
  *****************************************/

  private JSONObject processGetBlackoutPeriodsList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve blackout period list
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> communicationChannelBlackoutList = new ArrayList<JSONObject>();
    for (GUIManagedObject blackoutPeriods : communicationChannelBlackoutService.getStoredCommunicationChannelBlackouts(includeArchived))
      {
        JSONObject blackoutPeriod = communicationChannelBlackoutService.generateResponseJSON(blackoutPeriods, fullDetails, now);
        communicationChannelBlackoutList.add(blackoutPeriod);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("blackoutPeriods", JSONUtilities.encodeArray(communicationChannelBlackoutList));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetBlackoutPeriods
  *
  *****************************************/

  private JSONObject processGetBlackoutPeriods(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String communicationChannelBlackoutPeriodID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /**************************************************************
    *
    *  retrieve and decorate communication channel blackout period
    *
    ***************************************************************/

    GUIManagedObject communicationChannelBlackoutPeriod = communicationChannelBlackoutService.getStoredCommunicationChannelBlackout(communicationChannelBlackoutPeriodID, includeArchived);
    JSONObject communicationChannelBlackoutPeriodJSON = communicationChannelBlackoutService.generateResponseJSON(communicationChannelBlackoutPeriod, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (communicationChannelBlackoutPeriod != null) ? "ok" : "communicationChannelBlackoutPeriodNotFound");
    if (communicationChannelBlackoutPeriod != null) response.put("blackoutPeriods", communicationChannelBlackoutPeriodJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutBlackoutPeriods
  *
  *****************************************/

  private JSONObject processPutBlackoutPeriods(String userID, JSONObject jsonRoot)
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
    *  CommunicationChannelBlackoutPeriodID
    *
    *****************************************/

    String communicationChannelBlackoutPeriodID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (communicationChannelBlackoutPeriodID == null)
      {
        communicationChannelBlackoutPeriodID = communicationChannelBlackoutService.generateCommunicationChannelBlackoutID();
        jsonRoot.put("id", communicationChannelBlackoutPeriodID);
      }

    /*****************************************
    *
    *  existing CommunicationChannelBlackoutPeriod
    *
    *****************************************/

    GUIManagedObject existingCommunicationChannelBlackoutPeriod = communicationChannelBlackoutService.getStoredCommunicationChannelBlackout(communicationChannelBlackoutPeriodID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingCommunicationChannelBlackoutPeriod != null && existingCommunicationChannelBlackoutPeriod.getReadOnly())
      {
        response.put("id", existingCommunicationChannelBlackoutPeriod.getGUIManagedObjectID());
        response.put("accepted", existingCommunicationChannelBlackoutPeriod.getAccepted());
        response.put("valid", existingCommunicationChannelBlackoutPeriod.getAccepted());
        response.put("processing", communicationChannelBlackoutService.isActiveCommunicationChannelBlackout(existingCommunicationChannelBlackoutPeriod, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process CommunicationChannelBlackoutPeriod
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate CommunicationChannelBlackoutPeriod
        *
        ****************************************/

        CommunicationChannelBlackoutPeriod communicationChannelBlackoutPeriod = new CommunicationChannelBlackoutPeriod(jsonRoot, epoch, existingCommunicationChannelBlackoutPeriod);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        communicationChannelBlackoutService.putCommunicationChannelBlackout(communicationChannelBlackoutPeriod, (existingCommunicationChannelBlackoutPeriod == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", communicationChannelBlackoutPeriod.getGUIManagedObjectID());
        response.put("accepted", communicationChannelBlackoutPeriod.getAccepted());
        response.put("valid", communicationChannelBlackoutPeriod.getAccepted());
        response.put("processing", communicationChannelBlackoutService.isActiveCommunicationChannelBlackout(communicationChannelBlackoutPeriod, now));
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

        communicationChannelBlackoutService.putCommunicationChannelBlackout(incompleteObject, (existingCommunicationChannelBlackoutPeriod == null), userID);

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
        response.put("responseCode", "communicationChannelBlackoutPeriodNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveBlackoutPeriods
  *
  *****************************************/

  private JSONObject processRemoveBlackoutPeriods(String userID, JSONObject jsonRoot)
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

    String blackoutPeriodID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject existingBlackoutPeriod = communicationChannelBlackoutService.getStoredCommunicationChannelBlackout(blackoutPeriodID);
    if (existingBlackoutPeriod != null && (force || !existingBlackoutPeriod.getReadOnly()))
      {
        communicationChannelBlackoutService.removeCommunicationChannelBlackout(blackoutPeriodID, userID);
      }

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (existingBlackoutPeriod != null && (force || !existingBlackoutPeriod.getReadOnly()))
      {
        responseCode = "ok";
      }
    else if (existingBlackoutPeriod != null)
      {
        responseCode = "failedReadOnly";
      }
    else
      {
        responseCode = "communicationChannelBlackoutNotFound";
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
  *  processGetLoyaltyProgramTypeList
  *
  *****************************************/

  private JSONObject processGetLoyaltyProgramTypeList(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve loyalty program type list
    *
    *****************************************/

    List<JSONObject> programTypeList = new ArrayList<JSONObject>();
    for (LoyaltyProgramType programType : LoyaltyProgramType.values())
      {
        if(!programType.equals(LoyaltyProgramType.Unknown)){
          Map<String, Object> programTypeJSON = new HashMap<String, Object>();
          programTypeJSON.put("display", programType.getExternalRepresentation());
          programTypeJSON.put("name", programType.getExternalRepresentation());
          programTypeList.add(JSONUtilities.encodeObject(programTypeJSON));
        }
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("loyaltyProgramTypes", JSONUtilities.encodeArray(programTypeList));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetLoyaltyProgramList
  *
  *****************************************/

  private JSONObject processGetLoyaltyProgramList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve loyalty program list
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> loyaltyProgramList = new ArrayList<JSONObject>();
    for (GUIManagedObject loyaltyProgram : loyaltyProgramService.getStoredGUIManagedObjects(includeArchived))
      {
        JSONObject loyaltyPro = loyaltyProgramService.generateResponseJSON(loyaltyProgram, fullDetails, now);
        loyaltyProgramList.add(loyaltyPro);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("loyaltyPrograms", JSONUtilities.encodeArray(loyaltyProgramList));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetLoyaltyProgram
  *
  *****************************************/

  private JSONObject processGetLoyaltyProgram(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String loyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /**************************************************************
    *
    *  retrieve and decorate loyalty program
    *
    ***************************************************************/

    GUIManagedObject loyaltyProgram = loyaltyProgramService.getStoredLoyaltyProgram(loyaltyProgramID, includeArchived);
    JSONObject loyaltyProgramJSON = loyaltyProgramService.generateResponseJSON(loyaltyProgram, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (loyaltyProgram != null) ? "ok" : "loyaltyProgramNotFound");
    if (loyaltyProgram != null) response.put("loyaltyProgram", loyaltyProgramJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutLoyaltyProgram
  *
  *****************************************/

  private JSONObject processPutLoyaltyProgram(String userID, JSONObject jsonRoot)
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
    *  loyaltyProgramID
    *
    *****************************************/

    String loyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (loyaltyProgramID == null)
      {
        loyaltyProgramID = loyaltyProgramService.generateLoyaltyProgramID();
        jsonRoot.put("id", loyaltyProgramID);
      }

    /*****************************************
    *
    *  existing LoyaltyProgram
    *
    *****************************************/

    GUIManagedObject existingLoyaltyProgram = loyaltyProgramService.getStoredGUIManagedObject(loyaltyProgramID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingLoyaltyProgram != null && existingLoyaltyProgram.getReadOnly())
      {
        response.put("id", existingLoyaltyProgram.getGUIManagedObjectID());
        response.put("accepted", existingLoyaltyProgram.getAccepted());
        response.put("valid", existingLoyaltyProgram.getAccepted());
        response.put("processing", loyaltyProgramService.isActiveLoyaltyProgram(existingLoyaltyProgram, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process LoyaltyProgram
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate LoyaltyProgram
        *
        ****************************************/

        LoyaltyProgram loyaltyProgram = null;
        switch (LoyaltyProgramType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "loyaltyProgramType", true)))
        {
          case POINTS:
            loyaltyProgram = new LoyaltyProgramPoints(jsonRoot, epoch, existingLoyaltyProgram, catalogCharacteristicService);
            break;

//          case BADGES:
//            // TODO
//            break;

          case Unknown:
            throw new GUIManagerException("unsupported loyalty program type", JSONUtilities.decodeString(jsonRoot, "loyaltyProgramType", false));
        }

        /*****************************************
        *
        *  store
        *
        *****************************************/

        loyaltyProgramService.putLoyaltyProgram(loyaltyProgram, (existingLoyaltyProgram == null), userID);
        
        /*****************************************
        *
        *  add dynamic criterion fields)
        *
        *****************************************/

        dynamicCriterionFieldService.addLoyaltyProgramCriterionFields(loyaltyProgram, (existingLoyaltyProgram == null));

        /*****************************************
        *
        *  revalidate
        *
        *****************************************/

        revalidateSubscriberMessageTemplates(now);
        revalidateOffers(now);
        revalidateTargets(now);
        revalidateJourneys(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", loyaltyProgram.getGUIManagedObjectID());
        response.put("accepted", loyaltyProgram.getAccepted());
        response.put("valid", loyaltyProgram.getAccepted());
        response.put("processing", loyaltyProgramService.isActiveLoyaltyProgram(loyaltyProgram, now));
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

        loyaltyProgramService.putLoyaltyProgram(incompleteObject, (existingLoyaltyProgram == null), userID);

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
        response.put("responseCode", "loyaltyProgramNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
  *
  *  processRemoveLoyaltyProgram
  *
  *****************************************/

  private JSONObject processRemoveLoyaltyProgram(String userID, JSONObject jsonRoot){

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

    String loyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject existingLoyaltyProgram = loyaltyProgramService.getStoredGUIManagedObject(loyaltyProgramID);
    if (existingLoyaltyProgram != null && (force || !existingLoyaltyProgram.getReadOnly()))
      {
        //
        //  remove loyalty program
        //

        loyaltyProgramService.removeLoyaltyProgram(loyaltyProgramID, userID);

        //
        //  remove dynamic criterion fields
        //
        
        dynamicCriterionFieldService.removeLoyaltyProgramCriterionFields(existingLoyaltyProgram);

        //
        //  revalidate
        //

        revalidateSubscriberMessageTemplates(now);
        revalidateOffers(now);
        revalidateTargets(now);
        revalidateJourneys(now);
      }

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (existingLoyaltyProgram != null && (force || !existingLoyaltyProgram.getReadOnly()))
      {
        responseCode = "ok";
      }
    else if (existingLoyaltyProgram != null)
      {
        responseCode = "failedReadOnly";
      }
    else
      {
        responseCode = "loyaltyProgamNotFound";
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
  *  processGetResellerList
  *
  *****************************************/

  private JSONObject processGetResellerList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve sales partner list
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> resellerList = new ArrayList<JSONObject>();
    List<JSONObject> salesChannelsList = new ArrayList<JSONObject>();
    for (GUIManagedObject reseller : resellerService.getStoredGUIManagedObjects(includeArchived))
      {
        JSONObject loyaltyPro = loyaltyProgramService.generateResponseJSON(reseller, fullDetails, now);
        resellerList.add(loyaltyPro);
      } 
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();   
    response.put("responseCode", "ok");
    response.put("resellers", JSONUtilities.encodeArray(resellerList));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetReseller
  *
  *****************************************/

  private JSONObject processGetReseller(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String resellerID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /**************************************************************
    *
    *  retrieve and decorate partner
    *
    ***************************************************************/

    GUIManagedObject reseller = resellerService.getStoredGUIManagedObject(resellerID, includeArchived);
    JSONObject resellerJSON = resellerService.generateResponseJSON(reseller, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (reseller != null) ? "ok" : "resellerNotFound");
    if (reseller != null) response.put("reseller", resellerJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutReseller
  *
  *****************************************/

  private JSONObject processPutReseller(String userID, JSONObject jsonRoot)
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
    *  resellerID
    *
    *****************************************/

    String resellerID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (resellerID == null)
      {
        resellerID = resellerService.generateResellerID();
        jsonRoot.put("id", resellerID);
      }

    /*****************************************
    *
    *  existing Partner
    *
    *****************************************/

    GUIManagedObject existingReseller= resellerService.getStoredGUIManagedObject(resellerID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingReseller != null && existingReseller.getReadOnly())
      {
        response.put("id", existingReseller.getGUIManagedObjectID());
        response.put("accepted", existingReseller.getAccepted());
        response.put("valid", existingReseller.getAccepted());
        response.put("processing", resellerService.isActiveReseller(existingReseller, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process Partner
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate Partner
        *
        ****************************************/

        Reseller reseller = new Reseller(jsonRoot, epoch, existingReseller);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        resellerService.putReseller(reseller, (existingReseller == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", reseller.getGUIManagedObjectID());
        response.put("accepted", reseller.getAccepted());
        response.put("valid", reseller.getAccepted());
        response.put("processing", resellerService.isActiveReseller(reseller, now));
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

        resellerService.putReseller(incompleteObject, (existingReseller == null), userID);

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
        response.put("responseCode", "loyaltyProgramNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  *  processGetExclusionInclusionTargetList
  *
  *****************************************/

  private JSONObject processGetExclusionInclusionTargetList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert exclusionInclusionTargets
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> exclusionInclusionTargets = new ArrayList<JSONObject>();
    for (GUIManagedObject exclusionInclusionTarget : exclusionInclusionTargetService.getStoredExclusionInclusionTargets(includeArchived))
      {
        exclusionInclusionTargets.add(exclusionInclusionTargetService.generateResponseJSON(exclusionInclusionTarget, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("exclusionInclusionTargets", JSONUtilities.encodeArray(exclusionInclusionTargets));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetExclusionInclusionTarget
  *
  *****************************************/

  private JSONObject processGetExclusionInclusionTarget(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String exclusionInclusionTargetID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate exclusionInclusionTarget
    *
    *****************************************/

    GUIManagedObject exclusionInclusionTarget = exclusionInclusionTargetService.getStoredExclusionInclusionTarget(exclusionInclusionTargetID, includeArchived);
    JSONObject exclusionInclusionTargetJSON = exclusionInclusionTargetService.generateResponseJSON(exclusionInclusionTarget, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (exclusionInclusionTarget != null) ? "ok" : "exclusionInclusionTargetNotFound");
    if (exclusionInclusionTarget != null) response.put("exclusionInclusionTarget", exclusionInclusionTargetJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutExclusionInclusionTarget
  *
  *****************************************/

  private JSONObject processPutExclusionInclusionTarget(String userID, JSONObject jsonRoot)
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
    *  exclusionInclusionTargetID
    *
    *****************************************/

    String exclusionInclusionTargetID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (exclusionInclusionTargetID == null)
      {
        exclusionInclusionTargetID = subscriberGroupSharedIDService.generateID();
        jsonRoot.put("id", exclusionInclusionTargetID);
      }

    /*****************************************
    *
    *  existing exclusionInclusionTarget
    *
    *****************************************/

    GUIManagedObject existingExclusionInclusionTarget = exclusionInclusionTargetService.getStoredExclusionInclusionTarget(exclusionInclusionTargetID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingExclusionInclusionTarget != null && existingExclusionInclusionTarget.getReadOnly())
      {
        response.put("id", existingExclusionInclusionTarget.getGUIManagedObjectID());
        response.put("accepted", existingExclusionInclusionTarget.getAccepted());
        response.put("valid", existingExclusionInclusionTarget.getAccepted());
        response.put("processing", exclusionInclusionTargetService.isActiveExclusionInclusionTarget(existingExclusionInclusionTarget, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process exclusionInclusionTarget
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate exclusionInclusionTarget
        *
        ****************************************/

        ExclusionInclusionTarget exclusionInclusionTarget = new ExclusionInclusionTarget(jsonRoot, epoch, existingExclusionInclusionTarget);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        exclusionInclusionTargetService.putExclusionInclusionTarget(exclusionInclusionTarget, uploadedFileService, subscriberIDService, (existingExclusionInclusionTarget == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", exclusionInclusionTarget.getExclusionInclusionTargetID());
        response.put("accepted", exclusionInclusionTarget.getAccepted());
        response.put("valid", exclusionInclusionTarget.getAccepted());
        response.put("processing", exclusionInclusionTargetService.isActiveExclusionInclusionTarget(exclusionInclusionTarget, now));
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

        exclusionInclusionTargetService.putExclusionInclusionTarget(incompleteObject, uploadedFileService, subscriberIDService, (existingExclusionInclusionTarget == null), userID);

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
        response.put("responseCode", "exclusionInclusionTargetNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  *  processGetSegmentContactPolicyList
  *
  *****************************************/

  private JSONObject processGetSegmentContactPolicyList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived)
  {
    /*****************************************
    *
    *  retrieve and convert SegmentContactPolicys
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> segmentContactPolicys = new ArrayList<JSONObject>();
    for (GUIManagedObject segmentContactPolicy : segmentContactPolicyService.getStoredSegmentContactPolicys(includeArchived))
      {
        segmentContactPolicys.add(segmentContactPolicyService.generateResponseJSON(segmentContactPolicy, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("segmentContactPolicys", JSONUtilities.encodeArray(segmentContactPolicys));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processPutSegmentContactPolicy
  *
  *****************************************/

  private JSONObject processPutSegmentContactPolicy(String userID, JSONObject jsonRoot)
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
    *  segmentContactPolicyID -- there is only one -- add the hardwired ID to the requestb
    *
    *****************************************/


    jsonRoot.put("id", SegmentContactPolicy.singletonID);

    /*****************************************
    *
    *  existing existingSegmentContactPolicy
    *
    *****************************************/

    GUIManagedObject existingSegmentContactPolicy = segmentContactPolicyService.getStoredSegmentContactPolicy(SegmentContactPolicy.singletonID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingSegmentContactPolicy != null && existingSegmentContactPolicy.getReadOnly())
      {
        response.put("id", existingSegmentContactPolicy.getGUIManagedObjectID());
        response.put("accepted", existingSegmentContactPolicy.getAccepted());
        response.put("valid", existingSegmentContactPolicy.getAccepted());
        response.put("processing", segmentContactPolicyService.isActiveSegmentContactPolicy(existingSegmentContactPolicy, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process existingSegmentContactPolicy
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate existingSegmentContactPolicy
        *
        ****************************************/

        SegmentContactPolicy segmentContactPolicy = new SegmentContactPolicy(jsonRoot, epoch, existingSegmentContactPolicy);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        segmentContactPolicyService.putSegmentContactPolicy(segmentContactPolicy, contactPolicyService, segmentationDimensionService, (existingSegmentContactPolicy == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", segmentContactPolicy.getGUIManagedObjectID());
        response.put("accepted", segmentContactPolicy.getAccepted());
        response.put("valid", segmentContactPolicy.getAccepted());
        response.put("processing", segmentContactPolicyService.isActiveSegmentContactPolicy(segmentContactPolicy, now));
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

        segmentContactPolicyService.putSegmentContactPolicy(incompleteObject, contactPolicyService, segmentationDimensionService, (existingSegmentContactPolicy == null), userID);

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
        response.put("responseCode", "segmentContactPolicyNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  *  processGetSegmentContactPolicy
  *
  *****************************************/

  private JSONObject processGetSegmentContactPolicy(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String segmentContactPolicyID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate segmentContactPolicy
    *
    *****************************************/

    GUIManagedObject segmentContactPolicy = segmentContactPolicyService.getStoredSegmentContactPolicy(segmentContactPolicyID, includeArchived);
    JSONObject segmentContactPolicyJSON = segmentContactPolicyService.generateResponseJSON(segmentContactPolicy, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (segmentContactPolicy != null) ? "ok" : "segmentContactPolicyNotFound");
    if (segmentContactPolicy != null) response.put("exclusionInclusionTarget", segmentContactPolicyJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processRemoveSegmentContactPolicy
  *
  *****************************************/

  private JSONObject processRemoveSegmentContactPolicy(String userID, JSONObject jsonRoot)
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

    String segmentContactPolicyID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject segmentContactPolicy = segmentContactPolicyService.getStoredSegmentContactPolicy(segmentContactPolicyID);
    if (segmentContactPolicy != null && (force || !segmentContactPolicy.getReadOnly())) segmentContactPolicyService.removeSegmentContactPolicy(segmentContactPolicyID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (segmentContactPolicy != null && (force || !segmentContactPolicy.getReadOnly()))
      responseCode = "ok";
    else if (segmentContactPolicy != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "segmentContactPolicyNotFound";

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
  *  processRemoveExclusionInclusionTarget
  *
  *****************************************/

  private JSONObject processRemoveExclusionInclusionTarget(String userID, JSONObject jsonRoot)
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

    String exclusionInclusionTargetID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject exclusionInclusionTarget = exclusionInclusionTargetService.getStoredExclusionInclusionTarget(exclusionInclusionTargetID);
    if (exclusionInclusionTarget != null && (force || !exclusionInclusionTarget.getReadOnly())) exclusionInclusionTargetService.removeExclusionInclusionTarget(exclusionInclusionTargetID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (exclusionInclusionTarget != null && (force || !exclusionInclusionTarget.getReadOnly()))
      responseCode = "ok";
    else if (exclusionInclusionTarget != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "exclusionInclusionTargetNotFound";

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
  *  processEnterCampaign
   * @throws GUIManagerException 
  *
  *****************************************/
  
  private JSONObject processEnterCampaign(String userID, JSONObject jsonRoot) throws GUIManagerException
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

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    
    /*****************************************
    *
    *  resolve subscriberID
    *
    *****************************************/
    String responseCode = null;
    
    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        log.info("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID {}", getCustomerAlternateID, customerID);
        responseCode = "CustomerNotFound";
      }

    /*****************************************
    *
    *  getSubscriberProfile
    *
    *****************************************/

    SubscriberProfile baseSubscriberProfile = null;
    if (subscriberID != null)
      {
        try
          {
            baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true, false);
            if (baseSubscriberProfile == null)
              {
                responseCode = "CustomerNotFound";
              }
          }
        catch (SubscriberProfileServiceException e)
          {
            throw new GUIManagerException(e);
          }
      }
    
    Journey journey = null;
    if(responseCode == null)
      {
        String campaignName = JSONUtilities.decodeString(jsonRoot, "campaignName", false);
        String campaignID = "" + JSONUtilities.decodeInteger(jsonRoot, "campaignId", false); // MK hack
        Collection<Journey> allJourneys = journeyService.getActiveJourneys(SystemTime.getCurrentTime());
        if(allJourneys != null)
          {
            for(Journey activeJourney : allJourneys)
              {
                if(campaignName != null)
                  {
                    if(activeJourney.getJourneyName().equalsIgnoreCase(campaignName))
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
                else if(campaignID != null)
                  {
                    if(activeJourney.getJourneyID().equals(campaignID))
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
                else 
                  {
                    responseCode = "campaignName or campaignID must be provided";
                    break;
                  }
              }
          }
      }
    
    if(journey != null)
      {
        String uniqueKey = UUID.randomUUID().toString();
        JourneyRequest journeyRequest = new JourneyRequest(uniqueKey, subscriberID, journey.getJourneyID(), baseSubscriberProfile.getUniversalControlGroup());
        DeliveryManagerDeclaration journeyManagerDeclaration = Deployment.getDeliveryManagers().get(journeyRequest.getDeliveryType());
        String journeyRequestTopic = journeyManagerDeclaration.getDefaultRequestTopic();
        
        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
        kafkaProducerProperties.put("acks", "all");
        kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer journeyProducer = new KafkaProducer<byte[], byte[]>(kafkaProducerProperties);
        journeyProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getJourneyRequestTopic(), StringKey.serde().serializer().serialize(journeyRequestTopic, new StringKey(journeyRequest.getSubscriberID())), ((ConnectSerde<DeliveryRequest>)journeyManagerDeclaration.getRequestSerde()).serializer().serialize(journeyRequestTopic, journeyRequest)));
      
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

  /*****************************************
  *
  *  processCreditBonus
  *
  *****************************************/
  
  private JSONObject processCreditBonus(String userID, JSONObject jsonRoot) throws GUIManagerException
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

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String bonusName = JSONUtilities.decodeString(jsonRoot, "bonusName", true);
    Integer quantity = JSONUtilities.decodeInteger(jsonRoot, "quantity", true);
    String origin = JSONUtilities.decodeString(jsonRoot, "origin", true);
    
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
        return JSONUtilities.encodeObject(response);
      }
    
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
    CommodityDeliveryManager.sendCommodityDeliveryRequest(null, null, deliveryRequestID, null, true, deliveryRequestID, Module.Customer_Care.getExternalRepresentation(), origin, subscriberID, searchedBonus.getFulfillmentProviderID(), searchedBonus.getDeliverableID(), CommodityDeliveryOperation.Credit, quantity, null, 0);

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
  
  private JSONObject processDebitBonus(String userID, JSONObject jsonRoot)
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

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String bonusName = JSONUtilities.decodeString(jsonRoot, "bonusName", true);
    Integer quantity = JSONUtilities.decodeInteger(jsonRoot, "quantity", true);
    String origin = JSONUtilities.decodeString(jsonRoot, "origin", true);
    
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
        return JSONUtilities.encodeObject(response);
      }
    
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
    CommodityDeliveryManager.sendCommodityDeliveryRequest(null, null, deliveryRequestID, null, true, deliveryRequestID, Module.Customer_Care.getExternalRepresentation(), origin, subscriberID, searchedBonus.getFulfillmentProviderID(), searchedBonus.getPaymentMeanID(), CommodityDeliveryOperation.Debit, quantity, null, 0);
    
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
  *  processRemoveReseller
  *
  *****************************************/

  private JSONObject processRemoveReseller(String userID, JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    Date now = SystemTime.getCurrentTime();

    /****************************************
    *
    *  argument
    *
    ****************************************/

    String resellerID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
    

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject existingReseller = resellerService.getStoredGUIManagedObject(resellerID);
    long epoch = epochServer.getKey();
    if (existingReseller != null && (force || !existingReseller.getReadOnly()))
      {
        resellerService.removeReseller(resellerID, userID);
      }
    List<SalesChannel> storedSalesChannels = new ArrayList<SalesChannel>();
    for (GUIManagedObject storedSalesChannel : salesChannelService.getStoredSalesChannels())
      {
       if (storedSalesChannel instanceof SalesChannel ) storedSalesChannels.add((SalesChannel) storedSalesChannel); 
      }    
    for(SalesChannel sChannel : storedSalesChannels) {
      List<String> resellers = sChannel.getResellerIDs();     
      if (resellers!= null && resellers.size() > 0 && resellers.contains(resellerID))
        {
          try {           
          JSONObject salesChannelJSON = sChannel.getJSONRepresentation();
          JSONArray newResellers = new JSONArray();
          resellers.remove(resellerID);      
          for (String reseller : resellers) {
            newResellers.add(reseller);
          }
          
          /*****************************************
          *
          *  Remove the old Reseller list and update with the new resellers 
          *
          *****************************************/
          salesChannelJSON.replace("resellerIDs", newResellers);   
          
          SalesChannel newSalesChannel = new SalesChannel(salesChannelJSON, epoch, sChannel);         
          

          /*****************************************
          *
          *  store
          *
          *****************************************/

          salesChannelService.putSalesChannel(newSalesChannel, callingChannelService, resellerService, true, userID);

          /*****************************************
          *
          *  revalidateOffers
          *
          *****************************************/

          revalidateOffers(now);
        
          
          }
          catch (JSONUtilitiesException|GUIManagerException e){
            
            log.error("JSONUtilitiesException "+e.getMessage());
          
          }           
          
        }     
      }     


    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (existingReseller != null && (force || !existingReseller.getReadOnly()))
      {
        responseCode = "ok";
      }
    else if (existingReseller != null)
      {
        responseCode = "failedReadOnly";
      }
    else
      {
        responseCode = "resellerNotFound";
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
  *  processConfigAdaptorSupportedLanguages
  *
  *****************************************/

  private JSONObject processConfigAdaptorSupportedLanguages(JSONObject jsonRoot)
  {
    return processGetSupportedLanguages(null, jsonRoot);
  }
  
  /*****************************************
  *
  *  processConfigAdaptorSubscriberMessageTemplate
  *
  *****************************************/

  private JSONObject processConfigAdaptorSubscriberMessageTemplate(JSONObject jsonRoot)
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
    *  retrieve template
    *
    *****************************************/

    SubscriberMessageTemplate template = subscriberMessageTemplateService.getActiveSubscriberMessageTemplate(templateID, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  format result
    *
    *****************************************/

    HashMap<String,Object> templateJSON = new HashMap<String,Object>();
    if (template != null)
      {
        /*****************************************
        *
        *  messageByLanguagea
        *
        *****************************************/

        Map<String,Map<String,Object>> messageByLanguage = new HashMap<String,Map<String,Object>>();
        for (Entry<String, DialogMessage> entry : template.getDialogMessages().entrySet())
          {
            String messageTextField = entry.getKey();
            DialogMessage messageText = entry.getValue();
            for (String language : messageText.getMessageTextByLanguage().keySet())
              {
                //
                //  create entry for language (if necessary)
                //

                Map<String,Object> fieldsByLanguage = messageByLanguage.get(language);
                if (fieldsByLanguage == null)
                  {
                    fieldsByLanguage = new HashMap<String,Object>();
                    fieldsByLanguage.put("language", language);
                    messageByLanguage.put(language, fieldsByLanguage);
                  }

                //
                //  field
                //

                fieldsByLanguage.put(messageTextField, messageText.getMessageTextByLanguage().get(language));
              }
          }

        /*****************************************
        *
        *  messageJSON
        *
        *****************************************/

        List<JSONObject> messageJSON = new ArrayList<JSONObject>();
        for (Map<String,Object> fieldsByLanguage : messageByLanguage.values())
          {
            messageJSON.add(JSONUtilities.encodeObject(fieldsByLanguage));
          }

        /*****************************************
        *
        *  template
        *
        *****************************************/
        
        templateJSON.put("id", template.getSubscriberMessageTemplateID());
        templateJSON.put("name", template.getSubscriberMessageTemplateName());
        templateJSON.put("processing", subscriberMessageTemplateService.isActiveGUIManagedObject(template, SystemTime.getCurrentTime()));
        templateJSON.put("templateType", template.getTemplateType());
        templateJSON.put("message", JSONUtilities.encodeArray(messageJSON));
      }

    
    /*****************************************
    *
    *  response
    *
    *****************************************/


    response.put("responseCode", (template != null) ? "ok" : "templateNotFound");
    if (template != null) response.put("template", JSONUtilities.encodeObject(templateJSON));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processConfigAdaptorOffer
  *
  *****************************************/

  private JSONObject processConfigAdaptorOffer(JSONObject jsonRoot)
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

    //
    //  remove gui specific fields
    //
    
    offerJSON.remove("readOnly");
    offerJSON.remove("accepted");
    offerJSON.remove("valid");
    offerJSON.remove("active");

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
  *  processConfigAdaptorProduct
  *
  *****************************************/

  private JSONObject processConfigAdaptorProduct(JSONObject jsonRoot)
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

    //
    //  remove gui specific fields
    //
    
    productJSON.remove("readOnly");
    productJSON.remove("accepted");
    productJSON.remove("valid");
    productJSON.remove("active");

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
  *  processConfigAdaptorPresentationStrategy
  *
  *****************************************/

  private JSONObject processConfigAdaptorPresentationStrategy(JSONObject jsonRoot)
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

    //
    //  remove gui specific fields
    //
    
    presentationStrategyJSON.remove("readOnly");
    presentationStrategyJSON.remove("accepted");
    presentationStrategyJSON.remove("valid");
    presentationStrategyJSON.remove("active");

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
  *  processConfigAdaptorScoringStrategy
  *
  *****************************************/

  private JSONObject processConfigAdaptorScoringStrategy(JSONObject jsonRoot)
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

    //
    //  remove gui specific fields
    //
    
    scoringStrategyJSON.remove("readOnly");
    scoringStrategyJSON.remove("accepted");
    scoringStrategyJSON.remove("valid");
    scoringStrategyJSON.remove("active");
    
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
  *  processConfigAdaptorCallingChannel
  *
  *****************************************/

  private JSONObject processConfigAdaptorCallingChannel(JSONObject jsonRoot)
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

    //
    //  remove gui specific fields
    //
    
    callingChannelJSON.remove("readOnly");
    callingChannelJSON.remove("accepted");
    callingChannelJSON.remove("valid");
    callingChannelJSON.remove("active");
    
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
  *  processConfigAdaptorSalesChannel
  *
  *****************************************/

  private JSONObject processConfigAdaptorSalesChannel(JSONObject jsonRoot)
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

    //
    //  remove gui specific fields
    //
    
    salesChannelJSON.remove("readOnly");
    salesChannelJSON.remove("accepted");
    salesChannelJSON.remove("valid");
    salesChannelJSON.remove("active");
    
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
  *  processConfigAdaptorCommunicationChannel
  *
  *****************************************/

  private JSONObject processConfigAdaptorCommunicationChannel(JSONObject jsonRoot)
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

    String communicationChannelID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate communication channel
    *
    *****************************************/

    GUIManagedObject communicationChannel = communicationChannelService.getStoredCommunicationChannel(communicationChannelID);
    JSONObject communicationChannelJSON = communicationChannelService.generateResponseJSON(communicationChannel, true, SystemTime.getCurrentTime());

    //
    //  remove gui specific fields
    //
    
    communicationChannelJSON.remove("readOnly");
    communicationChannelJSON.remove("accepted");
    communicationChannelJSON.remove("valid");
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (communicationChannel != null) ? "ok" : "communicationChannelNotFound");
    if (communicationChannel != null) response.put("communicationChannel", communicationChannelJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processConfigAdaptorBlackoutPeriods
  *
  *****************************************/

  private JSONObject processConfigAdaptorBlackoutPeriods(JSONObject jsonRoot)
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

    String communicationChannelBlackoutPeriodID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /**************************************************************
    *
    *  retrieve and decorate communication channel blackout period
    *
    ***************************************************************/

    GUIManagedObject communicationChannelBlackoutPeriod = communicationChannelBlackoutService.getStoredCommunicationChannelBlackout(communicationChannelBlackoutPeriodID);
    JSONObject communicationChannelBlackoutPeriodJSON = communicationChannelBlackoutService.generateResponseJSON(communicationChannelBlackoutPeriod, true, SystemTime.getCurrentTime());

    //
    //  remove gui specific fields
    //
    
    communicationChannelBlackoutPeriodJSON.remove("readOnly");
    communicationChannelBlackoutPeriodJSON.remove("accepted");
    communicationChannelBlackoutPeriodJSON.remove("valid");
    communicationChannelBlackoutPeriodJSON.remove("active");
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (communicationChannelBlackoutPeriod != null) ? "ok" : "communicationChannelBlackoutPeriodNotFound");
    if (communicationChannelBlackoutPeriod != null) response.put("blackoutPeriods", communicationChannelBlackoutPeriodJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processConfigAdaptorContactPolicy
  *
  *****************************************/

  private JSONObject processConfigAdaptorContactPolicy(JSONObject jsonRoot)
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

    //
    //  remove gui specific fields
    //
    
    contactPolicyJSON.remove("readOnly");
    contactPolicyJSON.remove("accepted");
    contactPolicyJSON.remove("valid");
    contactPolicyJSON.remove("active");
    
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
  *  processConfigAdaptorSegmentationDimension
  *
  *****************************************/

  private JSONObject processConfigAdaptorSegmentationDimension(JSONObject jsonRoot)
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

    //
    //  remove gui specific fields
    //
    
    segmentationDimensionJSON.remove("readOnly");
    segmentationDimensionJSON.remove("accepted");
    segmentationDimensionJSON.remove("valid");
    segmentationDimensionJSON.remove("active");
    
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
  *  processConfigAdaptorCampaign
  *
  *****************************************/

  private JSONObject processConfigAdaptorCampaign(JSONObject jsonRoot)
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
    *  retrieve
    *
    *****************************************/

    GUIManagedObject journey = journeyService.getStoredJourney(journeyID);
    journey = (journey != null && journey.getGUIManagedObjectType() == GUIManagedObjectType.Campaign) ? journey : null;
    JSONObject journeyJSON = journeyService.generateResponseJSON(journey, true, SystemTime.getCurrentTime());

    //
    //  remove gui specific fields
    //
    
    journeyJSON.remove("readOnly");
    journeyJSON.remove("accepted");
    journeyJSON.remove("valid");
    journeyJSON.remove("effectiveEndDate");
    journeyJSON.remove("targetNoOfConversions");
    journeyJSON.remove("targetingEvent");
    journeyJSON.remove("effectiveEntryPeriodEndDate");
    journeyJSON.remove("maxNoOfCustomers");
    journeyJSON.remove("description");
    journeyJSON.remove("nodes");
    journeyJSON.remove("name");
    journeyJSON.remove("effectiveStartDate");
    journeyJSON.remove("eligibilityCriteria");
    journeyJSON.remove("targetingType");
    journeyJSON.remove("links");
    journeyJSON.remove("status");
    journeyJSON.remove("targetingCriteria");
    
    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (journey != null) ? "ok" : "campaignNotFound");
    if (journey != null) response.put("campaign", journeyJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processConfigAdaptorJourneyObjective
  *
  *****************************************/

  private JSONObject processConfigAdaptorJourneyObjective(JSONObject jsonRoot)
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

    //
    //  remove gui specific fields
    //
    
    journeyObjectiveJSON.remove("readOnly");
    journeyObjectiveJSON.remove("accepted");
    journeyObjectiveJSON.remove("valid");
    journeyObjectiveJSON.remove("targetingLimitWaitingPeriodDuration");
    journeyObjectiveJSON.remove("targetingLimitMaxSimultaneous");
    journeyObjectiveJSON.remove("display");
    journeyObjectiveJSON.remove("targetingLimitMaxOccurrence");
    journeyObjectiveJSON.remove("targetingLimitSlidingWindowTimeUnit");
    journeyObjectiveJSON.remove("targetingLimitWaitingPeriodTimeUnit");
    journeyObjectiveJSON.remove("targetingLimitSlidingWindowDuration");
    journeyObjectiveJSON.remove("name");
    journeyObjectiveJSON.remove("catalogCharacteristics");        
    journeyObjectiveJSON.remove("active");

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
  *  processConfigAdaptorProductType
  *
  *****************************************/

  private JSONObject processConfigAdaptorProductType(JSONObject jsonRoot)
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

    //
    //  remove gui specific fields
    //
    
    productTypeJSON.remove("readOnly");
    productTypeJSON.remove("accepted");
    productTypeJSON.remove("valid");
    productTypeJSON.remove("active");

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
  *  processConfigAdaptorOfferObjective
  *
  *****************************************/

  private JSONObject processConfigAdaptorOfferObjective(JSONObject jsonRoot)
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

    //
    //  remove gui specific fields
    //
    
    offerObjectiveJSON.remove("readOnly");
    offerObjectiveJSON.remove("accepted");
    offerObjectiveJSON.remove("valid");
    offerObjectiveJSON.remove("active");

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
  *  processConfigAdaptorScoringEngines
  *
  *****************************************/

  private JSONObject processConfigAdaptorScoringEngines(JSONObject jsonRoot)
  {
    return processGetScoringEngines(null, jsonRoot);
  }

  /*****************************************
  *
  *  processConfigAdaptorPresentationCriterionFields
  *
  *****************************************/

  private JSONObject processConfigAdaptorPresentationCriterionFields(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve presentation criterion fields
    *
    *****************************************/

    List<JSONObject> presentationCriterionFields = processCriterionFields(CriterionContext.Presentation.getCriterionFields(), false);

    //
    //  remove gui specific objects
    //

    for(JSONObject jSONObject : presentationCriterionFields)
      {
        jSONObject.remove("minValue");
        jSONObject.remove("operators");
        jSONObject.remove("maxValue");
        jSONObject.remove("display");
        jSONObject.remove("singletonComparableFields");
        jSONObject.remove("tagMaxLength");
        jSONObject.remove("availableValues");
        jSONObject.remove("setValuedComparableFields");
      }
    
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
  *  processConfigAdaptorDefaultNoftificationDailyWindows
  *
  *****************************************/

  private JSONObject processConfigAdaptorDefaultNoftificationDailyWindows(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    NotificationDailyWindows notifWindows = Deployment.getNotificationDailyWindows().get("0");
    response.put("defaultNoftificationDailyWindows", notifWindows.getJSONRepresentation());
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processConfigAdaptorDeliverable
  *
  *****************************************/

  private JSONObject processConfigAdaptorDeliverable(JSONObject jsonRoot)
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

    //
    //  remove gui specific fields
    //
    
    deliverableJSON.remove("readOnly");
    deliverableJSON.remove("accepted");
    deliverableJSON.remove("valid");
    deliverableJSON.remove("active");

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
   *  processGetTokensCodesList
   *
   *****************************************/

  private JSONObject processGetTokensCodesList(String userID, JSONObject jsonRoot) throws GUIManagerException
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
    
    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String tokenStatus = JSONUtilities.decodeString(jsonRoot, "tokenStatus", false);

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
        return JSONUtilities.encodeObject(response);
      }

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
            String str = "bad tokenStatus : " + tokenStatus;
            log.error(str);
            response.put("responseCode", str);
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
      SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      if (subscriberProfile == null)
        {
          response.put("responseCode", "CustomerNotFound");
        } 
      else
        {
          List<JSONObject> tokensJson;
          List<Token> tokens = subscriberProfile.getTokens();
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
                  .map(token -> ThirdPartyJSONGenerator.generateTokenJSONForThirdParty(token, journeyService, offerService, scoringStrategyService, offerObjectiveService, loyaltyProgramService))
                  .collect(Collectors.toList());
            }

          /*****************************************
          *
          *  decorate and response
          *
          *****************************************/

          response.put("tokens", JSONUtilities.encodeArray(tokensJson));
          response.put("responseCode", "ok");
        }
    }
    catch (SubscriberProfileServiceException e)
    {
      throw new GUIManagerException(e);
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

  private JSONObject processGetCustomerNBOs(String userID, JSONObject jsonRoot) throws GUIManagerException
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
    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String tokenCode = JSONUtilities.decodeString(jsonRoot, "tokenCode", false);
    Boolean viewOffersOnly = JSONUtilities.decodeBoolean(jsonRoot, "viewOffersOnly", Boolean.FALSE);

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
        return JSONUtilities.encodeObject(response);
      }

    Date now = SystemTime.getCurrentTime();

    /*****************************************
     *
     *  getSubscriberProfile
     *
     *****************************************/

    try
    {
      SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      if (subscriberProfile == null)
        {
          response.put("responseCode", "CustomerNotFound");
        } 
      else
        {
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
              String str = "No tokens returned";
              log.error(str);
              response.put("responseCode", str);
              generateTokenChange(subscriberID, now, tokenCode, userID, TokenChange.ALLOCATE, str);
              return JSONUtilities.encodeObject(response);
            }

          if (!(subscriberToken instanceof DNBOToken))
            {
              // TODO can this really happen ?
              String str = "Bad token type";
              log.error(str);
              response.put("responseCode", str);
              generateTokenChange(subscriberID, now, tokenCode, userID, TokenChange.ALLOCATE, str);
              return JSONUtilities.encodeObject(response);
            }

          DNBOToken subscriberStoredToken = (DNBOToken) subscriberToken;
          List<String> sss = subscriberStoredToken.getScoringStrategyIDs();
          if (sss == null)
            {
              String str = "Bad strategy : null value";
              log.error(str);
              response.put("responseCode", str);
              return JSONUtilities.encodeObject(response);
            }
          if (sss.size() == 0)
            {
              String str = "Bad strategy : empty list";
              log.error(str);
              response.put("responseCode", str);
              return JSONUtilities.encodeObject(response);
            }
          String strategyID = sss.get(0); // MK : not sure why we could have >1, only consider first one
          ScoringStrategy scoringStrategy = (ScoringStrategy) scoringStrategyService.getStoredScoringStrategy(strategyID);
          if (scoringStrategy == null)
            {
              String str = "Bad strategy : unknown id : "+strategyID;
              log.error(str);
              response.put("responseCode", str);
              generateTokenChange(subscriberID, now, tokenCode, userID, TokenChange.ALLOCATE, str);
              return JSONUtilities.encodeObject(response);
            }

          if (!viewOffersOnly)
            {
              StringBuffer returnedLog = new StringBuffer();
              double rangeValue = 0; // Not significant
              DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameters = new DNBOMatrixAlgorithmParameters(dnboMatrixService,rangeValue);

              // Allocate offers for this subscriber, and associate them in the token
              // Here we have no saleschannel (we pass null), this means only the first salesChannelsAndPrices of the offer will be used and returned.  
              Collection<ProposedOfferDetails> presentedOffers = TokenUtils.getOffers(
                  now, null,
                  subscriberProfile, scoringStrategy, productService,
                  productTypeService, catalogCharacteristicService,
                  propensityDataReader,
                  subscriberGroupEpochReader,
                  segmentationDimensionService, dnboMatrixAlgorithmParameters, offerService, returnedLog, subscriberID
                  );

              if (presentedOffers.isEmpty())
                {
                  generateTokenChange(subscriberID, now, tokenCode, userID, TokenChange.ALLOCATE, "no offers presented");
                }
              else
                {
                  // Send a PresentationLog to EvolutionEngine

                  String channelID = "channelID";
                  String presentationStrategyID = strategyID; // HACK, see above
                  String controlGroupState = "controlGroupState";
                  String featureID = (userID != null) ? userID : "1"; // for PTT tests, never happens when called by browser
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
                      scoringStrategyIDs.add(strategyID);
                    }
                  String salesChannelID = presentedOffers.iterator().next().getSalesChannelId(); // They all have the same one, set by TokenUtils.getOffers()
                  int transactionDurationMs = 0; // TODO
                  String callUniqueIdentifier = ""; 

                  PresentationLog presentationLog = new PresentationLog(
                      subscriberID, subscriberID, now, 
                      callUniqueIdentifier, channelID, salesChannelID, userID,
                      tokenCode, 
                      presentationStrategyID, transactionDurationMs, 
                      presentedOfferIDs, presentedOfferScores, positions, 
                      controlGroupState, scoringStrategyIDs, null, null, null, moduleID, featureID
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
          response = ThirdPartyJSONGenerator.generateTokenJSONForThirdParty(subscriberStoredToken, journeyService, offerService, scoringStrategyService, offerObjectiveService, loyaltyProgramService);
          response.put("responseCode", "ok");
        }
    }
    catch (SubscriberProfileServiceException e)
    {
      throw new GUIManagerException(e);
    }
    catch (GetOfferException e) 
    {
      log.error(e.getLocalizedMessage());
      throw new GUIManagerException(e) ;
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
   *  processAcceptOffer
   *
   *****************************************/

  private JSONObject processAcceptOffer(String userID, JSONObject jsonRoot) throws GUIManagerException
  {
    /****************************************
     *
     *  response
     *
     ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    Date now = SystemTime.getCurrentTime();

    /****************************************
     *
     *  argument
     *
     ****************************************/
    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String tokenCode = JSONUtilities.decodeString(jsonRoot, "tokenCode", false);
    String offerID = JSONUtilities.decodeString(jsonRoot, "offerID", false);
    String origin = JSONUtilities.decodeString(jsonRoot, "origin", false);

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
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
     *
     *  getSubscriberProfile
     *
     *****************************************/
    String deliveryRequestID = "";

    try
    {
      SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
      if (subscriberProfile == null)
        {
          response.put("responseCode", "CustomerNotFound");
        } 
      else
        {
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
              String str = "No tokens returned";
              log.error(str);
              response.put("responseCode", str);
              generateTokenChange(subscriberID, now, tokenCode, userID, TokenChange.REDEEM, str);
              return JSONUtilities.encodeObject(response);
            }

          if (!(subscriberToken instanceof DNBOToken))
            {
              // TODO can this really happen ?
              String str = "Bad token type";
              log.error(str);
              response.put("responseCode", str);
              generateTokenChange(subscriberID, now, tokenCode, userID, TokenChange.REDEEM, str);
              return JSONUtilities.encodeObject(response);
            }

          DNBOToken subscriberStoredToken = (DNBOToken) subscriberToken;

          if (subscriberStoredToken.getTokenStatus() == TokenStatus.Redeemed)
            {
              String str = "Token already in Redeemed state";
              log.error(str);
              response.put("responseCode", str);
              generateTokenChange(subscriberID, now, tokenCode, userID, TokenChange.REDEEM, str);
              return JSONUtilities.encodeObject(response);
            }

          if (subscriberStoredToken.getTokenStatus() != TokenStatus.Bound)
            {
              String str = "No offers allocated for this token";
              log.error(str);
              response.put("responseCode", str);
              generateTokenChange(subscriberID, now, tokenCode, userID, TokenChange.REDEEM, str);
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
              String str = "Offer has not been presented";
              log.error(str);
              response.put("responseCode", str);
              generateTokenChange(subscriberID, now, tokenCode, userID, TokenChange.REDEEM, str);
              return JSONUtilities.encodeObject(response);
            }
          String salesChannelID = subscriberStoredToken.getPresentedOffersSalesChannel();
          String featureID = (userID != null) ? userID : "1"; // for PTT tests, never happens when called by browser
          String moduleID = DeliveryRequest.Module.Customer_Care.getExternalRepresentation(); 
          Offer offer = offerService.getActiveOffer(offerID, now);
          deliveryRequestID = purchaseOffer(subscriberID, offerID, salesChannelID, 1, moduleID, featureID, origin, kafkaProducer);

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

          AcceptanceLog acceptanceLog = new AcceptanceLog(
              msisdn, subscriberID, now, 
              callUniqueIdentifier, channelID, salesChannelID,
              userID, tokenCode,
              presentationStrategyID, transactionDurationMs,
              controlGroupState, offerID, fulfilledDate, position, actionCall, moduleID, featureID);

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
          }
          response.put("responseCode", "ok");
        }
    }
    catch (SubscriberProfileServiceException e) 
    {
      throw new GUIManagerException(e);
    } 

    /*****************************************
     *
     *  decorate and response
     *
     *****************************************/
    response.put("deliveryRequestID", deliveryRequestID);
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processGetTokenEventDetails
  *
  *****************************************/

 private JSONObject processGetTokenEventDetails(String userID, JSONObject jsonRoot) throws GUIManagerException
 {
   /****************************************
    *
    *  response
    *
    ****************************************/

   HashMap<String,Object> response = new HashMap<String,Object>();
   Date now = SystemTime.getCurrentTime();

   /****************************************
    *
    *  argument
    *
    ****************************************/
   String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
   String tokenCode  = JSONUtilities.decodeString(jsonRoot, "tokenCode",  true);
   List<Object> res = new ArrayList<>();
   try
   {
     // (subscriberID:"106") AND (tokenCode:"YCWXT")
     String queryString = "("+TokenChangeESSinkConnector.TokenChangeESSinkTask.ES_FIELD_SUBSCRIBER_ID+":\""+customerID+"\") AND ("+TokenChangeESSinkConnector.TokenChangeESSinkTask.ES_FIELD_TOKEN_CODE+":\""+tokenCode+"\")";
     QueryBuilder query = QueryBuilders.queryStringQuery(queryString);
     SearchRequest searchRequest = new SearchRequest("detailedrecords_tokens-*").source(new SearchSourceBuilder().query(query)); 
     Scroll scroll = new Scroll(TimeValue.timeValueSeconds(10L));
     searchRequest.scroll(scroll);
     searchRequest.source().size(1000);
     SearchResponse searchResponse = elasticsearch.search(searchRequest, RequestOptions.DEFAULT);
     String scrollId = searchResponse.getScrollId(); // always null
     SearchHit[] searchHits = searchResponse.getHits().getHits();
     while (searchHits != null && searchHits.length > 0)
       {
         for (SearchHit searchHit : searchHits)
           {
             Map<String, Object> map = searchHit.getSourceAsMap();
             map.remove("subscriberID");
             map.remove("tokenCode");
             res.add(JSONUtilities.encodeObject(map));
           }
         SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
         scrollRequest.scroll(scroll);
         searchResponse = elasticsearch.searchScroll(scrollRequest, RequestOptions.DEFAULT);
         scrollId = searchResponse.getScrollId();
         searchHits = searchResponse.getHits().getHits();
       }
   }
   catch (IOException e)
     {
       String str = "Error when searching ElasticSearch";
       log.error(str);
       response.put("responseCode", str);
       response.put("responseMessage", e.getMessage());
       response.put("responseParameter", null);
       return JSONUtilities.encodeObject(response);
     }
   response.put("customerID", customerID);
   response.put("tokenCode", tokenCode);
   response.put("events", JSONUtilities.encodeArray(res));
   response.put("responseCode", "ok");
   
   /*****************************************
    *
    *  decorate and response
    *
    *****************************************/
   return JSONUtilities.encodeObject(response);
 }
 
  /*****************************************
   *
   * processGetSourceAddressList
   *
   *****************************************/

  private JSONObject processGetSourceAddressList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived) throws GUIManagerException
  {
    /****************************************
     *
     * response
     *
     ****************************************/

    List<JSONObject> sourceAddresses = new ArrayList<JSONObject>(); 
    HashMap<String,Object> response = new HashMap<String,Object>(); 
    
    /*****************************************
    *
    *  retrieve
    *
    *****************************************/
    Date now = SystemTime.getCurrentTime();
    
    for (GUIManagedObject sourceAddress : sourceAddressService.getStoredSourceAddresses(includeArchived))
      {
        JSONObject sourceAddressJSON = sourceAddressService.generateResponseJSON(sourceAddress, fullDetails, now); 
        sourceAddresses.add(sourceAddressJSON);
      }

    /*****************************************
    *
    * decorate and response
    *
    *****************************************/
    
    response.put("responseCode", "ok" );    
    response.put("sourceAddresses", JSONUtilities.encodeArray(sourceAddresses));  
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processGetSourceAddress
  *
  *****************************************/

  private JSONObject processGetSourceAddress(String userID, JSONObject jsonRoot, boolean includeArchived)
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

    String sourceAddressID = JSONUtilities.decodeString(jsonRoot, "id", true);

    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject sourceAddress = sourceAddressService.getStoredSourceAddress(sourceAddressID, includeArchived);
    JSONObject sourceAddressJSON = sourceAddressService.generateResponseJSON(sourceAddress, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (sourceAddress != null) ? "ok" : "sourceAddressNotFound");
    if (sourceAddress != null) response.put("sourceAddress", sourceAddressJSON);
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processPutSourceAddress
  *
  *****************************************/

  private JSONObject processPutSourceAddress(String userID, JSONObject jsonRoot)
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
    *  sourceAddressID
    *
    *****************************************/

    String sourceAddressID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (sourceAddressID == null)
      {
        sourceAddressID = sourceAddressService.generateSourceAddressID();
        jsonRoot.put("id", sourceAddressID);
      }

    /*****************************************
    *
    *  existing sourceAddress
    *
    *****************************************/

    GUIManagedObject existingSourceAddress = sourceAddressService.getStoredSourceAddress(sourceAddressID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingSourceAddress != null && existingSourceAddress.getReadOnly())
      {
        response.put("id", existingSourceAddress.getGUIManagedObjectID());
        response.put("accepted", existingSourceAddress.getAccepted());
        response.put("valid", existingSourceAddress.getAccepted());
        response.put("processing", sourceAddressService.isActiveSourceAddress(existingSourceAddress, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process sourceAddress
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        //
        //  isDefault
        //
        
        boolean isDefault = JSONUtilities.decodeBoolean(jsonRoot, "default", Boolean.FALSE);
        if (!isDefault) jsonRoot.put("default", isDefault);
        
        /****************************************
        *
        *  instantiate sourceAddress
        *
        ****************************************/
        
        SourceAddress sourceAddress = new SourceAddress(jsonRoot, epoch, existingSourceAddress);
        
        /*****************************************
        *
        *  store
        *
        *****************************************/
        
        sourceAddressService.putSourceAddress(sourceAddress, communicationChannelService, (existingSourceAddress == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", sourceAddress.getSourceAddressId());
        response.put("accepted", sourceAddress.getAccepted());
        response.put("valid", sourceAddress.getAccepted());
        response.put("processing", sourceAddressService.isActiveSourceAddress(sourceAddress, now));
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

        sourceAddressService.putSourceAddress(incompleteObject, communicationChannelService, (existingSourceAddress == null), userID);

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
        response.put("responseCode", "sourceAddressNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  *  processRemoveSourceAddress
  *
  *****************************************/

  private JSONObject processRemoveSourceAddress(String userID, JSONObject jsonRoot)
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

    String sourceAddressID = JSONUtilities.decodeString(jsonRoot, "id", true);
    boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);

    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject sourceAddress = sourceAddressService.getStoredSourceAddress(sourceAddressID);
    if (sourceAddress != null && (force || !sourceAddress.getReadOnly())) sourceAddressService.removeSourceAddress(sourceAddressID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (sourceAddress != null && (force || !sourceAddress.getReadOnly()))
      responseCode = "ok";
    else if (sourceAddress != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "sourceAddressNotFound";

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", responseCode);
    return JSONUtilities.encodeObject(response);
  }
  
  private static final Class<?> PURCHASE_FULFILLMENT_REQUEST_CLASS = com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest.class;

  /*****************************************
   *
   *  purchaseOffer
   *
   *****************************************/
  
  private String purchaseOffer(String subscriberID, String offerID, String salesChannelID, int quantity, 
      String moduleID, String featureID, String origin, KafkaProducer<byte[],byte[]> kafkaProducer) throws GUIManagerException
  {
    DeliveryManagerDeclaration deliveryManagerDeclaration = null;
    for (DeliveryManagerDeclaration dmd : Deployment.getDeliveryManagers().values())
      {
        String className = dmd.getRequestClassName();
        try
        {
          if (PURCHASE_FULFILLMENT_REQUEST_CLASS.isAssignableFrom(Class.forName(className)))
            {
              deliveryManagerDeclaration = dmd;
              // TODO : if we have multiple delivery managers of the right class, we should not always take the first one (for load balancing)
              break;
            }
        }
        catch (ClassNotFoundException e) {
          log.warn("DeliveryManager with wrong classname, skip it : "+className);
        }
      }
    if (deliveryManagerDeclaration == null)
      {
        String str = "Internal error, cannot find a deliveryManager with a RequestClassName as com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest"; 
        log.error(str);
        throw new GUIManagerException("Internal error", str);
      }
    String topic = deliveryManagerDeclaration.getDefaultRequestTopic();
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
    request.put("deliveryType", deliveryManagerDeclaration.getDeliveryType());
    JSONObject valueRes = JSONUtilities.encodeObject(request);
    
    PurchaseFulfillmentRequest pfr = new PurchaseFulfillmentRequest(valueRes, deliveryManagerDeclaration, offerService, paymentMeanService, SystemTime.getCurrentTime());

    // Write it to the right topic
    kafkaProducer.send(new ProducerRecord<byte[],byte[]>(
        topic,
        keySerializer.serialize(topic, new StringKey(deliveryRequestID)),
        valueSerializer.serialize(topic, pfr)
        ));
    return deliveryRequestID;
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

                        jsonResponse.put("id", fileID);
                        jsonResponse.put("accepted", true);
                        jsonResponse.put("valid", true);
                        jsonResponse.put("processing", true);
                        if(uploadedFile.getMetaData() != null && uploadedFile.getMetaData().get("segmentCounts") != null) 
                          {
                            jsonResponse.put("segmentCounts", uploadedFile.getMetaData().get("segmentCounts"));
                          }
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
            String csvFilenameRegex = reportName+ "_"+ ".*"+ "\\."+ fileExtension+ReportUtils.ZIP_EXTENSION;

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

              if(reportFile != null) {
                if(reportFile.length() > 0) {
                  try {
                    FileInputStream fis = new FileInputStream(reportFile);
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("Content-Disposition", "attachment; filename=" + reportFile.getName());
                    exchange.sendResponseHeaders(200, reportFile.length());
                    OutputStream os = exchange.getResponseBody();
                    byte data[] = new byte[10_000]; // allow some bufferization
                    int length;
                    while ((length = fis.read(data)) != -1) {
                      os.write(data, 0, length);
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
                  responseCode = "Report size is 0, report file is empty";
                }
              }else {
                responseCode = "Report is null, cant find this report";
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
  *  processCriterionFields
  *
  *****************************************/
  private List<JSONObject> processCriterionFields(Map<String,CriterionField> baseCriterionFields, boolean tagsOnly)
  {
    return processCriterionFields(baseCriterionFields, tagsOnly, null);
  }

  private List<JSONObject> processCriterionFields(Map<String,CriterionField> baseCriterionFields, boolean tagsOnly, Map<String, List<JSONObject>> currentGroups)
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

    int nextGroupID = 1;
    
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
            List<JSONObject> singleton = evaluateComparableFields(criterionField.getID(), criterionFieldJSON, defaultComparableFields, true);

            // TODO next line to be removed later when GUI handles the new "singletonComparableFieldsGroup" field
            criterionFieldJSON.put("singletonComparableFields", singleton);
            
            if (currentGroups != null)
              {
                // known group ?
                String groupID = null;
                for (String existingGroupID : currentGroups.keySet())
                  {
                    List<JSONObject> group = currentGroups.get(existingGroupID);
                    // is group the same list as singleton ?
                    if (singleton.size() != group.size()) continue; // cannot be the same
                    // TODO should be able to optimize next line
                    if (!singleton.containsAll(group)) continue;
                    groupID = existingGroupID;
                    break;
                  }
                if (groupID == null)
                  {
                    groupID = ""+nextGroupID;
                    log.trace("Found new group : "+groupID+" with "+singleton.size()+" elements");
                    currentGroups.put(groupID, singleton);
                    nextGroupID++;
                  }
                criterionFieldJSON.put("singletonComparableFieldsGroup", groupID);
              }
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
            //  rename fields
            //

            RenamedProfileCriterionField renamedProfileCriterionField = renamedProfileCriterionFieldReader.get(criterionField.getID());
            if (renamedProfileCriterionField != null) 
              {
                criterionFieldJSON.put("display", renamedProfileCriterionField.getDisplay());
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
        if (     (! excludedComparableFieldIDs.contains(comparableFieldID))
              && (singleton == criterionField.getFieldDataType().getSingletonType())
//            && (! comparableFieldID.equals(criterionFieldID))
           )
          {
            HashMap<String,Object> comparableFieldJSON = new HashMap<String,Object>();
            comparableFieldJSON.put("id", criterionField.getID());
            comparableFieldJSON.put("display", criterionField.getDisplay());
            result.add(JSONUtilities.encodeObject(comparableFieldJSON));
          }
      }

//    // order list by ID
//    Collections.sort(result, new Comparator<JSONObject>()
//    {
//      @Override
//      public int compare(JSONObject o1, JSONObject o2)
//      {
//        String id1 = (String) o1.get("id");
//        String id2 = (String) o2.get("id");
//        return id1.compareTo(id2);
//      }
//    });

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

  private List<JSONObject> processNodeTypes(Map<String,NodeType> nodeTypes, Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables) throws GUIManagerException
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

                CriterionContext criterionContext = new CriterionContext(journeyParameters, contextVariables, nodeType, (EvolutionEngineEventDeclaration) null, (Journey) null);
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

        case "callableWorkflows":
          if (includeDynamic)
            {
              for (GUIManagedObject workflowUnchecked : journeyService.getStoredJourneys())
                {
                  if (workflowUnchecked.getAccepted())
                    {
                      Journey workflow = (Journey) workflowUnchecked;
                      switch (workflow.getTargetingType())
                        {
                          case Manual:
                            switch (workflow.getGUIManagedObjectType())
                              {
                                case Workflow:
                                  HashMap<String,Object> availableValue = new HashMap<String,Object>();
                                  availableValue.put("id", workflow.getJourneyID());
                                  availableValue.put("display", workflow.getJourneyName());
                                  result.add(JSONUtilities.encodeObject(availableValue));
                                  break;
                              }
                            break;
                        }
                    }
                }
            }
          break;

        case "callableLoyaltyPrograms":
          if (includeDynamic)
            {
              for (GUIManagedObject loyaltyProgramUnchecked : loyaltyProgramService.getStoredLoyaltyPrograms())
                {
                  if (loyaltyProgramUnchecked.getAccepted())
                    {
                      LoyaltyProgram loyaltyProgram = (LoyaltyProgram) loyaltyProgramUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", loyaltyProgram.getLoyaltyProgramID());
                      availableValue.put("display", loyaltyProgram.getLoyaltyProgramDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
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

        case "deliverableIds":
          if (includeDynamic)
            {
              for (GUIManagedObject deliverablesUnchecked : deliverableService.getStoredDeliverables())
                {
                  if (deliverablesUnchecked.getAccepted())
                    {
                      Deliverable deliverable = (Deliverable) deliverablesUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", deliverable.getGUIManagedObjectID());
                      availableValue.put("display", deliverable.getDeliverableDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "deliverableNames":
          if (includeDynamic)
            {
              for (GUIManagedObject deliverablesUnchecked : deliverableService.getStoredDeliverables())
                {
                  if (deliverablesUnchecked.getAccepted())
                    {
                      Deliverable deliverable = (Deliverable) deliverablesUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", deliverable.getDeliverableName());
                      availableValue.put("display", deliverable.getDeliverableDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;
          
        case "deliveryStatuses":
          if (includeDynamic)
            {
              for (DeliveryStatus deliveryStatus : DeliveryManager.DeliveryStatus.values())
                {
                  HashMap<String,Object> availableValue = new HashMap<String,Object>();
                  availableValue.put("id", deliveryStatus.getExternalRepresentation());
                  availableValue.put("display", deliveryStatus.name());
                  result.add(JSONUtilities.encodeObject(availableValue));
                }
            }
          break;

        case "eventNames":
          for (EvolutionEngineEventDeclaration evolutionEngineEventDeclaration : dynamicEventDeclarationsService.getStaticAndDynamicEvolutionEventDeclarations().values())
            {
              HashMap<String,Object> availableValue = new HashMap<String,Object>();
              availableValue.put("id", evolutionEngineEventDeclaration.getName());
              availableValue.put("display", evolutionEngineEventDeclaration.getName());
              result.add(JSONUtilities.encodeObject(availableValue));
            }
          break;

        case "historicalBulkCampaigns":
          if (includeDynamic)
            {
              for (GUIManagedObject journeyUnchecked : journeyService.getStoredJourneys(false))
                {
                  if (journeyUnchecked.getAccepted() && journeyUnchecked.getGUIManagedObjectType() == GUIManagedObjectType.BulkCampaign)
                    {
                      Journey journey = (Journey) journeyUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", journey.getJourneyID());
                      availableValue.put("display", journey.getGUIManagedObjectDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "historicalCampaigns":
          if (includeDynamic)
            {
              for (GUIManagedObject journeyUnchecked : journeyService.getStoredJourneys(false))
                {
                  if (journeyUnchecked.getAccepted() && journeyUnchecked.getGUIManagedObjectType() == GUIManagedObjectType.Campaign)
                    {
                      Journey journey = (Journey) journeyUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", journey.getJourneyID());
                      availableValue.put("display", journey.getGUIManagedObjectDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "historicalJourneys":
          if (includeDynamic)
            {
              for (GUIManagedObject journeyUnchecked : journeyService.getStoredJourneys(false))
                {
                  if (journeyUnchecked.getAccepted() && journeyUnchecked.getGUIManagedObjectType() == GUIManagedObjectType.Journey)
                    {
                      Journey journey = (Journey) journeyUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", journey.getJourneyID());
                      availableValue.put("display", journey.getGUIManagedObjectDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "loyaltyPrograms":
          if (includeDynamic)
            {
              for (GUIManagedObject loyaltyProgramsUnchecked : loyaltyProgramService.getStoredLoyaltyPrograms())
                {
                  if (loyaltyProgramsUnchecked.getAccepted())
                    {
                      LoyaltyProgram loyaltyProgram = (LoyaltyProgram) loyaltyProgramsUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", loyaltyProgram.getLoyaltyProgramName());
                      availableValue.put("display", loyaltyProgram.getLoyaltyProgramDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "loyaltyProgramPointsEventNames":
          for (EvolutionEngineEventDeclaration evolutionEngineEventDeclaration : Deployment.getEvolutionEngineEvents().values())
            {
              try
                {
                  Class eventClass = Class.forName(evolutionEngineEventDeclaration.getEventClassName());
                  for(Class currentInterface : eventClass.getInterfaces()){
                    if(currentInterface.equals(LoyaltyProgramPointsEvent.class)){
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", evolutionEngineEventDeclaration.getName());
                      availableValue.put("display", evolutionEngineEventDeclaration.getName());
                      result.add(JSONUtilities.encodeObject(availableValue));
                      break;
                    }
                  }
                } catch (Exception e)
                {
                }
            }
          break;
          
        case "moduleIds":
          if (includeDynamic)
            {
              for (Module module : DeliveryRequest.Module.values())
                {
                  HashMap<String,Object> availableValue = new HashMap<String,Object>();
                  availableValue.put("id", module.getExternalRepresentation());
                  availableValue.put("display", module.name());
                  result.add(JSONUtilities.encodeObject(availableValue));
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

        case "offerIDs":
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

        case "offerObjectives":
          if (includeDynamic)
            {
              for (GUIManagedObject offerObjectiveUnchecked : offerObjectiveService.getStoredOfferObjectives())
                {
                  if (offerObjectiveUnchecked.getAccepted())
                    {
                      OfferObjective offerObjective = (OfferObjective) offerObjectiveUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", offerObjective.getOfferObjectiveID());
                      availableValue.put("display", offerObjective.getDisplay());
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
                            availableValue.put("display", tokenType.getTokenTypeDisplay());
                            result.add(JSONUtilities.encodeObject(availableValue));
                            break;
                        }
                    }
                }
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

        case "pointDeliverables":
          if (includeDynamic)
            {
              DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("pointFulfillment");
              JSONObject deliveryManagerJSON = (deliveryManager != null) ? deliveryManager.getJSONRepresentation() : null;
              String providerID = (deliveryManagerJSON != null) ? (String) deliveryManagerJSON.get("providerID") : null;
              if (providerID != null)
                {
                  for (GUIManagedObject deliverableUnchecked : deliverableService.getStoredDeliverables())
                    {
                      if (deliverableUnchecked.getAccepted())
                        {
                          Deliverable deliverable = (Deliverable) deliverableUnchecked;
                          if(deliverable.getFulfillmentProviderID().equals(providerID)){
                            HashMap<String,Object> availableValue = new HashMap<String,Object>();
                            availableValue.put("id", deliverable.getDeliverableID());
                            availableValue.put("display", deliverable.getDeliverableDisplay());
                            result.add(JSONUtilities.encodeObject(availableValue));
                          }
                        }
                    }
               }
            }
          break;

        case "pointPaymentMeans":
          if (includeDynamic)
            {
              DeliveryManagerDeclaration deliveryManager = Deployment.getDeliveryManagers().get("pointFulfillment");
              JSONObject deliveryManagerJSON = (deliveryManager != null) ? deliveryManager.getJSONRepresentation() : null;
              String providerID = (deliveryManagerJSON != null) ? (String) deliveryManagerJSON.get("providerID") : null;
              if (providerID != null)
                {
                  for (GUIManagedObject paymentMeanUnchecked : paymentMeanService.getStoredPaymentMeans())
                    {
                      if (paymentMeanUnchecked.getAccepted())
                        {
                          PaymentMean paymentMean = (PaymentMean) paymentMeanUnchecked;
                          if(paymentMean.getFulfillmentProviderID().equals(providerID)){
                            HashMap<String,Object> availableValue = new HashMap<String,Object>();
                            availableValue.put("id", paymentMean.getPaymentMeanID());
                            availableValue.put("display", paymentMean.getPaymentMeanDisplay());
                            result.add(JSONUtilities.encodeObject(availableValue));
                          }
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
                      availableValue.put("display", productType.getDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "providerIds":
          if (includeDynamic)
            {
              for(DeliveryManagerDeclaration deliveryManager : Deployment.getFulfillmentProviders().values())
                {
                  HashMap<String,Object> availableValue = new HashMap<String,Object>();
                  availableValue.put("id", deliveryManager.getProviderID());
                  availableValue.put("display", deliveryManager.getProviderName());
                  result.add(JSONUtilities.encodeObject(availableValue));
                }
            }
          break;

        case "pushTemplates_app":
          if (includeDynamic)
            {
              filterPushTemplates("3", result, now);  //Note : "3" is the id of the communication channel (defined in deployment.json)
            }
          break;
          
        case "pushTemplates_USSD":
          if (includeDynamic)
            {
              filterPushTemplates("4", result, now);  //Note : "4" is the id of the communication channel (defined in deployment.json)
            }
          break;

        case "scoringStrategies":
          if (includeDynamic)
            {
              for (GUIManagedObject scoringStrategyUnchecked : scoringStrategyService.getStoredScoringStrategies())
                {
                  if (scoringStrategyUnchecked.getAccepted())
                    {
                      ScoringStrategy scoringStrategy = (ScoringStrategy) scoringStrategyUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", scoringStrategy.getScoringStrategyID());
                      availableValue.put("display", scoringStrategy.getGUIManagedObjectDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
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
                      availableValue.put("display", dimension.getSegmentationDimensionDisplay() + ":" + segment.getName());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;
          
        case "smsTemplates":
          if (includeDynamic)
            {
              for (GUIManagedObject messageTemplateUnchecked : subscriberMessageTemplateService.getStoredSMSTemplates(true, false))
                {
                  if (messageTemplateUnchecked.getAccepted())
                    {
                      SMSTemplate messageTemplate = (SMSTemplate) messageTemplateUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", messageTemplate.getSubscriberMessageTemplateID());
                      availableValue.put("display", messageTemplate.getSubscriberMessageTemplateDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;

        case "subscriberJourneyStatuses":
          if (includeDynamic)
            {
              for (SubscriberJourneyStatus subscriberJourneyStatus : Journey.SubscriberJourneyStatus.values())
                {
                  switch (subscriberJourneyStatus)
                    {
                      case Unknown:
                        break;

                      default:
                        HashMap<String,Object> availableValue = new HashMap<String,Object>();
                        availableValue.put("id", subscriberJourneyStatus.getExternalRepresentation());
                        availableValue.put("display", subscriberJourneyStatus.getDisplay());
                        result.add(JSONUtilities.encodeObject(availableValue));
                        break;
                    }
                }
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

        case "supportedLanguages":
          for (SupportedLanguage supportedLanguage : Deployment.getSupportedLanguages().values())
            {
              HashMap<String,Object> availableValue = new HashMap<String,Object>();
              availableValue.put("id", supportedLanguage.getID());
              availableValue.put("display", supportedLanguage.getDisplay());
              result.add(JSONUtilities.encodeObject(availableValue));
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

        case "tokenTypes":
          if (includeDynamic)
            {
              for (GUIManagedObject tokenTypesUnchecked : tokenTypeService.getStoredTokenTypes())
                {
                  if (tokenTypesUnchecked.getAccepted())
                    {
                      TokenType tokenType = (TokenType) tokenTypesUnchecked;
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      availableValue.put("id", tokenType.getGUIManagedObjectID());
                      availableValue.put("display", tokenType.getTokenTypeDisplay());
                      result.add(JSONUtilities.encodeObject(availableValue));
                    }
                }
            }
          break;
          
        default:
          boolean foundValue = false;
          if(includeDynamic)
            {
              for(CriterionFieldAvailableValues availableValues : criterionFieldAvailableValuesService.getActiveCriterionFieldAvailableValues(now))
                {
                  if(availableValues.getGUIManagedObjectID().equals(reference))
                    {
                      HashMap<String,Object> availableValue = new HashMap<String,Object>();
                      if(availableValues.getAvailableValues() != null)
                        {
                          for(Pair<String, String> pair : availableValues.getAvailableValues())
                            {
                              availableValue.put("id", pair.getFirstElement());
                              availableValue.put("display", pair.getSecondElement());
                              result.add(JSONUtilities.encodeObject(availableValue));
                              foundValue = true;
                            }
                        }
                    }
                }
            }
          if (guiManagerExtensionEvaluateEnumeratedValuesMethod != null && !foundValue)
            {
              try
              {
                result.addAll((List<JSONObject>) guiManagerExtensionEvaluateEnumeratedValuesMethod.invoke(null, guiManagerContext, reference, now, includeDynamic));
              }
              catch (IllegalAccessException|InvocationTargetException|RuntimeException e)
              {
                log.error("failed deployment evaluate enumerated values for reference {}", reference);
                StringWriter stackTraceWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                log.error(stackTraceWriter.toString());
              }
            }
          break;
      }
    return result;
  }

  private void filterPushTemplates(String communicationChannelID, List<JSONObject> result, Date now){
    for (SubscriberMessageTemplate messageTemplate : subscriberMessageTemplateService.getActiveSubscriberMessageTemplates(now))
      {
        if (messageTemplate.getAccepted() && !messageTemplate.getInternalOnly())
          {
            switch (messageTemplate.getTemplateType())
            {
            case "push":
              PushTemplate pushTemplate = (PushTemplate) messageTemplate;
              if(pushTemplate.getCommunicationChannelID().equals(communicationChannelID)){
                HashMap<String,Object> availableValue = new HashMap<String,Object>();
                availableValue.put("id", messageTemplate.getSubscriberMessageTemplateID());
                //TODO : Gui is not sending the display field yet. Change this when GUI will be updated ...
                //availableValue.put("display", messageTemplate.getSubscriberMessageTemplateDisplay());
                availableValue.put("display", ((messageTemplate.getSubscriberMessageTemplateDisplay() != null && !messageTemplate.getSubscriberMessageTemplateDisplay().isEmpty()) ? messageTemplate.getSubscriberMessageTemplateDisplay() : messageTemplate.getSubscriberMessageTemplateName()));
                result.add(JSONUtilities.encodeObject(availableValue));
                break;
              }
            }
          }
      }
  }
  
  /*****************************************
  *
  *  deactivateOtherUCGRules
  *
  *****************************************/

  private void deactivateOtherUCGRules(UCGRule currentUCGRule,Date date)
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
        if(currentUCGRule.getGUIManagedObjectID() != existingUCGRule.getGUIManagedObjectID() && existingUCGRule.getActive() == true)
          {
            long epoch = epochServer.getKey();
            GUIManagedObject modifiedUCGRule;
            try
              {
                JSONObject existingRuleJSON = existingUCGRule.getJSONRepresentation();
                existingRuleJSON.replace("active", Boolean.FALSE);
                UCGRule ucgRule = new UCGRule(existingRuleJSON, epoch, existingUCGRule);
                //ucgRule.validate(ucgRuleService, segmentationDimensionService, date);
                modifiedUCGRule = ucgRule;
              }
            catch (JSONUtilitiesException | GUIManagerException e)
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
            Journey journey = new Journey(existingJourney.getJSONRepresentation(), existingJourney.getGUIManagedObjectType(), epoch, existingJourney, journeyService, catalogCharacteristicService, subscriberMessageTemplateService, dynamicEventDeclarationsService, communicationChannelService);
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
  *  revalidateSubscriberMessageTemplates
  *
  *****************************************/

  private void revalidateSubscriberMessageTemplates(Date date)
  {
    /****************************************
    *
    *  identify
    *
    ****************************************/

    Set<GUIManagedObject> modifiedSubscriberMessageTemplates = new HashSet<GUIManagedObject>();
    for (GUIManagedObject existingSubscriberMessageTemplate : subscriberMessageTemplateService.getStoredSubscriberMessageTemplates())
      {
        //
        //  modifiedSubscriberMessageTemplate
        //

        long epoch = epochServer.getKey();
        GUIManagedObject modifiedSubscriberMessageTemplate;
        try
          {
            SubscriberMessageTemplate subscriberMessageTemplate = null;
            if (existingSubscriberMessageTemplate instanceof SMSTemplate) subscriberMessageTemplate = new SMSTemplate(communicationChannelService, existingSubscriberMessageTemplate.getJSONRepresentation(), epoch, existingSubscriberMessageTemplate);
            if (existingSubscriberMessageTemplate instanceof MailTemplate) subscriberMessageTemplate = new MailTemplate(communicationChannelService, existingSubscriberMessageTemplate.getJSONRepresentation(), epoch, existingSubscriberMessageTemplate);
            if (existingSubscriberMessageTemplate instanceof PushTemplate) subscriberMessageTemplate = new PushTemplate(communicationChannelService, existingSubscriberMessageTemplate.getJSONRepresentation(), epoch, existingSubscriberMessageTemplate);
            if ( !(existingSubscriberMessageTemplate instanceof IncompleteObject) && subscriberMessageTemplate == null) throw new ServerRuntimeException("illegal subscriberMessageTemplate");            
            modifiedSubscriberMessageTemplate = subscriberMessageTemplate;
          }
        catch (JSONUtilitiesException|GUIManagerException e)
          {
            modifiedSubscriberMessageTemplate = new IncompleteObject(existingSubscriberMessageTemplate.getJSONRepresentation(), existingSubscriberMessageTemplate.getGUIManagedObjectType(), epoch);
          }

        //
        //  changed?
        //

        if (modifiedSubscriberMessageTemplate != null && existingSubscriberMessageTemplate.getAccepted() != modifiedSubscriberMessageTemplate.getAccepted())
          {
            modifiedSubscriberMessageTemplates.add(modifiedSubscriberMessageTemplate);
          }
      }

    /****************************************
    *
    *  update
    *
    ****************************************/

    for (GUIManagedObject modifiedSubscriberMessageTemplate : modifiedSubscriberMessageTemplates)
      {
        subscriberMessageTemplateService.putGUIManagedObject(modifiedSubscriberMessageTemplate, date, false, null);
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
            Product product = new Product(existingProduct.getJSONRepresentation(), epoch, existingProduct, deliverableService, catalogCharacteristicService);
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
            salesChannel.validate(callingChannelService, resellerService, date);
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
  *  prepareEndDate
  *
  *****************************************/

  private Date prepareEndDate(Date endDate)
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

  private Date prepareStartDate(Date startDate)
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
  *  resolveJourneyParameters
  *
  *****************************************/

  private JSONObject resolveJourneyParameters(JSONObject journeyJSON, Date now)
  {
    //
    //  resolve
    //

    List<JSONObject>  resolvedParameters = new ArrayList<JSONObject>();
    JSONArray parameters = JSONUtilities.decodeJSONArray(journeyJSON, "journeyParameters", new JSONArray());
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
        //  result
        //

        resolvedParameters.add(parameterJSON);
      }

    //
    //  replace
    //

    journeyJSON.put("journeyParameters", JSONUtilities.encodeArray(resolvedParameters));

    //
    //  return
    //

    return journeyJSON;
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
  *  class SharedIDService
  *
  *****************************************/

  public static class SharedIDService
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private int lastGeneratedObjectID = 0;
    private Set<GUIService> baseServices;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public SharedIDService(GUIService... baseServices)
    {
      this.baseServices = new HashSet<GUIService>();
      for (GUIService guiService : baseServices)
        {
          this.baseServices.add(guiService);
          lastGeneratedObjectID = Math.max(lastGeneratedObjectID, guiService.getLastGeneratedObjectID());
        }
    }

    /*****************************************
    *
    *  generateGUIManagedObjectID
    *
    *****************************************/

    public String generateID()
    {
      synchronized (this)
        {
          //
          //  update lastGeneratedObjectID
          //

          for (GUIService guiService : baseServices)
            {
              lastGeneratedObjectID = Math.max(lastGeneratedObjectID, guiService.getLastGeneratedObjectID());
            }

          //
          //  generate
          //

          lastGeneratedObjectID += 1;
          return String.format(Deployment.getGenerateNumericIDs() ? "%d" : "%03d", lastGeneratedObjectID);
        }
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
      consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
      consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
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
          //  poll
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
              
          //
          //  commit offsets
          //

          consumer.commitSync();
        }
      while (!stopRequested);
    }
  }

  /*****************************************
  *
  *  class RenamedProfileCriterionField
  *
  *****************************************/

  public static class RenamedProfileCriterionField implements ReferenceDataValue<String>
  {
    /****************************************
    *
    *  data
    *
    ****************************************/

    private String criterionFieldID;
    private String display;
    private boolean deleted;

    /****************************************
    *
    *  accessors - basic
    *
    ****************************************/

    public String getCriterionFieldID() { return criterionFieldID; }
    public String getDisplay() { return display; }
    public boolean getDeleted() { return deleted; }

    //
    //  ReferenceDataValue
    //

    @Override public String getKey() { return criterionFieldID; }

    /*****************************************
    *
    *  constructor (unpack)
    *
    *****************************************/

    private RenamedProfileCriterionField(String criterionFieldID, String display, boolean deleted)
    {
      this.criterionFieldID = criterionFieldID;
      this.display = display;
      this.deleted = deleted;
    }

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static RenamedProfileCriterionField unpack(org.apache.kafka.connect.data.SchemaAndValue schemaAndValue)
    {
      //
      //  data
      //

      org.apache.kafka.connect.data.Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? com.evolving.nglm.core.SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

      //
      //  unpack
      //

      org.apache.kafka.connect.data.Struct valueStruct = (org.apache.kafka.connect.data.Struct) value;

      String criterionFieldID = valueStruct.getString("criterion_field_id");
      String display = valueStruct.getString("display");
      boolean deleted = valueStruct.getInt8("deleted").intValue() != 0;

      //
      //  return
      //

      return new RenamedProfileCriterionField(criterionFieldID, display, deleted);
    }
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
    private SourceAddressService sourceAddressService;
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
    private VoucherTypeService voucherTypeService;
    private VoucherService voucherService;
    private SubscriberMessageTemplateService subscriberMessageTemplateService;
    private SubscriberProfileService subscriberProfileService;
    private SubscriberIDService subscriberIDService;
    private DeliverableSourceService deliverableSourceService;
    private UploadedFileService uploadedFileService;
    private TargetService targetService;
    private CommunicationChannelService communicationChannelService;
    private CommunicationChannelBlackoutService communicationChannelBlackoutService;
    private LoyaltyProgramService loyaltyProgramService;
    private ExclusionInclusionTargetService exclusionInclusionTargetService;
    private ResellerService resellerService;
    private SegmentContactPolicyService segmentContactPolicyService;
    private CriterionFieldAvailableValuesService criterionFieldAvailableValuesService;

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
    public SourceAddressService getSourceAddressService() { return sourceAddressService; }
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
    public VoucherTypeService getVoucherTypeService() { return voucherTypeService; }
    public VoucherService getVoucherService() { return voucherService; }
    public SubscriberMessageTemplateService getSubscriberMessageTemplateService() { return subscriberMessageTemplateService; }
    public SubscriberProfileService getSubscriberProfileService() { return subscriberProfileService; }
    public SubscriberIDService getSubscriberIDService() { return subscriberIDService; }
    public DeliverableSourceService getDeliverableSourceService() { return deliverableSourceService; }
    public UploadedFileService getUploadFileService() { return uploadedFileService; }
    public TargetService getTargetService() { return targetService; }
    public CommunicationChannelService getCommunicationChannelService() { return communicationChannelService; }
    public CommunicationChannelBlackoutService getCommunicationChannelBlackoutService() { return communicationChannelBlackoutService; }
    public LoyaltyProgramService getLoyaltyProgramService() { return loyaltyProgramService; }
    public ExclusionInclusionTargetService getExclusionInclusionTargetService() { return exclusionInclusionTargetService; }
    public ResellerService getPartnerService() { return resellerService; }
    public SegmentContactPolicyService getSegmentContactPolicyService() { return segmentContactPolicyService; }
    public CriterionFieldAvailableValuesService getCriterionFieldAvailableValuesService() { return criterionFieldAvailableValuesService; }
    

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public GUIManagerContext(JourneyService journeyService, SegmentationDimensionService segmentationDimensionService, PointService pointService, OfferService offerService, ReportService reportService, PaymentMeanService paymentMeanService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, CallingChannelService callingChannelService, SalesChannelService salesChannelService, SourceAddressService sourceAddressService, SupplierService supplierService, ProductService productService, CatalogCharacteristicService catalogCharacteristicService, ContactPolicyService contactPolicyService, JourneyObjectiveService journeyObjectiveService, OfferObjectiveService offerObjectiveService, ProductTypeService productTypeService, UCGRuleService ucgRuleService, DeliverableService deliverableService, TokenTypeService tokenTypeService, VoucherTypeService voucherTypeService, VoucherService voucherService, SubscriberMessageTemplateService subscriberTemplateService, SubscriberProfileService subscriberProfileService, SubscriberIDService subscriberIDService, DeliverableSourceService deliverableSourceService, UploadedFileService uploadedFileService, TargetService targetService, CommunicationChannelService communicationChannelService, CommunicationChannelBlackoutService communicationChannelBlackoutService, LoyaltyProgramService loyaltyProgramService, ResellerService resellerService, ExclusionInclusionTargetService exclusionInclusionTargetService, SegmentContactPolicyService segmentContactPolicyService, CriterionFieldAvailableValuesService criterionFieldAvailableValuesService)
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
      this.sourceAddressService = sourceAddressService;
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
      this.voucherTypeService = voucherTypeService;
      this.voucherService = voucherService;
      this.subscriberMessageTemplateService = subscriberMessageTemplateService;
      this.subscriberProfileService = subscriberProfileService;
      this.subscriberIDService = subscriberIDService;
      this.deliverableSourceService = deliverableSourceService;
      this.uploadedFileService = uploadedFileService;
      this.targetService = targetService;
      this.communicationChannelService = communicationChannelService;
      this.communicationChannelBlackoutService = communicationChannelBlackoutService;
      this.loyaltyProgramService = loyaltyProgramService;
      this.exclusionInclusionTargetService = exclusionInclusionTargetService;
      this.resellerService = resellerService;
      this.segmentContactPolicyService = segmentContactPolicyService;
      this.criterionFieldAvailableValuesService = criterionFieldAvailableValuesService;
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
      return super.toString() + " (" + responseParameter + ")";
    }
  }

  /*****************************************
  *
  *  normalizeSegmentName
  *
  *****************************************/

  public static String normalizeSegmentName(String segmentName)
  {
    return segmentName.replace(" ",".").toLowerCase();
  }

  /*****************************************
  *
  *  validateURIandContext
  *
  *****************************************/

  private void validateURIandContext(HttpExchange exchange) throws GUIManagerException
  {
    String path = exchange.getRequestURI().getPath();
    if (path.endsWith("/")) path = path.substring(0, path.length()-1);

    //
    //  validate
    //

    if (! path.equals(exchange.getHttpContext().getPath()))
      {
        log.warn("invalid url {} should be {}", path, exchange.getHttpContext().getPath());
        throw new GUIManagerException("invalid URL", "404");
      }
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
  
  /*****************************************
  *
  *  checkDeliverableIDs
  *
  *****************************************/

  public static boolean checkDeliverableIDs(JSONArray deliverableIDs, String idToCheck)
  {
    if(deliverableIDs != null)
      {
        for(int i=0; i<deliverableIDs.size(); i++)
          {
            if(deliverableIDs.get(i).toString().equals(idToCheck))
              {
                return true;
              }
          }
      }
    return false;
  }  

  /*****************************************
  *
  *  generateTokenChange
  *
  *****************************************/

  private void generateTokenChange(String subscriberID, Date now, String tokenCode, String userID, String action, String str)
  {
    String topic = Deployment.getTokenChangeTopic();
    Serializer<StringKey> keySerializer = StringKey.serde().serializer();
    Serializer<TokenChange> valueSerializer = TokenChange.serde().serializer();
    int userIDint = 1;
    try
    {
      userIDint = (userID != null) ? Integer.parseInt(userID) : 1;
    }
    catch (NumberFormatException e)
    {
      log.warn("userID is not an integer : " + userID + " using " + userIDint);
    }
    TokenChange tokenChange = new TokenChange(subscriberID, now, "", tokenCode, action, str, "guiManager", Module.Customer_Care, userIDint); 
    kafkaProducer.send(new ProducerRecord<byte[],byte[]>(
        topic,
        keySerializer.serialize(topic, new StringKey(subscriberID)),
        valueSerializer.serialize(topic, tokenChange)
        ));
  }

}

