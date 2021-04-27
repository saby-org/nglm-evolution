/*****************************************************************************
 *
 *  Deployment.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.File;
import java.util.*;

import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.kafka.Topic;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration.EventRule;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.datacubes.SubscriberProfileDatacubeMetric;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchConnectionSettings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Deployment extends com.evolving.nglm.core.Deployment
{
  /*****************************************
   *
   *  data
   *
   *****************************************/

  //
  //  log
  //

  private static final Logger log = LoggerFactory.getLogger(Deployment.class);
  
  //
  // deploymentsPerTenant
  //
  
  private static Map<Integer, Deployment> deploymentsPerTenant = new HashMap<>();
  private static Object lock = new Object();
  
  private static boolean loaded; // If the java class is statically loaded 
  public static boolean isDeploymentLoaded() { return loaded; }

  //
  //  data
  //

  private static int topicSubscriberPartitions;
  private static int topicReplication;
  private static String topicMinInSyncReplicas;
  private static String topicRetentionShortMs;
  private static String topicRetentionMs;
  private static String topicRetentionLongMs;
  private static int httpServerScalingFactor = 1;    
  private static int evolutionEngineStreamThreads;
  private static int evolutionEngineInstanceNumbers;
  private String subscriberGroupLoaderAlternateID;
  private static String getCustomerAlternateID;
  private boolean subscriberGroupLoaderAutoProvision;
  private static String criterionFieldRetrieverClassName;
  private static String evolutionEngineExtensionClassName;
  private static String guiManagerExtensionClassName;
  private static String subscriberProfileClassName;
  private static String extendedSubscriberProfileClassName;
  private static String evolutionEngineExternalAPIClassName;
  private static Map<String,EvolutionEngineEventDeclaration> evolutionEngineEvents = new LinkedHashMap<String,EvolutionEngineEventDeclaration>();
  private Map<String, CriterionField> profileChangeDetectionCriterionFields = new HashMap<>();
  private Map<String, CriterionField> profileChangeGeneratedCriterionFields = new HashMap<>();
  private boolean enableProfileSegmentChange;
  private static String journeyTopic;
  private static String journeyTemplateTopic;
  private static String segmentationDimensionTopic;
  private static String pointTopic;
  private static String complexObjectTypeTopic;
  private static String offerTopic;
  private static String reportTopic;
  private static String paymentMeanTopic;
  private static String presentationStrategyTopic;
  private static String scoringStrategyTopic;
  private static String callingChannelTopic;
  private static String salesChannelTopic;
  private static String supplierTopic;
  private static String resellerTopic;
  private static String productTopic;
  private static String catalogCharacteristicTopic;
  private static String contactPolicyTopic;
  private static String journeyObjectiveTopic;
  private static String offerObjectiveTopic;
  private static String productTypeTopic;
  private static String ucgRuleTopic;
  private static String deliverableTopic;
  private static String tokenTypeTopic;
  private static String voucherTypeTopic;
  private static String voucherTopic;
  private static String subscriberMessageTemplateTopic;
  private static String guiAuditTopic;
  private static String subscriberGroupTopic;
  private static String subscriberGroupAssignSubscriberIDTopic;
  private static String subscriberGroupEpochTopic;
  private static String ucgStateTopic;
  private static String renamedProfileCriterionFieldTopic;
  private static String timedEvaluationTopic;
  private static String evaluateTargetsTopic;
  private static String subscriberProfileForceUpdateTopic;
  private static String executeActionOtherSubscriberTopic;
  private static String subscriberStateChangeLog;
  private static String subscriberStateChangeLogTopic;
  private static String extendedSubscriberProfileChangeLog;
  private static String extendedSubscriberProfileChangeLogTopic;
  private static String subscriberHistoryChangeLog;
  private static String subscriberHistoryChangeLogTopic;
  private static String journeyStatisticTopic;
  private static String journeyMetricTopic;
  private static String presentationLogTopic;
  private static String acceptanceLogTopic;
  private static String profileLoyaltyProgramChangeEventTopic;
  private static String profileChangeEventTopic;
  private static String profileSegmentChangeEventTopic;
  private static String voucherActionTopic;
  private static String fileWithVariableEventTopic;
  private static String tokenRedeemedTopic;
  private int propensityInitialisationPresentationThreshold;
  private int propensityInitialisationDurationInDaysThreshold;
  private static String tokenChangeTopic;
  private static String subscriberProfileRegistrySubject;
  private static Map<String,ScheduledJobConfiguration> datacubeJobsScheduling = new LinkedHashMap<String,ScheduledJobConfiguration>();
  private static Map<String,ScheduledJobConfiguration> elasticsearchJobsScheduling = new LinkedHashMap<String,ScheduledJobConfiguration>();
  private PropensityRule propensityRule;
  private Map<String,Long> journeyTemplateCapacities = new LinkedHashMap<String,Long>();
  private Map<String,SupportedLanguage> supportedLanguages = new LinkedHashMap<String,SupportedLanguage>();
  private static Map<String,ExternalAPITopic> externalAPITopics = new LinkedHashMap<String,ExternalAPITopic>();
  private String baseLanguageID;
  private Map<String,SupportedCurrency> supportedCurrencies = new LinkedHashMap<String,SupportedCurrency>();
  private Map<String,SupportedTimeUnit> supportedTimeUnits = new LinkedHashMap<String,SupportedTimeUnit>();
  private Map<String,SupportedTokenCodesFormat> supportedTokenCodesFormats = new LinkedHashMap<String,SupportedTokenCodesFormat>();
  private Map<String,SupportedVoucherCodePattern> supportedVoucherCodePatternList = new LinkedHashMap<String,SupportedVoucherCodePattern>();
  private Map<String,SupportedRelationship> supportedRelationships = new LinkedHashMap<String,SupportedRelationship>();
  private Map<String,CallingChannelProperty> callingChannelProperties = new LinkedHashMap<String,CallingChannelProperty>();
  private Map<String,CatalogCharacteristicUnit> catalogCharacteristicUnits = new LinkedHashMap<String,CatalogCharacteristicUnit>();
  private Map<String,PartnerType> partnerTypes = new LinkedHashMap<String,PartnerType>();
  private Map<String,BillingMode> billingModes = new LinkedHashMap<String,BillingMode>();
  private JSONArray initialCallingChannelsJSONArray = null;
  private JSONArray initialSalesChannelsJSONArray = null;
  private JSONArray initialSourceAddressesJSONArray = null;
  private JSONArray initialSuppliersJSONArray = null;
  private JSONArray initialPartnersJSONArray = null;
  private JSONArray initialProductsJSONArray = null;
  private JSONArray initialReportsJSONArray = null;
  private JSONArray initialCatalogCharacteristicsJSONArray = null;
  private JSONArray initialContactPoliciesJSONArray = null;
  private JSONArray initialJourneyTemplatesJSONArray = null;
  private JSONArray initialJourneyObjectivesJSONArray = null;
  private JSONArray initialOfferObjectivesJSONArray = null;
  private JSONArray initialProductTypesJSONArray = null;
  private JSONArray initialTokenTypesJSONArray = null;
  private JSONArray initialVoucherCodeFormatsJSONArray = null;
  private JSONArray initialScoringTypesJSONArray = null;
  private JSONArray initialSegmentationDimensionsJSONArray = null;
  private static JSONArray initialComplexObjectJSONArray = null;
  private boolean generateSimpleProfileDimensions;
  private static Map<String,CommunicationChannel> communicationChannels = new LinkedHashMap<>();
  private Map<String,SupportedDataType> supportedDataTypes = new LinkedHashMap<String,SupportedDataType>();
  private static JourneyMetricConfiguration journeyMetricConfiguration = null;
  private static Map<String,SubscriberProfileDatacubeMetric> subscriberProfileDatacubeMetrics = new LinkedHashMap<String,SubscriberProfileDatacubeMetric>();
  private static Map<String,CriterionField> profileCriterionFields = new LinkedHashMap<String,CriterionField>();
  private static Map<String,CriterionField> baseProfileCriterionFields = new LinkedHashMap<String,CriterionField>();
  private static Map<String,CriterionField> extendedProfileCriterionFields = new LinkedHashMap<String,CriterionField>();
  private static Map<String,CriterionField> presentationCriterionFields = new LinkedHashMap<String,CriterionField>();
  private static List<EvaluationCriterion> universalControlGroupCriteria = new ArrayList<EvaluationCriterion>();
//  private List<EvaluationCriterion> controlGroupCriteria = new ArrayList<EvaluationCriterion>();
  private Map<String,OfferProperty> offerProperties = new LinkedHashMap<String,OfferProperty>();
  private static Map<String,ScoringEngine> scoringEngines = new LinkedHashMap<String,ScoringEngine>();
  private static Map<String,OfferOptimizationAlgorithm> offerOptimizationAlgorithms = new LinkedHashMap<String,OfferOptimizationAlgorithm>();
  private Map<String,ScoringType> scoringTypes = new LinkedHashMap<String,ScoringType>();
  private Map<String,DNBOMatrixVariable> dnboMatrixVariables = new LinkedHashMap<String,DNBOMatrixVariable>();
  private static Map<String,DeliveryManagerDeclaration> deliveryManagers = new LinkedHashMap<String,DeliveryManagerDeclaration>();
  private static Map<String,DeliveryManagerDeclaration> fulfillmentProviders = new LinkedHashMap<String,DeliveryManagerDeclaration>();
  private static Map<String,DeliveryManagerAccount> deliveryManagerAccounts = new HashMap<String,DeliveryManagerAccount>();
  private List<EvaluationCriterion> journeyUniversalEligibilityCriteria = new ArrayList<>();
  private static Map<String,NodeType> nodeTypes = new LinkedHashMap<String,NodeType>();
  private Map<String,ToolboxSection> journeyToolbox = new LinkedHashMap<String,ToolboxSection>();
  private Map<String,ToolboxSection> campaignToolbox = new LinkedHashMap<String,ToolboxSection>();
  private Map<String,ToolboxSection> workflowToolbox = new LinkedHashMap<String,ToolboxSection>();
  private Map<String,ToolboxSection> loyaltyWorkflowToolbox = new LinkedHashMap<String,ToolboxSection>();
  private static Map<String,ThirdPartyMethodAccessLevel> thirdPartyMethodPermissionsMap = new LinkedHashMap<String,ThirdPartyMethodAccessLevel>();
  private CommunicationChannelTimeWindow defaultNotificationTimeWindowsMap;
  private static Integer authResponseCacheLifetimeInMinutes = null;
  private static Integer reportManagerMaxMessageLength = null;
  private static int stockRefreshPeriod;
  private static String periodicEvaluationCronEntry;
  private static String ucgEvaluationCronEntry;
  private Map<String,Report> initialReports = new LinkedHashMap<>();
  private static String reportManagerZookeeperDir;
  private String reportManagerOutputPath;
  private String reportManagerDateFormat;
  private String reportManagerFileExtension;
  private String reportManagerStreamsTempDir;
  private static String reportManagerTopicsCreationProperties;
  private static String reportManagerCsvSeparator;
  private static String reportManagerFieldSurrounder;
  private static String uploadedFileSeparator;
  private static CustomerMetaData customerMetaData = null;
  private static String APIresponseDateFormat;
  private static String uploadedFileTopic;
  private static String targetTopic;
  private static String communicationChannelTopic;
  public static String communicationChannelBlackoutTopic;
  public static String communicationChannelTimeWindowTopic;
  public static String loyaltyProgramTopic;
  private static String exclusionInclusionTargetTopic;
  private static String dnboMatrixTopic;
  private static String segmentContactPolicyTopic;
  private static String dynamicEventDeclarationsTopic;
  private static String dynamicCriterionFieldsTopic;
  private static Map<String,ElasticsearchConnectionSettings> elasticsearchConnectionSettings = new LinkedHashMap<String,ElasticsearchConnectionSettings>();
  private static int maxPollIntervalMs;
  private static String criterionFieldAvailableValuesTopic;
  private static String sourceAddressTopic;
  private static Map<String,String> deliveryTypeCommunicationChannelIDMap = new LinkedHashMap<>();
  private static int purchaseTimeoutMs;
  private static String voucherChangeRequestTopic;
  private static String voucherChangeResponseTopic;
  private static String hourlyReportCronEntryString;
  private static String dailyReportCronEntryString;
  private static String weeklyReportCronEntryString;
  private static String monthlyReportCronEntryString;
  private static boolean enableEvaluateTargetRandomness;
  private static int minExpiryDelayForVoucherDeliveryInHours;
  private static int importVoucherFileBulkSize;
  private static int voucherESCacheCleanerFrequencyInSec;
  private static int numberConcurrentVoucherAllocationToES;
  private static int propensityReaderRefreshPeriodMs;
  private static int propensityWriterRefreshPeriodMs;
  private static int kafkaRetentionDaysExpiredTokens;
  private static int kafkaRetentionDaysExpiredVouchers;
  private static int kafkaRetentionDaysJourneys;
  private static int kafkaRetentionDaysCampaigns;
  private static int kafkaRetentionDaysBulkCampaigns;
  private static int kafkaRetentionDaysLoyaltyPrograms;
  private static int kafkaRetentionDaysODR;
  private static int kafkaRetentionDaysBDR;
  private static int kafkaRetentionDaysMDR;
  private static int kafkaRetentionDaysTargets;
  private static int journeysReportMaxParallelThreads;
  
  private static int detailedrecordReportsArrearCount;
  private static int journeyReportsArrearCount;
  private static int subscriberprofileReportsArrearCount;
  
  // EVPRO-886
  private static int nodesTransitionsHistorySize;

  private  boolean enableContactPolicyProcessing;
  
  //EVPRO-865  
  private static int firstDayOfTheWeek;

  //extracts configuration
  private  static String extractManagerZookeeperDir;
  private  static String extractManagerOutputPath;
  private  static String extractManagerDateFormat;
  private  static String extractManagerFileExtension;
  private  static String extractManagerCsvSeparator;
  private  String extractManagerFieldSurrounder;
  
  //
  //  recurrentCampaignCreationDaysRange
  //
  
  private  static int recurrentCampaignCreationDaysRange;

  // generated
  private static Map<String,Topic> allTopics;
  private static boolean isPreprocessorNeeded = false;

  /*****************************************
   *
   *  accessors
   *
   *****************************************/

  //
  //  evolution accessors
  //

  public static boolean getRegressionMode() { return System.getProperty("use.regression","0").equals("1"); }
  public static String getSubscriberProfileEndpoints() { return System.getProperty("subscriberprofile.endpoints",""); }
  public static int getTopicSubscriberPartitions() { return topicSubscriberPartitions; }
  public static int getTopicReplication() { return topicReplication; }
  public static String getTopicMinInSyncReplicas() { return topicMinInSyncReplicas; }
  public static String getTopicRetentionShortMs() { return topicRetentionShortMs; }
  public static int getHttpServerScalingFactor() { return httpServerScalingFactor; }
  public static String getTopicRetentionMs() { return topicRetentionMs; }
  public static String getTopicRetentionLongMs() { return topicRetentionLongMs; }
  public static int getEvolutionEngineStreamThreads() { return evolutionEngineStreamThreads; }
  public static int getEvolutionEngineInstanceNumbers() { return evolutionEngineInstanceNumbers; }
  public String getSubscriberGroupLoaderAlternateID() { return subscriberGroupLoaderAlternateID; }
  public static String getGetCustomerAlternateID() { return getCustomerAlternateID; }  // EVPRO-99 check for tenant and static
  public boolean getSubscriberGroupLoaderAutoProvision() { return subscriberGroupLoaderAutoProvision; }
  public static String getCriterionFieldRetrieverClassName() { return criterionFieldRetrieverClassName; }
  public static String getEvolutionEngineExtensionClassName() { return evolutionEngineExtensionClassName; }
  public static String getGUIManagerExtensionClassName() { return guiManagerExtensionClassName; }
  public static String getSubscriberProfileClassName() { return subscriberProfileClassName; }
  public static String getExtendedSubscriberProfileClassName() { return extendedSubscriberProfileClassName; }
  public static Map<String,EvolutionEngineEventDeclaration> getEvolutionEngineEvents() { return evolutionEngineEvents; }
  public boolean getEnableProfileSegmentChange() { return enableProfileSegmentChange; }
  public static String getJourneyTopic() { return journeyTopic; }
  public static String getJourneyTemplateTopic() { return journeyTemplateTopic; }
  public static String getSegmentationDimensionTopic() { return segmentationDimensionTopic; }
  public static String getPointTopic() { return pointTopic; }
  public static String getOfferTopic() { return offerTopic; }
  public static String getReportTopic() { return reportTopic; }
  public static String getComplexObjectTypeTopic() { return complexObjectTypeTopic; }
  public static String getPaymentMeanTopic() { return paymentMeanTopic; }
  public static String getPresentationStrategyTopic() { return presentationStrategyTopic; }
  public static String getScoringStrategyTopic() { return scoringStrategyTopic; }
  public static String getCallingChannelTopic() { return callingChannelTopic; }
  public static String getSalesChannelTopic() { return salesChannelTopic; }
  public static String getSupplierTopic() { return supplierTopic; }
  public static String getResellerTopic() { return resellerTopic; }
  public static String getProductTopic() { return productTopic; }
  public static String getCatalogCharacteristicTopic() { return catalogCharacteristicTopic; }
  public static String getContactPolicyTopic() { return contactPolicyTopic; }
  public static String getJourneyObjectiveTopic() { return journeyObjectiveTopic; }
  public static String getOfferObjectiveTopic() { return offerObjectiveTopic; }
  public static String getProductTypeTopic() { return productTypeTopic; }
  public static String getUCGRuleTopic() { return ucgRuleTopic; }
  public static String getDeliverableTopic() { return deliverableTopic; }
  public static String getTokenTypeTopic() { return tokenTypeTopic; }
  public static String getVoucherTypeTopic() { return voucherTypeTopic; }
  public static String getVoucherTopic() { return voucherTopic; }
  public static String getSubscriberMessageTemplateTopic() { return subscriberMessageTemplateTopic; }
  public static String getGUIAuditTopic() { return guiAuditTopic; }
  public static String getSubscriberGroupTopic() { return subscriberGroupTopic; }
  public static String getSubscriberGroupAssignSubscriberIDTopic() { return subscriberGroupAssignSubscriberIDTopic; }
  public static String getSubscriberGroupEpochTopic() { return subscriberGroupEpochTopic; }
  public static String getUCGStateTopic() { return ucgStateTopic; }
  public static String getRenamedProfileCriterionFieldTopic() { return renamedProfileCriterionFieldTopic; }
  public static String getTimedEvaluationTopic() { return timedEvaluationTopic; }
  public static String getEvaluateTargetsTopic() { return evaluateTargetsTopic; }
  public static String getSubscriberProfileForceUpdateTopic() { return subscriberProfileForceUpdateTopic; }
  public static String getExecuteActionOtherSubscriberTopic() { return executeActionOtherSubscriberTopic; }
  public static String getSubscriberStateChangeLog() { return subscriberStateChangeLog; }
  public static String getSubscriberStateChangeLogTopic() { return subscriberStateChangeLogTopic; }
  public static String getExtendedSubscriberProfileChangeLog() { return extendedSubscriberProfileChangeLog; }
  public static String getExtendedSubscriberProfileChangeLogTopic() { return extendedSubscriberProfileChangeLogTopic; }
  public static String getSubscriberHistoryChangeLog() { return subscriberHistoryChangeLog; }
  public static String getSubscriberHistoryChangeLogTopic() { return subscriberHistoryChangeLogTopic; }
  public static String getJourneyStatisticTopic() { return journeyStatisticTopic; }
  public static String getJourneyMetricTopic() { return journeyMetricTopic; }
  public static String getPresentationLogTopic() { return presentationLogTopic; }
  public static String getAcceptanceLogTopic() { return acceptanceLogTopic; }
  public static String getProfileChangeEventTopic() { return profileChangeEventTopic;}
  public static String getProfileSegmentChangeEventTopic() { return profileSegmentChangeEventTopic;}
  public static String getProfileLoyaltyProgramChangeEventTopic() { return profileLoyaltyProgramChangeEventTopic;}
  public static String getVoucherActionTopic() { return voucherActionTopic; }
  public static String getFileWithVariableEventTopic() { return fileWithVariableEventTopic; }
  public static String getTokenRedeemedTopic() { return tokenRedeemedTopic; }
  public int getPropensityInitialisationPresentationThreshold() { return propensityInitialisationPresentationThreshold; }
  public int getPropensityInitialisationDurationInDaysThreshold() { return propensityInitialisationDurationInDaysThreshold; }
  public static String getTokenChangeTopic() { return tokenChangeTopic; }
  public static String getSubscriberProfileRegistrySubject() { return subscriberProfileRegistrySubject; }
  public static Map<String,ScheduledJobConfiguration> getDatacubeJobsScheduling() { return datacubeJobsScheduling; } // EVPRO-99 check static for tenant...
  public static Map<String,ScheduledJobConfiguration> getElasticsearchJobsScheduling() { return elasticsearchJobsScheduling; }
  public PropensityRule getPropensityRule() { return propensityRule; }
  public Map<String,Long> getJourneyTemplateCapacities() { return journeyTemplateCapacities; }
  public Map<String,SupportedLanguage> getSupportedLanguages() { return supportedLanguages; }
  public static Map<String,ExternalAPITopic> getExternalAPITopics() { return externalAPITopics; }
  public String getBaseLanguageID() { return baseLanguageID; }
  public Map<String,SupportedCurrency> getSupportedCurrencies() { return supportedCurrencies; }
  public Map<String,SupportedTimeUnit> getSupportedTimeUnits() { return supportedTimeUnits; }
  public Map<String,SupportedTokenCodesFormat> getSupportedTokenCodesFormats() { return supportedTokenCodesFormats; }
  public Map<String,SupportedVoucherCodePattern> getSupportedVoucherCodePatternList() { return supportedVoucherCodePatternList; }
  public Map<String,SupportedRelationship> getSupportedRelationships() { return supportedRelationships; }
  public Map<String,CallingChannelProperty> getCallingChannelProperties() { return callingChannelProperties; }
  public Map<String,CatalogCharacteristicUnit> getCatalogCharacteristicUnits() { return catalogCharacteristicUnits; }
  public JSONArray getInitialCallingChannelsJSONArray() { return initialCallingChannelsJSONArray; }
  public JSONArray getInitialSalesChannelsJSONArray() { return initialSalesChannelsJSONArray; }
  public JSONArray getInitialSourceAddressesJSONArray() { return initialSourceAddressesJSONArray; }
  public JSONArray getInitialSuppliersJSONArray() { return initialSuppliersJSONArray; }
  public JSONArray getInitialPartnersJSONArray() { return initialPartnersJSONArray; }
  public JSONArray getInitialProductsJSONArray() { return initialProductsJSONArray; }
  public JSONArray getInitialReportsJSONArray() { return initialReportsJSONArray; }
  public JSONArray getInitialCatalogCharacteristicsJSONArray() { return initialCatalogCharacteristicsJSONArray; }
  public JSONArray getInitialContactPoliciesJSONArray() { return initialContactPoliciesJSONArray; }
  public JSONArray getInitialJourneyTemplatesJSONArray() { return initialJourneyTemplatesJSONArray; }
  public JSONArray getInitialJourneyObjectivesJSONArray() { return initialJourneyObjectivesJSONArray; }
  public JSONArray getInitialOfferObjectivesJSONArray() { return initialOfferObjectivesJSONArray; }
  public JSONArray getInitialProductTypesJSONArray() { return initialProductTypesJSONArray; }
  public JSONArray getInitialTokenTypesJSONArray() { return initialTokenTypesJSONArray; }
  public JSONArray getInitialVoucherCodeFormatsJSONArray() { return initialVoucherCodeFormatsJSONArray; }
  public JSONArray getInitialSegmentationDimensionsJSONArray() { return initialSegmentationDimensionsJSONArray; }
  public static JSONArray getInitialComplexObjectJSONArray() { return initialComplexObjectJSONArray; }
  public boolean getGenerateSimpleProfileDimensions() { return generateSimpleProfileDimensions; }
  public Map<String,SupportedDataType> getSupportedDataTypes() { return supportedDataTypes; }
  public static JourneyMetricConfiguration getJourneyMetricConfiguration() { return journeyMetricConfiguration; }
  public static Map<String,SubscriberProfileDatacubeMetric> getSubscriberProfileDatacubeMetrics() { return subscriberProfileDatacubeMetrics; } // EVPRO-99 check for tenant and static 
  public static Map<String,CriterionField> getProfileCriterionFields() { return profileCriterionFields; } // EVPRO-99 check for tenant and static
  public static Map<String,CriterionField> getBaseProfileCriterionFields() { return baseProfileCriterionFields; }
  public Map<String,CriterionField> getExtendedProfileCriterionFields() { return extendedProfileCriterionFields; }
  public Map<String, CriterionField> getProfileChangeDetectionCriterionFields() { return profileChangeDetectionCriterionFields; }
  public Map<String, CriterionField> getProfileChangeGeneratedCriterionFields() { return profileChangeGeneratedCriterionFields; }
  public Map<String,CriterionField> getPresentationCriterionFields() { return presentationCriterionFields; }
//  public List<EvaluationCriterion> getUniversalControlGroupCriteria() { return universalControlGroupCriteria; } // still usefull ?
//  public List<EvaluationCriterion> getControlGroupCriteria() { return controlGroupCriteria; }
  public Map<String,OfferProperty> getOfferProperties() { return offerProperties; }
  public static Map<String,ScoringEngine> getScoringEngines() { return scoringEngines; } // EVPRO-99 check for tenant and static
  public static Map<String,OfferOptimizationAlgorithm> getOfferOptimizationAlgorithms() { return offerOptimizationAlgorithms; } // EVPRO-99 check for tenant and static
  public Map<String,ScoringType> getScoringTypes() { return scoringTypes; }
  public Map<String,DNBOMatrixVariable> getDNBOMatrixVariables() { return dnboMatrixVariables; }
  public static Map<String,DeliveryManagerDeclaration> getDeliveryManagers() { return deliveryManagers; }
  public static Map<String,DeliveryManagerDeclaration> getFulfillmentProviders() { return fulfillmentProviders; } // TODO EVPRO-99 fulfillmentProviders accounts per tenant ?
  public static Map<String,DeliveryManagerAccount> getDeliveryManagerAccounts() { return deliveryManagerAccounts; } // TODO EVPRO-99 deliveryManager accounts per tenant ?
  public static Map<String,NodeType> getNodeTypes() { return nodeTypes; } // EVPRO-99 should not be per tenant...
  public Map<String,ToolboxSection> getJourneyToolbox() { return journeyToolbox; }
  public Map<String,ToolboxSection> getCampaignToolbox() { return campaignToolbox; }
  public Map<String,ToolboxSection> getWorkflowToolbox() { return workflowToolbox; }
  public Map<String,ToolboxSection> getLoyaltyWorkflowToolbox() { return loyaltyWorkflowToolbox; }
  public static Map<String,ThirdPartyMethodAccessLevel> getThirdPartyMethodPermissionsMap() { return thirdPartyMethodPermissionsMap; } // TODO EVPRO-99 check for tenant and static
  public static Integer getAuthResponseCacheLifetimeInMinutes() { return authResponseCacheLifetimeInMinutes; }
  public static Integer getReportManagerMaxMessageLength() { return reportManagerMaxMessageLength; } // TODO EVPRO-99 check for tenant and static
  public static int getStockRefreshPeriod() { return stockRefreshPeriod; } // TODO EVPRO-99 check for tenant and static
  public static String getPeriodicEvaluationCronEntry() { return periodicEvaluationCronEntry; }
  public static String getUCGEvaluationCronEntry() { return ucgEvaluationCronEntry; } // TODO EVPRO-99 check for tenant and static
  public Map<String,Report> getInitialReports() { return initialReports; }
  public static String getReportManagerZookeeperDir() { return reportManagerZookeeperDir; }
  public String getReportManagerOutputPath() { return reportManagerOutputPath; }
  public String getReportManagerDateFormat() { return reportManagerDateFormat; }
  public String getReportManagerFileExtension() { return reportManagerFileExtension; }
  public static String getReportManagerCsvSeparator() { return reportManagerCsvSeparator; } // EVPRO-99 check for tenant and static
  public static String getReportManagerFieldSurrounder() { return reportManagerFieldSurrounder; } // EVPRO-99 check for tenant and static
  public static String getUploadedFileSeparator() { return uploadedFileSeparator; } // EVPRO-99 check for tenant and static
  public String getReportManagerStreamsTempDir() { return reportManagerStreamsTempDir; }
  public static String getReportManagerTopicsCreationProperties() { return reportManagerTopicsCreationProperties; }
  public static CustomerMetaData getCustomerMetaData() { return customerMetaData; }
  public static String getAPIresponseDateFormat() { return APIresponseDateFormat; } // EVPRO-99 check for tenant and static
  public static String getUploadedFileTopic() { return uploadedFileTopic; }
  public static String getTargetTopic() { return targetTopic; }
  public CommunicationChannelTimeWindow getDefaultNotificationDailyWindows() { return defaultNotificationTimeWindowsMap; }
  public static String getCommunicationChannelTopic() { return communicationChannelTopic; }
  public static String getCommunicationChannelBlackoutTopic() { return communicationChannelBlackoutTopic; }
  public static String getCommunicationChannelTimeWindowTopic() { return communicationChannelTimeWindowTopic; }
  public static String getLoyaltyProgramTopic() { return loyaltyProgramTopic; }
  public static String getExclusionInclusionTargetTopic() { return exclusionInclusionTargetTopic; }
  public static String getDNBOMatrixTopic() { return dnboMatrixTopic; }
  public static String getSegmentContactPolicyTopic() { return segmentContactPolicyTopic; }
  public static String getDynamicEventDeclarationsTopic() { return dynamicEventDeclarationsTopic; }
  public static String getDynamicCriterionFieldTopic() { return dynamicCriterionFieldsTopic; }
  public Map<String,PartnerType> getPartnerTypes() { return partnerTypes; }
  public Map<String,BillingMode> getBillingModes() { return billingModes; }
  public static ElasticsearchConnectionSettings getElasticsearchConnectionSettings(String name, boolean forConnect) {
    ElasticsearchConnectionSettings toRet = elasticsearchConnectionSettings.get(name);
    if(toRet==null) {
      if(forConnect) return elasticsearchConnectionSettings.get("connectDefault");
      toRet = elasticsearchConnectionSettings.get("default");
    }
    return toRet;
  }
  public static int getMaxPollIntervalMs() {return maxPollIntervalMs; }
  public static int getPurchaseTimeoutMs() {return purchaseTimeoutMs; }
  public static String getCriterionFieldAvailableValuesTopic() { return criterionFieldAvailableValuesTopic; }
  public static String getSourceAddressTopic() { return sourceAddressTopic; }
  public static Map<String,CommunicationChannel> getCommunicationChannels(){ return communicationChannels; }; // TODO EVPRO-99 how communication channels are handled into multitenancy ??
  public static Map<String,String> getDeliveryTypeCommunicationChannelIDMap(){ return deliveryTypeCommunicationChannelIDMap; };  // TODO EVPRO-99 how communication channels are handled into multitenancy ??
  public static String getVoucherChangeRequestTopic() { return voucherChangeRequestTopic; }
  public static String getVoucherChangeResponseTopic() { return voucherChangeResponseTopic; }
  public static int getMinExpiryDelayForVoucherDeliveryInHours() { return minExpiryDelayForVoucherDeliveryInHours; } // TODO EVPRO-99 check for tenant and static
  public static int getImportVoucherFileBulkSize() { return importVoucherFileBulkSize; } // TODO EVPRO-99 check for tenant and static
  public static int getNumberConcurrentVoucherAllocationToES() { return numberConcurrentVoucherAllocationToES; }
  public static int getVoucherESCacheCleanerFrequencyInSec() { return voucherESCacheCleanerFrequencyInSec; }
  public static String getHourlyReportCronEntryString() { return hourlyReportCronEntryString; }
  public static String getDailyReportCronEntryString() { return dailyReportCronEntryString; }
  public static String getWeeklyReportCronEntryString() { return weeklyReportCronEntryString; }
  public static String getMonthlyReportCronEntryString() { return monthlyReportCronEntryString; }
  public static boolean getEnableEvaluateTargetRandomness() { return enableEvaluateTargetRandomness; }
  public static int getPropensityReaderRefreshPeriodMs() { return propensityReaderRefreshPeriodMs; }
  public static int getPropensityWriterRefreshPeriodMs() { return propensityWriterRefreshPeriodMs; }
  public int getKafkaRetentionDaysExpiredTokens() { return kafkaRetentionDaysExpiredTokens; }
  public static int getKafkaRetentionDaysExpiredVouchers() { return kafkaRetentionDaysExpiredVouchers; }
  public static int getKafkaRetentionDaysJourneys() { return kafkaRetentionDaysJourneys; }
  public static int getKafkaRetentionDaysCampaigns() { return kafkaRetentionDaysCampaigns; }
  public static int getKafkaRetentionDaysBulkCampaigns() { return kafkaRetentionDaysBulkCampaigns; }
  public static int getKafkaRetentionDaysLoyaltyPrograms() { return kafkaRetentionDaysLoyaltyPrograms; }
  public static int getKafkaRetentionDaysODR() { return kafkaRetentionDaysODR; }
  public static int getKafkaRetentionDaysBDR() { return kafkaRetentionDaysBDR; }
  public static int getKafkaRetentionDaysMDR() { return kafkaRetentionDaysMDR; }
  public boolean getEnableContactPolicyProcessing(){ return  enableContactPolicyProcessing;}
  public static String getExtractManagerZookeeperDir() { return extractManagerZookeeperDir; }
  public static String getExtractManagerOutputPath() { return extractManagerOutputPath; } // TODO EVPRO-99 check tenant ?
  public static String getExtractManagerDateFormat() { return extractManagerDateFormat; }// TODO EVPRO-99 check tenant ?
  public static String getExtractManagerFileExtension() { return extractManagerFileExtension; } // TODO EVPRO-99 check tenant ?
  public static String getExtractManagerCsvSeparator() { return extractManagerCsvSeparator; }// EVPRO-99 check static for tenant
  public String getExtractManagerFieldSurrounder() { return extractManagerFieldSurrounder; }
  public static int getRecurrentCampaignCreationDaysRange() { return recurrentCampaignCreationDaysRange; } // TODO EVPRO-99 check tenant aspect
  public static Set<Topic> getAllTopics() { return new HashSet<>(allTopics.values()); }
  public List<EvaluationCriterion> getJourneyUniversalEligibilityCriteria() { return journeyUniversalEligibilityCriteria; } 
  public static boolean isPreprocessorNeeded() { return isPreprocessorNeeded; }
  //EVPRO-574
  public static int getKafkaRetentionDaysTargets() { return kafkaRetentionDaysTargets; } 
  public static int getJourneysReportMaxParallelThreads() { return journeysReportMaxParallelThreads; }
  
  public static int getDetailedrecordReportsArrearCount() { return detailedrecordReportsArrearCount; }
  public static int getJourneyReportsArrearCount() { return journeyReportsArrearCount; }
  public static int getSubscriberprofileReportsArrearCount() { return subscriberprofileReportsArrearCount; }
  
  //EVPRO-865
  public static int getFirstDayOfTheWeek() { return firstDayOfTheWeek; }
  // EVPRO-886
  public static int getNodesTransitionsHistorySize() { return nodesTransitionsHistorySize; }

  // addProfileCriterionField
  //
  public void addProfileCriterionField(String key, CriterionField criterion) { profileCriterionFields.put(key, criterion); }

  /*****************************************
   *
   *  getCriterionFieldRetrieverClass
   *
   *****************************************/

  public static Class<? extends CriterionFieldRetriever> getCriterionFieldRetrieverClass()
  {
    try
      {
        Class<? extends CriterionFieldRetriever> criterionFieldRetrieverClass = (Class<? extends CriterionFieldRetriever>) Class.forName(criterionFieldRetrieverClassName);
        return criterionFieldRetrieverClass;
      }
    catch (ClassNotFoundException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
   *
   *  getEvolutionEngineExtensionClass
   *
   *****************************************/

  public static Class<? extends EvolutionEngineExtension> getEvolutionEngineExtensionClass()
  {
    try
      {
        Class<? extends EvolutionEngineExtension> evolutionEngineExtensionClass = (Class<? extends EvolutionEngineExtension>) Class.forName(evolutionEngineExtensionClassName);
        return evolutionEngineExtensionClass;
      }
    catch (ClassNotFoundException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
   *
   *  getGUIManagerExtensionClass
   *
   *****************************************/

  public static Class<? extends GUIManagerExtension> getGUIManagerExtensionClass()
  {
    try
      {
        Class<? extends GUIManagerExtension> guiManagerExtensionClass = (guiManagerExtensionClassName != null) ? (Class<? extends GUIManagerExtension>) Class.forName(guiManagerExtensionClassName) : null;
        return guiManagerExtensionClass;
      }
    catch (ClassNotFoundException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
   *
   *  getSubscriberProfileClass
   *
   *****************************************/

  public static Class<SubscriberProfile> getSubscriberProfileClass()
  {
    try
      {
        Class<SubscriberProfile> subscriberProfileClass = (Class<SubscriberProfile>) Class.forName(subscriberProfileClassName);
        return subscriberProfileClass;
      }
    catch (ClassNotFoundException e)
      {
        throw new ServerRuntimeException(e);
      }
  }

  /*****************************************
   *
   *  generateProfileChangeCriterionFields
   *
   *****************************************/

  public static Map<String, CriterionField> generateProfileChangeCriterionFields(CriterionField originalCriterionField) throws GUIManagerException
  {
    HashMap<String, CriterionField> result = new HashMap<>();
    JSONObject criterionFieldJSON = new JSONObject();
    criterionFieldJSON.putAll(originalCriterionField.getJSONRepresentation());
    criterionFieldJSON.put("id", ProfileChangeEvent.CRITERION_FIELD_NAME_OLD_PREFIX + originalCriterionField.getID());
    criterionFieldJSON.put("display", "Old " + originalCriterionField.getID() + " value");
    criterionFieldJSON.put("retriever", "getProfileChangeFieldOldValue");
    criterionFieldJSON.put("mandatory", Boolean.FALSE);
    criterionFieldJSON.put("esField", null);
    criterionFieldJSON.put("expressionValuedParameter", Boolean.FALSE);
    criterionFieldJSON.put("profileChangeEvent", Boolean.FALSE);
    CriterionField field = new CriterionField(criterionFieldJSON);
    result.put(field.getID(), field);

    criterionFieldJSON = new JSONObject();
    criterionFieldJSON.putAll(originalCriterionField.getJSONRepresentation());
    criterionFieldJSON.put("id", ProfileChangeEvent.CRITERION_FIELD_NAME_NEW_PREFIX + originalCriterionField.getID());
    criterionFieldJSON.put("display", "New " + originalCriterionField.getID() + " value");
    criterionFieldJSON.put("retriever", "getProfileChangeFieldNewValue");
    criterionFieldJSON.put("mandatory", Boolean.FALSE);
    criterionFieldJSON.put("esField", null);
    criterionFieldJSON.put("expressionValuedParameter", Boolean.FALSE);
    criterionFieldJSON.put("profileChangeEvent", Boolean.FALSE);
    field = new CriterionField(criterionFieldJSON);
    result.put(field.getID(), field);

    criterionFieldJSON = new JSONObject();
    criterionFieldJSON.put("id", ProfileChangeEvent.CRITERION_FIELD_NAME_IS_UPDATED_PREFIX +  originalCriterionField.getID());
    criterionFieldJSON.put("display", "Is " + originalCriterionField.getID() + " updated");
    criterionFieldJSON.put("dataType", "boolean");
    criterionFieldJSON.put("retriever", "getProfileChangeFieldsUpdated");
    field = new CriterionField(criterionFieldJSON);
    result.put(field.getID(), field);
    return result;
  }

  /*****************************************
   *
   *  getExtendedSubscriberProfileClass
   *
   *****************************************/

  public static Class<ExtendedSubscriberProfile> getExtendedSubscriberProfileClass()
  {
    try
      {
        Class<ExtendedSubscriberProfile> extendedSubscriberProfileClass = (Class<ExtendedSubscriberProfile>) Class.forName(extendedSubscriberProfileClassName);
        return extendedSubscriberProfileClass;
      }
    catch (ClassNotFoundException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
   *
   *  getEvolutionEngineExternalAPIClass
   *
   *****************************************/

  public static Class<ExternalAPI> getEvolutionEngineExternalAPIClass()
  {
    try
      {
        Class<ExternalAPI> evolutionEngineExternalAPIClass = (evolutionEngineExternalAPIClassName != null) ? (Class<ExternalAPI>) Class.forName(evolutionEngineExternalAPIClassName) : null;
        return evolutionEngineExternalAPIClass;
      }
    catch (ClassNotFoundException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
   *
   *  getEvolutionEngineExternalAPITopicID
   *
   *****************************************/

  public String getEvolutionEngineExternalAPITopicID(String topic)
  {
    String evolutionEngineExternalAPITopicID = null;
    for (ExternalAPITopic externalAPITopic : externalAPITopics.values())
      {
        if (Objects.equals(topic, externalAPITopic.getName()))
          {
            evolutionEngineExternalAPITopicID = externalAPITopic.getID();
            break;
          }
      }
    return evolutionEngineExternalAPITopicID;
  }

  /*****************************************
   *
   *  getSupportedLanguageID
   *
   *****************************************/

  public String getSupportedLanguageID(String language)
  {
    String supportedLanguageID = null;
    for (SupportedLanguage supportedLanguage : supportedLanguages.values())
      {
        if (Objects.equals(language, supportedLanguage.getName()))
          {
            supportedLanguageID = supportedLanguage.getID();
            break;
          }
      }
    return supportedLanguageID;
  }
  
  /*****************************************
  *
  *  static
  *
  *****************************************/
  static 
  {    
    // just init the tenant0 for static configuration
    new Deployment(0);
  }

  /*****************************************
   *
   *  per tenant initialization
   *
   *****************************************/

  public Deployment(int tenantID)
    {
      super(tenantID);
      
      deploymentsPerTenant.put(tenantID, this);
      
      /*****************************************
       *
       *  super class
       *
       *****************************************/

      JSONObject jsonRoot = com.evolving.nglm.core.Deployment.getTenantJSONRoot(tenantID);

      /*****************************************
       *
       *  configuration
       *
       *****************************************/

      //
      // kafka topics configuration
      //
      try
        {
          topicSubscriberPartitions = Integer.parseInt(JSONUtilities.decodeString(jsonRoot, "topicSubscriberPartitions", true));
          topicReplication = Integer.parseInt(JSONUtilities.decodeString(jsonRoot, "topicReplication", true));
          topicMinInSyncReplicas = JSONUtilities.decodeString(jsonRoot, "topicMinInSyncReplicas", true);
          topicRetentionShortMs = ""+(JSONUtilities.decodeInteger(jsonRoot, "topicRetentionShortHour", true) * 3600 * 1000L);
          topicRetentionMs = ""+(JSONUtilities.decodeInteger(jsonRoot, "topicRetentionDay", true) * 24 * 3600 * 1000L);
          topicRetentionLongMs = ""+(JSONUtilities.decodeInteger(jsonRoot, "topicRetentionLongDay", true) * 24 * 3600 * 1000L);
        }
      catch (JSONUtilitiesException|NumberFormatException e)
        {
          throw new ServerRuntimeException("deployment : topic configuration", e);
        }

      //
      //  httpServerScalingFactor
      //
      
      try
        {
          httpServerScalingFactor = Integer.parseInt(System.getenv().get("HTTP_SERVER_SCALING_FACTOR"));
          log.info("Deployment: HTTP_SERVER_SCALING_FACTOR set to value - " + httpServerScalingFactor);          
        }
      catch (NumberFormatException e)
        {
          log.info("Deployment: HTTP_SERVER_SCALING_FACTOR set to default value - " + httpServerScalingFactor);
        }
      
      try
        {
          evolutionEngineStreamThreads = Integer.parseInt(System.getProperty("evolutionengine.streamthreads","1"));
        }
      catch (NumberFormatException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
      evolutionEngineInstanceNumbers = getSubscriberProfileEndpoints().split(",").length;
      if(evolutionEngineInstanceNumbers<1){
        log.warn("Deployment: subscriberprofile.endpoints : '" + getSubscriberProfileEndpoints() + "' seems wrong");
        evolutionEngineInstanceNumbers=1;
      }
      
      //
      //  subscriberGroupLoaderAlternateID
      //

      try
        {
          subscriberGroupLoaderAlternateID = JSONUtilities.decodeString(jsonRoot, "subscriberGroupLoaderAlternateID", false);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  getCustomerAlternateID
      //

      try
        {
          getCustomerAlternateID = JSONUtilities.decodeString(jsonRoot, "getCustomerAlternateID", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }


      //
      //  subscriberGroupLoaderAutoProvision
      //

      try
        {
          subscriberGroupLoaderAutoProvision = JSONUtilities.decodeBoolean(jsonRoot, "subscriberGroupLoaderAutoProvision", Boolean.FALSE);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  criterionFieldRetrieverClassName
      //

      try
        {
          criterionFieldRetrieverClassName = JSONUtilities.decodeString(jsonRoot, "criterionFieldRetrieverClass", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  evolutionEngineExtensionClassName
      //

      try
        {
          evolutionEngineExtensionClassName = JSONUtilities.decodeString(jsonRoot, "evolutionEngineExtensionClass", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  guiManagerExtensionClassName
      //

      try
        {
          guiManagerExtensionClassName = JSONUtilities.decodeString(jsonRoot, "guiManagerExtensionClass", false);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  subscriberProfileClassName
      //

      try
        {
          subscriberProfileClassName = JSONUtilities.decodeString(jsonRoot, "subscriberProfileClass", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  extendedSubscriberProfileClassName
      //

      try
        {
          extendedSubscriberProfileClassName = JSONUtilities.decodeString(jsonRoot, "extendedSubscriberProfileClass", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  evolutionEngineExternalAPIClassName
      //

      try
        {
          evolutionEngineExternalAPIClassName = JSONUtilities.decodeString(jsonRoot, "externalAPIClass", false);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  enableProfileSegmentChange
      //

      try
        {
          enableProfileSegmentChange = JSONUtilities.decodeBoolean(jsonRoot, "enableProfileSegmentChange", Boolean.FALSE);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
      
      //
      //  VoucherActionTopic
      //

      try
        {
          voucherActionTopic = JSONUtilities.decodeString(jsonRoot, "voucherActionTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
      
      //
      //  fileWithVariableEventTopic
      //

      try
        {
          fileWithVariableEventTopic = JSONUtilities.decodeString(jsonRoot, "fileWithVariableEventTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
      
      //
      //  tokenRedeemedTopic
      //

      try
        {
          tokenRedeemedTopic = JSONUtilities.decodeString(jsonRoot, "tokenRedeemedTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  evolutionEngineEvents
      //

      try
        {
          //
          //  deployment-level events
          //

          JSONArray evolutionEngineEventValues = JSONUtilities.decodeJSONArray(jsonRoot, "evolutionEngineEvents", true);
          for (int i=0; i<evolutionEngineEventValues.size(); i++)
            {
              JSONObject evolutionEngineEventJSON = (JSONObject) evolutionEngineEventValues.get(i);
              EvolutionEngineEventDeclaration evolutionEngineEventDeclaration = new EvolutionEngineEventDeclaration(evolutionEngineEventJSON);
              evolutionEngineEvents.put(evolutionEngineEventDeclaration.getName(), evolutionEngineEventDeclaration);
            }

          //
          // core-level events
          //

          JSONArray evolutionEngineCoreEventValues = JSONUtilities.decodeJSONArray(jsonRoot, "evolutionEngineCoreEvents", true);
          for (int i=0; i<evolutionEngineCoreEventValues.size(); i++)
            {
              JSONObject evolutionEngineEventJSON = (JSONObject) evolutionEngineCoreEventValues.get(i);
              EvolutionEngineEventDeclaration evolutionEngineEventDeclaration = new EvolutionEngineEventDeclaration(evolutionEngineEventJSON);
              evolutionEngineEvents.put(evolutionEngineEventDeclaration.getName(), evolutionEngineEventDeclaration);
            }
        }
      catch (GUIManagerException | JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }


      //
      //  communicationChannels
      //

      try
        {
          JSONArray communicationChannelsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "communicationChannels", new JSONArray());
          for (int i=0; i<communicationChannelsJSONArray.size(); i++)
            {
              JSONObject communicationChannelJSON = (JSONObject) communicationChannelsJSONArray.get(i);
              CommunicationChannel communicationChannel = new CommunicationChannel(communicationChannelJSON, JSONUtilities.decodeInteger(communicationChannelJSON, "tenantID", 1));
              communicationChannels.put(communicationChannel.getID(), communicationChannel);
            }
        }
      catch (GUIManagerException | JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  notificationDailyWindows
      //
      try
        {
          JSONObject defaultTimeWindowJSON = (JSONObject) jsonRoot.get("notificationDailyWindows");
          if(defaultTimeWindowJSON != null)
            {
              defaultTimeWindowJSON.put("id", "default");
              defaultTimeWindowJSON.put("name", "default");
              defaultTimeWindowJSON.put("display", "default");
              defaultTimeWindowJSON.put("active", true);
              defaultTimeWindowJSON.put("communicationChannelID", "default");
            }
          GUIManagedObject.commonSchema();//avoiding a NPE in a "static init" loop
          defaultNotificationTimeWindowsMap = new CommunicationChannelTimeWindow(defaultTimeWindowJSON, System.currentTimeMillis() * 1000, null, tenantID);          
        }
      catch (GUIManagerException | JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
     
      //
      //  journeyTopic
      //

      try
        {
          journeyTopic = JSONUtilities.decodeString(jsonRoot, "journeyTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  journeyTemplateTopic
      //

      try
        {
          journeyTemplateTopic = JSONUtilities.decodeString(jsonRoot, "journeyTemplateTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  segmentationDimensionTopic
      //

      try
        {
          segmentationDimensionTopic = JSONUtilities.decodeString(jsonRoot, "segmentationDimensionTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  pointTopic
      //

      try
        {
          pointTopic = JSONUtilities.decodeString(jsonRoot, "pointTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
      
      //
      //  pointTopic
      //

      try
        {
          complexObjectTypeTopic = JSONUtilities.decodeString(jsonRoot, "complexObjectTypeTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  maxPollIntervalMs
      //

      try
        {
          maxPollIntervalMs = JSONUtilities.decodeInteger(jsonRoot, "maxPollIntervalMs", 300000);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      // purchaseTimeoutMs
      //
      try
        {
          purchaseTimeoutMs = JSONUtilities.decodeInteger(jsonRoot, "purchaseTimeoutMs", 15000);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  offerTopic
      //

      try
        {
          offerTopic = JSONUtilities.decodeString(jsonRoot, "offerTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  reportTopic
      //

      try
        {
          reportTopic = JSONUtilities.decodeString(jsonRoot, "reportTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  paymentMeanTopic
      //

      try
        {
          paymentMeanTopic = JSONUtilities.decodeString(jsonRoot, "paymentMeanTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  presentationStrategyTopic
      //

      try
        {
          presentationStrategyTopic = JSONUtilities.decodeString(jsonRoot, "presentationStrategyTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  scoringStrategyTopic
      //

      try
        {
          scoringStrategyTopic = JSONUtilities.decodeString(jsonRoot, "scoringStrategyTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  callingChannelTopic
      //

      try
        {
          callingChannelTopic = JSONUtilities.decodeString(jsonRoot, "callingChannelTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  salesChannelTopic
      //

      try
        {
          salesChannelTopic = JSONUtilities.decodeString(jsonRoot, "salesChannelTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  supplierTopic
      //

      try
        {
          supplierTopic = JSONUtilities.decodeString(jsonRoot, "supplierTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  resellerTopic
      //

      try
        {
          resellerTopic = JSONUtilities.decodeString(jsonRoot, "resellerTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  productTopic
      //

      try
        {
          productTopic = JSONUtilities.decodeString(jsonRoot, "productTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  catalogCharacteristicTopic
      //

      try
        {
          catalogCharacteristicTopic = JSONUtilities.decodeString(jsonRoot, "catalogCharacteristicTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  contactPolicyTopic
      //

      try
        {
          contactPolicyTopic = JSONUtilities.decodeString(jsonRoot, "contactPolicyTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  journeyObjectiveTopic
      //

      try
        {
          journeyObjectiveTopic = JSONUtilities.decodeString(jsonRoot, "journeyObjectiveTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  offerObjectiveTopic
      //

      try
        {
          offerObjectiveTopic = JSONUtilities.decodeString(jsonRoot, "offerObjectiveTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  productTypeTopic
      //

      try
        {
          productTypeTopic = JSONUtilities.decodeString(jsonRoot, "productTypeTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  ucgRuleTopic
      //

      try
        {
          ucgRuleTopic = JSONUtilities.decodeString(jsonRoot, "ucgRuleTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  deliverable
      //

      try
        {
          deliverableTopic = JSONUtilities.decodeString(jsonRoot, "deliverableTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  tokenType
      //

      try
        {
          tokenTypeTopic = JSONUtilities.decodeString(jsonRoot, "tokenTypeTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  voucherType
      //

      try
        {
          voucherTypeTopic = JSONUtilities.decodeString(jsonRoot, "voucherTypeTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  voucher
      //

      try
        {
          voucherTopic = JSONUtilities.decodeString(jsonRoot, "voucherTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  subscriberMessageTemplateTopic
      //

      try
        {
          subscriberMessageTemplateTopic = JSONUtilities.decodeString(jsonRoot, "subscriberMessageTemplateTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  guiAuditTopic
      //

      try
        {
          guiAuditTopic = JSONUtilities.decodeString(jsonRoot, "guiAuditTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  subscriberGroupTopic
      //

      try
        {
          subscriberGroupTopic = JSONUtilities.decodeString(jsonRoot, "subscriberGroupTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  subscriberGroupAssignSubscriberIDTopic
      //

      try
        {
          subscriberGroupAssignSubscriberIDTopic = JSONUtilities.decodeString(jsonRoot, "subscriberGroupAssignSubscriberIDTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  subscriberGroupEpochTopic
      //

      try
        {
          subscriberGroupEpochTopic = JSONUtilities.decodeString(jsonRoot, "subscriberGroupEpochTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  ucgStateTopic
      //

      try
        {
          ucgStateTopic = JSONUtilities.decodeString(jsonRoot, "ucgStateTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  renamedProfileCriterionFieldTopic
      //

      try
        {
          renamedProfileCriterionFieldTopic = JSONUtilities.decodeString(jsonRoot, "renamedProfileCriterionFieldTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  uploadFileTopic
      //

      try
        {
          uploadedFileTopic = JSONUtilities.decodeString(jsonRoot, "uploadedFileTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  targetTopic
      //

      try
        {
          targetTopic = JSONUtilities.decodeString(jsonRoot, "targetTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  exclusionInclusionTargetTopic
      //

      try
        {
          exclusionInclusionTargetTopic = JSONUtilities.decodeString(jsonRoot, "exclusionInclusionTargetTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  dnboMatrixTopic
      //

      try
        {
          dnboMatrixTopic = JSONUtilities.decodeString(jsonRoot, "dnboMatrixTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }


      //
      //  dynamicEventDeclarationsTopic
      //

      try
        {
          dynamicEventDeclarationsTopic = JSONUtilities.decodeString(jsonRoot, "dynamicEventDeclarationsTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  dynamicCriterionFieldsTopic
      //

      try
        {
          dynamicCriterionFieldsTopic = JSONUtilities.decodeString(jsonRoot, "dynamicCriterionFieldTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  communicationChannelBlackoutTopic
      //

      try
        {
          communicationChannelBlackoutTopic = JSONUtilities.decodeString(jsonRoot, "communicationChannelBlackoutTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
      
      //
      //  communicationChannelTimeWindowTopic
      //

      try
        {
          communicationChannelTimeWindowTopic = JSONUtilities.decodeString(jsonRoot, "communicationChannelTimeWindowTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  communicationChannelTopic
      //

      try
        {
          communicationChannelTopic = JSONUtilities.decodeString(jsonRoot, "communicationChannelTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  loyaltyProgramTopic
      //

      try
        {
          loyaltyProgramTopic = JSONUtilities.decodeString(jsonRoot, "loyaltyProgramTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  timedEvaluationTopic
      //

      try
        {
          timedEvaluationTopic = JSONUtilities.decodeString(jsonRoot, "timedEvaluationTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  evaluateTargetsTopic
      //

      try
        {
          evaluateTargetsTopic = JSONUtilities.decodeString(jsonRoot, "evaluateTargetsTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  subscriberProfileForceUpdateTopic
      //

      try
        {
          subscriberProfileForceUpdateTopic = JSONUtilities.decodeString(jsonRoot, "subscriberProfileForceUpdateTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  executeActionOtherSubscriberTopic
      //

      try
        {
          executeActionOtherSubscriberTopic = JSONUtilities.decodeString(jsonRoot, "executeActionOtherSubscriberTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
      
      //
      //  subscriberStateChangeLog
      //

      try
        {
          subscriberStateChangeLog = JSONUtilities.decodeString(jsonRoot, "subscriberStateChangeLog", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  subscriberStateChangeLogTopic
      //

      try
        {
          subscriberStateChangeLogTopic = JSONUtilities.decodeString(jsonRoot, "subscriberStateChangeLogTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  extendedSubscriberProfileChangeLog
      //

      try
        {
          extendedSubscriberProfileChangeLog = JSONUtilities.decodeString(jsonRoot, "extendedSubscriberProfileChangeLog", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  extendedSubscriberProfileChangeLogTopic
      //

      try
        {
          extendedSubscriberProfileChangeLogTopic = JSONUtilities.decodeString(jsonRoot, "extendedSubscriberProfileChangeLogTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  subscriberHistoryChangeLog
      //

      try
        {
          subscriberHistoryChangeLog = JSONUtilities.decodeString(jsonRoot, "subscriberHistoryChangeLog", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  subscriberHistoryChangeLogTopic
      //

      try
        {
          subscriberHistoryChangeLogTopic = JSONUtilities.decodeString(jsonRoot, "subscriberHistoryChangeLogTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  journeyStatisticTopic
      //

      try
        {
          journeyStatisticTopic = JSONUtilities.decodeString(jsonRoot, "journeyStatisticTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  journeyMetricTopic
      //

      try
        {
          journeyMetricTopic = JSONUtilities.decodeString(jsonRoot, "journeyMetricTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  presentationLogTopic
      //

      try
        {
          presentationLogTopic = JSONUtilities.decodeString(jsonRoot, "presentationLogTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  acceptanceLogTopic
      //

      try
        {
          acceptanceLogTopic = JSONUtilities.decodeString(jsonRoot, "acceptanceLogTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  segmentContactPolicyTopic
      //

      try
        {
          segmentContactPolicyTopic = JSONUtilities.decodeString(jsonRoot, "segmentContactPolicyTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  profileChangeEventTopic
      //

      try
        {
          profileChangeEventTopic = JSONUtilities.decodeString(jsonRoot, "profileChangeEventTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //
      //  profileSegmentChangeEventTopic
      //

      try
        {
          profileSegmentChangeEventTopic = JSONUtilities.decodeString(jsonRoot, "profileSegmentChangeEventTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }


      //
      //  profileLoyaltyProgramChangeEventTopic
      //

      try
        {
          profileLoyaltyProgramChangeEventTopic = JSONUtilities.decodeString(jsonRoot, "profileLoyaltyProgramChangeEventTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  propensityInitialisationPresentationThreshold
      //

      try
        {
          propensityInitialisationPresentationThreshold = JSONUtilities.decodeInteger(jsonRoot, "propensityInitialisationPresentationThreshold", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  propensityInitialisationDurationInDaysThreshold
      //

      try
        {
          propensityInitialisationDurationInDaysThreshold = JSONUtilities.decodeInteger(jsonRoot, "propensityInitialisationDurationInDaysThreshold", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
      
      //
      //  tokenChangeTopic
      //

      try
        {
          tokenChangeTopic = JSONUtilities.decodeString(jsonRoot, "tokenChangeTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  subscriberProfileRegistrySubject
      //

      try
        {
          subscriberProfileRegistrySubject = JSONUtilities.decodeString(jsonRoot, "subscriberProfileRegistrySubject", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  datacubeJobsScheduling & elasticsearchJobsScheduling & others...
      //

      try
        {
          //  datacubeJobsScheduling
          JSONObject datacubeJobsSchedulingJSON = JSONUtilities.decodeJSONObject(jsonRoot, "datacubeJobsScheduling", true);
          for (Object key : datacubeJobsSchedulingJSON.keySet()) {
            datacubeJobsScheduling.put((String) key, new ScheduledJobConfiguration((JSONObject) datacubeJobsSchedulingJSON.get(key)));
          }

          //  elasticsearchJobsScheduling
          JSONObject elasticsearchJobsSchedulingJSON = JSONUtilities.decodeJSONObject(jsonRoot, "elasticsearchJobsScheduling", true);
          for (Object key : elasticsearchJobsSchedulingJSON.keySet()) {
            elasticsearchJobsScheduling.put((String) key, new ScheduledJobConfiguration((JSONObject) elasticsearchJobsSchedulingJSON.get(key)));
          }
          
          // elasticsearchConnectionSettings
          for (Object elasticsearchConnectionSettingsObject:JSONUtilities.decodeJSONArray(jsonRoot, "elasticsearchConnectionSettings", true).toArray()){
            ElasticsearchConnectionSettings elasticsearchConnectionSetting = new ElasticsearchConnectionSettings((JSONObject) elasticsearchConnectionSettingsObject);
            elasticsearchConnectionSettings.put(elasticsearchConnectionSetting.getId(), elasticsearchConnectionSetting);
          }
          
        }
      catch (JSONUtilitiesException|IllegalArgumentException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  propensityRule
      //

      try
        {
          JSONObject propensityRuleJSON = (JSONObject) jsonRoot.get("propensityRule");
          propensityRule = new PropensityRule(propensityRuleJSON);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  journeyTemplateCapacities
      //

      try
        {
          JSONObject journeyTemplateCapacitiesJSON = JSONUtilities.decodeJSONObject(jsonRoot, "journeyTemplateCapacities", true);
          for (Object key : journeyTemplateCapacitiesJSON.keySet())
            {
              journeyTemplateCapacities.put((String) key, (Long) journeyTemplateCapacitiesJSON.get(key));
            }
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  externalAPITopics
      //

      try
        {
          JSONArray externalAPITopicValues = JSONUtilities.decodeJSONArray(jsonRoot, "externalAPITopics", false);
          if (externalAPITopicValues != null)
            {
              for (int i=0; i<externalAPITopicValues.size(); i++)
                {
                  JSONObject externalAPITopicJSON = (JSONObject) externalAPITopicValues.get(i);
                  ExternalAPITopic externalAPITopic = new ExternalAPITopic(externalAPITopicJSON);
                  externalAPITopics.put(externalAPITopic.getID(), externalAPITopic);
                }
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  supportedLanguages
      //

      try
        {
          JSONArray supportedLanguageValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedLanguages", true);
          for (int i=0; i<supportedLanguageValues.size(); i++)
            {
              JSONObject supportedLanguageJSON = (JSONObject) supportedLanguageValues.get(i);
              SupportedLanguage supportedLanguage = new SupportedLanguage(supportedLanguageJSON);
              supportedLanguages.put(supportedLanguage.getID(), supportedLanguage);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
      
      //
      //  firstDayOfTheWeek EVPRO-865
      //

      try
        {
          String day = JSONUtilities.decodeString(jsonRoot, "firstDayOfTheWeek", "SUNDAY");
          switch (day.trim().toUpperCase())
            {
            case "SUNDAY":
              firstDayOfTheWeek = Calendar.SUNDAY;
              break;
            case "MONDAY":
              firstDayOfTheWeek = Calendar.MONDAY;
              break;
            case "TUESDAY":
              firstDayOfTheWeek = Calendar.TUESDAY;
              break;
            case "WEDNESDAY":
              firstDayOfTheWeek = Calendar.WEDNESDAY;
              break;
            case "THURSDAY":
              firstDayOfTheWeek = Calendar.THURSDAY;
              break;
            case "FRIDAY":
              firstDayOfTheWeek = Calendar.FRIDAY;
              break;
            case "SATURDAY":
              firstDayOfTheWeek = Calendar.SATURDAY;
              break;
            }
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
      

      //
      //  partnerTypes
      //

      try
        {
          JSONArray partnerTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "partnerTypes", true);
          for (int i=0; i<partnerTypeValues.size(); i++)
            {
              JSONObject partnerTypesJSON = (JSONObject) partnerTypeValues.get(i);
              PartnerType partnerType = new PartnerType(partnerTypesJSON);
              partnerTypes.put(partnerType.getID(), partnerType);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  billingModes
      //

      try
        {
          JSONArray billingModeValues = JSONUtilities.decodeJSONArray(jsonRoot, "billingModes", true);
          for (int i=0; i<billingModeValues.size(); i++)
            {
              JSONObject billingModesJSON = (JSONObject) billingModeValues.get(i);
              BillingMode billingMode = new BillingMode(billingModesJSON);
              billingModes.put(billingMode.getID(), billingMode);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  criterionFieldAvailableValuesTopic
      //

      try
        {
          criterionFieldAvailableValuesTopic = JSONUtilities.decodeString(jsonRoot, "criterionFieldAvailableValuesTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  sourceAddressTopic
      //

      try
        {
          sourceAddressTopic = JSONUtilities.decodeString(jsonRoot, "sourceAddressTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  voucherChangeRequestTopicTopic
      //

      try
        {
          voucherChangeRequestTopic = JSONUtilities.decodeString(jsonRoot, "voucherChangeRequestTopic", true);
          voucherChangeResponseTopic = JSONUtilities.decodeString(jsonRoot, "voucherChangeResponseTopic", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  baseLanguageID
      //

      baseLanguageID = getSupportedLanguageID(getBaseLanguage());

      //
      //  supportedCurrencies
      //

      try
        {
          JSONArray supportedCurrencyValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedCurrencies", true);
          for (int i=0; i<supportedCurrencyValues.size(); i++)
            {
              JSONObject supportedCurrencyJSON = (JSONObject) supportedCurrencyValues.get(i);
              SupportedCurrency supportedCurrency = new SupportedCurrency(supportedCurrencyJSON);
              supportedCurrencies.put(supportedCurrency.getID(), supportedCurrency);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  supportedTimeUnits
      //

      try
        {
          JSONArray supportedTimeUnitValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedTimeUnits", true);
          for (int i=0; i<supportedTimeUnitValues.size(); i++)
            {
              JSONObject supportedTimeUnitJSON = (JSONObject) supportedTimeUnitValues.get(i);
              SupportedTimeUnit supportedTimeUnit = new SupportedTimeUnit(supportedTimeUnitJSON);
              supportedTimeUnits.put(supportedTimeUnit.getID(), supportedTimeUnit);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  supportedTokenCodesFormats
      //

      try
        {
          JSONArray supportedTokenCodesFormatsValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedTokenCodesFormats", new JSONArray());
          for (int i=0; i<supportedTokenCodesFormatsValues.size(); i++)
            {
              JSONObject supportedTokenCodesFormatJSON = (JSONObject) supportedTokenCodesFormatsValues.get(i);
              SupportedTokenCodesFormat supportedTokenCodesFormat = new SupportedTokenCodesFormat(supportedTokenCodesFormatJSON);
              supportedTokenCodesFormats.put(supportedTokenCodesFormat.getID(), supportedTokenCodesFormat);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  voucherCodePatternList
      //

      try
        {
          JSONArray voucherCodePatternListValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedVoucherCodePatternList", new JSONArray());
          for (int i=0; i<voucherCodePatternListValues.size(); i++)
            {
              JSONObject voucherCodePatternListJSON = (JSONObject) voucherCodePatternListValues.get(i);
              SupportedVoucherCodePattern voucherCodePattern = new SupportedVoucherCodePattern(voucherCodePatternListJSON);
              supportedVoucherCodePatternList.put(voucherCodePattern.getID(), voucherCodePattern);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
      
      
      
      
      //
      //  supportedRelationships
      //

      try
        {
          JSONArray supportedRelationshipValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedRelationships", new JSONArray());
          for (int i=0; i<supportedRelationshipValues.size(); i++)
            {
              JSONObject supportedRelationshipJSON = (JSONObject) supportedRelationshipValues.get(i);
              SupportedRelationship supportedRelationship = new SupportedRelationship(supportedRelationshipJSON);
              supportedRelationships.put(supportedRelationship.getID(), supportedRelationship);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  callingChannelProperties
      //

      try
        {
          JSONArray callingChannelPropertyValues = JSONUtilities.decodeJSONArray(jsonRoot, "callingChannelProperties", true);
          for (int i=0; i<callingChannelPropertyValues.size(); i++)
            {
              JSONObject callingChannelPropertyJSON = (JSONObject) callingChannelPropertyValues.get(i);
              CallingChannelProperty callingChannelProperty = new CallingChannelProperty(callingChannelPropertyJSON);
              callingChannelProperties.put(callingChannelProperty.getID(), callingChannelProperty);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  catalogCharacteristicUnits
      //

      try
        {
          JSONArray catalogCharacteristicUnitValues = JSONUtilities.decodeJSONArray(jsonRoot, "catalogCharacteristicUnits", new JSONArray());
          for (int i=0; i<catalogCharacteristicUnitValues.size(); i++)
            {
              JSONObject catalogCharacteristicUnitJSON = (JSONObject) catalogCharacteristicUnitValues.get(i);
              CatalogCharacteristicUnit catalogCharacteristicUnit = new CatalogCharacteristicUnit(catalogCharacteristicUnitJSON);
              catalogCharacteristicUnits.put(catalogCharacteristicUnit.getID(), catalogCharacteristicUnit);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  initialCallingChannelsJSONArray
      //

      initialCallingChannelsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialCallingChannels", new JSONArray());

      //
      //  initialSalesChannelsJSONArray
      //

      initialSalesChannelsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialSalesChannels", new JSONArray());

      //
      //  initialSourceAddresses
      //

      initialSourceAddressesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialSourceAddresses", new JSONArray());

      //
      //  initialSuppliersJSONArray
      //

      initialSuppliersJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialSuppliers", new JSONArray());

      //
      //  initialPartnersJSONArray
      //

      initialPartnersJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialPartners", new JSONArray());

      //
      //  initialProductsJSONArray
      //

      initialProductsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialProducts", new JSONArray());

      //
      //  initialReportsJSONArray
      //

      initialReportsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialReports", new JSONArray());

      //
      //  initialCatalogCharacteristicsJSONArray
      //

      initialCatalogCharacteristicsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialCatalogCharacteristics", new JSONArray());

      //
      //  initialContactPoliciesJSONArray
      //

      initialContactPoliciesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialContactPolicies", new JSONArray());

      //
      //  initialJourneyTemplatesJSONArray
      //

      initialJourneyTemplatesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialJourneyTemplates", new JSONArray());

      //
      //  initialJourneyObjectivesJSONArray
      //

      initialJourneyObjectivesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialJourneyObjectives", new JSONArray());

      //
      //  initialOfferObjectivesJSONArray
      //

      initialOfferObjectivesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialOfferObjectives", new JSONArray());

      //
      //  initialProductTypesJSONArray
      //

      initialProductTypesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialProductTypes", new JSONArray());

      //
      //  initialTokenTypesJSONArray
      //

      initialTokenTypesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialTokenTypes", new JSONArray());

      //
      //  initialVoucherCodeFormatsJSONArray
      //

      initialVoucherCodeFormatsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialVoucherCodeFormats", new JSONArray());

      //
      //  initialSegmentationDimensionsJSONArray
      //

      initialSegmentationDimensionsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialSegmentationDimensions", new JSONArray());
      
      //
      //  initialComplexObjectJSONArray
      //

      initialComplexObjectJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialComplexObjects", new JSONArray());

      //
      //  generateSimpleProfileDimensions
      //

      try
        {
          generateSimpleProfileDimensions = JSONUtilities.decodeBoolean(jsonRoot, "generateSimpleProfileDimensions", Boolean.TRUE);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  supportedDataTypes
      //

      try
        {
          JSONArray supportedDataTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedDataTypes", new JSONArray());
          for (int i=0; i<supportedDataTypeValues.size(); i++)
            {
              JSONObject supportedDataTypeJSON = (JSONObject) supportedDataTypeValues.get(i);
              SupportedDataType supportedDataType = new SupportedDataType(supportedDataTypeJSON);
              supportedDataTypes.put(supportedDataType.getID(), supportedDataType);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  journeyMetricConfiguration
      //

      try
        {
          JSONObject journeyMetricConfigurationJSON = (JSONObject) jsonRoot.get("journeyMetrics");
          if( journeyMetricConfigurationJSON.isEmpty() ) {
            // JourneyMetric are therefore disabled
            journeyMetricConfiguration = new JourneyMetricConfiguration();
          } else {
            int priorPeriodDays = JSONUtilities.decodeInteger(journeyMetricConfigurationJSON, "priorPeriodDays", true);
            if(priorPeriodDays < 1) {
              throw new ServerRuntimeException("ERROR: Bad 'journeyMetrics' settings. 'priorPeriodDays' field cannot be negative or null.");
            }
            
            int postPeriodDays = JSONUtilities.decodeInteger(journeyMetricConfigurationJSON, "postPeriodDays", true);
            if(postPeriodDays < 1) {
              throw new ServerRuntimeException("ERROR: Bad 'journeyMetrics' settings. 'postPeriodDays' field cannot be negative or null.");
            }
            
            Map<String,JourneyMetricDeclaration> journeyMetricDeclarations = new LinkedHashMap<String,JourneyMetricDeclaration>();
            
            JSONArray journeyMetricDeclarationValues = JSONUtilities.decodeJSONArray(journeyMetricConfigurationJSON, "metrics", new JSONArray());
            if(journeyMetricDeclarationValues.isEmpty()) {
              // JourneyMetric are therefore disabled
              journeyMetricConfiguration = new JourneyMetricConfiguration();
            } else {
              for (int i=0; i<journeyMetricDeclarationValues.size(); i++)
                {
                  JSONObject journeyMetricDeclarationJSON = (JSONObject) journeyMetricDeclarationValues.get(i);
                  JourneyMetricDeclaration journeyMetricDeclaration = new JourneyMetricDeclaration(journeyMetricDeclarationJSON);
                  journeyMetricDeclarations.put(journeyMetricDeclaration.getID(), journeyMetricDeclaration);
                }
              
              journeyMetricConfiguration = new JourneyMetricConfiguration(priorPeriodDays, postPeriodDays, journeyMetricDeclarations);
            }
          }
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  subscriberProfileDatacubeMetrics
      //

      try
        {
          JSONArray jsonArray = JSONUtilities.decodeJSONArray(jsonRoot, "subscriberProfileDatacubeMetrics", new JSONArray());
          for (int i=0; i<jsonArray.size(); i++)
            {
              JSONObject jsonObject = (JSONObject) jsonArray.get(i);
              SubscriberProfileDatacubeMetric subscriberProfileDatacubeMetric = new SubscriberProfileDatacubeMetric(jsonObject);
              subscriberProfileDatacubeMetrics.put(subscriberProfileDatacubeMetric.getID(), subscriberProfileDatacubeMetric);
            }
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  profileCriterionFields
      //

      try
        {
          //
          //  profileCriterionFields (evolution)
          //

          JSONArray criterionFieldValues = JSONUtilities.decodeJSONArray(jsonRoot, "evolutionProfileCriterionFields", new JSONArray());
          for (int i=0; i<criterionFieldValues.size(); i++)
            {
              JSONObject criterionFieldJSON = (JSONObject) criterionFieldValues.get(i);
              CriterionField criterionField = new CriterionField(criterionFieldJSON);
              profileCriterionFields.put(criterionField.getID(), criterionField);
              if(criterionField.getProfileChangeEvent()) {
                profileChangeDetectionCriterionFields.put(criterionField.getID(), criterionField);

                //
                // generation of CriterionFields related to oldValue, newValue and changedField
                //

                profileChangeGeneratedCriterionFields.putAll(generateProfileChangeCriterionFields(criterionField));
              }
            }

          //
          //  profileCriterionFields (deployment)
          //

          criterionFieldValues = JSONUtilities.decodeJSONArray(jsonRoot, "profileCriterionFields", new JSONArray());
          for (int i=0; i<criterionFieldValues.size(); i++)
            {
              JSONObject criterionFieldJSON = (JSONObject) criterionFieldValues.get(i);
              CriterionField criterionField = new CriterionField(criterionFieldJSON);
              profileCriterionFields.put(criterionField.getID(), criterionField);
              baseProfileCriterionFields.put(criterionField.getID(), criterionField);
              if(criterionField.getProfileChangeEvent()) {
                profileChangeDetectionCriterionFields.put(criterionField.getID(), criterionField);

                //
                // generation of CriterionFields related to oldValue, newValue and changedField
                //

                profileChangeGeneratedCriterionFields.putAll(generateProfileChangeCriterionFields(criterionField));
              }
            }


          //
          //  profileChangeEvent
          //

          EvolutionEngineEventDeclaration profileChangeEvent = new EvolutionEngineEventDeclaration("profile update", ProfileChangeEvent.class.getName(), getProfileChangeEventTopic(), EventRule.Standard, getProfileChangeGeneratedCriterionFields());
          evolutionEngineEvents.put(profileChangeEvent.getName(), profileChangeEvent);

        }
      catch (GUIManagerException | JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  extendedProfileCriterionFields
      //

      try
        {
          JSONArray criterionFieldValues = JSONUtilities.decodeJSONArray(jsonRoot, "extendedProfileCriterionFields", new JSONArray());
          for (int i=0; i<criterionFieldValues.size(); i++)
            {
              JSONObject criterionFieldJSON = (JSONObject) criterionFieldValues.get(i);
              CriterionField criterionField = new CriterionField(criterionFieldJSON);
              extendedProfileCriterionFields.put(criterionField.getID(), criterionField);
            }
        }
      catch (GUIManagerException | JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  presentationCriterionFields
      //

      try
        {
          JSONArray criterionFieldValues = JSONUtilities.decodeJSONArray(jsonRoot, "presentationCriterionFields", new JSONArray());
          for (int i=0; i<criterionFieldValues.size(); i++)
            {
              JSONObject criterionFieldJSON = (JSONObject) criterionFieldValues.get(i);
              CriterionField criterionField = new CriterionField(criterionFieldJSON);
              presentationCriterionFields.put(criterionField.getID(), criterionField);
            }
        }
      catch (GUIManagerException | JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

//      //
//      //  universalControlGroupCriteria
//      //
//
//      try
//        {
//          JSONArray evaluationCriterionValues = JSONUtilities.decodeJSONArray(jsonRoot, "universalControlGroupCriteria", new JSONArray());
//          for (int i=0; i<evaluationCriterionValues.size(); i++)
//            {
//              JSONObject evaluationCriterionJSON = (JSONObject) evaluationCriterionValues.get(i);
//              EvaluationCriterion evaluationCriterion = new EvaluationCriterion(evaluationCriterionJSON, CriterionContext.Profile(tenantID));
//              universalControlGroupCriteria.add(evaluationCriterion);
//            }
//        }
//      catch (GUIManagerException | JSONUtilitiesException e)
//        {
//          throw new ServerRuntimeException("deployment", e);
//        }
//
//      //
//      //  controlGroupCriteria
//      //
//
//      try
//        {
//          JSONArray evaluationCriterionValues = JSONUtilities.decodeJSONArray(jsonRoot, "controlGroupCriteria", new JSONArray());
//          for (int i=0; i<evaluationCriterionValues.size(); i++)
//            {
//              JSONObject evaluationCriterionJSON = (JSONObject) evaluationCriterionValues.get(i);
//              EvaluationCriterion evaluationCriterion = new EvaluationCriterion(evaluationCriterionJSON, CriterionContext.Profile);
//              controlGroupCriteria.add(evaluationCriterion);
//            }
//        }
//      catch (GUIManagerException | JSONUtilitiesException e)
//        {
//          throw new ServerRuntimeException("deployment", e);
//        }

      //
      //  offerProperties
      //

      try
        {
          JSONArray offerPropertyValues = JSONUtilities.decodeJSONArray(jsonRoot, "offerProperties", new JSONArray());
          for (int i=0; i<offerPropertyValues.size(); i++)
            {
              JSONObject offerPropertyJSON = (JSONObject) offerPropertyValues.get(i);
              OfferProperty offerProperty = new OfferProperty(offerPropertyJSON);
              offerProperties.put(offerProperty.getID(), offerProperty);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  scoringEngines
      //

      try
        {
          JSONArray scoringEngineValues = JSONUtilities.decodeJSONArray(jsonRoot, "scoringEngines", new JSONArray());
          for (int i=0; i<scoringEngineValues.size(); i++)
            {
              JSONObject scoringEngineJSON = (JSONObject) scoringEngineValues.get(i);
              ScoringEngine scoringEngine = new ScoringEngine(scoringEngineJSON);
              scoringEngines.put(scoringEngine.getID(), scoringEngine);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  offerOptimizationAlgorithms
      //

      try
        {
          JSONArray offerOptimizationAlgorithmValuesCommon = JSONUtilities.decodeJSONArray(jsonRoot, "offerOptimizationAlgorithmsCommon", new JSONArray());
          JSONArray offerOptimizationAlgorithmValues = JSONUtilities.decodeJSONArray(jsonRoot, "offerOptimizationAlgorithms", new JSONArray());
          offerOptimizationAlgorithmValues.addAll(offerOptimizationAlgorithmValuesCommon);
          for (int i=0; i<offerOptimizationAlgorithmValues.size(); i++)
            {
              JSONObject offerOptimizationAlgorithmJSON = (JSONObject) offerOptimizationAlgorithmValues.get(i);
              OfferOptimizationAlgorithm offerOptimizationAlgorithm = new OfferOptimizationAlgorithm(offerOptimizationAlgorithmJSON);
              offerOptimizationAlgorithms.put(offerOptimizationAlgorithm.getID(), offerOptimizationAlgorithm);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }


      //
      //  scoringTypes
      //

      try
        {
          JSONArray scoringTypesValues = JSONUtilities.decodeJSONArray(jsonRoot, "scoringTypes", new JSONArray());
          for (int i=0; i<scoringTypesValues.size(); i++)
            {
              JSONObject scoringTypeJSON = (JSONObject) scoringTypesValues.get(i);
              ScoringType scoringType = new ScoringType(scoringTypeJSON);
              scoringTypes.put(scoringType.getID(), scoringType);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }



      //
      //  dnboMatrixVariable
      //

      try
        {
          JSONArray dnboMatrixVariableValues = JSONUtilities.decodeJSONArray(jsonRoot, "dnboMatrixVariables", new JSONArray());
          for (int i=0; i<dnboMatrixVariableValues.size(); i++)
            {
              JSONObject dnboMatrixJSON = (JSONObject) dnboMatrixVariableValues.get(i);
              DNBOMatrixVariable dnboMatrixvariable = new DNBOMatrixVariable(dnboMatrixJSON);
              dnboMatrixVariables.put(dnboMatrixvariable.getID(), dnboMatrixvariable);
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }


      //
      //  deliveryManagers/fulfillmentProviders
      //

      try
        {
          JSONArray deliveryManagerValues = JSONUtilities.decodeJSONArray(jsonRoot, "deliveryManagers", new JSONArray());
          for (int i=0; i<deliveryManagerValues.size(); i++)
            {
              JSONObject deliveryManagerJSON = (JSONObject) deliveryManagerValues.get(i);
              DeliveryManagerDeclaration deliveryManagerDeclaration = new DeliveryManagerDeclaration(deliveryManagerJSON);
              deliveryManagers.put(deliveryManagerDeclaration.getDeliveryType(), deliveryManagerDeclaration);
              if (deliveryManagerDeclaration.getProviderID() != null)
                {
                  fulfillmentProviders.put(deliveryManagerDeclaration.getProviderID(), deliveryManagerDeclaration);
                }
            }
          //TODO remove later, forcing conf cleaning
          if(getDeliveryManagers().get("notificationmanager")!=null)
            {
              log.warn("notificationmanager deliveryManager declaration is not possible anymore, clean it once you don't have any more in history");
            }
          // auto generated notif ones
          for (CommunicationChannel cc:getCommunicationChannels().values())
            {
              if (cc.getDeliveryManagerDeclaration()!=null) deliveryManagers.put(cc.getDeliveryManagerDeclaration().getDeliveryType(),cc.getDeliveryManagerDeclaration());
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  deliveryManagerAccounts
      //

      try
        {
          JSONArray deliveryManagerAccountValues = JSONUtilities.decodeJSONArray(jsonRoot, "deliveryManagerAccounts", new JSONArray());
          for (int i=0; i<deliveryManagerAccountValues.size(); i++)
            {
              JSONObject deliveryManagerAccountJSON = (JSONObject) deliveryManagerAccountValues.get(i);
              DeliveryManagerAccount deliveryManagerAccount = new DeliveryManagerAccount(deliveryManagerAccountJSON);
              if(deliveryManagerAccount != null ){
                deliveryManagerAccounts.put(deliveryManagerAccount.getProviderID(), deliveryManagerAccount);
              }
            }
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  journeyUniversalEligibilityCriteria
      //
      try
        {
          JSONArray evaluationCriterionValues = JSONUtilities.decodeJSONArray(jsonRoot, "journeyUniversalEligibilityCriteria", new JSONArray());
          for (int i=0; i<evaluationCriterionValues.size(); i++)
            {
              JSONObject evaluationCriterionJSON = (JSONObject) evaluationCriterionValues.get(i);
              EvaluationCriterion evaluationCriterion = new EvaluationCriterion(evaluationCriterionJSON, CriterionContext.Profile(tenantID), tenantID);
              getJourneyUniversalEligibilityCriteria().add(evaluationCriterion);                  
            }
        }
      catch (GUIManagerException | JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }


      //
      //  nodeTypes
      //

      try
        {
          JSONArray nodeTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "nodeTypes", new JSONArray());
          for (int i=0; i<nodeTypeValues.size(); i++)
            {
              JSONObject nodeTypeJSON = (JSONObject) nodeTypeValues.get(i);
              NodeType nodeType = new NodeType(nodeTypeJSON);
              nodeTypes.put(nodeType.getID(), nodeType);
            }

          // Add generated Node Types:
          // * Generic Notification Manager
          ArrayList<String> notificationNodeTypesAsString = NotificationManager.getNotificationNodeTypes();
          for(String current : notificationNodeTypesAsString) {
            try {
              JSONObject jsonNodeTypeRoot = (JSONObject) (new JSONParser()).parse(current);
              NodeType nodeType = new NodeType(jsonNodeTypeRoot);
              nodeTypes.put(nodeType.getID(), nodeType);
            }
            catch(Exception e) {
              log.warn("Deployment: Can't interpret nodeType definition : " + current + " due to exception " + e.getClass().getName(), e);
            }
          }
        }
      catch (GUIManagerException | JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      // check nodeTypes about action manager TriggerEventAction
      //

      for(NodeType nodeType:nodeTypes.values()){
        if(nodeType.getActionManager()==null) continue;
        if(nodeType.getActionManager() instanceof EvolutionEngine.TriggerEventAction){
          EvolutionEngine.TriggerEventAction triggerEventAction = (EvolutionEngine.TriggerEventAction)nodeType.getActionManager();
          triggerEventAction.getEventDeclaration().markAsTriggerEvent();
          if(!triggerEventAction.getEventDeclaration().getEventClass().getSuperclass().equals(SubscriberStreamOutput.class)){
            throw new ServerRuntimeException("deployment nodeType "+nodeType.getID()+" "+nodeType.getName()+" eventName in action declaration does not extends "+SubscriberStreamOutput.class.getCanonicalName()+", it needs to");
          }
        }
      }

      //
      //  journeyToolboxSections
      //

      try
        {
          JSONArray journeyToolboxSectionValues = JSONUtilities.decodeJSONArray(jsonRoot, "journeyToolbox", new JSONArray());
          for (int i=0; i<journeyToolboxSectionValues.size(); i++)
            {
              JSONObject journeyToolboxSectionValueJSON = (JSONObject) journeyToolboxSectionValues.get(i);
              ToolboxSection journeyToolboxSection = new ToolboxSection(journeyToolboxSectionValueJSON);
              journeyToolbox.put(journeyToolboxSection.getID(), journeyToolboxSection);
            }

          // Iterate over the communication channels and, for generic ones, let enrich, if needed the journey toolbox
          for(CommunicationChannel cc : getCommunicationChannels().values())
            {
              if(cc.isGeneric() && cc.getJourneyGUINodeSectionID() != null)
                {
                  ToolboxSection section = journeyToolbox.get(cc.getJourneyGUINodeSectionID());
                  if(section == null) {
                    log.warn("Deployment: Can't retrieve Journey ToolBoxSection for " + cc.getJourneyGUINodeSectionID() + " for communicationChannel " + cc.getID());
                  }
                  else {
                    JSONArray items = JSONUtilities.decodeJSONArray(section.getJSONRepresentation(), "items");
                    if(items != null) {
                      JSONObject item = new JSONObject();
                      item.put("id", cc.getToolboxID());
                      item.put("name", cc.getName());
                      // ensure this box effectively exists
                      if(getNodeTypes().get(cc.getToolboxID()) != null) {
                        items.add(item);
                      }
                      else {
                        log.warn("Deployment: Can't retrieve Journey NodeType for " + cc.getToolboxID() + " for communicationChannel " + cc.getID());
                      }
                    }
                    section.getJSONRepresentation().put("items", items);
                  }
                }
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  campaignToolboxSections
      //

      try
        {
          JSONArray campaignToolboxSectionValues = JSONUtilities.decodeJSONArray(jsonRoot, "campaignToolbox", new JSONArray());
          for (int i=0; i<campaignToolboxSectionValues.size(); i++)
            {
              JSONObject campaignToolboxSectionValueJSON = (JSONObject) campaignToolboxSectionValues.get(i);
              ToolboxSection campaignToolboxSection = new ToolboxSection(campaignToolboxSectionValueJSON);
              campaignToolbox.put(campaignToolboxSection.getID(), campaignToolboxSection);
            }

          // Iterate over the communication channels and, for generic ones, let enrich, if needed the campaign toolbox
          for(CommunicationChannel cc : getCommunicationChannels().values())
            {
              if(cc.isGeneric() && cc.getCampaignGUINodeSectionID() != null)
                {
                  ToolboxSection section = campaignToolbox.get(cc.getCampaignGUINodeSectionID());
                  if(section == null) {
                    log.warn("Deployment: Can't retrieve Campaign ToolBoxSection for " + cc.getCampaignGUINodeSectionID() + " for communicationChannel " + cc.getID());
                  }
                  else {
                    JSONArray items = JSONUtilities.decodeJSONArray(section.getJSONRepresentation(), "items");
                    if(items != null) {
                      JSONObject item = new JSONObject();
                      item.put("id", cc.getToolboxID());
                      item.put("name", cc.getName());
                      // ensure this box effectively exists
                      if(getNodeTypes().get(cc.getToolboxID()) != null) {
                        items.add(item);
                      }
                      else {
                        log.warn("Deployment: Can't retrieve Campaign NodeType for " + cc.getToolboxID() + " for communicationChannel " + cc.getID());
                      }
                    }
                    section.getJSONRepresentation().put("items", items);
                  }
                }
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  workflowToolboxSections
      //

      try
        {
          JSONArray workflowToolboxSectionValues = JSONUtilities.decodeJSONArray(jsonRoot, "workflowToolbox", new JSONArray());
          for (int i=0; i<workflowToolboxSectionValues.size(); i++)
            {
              JSONObject workflowToolboxSectionValueJSON = (JSONObject) workflowToolboxSectionValues.get(i);
              ToolboxSection workflowToolboxSection = new ToolboxSection(workflowToolboxSectionValueJSON);
              workflowToolbox.put(workflowToolboxSection.getID(), workflowToolboxSection);
            }

          // Iterate over the communication channels and, for generic ones, let enrich, if needed the workflow toolbox
          for(CommunicationChannel cc : getCommunicationChannels().values())
            {
              if(cc.isGeneric() && cc.getWorkflowGUINodeSectionID() != null)
                {
                  ToolboxSection section = workflowToolbox.get(cc.getWorkflowGUINodeSectionID());
                  if(section == null) {
                    log.warn("Deployment: Can't retrieve ToolBoxSection for " + cc.getWorkflowGUINodeSectionID() + " for communicationChannel " + cc.getID());
                  }
                  else {
                    JSONArray items = JSONUtilities.decodeJSONArray(section.getJSONRepresentation(), "items");
                    if(items != null) {
                      JSONObject item = new JSONObject();
                      item.put("id", cc.getToolboxID());
                      item.put("name", cc.getName());
                      // ensure this box effectively exists
                      if(getNodeTypes().get(cc.getToolboxID()) != null) {
                        items.add(item);
                      }
                      else {
                        log.warn("Deployment: Can't retrieve NodeType for " + cc.getToolboxID() + " for communicationChannel " + cc.getID());
                      }
                    }
                    section.getJSONRepresentation().put("items", items);
                  }
                }
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  loyaltyWorkflowToolboxSections
      //

      try
        {
          JSONArray loyaltyWorkflowToolboxSectionValues = JSONUtilities.decodeJSONArray(jsonRoot, "loyaltyWorkflowToolbox", new JSONArray());
          for (int i=0; i<loyaltyWorkflowToolboxSectionValues.size(); i++)
            {
              JSONObject workflowToolboxSectionValueJSON = (JSONObject) loyaltyWorkflowToolboxSectionValues.get(i);
              ToolboxSection loyaltyWorkflowToolboxSection = new ToolboxSection(workflowToolboxSectionValueJSON);
              loyaltyWorkflowToolbox.put(loyaltyWorkflowToolboxSection.getID(), loyaltyWorkflowToolboxSection);
            }

          // Iterate over the communication channels and, for generic ones, let enrich, if needed the workflow toolbox
          for(CommunicationChannel cc : getCommunicationChannels().values())
            {
              if(cc.isGeneric() && cc.getWorkflowGUINodeSectionID() != null)
                {
                  ToolboxSection section = loyaltyWorkflowToolbox.get(cc.getWorkflowGUINodeSectionID());
                  if(section == null) {
                    log.warn("Deployment: Can't retrieve ToolBoxSection for " + cc.getWorkflowGUINodeSectionID() + " for communicationChannel " + cc.getID());
                  }
                  else {
                    JSONArray items = JSONUtilities.decodeJSONArray(section.getJSONRepresentation(), "items");
                    if(items != null) {
                      JSONObject item = new JSONObject();
                      item.put("id", cc.getToolboxID());
                      item.put("name", cc.getName());
                      // ensure this box effectively exists
                      if(getNodeTypes().get(cc.getToolboxID()) != null) {
                        items.add(item);
                      }
                      else {
                        log.warn("Deployment: Can't retrieve NodeType for " + cc.getToolboxID() + " for communicationChannel " + cc.getID());
                      }
                    }
                    section.getJSONRepresentation().put("items", items);
                  }
                }
            }
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  thirdPartyMethodPermissions
      //

      try
        {
          JSONArray thirdPartyMethodPermissions = JSONUtilities.decodeJSONArray(jsonRoot, "thirdPartyMethodPermissions", new JSONArray());
          for (int i=0; i<thirdPartyMethodPermissions.size(); i++)
            {
              JSONObject thirdPartyMethodPermissionsJSON = (JSONObject) thirdPartyMethodPermissions.get(i);
              String methodName = JSONUtilities.decodeString(thirdPartyMethodPermissionsJSON, "methodName", Boolean.TRUE);
              ThirdPartyMethodAccessLevel thirdPartyMethodAccessLevel = new ThirdPartyMethodAccessLevel(thirdPartyMethodPermissionsJSON);
              thirdPartyMethodPermissionsMap.put(methodName, thirdPartyMethodAccessLevel);
            }
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  authResponseCacheLifetimeInMinutes
      //

      try
        {
          authResponseCacheLifetimeInMinutes = JSONUtilities.decodeInteger(jsonRoot, "authResponseCacheLifetimeInMinutes", false);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      try
      {
        reportManagerMaxMessageLength = JSONUtilities.decodeInteger(jsonRoot, "reportManagerMaxMessageLength", 200);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
      
      //
      //  stockRefreshPeriod
      //

      stockRefreshPeriod = JSONUtilities.decodeInteger(jsonRoot, "stockRefreshPeriod", 5);

      //
      //  periodicEvaluationCronEntry
      //

      periodicEvaluationCronEntry = JSONUtilities.decodeString(jsonRoot, "periodicEvaluationCronEntry", false);

      //
      //  ucgEvaluationCronEntry
      //

      ucgEvaluationCronEntry = JSONUtilities.decodeString(jsonRoot, "ucgEvaluationCronEntry", false);

      //
      //  Reports
      //

      try
        {
          JSONObject reportManager = JSONUtilities.decodeJSONObject(jsonRoot, "reportManager", false);
          if (reportManager != null)
            {
              reportManagerZookeeperDir = JSONUtilities.decodeString(reportManager, "reportManagerZookeeperDir", true);
              reportManagerOutputPath = JSONUtilities.decodeString(reportManager, "reportManagerOutputPath", "/app/reports");
              reportManagerDateFormat = JSONUtilities.decodeString(reportManager, "reportManagerDateFormat", "yyyy-MM-dd_HH-mm-ss_SSSS");
              reportManagerFileExtension = JSONUtilities.decodeString(reportManager, "reportManagerFileExtension", "csv");
              reportManagerCsvSeparator = JSONUtilities.decodeString(reportManager, "reportManagerCsvSeparator", ";");
              reportManagerFieldSurrounder = JSONUtilities.decodeString(reportManager, "reportManagerFieldSurrounder", "'");
              reportManagerStreamsTempDir = JSONUtilities.decodeString(reportManager, "reportManagerStreamsTempDir", System.getProperty("java.io.tmpdir"));
              reportManagerTopicsCreationProperties = JSONUtilities.decodeString(reportManager, "reportManagerTopicsCreationProperties", "cleanup.policy=delete segment.bytes=52428800 retention.ms=86400000");
              journeysReportMaxParallelThreads = JSONUtilities.decodeInteger(reportManager, "journeysReportMaxParallelThreads", 10);
              detailedrecordReportsArrearCount = JSONUtilities.decodeInteger(reportManager, "detailedrecordReportsArrearCount", 7);
              journeyReportsArrearCount = JSONUtilities.decodeInteger(reportManager, "journeyReportsArrearCount", 2);
              subscriberprofileReportsArrearCount = JSONUtilities.decodeInteger(reportManager, "subscriberprofileReportsArrearCount", 2);
            }
          else
            {
              reportManagerZookeeperDir = getZookeeperRoot() + File.separator + "reports";
              reportManagerOutputPath = "/app/reports";
              reportManagerDateFormat = "yyyy-MM-dd_HH-mm-ss_SSSS";
              reportManagerFileExtension = "csv";
              reportManagerCsvSeparator = ";";
              reportManagerFieldSurrounder = "'";
              reportManagerStreamsTempDir = System.getProperty("java.io.tmpdir");
              reportManagerTopicsCreationProperties = "cleanup.policy=delete segment.bytes=52428800 retention.ms=86400000";
              journeysReportMaxParallelThreads = 10;
            }
          if (reportManagerFieldSurrounder.length() > 1)
            {
              log.warn("reportManagerFieldSurrounder is not a single character, this would lead to errors in the reports, truncating, please fix this : " + reportManagerFieldSurrounder);
              reportManagerFieldSurrounder = reportManagerFieldSurrounder.substring(0, 1);
            }
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment : reportManager", e);
        }

      //
      //  customerMetaData
      //

      try
        {
          customerMetaData = new CustomerMetaData(JSONUtilities.decodeJSONObject(jsonRoot, "customerMetaData", true));
        }
      catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  uploadedFileSeparator
      //

      try
        {
          uploadedFileSeparator = JSONUtilities.decodeString(jsonRoot, "uploadedFileSeparator", false);
          if(uploadedFileSeparator == null) {
            uploadedFileSeparator = ";";
          }
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  APIresponseDateFormat
      //

      try
        {
          APIresponseDateFormat = JSONUtilities.decodeString(jsonRoot, "APIresponseDateFormat", false);
          if (null == APIresponseDateFormat) APIresponseDateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZ" ;
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  deliveryTypeCommunicationChannelMap
      //

      try
        {
          for (DeliveryManagerDeclaration deliveryManagerDeclaration:getDeliveryManagers().values())
            {
              for (CommunicationChannel communicationChannel:getCommunicationChannels().values())
                {
                  if (deliveryManagerDeclaration.getDeliveryType().equals(communicationChannel.getDeliveryType()))
                    {
                      deliveryTypeCommunicationChannelIDMap.put(communicationChannel.getDeliveryType(), communicationChannel.getID());
                    }
                }
            }
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  hourlyReportCronEntryString
      //

      try
        {
          hourlyReportCronEntryString = JSONUtilities.decodeString(jsonRoot, "hourlyReportCronEntryString", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  dailyReportCronEntryString
      //

      try
        {
          dailyReportCronEntryString = JSONUtilities.decodeString(jsonRoot, "dailyReportCronEntryString", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  weeklyReportCronEntryString
      //

      try
        {
          weeklyReportCronEntryString = JSONUtilities.decodeString(jsonRoot, "weeklyReportCronEntryString", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  monthlyReportCronEntryString
      //

      try
        {
          monthlyReportCronEntryString = JSONUtilities.decodeString(jsonRoot, "monthlyReportCronEntryString", true);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      //  enableProfileSegmentChange
      //

      try
        {
          enableEvaluateTargetRandomness = JSONUtilities.decodeBoolean(jsonRoot, "enableEvaluateTargetRandomness", Boolean.FALSE);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      // conf for elasticsearch & voucher
      //

      try
        {
          // we won't deliver a voucher that expiry in less than X hours from now :
          minExpiryDelayForVoucherDeliveryInHours = JSONUtilities.decodeInteger(jsonRoot, "minExpiryDelayForVoucherDeliveryInHours",4);
          // the bulk size when importing voucher file into ES
          importVoucherFileBulkSize = JSONUtilities.decodeInteger(jsonRoot, "importVoucherFileBulkSize",5000);
          // the cache cleaner frequency in seconds for caching voucher with 0 stock from ES, and shrinking back "auto adjust concurrency number"
          voucherESCacheCleanerFrequencyInSec = JSONUtilities.decodeInteger(jsonRoot, "voucherESCacheCleanerFrequencyInSec",300);
          // an approximation of number of total concurrent process tyring to allocate Voucher in // to ES, but should not need to configure, algo should auto-adjust this
          numberConcurrentVoucherAllocationToES = JSONUtilities.decodeInteger(jsonRoot, "numberConcurrentVoucherAllocationToES",10);
        }
      catch (JSONUtilitiesException|NumberFormatException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }


      //
      // conf for propensity service
      //

      try
        {
          // period in ms global propensity state will be read from zookeeper :
          propensityReaderRefreshPeriodMs = JSONUtilities.decodeInteger(jsonRoot, "propensityReaderRefreshPeriodMs",10000);
          // period in ms local propensity state will be write to zookeeper :
          propensityWriterRefreshPeriodMs = JSONUtilities.decodeInteger(jsonRoot, "propensityWriterRefreshPeriodMs",10000);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

        try
          {
            kafkaRetentionDaysExpiredTokens = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysExpiredTokens",31);
            kafkaRetentionDaysExpiredVouchers = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysExpiredVouchers",31);
            kafkaRetentionDaysJourneys = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysJourneys",31);
            kafkaRetentionDaysCampaigns = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysCampaigns",31);
            // adjusting and warning if too low for journey metric feature to work
            if(Deployment.getJourneyMetricConfiguration().getPostPeriodDays() > kafkaRetentionDaysCampaigns+2){
              kafkaRetentionDaysCampaigns = Deployment.getJourneyMetricConfiguration().getPostPeriodDays() + 2;
              log.warn("Deployment: auto increasing kafkaRetentionDaysCampaigns to "+kafkaRetentionDaysCampaigns+" to comply with configured journey metric postPeriodDays of "+Deployment.getJourneyMetricConfiguration().getPostPeriodDays()+" (need at least 2 days more)");
            }
            kafkaRetentionDaysBulkCampaigns = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysBulkCampaigns",7);
            kafkaRetentionDaysLoyaltyPrograms = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysLoyaltyPrograms",31);
            kafkaRetentionDaysODR = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysODR",91);
            kafkaRetentionDaysBDR = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysBDR",91);
            kafkaRetentionDaysMDR = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysMDR",91);
            kafkaRetentionDaysTargets = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysTargets",91);
          }
        catch (JSONUtilitiesException|NumberFormatException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }

      try
        {
          nodesTransitionsHistorySize = JSONUtilities.decodeInteger(jsonRoot, "nodesTransitionsHistorySize",10);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
        
      try
        {
          enableContactPolicyProcessing = JSONUtilities.decodeBoolean(jsonRoot, "enableContactPolicyProcessing", Boolean.TRUE);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      try
        {
          enableContactPolicyProcessing = JSONUtilities.decodeBoolean(jsonRoot, "enableContactPolicyProcessing", Boolean.TRUE);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }

      //
      // configuration for extracts
      //
      try
        {
          JSONObject extractManager = JSONUtilities.decodeJSONObject(jsonRoot, "extractManager", false);
          if (extractManager != null)
            {
              extractManagerZookeeperDir = JSONUtilities.decodeString(extractManager, "extractManagerZookeeperDir", true);
              extractManagerOutputPath = JSONUtilities.decodeString(extractManager, "extractManagerOutputPath", "/app/extracts");
              extractManagerDateFormat = JSONUtilities.decodeString(extractManager, "extractManagerDateFormat", "yyyy-MM-dd_HH-mm-ss_SSSS");
              extractManagerFileExtension = JSONUtilities.decodeString(extractManager, "extractManagerFileExtension", "csv");
              extractManagerCsvSeparator = JSONUtilities.decodeString(extractManager, "extractManagerCsvSeparator", ";");
              extractManagerFieldSurrounder = JSONUtilities.decodeString(extractManager, "extractManagerFieldSurrounder", "'");
            }
          else
            {
              extractManagerZookeeperDir = getZookeeperRoot() + File.separator + "extracts";
              extractManagerOutputPath = "/app/extracts";
              extractManagerDateFormat = "yyyy-MM-dd_HH-mm-ss_SSSS";
              extractManagerFileExtension = "csv";
              extractManagerCsvSeparator = ";";
              extractManagerFieldSurrounder = "'";
            }
          if (extractManagerFieldSurrounder.length() > 1)
            {
              log.warn("extractManagerFieldSurrounder is not a single character, this would lead to errors in the extracts, truncating, please fix this : " + extractManagerFieldSurrounder);
              extractManagerFieldSurrounder = extractManagerFieldSurrounder.substring(0, 1);
            }
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment : extractManager", e);
        }
      
      try
        {
          recurrentCampaignCreationDaysRange = JSONUtilities.decodeInteger(jsonRoot, "recurrentCampaignCreationDaysRange", 3);
        }
      catch (JSONUtilitiesException e)
        {
          throw new ServerRuntimeException("deployment", e);
        }
      
      //
      // all dynamic topics
      //
      allTopics = new HashMap<>();
      // from delivery managers
      for(DeliveryManagerDeclaration dmd:getDeliveryManagers().values())
        {
          dmd.getRequestTopics().stream().filter(Topic::isAutoCreated).forEach(topic->allTopics.put(topic.getName(),topic));
          dmd.getResponseTopics().stream().filter(Topic::isAutoCreated).forEach(topic->allTopics.put(topic.getName(),topic));
          if(dmd.getRoutingTopic()!=null&&dmd.getRoutingTopic().isAutoCreated()) allTopics.put(dmd.getRoutingTopic().getName(),dmd.getRoutingTopic());
        }
      // for event (only preprocessor topics one for now!)
      for(EvolutionEngineEventDeclaration declaration:getEvolutionEngineEvents().values())
        {
          if(declaration.getPreprocessTopic()!=null){
            isPreprocessorNeeded=true;
            allTopics.put(declaration.getPreprocessTopic().getName(),declaration.getPreprocessTopic());
          }
        }
      
      
      
      //
      // End of initialization 
      //
      loaded = true;
    }
  
  
  public static Deployment getDeployment(int tenantID)
  {
    Deployment result = deploymentsPerTenant.get(tenantID);
    if(result == null)
      {
        synchronized(lock)
          {
            result = deploymentsPerTenant.get(tenantID);
            if(result == null)
              {
                result = new Deployment(tenantID);
                deploymentsPerTenant.put(tenantID, result);
              }
          }
      }
    return deploymentsPerTenant.get(tenantID);
  }
  
  public static Map<Integer, Deployment> getDeployments()
  {
    return deploymentsPerTenant;
  }

  /*****************************************
   *
   *  main (for validation)
   *
   *****************************************/

  public static void main(String[] args)
  {
    //System.out.println("zookeeper root: " + getZookeeperRoot());
  }
}
