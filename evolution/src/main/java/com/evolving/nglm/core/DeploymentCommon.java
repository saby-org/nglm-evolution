package com.evolving.nglm.core;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.evolution.BillingMode;
import com.evolving.nglm.evolution.CallingChannelProperty;
import com.evolving.nglm.evolution.CatalogCharacteristicUnit;
import com.evolving.nglm.evolution.CommunicationChannel;
import com.evolving.nglm.evolution.CommunicationChannelTimeWindow;
import com.evolving.nglm.evolution.CriterionContext;
import com.evolving.nglm.evolution.CriterionField;
import com.evolving.nglm.evolution.CriterionFieldRetriever;
import com.evolving.nglm.evolution.CustomerMetaData;
import com.evolving.nglm.evolution.DNBOMatrixVariable;
import com.evolving.nglm.evolution.DeliveryManagerAccount;
import com.evolving.nglm.evolution.DeliveryManagerDeclaration;
import com.evolving.nglm.evolution.EvaluationCriterion;
import com.evolving.nglm.evolution.EvolutionEngine;
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration;
import com.evolving.nglm.evolution.EvolutionEngineExtension;
import com.evolving.nglm.evolution.ExtendedSubscriberProfile;
import com.evolving.nglm.evolution.ExternalAPI;
import com.evolving.nglm.evolution.ExternalAPITopic;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManagerExtension;
import com.evolving.nglm.evolution.JourneyMetricDeclaration;
import com.evolving.nglm.evolution.NodeType;
import com.evolving.nglm.evolution.NotificationManager;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm;
import com.evolving.nglm.evolution.OfferProperty;
import com.evolving.nglm.evolution.PartnerType;
import com.evolving.nglm.evolution.ProfileChangeEvent;
import com.evolving.nglm.evolution.PropensityRule;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.ScheduledJobConfiguration;
import com.evolving.nglm.evolution.ScoringEngine;
import com.evolving.nglm.evolution.ScoringType;
import com.evolving.nglm.evolution.SubscriberProfile;
import com.evolving.nglm.evolution.SupportedCurrency;
import com.evolving.nglm.evolution.SupportedDataType;
import com.evolving.nglm.evolution.SupportedLanguage;
import com.evolving.nglm.evolution.SupportedRelationship;
import com.evolving.nglm.evolution.SupportedTimeUnit;
import com.evolving.nglm.evolution.SupportedTokenCodesFormat;
import com.evolving.nglm.evolution.SupportedVoucherCodePattern;
import com.evolving.nglm.evolution.ThirdPartyMethodAccessLevel;
import com.evolving.nglm.evolution.ToolboxSection;
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration.EventRule;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.datacubes.SubscriberProfileDatacubeMetric;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchConnectionSettings;
import com.evolving.nglm.evolution.kafka.Topic;

/**
 * This class does only contains static variable because all those settings are shared between
 * each tenant and should not be override.  
 */
public class DeploymentCommon
{
  protected static final Logger log = LoggerFactory.getLogger(DeploymentCommon.class);
  protected static Map<Integer, JSONObject> jsonConfigPerTenant = new HashMap<>();
  
  /**
   * Static initialization of DeploymentCommon 
   * 
   * This should be one of the first code executed because a lot of static code in GUIManagedObject 
   * depends of variables that will be initialized here.
   * 
   * Be careful, the order of every call is really important to avoid circular dead lock. 
   */
  static 
  {
    try
      {
        //
        // Extract jsonRoot from Zookeeper
        //
        JSONObject brutJsonRoot = getBrutJsonRoot();
    
        //
        // Build & store every version of jsonRoot per tenant
        //
        buildJsonPerTenant(brutJsonRoot);
        
        //
        // Get jsonRoot for tenantID = 0 (common settings)
        //
        JSONObject commonJsonRoot = jsonConfigPerTenant.get(0); 
        
        //
        // Init settings that will be needed for the initialization of GUIManagedObject static code
        //
        initializeCoreSettings(commonJsonRoot);
        
        //
        // Init of static GUIManagedObject
        //
        // The goal of this line is to be sure that the static code of GUIManagedObject will be executed before any static code of any class that 
        // extends GUIManagedObject. If one class is init before, then the call of commonSchema() in the static code of this class will return null,
        // resulting in a java.lang.ExceptionInInitializerError caused by: java.lang.NullPointerException
        GUIManagedObject.commonSchema();
        
        //
        // Init of all other common settings
        //
        initializeCommonSettings(jsonConfigPerTenant.get(0));
      } 
    catch (Exception e)
      {
        throw new RuntimeException("Unable to run DeploymentCommon static initialization.", e);
      }
  }
  

  /*****************************************
  *
  * Common data
  *
  *****************************************/
  // /!\ WARNING: DO NOT INSTANTIATE ANY VARIABLE HERE. BECAUSE IT IS VERY BAD PRACTICE, PLUS
  // IT WILL BE READ AFTER THE STATIC MAIN BLOC AND THEREFORE WILL OVERRIDE THE VALUE AFTERWARDS,
  // RESULTING IN A NULL POINTER EXCEPTION, OR WORSE.
  
  //
  // System
  //
  private static String evolutionVersion;
  private static String customerVersion;
  private static JSONObject licenseManagement;
  //
  // Elasticsearch
  //
  private static String elasticsearchHost;
  private static int elasticsearchPort;
  private static String elasticsearchUserName;
  private static String elasticsearchUserPassword;
  private static int elasticsearchScrollSize;
  private static int elasticSearchScrollKeepAlive;
  private static int elasticsearchDefaultShards;
  private static int elasticsearchDefaultReplicas;
  private static int elasticsearchSubscriberprofileShards;
  private static int elasticsearchSubscriberprofileReplicas;
  private static int elasticsearchSnapshotShards;
  private static int elasticsearchSnapshotReplicas;
  private static int elasticsearchLiveVoucherShards;
  private static int elasticsearchLiveVoucherReplicas;
  private static int elasticsearchRetentionDaysODR;
  private static int elasticsearchRetentionDaysBDR;
  private static int elasticsearchRetentionDaysMDR;
  private static int elasticsearchRetentionDaysTokens;
  private static int elasticsearchRetentionDaysSnapshots;
  private static int elasticsearchRetentionDaysVDR;
  //
  // Kafka
  //
  private static int topicSubscriberPartitions;
  private static int topicReplication;
  private static String topicMinInSyncReplicas;
  private static String topicRetentionShortMs;
  private static String topicRetentionMs;
  private static String topicRetentionLongMs;
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
  //
  // Topics
  //
  private static String subscriberTraceControlTopic;
  private static String subscriberTraceControlAssignSubscriberIDTopic;
  private static String subscriberTraceTopic;
  private static String simulatedTimeTopic;
  private static String assignSubscriberIDsTopic;
  private static String assignExternalSubscriberIDsTopic;
  private static String updateExternalSubscriberIDTopic;
  private static String recordSubscriberIDTopic;
  private static String recordAlternateIDTopic;
  private static String autoProvisionedSubscriberChangeLog;
  private static String autoProvisionedSubscriberChangeLogTopic;
  private static String rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic;
  private static String cleanupSubscriberTopic;
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
  private static String deliverableSourceTopic;
  private static String presentationLogTopic;
  private static String acceptanceLogTopic;
  private static String profileLoyaltyProgramChangeEventTopic;
  private static String profileChangeEventTopic;
  private static String profileSegmentChangeEventTopic;
  private static String voucherActionTopic;
  private static String fileWithVariableEventTopic;
  private static String tokenRedeemedTopic;
  private static String uploadedFileTopic;
  private static String targetTopic;
  private static String communicationChannelTopic;
  private static String communicationChannelBlackoutTopic;
  private static String communicationChannelTimeWindowTopic;
  private static String tokenChangeTopic;
  private static String loyaltyProgramTopic;
  private static String exclusionInclusionTargetTopic;
  private static String dnboMatrixTopic;
  private static String segmentContactPolicyTopic;
  private static String dynamicEventDeclarationsTopic;
  private static String dynamicCriterionFieldsTopic;
  private static String criterionFieldAvailableValuesTopic;
  private static String sourceAddressTopic;
  private static String voucherChangeRequestTopic;
  private static String voucherChangeResponseTopic;
  //
  //
  // ??????????????????????????????????????????????????????????????
  private static Map<String,AlternateID> alternateIDs;
  private static String externalSubscriberID;
  
  
  
  //
  // TODO EVPRO-99 A TRIER (sortir de Common & retirer static)
  //
  private static String reportManagerZookeeperDir;
  private static String reportManagerOutputPath;
  private static String reportManagerDateFormat;
  private static String reportManagerFileExtension;
  private static String reportManagerStreamsTempDir;
  private static String reportManagerTopicsCreationProperties;
  private static String reportManagerCsvSeparator;
  private static String reportManagerFieldSurrounder;
  private static String hourlyReportCronEntryString;
  private static String dailyReportCronEntryString;
  private static String weeklyReportCronEntryString;
  private static String monthlyReportCronEntryString;
  private static Set<String> cleanupSubscriberElasticsearchIndexes;
  private static String subscriberTraceControlAlternateID;
  private static boolean subscriberTraceControlAutoProvision;
  private static Map<String,AutoProvisionEvent> autoProvisionEvents;
  private static int httpServerScalingFactor;
  private static int evolutionEngineStreamThreads;
  private static int evolutionEngineInstanceNumbers;
  private static String subscriberGroupLoaderAlternateID;
  private static String getCustomerAlternateID;
  private static boolean subscriberGroupLoaderAutoProvision;
  private static String criterionFieldRetrieverClassName;
  private static String evolutionEngineExtensionClassName;
  private static String guiManagerExtensionClassName;
  private static String subscriberProfileClassName;
  private static String extendedSubscriberProfileClassName;
  private static String evolutionEngineExternalAPIClassName;
  private static Map<String,EvolutionEngineEventDeclaration> evolutionEngineEvents;
  private static Map<String,CriterionField> profileChangeDetectionCriterionFields;
  private static Map<String,CriterionField> profileChangeGeneratedCriterionFields;
  private static boolean enableProfileSegmentChange;
  private static int propensityInitialisationPresentationThreshold;
  private static int propensityInitialisationDurationInDaysThreshold;
  private static String subscriberProfileRegistrySubject;
  private static Map<String,Long> journeyTemplateCapacities;
  private static Map<String,ExternalAPITopic> externalAPITopics;
  private static Map<String,CallingChannelProperty> callingChannelProperties;
  private static Map<String,CatalogCharacteristicUnit> catalogCharacteristicUnits;
  private static Map<String,SupportedTokenCodesFormat> supportedTokenCodesFormats;
  private static Map<String,SupportedVoucherCodePattern> supportedVoucherCodePatternList;
  private static Map<String,SupportedRelationship> supportedRelationships;
  private static Map<String,PartnerType> partnerTypes;
  private static JSONArray initialCallingChannelsJSONArray;
  private static JSONArray initialSalesChannelsJSONArray;
  private static JSONArray initialSourceAddressesJSONArray;
  private static JSONArray initialSuppliersJSONArray;
  private static JSONArray initialPartnersJSONArray;
  private static JSONArray initialProductsJSONArray;
  private static JSONArray initialReportsJSONArray;
  private static JSONArray initialCatalogCharacteristicsJSONArray;
  private static JSONArray initialContactPoliciesJSONArray;
  private static JSONArray initialJourneyTemplatesJSONArray;
  private static JSONArray initialJourneyObjectivesJSONArray;
  private static JSONArray initialOfferObjectivesJSONArray;
  private static JSONArray initialProductTypesJSONArray;
  private static JSONArray initialTokenTypesJSONArray;
  private static JSONArray initialVoucherCodeFormatsJSONArray;
  private static JSONArray initialSegmentationDimensionsJSONArray;
  private static JSONArray initialComplexObjectJSONArray;
  private static boolean generateSimpleProfileDimensions;
  private static Map<String,CommunicationChannel> communicationChannels;
  private static Map<String,SupportedDataType> supportedDataTypes;
  private static Map<String,JourneyMetricDeclaration> journeyMetricDeclarations;
  private static Map<String,SubscriberProfileDatacubeMetric> subscriberProfileDatacubeMetrics;
  private static Map<String,CriterionField> profileCriterionFields;
  private static Map<String,CriterionField> baseProfileCriterionFields;
  private static Map<String,CriterionField> extendedProfileCriterionFields;
  private static Map<String,CriterionField> presentationCriterionFields;
  private static Map<String,OfferProperty> offerProperties;
  private static Map<String,ScoringEngine> scoringEngines;
  private static Map<String,OfferOptimizationAlgorithm> offerOptimizationAlgorithms;
  private static Map<String,ScoringType> scoringTypes;
  private static Map<String,DNBOMatrixVariable> dnboMatrixVariables;
  private static Map<String,DeliveryManagerDeclaration> deliveryManagers;
  private static Map<String,DeliveryManagerDeclaration> fulfillmentProviders;
  private static Map<String,DeliveryManagerAccount> deliveryManagerAccounts;
  private static Map<String,NodeType> nodeTypes;
  private static Map<String,ToolboxSection> journeyToolbox;
  private static Map<String,ToolboxSection> campaignToolbox;
  private static Map<String,ToolboxSection> workflowToolbox;
  private static Map<String,ToolboxSection> loyaltyWorkflowToolbox;
  private static Map<String,ThirdPartyMethodAccessLevel> thirdPartyMethodPermissionsMap;
  private static Integer authResponseCacheLifetimeInMinutes;
  private static Integer reportManagerMaxMessageLength;
  private static int stockRefreshPeriod;
  private static String periodicEvaluationCronEntry;
  private static String ucgEvaluationCronEntry;
  private static String uploadedFileSeparator;
  private static CustomerMetaData customerMetaData;
  private static String APIresponseDateFormat;
  private static Map<String,ElasticsearchConnectionSettings> elasticsearchConnectionSettings;
  private static int maxPollIntervalMs;
  private static Map<String,String> deliveryTypeCommunicationChannelIDMap;
  private static int purchaseTimeoutMs;
  private static boolean enableEvaluateTargetRandomness;
  private static int minExpiryDelayForVoucherDeliveryInHours;
  private static int importVoucherFileBulkSize;
  private static int voucherESCacheCleanerFrequencyInSec;
  private static int numberConcurrentVoucherAllocationToES;
  private static int propensityReaderRefreshPeriodMs;
  private static int propensityWriterRefreshPeriodMs;
  private static boolean enableContactPolicyProcessing;
  private static String extractManagerZookeeperDir;
  private static String extractManagerOutputPath;
  private static String extractManagerDateFormat;
  private static String extractManagerFileExtension;
  private static String extractManagerCsvSeparator;
  private static String extractManagerFieldSurrounder;
  private static int recurrentCampaignCreationDaysRange;
  private static Map<String,Topic> allTopics;
  private static boolean isPreprocessorNeeded;
  
  /*****************************************
  *
  * Common accessors
  *
  *****************************************/
  //
  // System
  //
  public static String getZookeeperRoot() { return System.getProperty("nglm.zookeeper.root"); }
  public static String getZookeeperConnect() { return System.getProperty("zookeeper.connect"); }
  public static String getBrokerServers() { return System.getProperty("broker.servers",""); }
  public static String getRedisSentinels() { return System.getProperty("redis.sentinels",""); }
  public static boolean getRegressionMode() { return System.getProperty("use.regression","0").equals("1"); }
  public static String getSubscriberProfileEndpoints() { return System.getProperty("subscriberprofile.endpoints",""); }
  public static JSONObject getLicenseManagement() { return licenseManagement; }
  public static String getEvolutionVersion() { return evolutionVersion!=null?evolutionVersion:"unknown"; }
  public static String getCustomerVersion() { return customerVersion!=null?customerVersion:"unknown"; }
  //
  // Elasticsearch
  //
  public static String getElasticSearchHost() { return elasticsearchHost; }
  public static int getElasticSearchPort() { return elasticsearchPort; }
  public static String getElasticSearchUserName() { return  elasticsearchUserName; }
  public static String getElasticSearchUserPassword() { return  elasticsearchUserPassword; }
  public static int getElasticsearchScrollSize() {return elasticsearchScrollSize; }
  public static int getElasticSearchScrollKeepAlive() {return elasticSearchScrollKeepAlive; }
  public static int getElasticsearchDefaultShards() { return elasticsearchDefaultShards; }
  public static int getElasticsearchDefaultReplicas() { return elasticsearchDefaultReplicas; }
  public static int getElasticsearchSubscriberprofileShards() { return elasticsearchSubscriberprofileShards; }
  public static int getElasticsearchSubscriberprofileReplicas() { return elasticsearchSubscriberprofileReplicas; }
  public static int getElasticsearchSnapshotShards() { return elasticsearchSnapshotShards; }
  public static int getElasticsearchSnapshotReplicas() { return elasticsearchSnapshotReplicas; }
  public static int getElasticsearchLiveVoucherShards() { return elasticsearchLiveVoucherShards; }
  public static int getElasticsearchLiveVoucherReplicas() { return elasticsearchLiveVoucherReplicas; }
  public static int getElasticsearchRetentionDaysODR() { return elasticsearchRetentionDaysODR; }
  public static int getElasticsearchRetentionDaysBDR() { return elasticsearchRetentionDaysBDR; }
  public static int getElasticsearchRetentionDaysMDR() { return elasticsearchRetentionDaysMDR; }
  public static int getElasticsearchRetentionDaysTokens() { return elasticsearchRetentionDaysTokens; }
  public static int getElasticsearchRetentionDaysSnapshots() { return elasticsearchRetentionDaysSnapshots; }
  public static int getElasticsearchRetentionDaysVDR() { return elasticsearchRetentionDaysVDR; }
  //
  // Kafka
  //
  public static int getTopicSubscriberPartitions() { return topicSubscriberPartitions; }
  public static int getTopicReplication() { return topicReplication; }
  public static String getTopicMinInSyncReplicas() { return topicMinInSyncReplicas; }
  public static String getTopicRetentionShortMs() { return topicRetentionShortMs; }
  public static String getTopicRetentionMs() { return topicRetentionMs; }
  public static String getTopicRetentionLongMs() { return topicRetentionLongMs; }
  public static int getKafkaRetentionDaysExpiredTokens() { return kafkaRetentionDaysExpiredTokens; }
  public static int getKafkaRetentionDaysExpiredVouchers() { return kafkaRetentionDaysExpiredVouchers; }
  public static int getKafkaRetentionDaysJourneys() { return kafkaRetentionDaysJourneys; }
  public static int getKafkaRetentionDaysCampaigns() { return kafkaRetentionDaysCampaigns; }
  public static int getKafkaRetentionDaysBulkCampaigns() { return kafkaRetentionDaysBulkCampaigns; }
  public static int getKafkaRetentionDaysLoyaltyPrograms() { return kafkaRetentionDaysLoyaltyPrograms; }
  public static int getKafkaRetentionDaysODR() { return kafkaRetentionDaysODR; }
  public static int getKafkaRetentionDaysBDR() { return kafkaRetentionDaysBDR; }
  public static int getKafkaRetentionDaysMDR() { return kafkaRetentionDaysMDR; }
  public static int getKafkaRetentionDaysTargets() { return kafkaRetentionDaysTargets; } 
  //
  // Topics
  //
  public static String getAssignSubscriberIDsTopic() { return assignSubscriberIDsTopic; }
  public static String getAssignExternalSubscriberIDsTopic() { return assignExternalSubscriberIDsTopic; }
  public static String getUpdateExternalSubscriberIDTopic() { return updateExternalSubscriberIDTopic; }
  public static String getRecordSubscriberIDTopic() { return recordSubscriberIDTopic; }
  public static String getRecordAlternateIDTopic() { return recordAlternateIDTopic; }
  public static String getAutoProvisionedSubscriberChangeLog() { return autoProvisionedSubscriberChangeLog; }
  public static String getAutoProvisionedSubscriberChangeLogTopic() { return autoProvisionedSubscriberChangeLogTopic; }
  public static String getRekeyedAutoProvisionedAssignSubscriberIDsStreamTopic() { return rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic; }
  public static String getCleanupSubscriberTopic() { return cleanupSubscriberTopic; }
  public static String getSubscriberTraceControlTopic() { return subscriberTraceControlTopic; }
  public static String getSubscriberTraceControlAssignSubscriberIDTopic() { return subscriberTraceControlAssignSubscriberIDTopic; }
  public static String getSubscriberTraceTopic() { return subscriberTraceTopic; }
  public static String getSimulatedTimeTopic() { return simulatedTimeTopic; }
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
  public static String getDeliverableSourceTopic() { return deliverableSourceTopic; }
  public static String getPresentationLogTopic() { return presentationLogTopic; }
  public static String getAcceptanceLogTopic() { return acceptanceLogTopic; }
  public static String getProfileChangeEventTopic() { return profileChangeEventTopic;}
  public static String getProfileSegmentChangeEventTopic() { return profileSegmentChangeEventTopic;}
  public static String getProfileLoyaltyProgramChangeEventTopic() { return profileLoyaltyProgramChangeEventTopic;}
  public static String getVoucherActionTopic() { return voucherActionTopic; }
  public static String getFileWithVariableEventTopic() { return fileWithVariableEventTopic; }
  public static String getTokenRedeemedTopic() { return tokenRedeemedTopic; }
  public static String getUploadedFileTopic() { return uploadedFileTopic; }
  public static String getTargetTopic() { return targetTopic; }
  public static String getCommunicationChannelTopic() { return communicationChannelTopic; }
  public static String getCommunicationChannelBlackoutTopic() { return communicationChannelBlackoutTopic; }
  public static String getCommunicationChannelTimeWindowTopic() { return communicationChannelTimeWindowTopic; }
  public static String getTokenChangeTopic() { return tokenChangeTopic; }
  public static String getLoyaltyProgramTopic() { return loyaltyProgramTopic; }
  public static String getExclusionInclusionTargetTopic() { return exclusionInclusionTargetTopic; }
  public static String getDNBOMatrixTopic() { return dnboMatrixTopic; }
  public static String getSegmentContactPolicyTopic() { return segmentContactPolicyTopic; }
  public static String getDynamicEventDeclarationsTopic() { return dynamicEventDeclarationsTopic; }
  public static String getDynamicCriterionFieldTopic() { return dynamicCriterionFieldsTopic; }
  public static String getCriterionFieldAvailableValuesTopic() { return criterionFieldAvailableValuesTopic; }
  public static String getSourceAddressTopic() { return sourceAddressTopic; }
  public static String getVoucherChangeRequestTopic() { return voucherChangeRequestTopic; }
  public static String getVoucherChangeResponseTopic() { return voucherChangeResponseTopic; }
  //
  //
  // ????????????????????????????????????????????????????????????????????????
  public static Map<String,AlternateID> getAlternateIDs() { return alternateIDs; }
  public static String getExternalSubscriberID() { return externalSubscriberID; }

  //
  // TODO EVPRO-99 A TRIER (sortir de Common)
  //
  public static String getSubscriberTraceControlAlternateID() { return subscriberTraceControlAlternateID; }
  public static boolean getSubscriberTraceControlAutoProvision() { return subscriberTraceControlAutoProvision; }
  public static Map<String,AutoProvisionEvent> getAutoProvisionEvents() { return autoProvisionEvents; }
  public static Set<String> getCleanupSubscriberElasticsearchIndexes() { return cleanupSubscriberElasticsearchIndexes; }
  public static int getHttpServerScalingFactor() { return httpServerScalingFactor; }
  public static int getEvolutionEngineStreamThreads() { return evolutionEngineStreamThreads; }
  public static int getEvolutionEngineInstanceNumbers() { return evolutionEngineInstanceNumbers; }
  public static String getSubscriberGroupLoaderAlternateID() { return subscriberGroupLoaderAlternateID; }
  public static String getGetCustomerAlternateID() { return getCustomerAlternateID; }  // EVPRO-99 check for tenant and static
  public static boolean getSubscriberGroupLoaderAutoProvision() { return subscriberGroupLoaderAutoProvision; }
  public static String getCriterionFieldRetrieverClassName() { return criterionFieldRetrieverClassName; }
  public static String getEvolutionEngineExtensionClassName() { return evolutionEngineExtensionClassName; }
  public static String getGUIManagerExtensionClassName() { return guiManagerExtensionClassName; }
  public static String getSubscriberProfileClassName() { return subscriberProfileClassName; }
  public static String getExtendedSubscriberProfileClassName() { return extendedSubscriberProfileClassName; }
  public static Map<String,EvolutionEngineEventDeclaration> getEvolutionEngineEvents() { return evolutionEngineEvents; }
  public static boolean getEnableProfileSegmentChange() { return enableProfileSegmentChange; }
  public static int getPropensityInitialisationPresentationThreshold() { return propensityInitialisationPresentationThreshold; }
  public static int getPropensityInitialisationDurationInDaysThreshold() { return propensityInitialisationDurationInDaysThreshold; }
  public static String getSubscriberProfileRegistrySubject() { return subscriberProfileRegistrySubject; }
  public static Map<String,Long> getJourneyTemplateCapacities() { return journeyTemplateCapacities; }
  public static Map<String,ExternalAPITopic> getExternalAPITopics() { return externalAPITopics; }
  public static Map<String,CallingChannelProperty> getCallingChannelProperties() { return callingChannelProperties; }
  public static Map<String,CatalogCharacteristicUnit> getCatalogCharacteristicUnits() { return catalogCharacteristicUnits; }
  public static Map<String,SupportedVoucherCodePattern> getSupportedVoucherCodePatternList() { return supportedVoucherCodePatternList; }
  public static Map<String,SupportedTokenCodesFormat> getSupportedTokenCodesFormats() { return supportedTokenCodesFormats; }
  public static Map<String,SupportedRelationship> getSupportedRelationships() { return supportedRelationships; }
  public static JSONArray getInitialCallingChannelsJSONArray() { return initialCallingChannelsJSONArray; }
  public static JSONArray getInitialSalesChannelsJSONArray() { return initialSalesChannelsJSONArray; }
  public static JSONArray getInitialSourceAddressesJSONArray() { return initialSourceAddressesJSONArray; }
  public static JSONArray getInitialSuppliersJSONArray() { return initialSuppliersJSONArray; }
  public static JSONArray getInitialPartnersJSONArray() { return initialPartnersJSONArray; }
  public static JSONArray getInitialProductsJSONArray() { return initialProductsJSONArray; }
  public static JSONArray getInitialReportsJSONArray() { return initialReportsJSONArray; }
  public static JSONArray getInitialCatalogCharacteristicsJSONArray() { return initialCatalogCharacteristicsJSONArray; }
  public static JSONArray getInitialContactPoliciesJSONArray() { return initialContactPoliciesJSONArray; }
  public static JSONArray getInitialJourneyTemplatesJSONArray() { return initialJourneyTemplatesJSONArray; }
  public static JSONArray getInitialJourneyObjectivesJSONArray() { return initialJourneyObjectivesJSONArray; }
  public static JSONArray getInitialOfferObjectivesJSONArray() { return initialOfferObjectivesJSONArray; }
  public static JSONArray getInitialProductTypesJSONArray() { return initialProductTypesJSONArray; }
  public static JSONArray getInitialTokenTypesJSONArray() { return initialTokenTypesJSONArray; }
  public static JSONArray getInitialVoucherCodeFormatsJSONArray() { return initialVoucherCodeFormatsJSONArray; }
  public static JSONArray getInitialSegmentationDimensionsJSONArray() { return initialSegmentationDimensionsJSONArray; }
  public static JSONArray getInitialComplexObjectJSONArray() { return initialComplexObjectJSONArray; }
  public static boolean getGenerateSimpleProfileDimensions() { return generateSimpleProfileDimensions; }
  public static Map<String,SupportedDataType> getSupportedDataTypes() { return supportedDataTypes; }
  public static Map<String,JourneyMetricDeclaration> getJourneyMetricDeclarations() { return journeyMetricDeclarations; } // EVPRO-99 check for tenant and static
  public static Map<String,SubscriberProfileDatacubeMetric> getSubscriberProfileDatacubeMetrics() { return subscriberProfileDatacubeMetrics; } // EVPRO-99 check for tenant and static 
  public static Map<String,CriterionField> getProfileCriterionFields() { return profileCriterionFields; } // EVPRO-99 check for tenant and static
  public static Map<String,CriterionField> getBaseProfileCriterionFields() { return baseProfileCriterionFields; }
  public static Map<String,CriterionField> getExtendedProfileCriterionFields() { return extendedProfileCriterionFields; }
  public static Map<String, CriterionField> getProfileChangeDetectionCriterionFields() { return profileChangeDetectionCriterionFields; }
  public static Map<String, CriterionField> getProfileChangeGeneratedCriterionFields() { return profileChangeGeneratedCriterionFields; }
  public static Map<String,CriterionField> getPresentationCriterionFields() { return presentationCriterionFields; }
  public static Map<String,OfferProperty> getOfferProperties() { return offerProperties; }
  public static Map<String,ScoringEngine> getScoringEngines() { return scoringEngines; } // EVPRO-99 check for tenant and static
  public static Map<String,OfferOptimizationAlgorithm> getOfferOptimizationAlgorithms() { return offerOptimizationAlgorithms; } // EVPRO-99 check for tenant and static
  public static Map<String,ScoringType> getScoringTypes() { return scoringTypes; }
  public static Map<String,DNBOMatrixVariable> getDNBOMatrixVariables() { return dnboMatrixVariables; }
  public static Map<String,DeliveryManagerDeclaration> getDeliveryManagers() { return deliveryManagers; }
  public static Map<String,DeliveryManagerDeclaration> getFulfillmentProviders() { return fulfillmentProviders; } // TODO EVPRO-99 fulfillmentProviders accounts per tenant ?
  public static Map<String,DeliveryManagerAccount> getDeliveryManagerAccounts() { return deliveryManagerAccounts; } // TODO EVPRO-99 deliveryManager accounts per tenant ?
  public static Map<String,NodeType> getNodeTypes() { return nodeTypes; } // EVPRO-99 should not be per tenant...
  public static Map<String,ToolboxSection> getJourneyToolbox() { return journeyToolbox; }
  public static Map<String,ToolboxSection> getCampaignToolbox() { return campaignToolbox; }
  public static Map<String,ToolboxSection> getWorkflowToolbox() { return workflowToolbox; }
  public static Map<String,ToolboxSection> getLoyaltyWorkflowToolbox() { return loyaltyWorkflowToolbox; }
  public static Map<String,ThirdPartyMethodAccessLevel> getThirdPartyMethodPermissionsMap() { return thirdPartyMethodPermissionsMap; } // TODO EVPRO-99 check for tenant and static
  public static Integer getAuthResponseCacheLifetimeInMinutes() { return authResponseCacheLifetimeInMinutes; }
  public static Integer getReportManagerMaxMessageLength() { return reportManagerMaxMessageLength; } // TODO EVPRO-99 check for tenant and static
  public static int getStockRefreshPeriod() { return stockRefreshPeriod; } // TODO EVPRO-99 check for tenant and static
  public static String getPeriodicEvaluationCronEntry() { return periodicEvaluationCronEntry; }
  public static String getUCGEvaluationCronEntry() { return ucgEvaluationCronEntry; } // TODO EVPRO-99 check for tenant and static
  public static String getReportManagerZookeeperDir() { return reportManagerZookeeperDir; }
  public static String getReportManagerOutputPath() { return reportManagerOutputPath; } // TODO EVPRO-99 Move in TENANT
  public static String getReportManagerDateFormat() { return reportManagerDateFormat; }
  public static String getReportManagerFileExtension() { return reportManagerFileExtension; } // TODO EVPRO-99 Move in TENANT
  public static String getReportManagerCsvSeparator() { return reportManagerCsvSeparator; } // EVPRO-99 check for tenant and static
  public static String getReportManagerFieldSurrounder() { return reportManagerFieldSurrounder; } // EVPRO-99 check for tenant and static
  public static String getUploadedFileSeparator() { return uploadedFileSeparator; } // EVPRO-99 check for tenant and static
  public static String getReportManagerStreamsTempDir() { return reportManagerStreamsTempDir; }
  public static String getReportManagerTopicsCreationProperties() { return reportManagerTopicsCreationProperties; }
  public static CustomerMetaData getCustomerMetaData() { return customerMetaData; }
  public static String getAPIresponseDateFormat() { return APIresponseDateFormat; } // EVPRO-99 check for tenant and static
  public static Map<String,PartnerType> getPartnerTypes() { return partnerTypes; }
  public static Map<String,ElasticsearchConnectionSettings> getElasticsearchConnectionSettings() { return elasticsearchConnectionSettings; }
  public static int getMaxPollIntervalMs() {return maxPollIntervalMs; }
  public static int getPurchaseTimeoutMs() {return purchaseTimeoutMs; }
  public static Map<String,CommunicationChannel> getCommunicationChannels(){ return communicationChannels; }; // TODO EVPRO-99 how communication channels are handled into multitenancy ??
  public static Map<String,String> getDeliveryTypeCommunicationChannelIDMap(){ return deliveryTypeCommunicationChannelIDMap; };  // TODO EVPRO-99 how communication channels are handled into multitenancy ??
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
  public static boolean getEnableContactPolicyProcessing(){ return  enableContactPolicyProcessing;}
  public static String getExtractManagerZookeeperDir() { return extractManagerZookeeperDir; }
  public static String getExtractManagerOutputPath() { return extractManagerOutputPath; } // TODO EVPRO-99 check tenant ?
  public static String getExtractManagerDateFormat() { return extractManagerDateFormat; }// TODO EVPRO-99 check tenant ?
  public static String getExtractManagerFileExtension() { return extractManagerFileExtension; } // TODO EVPRO-99 check tenant ?
  public static String getExtractManagerCsvSeparator() { return extractManagerCsvSeparator; }// EVPRO-99 check static for tenant
  public static String getExtractManagerFieldSurrounder() { return extractManagerFieldSurrounder; }
  public static int getRecurrentCampaignCreationDaysRange() { return recurrentCampaignCreationDaysRange; } // TODO EVPRO-99 check tenant aspect
  public static Set<Topic> getAllTopics() { return new HashSet<>(allTopics.values()); }
  public static boolean isPreprocessorNeeded() { return isPreprocessorNeeded; }
  
  /****************************************
  *
  * Load all variables need by static code (GUIManagedObject init)
  *
  ****************************************/
  private static void initializeCoreSettings(JSONObject jsonRoot) throws Exception {
    criterionFieldRetrieverClassName = JSONUtilities.decodeString(jsonRoot, "criterionFieldRetrieverClass", true);
    evolutionEngineExtensionClassName = JSONUtilities.decodeString(jsonRoot, "evolutionEngineExtensionClass", true);
    guiManagerExtensionClassName = JSONUtilities.decodeString(jsonRoot, "guiManagerExtensionClass", false);
    subscriberProfileClassName = JSONUtilities.decodeString(jsonRoot, "subscriberProfileClass", true);
    extendedSubscriberProfileClassName = JSONUtilities.decodeString(jsonRoot, "extendedSubscriberProfileClass", true);
    evolutionEngineExternalAPIClassName = JSONUtilities.decodeString(jsonRoot, "externalAPIClass", false);
  }
  
  /****************************************
  *
  * Load all common variables from JSONObject
  *
  ****************************************/
  private static void initializeCommonSettings(JSONObject jsonRoot) throws Exception {
    //
    // System
    //
    licenseManagement = JSONUtilities.decodeJSONObject(jsonRoot, "licenseManagement", true);
    evolutionVersion = JSONUtilities.decodeString(jsonRoot, "evolutionVersion", true);
    customerVersion = JSONUtilities.decodeString(jsonRoot, "customerVersion", true);
    
    //
    // Elasticsearch
    //
    // TODO EVPRO-99 @rl getenv
    elasticsearchHost = System.getenv("ELASTICSEARCH_HOST");
    elasticsearchPort = -1;
    try
      {
        elasticsearchPort = Integer.parseInt(System.getenv("ELASTICSEARCH_PORT"));
      }
    catch (NumberFormatException e)
      {
        log.info("deployment : can not get/parse env conf ELASTICSEARCH_PORT");
      }
    elasticsearchUserName = System.getenv("ELASTICSEARCH_USERNAME");
    elasticsearchUserPassword = System.getenv("ELASTICSEARCH_USERPASSWORD");
    
    elasticsearchScrollSize = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchScrollSize", true);
    elasticSearchScrollKeepAlive = JSONUtilities.decodeInteger(jsonRoot, "elasticSearchScrollKeepAlive", 0);
    // Shards & replicas
    elasticsearchDefaultShards = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchDefaultShards", true);
    elasticsearchDefaultReplicas = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchDefaultReplicas", true);
    elasticsearchSubscriberprofileShards = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchSubscriberprofileShards", elasticsearchDefaultShards);
    elasticsearchSubscriberprofileReplicas = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchSubscriberprofileReplicas",elasticsearchDefaultReplicas);
    elasticsearchSnapshotShards = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchSnapshotShards", elasticsearchDefaultShards);
    elasticsearchSnapshotReplicas = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchSnapshotReplicas",elasticsearchDefaultReplicas);
    elasticsearchLiveVoucherShards = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchLiveVoucherShards", elasticsearchDefaultShards);
    elasticsearchLiveVoucherReplicas = JSONUtilities.decodeInteger(jsonRoot, "elasticsearchLiveVoucherReplicas",elasticsearchDefaultReplicas);
    // Retention days
    elasticsearchRetentionDaysODR = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysODR", true);
    elasticsearchRetentionDaysBDR = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysBDR", true);
    elasticsearchRetentionDaysMDR = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysMDR", true);
    elasticsearchRetentionDaysTokens = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysTokens", true);
    elasticsearchRetentionDaysSnapshots = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysSnapshots", true);
    elasticsearchRetentionDaysVDR = JSONUtilities.decodeInteger(jsonRoot, "ESRetentionDaysVDR", true);

    //
    //
    // ????????????????????????????????????????????????????????????????????????
    //
    //  alternateIDs
    //
    alternateIDs = new LinkedHashMap<String,AlternateID>();
    JSONArray alternateIDValues = JSONUtilities.decodeJSONArray(jsonRoot, "alternateIDs", false);
    if (alternateIDValues != null)
      {
        for (int i=0; i<alternateIDValues.size(); i++)
          {
            JSONObject alternateIDJSON = (JSONObject) alternateIDValues.get(i);
            AlternateID alternateID = new AlternateID(alternateIDJSON);
            alternateIDs.put(alternateID.getID(), alternateID);
          }
      }
    
    //
    //  externalSubscriberID
    //
    externalSubscriberID = null;
    for (AlternateID alternateID : alternateIDs.values())
      {
        if (alternateID.getExternalSubscriberID())
          {
            if (alternateID.getSharedID()) throw new ServerRuntimeException("externalSubscriberID cannot be specified to be a shared id");
            if (externalSubscriberID != null) throw new ServerRuntimeException("multiple externalSubscriberID alternateIDs");
            externalSubscriberID = alternateID.getID();
          }
      }
    
    //
    //  journeyMetricDeclarations
    //
    journeyMetricDeclarations = new LinkedHashMap<String,JourneyMetricDeclaration>();
    JSONArray journeyMetricDeclarationValues = JSONUtilities.decodeJSONArray(jsonRoot, "journeyMetrics", new JSONArray());
    for (int i=0; i<journeyMetricDeclarationValues.size(); i++)
      {
        JSONObject journeyMetricDeclarationJSON = (JSONObject) journeyMetricDeclarationValues.get(i);
        JourneyMetricDeclaration journeyMetricDeclaration = new JourneyMetricDeclaration(journeyMetricDeclarationJSON);
        journeyMetricDeclarations.put(journeyMetricDeclaration.getID(), journeyMetricDeclaration);
      }
    
    
    //
    // Kafka
    //
    topicSubscriberPartitions = Integer.parseInt(JSONUtilities.decodeString(jsonRoot, "topicSubscriberPartitions", true));
    topicReplication = Integer.parseInt(JSONUtilities.decodeString(jsonRoot, "topicReplication", true));
    topicMinInSyncReplicas = JSONUtilities.decodeString(jsonRoot, "topicMinInSyncReplicas", true);
    topicRetentionShortMs = ""+(JSONUtilities.decodeInteger(jsonRoot, "topicRetentionShortHour", true) * 3600 * 1000L);
    topicRetentionMs = ""+(JSONUtilities.decodeInteger(jsonRoot, "topicRetentionDay", true) * 24 * 3600 * 1000L);
    topicRetentionLongMs = ""+(JSONUtilities.decodeInteger(jsonRoot, "topicRetentionLongDay", true) * 24 * 3600 * 1000L);
    kafkaRetentionDaysExpiredTokens = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysExpiredTokens",31);
    kafkaRetentionDaysExpiredVouchers = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysExpiredVouchers",31);
    kafkaRetentionDaysJourneys = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysJourneys",31);
    kafkaRetentionDaysCampaigns = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysCampaigns",31);
    // adjusting and warning if too low for journey metric feature to work
    for (JourneyMetricDeclaration journeyMetricDeclaration : getJourneyMetricDeclarations().values()){
      if(journeyMetricDeclaration.getPostPeriodDays() > kafkaRetentionDaysCampaigns + 2){
        kafkaRetentionDaysCampaigns = journeyMetricDeclaration.getPostPeriodDays() + 2;
        log.warn("Deployment: auto increasing kafkaRetentionDaysCampaigns to "+kafkaRetentionDaysCampaigns+" to comply with configured journey metric "+journeyMetricDeclaration.getID()+" postPeriodDays of "+journeyMetricDeclaration.getPostPeriodDays()+" (need at least 2 days more)");
      }
    }
    kafkaRetentionDaysBulkCampaigns = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysBulkCampaigns",7);
    kafkaRetentionDaysLoyaltyPrograms = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysLoyaltyPrograms",31);
    kafkaRetentionDaysODR = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysODR",91);
    kafkaRetentionDaysBDR = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysBDR",91);
    kafkaRetentionDaysMDR = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysMDR",91);
    kafkaRetentionDaysTargets = JSONUtilities.decodeInteger(jsonRoot, "kafkaRetentionDaysTargets",91);
    
    //
    // Topics
    //
    subscriberTraceControlTopic = JSONUtilities.decodeString(jsonRoot, "subscriberTraceControlTopic", "subscribertracecontrol");
    subscriberTraceControlAssignSubscriberIDTopic = JSONUtilities.decodeString(jsonRoot, "subscriberTraceControlAssignSubscriberIDTopic", "subscribertracecontrol-assignsubscriberid");
    subscriberTraceTopic = JSONUtilities.decodeString(jsonRoot, "subscriberTraceTopic", "subscribertrace");
    simulatedTimeTopic = JSONUtilities.decodeString(jsonRoot, "simulatedTimeTopic", "simulatedtime");
    assignSubscriberIDsTopic = JSONUtilities.decodeString(jsonRoot, "assignSubscriberIDsTopic", (alternateIDs.size() > 0));
    updateExternalSubscriberIDTopic = JSONUtilities.decodeString(jsonRoot, "updateExternalSubscriberIDTopic", (alternateIDs.size() > 0));
    assignExternalSubscriberIDsTopic = JSONUtilities.decodeString(jsonRoot, "assignExternalSubscriberIDsTopic", (alternateIDs.size() > 0));
    recordSubscriberIDTopic = JSONUtilities.decodeString(jsonRoot, "recordSubscriberIDTopic", (alternateIDs.size() > 0));
    recordAlternateIDTopic = JSONUtilities.decodeString(jsonRoot, "recordAlternateIDTopic", (alternateIDs.size() > 0));
    autoProvisionedSubscriberChangeLog = JSONUtilities.decodeString(jsonRoot, "autoProvisionedSubscriberChangeLog", (alternateIDs.size() > 0));
    autoProvisionedSubscriberChangeLogTopic = JSONUtilities.decodeString(jsonRoot, "autoProvisionedSubscriberChangeLogTopic", (alternateIDs.size() > 0));
    rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic = JSONUtilities.decodeString(jsonRoot, "rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic", (alternateIDs.size() > 0));
    cleanupSubscriberTopic = JSONUtilities.decodeString(jsonRoot, "cleanupSubscriberTopic", true);
    journeyTopic = JSONUtilities.decodeString(jsonRoot, "journeyTopic", true);
    journeyTemplateTopic = JSONUtilities.decodeString(jsonRoot, "journeyTemplateTopic", true);
    segmentationDimensionTopic = JSONUtilities.decodeString(jsonRoot, "segmentationDimensionTopic", true);
    pointTopic = JSONUtilities.decodeString(jsonRoot, "pointTopic", true);
    complexObjectTypeTopic = JSONUtilities.decodeString(jsonRoot, "complexObjectTypeTopic", true);
    offerTopic = JSONUtilities.decodeString(jsonRoot, "offerTopic", true);
    reportTopic = JSONUtilities.decodeString(jsonRoot, "reportTopic", true);
    paymentMeanTopic = JSONUtilities.decodeString(jsonRoot, "paymentMeanTopic", true);
    presentationStrategyTopic = JSONUtilities.decodeString(jsonRoot, "presentationStrategyTopic", true);
    scoringStrategyTopic = JSONUtilities.decodeString(jsonRoot, "scoringStrategyTopic", true);
    callingChannelTopic = JSONUtilities.decodeString(jsonRoot, "callingChannelTopic", true);
    salesChannelTopic = JSONUtilities.decodeString(jsonRoot, "salesChannelTopic", true);
    supplierTopic = JSONUtilities.decodeString(jsonRoot, "supplierTopic", true);
    resellerTopic = JSONUtilities.decodeString(jsonRoot, "resellerTopic", true);
    productTopic = JSONUtilities.decodeString(jsonRoot, "productTopic", true);
    catalogCharacteristicTopic = JSONUtilities.decodeString(jsonRoot, "catalogCharacteristicTopic", true);
    contactPolicyTopic = JSONUtilities.decodeString(jsonRoot, "contactPolicyTopic", true);
    journeyObjectiveTopic = JSONUtilities.decodeString(jsonRoot, "journeyObjectiveTopic", true);
    offerObjectiveTopic = JSONUtilities.decodeString(jsonRoot, "offerObjectiveTopic", true);
    productTypeTopic = JSONUtilities.decodeString(jsonRoot, "productTypeTopic", true);
    ucgRuleTopic = JSONUtilities.decodeString(jsonRoot, "ucgRuleTopic", true);
    deliverableTopic = JSONUtilities.decodeString(jsonRoot, "deliverableTopic", true);
    tokenTypeTopic = JSONUtilities.decodeString(jsonRoot, "tokenTypeTopic", true);
    voucherTypeTopic = JSONUtilities.decodeString(jsonRoot, "voucherTypeTopic", true);
    voucherTopic = JSONUtilities.decodeString(jsonRoot, "voucherTopic", true);
    subscriberMessageTemplateTopic = JSONUtilities.decodeString(jsonRoot, "subscriberMessageTemplateTopic", true);
    guiAuditTopic = JSONUtilities.decodeString(jsonRoot, "guiAuditTopic", true);
    subscriberGroupTopic = JSONUtilities.decodeString(jsonRoot, "subscriberGroupTopic", true);
    subscriberGroupAssignSubscriberIDTopic = JSONUtilities.decodeString(jsonRoot, "subscriberGroupAssignSubscriberIDTopic", true);
    subscriberGroupEpochTopic = JSONUtilities.decodeString(jsonRoot, "subscriberGroupEpochTopic", true);
    ucgStateTopic = JSONUtilities.decodeString(jsonRoot, "ucgStateTopic", true);
    renamedProfileCriterionFieldTopic = JSONUtilities.decodeString(jsonRoot, "renamedProfileCriterionFieldTopic", true);
    uploadedFileTopic = JSONUtilities.decodeString(jsonRoot, "uploadedFileTopic", true);
    targetTopic = JSONUtilities.decodeString(jsonRoot, "targetTopic", true);
    exclusionInclusionTargetTopic = JSONUtilities.decodeString(jsonRoot, "exclusionInclusionTargetTopic", true);
    dnboMatrixTopic = JSONUtilities.decodeString(jsonRoot, "dnboMatrixTopic", true);
    dynamicEventDeclarationsTopic = JSONUtilities.decodeString(jsonRoot, "dynamicEventDeclarationsTopic", true);
    dynamicCriterionFieldsTopic = JSONUtilities.decodeString(jsonRoot, "dynamicCriterionFieldTopic", true);
    communicationChannelBlackoutTopic = JSONUtilities.decodeString(jsonRoot, "communicationChannelBlackoutTopic", true);
    communicationChannelTimeWindowTopic = JSONUtilities.decodeString(jsonRoot, "communicationChannelTimeWindowTopic", true);
    communicationChannelTopic = JSONUtilities.decodeString(jsonRoot, "communicationChannelTopic", true);
    tokenChangeTopic = JSONUtilities.decodeString(jsonRoot, "tokenChangeTopic", true);
    loyaltyProgramTopic = JSONUtilities.decodeString(jsonRoot, "loyaltyProgramTopic", true);
    timedEvaluationTopic = JSONUtilities.decodeString(jsonRoot, "timedEvaluationTopic", true);
    evaluateTargetsTopic = JSONUtilities.decodeString(jsonRoot, "evaluateTargetsTopic", true);
    subscriberProfileForceUpdateTopic = JSONUtilities.decodeString(jsonRoot, "subscriberProfileForceUpdateTopic", true);
    executeActionOtherSubscriberTopic = JSONUtilities.decodeString(jsonRoot, "executeActionOtherSubscriberTopic", true);
    subscriberStateChangeLog = JSONUtilities.decodeString(jsonRoot, "subscriberStateChangeLog", true);
    subscriberStateChangeLogTopic = JSONUtilities.decodeString(jsonRoot, "subscriberStateChangeLogTopic", true);
    extendedSubscriberProfileChangeLog = JSONUtilities.decodeString(jsonRoot, "extendedSubscriberProfileChangeLog", true);
    extendedSubscriberProfileChangeLogTopic = JSONUtilities.decodeString(jsonRoot, "extendedSubscriberProfileChangeLogTopic", true);
    subscriberHistoryChangeLog = JSONUtilities.decodeString(jsonRoot, "subscriberHistoryChangeLog", true);
    subscriberHistoryChangeLogTopic = JSONUtilities.decodeString(jsonRoot, "subscriberHistoryChangeLogTopic", true);
    journeyStatisticTopic = JSONUtilities.decodeString(jsonRoot, "journeyStatisticTopic", true);
    journeyMetricTopic = JSONUtilities.decodeString(jsonRoot, "journeyMetricTopic", true);
    deliverableSourceTopic = JSONUtilities.decodeString(jsonRoot, "deliverableSourceTopic", true);
    presentationLogTopic = JSONUtilities.decodeString(jsonRoot, "presentationLogTopic", true);
    acceptanceLogTopic = JSONUtilities.decodeString(jsonRoot, "acceptanceLogTopic", true);
    segmentContactPolicyTopic = JSONUtilities.decodeString(jsonRoot, "segmentContactPolicyTopic", true);
    profileChangeEventTopic = JSONUtilities.decodeString(jsonRoot, "profileChangeEventTopic", true);
    profileSegmentChangeEventTopic = JSONUtilities.decodeString(jsonRoot, "profileSegmentChangeEventTopic", true);
    profileLoyaltyProgramChangeEventTopic = JSONUtilities.decodeString(jsonRoot, "profileLoyaltyProgramChangeEventTopic", true);
    voucherActionTopic = JSONUtilities.decodeString(jsonRoot, "voucherActionTopic", true);
    fileWithVariableEventTopic = JSONUtilities.decodeString(jsonRoot, "fileWithVariableEventTopic", true);
    tokenRedeemedTopic = JSONUtilities.decodeString(jsonRoot, "tokenRedeemedTopic", true);
    criterionFieldAvailableValuesTopic = JSONUtilities.decodeString(jsonRoot, "criterionFieldAvailableValuesTopic", true);
    sourceAddressTopic = JSONUtilities.decodeString(jsonRoot, "sourceAddressTopic", true);
    voucherChangeRequestTopic = JSONUtilities.decodeString(jsonRoot, "voucherChangeRequestTopic", true);
    voucherChangeResponseTopic = JSONUtilities.decodeString(jsonRoot, "voucherChangeResponseTopic", true);
    
    
    

    //
    // TODO EVPRO-99 A TRIER (sortir de Common & retirer static)
    //
    
    
    // Elasticsearch connection settings
    elasticsearchConnectionSettings = new LinkedHashMap<String,ElasticsearchConnectionSettings>();
    for (Object elasticsearchConnectionSettingsObject:JSONUtilities.decodeJSONArray(jsonRoot, "elasticsearchConnectionSettings", true).toArray()){
      ElasticsearchConnectionSettings elasticsearchConnectionSetting = new ElasticsearchConnectionSettings((JSONObject) elasticsearchConnectionSettingsObject);
      elasticsearchConnectionSettings.put(elasticsearchConnectionSetting.getId(), elasticsearchConnectionSetting);
    }
    //  cleanupSubscriberElasticsearchIndexes
    cleanupSubscriberElasticsearchIndexes = new HashSet<String>();
    JSONArray subscriberESIndexesJSON = JSONUtilities.decodeJSONArray(jsonRoot, "cleanupSubscriberElasticsearchIndexes", new JSONArray());
    for (int i=0; i<subscriberESIndexesJSON.size(); i++)
      {
        cleanupSubscriberElasticsearchIndexes.add((String) subscriberESIndexesJSON.get(i));
      }
    
    
    
    //
    // httpServerScalingFactor
    //  
    // TODO EVPRO-99 @rl : wtf ? getenv ? rly
    httpServerScalingFactor = 1;
    try 
      {
        httpServerScalingFactor = Integer.parseInt(System.getenv().get("HTTP_SERVER_SCALING_FACTOR"));
        log.info("Deployment: HTTP_SERVER_SCALING_FACTOR set to value - " + httpServerScalingFactor);
      }
    catch (NumberFormatException e)
      {
        log.info("Deployment: HTTP_SERVER_SCALING_FACTOR set to default value - " + httpServerScalingFactor);
      }
    
    //
    // evolutionEngineStreamThreads
    //  
    // TODO EVPRO-99 @rl : same here
    evolutionEngineStreamThreads = Integer.parseInt(System.getProperty("evolutionengine.streamthreads","1"));
    evolutionEngineInstanceNumbers = getSubscriberProfileEndpoints().split(",").length;
    if(evolutionEngineInstanceNumbers < 1){
      log.warn("Deployment: subscriberprofile.endpoints : '" + getSubscriberProfileEndpoints() + "' seems wrong");
      evolutionEngineInstanceNumbers = 1;
    }
    
    log.info("Hello 1");

    subscriberGroupLoaderAlternateID = JSONUtilities.decodeString(jsonRoot, "subscriberGroupLoaderAlternateID", false);
    getCustomerAlternateID = JSONUtilities.decodeString(jsonRoot, "getCustomerAlternateID", true);
    subscriberGroupLoaderAutoProvision = JSONUtilities.decodeBoolean(jsonRoot, "subscriberGroupLoaderAutoProvision", Boolean.FALSE);
    enableProfileSegmentChange = JSONUtilities.decodeBoolean(jsonRoot, "enableProfileSegmentChange", Boolean.FALSE);
    
    log.info("Hello 2");

    //
    // subscriberTrace
    //
    subscriberTraceControlAlternateID = JSONUtilities.decodeString(jsonRoot, "subscriberTraceControlAlternateID", false);
    subscriberTraceControlAutoProvision = JSONUtilities.decodeBoolean(jsonRoot, "subscriberTraceControlAutoProvision", Boolean.FALSE);
    
    
    //
    // Topics
    //
    
    maxPollIntervalMs = JSONUtilities.decodeInteger(jsonRoot, "maxPollIntervalMs", 300000);
    purchaseTimeoutMs = JSONUtilities.decodeInteger(jsonRoot, "purchaseTimeoutMs", 15000);
    
    
    //
    //  autoProvisionEvents
    //
    autoProvisionEvents = new LinkedHashMap<String,AutoProvisionEvent>();
    JSONArray autoProvisionEventValues = JSONUtilities.decodeJSONArray(jsonRoot, "autoProvisionEvents", false);
    if (autoProvisionEventValues != null)
      {
        for (int i=0; i<autoProvisionEventValues.size(); i++)
          {
            JSONObject autoProvisionEventJSON = (JSONObject) autoProvisionEventValues.get(i);
            AutoProvisionEvent autoProvisionEvent = new AutoProvisionEvent(autoProvisionEventJSON);
            autoProvisionEvents.put(autoProvisionEvent.getID(), autoProvisionEvent);
          }
      }
    //
    //  evolutionEngineEvents
    //
    
    //  deployment-level events
    evolutionEngineEvents = new LinkedHashMap<String,EvolutionEngineEventDeclaration>();
    JSONArray evolutionEngineEventValues = JSONUtilities.decodeJSONArray(jsonRoot, "evolutionEngineEvents", true);
    for (int i=0; i<evolutionEngineEventValues.size(); i++)
      {
        JSONObject evolutionEngineEventJSON = (JSONObject) evolutionEngineEventValues.get(i);
        EvolutionEngineEventDeclaration evolutionEngineEventDeclaration = new EvolutionEngineEventDeclaration(evolutionEngineEventJSON);
        evolutionEngineEvents.put(evolutionEngineEventDeclaration.getName(), evolutionEngineEventDeclaration);
      }

    // core-level events
    JSONArray evolutionEngineCoreEventValues = JSONUtilities.decodeJSONArray(jsonRoot, "evolutionEngineCoreEvents", true);
    for (int i=0; i<evolutionEngineCoreEventValues.size(); i++)
      {
        JSONObject evolutionEngineEventJSON = (JSONObject) evolutionEngineCoreEventValues.get(i);
        EvolutionEngineEventDeclaration evolutionEngineEventDeclaration = new EvolutionEngineEventDeclaration(evolutionEngineEventJSON);
        evolutionEngineEvents.put(evolutionEngineEventDeclaration.getName(), evolutionEngineEventDeclaration);
      }

    //
    //  communicationChannels
    //
    communicationChannels = new LinkedHashMap<>();
    JSONArray communicationChannelsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "communicationChannels", new JSONArray());
    for (int i=0; i<communicationChannelsJSONArray.size(); i++)
      {
        JSONObject communicationChannelJSON = (JSONObject) communicationChannelsJSONArray.get(i);
        // TODO EVPRO-99 : why tenantID=1 here ?
        CommunicationChannel communicationChannel = new CommunicationChannel(communicationChannelJSON, JSONUtilities.decodeInteger(communicationChannelJSON, "tenantID", 1));
        communicationChannels.put(communicationChannel.getID(), communicationChannel);
      }
    
    propensityInitialisationPresentationThreshold = JSONUtilities.decodeInteger(jsonRoot, "propensityInitialisationPresentationThreshold", true);
    propensityInitialisationDurationInDaysThreshold = JSONUtilities.decodeInteger(jsonRoot, "propensityInitialisationDurationInDaysThreshold", true);
    subscriberProfileRegistrySubject = JSONUtilities.decodeString(jsonRoot, "subscriberProfileRegistrySubject", true);

    //
    //  journeyTemplateCapacities
    //
    journeyTemplateCapacities = new LinkedHashMap<String,Long>();    
    JSONObject journeyTemplateCapacitiesJSON = JSONUtilities.decodeJSONObject(jsonRoot, "journeyTemplateCapacities", true);
    for (Object key : journeyTemplateCapacitiesJSON.keySet())
      {
        journeyTemplateCapacities.put((String) key, (Long) journeyTemplateCapacitiesJSON.get(key));
      }
    
    //
    //  externalAPITopics
    //
    externalAPITopics = new LinkedHashMap<String,ExternalAPITopic>();
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

    //
    //  partnerTypes
    //
    partnerTypes = new LinkedHashMap<String,PartnerType>();
    JSONArray partnerTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "partnerTypes", true);
    for (int i=0; i<partnerTypeValues.size(); i++)
      {
        JSONObject partnerTypesJSON = (JSONObject) partnerTypeValues.get(i);
        PartnerType partnerType = new PartnerType(partnerTypesJSON);
        partnerTypes.put(partnerType.getID(), partnerType);
      }

    //
    //  supportedTokenCodesFormats
    //
    supportedTokenCodesFormats = new LinkedHashMap<String,SupportedTokenCodesFormat>();
    JSONArray supportedTokenCodesFormatsValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedTokenCodesFormats", new JSONArray());
    for (int i=0; i<supportedTokenCodesFormatsValues.size(); i++)
      {
        JSONObject supportedTokenCodesFormatJSON = (JSONObject) supportedTokenCodesFormatsValues.get(i);
        SupportedTokenCodesFormat supportedTokenCodesFormat = new SupportedTokenCodesFormat(supportedTokenCodesFormatJSON);
        supportedTokenCodesFormats.put(supportedTokenCodesFormat.getID(), supportedTokenCodesFormat);
      }
    
    //
    //  voucherCodePatternList
    //
    supportedVoucherCodePatternList = new LinkedHashMap<String,SupportedVoucherCodePattern>();
    JSONArray voucherCodePatternListValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedVoucherCodePatternList", new JSONArray());
    for (int i=0; i<voucherCodePatternListValues.size(); i++)
      {
        JSONObject voucherCodePatternListJSON = (JSONObject) voucherCodePatternListValues.get(i);
        SupportedVoucherCodePattern voucherCodePattern = new SupportedVoucherCodePattern(voucherCodePatternListJSON);
        supportedVoucherCodePatternList.put(voucherCodePattern.getID(), voucherCodePattern);
      }
    
    //
    //  supportedRelationships
    //
    supportedRelationships = new LinkedHashMap<String,SupportedRelationship>();
    JSONArray supportedRelationshipValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedRelationships", new JSONArray());
    for (int i=0; i<supportedRelationshipValues.size(); i++)
      {
        JSONObject supportedRelationshipJSON = (JSONObject) supportedRelationshipValues.get(i);
        SupportedRelationship supportedRelationship = new SupportedRelationship(supportedRelationshipJSON);
        supportedRelationships.put(supportedRelationship.getID(), supportedRelationship);
      }

    //
    //  callingChannelProperties
    //
    callingChannelProperties = new LinkedHashMap<String,CallingChannelProperty>();
    JSONArray callingChannelPropertyValues = JSONUtilities.decodeJSONArray(jsonRoot, "callingChannelProperties", true);
    for (int i=0; i<callingChannelPropertyValues.size(); i++)
      {
        JSONObject callingChannelPropertyJSON = (JSONObject) callingChannelPropertyValues.get(i);
        CallingChannelProperty callingChannelProperty = new CallingChannelProperty(callingChannelPropertyJSON);
        callingChannelProperties.put(callingChannelProperty.getID(), callingChannelProperty);
      }

    //
    //  catalogCharacteristicUnits
    //
    catalogCharacteristicUnits = new LinkedHashMap<String,CatalogCharacteristicUnit>();
    JSONArray catalogCharacteristicUnitValues = JSONUtilities.decodeJSONArray(jsonRoot, "catalogCharacteristicUnits", new JSONArray());
    for (int i=0; i<catalogCharacteristicUnitValues.size(); i++)
      {
        JSONObject catalogCharacteristicUnitJSON = (JSONObject) catalogCharacteristicUnitValues.get(i);
        CatalogCharacteristicUnit catalogCharacteristicUnit = new CatalogCharacteristicUnit(catalogCharacteristicUnitJSON);
        catalogCharacteristicUnits.put(catalogCharacteristicUnit.getID(), catalogCharacteristicUnit);
      }
    
    initialCallingChannelsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialCallingChannels", new JSONArray());
    initialSalesChannelsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialSalesChannels", new JSONArray());
    initialSourceAddressesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialSourceAddresses", new JSONArray());
    initialSuppliersJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialSuppliers", new JSONArray());
    initialPartnersJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialPartners", new JSONArray());
    initialProductsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialProducts", new JSONArray());
    initialReportsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialReports", new JSONArray());
    initialCatalogCharacteristicsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialCatalogCharacteristics", new JSONArray());
    initialContactPoliciesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialContactPolicies", new JSONArray());
    initialJourneyTemplatesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialJourneyTemplates", new JSONArray());
    initialJourneyObjectivesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialJourneyObjectives", new JSONArray());
    initialOfferObjectivesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialOfferObjectives", new JSONArray());
    initialProductTypesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialProductTypes", new JSONArray());
    initialTokenTypesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialTokenTypes", new JSONArray());
    initialVoucherCodeFormatsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialVoucherCodeFormats", new JSONArray());
    initialSegmentationDimensionsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialSegmentationDimensions", new JSONArray());
    initialComplexObjectJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialComplexObjects", new JSONArray());

    generateSimpleProfileDimensions = JSONUtilities.decodeBoolean(jsonRoot, "generateSimpleProfileDimensions", Boolean.TRUE);
    
    //
    //  supportedDataTypes
    //
    supportedDataTypes = new LinkedHashMap<String,SupportedDataType>();
    JSONArray supportedDataTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedDataTypes", new JSONArray());
    for (int i=0; i<supportedDataTypeValues.size(); i++)
      {
        JSONObject supportedDataTypeJSON = (JSONObject) supportedDataTypeValues.get(i);
        SupportedDataType supportedDataType = new SupportedDataType(supportedDataTypeJSON);
        supportedDataTypes.put(supportedDataType.getID(), supportedDataType);
      }

    //
    //  subscriberProfileDatacubeMetrics
    //
    subscriberProfileDatacubeMetrics = new LinkedHashMap<String,SubscriberProfileDatacubeMetric>();
    JSONArray jsonArray = JSONUtilities.decodeJSONArray(jsonRoot, "subscriberProfileDatacubeMetrics", new JSONArray());
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject jsonObject = (JSONObject) jsonArray.get(i);
        SubscriberProfileDatacubeMetric subscriberProfileDatacubeMetric = new SubscriberProfileDatacubeMetric(jsonObject);
        subscriberProfileDatacubeMetrics.put(subscriberProfileDatacubeMetric.getID(), subscriberProfileDatacubeMetric);
      }

    //
    //  profileCriterionFields
    //
    profileCriterionFields = new LinkedHashMap<String,CriterionField>();
    baseProfileCriterionFields = new LinkedHashMap<String,CriterionField>();
    extendedProfileCriterionFields = new LinkedHashMap<String,CriterionField>();
    profileChangeDetectionCriterionFields = new HashMap<>();
    profileChangeGeneratedCriterionFields = new HashMap<>();
    
    //  profileCriterionFields (evolution)
    JSONArray evCriterionFieldValues = JSONUtilities.decodeJSONArray(jsonRoot, "evolutionProfileCriterionFields", new JSONArray());
    for (int i=0; i<evCriterionFieldValues.size(); i++)
      {
        JSONObject criterionFieldJSON = (JSONObject) evCriterionFieldValues.get(i);
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

    //  profileCriterionFields (deployment)
    JSONArray deplCriterionFieldValues = JSONUtilities.decodeJSONArray(jsonRoot, "profileCriterionFields", new JSONArray());
    for (int i=0; i<deplCriterionFieldValues.size(); i++)
      {
        JSONObject criterionFieldJSON = (JSONObject) deplCriterionFieldValues.get(i);
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

    //  profileChangeEvent
    EvolutionEngineEventDeclaration profileChangeEvent = new EvolutionEngineEventDeclaration("profile update", ProfileChangeEvent.class.getName(), getProfileChangeEventTopic(), EventRule.Standard, getProfileChangeGeneratedCriterionFields());
    evolutionEngineEvents.put(profileChangeEvent.getName(), profileChangeEvent);

    //
    //  extendedProfileCriterionFields
    //
    JSONArray extCriterionFieldValues = JSONUtilities.decodeJSONArray(jsonRoot, "extendedProfileCriterionFields", new JSONArray());
    for (int i=0; i<extCriterionFieldValues.size(); i++)
      {
        JSONObject criterionFieldJSON = (JSONObject) extCriterionFieldValues.get(i);
        CriterionField criterionField = new CriterionField(criterionFieldJSON);
        extendedProfileCriterionFields.put(criterionField.getID(), criterionField);
      }

    //
    //  presentationCriterionFields
    //
    presentationCriterionFields = new LinkedHashMap<String,CriterionField>();
    JSONArray presCriterionFieldValues = JSONUtilities.decodeJSONArray(jsonRoot, "presentationCriterionFields", new JSONArray());
    for (int i=0; i<presCriterionFieldValues.size(); i++)
      {
        JSONObject criterionFieldJSON = (JSONObject) presCriterionFieldValues.get(i);
        CriterionField criterionField = new CriterionField(criterionFieldJSON);
        presentationCriterionFields.put(criterionField.getID(), criterionField);
      }
    
    //
    //  offerProperties
    //
    offerProperties = new LinkedHashMap<String,OfferProperty>();
    JSONArray offerPropertyValues = JSONUtilities.decodeJSONArray(jsonRoot, "offerProperties", new JSONArray());
    for (int i=0; i<offerPropertyValues.size(); i++)
      {
        JSONObject offerPropertyJSON = (JSONObject) offerPropertyValues.get(i);
        OfferProperty offerProperty = new OfferProperty(offerPropertyJSON);
        offerProperties.put(offerProperty.getID(), offerProperty);
      }
    
    //
    //  scoringEngines
    //
    scoringEngines = new LinkedHashMap<String,ScoringEngine>();
    JSONArray scoringEngineValues = JSONUtilities.decodeJSONArray(jsonRoot, "scoringEngines", new JSONArray());
    for (int i=0; i<scoringEngineValues.size(); i++)
      {
        JSONObject scoringEngineJSON = (JSONObject) scoringEngineValues.get(i);
        ScoringEngine scoringEngine = new ScoringEngine(scoringEngineJSON);
        scoringEngines.put(scoringEngine.getID(), scoringEngine);
      }

    //
    //  offerOptimizationAlgorithms
    //
    offerOptimizationAlgorithms = new LinkedHashMap<String,OfferOptimizationAlgorithm>();
    JSONArray offerOptimizationAlgorithmValuesCommon = JSONUtilities.decodeJSONArray(jsonRoot, "offerOptimizationAlgorithmsCommon", new JSONArray());
    JSONArray offerOptimizationAlgorithmValues = JSONUtilities.decodeJSONArray(jsonRoot, "offerOptimizationAlgorithms", new JSONArray());
    offerOptimizationAlgorithmValues.addAll(offerOptimizationAlgorithmValuesCommon);
    for (int i=0; i<offerOptimizationAlgorithmValues.size(); i++)
      {
        JSONObject offerOptimizationAlgorithmJSON = (JSONObject) offerOptimizationAlgorithmValues.get(i);
        OfferOptimizationAlgorithm offerOptimizationAlgorithm = new OfferOptimizationAlgorithm(offerOptimizationAlgorithmJSON);
        offerOptimizationAlgorithms.put(offerOptimizationAlgorithm.getID(), offerOptimizationAlgorithm);
      }

    //
    //  scoringTypes
    //
    scoringTypes = new LinkedHashMap<String,ScoringType>();
    JSONArray scoringTypesValues = JSONUtilities.decodeJSONArray(jsonRoot, "scoringTypes", new JSONArray());
    for (int i=0; i<scoringTypesValues.size(); i++)
      {
        JSONObject scoringTypeJSON = (JSONObject) scoringTypesValues.get(i);
        ScoringType scoringType = new ScoringType(scoringTypeJSON);
        scoringTypes.put(scoringType.getID(), scoringType);
      }

    //
    //  dnboMatrixVariable
    //
    dnboMatrixVariables = new LinkedHashMap<String,DNBOMatrixVariable>();
    JSONArray dnboMatrixVariableValues = JSONUtilities.decodeJSONArray(jsonRoot, "dnboMatrixVariables", new JSONArray());
    for (int i=0; i<dnboMatrixVariableValues.size(); i++)
      {
        JSONObject dnboMatrixJSON = (JSONObject) dnboMatrixVariableValues.get(i);
        DNBOMatrixVariable dnboMatrixvariable = new DNBOMatrixVariable(dnboMatrixJSON);
        dnboMatrixVariables.put(dnboMatrixvariable.getID(), dnboMatrixvariable);
      }


    //
    //  deliveryManagers/fulfillmentProviders
    //
    deliveryManagers = new LinkedHashMap<String,DeliveryManagerDeclaration>();
    fulfillmentProviders = new LinkedHashMap<String,DeliveryManagerDeclaration>();
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
    
    //
    //  deliveryManagerAccounts
    //
    deliveryManagerAccounts = new HashMap<String,DeliveryManagerAccount>();
    JSONArray deliveryManagerAccountValues = JSONUtilities.decodeJSONArray(jsonRoot, "deliveryManagerAccounts", new JSONArray());
    for (int i=0; i<deliveryManagerAccountValues.size(); i++)
      {
        JSONObject deliveryManagerAccountJSON = (JSONObject) deliveryManagerAccountValues.get(i);
        DeliveryManagerAccount deliveryManagerAccount = new DeliveryManagerAccount(deliveryManagerAccountJSON);
        if(deliveryManagerAccount != null ){
          deliveryManagerAccounts.put(deliveryManagerAccount.getProviderID(), deliveryManagerAccount);
        }
      }

    //
    //  nodeTypes
    //
    nodeTypes = new LinkedHashMap<String,NodeType>();
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
    journeyToolbox = new LinkedHashMap<String,ToolboxSection>();
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

    //
    //  campaignToolboxSections
    //
    campaignToolbox = new LinkedHashMap<String,ToolboxSection>();
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
    
    //
    //  workflowToolboxSections
    //
    workflowToolbox = new LinkedHashMap<String,ToolboxSection>();
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
    
    //
    //  loyaltyWorkflowToolboxSections
    //
    loyaltyWorkflowToolbox = new LinkedHashMap<String,ToolboxSection>();
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
    
    //
    //  thirdPartyMethodPermissions
    //
    thirdPartyMethodPermissionsMap = new LinkedHashMap<String,ThirdPartyMethodAccessLevel>();
    JSONArray thirdPartyMethodPermissions = JSONUtilities.decodeJSONArray(jsonRoot, "thirdPartyMethodPermissions", new JSONArray());
    for (int i=0; i<thirdPartyMethodPermissions.size(); i++)
      {
        JSONObject thirdPartyMethodPermissionsJSON = (JSONObject) thirdPartyMethodPermissions.get(i);
        String methodName = JSONUtilities.decodeString(thirdPartyMethodPermissionsJSON, "methodName", Boolean.TRUE);
        ThirdPartyMethodAccessLevel thirdPartyMethodAccessLevel = new ThirdPartyMethodAccessLevel(thirdPartyMethodPermissionsJSON);
        thirdPartyMethodPermissionsMap.put(methodName, thirdPartyMethodAccessLevel);
      }

    authResponseCacheLifetimeInMinutes = JSONUtilities.decodeInteger(jsonRoot, "authResponseCacheLifetimeInMinutes", false);
    reportManagerMaxMessageLength = JSONUtilities.decodeInteger(jsonRoot, "reportManagerMaxMessageLength", 200);
    stockRefreshPeriod = JSONUtilities.decodeInteger(jsonRoot, "stockRefreshPeriod", 30);
    periodicEvaluationCronEntry = JSONUtilities.decodeString(jsonRoot, "periodicEvaluationCronEntry", false);
    ucgEvaluationCronEntry = JSONUtilities.decodeString(jsonRoot, "ucgEvaluationCronEntry", false);

    //
    //  Reports
    //
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
      }
    if (reportManagerFieldSurrounder.length() > 1)
      {
        log.warn("reportManagerFieldSurrounder is not a single character, this would lead to errors in the reports, truncating, please fix this : " + reportManagerFieldSurrounder);
        reportManagerFieldSurrounder = reportManagerFieldSurrounder.substring(0, 1);
      }

    customerMetaData = new CustomerMetaData(JSONUtilities.decodeJSONObject(jsonRoot, "customerMetaData", true));

    //
    //  uploadedFileSeparator
    //
    uploadedFileSeparator = JSONUtilities.decodeString(jsonRoot, "uploadedFileSeparator", false);
    if(uploadedFileSeparator == null) {
      uploadedFileSeparator = ";";
    }

    //
    //  APIresponseDateFormat
    //
    APIresponseDateFormat = JSONUtilities.decodeString(jsonRoot, "APIresponseDateFormat", false);
    if (null == APIresponseDateFormat) APIresponseDateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZ" ;

    //
    //  deliveryTypeCommunicationChannelMap
    //
    deliveryTypeCommunicationChannelIDMap = new LinkedHashMap<>();
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
    
    hourlyReportCronEntryString = JSONUtilities.decodeString(jsonRoot, "hourlyReportCronEntryString", true);
    dailyReportCronEntryString = JSONUtilities.decodeString(jsonRoot, "dailyReportCronEntryString", true);
    weeklyReportCronEntryString = JSONUtilities.decodeString(jsonRoot, "weeklyReportCronEntryString", true);
    monthlyReportCronEntryString = JSONUtilities.decodeString(jsonRoot, "monthlyReportCronEntryString", true);
    enableEvaluateTargetRandomness = JSONUtilities.decodeBoolean(jsonRoot, "enableEvaluateTargetRandomness", Boolean.FALSE);
    
    //
    // conf for elasticsearch & voucher
    //
    // we won't deliver a voucher that expiry in less than X hours from now :
    minExpiryDelayForVoucherDeliveryInHours = JSONUtilities.decodeInteger(jsonRoot, "minExpiryDelayForVoucherDeliveryInHours",4);
    // the bulk size when importing voucher file into ES
    importVoucherFileBulkSize = JSONUtilities.decodeInteger(jsonRoot, "importVoucherFileBulkSize",5000);
    // the cache cleaner frequency in seconds for caching voucher with 0 stock from ES, and shrinking back "auto adjust concurrency number"
    voucherESCacheCleanerFrequencyInSec = JSONUtilities.decodeInteger(jsonRoot, "voucherESCacheCleanerFrequencyInSec",300);
    // an approximation of number of total concurrent process tyring to allocate Voucher in // to ES, but should not need to configure, algo should auto-adjust this
    numberConcurrentVoucherAllocationToES = JSONUtilities.decodeInteger(jsonRoot, "numberConcurrentVoucherAllocationToES",10);

    //
    // conf for propensity service
    //
    // period in ms global propensity state will be read from zookeeper :
    propensityReaderRefreshPeriodMs = JSONUtilities.decodeInteger(jsonRoot, "propensityReaderRefreshPeriodMs",10000);
    // period in ms local propensity state will be write to zookeeper :
    propensityWriterRefreshPeriodMs = JSONUtilities.decodeInteger(jsonRoot, "propensityWriterRefreshPeriodMs",10000);

    enableContactPolicyProcessing = JSONUtilities.decodeBoolean(jsonRoot, "enableContactPolicyProcessing", Boolean.TRUE);

    //
    // configuration for extracts
    //
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
    
    recurrentCampaignCreationDaysRange = JSONUtilities.decodeInteger(jsonRoot, "recurrentCampaignCreationDaysRange", 3);
    
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
    isPreprocessorNeeded = false;
    for(EvolutionEngineEventDeclaration declaration:getEvolutionEngineEvents().values())
      {
        if(declaration.getPreprocessTopic()!=null){
          isPreprocessorNeeded = true;
          allTopics.put(declaration.getPreprocessTopic().getName(),declaration.getPreprocessTopic());
        }
      }
    
  }

  
  /*****************************************
  *
  * Utils
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

  public static String getEvolutionEngineExternalAPITopicID(String topic)
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
  
  public static String getSupportedLanguageID(String language, Map<String,SupportedLanguage> supportedLanguages)
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
  * Retrieve whole json from Zookeeper once
  *
  *****************************************/
  private static JSONObject getBrutJsonRoot()
  {
  
    /*****************************************
    *
    *  zookeeper -- retrieve configuration
    *
    *****************************************/

    //
    //  create a client
    // 

    ZooKeeper zookeeper = null;
    while (zookeeper == null)
      {
        try
          {
            zookeeper = new ZooKeeper(System.getProperty("zookeeper.connect"), 3000, new Watcher() { @Override public void process(WatchedEvent event) {} }, true);
          }
        catch (IOException e)
          {
            // ignore
          }
      }
    
    //
    //  ensure connected
    //

    while (zookeeper.getState().isAlive() && ! zookeeper.getState().isConnected())
      {
        try { Thread.currentThread().sleep(200); } catch (InterruptedException ie) { }
      }

    //
    //  verify connected
    //

    if (! zookeeper.getState().isConnected())
      {
        throw new RuntimeException("deployment");
      }

    DeploymentConfiguration deploymentConfiguration = null;
    TreeMap<String, DeploymentConfiguration> additionalDeploymentConfigurations = new TreeMap<String, DeploymentConfiguration>();
    TreeMap<String, DeploymentConfiguration> productDeploymentConfigurations = new TreeMap<String, DeploymentConfiguration>();
    
    String localDeploymentFiles = System.getProperty("deployment.repository");
    if(localDeploymentFiles != null) {
      // for development environment, don't get deployment*.json from Zookeeper, but from local disk
      File repository = new File(localDeploymentFiles);
      //      deployment.json
      
      //      deployment-templates.json
      //      deployment-toolbox.json
            
      //      deployment-product-evolution.json
      //      deployment-product-toolbox.json

      if(repository.exists()) {
        for(File f : repository.listFiles()) {
          if(f.getName().equals("deployment.json")) {
            // deployment.json
            try
              {
                String content = new String ( Files.readAllBytes( Paths.get(f.getAbsolutePath()) ) );
                DeploymentConfigurationPart part = DeploymentConfigurationPart.process("deployment", content.getBytes(), true);
                if (deploymentConfiguration == null)
                  {
                    deploymentConfiguration = new DeploymentConfiguration(part);
                  }
                else
                  {
                    deploymentConfiguration.setPart(part);
                  }
                
              } 
            catch (IOException e) 
              {
                e.printStackTrace();
              }
          }
          else if(f.getName().startsWith("deployment-") && !f.getName().contains("-product") && f.getName().endsWith(".json")) {
            // other that are NOT product-* so by example deployment-templates.json
            try 
              {
                String content = new String ( Files.readAllBytes( Paths.get(f.getAbsolutePath()) ) );
                DeploymentConfigurationPart part = DeploymentConfigurationPart.process(f.getName().substring("deployment-".length(),f.getName().indexOf(".json")), content.getBytes(), false);
                DeploymentConfiguration additionalDeploymentConfiguration = additionalDeploymentConfigurations.get(part.getBaseName());
                if (additionalDeploymentConfiguration == null)
                  {
                    additionalDeploymentConfiguration = new DeploymentConfiguration(part);
                    additionalDeploymentConfigurations.put(part.getBaseName(), additionalDeploymentConfiguration);
                  }
                else
                  {
                    additionalDeploymentConfiguration.setPart(part);
                  }
              }
            catch (IOException e) 
              {
                e.printStackTrace();
              }            
          }
          else if(f.getName().startsWith("deployment-product") && f.getName().endsWith(".json")) {
            //
            try 
              {
                String content = new String ( Files.readAllBytes( Paths.get(f.getAbsolutePath()) ) );
                String baseName = f.getName().substring(0,f.getName().indexOf(".json"));
                baseName = baseName.substring("deployment-product-".length(), baseName.length());
                DeploymentConfigurationPart part = DeploymentConfigurationPart.process(baseName, content.getBytes(), false);
                DeploymentConfiguration productDeploymentConfiguration = productDeploymentConfigurations.get(part.getBaseName());
                if (productDeploymentConfiguration == null)
                {
                  productDeploymentConfiguration = new DeploymentConfiguration(part);
                  productDeploymentConfigurations.put(part.getBaseName(), productDeploymentConfiguration);
                }
                else
                {
                  productDeploymentConfiguration.setPart(part);
                }
              }
            catch (IOException e) 
              {
                e.printStackTrace();
              }  
          }
        }
      }
      else {
        log.warn("Deployment repository gotten from System.getProperties deployment.repository " + localDeploymentFiles + " does not exist");
        throw new RuntimeException("Deployment repository gotten from System.getProperties deployment.repository " + localDeploymentFiles + " does not exist");
      }
      
    }
    else {
      
      //
      //  read configuration from zookeeper (this load file of deployment.json, which can be split in sub parts)
      //
      try
        {
          for (String node : zookeeper.getChildren(getZookeeperRoot(), null, null))
            {
              if(log.isDebugEnabled()) log.debug("checking for base conf "+node);
              if (! node.startsWith("deployment")) continue;
              byte[] bytes = zookeeper.getData(getZookeeperRoot() + "/" + node, null, null);
              if (bytes == null || bytes.length <= 1) continue;
              DeploymentConfigurationPart part = DeploymentConfigurationPart.process(node, bytes, true);
              if (part == null) continue;
              if (deploymentConfiguration == null)
                {
                  deploymentConfiguration = new DeploymentConfiguration(part);
                }
              else
                {
                  deploymentConfiguration.setPart(part);
                }
              if(log.isDebugEnabled()) log.debug("adding base conf read "+node);
            }
        }
      catch (KeeperException|InterruptedException e)
        {
          throw new RuntimeException("deployment", e);
        }
      
      //
      //  read additional configuration from zookeeper (this load file of deployment-xxx.json, which can be splited in sub parts)
      //
  
      try
        {
          for (String node : zookeeper.getChildren(getZookeeperRoot() + "/deployment", null, null))
            {
              if(log.isDebugEnabled()) log.debug("checking for additional conf "+node);
              if (node.startsWith("product-")) continue;//skip the product ones
              byte[] bytes = zookeeper.getData(getZookeeperRoot() + "/deployment/" + node, null, null);
              if (bytes == null || bytes.length <= 1) continue;
              DeploymentConfigurationPart part = DeploymentConfigurationPart.process(node, bytes, false);
              if (part == null) continue;
              DeploymentConfiguration additionalDeploymentConfiguration = additionalDeploymentConfigurations.get(part.getBaseName());
              if (additionalDeploymentConfiguration == null)
                {
                  additionalDeploymentConfiguration = new DeploymentConfiguration(part);
                  additionalDeploymentConfigurations.put(part.getBaseName(), additionalDeploymentConfiguration);
                }
              else
                {
                  additionalDeploymentConfiguration.setPart(part);
                }
              if(log.isDebugEnabled()) log.debug("adding additional conf read "+node);
            }
        }
      catch (KeeperException|InterruptedException e)
        {
          throw new RuntimeException("deployment", e);
        }
  
      //
      //  (sorry for the 3rd copy/past...) this load file of deployment-product-xxx.json, which can be splited in sub parts
      //
  
      try
      {
        for (String node : zookeeper.getChildren(getZookeeperRoot() + "/deployment", null, null))
        {
          if(log.isDebugEnabled()) log.debug("checking for product conf "+node);
          if (!node.startsWith("product-")) continue;//takes only the product ones
          byte[] bytes = zookeeper.getData(getZookeeperRoot() + "/deployment/" + node, null, null);
          if (bytes == null || bytes.length <= 1) continue;
          DeploymentConfigurationPart part = DeploymentConfigurationPart.process(node.replace("product-",""), bytes, false);
          if (part == null) continue;
          DeploymentConfiguration productDeploymentConfiguration = productDeploymentConfigurations.get(part.getBaseName());
          if (productDeploymentConfiguration == null)
          {
            productDeploymentConfiguration = new DeploymentConfiguration(part);
            productDeploymentConfigurations.put(part.getBaseName(), productDeploymentConfiguration);
          }
          else
          {
            productDeploymentConfiguration.setPart(part);
          }
          if(log.isDebugEnabled()) log.debug("adding product conf read "+node);
        }
      }
      catch (KeeperException|InterruptedException e)
      {
        throw new RuntimeException("deployment", e);
      }
    }

    //
    //  close
    //

    try
      {
        zookeeper.close();
      }
    catch (InterruptedException e)
      {
        // ignore
      }

    /*****************************************
    *
    *  configuration -- json
    *
    *****************************************/
    try
      {

        //
        //  process additional deployment nodes first of "custo" conf
        //

        JSONObject custoJson=new JSONObject();
        for (DeploymentConfiguration additionalDeploymentConfiguration : additionalDeploymentConfigurations.values())
          {
            JSONObject additionalJsonConfiguration = (JSONObject) (new JSONParser()).parse(additionalDeploymentConfiguration.getContents());
            if(log.isDebugEnabled()) log.debug("adding additional conf values of "+additionalDeploymentConfiguration.getBaseName());
            custoJson.putAll(additionalJsonConfiguration);
          }

        //
        //  base deployment node
        //

        JSONObject baseJsonConfiguration = (JSONObject) (new JSONParser()).parse(deploymentConfiguration.getContents());
        if(log.isDebugEnabled()) log.debug("adding base conf values of "+deploymentConfiguration.getBaseName());
        custoJson.putAll(baseJsonConfiguration);

        //
        // now process the "product" ones
        //

        JSONObject productJson=new JSONObject();
        for (DeploymentConfiguration productDeploymentConfiguration : productDeploymentConfigurations.values())
        {
          JSONObject productJsonConfiguration = (JSONObject) (new JSONParser()).parse(productDeploymentConfiguration.getContents());
          if(log.isDebugEnabled()) log.debug("adding product conf values of "+productDeploymentConfiguration.getBaseName());
          productJson.putAll(productJsonConfiguration);
        }

        //
        // merge both
        //
        JSONObject brutJSONRoot = JSONUtilities.jsonMergerOverrideOrAdd(productJson,custoJson,(product,custo) -> product.get("id")!=null && custo.get("id")!=null && product.get("id").equals(custo.get("id")));//json object in array match thanks to "id" field only
        // the final running conf could be so hard to understand from all deployment files, we have to provide it to support team, hence the info log, even if big :
        log.info("LOADED BRUT CONF : "+brutJSONRoot.toJSONString());
        return brutJSONRoot;

      }
    catch (org.json.simple.parser.ParseException e)
      {
        throw new RuntimeException("deployment", e);
      }
  }
  
  /*****************************************
  *
  * store JSONObject for every tenant
  *
  *****************************************/
  private static void buildJsonPerTenant(JSONObject brutJsonRoot)
  {
    //
    // First, check if there are some first level configuration with name starting with "tenantConfiguration"
    //
    
    ArrayList<JSONObject> tenantSpecificConfigurations = new ArrayList<>();
    for(Object key : brutJsonRoot.keySet())
      {
        if(key instanceof String && ((String)key).startsWith("tenantConfiguration"))
          {
            JSONObject tenantConfiguration = (JSONObject) brutJsonRoot.get(key);
            tenantSpecificConfigurations.add(tenantConfiguration);            
          }
      }
    
    // also add fake tenant 0 (for static configurations of Deployment)
    JSONObject tenant0Configuration = new JSONObject();
    tenant0Configuration.put("tenantID", 0);
    tenantSpecificConfigurations.add(tenant0Configuration);
    
    //
    // now analyse the configurations of tenants
    //
    for(JSONObject tenantSpecificConfiguration : tenantSpecificConfigurations)
      {
        //
        // first get global jsonRoot that contains also the tenant specific configuration
        //
        
        JSONObject brutJSONObject = getBrutJsonRoot();
        
        //
        // remove tenantSpecific configurations
        //
        
        ArrayList<String> keysToRemove = new ArrayList<>();
        for(Object key : brutJSONObject.keySet())
          {
            if(key instanceof String && ((String)key).startsWith("tenantConfiguration"))
              {
                keysToRemove.add((String)key);
              }
          }
        for(String keyToRemove : keysToRemove)
          {
            brutJSONObject.remove(keyToRemove);        
          }
        
        //
        // now merge the tenant specific configuration with the brut configuration, so that we have the effective JSONRoot configuration for the current tenant
        //
        
        JSONObject tenantJSON = JSONUtilities.jsonMergerOverrideOrAdd(brutJSONObject,tenantSpecificConfiguration,(brut,tenant) -> brut.get("id")!=null && tenant.get("id")!=null && brut.get("id").equals(tenant.get("id")));//json object in array match thanks to "id" field only
        
        //
        // get the tenantID
        //
        
        int tenantID = JSONUtilities.decodeInteger(tenantJSON, "tenantID", true);
        
        //
        // let reference the tenantJSONObject configuration available for all subclasses of Deployment
        //
        
        jsonConfigPerTenant.put(tenantID, tenantJSON);
      }
  }
  
  /****************************************
  *
  *  DeploymentConfiguration
  *
  ****************************************/
  private static class DeploymentConfiguration
  {
    /****************************************
    *
    *  attributes
    *
    ****************************************/
    
    private String baseName;
    private TreeMap<Integer, DeploymentConfigurationPart> parts = new TreeMap<>();

    /****************************************
    *
    *  accessors
    *
    ****************************************/
    
    public String getBaseName() { return baseName; }

    //
    //  getContents
    //

    public String getContents()
    {
      StringBuilder result = new StringBuilder();
      int expectedPart = 1;
      for (Integer partNumber : parts.keySet())
        {
          if (partNumber.intValue() != expectedPart) throw new RuntimeException("deployment component " + baseName + " missing part " + expectedPart);
          result.append(parts.get(partNumber).getContents());
          expectedPart += 1;
        }
      return result.toString();
    }
    
    /****************************************
    *
    *  setters
    *
    ****************************************/
    
    public void setPart(DeploymentConfigurationPart part)
    {
      if (parts.containsKey(part.getPartNumber())) throw new RuntimeException("part " + part.getPartNumber() + " already exists for deployment component " + baseName);
      if (! Objects.equals(baseName, part.getBaseName())) throw new RuntimeException();
      parts.put(part.getPartNumber(), part);
    }

    /****************************************
    *
    *  constructor
    *
    ****************************************/

    public DeploymentConfiguration(DeploymentConfigurationPart part)
    {
      this.baseName = part.getBaseName();
      parts.put(part.getPartNumber(), part);
    }
  }

  /****************************************
  *
  *  DeploymentConfigurationPart
  *
  ****************************************/
  private static class DeploymentConfigurationPart
  {
    //
    //  attributes
    //
    
    private String baseName;
    private int partNumber;
    private String contents;

    //
    //  accessors
    //

    String getBaseName() { return baseName; }
    int getPartNumber() { return partNumber; }
    String getContents() { return contents; }
    
    //
    //  constructor
    //
    
    private DeploymentConfigurationPart(String baseName, int partNumber, String contents)
    {
      this.baseName = baseName;
      this.partNumber = partNumber;
      this.contents = contents;
    }

    //
    //  static processor
    //

    static DeploymentConfigurationPart process(String fullName, byte[] bytes, boolean deploymentPrefix)
    {
      //
      //  parse name
      //
      
      Pattern pattern = deploymentPrefix ? Pattern.compile("^deployment(-([a-zA-Z0-9]+))?(_part_([0-9]+))?$") : Pattern.compile("^([a-zA-Z0-9]+)?(_part_([0-9]+))?$");
      Matcher matcher = pattern.matcher(fullName);
      
      //
      //  return null if no contents or not matching
      //

      if (bytes == null || bytes.length == 0 || ! matcher.matches())
        {
          return null;
        }
      
      //
      //  parse out name and part number
      //
      
      int baseNameGroup = deploymentPrefix ? 2 : 1;
      int partNumberGroup = deploymentPrefix ? 4 : 3;
      String baseName = (matcher.group(baseNameGroup) != null) ? matcher.group(baseNameGroup) : "deployment";
      int partNumber = (matcher.group(partNumberGroup) != null) ? Integer.parseInt(matcher.group(partNumberGroup)) : 1;
      String contents = new String(bytes, StandardCharsets.UTF_8);
      return new DeploymentConfigurationPart(baseName, partNumber, contents);
    }
  }
}
