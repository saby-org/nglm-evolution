package com.evolving.nglm.core;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import com.evolving.nglm.evolution.CallingChannelProperty;
import com.evolving.nglm.evolution.CatalogCharacteristicUnit;
import com.evolving.nglm.evolution.CommunicationChannel;
import com.evolving.nglm.evolution.CriterionField;
import com.evolving.nglm.evolution.CriterionFieldRetriever;
import com.evolving.nglm.evolution.CustomerMetaData;
import com.evolving.nglm.evolution.DNBOMatrixVariable;
import com.evolving.nglm.evolution.DeliveryManagerAccount;
import com.evolving.nglm.evolution.DeliveryManagerDeclaration;
import com.evolving.nglm.evolution.EvolutionEngine;
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration;
import com.evolving.nglm.evolution.EvolutionEngineExtension;
import com.evolving.nglm.evolution.ExtendedSubscriberProfile;
import com.evolving.nglm.evolution.ExternalAPI;
import com.evolving.nglm.evolution.ExternalAPITopic;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManagerExtension;
import com.evolving.nglm.evolution.JourneyMetricConfiguration;
import com.evolving.nglm.evolution.JourneyMetricDeclaration;
import com.evolving.nglm.evolution.NodeType;
import com.evolving.nglm.evolution.NotificationManager;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm;
import com.evolving.nglm.evolution.OfferProperty;
import com.evolving.nglm.evolution.PartnerType;
import com.evolving.nglm.evolution.ProfileChangeEvent;
import com.evolving.nglm.evolution.ScoringEngine;
import com.evolving.nglm.evolution.ScoringType;
import com.evolving.nglm.evolution.SubscriberProfile;
import com.evolving.nglm.evolution.SupportedDataType;
import com.evolving.nglm.evolution.SupportedLanguage;
import com.evolving.nglm.evolution.SupportedRelationship;
import com.evolving.nglm.evolution.SupportedTokenCodesFormat;
import com.evolving.nglm.evolution.SupportedVoucherCodePattern;
import com.evolving.nglm.evolution.ThirdPartyMethodAccessLevel;
import com.evolving.nglm.evolution.ToolboxSection;
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration.EventRule;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.datacubes.SubscriberProfileDatacubeMetric;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchConnectionSettings;
import com.evolving.nglm.evolution.kafka.Topic;
import com.evolving.nglm.evolution.tenancy.Tenant;

/**
 * This class does only contains static variable because all those settings are shared between
 * each tenant and should not be override.  
 */
public class DeploymentCommon
{
  protected static final Logger log = LoggerFactory.getLogger(DeploymentCommon.class);
  
  private static Map<Tenant, JSONObject> jsonConfigPerTenant;
  private static Tenant defaultTenant;
  private static Map<Integer, Deployment> deploymentsPerTenant; // Will contains instance of Deployment class from nglm-project.

  public static void initialize() { /* Just to be sure the static bloc is read. */ }
  public static Set<Tenant> getTenants() { return jsonConfigPerTenant.keySet(); }
  public static Tenant getDefaultTenant() { return defaultTenant; }
  public static Set<Tenant> getRealTenants() { // all but tenant 0
    Set<Tenant> tenants = new HashSet<>();
    for (Tenant tenant : getTenants()) {
      if (tenant.getTenantID() != 0) tenants.add(tenant);
    }
    return tenants;
  }
  public static Deployment getDeployment(int tenantID) { return deploymentsPerTenant.get(tenantID); }
  public static Deployment getDefault() { return getDeployment(0); }
  
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
        // Init variables
        //
        jsonConfigPerTenant = new HashMap<>();
        deploymentsPerTenant = new HashMap<>();
            
        //
        // Extract jsonRoot from Zookeeper
        //
        JSONObject brutJsonRoot = getBrutJsonRoot();
        
        //
        // Remove comments fields
        //
        JSONObject noCommentsJsonRoot = removeComments(brutJsonRoot);
        DeploymentJSONReader commonJsonReader = new DeploymentJSONReader(noCommentsJsonRoot);
        
        //
        // Init settings that will be needed for the initialization of GUIManagedObject static code
        //
        loadCoreSettings(commonJsonReader);
        
        //
        // Init of static GUIManagedObject
        //
        // The goal of this line is to be sure that the static code of GUIManagedObject will be executed before any static code of any class that 
        // extends GUIManagedObject. If one class is init before, then the call of commonSchema() in the static code of this class will return null,
        // resulting in a java.lang.ExceptionInInitializerError caused by: java.lang.NullPointerException
        GUIManagedObject.commonSchema();
        
        //
        // Init of all other common settings (nglm-product)
        //
        loadProductCommonSettings(commonJsonReader);
        
        //
        // Init of all other common settings (nglm-project)
        //
        Class<? extends Deployment> projectDeploymentClass = getProjectDeploymentClass();
        Method loadProjectCommonSettings = projectDeploymentClass.getMethod("loadProjectCommonSettings", DeploymentJSONReader.class);
        loadProjectCommonSettings.invoke(null, commonJsonReader);
        
        //
        // Build remaining json after common variable extraction
        // Meaning that this json does only contain variables that can be override.
        //
        JSONObject remainingJsonRoot = commonJsonReader.buildRemaining();
    
        //
        // Build & store every version of jsonRoot per tenant
        //
        buildJsonPerTenant(remainingJsonRoot);
        
        //
        // Check that at least one tenant is defined
        //
        if(getTenants().size() <= 1) {
          throw new ServerRuntimeException("You need to define at least one tenant in your deployment JSON settings");
        }
        
        //
        // Load all tenant Deployment instance 
        //
        for(Tenant tenant : getTenants()) {
          Deployment deployment = projectDeploymentClass.newInstance();
          
          //
          // Retrieve jsonRoot
          //
          JSONObject jsonRoot = jsonConfigPerTenant.get(tenant);
          DeploymentJSONReader jsonReader = new DeploymentJSONReader(jsonRoot);
          
          //
          // Load variables (nglm-product and nglm-project)
          //
          deployment.loadProductTenantSettings(jsonReader, tenant);
          deployment.loadProjectTenantSettings(jsonReader, tenant);
          
          //
          // Display warnings (unused fields, forbidden overrides)
          //
          if(tenant.getTenantID() != 0) { // Do not display for default, some tenant variables have no "default" meaning
            jsonReader.checkUnusedFields("jsonRoot(tenant "+tenant.getTenantID()+")");
          }
          
          //
          // Save it
          //
          deploymentsPerTenant.put(tenant.getTenantID(), deployment);
        }
      } 
    catch (Exception e)
      {
        throw new ServerRuntimeException("Unable to run DeploymentCommon static initialization.", e);
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
  // Core (Classes)
  //
  private static String projectDeploymentClassName;
  private static String criterionFieldRetrieverClassName;
  private static String evolutionEngineExtensionClassName;
  private static String guiManagerExtensionClassName;
  private static String subscriberProfileClassName;
  private static String extendedSubscriberProfileClassName;
  private static String evolutionEngineExternalAPIClassName;
  //
  // System
  //
  private static String evolutionVersion;
  private static String customerVersion;
  private static JSONObject licenseManagement;
  //
  // Elasticsearch
  //
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
  private static int elasticsearchRetentionDaysEDR;
  private static int elasticsearchRetentionDaysJourneys;
  private static int elasticsearchRetentionDaysCampaigns;
  private static int elasticsearchRetentionDaysBulkCampaigns;
  private static int elasticsearchRetentionWeeksDatacubeJourneys;
  private static Map<String, Long> elasticsearchTemplatesVersion;
  private static Map<String, Long> elasticsearchIndexByPassVersion;

  private static Map<String, ConnectTaskConfiguration> connectTask;
  private static ConnectTaskConfiguration connectTaskConfigDefault;
  
  
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
  private static int kafkaRetentionDaysLoyaltyPrograms;
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
  private static String otpTypeTopic;
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
  private static String uploadedFileTopic;
  private static String targetTopic;
  private static String communicationChannelTopic;
  private static String communicationChannelBlackoutTopic;
  private static String communicationChannelTimeWindowTopic;
  private static String tokenChangeTopic;
  private static String loyaltyProgramTopic;
  private static String badgeTopic;
  private static String badgeObjectiveTopic;
  private static String exclusionInclusionTargetTopic;
  private static String dnboMatrixTopic;
  private static String segmentContactPolicyTopic;
  private static String dynamicEventDeclarationsTopic;
  private static String dynamicCriterionFieldsTopic;
  private static String criterionFieldAvailableValuesTopic;
  private static String sourceAddressTopic;
  private static String voucherChangeRequestTopic;
  private static String voucherChangeResponseTopic;
  private static String otpInstanceChangeRequestTopic;
  private static String otpInstanceChangeResponseTopic;
  private static String edrDetailsTopic;
  private static String workflowEventTopic;
  private static String subscriberProfileForceUpdateResponseTopic;
  private static String notificationEventTopic;
  private static String badgeChangeRequestTopic;
  private static String badgeChangeResponseTopic;
    
  //
  // Others
  //
  private static Map<String,AlternateID> alternateIDs;
  private static String externalSubscriberID;
  private static String reportManagerZookeeperDir;
  private static String reportManagerOutputPath;
  private static String reportManagerFileDateFormat;
  private static String reportManagerContentDateFormat;
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
  private static JSONArray initialOTPTypesJSONArray;
  private static JSONArray initialVoucherCodeFormatsJSONArray;
  private static JSONArray initialSegmentationDimensionsJSONArray;
  private static JSONArray initialComplexObjectJSONArray;
  private static boolean generateSimpleProfileDimensions;
  private static Map<String,CommunicationChannel> communicationChannels;
  private static Map<String,SupportedDataType> supportedDataTypes;
  private static JourneyMetricConfiguration journeyMetricConfiguration;
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
  private static Map<String,NodeType> nodeTypes;
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
  private static int nodesTransitionsHistorySize;
  private static int firstDayOfTheWeek;
  private static int journeysReportMaxParallelThreads;
  private static int detailedrecordReportsArrearCount;
  private static int journeyReportsArrearCount;
  private static int subscriberprofileReportsArrearCount;
  
  private static int guiConfigurationSoftRetentionDays;// "soft" this is the number of days after which we stopped loading in memory deleted GUIManagedObjects
  private static int guiConfigurationRetentionDays;// this is the number of days after which we delete record from topic deleted GUIManagedObjects
  private static long guiConfigurationCleanerThreadPeriodMs;
  private static int guiConfigurationInitialConsumerMaxPollRecords;
  private static int guiConfigurationInitialConsumerMaxFetchBytes;

  private static boolean addSubscribersToUcgByCounting;


  
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
  public static ElasticsearchConnectionSettings getElasticsearchConnectionSettings(String name, boolean forConnect) {
    if(elasticsearchConnectionSettings.get(name) == null) {
      return (forConnect) ? elasticsearchConnectionSettings.get("connectDefault") : elasticsearchConnectionSettings.get("default");
    }
    return elasticsearchConnectionSettings.get(name);
  }
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
  public static int getElasticsearchRetentionDaysEDR() { return elasticsearchRetentionDaysEDR; }
  public static int getElasticsearchRetentionDaysJourneys() { return elasticsearchRetentionDaysJourneys; }
  public static int getElasticsearchRetentionDaysCampaigns() { return elasticsearchRetentionDaysCampaigns; }
  public static int getElasticsearchRetentionDaysBulkCampaigns() { return elasticsearchRetentionDaysBulkCampaigns; }
  public static int getElasticsearchRetentionWeeksDatacubeJourneys() { return elasticsearchRetentionWeeksDatacubeJourneys; }
  public static Map<String, Long> getElasticsearchTemplatesVersion() { return elasticsearchTemplatesVersion; }
  public static Map<String, Long> getElasticsearchIndexByPassVersion() { return elasticsearchIndexByPassVersion; }
  public static Long getElasticsearchRootTemplateVersion() { return elasticsearchTemplatesVersion.get("root"); }
  public static Long getElasticsearchSubscriberprofileTemplateVersion() { return elasticsearchTemplatesVersion.get("subscriberprofile"); }
  public static Long getElasticsearchBdrTemplateVersion() { return elasticsearchTemplatesVersion.get("detailedrecords_bonuses"); }
  public static Long getElasticsearchTokenTemplateVersion() { return elasticsearchTemplatesVersion.get("detailedrecords_tokens"); }
  public static Long getElasticsearchOdrTemplateVersion() { return elasticsearchTemplatesVersion.get("detailedrecords_offers"); }
  public static Long getElasticsearchVdrTemplateVersion() { return elasticsearchTemplatesVersion.get("detailedrecords_vouchers"); }
  public static Long getElasticsearchMdrTemplateVersion() { return elasticsearchTemplatesVersion.get("detailedrecords_messages"); }
  public static Long getElasticsearchEdrTemplateVersion() { return elasticsearchTemplatesVersion.get("detailedrecords_events"); }
  public static Long getElasticsearcJourneystatisticTemplateVersion() { return elasticsearchTemplatesVersion.get("journeystatistic"); }
  public static Long getElasticsearchDatacubeSubscriberprofileTemplateVersion() { return elasticsearchTemplatesVersion.get("datacube_subscriberprofile"); }
  public static Long getElasticsearchDatacubeLoyaltyprogramshistoryTemplateVersion() { return elasticsearchTemplatesVersion.get("datacube_loyaltyprogramshistory"); }
  public static Long getElasticsearchDatacubeLoyaltyprogramschangesTemplateVersion() { return elasticsearchTemplatesVersion.get("datacube_loyaltyprogramschanges"); }
  public static Long getElasticsearchDatacubeJourneytrafficTemplateVersion() { return elasticsearchTemplatesVersion.get("datacube_journeytraffic"); }
  public static Long getElasticsearchDatacubeJourneyrewardsTemplateVersion() { return elasticsearchTemplatesVersion.get("datacube_journeyrewards"); }
  public static Long getElasticsearchDatacubeOdrTemplateVersion() { return elasticsearchTemplatesVersion.get("datacube_odr"); }
  public static Long getElasticsearchDatacubeBdrTemplateVersion() { return elasticsearchTemplatesVersion.get("datacube_bdr"); }
  public static Long getElasticsearchDatacubeMessagesTemplateVersion() { return elasticsearchTemplatesVersion.get("datacube_messages"); }
  public static Long getElasticsearchDatacubeVdrTemplateVersion() { return elasticsearchTemplatesVersion.get("datacube_vdr"); }
  public static Long getElasticsearchMappingModulesTemplateVersion() { return elasticsearchTemplatesVersion.get("mapping_modules"); }
  public static Long getElasticsearchMappingJourneysTemplateVersion() { return elasticsearchTemplatesVersion.get("mapping_journeys"); }
  public static Long getElasticsearchMappingJourneyrewardsTemplateVersion() { return elasticsearchTemplatesVersion.get("mapping_journeyrewards"); }
  public static Long getElasticsearchMappingDeliverablesTemplateVersion() { return elasticsearchTemplatesVersion.get("mapping_deliverables"); }
  public static Long getElasticsearchMappingPartnersTemplateVersion() { return elasticsearchTemplatesVersion.get("mapping_partners"); }
  public static Long getElasticsearchMappingBasemanagementTemplateVersion() { return elasticsearchTemplatesVersion.get("mapping_basemanagment"); }
  public static Long getElasticsearchMappingJourneyobjectiveTemplateVersion() { return elasticsearchTemplatesVersion.get("mapping_journeyobjective"); }

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
  public static int getKafkaRetentionDaysLoyaltyPrograms() { return kafkaRetentionDaysLoyaltyPrograms; }
  public static int getKafkaRetentionDaysTargets() { return kafkaRetentionDaysTargets; } 
  public static int getJourneysReportMaxParallelThreads() { return journeysReportMaxParallelThreads; }
  public static int getDetailedrecordReportsArrearCount() { return detailedrecordReportsArrearCount; }
  public static int getJourneyReportsArrearCount() { return journeyReportsArrearCount; }
  public static int getSubscriberprofileReportsArrearCount() { return subscriberprofileReportsArrearCount; }
  
  public static int getGuiConfigurationSoftRetentionDays() { return guiConfigurationSoftRetentionDays; }
  public static int getGuiConfigurationRetentionDays() { return guiConfigurationRetentionDays; }
  public static long getGuiConfigurationCleanerThreadPeriodMs() { return guiConfigurationCleanerThreadPeriodMs; }
  public static int getGuiConfigurationInitialConsumerMaxPollRecords() { return guiConfigurationInitialConsumerMaxPollRecords; }
  public static int getGuiConfigurationInitialConsumerMaxFetchBytes() { return guiConfigurationInitialConsumerMaxFetchBytes; }
  
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
  public static String getOTPTypeTopic() { return otpTypeTopic; }
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
  public static String getUploadedFileTopic() { return uploadedFileTopic; }
  public static String getTargetTopic() { return targetTopic; }
  public static String getCommunicationChannelTopic() { return communicationChannelTopic; }
  public static String getCommunicationChannelBlackoutTopic() { return communicationChannelBlackoutTopic; }
  public static String getCommunicationChannelTimeWindowTopic() { return communicationChannelTimeWindowTopic; }
  public static String getTokenChangeTopic() { return tokenChangeTopic; }
  public static String getLoyaltyProgramTopic() { return loyaltyProgramTopic; }
  public static String getBadgeTopic() { return badgeTopic; }
  public static String getBadgeObjectiveTopic() { return badgeObjectiveTopic; }
  public static String getExclusionInclusionTargetTopic() { return exclusionInclusionTargetTopic; }
  public static String getDNBOMatrixTopic() { return dnboMatrixTopic; }
  public static String getSegmentContactPolicyTopic() { return segmentContactPolicyTopic; }
  public static String getDynamicEventDeclarationsTopic() { return dynamicEventDeclarationsTopic; }
  public static String getDynamicCriterionFieldTopic() { return dynamicCriterionFieldsTopic; }
  public static String getCriterionFieldAvailableValuesTopic() { return criterionFieldAvailableValuesTopic; }
  public static String getSourceAddressTopic() { return sourceAddressTopic; }
  public static String getVoucherChangeRequestTopic() { return voucherChangeRequestTopic; }
  public static String getVoucherChangeResponseTopic() { return voucherChangeResponseTopic; }
  public static String getOTPInstanceChangeRequestTopic() { return otpInstanceChangeRequestTopic;}
  public static String getOTPInstanceChangeResponseTopic() { return otpInstanceChangeResponseTopic;}
  public static String getEdrDetailsTopic() { return edrDetailsTopic; }
  public static String getWorkflowEventTopic() { return workflowEventTopic; }
  public static String getSubscriberProfileForceUpdateResponseTopic() { return subscriberProfileForceUpdateResponseTopic; }    
  public static String getNotificationEventTopic() { return notificationEventTopic; }
  public static String getBadgeChangeRequestTopic() { return badgeChangeRequestTopic; }
  public static String getBadgeChangeResponseTopic() { return badgeChangeResponseTopic; }
  
  
  //
  // Others
  //
  
  public static Map<String,AlternateID> getAlternateIDs() { return alternateIDs; }
  public static String getExternalSubscriberID() { return externalSubscriberID; }
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
  public static JSONArray getInitialOTPTypesJSONArray() { return initialOTPTypesJSONArray; }
  public static JSONArray getInitialVoucherCodeFormatsJSONArray() { return initialVoucherCodeFormatsJSONArray; }
  public static JSONArray getInitialSegmentationDimensionsJSONArray() { return initialSegmentationDimensionsJSONArray; }
  public static JSONArray getInitialComplexObjectJSONArray() { return initialComplexObjectJSONArray; }
  public static boolean getGenerateSimpleProfileDimensions() { return generateSimpleProfileDimensions; }
  public static Map<String,SupportedDataType> getSupportedDataTypes() { return supportedDataTypes; }
  public static JourneyMetricConfiguration getJourneyMetricConfiguration() { return journeyMetricConfiguration; }
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
  public static Map<String,NodeType> getNodeTypes() { return nodeTypes; } // EVPRO-99 should not be per tenant...
  public static Map<String,ThirdPartyMethodAccessLevel> getThirdPartyMethodPermissionsMap() { return thirdPartyMethodPermissionsMap; } // TODO EVPRO-99 check for tenant and static
  public static Integer getAuthResponseCacheLifetimeInMinutes() { return authResponseCacheLifetimeInMinutes; }
  public static Integer getReportManagerMaxMessageLength() { return reportManagerMaxMessageLength; } // TODO EVPRO-99 check for tenant and static
  public static int getStockRefreshPeriod() { return stockRefreshPeriod; } // TODO EVPRO-99 check for tenant and static
  public static String getPeriodicEvaluationCronEntry() { return periodicEvaluationCronEntry; }
  public static String getUCGEvaluationCronEntry() { return ucgEvaluationCronEntry; } // TODO EVPRO-99 check for tenant and static
  public static String getReportManagerZookeeperDir() { return reportManagerZookeeperDir; }
  public static String getReportManagerOutputPath() { return reportManagerOutputPath; } // TODO EVPRO-99 Move in TENANT
  public static String getReportManagerContentDateFormat() { return reportManagerContentDateFormat; }
  public static String getReportManagerFileDateFormat() { return reportManagerFileDateFormat; }
  public static String getReportManagerFileExtension() { return reportManagerFileExtension; } // TODO EVPRO-99 Move in TENANT
  public static String getReportManagerCsvSeparator() { return reportManagerCsvSeparator; } // EVPRO-99 check for tenant and static
  public static String getReportManagerFieldSurrounder() { return reportManagerFieldSurrounder; } // EVPRO-99 check for tenant and static
  public static String getUploadedFileSeparator() { return uploadedFileSeparator; } // EVPRO-99 check for tenant and static
  public static String getReportManagerStreamsTempDir() { return reportManagerStreamsTempDir; }
  public static String getReportManagerTopicsCreationProperties() { return reportManagerTopicsCreationProperties; }
  public static CustomerMetaData getCustomerMetaData() { return customerMetaData; }
  @Deprecated // TODO EVPRO-99 TO BE REMOVED
  public static String getAPIresponseDateFormat() { return APIresponseDateFormat; } // EVPRO-99 check for tenant and static
  public static Map<String,PartnerType> getPartnerTypes() { return partnerTypes; }
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
  public static int getNodesTransitionsHistorySize() { return nodesTransitionsHistorySize; }
  public static int getFirstDayOfTheWeek() { return firstDayOfTheWeek; }
  public static boolean getAddSubscribersToUcgByCounting() { return addSubscribersToUcgByCounting; }
  
  
  /****************************************
  *
  * Load all variables need by static code (GUIManagedObject init)
  *
  ****************************************/
  private static void loadCoreSettings(DeploymentJSONReader jsonReader) throws Exception {
    projectDeploymentClassName = jsonReader.decodeString("projectDeploymentClass");
    criterionFieldRetrieverClassName = jsonReader.decodeString("criterionFieldRetrieverClass");
    evolutionEngineExtensionClassName = jsonReader.decodeString("evolutionEngineExtensionClass");
    guiManagerExtensionClassName = jsonReader.decodeString("guiManagerExtensionClass");
    subscriberProfileClassName = jsonReader.decodeString("subscriberProfileClass");
    extendedSubscriberProfileClassName = jsonReader.decodeString("extendedSubscriberProfileClass");
    evolutionEngineExternalAPIClassName = jsonReader.decodeString("externalAPIClass");
  }
  
  /****************************************
  *
  * Load all common variables from JSONObject
  *
  ****************************************/
  // This method needs to be overriden in nglm-project (even with nothing inside if there is nothing to do).
  protected static void loadProjectCommonSettings(DeploymentJSONReader jsonReader) throws Exception {
    throw new ServerRuntimeException("loadProjectCommonSettings methods needs to be overriden in your project Deployment class.");
  }
  
  private static void loadProductCommonSettings(DeploymentJSONReader jsonReader) throws Exception {
    //
    // System
    //
    licenseManagement = jsonReader.decodeJSONObject("licenseManagement"); // EVPRO-99
    evolutionVersion = jsonReader.decodeString("evolutionVersion");
    customerVersion = jsonReader.decodeString("customerVersion");
    
    //
    // Elasticsearch
    //
    
    elasticsearchScrollSize = jsonReader.decodeInteger("elasticsearchScrollSize");
    elasticSearchScrollKeepAlive = jsonReader.decodeInteger("elasticSearchScrollKeepAlive");
    // Shards & replicas
    elasticsearchDefaultShards = jsonReader.decodeInteger("elasticsearchDefaultShards");
    elasticsearchDefaultReplicas = jsonReader.decodeInteger("elasticsearchDefaultReplicas");
    elasticsearchSubscriberprofileShards = jsonReader.decodeInteger("elasticsearchSubscriberprofileShards");
    elasticsearchSubscriberprofileReplicas = jsonReader.decodeInteger("elasticsearchSubscriberprofileReplicas");
    elasticsearchSnapshotShards = jsonReader.decodeInteger("elasticsearchSnapshotShards");
    elasticsearchSnapshotReplicas = jsonReader.decodeInteger("elasticsearchSnapshotReplicas");
    elasticsearchLiveVoucherShards = jsonReader.decodeInteger("elasticsearchLiveVoucherShards");
    elasticsearchLiveVoucherReplicas = jsonReader.decodeInteger("elasticsearchLiveVoucherReplicas");
    // Retention days
    elasticsearchRetentionDaysODR = jsonReader.decodeInteger("ESRetentionDaysODR");
    elasticsearchRetentionDaysBDR = jsonReader.decodeInteger("ESRetentionDaysBDR");
    elasticsearchRetentionDaysMDR = jsonReader.decodeInteger("ESRetentionDaysMDR");
    elasticsearchRetentionDaysTokens = jsonReader.decodeInteger("ESRetentionDaysTokens");
    elasticsearchRetentionDaysSnapshots = jsonReader.decodeInteger("ESRetentionDaysSnapshots");
    elasticsearchRetentionDaysVDR = jsonReader.decodeInteger("ESRetentionDaysVDR");
    elasticsearchRetentionDaysEDR = jsonReader.decodeInteger("ESRetentionDaysEDR");
    elasticsearchRetentionDaysJourneys = jsonReader.decodeInteger("ESRetentionDaysJourneys");
    elasticsearchRetentionDaysCampaigns = jsonReader.decodeInteger("ESRetentionDaysCampaigns");
    elasticsearchRetentionDaysBulkCampaigns = jsonReader.decodeInteger("ESRetentionDaysBulkCampaigns");
    elasticsearchRetentionWeeksDatacubeJourneys = jsonReader.decodeInteger("ESRetentionWeeksDatacubeJourneys");
    JSONObject elasticsearchTemplatesVersionJSON = jsonReader.decodeJSONObject("elasticsearchTemplatesVersion");
    elasticsearchTemplatesVersion = new LinkedHashMap<String, Long>();
    for (Object key : elasticsearchTemplatesVersionJSON.keySet()) {
      elasticsearchTemplatesVersion.put((String) key, (Long) elasticsearchTemplatesVersionJSON.get(key));
    }
    JSONObject elasticsearchIndexByPassVersionJSON = jsonReader.decodeJSONObject("elasticsearchIndexByPassVersion");
    elasticsearchIndexByPassVersion = new LinkedHashMap<String, Long>();
    for (Object key : elasticsearchIndexByPassVersionJSON.keySet()) {
      elasticsearchIndexByPassVersion.put((String) key, (Long) elasticsearchIndexByPassVersionJSON.get(key));
    }

    // connectTask
    connectTask = new HashMap<>();
    JSONObject connectTaskJSON = jsonReader.decodeJSONObject("connectTask");
    for (Object key : connectTaskJSON.keySet()) {
      connectTask.put((String) key, new ConnectTaskConfiguration((JSONObject) connectTaskJSON.get(key)));
    }
    connectTaskConfigDefault = connectTask.get("default");
    if (connectTaskConfigDefault == null) {
      throw new ServerRuntimeException("connectTask section must have a \"default\" configuration.");
    }
    
    //
    // Kafka
    //
    topicSubscriberPartitions = Integer.parseInt(jsonReader.decodeString("topicSubscriberPartitions"));
    topicReplication = Integer.parseInt(jsonReader.decodeString("topicReplication"));
    topicMinInSyncReplicas = jsonReader.decodeString("topicMinInSyncReplicas");
    topicRetentionShortMs = ""+(jsonReader.decodeInteger("topicRetentionShortHour") * 3600 * 1000L);
    topicRetentionMs = ""+(jsonReader.decodeInteger("topicRetentionDay") * 24 * 3600 * 1000L);
    topicRetentionLongMs = ""+(jsonReader.decodeInteger("topicRetentionLongDay") * 24 * 3600 * 1000L);
    kafkaRetentionDaysExpiredTokens = jsonReader.decodeInteger("kafkaRetentionDaysExpiredTokens");
    kafkaRetentionDaysExpiredVouchers = jsonReader.decodeInteger("kafkaRetentionDaysExpiredVouchers");
    kafkaRetentionDaysLoyaltyPrograms = jsonReader.decodeInteger("kafkaRetentionDaysLoyaltyPrograms");
    kafkaRetentionDaysTargets = jsonReader.decodeInteger("kafkaRetentionDaysTargets");
    maxPollIntervalMs = jsonReader.decodeInteger("maxPollIntervalMs");
    purchaseTimeoutMs = jsonReader.decodeInteger("purchaseTimeoutMs");
    
    //
    // Topics
    //
    subscriberTraceControlTopic = jsonReader.decodeString("subscriberTraceControlTopic");
    subscriberTraceControlAssignSubscriberIDTopic = jsonReader.decodeString("subscriberTraceControlAssignSubscriberIDTopic");
    subscriberTraceTopic = jsonReader.decodeString("subscriberTraceTopic");
    simulatedTimeTopic = jsonReader.decodeString("simulatedTimeTopic");
    assignSubscriberIDsTopic = jsonReader.decodeString("assignSubscriberIDsTopic");
    updateExternalSubscriberIDTopic = jsonReader.decodeString("updateExternalSubscriberIDTopic");
    assignExternalSubscriberIDsTopic = jsonReader.decodeString("assignExternalSubscriberIDsTopic");
    recordSubscriberIDTopic = jsonReader.decodeString("recordSubscriberIDTopic");
    recordAlternateIDTopic = jsonReader.decodeString("recordAlternateIDTopic");
    autoProvisionedSubscriberChangeLog = jsonReader.decodeString("autoProvisionedSubscriberChangeLog");
    autoProvisionedSubscriberChangeLogTopic = jsonReader.decodeString("autoProvisionedSubscriberChangeLogTopic");
    rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic = jsonReader.decodeString("rekeyedAutoProvisionedAssignSubscriberIDsStreamTopic");
    cleanupSubscriberTopic = jsonReader.decodeString("cleanupSubscriberTopic");
    journeyTopic = jsonReader.decodeString("journeyTopic");
    journeyTemplateTopic = jsonReader.decodeString("journeyTemplateTopic");
    segmentationDimensionTopic = jsonReader.decodeString("segmentationDimensionTopic");
    pointTopic = jsonReader.decodeString("pointTopic");
    complexObjectTypeTopic = jsonReader.decodeString("complexObjectTypeTopic");
    offerTopic = jsonReader.decodeString("offerTopic");
    reportTopic = jsonReader.decodeString("reportTopic");
    paymentMeanTopic = jsonReader.decodeString("paymentMeanTopic");
    presentationStrategyTopic = jsonReader.decodeString("presentationStrategyTopic");
    scoringStrategyTopic = jsonReader.decodeString("scoringStrategyTopic");
    callingChannelTopic = jsonReader.decodeString("callingChannelTopic");
    salesChannelTopic = jsonReader.decodeString("salesChannelTopic");
    supplierTopic = jsonReader.decodeString("supplierTopic");
    resellerTopic = jsonReader.decodeString("resellerTopic");
    productTopic = jsonReader.decodeString("productTopic");
    catalogCharacteristicTopic = jsonReader.decodeString("catalogCharacteristicTopic");
    contactPolicyTopic = jsonReader.decodeString("contactPolicyTopic");
    journeyObjectiveTopic = jsonReader.decodeString("journeyObjectiveTopic");
    offerObjectiveTopic = jsonReader.decodeString("offerObjectiveTopic");
    productTypeTopic = jsonReader.decodeString("productTypeTopic");
    ucgRuleTopic = jsonReader.decodeString("ucgRuleTopic");
    deliverableTopic = jsonReader.decodeString("deliverableTopic");
    tokenTypeTopic = jsonReader.decodeString("tokenTypeTopic");
    otpTypeTopic = jsonReader.decodeString("otpTypeTopic");
    voucherTypeTopic = jsonReader.decodeString("voucherTypeTopic");
    voucherTopic = jsonReader.decodeString("voucherTopic");
    subscriberMessageTemplateTopic = jsonReader.decodeString("subscriberMessageTemplateTopic");
    guiAuditTopic = jsonReader.decodeString("guiAuditTopic");
    subscriberGroupTopic = jsonReader.decodeString("subscriberGroupTopic");
    subscriberGroupAssignSubscriberIDTopic = jsonReader.decodeString("subscriberGroupAssignSubscriberIDTopic");
    subscriberGroupEpochTopic = jsonReader.decodeString("subscriberGroupEpochTopic");
    ucgStateTopic = jsonReader.decodeString("ucgStateTopic");
    renamedProfileCriterionFieldTopic = jsonReader.decodeString("renamedProfileCriterionFieldTopic");
    uploadedFileTopic = jsonReader.decodeString("uploadedFileTopic");
    targetTopic = jsonReader.decodeString("targetTopic");
    exclusionInclusionTargetTopic = jsonReader.decodeString("exclusionInclusionTargetTopic");
    dnboMatrixTopic = jsonReader.decodeString("dnboMatrixTopic");
    dynamicEventDeclarationsTopic = jsonReader.decodeString("dynamicEventDeclarationsTopic");
    dynamicCriterionFieldsTopic = jsonReader.decodeString("dynamicCriterionFieldTopic");
    communicationChannelBlackoutTopic = jsonReader.decodeString("communicationChannelBlackoutTopic");
    communicationChannelTimeWindowTopic = jsonReader.decodeString("communicationChannelTimeWindowTopic");
    communicationChannelTopic = jsonReader.decodeString("communicationChannelTopic");
    tokenChangeTopic = jsonReader.decodeString("tokenChangeTopic");
    loyaltyProgramTopic = jsonReader.decodeString("loyaltyProgramTopic");
    badgeTopic = jsonReader.decodeString("badgeTopic");
    badgeObjectiveTopic = jsonReader.decodeString("badgeObjectiveTopic");
    timedEvaluationTopic = jsonReader.decodeString("timedEvaluationTopic");
    evaluateTargetsTopic = jsonReader.decodeString("evaluateTargetsTopic");
    subscriberProfileForceUpdateTopic = jsonReader.decodeString("subscriberProfileForceUpdateTopic");
    executeActionOtherSubscriberTopic = jsonReader.decodeString("executeActionOtherSubscriberTopic");
    subscriberStateChangeLog = jsonReader.decodeString("subscriberStateChangeLog");
    subscriberStateChangeLogTopic = jsonReader.decodeString("subscriberStateChangeLogTopic");
    extendedSubscriberProfileChangeLog = jsonReader.decodeString("extendedSubscriberProfileChangeLog");
    extendedSubscriberProfileChangeLogTopic = jsonReader.decodeString("extendedSubscriberProfileChangeLogTopic");
    journeyStatisticTopic = jsonReader.decodeString("journeyStatisticTopic");
    journeyMetricTopic = jsonReader.decodeString("journeyMetricTopic");
    presentationLogTopic = jsonReader.decodeString("presentationLogTopic");
    acceptanceLogTopic = jsonReader.decodeString("acceptanceLogTopic");
    segmentContactPolicyTopic = jsonReader.decodeString("segmentContactPolicyTopic");
    profileChangeEventTopic = jsonReader.decodeString("profileChangeEventTopic");
    profileSegmentChangeEventTopic = jsonReader.decodeString("profileSegmentChangeEventTopic");
    profileLoyaltyProgramChangeEventTopic = jsonReader.decodeString("profileLoyaltyProgramChangeEventTopic");
    voucherActionTopic = jsonReader.decodeString("voucherActionTopic");
    fileWithVariableEventTopic = jsonReader.decodeString("fileWithVariableEventTopic");
    tokenRedeemedTopic = jsonReader.decodeString("tokenRedeemedTopic");
    criterionFieldAvailableValuesTopic = jsonReader.decodeString("criterionFieldAvailableValuesTopic");
    sourceAddressTopic = jsonReader.decodeString("sourceAddressTopic");
    voucherChangeRequestTopic = jsonReader.decodeString("voucherChangeRequestTopic");
    voucherChangeResponseTopic = jsonReader.decodeString("voucherChangeResponseTopic");
    otpInstanceChangeRequestTopic = jsonReader.decodeString("otpInstanceChangeRequestTopic");
    otpInstanceChangeResponseTopic = jsonReader.decodeString("otpInstanceChangeResponseTopic");
    subscriberProfileForceUpdateResponseTopic = jsonReader.decodeString("subscriberProfileForceUpdateResponseTopic");
    edrDetailsTopic = jsonReader.decodeString("edrDetailsTopic");
    workflowEventTopic = jsonReader.decodeString("workflowEventTopic");
    notificationEventTopic = jsonReader.decodeString("notificationEventTopic");
    badgeChangeRequestTopic = jsonReader.decodeString("badgeChangeRequestTopic");
    badgeChangeResponseTopic = jsonReader.decodeString("badgeChangeResponseTopic");
    
    alternateIDs = jsonReader.decodeMapFromArray(AlternateID.class, "alternateIDs");
    
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
    
    // Elasticsearch connection settings
    elasticsearchConnectionSettings = new LinkedHashMap<String,ElasticsearchConnectionSettings>();
    for (Object elasticsearchConnectionSettingsObject: jsonReader.decodeJSONArray("elasticsearchConnectionSettings").toArray()){
      ElasticsearchConnectionSettings elasticsearchConnectionSetting = new ElasticsearchConnectionSettings((JSONObject) elasticsearchConnectionSettingsObject);
      elasticsearchConnectionSettings.put(elasticsearchConnectionSetting.getId(), elasticsearchConnectionSetting);
    }
    //  cleanupSubscriberElasticsearchIndexes
    cleanupSubscriberElasticsearchIndexes = new HashSet<String>();
    JSONArray subscriberESIndexesJSON = jsonReader.decodeJSONArray("cleanupSubscriberElasticsearchIndexes");
    for (int i=0; i<subscriberESIndexesJSON.size(); i++) {
      cleanupSubscriberElasticsearchIndexes.add((String) subscriberESIndexesJSON.get(i));
    }
    
    //
    // httpServerScalingFactor
    //  
    // TODO EVPRO-99 @rl : stop getenv ...
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

    subscriberGroupLoaderAlternateID = jsonReader.decodeOptionalString("subscriberGroupLoaderAlternateID"); // if null, disable feature
    getCustomerAlternateID = jsonReader.decodeString("getCustomerAlternateID");
    subscriberGroupLoaderAutoProvision = jsonReader.decodeBoolean("subscriberGroupLoaderAutoProvision");
    enableProfileSegmentChange = jsonReader.decodeBoolean("enableProfileSegmentChange");

    //
    // subscriberTrace
    //
    subscriberTraceControlAlternateID = jsonReader.decodeOptionalString("subscriberTraceControlAlternateID"); // if null, disable feature TODO EVPRO-99 a tester !
    subscriberTraceControlAutoProvision = jsonReader.decodeBoolean("subscriberTraceControlAutoProvision");
    autoProvisionEvents = jsonReader.decodeMapFromArray(AutoProvisionEvent.class, "autoProvisionEvents");
    
    //
    //  evolutionEngineEvents
    //
    
    //  deployment-level events
    evolutionEngineEvents = new LinkedHashMap<String,EvolutionEngineEventDeclaration>();
    JSONArray evolutionEngineEventValues = jsonReader.decodeJSONArray("evolutionEngineEvents");
    for (int i=0; i<evolutionEngineEventValues.size(); i++)
      {
        JSONObject evolutionEngineEventJSON = (JSONObject) evolutionEngineEventValues.get(i);
        EvolutionEngineEventDeclaration evolutionEngineEventDeclaration = new EvolutionEngineEventDeclaration(evolutionEngineEventJSON);
        evolutionEngineEvents.put(evolutionEngineEventDeclaration.getName(), evolutionEngineEventDeclaration);
      }

    // core-level events
    JSONArray evolutionEngineCoreEventValues = jsonReader.decodeJSONArray("evolutionEngineCoreEvents");
    for (int i=0; i<evolutionEngineCoreEventValues.size(); i++)
      {
        JSONObject evolutionEngineEventJSON = (JSONObject) evolutionEngineCoreEventValues.get(i);
        EvolutionEngineEventDeclaration evolutionEngineEventDeclaration = new EvolutionEngineEventDeclaration(evolutionEngineEventJSON);
        evolutionEngineEvents.put(evolutionEngineEventDeclaration.getName(), evolutionEngineEventDeclaration);
      }

    propensityInitialisationPresentationThreshold = jsonReader.decodeInteger("propensityInitialisationPresentationThreshold");
    propensityInitialisationDurationInDaysThreshold = jsonReader.decodeInteger("propensityInitialisationDurationInDaysThreshold");
    subscriberProfileRegistrySubject = jsonReader.decodeString("subscriberProfileRegistrySubject");

    //
    //  journeyTemplateCapacities
    //
    journeyTemplateCapacities = new LinkedHashMap<String,Long>();    
    JSONObject journeyTemplateCapacitiesJSON = jsonReader.decodeJSONObject("journeyTemplateCapacities");
    for (Object key : journeyTemplateCapacitiesJSON.keySet())
      {
        journeyTemplateCapacities.put((String) key, (Long) journeyTemplateCapacitiesJSON.get(key));
      }
    
    externalAPITopics = jsonReader.decodeMapFromArray(ExternalAPITopic.class, "externalAPITopics");
    partnerTypes = jsonReader.decodeMapFromArray(PartnerType.class, "partnerTypes");
    supportedTokenCodesFormats = jsonReader.decodeMapFromArray(SupportedTokenCodesFormat.class, "supportedTokenCodesFormats");
    supportedVoucherCodePatternList = jsonReader.decodeMapFromArray(SupportedVoucherCodePattern.class, "supportedVoucherCodePatternList");
    supportedRelationships = jsonReader.decodeMapFromArray(SupportedRelationship.class, "supportedRelationships");
    callingChannelProperties = jsonReader.decodeMapFromArray(CallingChannelProperty.class, "callingChannelProperties");
    catalogCharacteristicUnits = jsonReader.decodeMapFromArray(CatalogCharacteristicUnit.class, "catalogCharacteristicUnits");    
    supportedDataTypes = jsonReader.decodeMapFromArray(SupportedDataType.class, "supportedDataTypes");
    subscriberProfileDatacubeMetrics = jsonReader.decodeMapFromArray(SubscriberProfileDatacubeMetric.class, "subscriberProfileDatacubeMetrics");
    
    initialCallingChannelsJSONArray = jsonReader.decodeJSONArray("initialCallingChannels");
    initialSalesChannelsJSONArray = jsonReader.decodeJSONArray("initialSalesChannels");
    initialSourceAddressesJSONArray = jsonReader.decodeJSONArray("initialSourceAddresses");
    initialSuppliersJSONArray = jsonReader.decodeJSONArray("initialSuppliers");
    initialPartnersJSONArray = jsonReader.decodeJSONArray("initialPartners");
    initialProductsJSONArray = jsonReader.decodeJSONArray("initialProducts");
    initialReportsJSONArray = jsonReader.decodeJSONArray("initialReports");
    initialCatalogCharacteristicsJSONArray = jsonReader.decodeJSONArray("initialCatalogCharacteristics");
    initialContactPoliciesJSONArray = jsonReader.decodeJSONArray("initialContactPolicies");
    initialJourneyTemplatesJSONArray = jsonReader.decodeJSONArray("initialJourneyTemplates");
    initialJourneyObjectivesJSONArray = jsonReader.decodeJSONArray("initialJourneyObjectives");
    initialOfferObjectivesJSONArray = jsonReader.decodeJSONArray("initialOfferObjectives");
    initialProductTypesJSONArray = jsonReader.decodeJSONArray("initialProductTypes");
    initialTokenTypesJSONArray = jsonReader.decodeJSONArray("initialTokenTypes");
    initialOTPTypesJSONArray = jsonReader.decodeJSONArray("initialOTPTypes");
    initialVoucherCodeFormatsJSONArray = jsonReader.decodeJSONArray("initialVoucherCodeFormats");
    initialSegmentationDimensionsJSONArray = jsonReader.decodeJSONArray("initialSegmentationDimensions");
    initialComplexObjectJSONArray = jsonReader.decodeJSONArray("initialComplexObjects");

    generateSimpleProfileDimensions = jsonReader.decodeBoolean("generateSimpleProfileDimensions"); // TODO EVPRO-99 move in Deployment ?
    
    // journeyMetricDeclarations
    DeploymentJSONReader journeyMetricConfigurationJsonReader = jsonReader.get("journeyMetrics");
    if( journeyMetricConfigurationJsonReader.keySet().isEmpty() ) {
      // JourneyMetric are therefore disabled
      journeyMetricConfiguration = new JourneyMetricConfiguration();
    } else {
      int priorPeriodDays = journeyMetricConfigurationJsonReader.decodeInteger("priorPeriodDays");
      if(priorPeriodDays < 1) {
        throw new ServerRuntimeException("ERROR: Bad 'journeyMetrics' settings. 'priorPeriodDays' field cannot be negative or zero.");
      }
      
      int postPeriodDays = journeyMetricConfigurationJsonReader.decodeInteger("postPeriodDays");
      if(postPeriodDays < 1) {
        throw new ServerRuntimeException("ERROR: Bad 'journeyMetrics' settings. 'postPeriodDays' field cannot be negative or zero.");
      }
      Map<String,JourneyMetricDeclaration> journeyMetricDeclarations = journeyMetricConfigurationJsonReader.decodeMapFromArray(JourneyMetricDeclaration.class, "metrics");
      if(journeyMetricDeclarations.isEmpty()) { // JourneyMetric are therefore disabled
        journeyMetricConfiguration = new JourneyMetricConfiguration();
      } else {
        journeyMetricConfiguration = new JourneyMetricConfiguration(priorPeriodDays, postPeriodDays, journeyMetricDeclarations);
      }
    }
    
    //
    //  profileCriterionFields
    //
    profileCriterionFields = new LinkedHashMap<String,CriterionField>();
    baseProfileCriterionFields = new LinkedHashMap<String,CriterionField>();
    profileChangeDetectionCriterionFields = new HashMap<>();
    profileChangeGeneratedCriterionFields = new HashMap<>();
    
    //  profileCriterionFields (evolution)
    JSONArray evCriterionFieldValues = jsonReader.decodeJSONArray("evolutionProfileCriterionFields");
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
    JSONArray deplCriterionFieldValues = jsonReader.decodeJSONArray("profileCriterionFields");
    for (int i=0; i<deplCriterionFieldValues.size(); i++)
      {
        JSONObject criterionFieldJSON = (JSONObject) deplCriterionFieldValues.get(i);
        log.info("Decoding profileCriterionField " + criterionFieldJSON.toString());
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
    //  communicationChannels (needs to be after "profileCriterionFields" because commChannels need them)
    //
    communicationChannels = new LinkedHashMap<>();
    JSONArray communicationChannelsJSONArray = jsonReader.decodeJSONArray("communicationChannels");
    for (int i=0; i<communicationChannelsJSONArray.size(); i++)
      {
        JSONObject communicationChannelJSON = (JSONObject) communicationChannelsJSONArray.get(i);
        // TODO EVPRO-99 : why tenantID=1 here ? // TODO EVPRO-99 Replace JSONUtilities
        CommunicationChannel communicationChannel = new CommunicationChannel(communicationChannelJSON, JSONUtilities.decodeInteger(communicationChannelJSON, "tenantID", 1));
        communicationChannels.put(communicationChannel.getID(), communicationChannel);
      }

    //  profileChangeEvent
    EvolutionEngineEventDeclaration profileChangeEvent = new EvolutionEngineEventDeclaration("profile update", ProfileChangeEvent.class.getName(), getProfileChangeEventTopic(), EventRule.Standard, getProfileChangeGeneratedCriterionFields());
    evolutionEngineEvents.put(profileChangeEvent.getName(), profileChangeEvent);

    extendedProfileCriterionFields = jsonReader.decodeMapFromArray(CriterionField.class, "extendedProfileCriterionFields");
    presentationCriterionFields = jsonReader.decodeMapFromArray(CriterionField.class, "presentationCriterionFields");

    offerProperties = jsonReader.decodeMapFromArray(OfferProperty.class, "offerProperties");
    scoringEngines = jsonReader.decodeMapFromArray(ScoringEngine.class, "scoringEngines");
    scoringTypes = jsonReader.decodeMapFromArray(ScoringType.class, "scoringTypes");
    dnboMatrixVariables = jsonReader.decodeMapFromArray(DNBOMatrixVariable.class, "dnboMatrixVariables");
    offerOptimizationAlgorithms = jsonReader.decodeMapFromArray(OfferOptimizationAlgorithm.class, "offerOptimizationAlgorithms");

    // (About offerOptimizationAlgorithmsCommon) 
    // !REMINDER Merge mecanism of deployment.json files already merge array of JSONObject.
    // Therefore it is not needed to split in parts Common, Deployment, Tenant, etc.
    // TO BE REMOVED : JSONArray offerOptimizationAlgorithmValuesCommon = jsonReader.decodeJSONArray("offerOptimizationAlgorithmsCommon");

    //
    //  deliveryManagers/fulfillmentProviders
    //
    deliveryManagers = new LinkedHashMap<String,DeliveryManagerDeclaration>();
    fulfillmentProviders = new LinkedHashMap<String,DeliveryManagerDeclaration>();
    JSONArray deliveryManagerValues = jsonReader.decodeJSONArray("deliveryManagers");
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
    //  nodeTypes
    //
    nodeTypes = jsonReader.decodeMapFromArray(NodeType.class, "nodeTypes");
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
    //  thirdPartyMethodPermissions
    //
    thirdPartyMethodPermissionsMap = new LinkedHashMap<String,ThirdPartyMethodAccessLevel>();
    JSONArray thirdPartyMethodPermissions = jsonReader.decodeJSONArray("thirdPartyMethodPermissions");
    for (int i=0; i<thirdPartyMethodPermissions.size(); i++)
      {
        JSONObject thirdPartyMethodPermissionsJSON = (JSONObject) thirdPartyMethodPermissions.get(i);
        String methodName = JSONUtilities.decodeString(thirdPartyMethodPermissionsJSON, "methodName", Boolean.TRUE);  // TODO EVPRO-99 remove JSONUtilities
        ThirdPartyMethodAccessLevel thirdPartyMethodAccessLevel = new ThirdPartyMethodAccessLevel(thirdPartyMethodPermissionsJSON);
        thirdPartyMethodPermissionsMap.put(methodName, thirdPartyMethodAccessLevel);
      }

    authResponseCacheLifetimeInMinutes = jsonReader.decodeInteger("authResponseCacheLifetimeInMinutes");
    stockRefreshPeriod = jsonReader.decodeInteger("stockRefreshPeriod");
    periodicEvaluationCronEntry = jsonReader.decodeString("periodicEvaluationCronEntry");
    ucgEvaluationCronEntry = jsonReader.decodeString("ucgEvaluationCronEntry");

    //
    //  Reports
    //
    JSONObject reportManager = jsonReader.decodeJSONObject("reportManager");
    reportManagerZookeeperDir = JSONUtilities.decodeString(reportManager, "reportManagerZookeeperDir"); // TODO EVPRO-99 JSONUtilities
    reportManagerOutputPath = JSONUtilities.decodeString(reportManager, "reportManagerOutputPath");
    reportManagerFileDateFormat = JSONUtilities.decodeString(reportManager, "reportManagerFileDateFormat","yyyy-MM-dd_HH-mm-ss");
    reportManagerFileExtension = JSONUtilities.decodeString(reportManager, "reportManagerFileExtension");
    reportManagerCsvSeparator = JSONUtilities.decodeString(reportManager, "reportManagerCsvSeparator");
    reportManagerFieldSurrounder = JSONUtilities.decodeString(reportManager, "reportManagerFieldSurrounder");
    reportManagerMaxMessageLength = JSONUtilities.decodeInteger(reportManager, "reportManagerMaxMessageLength");
    reportManagerStreamsTempDir = JSONUtilities.decodeString(reportManager, "reportManagerStreamsTempDir");
    reportManagerTopicsCreationProperties = JSONUtilities.decodeString(reportManager, "reportManagerTopicsCreationProperties");
    journeysReportMaxParallelThreads = JSONUtilities.decodeInteger(reportManager, "journeysReportMaxParallelThreads");
    detailedrecordReportsArrearCount = JSONUtilities.decodeInteger(reportManager, "detailedrecordReportsArrearCount", 7);
    journeyReportsArrearCount = JSONUtilities.decodeInteger(reportManager, "journeyReportsArrearCount", 2);
    subscriberprofileReportsArrearCount = JSONUtilities.decodeInteger(reportManager, "subscriberprofileReportsArrearCount", 2);
    reportManagerContentDateFormat = JSONUtilities.decodeString(reportManager, "reportManagerContentDateFormat", "yyyy-MM-dd'T'HH:mm:ssZZZZ");

    if (reportManagerFieldSurrounder.length() > 1) {
      throw new ServerRuntimeException("reportManagerFieldSurrounder is not a single character, this would lead to errors in the reports, truncating, please fix this : " + reportManagerFieldSurrounder);
    }
    
    guiConfigurationSoftRetentionDays = jsonReader.decodeInteger("guiConfigurationSoftRetentionDays");
    guiConfigurationRetentionDays = jsonReader.decodeInteger("guiConfigurationRetentionDays");
    guiConfigurationCleanerThreadPeriodMs = jsonReader.decodeInteger("guiConfigurationCleanerThreadPeriodSeconds") * 1000;
    guiConfigurationInitialConsumerMaxPollRecords  = jsonReader.decodeInteger("guiConfigurationInitialConsumerMaxPollRecords");
    guiConfigurationInitialConsumerMaxFetchBytes = jsonReader.decodeInteger("guiConfigurationInitialConsumerMaxFetchBytes");
    
    //
    // configuration for extracts
    //
    JSONObject extractManager = jsonReader.decodeJSONObject("extractManager");
    extractManagerZookeeperDir = JSONUtilities.decodeString(extractManager, "extractManagerZookeeperDir"); // TODO EVPRO-99 JSONUtilities
    extractManagerOutputPath = JSONUtilities.decodeString(extractManager, "extractManagerOutputPath");
    extractManagerDateFormat = JSONUtilities.decodeString(extractManager, "extractManagerDateFormat");
    extractManagerFileExtension = JSONUtilities.decodeString(extractManager, "extractManagerFileExtension");
    extractManagerCsvSeparator = JSONUtilities.decodeString(extractManager, "extractManagerCsvSeparator");
    extractManagerFieldSurrounder = JSONUtilities.decodeString(extractManager, "extractManagerFieldSurrounder");
    if (extractManagerFieldSurrounder.length() > 1) {
      throw new ServerRuntimeException("extractManagerFieldSurrounder is not a single character, this would lead to errors in the extracts, truncating, please fix this : " + extractManagerFieldSurrounder);
    }
    
    customerMetaData = new CustomerMetaData(jsonReader.decodeJSONObject("customerMetaData"));

    uploadedFileSeparator = jsonReader.decodeString("uploadedFileSeparator");
    APIresponseDateFormat = jsonReader.decodeString("APIresponseDateFormat");

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
    
    hourlyReportCronEntryString = jsonReader.decodeString("hourlyReportCronEntryString");
    dailyReportCronEntryString = jsonReader.decodeString("dailyReportCronEntryString");
    weeklyReportCronEntryString = jsonReader.decodeString("weeklyReportCronEntryString");
    monthlyReportCronEntryString = jsonReader.decodeString("monthlyReportCronEntryString");
    enableEvaluateTargetRandomness = jsonReader.decodeBoolean("enableEvaluateTargetRandomness");
    
    //
    // conf for elasticsearch & voucher
    //
    // we won't deliver a voucher that expiry in less than X hours from now :
    minExpiryDelayForVoucherDeliveryInHours = jsonReader.decodeInteger("minExpiryDelayForVoucherDeliveryInHours");
    // the bulk size when importing voucher file into ES
    importVoucherFileBulkSize = jsonReader.decodeInteger("importVoucherFileBulkSize");
    // the cache cleaner frequency in seconds for caching voucher with 0 stock from ES, and shrinking back "auto adjust concurrency number"
    voucherESCacheCleanerFrequencyInSec = jsonReader.decodeInteger("voucherESCacheCleanerFrequencyInSec");
    // an approximation of number of total concurrent process tyring to allocate Voucher in // to ES, but should not need to configure, algo should auto-adjust this
    numberConcurrentVoucherAllocationToES = jsonReader.decodeInteger("numberConcurrentVoucherAllocationToES");

    //
    // conf for propensity service
    //
    // period in ms global propensity state will be read from zookeeper :
    propensityReaderRefreshPeriodMs = jsonReader.decodeInteger("propensityReaderRefreshPeriodMs");
    // period in ms local propensity state will be write to zookeeper :
    propensityWriterRefreshPeriodMs = jsonReader.decodeInteger("propensityWriterRefreshPeriodMs");

    enableContactPolicyProcessing = jsonReader.decodeBoolean("enableContactPolicyProcessing");
    recurrentCampaignCreationDaysRange = jsonReader.decodeInteger("recurrentCampaignCreationDaysRange");
    
    nodesTransitionsHistorySize = jsonReader.decodeInteger("nodesTransitionsHistorySize");
    
    String firstDayOfTheWeekString = jsonReader.decodeString("firstDayOfTheWeek");
    switch (firstDayOfTheWeekString.trim().toUpperCase()) {
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

    addSubscribersToUcgByCounting = jsonReader.decodeBoolean("addSubscribersToUcgByCounting");
    
  }

  
  /*****************************************
  *
  * Utils
  *
  *****************************************/
  public static Class<? extends Deployment> getProjectDeploymentClass()
  {
    try
      {
        Class<? extends Deployment> projectDeploymentClass = (Class<? extends Deployment>) Class.forName(projectDeploymentClassName);
        return projectDeploymentClass;
      }
    catch (ClassNotFoundException e)
      {
        throw new RuntimeException(e);
      }
  }
  
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
  
  public static int getConnectTaskInitialWait(String connectorName)
  {
    int res;
    ConnectTaskConfiguration connectTaskConfig = connectTask.get(connectorName);
    if (connectTaskConfig != null) {
      res = connectTaskConfig.getInitialWait();
    } else {
      res = connectTaskConfigDefault.getInitialWait();
    }
    return res;
  }

  public static int getConnectTaskRetries(String connectorName)
  {
    int res;
    ConnectTaskConfiguration connectTaskConfig = connectTask.get(connectorName);
    if (connectTaskConfig != null) {
      res = connectTaskConfig.getRetries();
    } else {
      res = connectTaskConfigDefault.getRetries();
    }
    return res;
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
  
  /**
   * Comments are root fields with ".comments" at the end.
   * /!\ Therefore, comments cannot be put inside objects. 
   */
  private static JSONObject removeComments(JSONObject brutJsonRoot) 
  {
    ArrayList<String> keysToRemove = new ArrayList<>();
    for(Object key : brutJsonRoot.keySet())
      {
        if(key instanceof String && ((String) key).endsWith(".comment"))
          {
            keysToRemove.add((String)key);
          }
      }
    // remove comments
    for(String key : keysToRemove)
      {
        brutJsonRoot.remove(key);
      }
    
    return brutJsonRoot;
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
    ArrayList<String> keysToRemove = new ArrayList<>();
    for(Object key : brutJsonRoot.keySet())
      {
        if(key instanceof String && ((String) key).startsWith("tenantConfiguration"))
          {
            JSONObject tenantConfiguration = (JSONObject) brutJsonRoot.get(key);
            tenantSpecificConfigurations.add(tenantConfiguration);
            keysToRemove.add((String)key);
          }
      }
    // remove tenantSpecific configurations
    for(String key : keysToRemove)
      {
        brutJsonRoot.remove(key);
      }
    
    // also add fake tenant 0 (for static configurations of Deployment)
    JSONObject tenant0Configuration = new JSONObject();
    tenant0Configuration.put("tenantID", 0);
    tenant0Configuration.put("name", "global");
    tenant0Configuration.put("description", "Global");
    tenant0Configuration.put("display", "Global");
    tenant0Configuration.put("languageID", "1");
    tenant0Configuration.put("isDefault", false);
    tenantSpecificConfigurations.add(tenant0Configuration);
    
    //
    // now analyse the configurations of tenants
    //
    for(JSONObject tenantSpecificConfiguration : tenantSpecificConfigurations)
      {
        // Close to a deep copy (except for primitive values that need to be read only)
        JSONObject brutJSONCopy = JSONUtilities.jsonCopyMap(brutJsonRoot);
        
        //
        // now merge the tenant specific configuration with the brut configuration, so that we have the effective JSONRoot configuration for the current tenant
        //
        
        JSONObject tenantJSON = JSONUtilities.jsonMergerOverrideOrAdd(brutJSONCopy,tenantSpecificConfiguration,(brut,tenant) -> brut.get("id")!=null && tenant.get("id")!=null && brut.get("id").equals(tenant.get("id")));//json object in array match thanks to "id" field only
        
        //
        // get the tenantID
        //
        
        int tenantID = JSONUtilities.decodeInteger(tenantJSON, "tenantID", true);
        String tenantName = JSONUtilities.decodeString(tenantJSON, "name", true);
        String tenantDescription = JSONUtilities.decodeString(tenantJSON, "description", true);
        String tenantDisplay = JSONUtilities.decodeString(tenantJSON, "display", true);
        String tenantLanguageID = JSONUtilities.decodeString(tenantJSON, "languageID", true);
        boolean tenantIsDefault = JSONUtilities.decodeBoolean(tenantJSON, "isDefault");
        
        tenantJSON.remove("tenantID");
        tenantJSON.remove("name");
        tenantJSON.remove("description");
        tenantJSON.remove("display");
        tenantJSON.remove("languageID");
        tenantJSON.remove("isDefault");
        
        Tenant tenant = new Tenant(tenantID, tenantName, tenantDescription, tenantDisplay, tenantLanguageID, tenantIsDefault);
        
        //
        // let reference the tenantJSONObject configuration available for all subclasses of Deployment
        //
        
        jsonConfigPerTenant.put(tenant, tenantJSON);
      }
    // now let ensure exactly one tenant is the default... consider the fist one default as the default
    Tenant tenantDefault = null;
    Tenant firstTenant = null;
    for(Tenant t : jsonConfigPerTenant.keySet())
      {
        if(firstTenant == null) { firstTenant = t;}
        if(t.isDefault()) 
          { 
            if(tenantDefault == null)
              {
                tenantDefault = t;
              }
            else 
              {
                log.warn("Tenant " + t.getName() + " is defined as default but " + tenantDefault.getName() + " is already default, so ignore default for " + t.getName());
                t.setDefault(false);
              }
          }
      }
    if(tenantDefault == null)
      {
        firstTenant.setDefault(true);
        log.warn("DeploymentCommon.buildJsonPerTenant No default tenant has been define, we consider tenant " + firstTenant.getName() + " as default");
      }
    defaultTenant = tenantDefault;
    
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
