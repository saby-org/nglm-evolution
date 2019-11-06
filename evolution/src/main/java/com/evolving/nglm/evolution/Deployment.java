/*****************************************************************************
*
*  Deployment.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SuspenseProcessEventConfiguration;
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration.EventRule;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.SubscriberProfile.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Deployment
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
  //  data
  //

  private static String subscriberGroupLoaderAlternateID;
  private static String getCustomerAlternateID;
  private static boolean subscriberGroupLoaderAutoProvision;
  private static String criterionFieldRetrieverClassName;
  private static String evolutionEngineExtensionClassName;
  private static String guiManagerExtensionClassName;
  private static String subscriberProfileClassName;
  private static String extendedSubscriberProfileClassName;
  private static String evolutionEngineExternalAPIClassName;
  private static Map<String,EvolutionEngineEventDeclaration> evolutionEngineEvents = new LinkedHashMap<String,EvolutionEngineEventDeclaration>();
  private static Map<String, CriterionField> profileChangeDetectionCriterionFields = new HashMap<>();
  private static Map<String, CriterionField> profileChangeGeneratedCriterionFields = new HashMap<>();
  private static boolean enableProfileSegmentChange;
  private static String emptyTopic;
  private static String journeyTopic;
  private static String journeyTemplateTopic;
  private static String segmentationDimensionTopic;
  private static String pointTopic;
  private static String pointFulfillmentRequestTopic;
  private static String pointFulfillmentResponseTopic;
  private static String pointFufillmentRepartitioningTopic;
  private static String offerTopic;
  private static String reportTopic;
  private static String paymentMeanTopic;
  private static String presentationStrategyTopic;
  private static String scoringStrategyTopic;
  private static String callingChannelTopic;
  private static String salesChannelTopic;
  private static String supplierTopic;
  private static String partnerTopic;
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
  private static String subscriberMessageTemplateTopic;
  private static String guiAuditTopic;
  private static String subscriberGroupTopic;
  private static String subscriberGroupAssignSubscriberIDTopic;
  private static String subscriberGroupEpochTopic;
  private static String ucgStateTopic;
  private static String renamedProfileCriterionFieldTopic;
  private static String timedEvaluationTopic;
  private static String subscriberProfileForceUpdateTopic;
  private static String subscriberStateChangeLog;
  private static String subscriberStateChangeLogTopic;
  private static String extendedSubscriberProfileChangeLog;
  private static String extendedSubscriberProfileChangeLogTopic;
  private static String subscriberHistoryChangeLog;
  private static String subscriberHistoryChangeLogTopic;
  private static String journeyRequestTopic;
  private static String journeyResponseTopic;
  private static String loyaltyProgramRequestTopic;
  private static String loyaltyProgramResponseTopic;
  private static String journeyStatisticTopic;
  private static String journeyMetricTopic;
  private static String deliverableSourceTopic;
  private static String presentationLogTopic;
  private static String acceptanceLogTopic;
  private static String propensityLogTopic;
  private static String propensityStateChangeLog;
  private static String propensityStateChangeLogTopic;
  private static String propensityRepartitioningTopic;
  private static String profileLoyaltyProgramChangeEventTopic;
  private static String profileChangeEventTopic;
  private static String profileSegmentChangeEventTopic;
  private static String tokenRedeemedTopic;
  private static String bonusDeliveryTopic;
  private static String offerDeliveryTopic;
  private static String messageDeliveryTopic;
  private static int propensityInitialisationPresentationThreshold;
  private static int propensityInitialisationDurationInDaysThreshold;
  private static String journeyTrafficChangeLog;
  private static String journeyTrafficChangeLogTopic;
  private static String subscriberProfileRegistrySubject;
  private static CompressionType subscriberProfileCompressionType;
  private static int journeyTrafficArchivePeriodInSeconds;
  private static int journeyTrafficArchiveMaxNumberOfPeriods;
  private static PropensityRule propensityRule;
  private static Map<String,SupportedLanguage> supportedLanguages = new LinkedHashMap<String,SupportedLanguage>();
  private static Map<String,ExternalAPITopic> externalAPITopics = new LinkedHashMap<String,ExternalAPITopic>();
  private static String baseLanguageID;
  private static Map<String,SupportedCurrency> supportedCurrencies = new LinkedHashMap<String,SupportedCurrency>();
  private static Map<String,SupportedTimeUnit> supportedTimeUnits = new LinkedHashMap<String,SupportedTimeUnit>();
  private static Map<String,SupportedTokenCodesFormat> supportedTokenCodesFormats = new LinkedHashMap<String,SupportedTokenCodesFormat>();
  private static Map<String,ServiceType> serviceTypes = new LinkedHashMap<String,ServiceType>();
  private static Map<String,SupportedShortCode> supportedShortCodes = new LinkedHashMap<String,SupportedShortCode>();
  private static Map<String,SupportedEmailAddress> supportedEmailAddresses = new LinkedHashMap<String,SupportedEmailAddress>();
  private static Map<String,SupportedRelationship> supportedRelationships = new LinkedHashMap<String,SupportedRelationship>();
  private static Map<String,CallingChannelProperty> callingChannelProperties = new LinkedHashMap<String,CallingChannelProperty>();
  private static Map<String,CatalogCharacteristicUnit> catalogCharacteristicUnits = new LinkedHashMap<String,CatalogCharacteristicUnit>();
  private static Map<String,PartnerType> partnerTypes = new LinkedHashMap<String,PartnerType>();
  private static Map<String,BillingMode> billingModes = new LinkedHashMap<String,BillingMode>();
  private static JSONArray initialCallingChannelsJSONArray = null;
  private static JSONArray initialSalesChannelsJSONArray = null;
  private static JSONArray initialSuppliersJSONArray = null;
  private static JSONArray initialPartnersJSONArray = null;
  private static JSONArray initialProductsJSONArray = null;
  private static JSONArray initialReportsJSONArray = null;
  private static JSONArray initialCatalogCharacteristicsJSONArray = null;
  private static JSONArray initialContactPoliciesJSONArray = null;
  private static JSONArray initialJourneyTemplatesJSONArray = null;
  private static JSONArray initialJourneyObjectivesJSONArray = null;
  private static JSONArray initialOfferObjectivesJSONArray = null;
  private static JSONArray initialProductTypesJSONArray = null;  
  private static JSONArray initialTokenTypesJSONArray = null;
  private static JSONArray initialScoringTypesJSONArray = null;
  private static JSONArray initialSegmentationDimensionsJSONArray = null;
  private static JSONArray initialCommunicationChannelsJSONArray = null;
  private static boolean generateSimpleProfileDimensions;
  private static Map<String,SupportedDataType> supportedDataTypes = new LinkedHashMap<String,SupportedDataType>();
  private static Map<String,JourneyMetricDeclaration> journeyMetricDeclarations = new LinkedHashMap<String,JourneyMetricDeclaration>();
  private static Map<String,CriterionField> profileCriterionFields = new LinkedHashMap<String,CriterionField>();
  private static Map<String,CriterionField> extendedProfileCriterionFields = new LinkedHashMap<String,CriterionField>();
  private static Map<String,CriterionField> presentationCriterionFields = new LinkedHashMap<String,CriterionField>();
  private static List<EvaluationCriterion> universalControlGroupCriteria = new ArrayList<EvaluationCriterion>();
  private static List<EvaluationCriterion> controlGroupCriteria = new ArrayList<EvaluationCriterion>();
  private static Map<String,OfferCategory> offerCategories = new LinkedHashMap<String,OfferCategory>();
  private static Map<String,OfferProperty> offerProperties = new LinkedHashMap<String,OfferProperty>();
  private static Map<String,ScoringEngine> scoringEngines = new LinkedHashMap<String,ScoringEngine>();
  private static Map<String,OfferOptimizationAlgorithm> offerOptimizationAlgorithms = new LinkedHashMap<String,OfferOptimizationAlgorithm>();
  private static Map<String,ScoringType> scoringTypes = new LinkedHashMap<String,ScoringType>();
  private static Map<String,DNBOMatrixVariable> dnboMatrixVariables = new LinkedHashMap<String,DNBOMatrixVariable>();
  private static Map<String,DeliveryManagerDeclaration> deliveryManagers = new LinkedHashMap<String,DeliveryManagerDeclaration>();
  private static Map<String,DeliveryManagerDeclaration> fulfillmentProviders = new LinkedHashMap<String,DeliveryManagerDeclaration>();
  private static Map<String,DeliveryManagerAccount> deliveryManagerAccounts = new HashMap<String,DeliveryManagerAccount>();
  private static int journeyDefaultTargetingWindowDuration;
  private static TimeUnit journeyDefaultTargetingWindowUnit;
  private static boolean journeyDefaultTargetingWindowRoundUp;
  private static List<EvaluationCriterion> journeyUniversalEligibilityCriteria = new ArrayList<EvaluationCriterion>();
  private static Map<String,NodeType> nodeTypes = new LinkedHashMap<String,NodeType>();
  private static Map<String,ToolboxSection> journeyToolbox = new LinkedHashMap<String,ToolboxSection>();
  private static Map<String,ToolboxSection> campaignToolbox = new LinkedHashMap<String,ToolboxSection>();
  private static Map<String,ThirdPartyMethodAccessLevel> thirdPartyMethodPermissionsMap = new LinkedHashMap<String,ThirdPartyMethodAccessLevel>();
  private static Map<String,NotificationDailyWindows> notificationTimeWindowsMap = new LinkedHashMap<String,NotificationDailyWindows>();
  private static Integer authResponseCacheLifetimeInMinutes = null;
  private static int stockRefreshPeriod;
  private static String periodicEvaluationCronEntry;
  private static String ucgEvaluationCronEntry;
  private static Map<String,Report> initialReports = new LinkedHashMap<>();
  private static String reportManagerZookeeperDir;
  private static String reportManagerOutputPath;
  private static String reportManagerDateFormat;
  private static String reportManagerFileExtension;
  private static String reportManagerStreamsTempDir;
  private static String reportManagerCsvSeparator;
  private static String uploadedFileSeparator;
  private static CustomerMetaData customerMetaData = null;
  private static String APIresponseDateFormat;
  private static String uploadedFileTopic;
  private static String targetTopic;
  private static String communicationChannelTopic;
  public static String communicationChannelBlackoutTopic;
  public static String loyaltyProgramTopic;
  private static int ucgEngineESConnectTimeout;
  private static int ucgEngineESSocketTimeout;
  private static int ucgEngineESMasRetryTimeout;
  private static String exclusionInclusionTargetTopic;
  private static String dnboMatrixTopic;
  private static String segmentContactPolicyTopic;
  private static String dynamicEventDeclarationsTopic;
  private static String elasticSearchDateFormat;
  private static int elasticSearchScrollSize;
  private static String criterionFieldAvailableValuesTopic;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  //
  //  core accessors
  //
  
  public static String getZookeeperRoot() { return com.evolving.nglm.core.Deployment.getZookeeperRoot(); }
  public static String getZookeeperConnect() { return com.evolving.nglm.core.Deployment.getZookeeperConnect(); }
  public static String getBrokerServers() { return com.evolving.nglm.core.Deployment.getBrokerServers(); }
  public static JSONObject getJSONRoot() { return com.evolving.nglm.core.Deployment.getJSONRoot(); }
  public static String getBaseTimeZone() { return com.evolving.nglm.core.Deployment.getBaseTimeZone(); }
  public static String getBaseLanguage() { return com.evolving.nglm.core.Deployment.getBaseLanguage(); }
  public static String getBaseCountry() { return com.evolving.nglm.core.Deployment.getBaseCountry(); }
  public static boolean getGenerateNumericIDs() { return com.evolving.nglm.core.Deployment.getGenerateNumericIDs(); }
  public static String getRedisSentinels() { return com.evolving.nglm.core.Deployment.getRedisSentinels(); }
  public static String getAssignSubscriberIDsTopic() { return com.evolving.nglm.core.Deployment.getAssignSubscriberIDsTopic(); }
  public static String getAssignExternalSubscriberIDsTopic() { return com.evolving.nglm.core.Deployment.getAssignExternalSubscriberIDsTopic(); }
  public static String getRecordSubscriberIDTopic() { return com.evolving.nglm.core.Deployment.getRecordSubscriberIDTopic(); }
  public static String getExternalSubscriberID() { return com.evolving.nglm.core.Deployment.getExternalSubscriberID(); }
  public static String getSubscriberTraceControlAlternateID() { return com.evolving.nglm.core.Deployment.getSubscriberTraceControlAlternateID(); }
  public static boolean getSubscriberTraceControlAutoProvision() { return com.evolving.nglm.core.Deployment.getSubscriberTraceControlAutoProvision(); }
  public static String getSubscriberTraceControlTopic() { return com.evolving.nglm.core.Deployment.getSubscriberTraceControlTopic(); }
  public static String getSubscriberTraceControlAssignSubscriberIDTopic() { return com.evolving.nglm.core.Deployment.getSubscriberTraceControlAssignSubscriberIDTopic(); }
  public static String getSubscriberTraceTopic() { return com.evolving.nglm.core.Deployment.getSubscriberTraceTopic(); }
  public static String getSuspenseCronEntry() { return com.evolving.nglm.core.Deployment.getSuspenseCronEntry(); }
  public static String getSuspenseTopic() { return com.evolving.nglm.core.Deployment.getSuspenseTopic(); }
  public static String getSuspenseAuditTopic() { return com.evolving.nglm.core.Deployment.getSuspenseAuditTopic(); }
  public static Map<String,SuspenseProcessEventConfiguration> getSuspenseProcessEventConfiguration() { return com.evolving.nglm.core.Deployment.getSuspenseProcessEventConfiguration(); }
  public static Map<String, AlternateID> getAlternateIDs() { return com.evolving.nglm.core.Deployment.getAlternateIDs(); }
  
  //
  //  evolution accessors
  //

  public static boolean getRegressionMode() { return System.getProperty("use.regression","0").equals("1"); }
  public static String getSubscriberProfileEndpoints() { return System.getProperty("subscriberprofile.endpoints",""); }
  public static String getSubscriberGroupLoaderAlternateID() { return subscriberGroupLoaderAlternateID; }
  public static String getGetCustomerAlternateID() { return getCustomerAlternateID; }
  public static boolean getSubscriberGroupLoaderAutoProvision() { return subscriberGroupLoaderAutoProvision; }
  public static String getCriterionFieldRetrieverClassName() { return criterionFieldRetrieverClassName; }
  public static String getEvolutionEngineExtensionClassName() { return evolutionEngineExtensionClassName; }
  public static String getGUIManagerExtensionClassName() { return guiManagerExtensionClassName; }
  public static String getSubscriberProfileClassName() { return subscriberProfileClassName; }
  public static String getExtendedSubscriberProfileClassName() { return extendedSubscriberProfileClassName; }
  public static Map<String,EvolutionEngineEventDeclaration> getEvolutionEngineEvents() { return evolutionEngineEvents; }
  public static boolean getEnableProfileSegmentChange() { return enableProfileSegmentChange; }
  public static String getEmptyTopic() { return emptyTopic; }
  public static String getJourneyTopic() { return journeyTopic; }
  public static String getJourneyTemplateTopic() { return journeyTemplateTopic; }
  public static String getSegmentationDimensionTopic() { return segmentationDimensionTopic; }
  public static String getPointTopic() { return pointTopic; }
  public static String getPointFulfillmentRequestTopic() { return pointFulfillmentRequestTopic; }
  public static String getPointFulfillmentResponseTopic() { return pointFulfillmentResponseTopic; }
  public static String getPointFufillmentRepartitioningTopic() { return pointFufillmentRepartitioningTopic; }
  public static String getOfferTopic() { return offerTopic; }
  public static String getReportTopic() { return reportTopic; }
  public static String getPaymentMeanTopic() { return paymentMeanTopic; }
  public static String getPresentationStrategyTopic() { return presentationStrategyTopic; }
  public static String getScoringStrategyTopic() { return scoringStrategyTopic; }
  public static String getCallingChannelTopic() { return callingChannelTopic; }
  public static String getSalesChannelTopic() { return salesChannelTopic; }
  public static String getSupplierTopic() { return supplierTopic; }
  public static String getPartnerTopic() { return partnerTopic; }
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
  public static String getSubscriberMessageTemplateTopic() { return subscriberMessageTemplateTopic; }
  public static String getGUIAuditTopic() { return guiAuditTopic; }
  public static String getSubscriberGroupTopic() { return subscriberGroupTopic; }
  public static String getSubscriberGroupAssignSubscriberIDTopic() { return subscriberGroupAssignSubscriberIDTopic; }
  public static String getSubscriberGroupEpochTopic() { return subscriberGroupEpochTopic; }
  public static String getUCGStateTopic() { return ucgStateTopic; }
  public static String getRenamedProfileCriterionFieldTopic() { return renamedProfileCriterionFieldTopic; }
  public static String getTimedEvaluationTopic() { return timedEvaluationTopic; }
  public static String getSubscriberProfileForceUpdateTopic() { return subscriberProfileForceUpdateTopic; }
  public static String getSubscriberStateChangeLog() { return subscriberStateChangeLog; }
  public static String getSubscriberStateChangeLogTopic() { return subscriberStateChangeLogTopic; }
  public static String getExtendedSubscriberProfileChangeLog() { return extendedSubscriberProfileChangeLog; }
  public static String getExtendedSubscriberProfileChangeLogTopic() { return extendedSubscriberProfileChangeLogTopic; }
  public static String getSubscriberHistoryChangeLog() { return subscriberHistoryChangeLog; }
  public static String getSubscriberHistoryChangeLogTopic() { return subscriberHistoryChangeLogTopic; }
  public static String getJourneyRequestTopic() { return journeyRequestTopic; }
  public static String getJourneyResponseTopic() { return journeyResponseTopic; }
  public static String getLoyaltyProgramRequestTopic() { return loyaltyProgramRequestTopic; }
  public static String getLoyaltyProgramResponseTopic() { return loyaltyProgramResponseTopic; }
  public static String getJourneyStatisticTopic() { return journeyStatisticTopic; }
  public static String getJourneyMetricTopic() { return journeyMetricTopic; }
  public static String getDeliverableSourceTopic() { return deliverableSourceTopic; }
  public static String getPresentationLogTopic() { return presentationLogTopic; }
  public static String getAcceptanceLogTopic() { return acceptanceLogTopic; }
  public static String getPropensityLogTopic() { return propensityLogTopic; }
  public static String getPropensityStateChangeLog() { return propensityStateChangeLog; }
  public static String getPropensityStateChangeLogTopic() { return propensityStateChangeLogTopic; }
  public static String getPropensityRepartitioningTopic() { return propensityRepartitioningTopic; }
  public static String getProfileChangeEventTopic() { return profileChangeEventTopic;}
  public static String getProfileSegmentChangeEventTopic() { return profileSegmentChangeEventTopic;}
  public static String getProfileLoyaltyProgramChangeEventTopic() { return profileLoyaltyProgramChangeEventTopic;}
  public static String getTokenRedeemedTopic() { return tokenRedeemedTopic; }
  public static String getBonusDeliveryTopic() { return bonusDeliveryTopic; }
  public static String getOfferDeliveryTopic() { return offerDeliveryTopic; }
  public static String getMessageDeliveryTopic() { return messageDeliveryTopic; }
  public static int getPropensityInitialisationPresentationThreshold() { return propensityInitialisationPresentationThreshold; }
  public static int getPropensityInitialisationDurationInDaysThreshold() { return propensityInitialisationDurationInDaysThreshold; }
  public static String getJourneyTrafficChangeLog() { return journeyTrafficChangeLog; }
  public static String getJourneyTrafficChangeLogTopic() { return journeyTrafficChangeLogTopic; }
  public static String getSubscriberProfileRegistrySubject() { return subscriberProfileRegistrySubject; }
  public static CompressionType getSubscriberProfileCompressionType() { return subscriberProfileCompressionType; }
  public static int getJourneyTrafficArchivePeriodInSeconds() { return journeyTrafficArchivePeriodInSeconds; }
  public static int getJourneyTrafficArchiveMaxNumberOfPeriods() { return journeyTrafficArchiveMaxNumberOfPeriods; }
  public static PropensityRule getPropensityRule() { return propensityRule; }
  public static Map<String,SupportedLanguage> getSupportedLanguages() { return supportedLanguages; }
  public static Map<String,ExternalAPITopic> getExternalAPITopics() { return externalAPITopics; }
  public static String getBaseLanguageID() { return baseLanguageID; }
  public static Map<String,SupportedCurrency> getSupportedCurrencies() { return supportedCurrencies; }
  public static Map<String,SupportedTimeUnit> getSupportedTimeUnits() { return supportedTimeUnits; }
  public static Map<String,SupportedTokenCodesFormat> getSupportedTokenCodesFormats() { return supportedTokenCodesFormats; }
  public static Map<String,ServiceType> getServiceTypes() { return serviceTypes; }
  public static Map<String,SupportedShortCode> getSupportedShortCodes() { return supportedShortCodes; }
  public static Map<String,SupportedEmailAddress> getSupportedEmailAddresses() { return supportedEmailAddresses; }
  public static Map<String,SupportedRelationship> getSupportedRelationships() { return supportedRelationships; }
  public static Map<String,CallingChannelProperty> getCallingChannelProperties() { return callingChannelProperties; }
  public static Map<String,CatalogCharacteristicUnit> getCatalogCharacteristicUnits() { return catalogCharacteristicUnits; }
  public static JSONArray getInitialCallingChannelsJSONArray() { return initialCallingChannelsJSONArray; }
  public static JSONArray getInitialSalesChannelsJSONArray() { return initialSalesChannelsJSONArray; }
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
  public static JSONArray getInitialSegmentationDimensionsJSONArray() { return initialSegmentationDimensionsJSONArray; }
  public static JSONArray getInitialCommunicationChannelsJSONArray() { return initialCommunicationChannelsJSONArray; }
  public static boolean getGenerateSimpleProfileDimensions() { return generateSimpleProfileDimensions; }
  public static Map<String,SupportedDataType> getSupportedDataTypes() { return supportedDataTypes; }
  public static Map<String,JourneyMetricDeclaration> getJourneyMetricDeclarations() { return journeyMetricDeclarations; }
  public static Map<String,CriterionField> getProfileCriterionFields() { return profileCriterionFields; }
  public static Map<String,CriterionField> getExtendedProfileCriterionFields() { return extendedProfileCriterionFields; }
  public static Map<String, CriterionField> getProfileChangeDetectionCriterionFields() { return profileChangeDetectionCriterionFields; }
  public static Map<String, CriterionField> getProfileChangeGeneratedCriterionFields() { return profileChangeGeneratedCriterionFields; }
  public static Map<String,CriterionField> getPresentationCriterionFields() { return presentationCriterionFields; }
  public static List<EvaluationCriterion> getUniversalControlGroupCriteria() { return universalControlGroupCriteria; }
  public static List<EvaluationCriterion> getControlGroupCriteria() { return controlGroupCriteria; }
  public static Map<String,OfferCategory> getOfferCategories() { return offerCategories; }
  public static Map<String,OfferProperty> getOfferProperties() { return offerProperties; }
  public static Map<String,ScoringEngine> getScoringEngines() { return scoringEngines; }
  public static Map<String,OfferOptimizationAlgorithm> getOfferOptimizationAlgorithms() { return offerOptimizationAlgorithms; }
  public static Map<String,ScoringType> getScoringTypes() { return scoringTypes; }
  public static Map<String,DNBOMatrixVariable> getDNBOMatrixVariables() { return dnboMatrixVariables; }
  public static Map<String,DeliveryManagerDeclaration> getDeliveryManagers() { return deliveryManagers; }
  public static Map<String,DeliveryManagerDeclaration> getFulfillmentProviders() { return fulfillmentProviders; }
  public static Map<String,DeliveryManagerAccount> getDeliveryManagerAccounts() { return deliveryManagerAccounts; }
  public static int getJourneyDefaultTargetingWindowDuration() { return journeyDefaultTargetingWindowDuration; }
  public static TimeUnit getJourneyDefaultTargetingWindowUnit() { return journeyDefaultTargetingWindowUnit; }
  public static boolean getJourneyDefaultTargetingWindowRoundUp() { return journeyDefaultTargetingWindowRoundUp; }
  public static List<EvaluationCriterion> getJourneyUniversalEligibilityCriteria() { return journeyUniversalEligibilityCriteria; }
  public static Map<String,NodeType> getNodeTypes() { return nodeTypes; }
  public static Map<String,ToolboxSection> getJourneyToolbox() { return journeyToolbox; }
  public static Map<String,ToolboxSection> getCampaignToolbox() { return campaignToolbox; }
  public static Map<String,ThirdPartyMethodAccessLevel> getThirdPartyMethodPermissionsMap() { return thirdPartyMethodPermissionsMap; }
  public static Integer getAuthResponseCacheLifetimeInMinutes() { return authResponseCacheLifetimeInMinutes; }
  public static int getStockRefreshPeriod() { return stockRefreshPeriod; }
  public static String getPeriodicEvaluationCronEntry() { return periodicEvaluationCronEntry; }
  public static String getUCGEvaluationCronEntry() { return ucgEvaluationCronEntry; }
  public static Map<String,Report> getInitialReports() { return initialReports; }
  public static String getReportManagerZookeeperDir() { return reportManagerZookeeperDir; }
  public static String getReportManagerOutputPath() { return reportManagerOutputPath; }
  public static String getReportManagerDateFormat() { return reportManagerDateFormat; }
  public static String getReportManagerFileExtension() { return reportManagerFileExtension; }
  public static String getReportManagerCsvSeparator() { return reportManagerCsvSeparator; }
  public static String getUploadedFileSeparator() { return uploadedFileSeparator; }
  public static String getReportManagerStreamsTempDir() { return reportManagerStreamsTempDir; }
  public static CustomerMetaData getCustomerMetaData() { return customerMetaData; }
  public static String getAPIresponseDateFormat() { return APIresponseDateFormat; }
  public static String getUploadedFileTopic() { return uploadedFileTopic; }
  public static String getTargetTopic() { return targetTopic; }
  public static Map<String,NotificationDailyWindows> getNotificationDailyWindows() { return notificationTimeWindowsMap; }
  public static String getCommunicationChannelTopic() { return communicationChannelTopic; }
  public static String getCommunicationChannelBlackoutTopic() { return communicationChannelBlackoutTopic; } 
  public static String getLoyaltyProgramTopic() { return loyaltyProgramTopic; } 
  public static int getUcgEngineESConnectTimeout() { return ucgEngineESConnectTimeout; }
  public static int getUcgEngineESSocketTimeout(){ return ucgEngineESSocketTimeout; }
  public static int getUcgEngineESMasRetryTimeout() { return ucgEngineESMasRetryTimeout; }
  public static String getExclusionInclusionTargetTopic() { return exclusionInclusionTargetTopic; }
  public static String getDNBOMatrixTopic() { return dnboMatrixTopic; }
  public static String getSegmentContactPolicyTopic() { return segmentContactPolicyTopic; }
  public static String getDynamicEventDeclarationsTopic() { return dynamicEventDeclarationsTopic; }
  public static Map<String,PartnerType> getPartnerTypes() { return partnerTypes; }
  public static Map<String,BillingMode> getBillingModes() { return billingModes; }
  public static String getElasticSearchDateFormat() { return elasticSearchDateFormat; }
  public static int getElasticSearchScrollSize() {return elasticSearchScrollSize; }
  public static String getCriterionFieldAvailableValuesTopic() { return criterionFieldAvailableValuesTopic; }

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
  
  /*****************************************
  *
  *  getSupportedLanguageID
  *
  *****************************************/

  public static String getSupportedLanguageID(String language)
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
  *  static initialization
  *
  *****************************************/
  
  static
  {
    /*****************************************
    *
    *  super class
    *
    *****************************************/

    JSONObject jsonRoot = com.evolving.nglm.core.Deployment.getJSONRoot();
    
    /*****************************************
    *
    *  configuration
    *
    *****************************************/
    
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
    //  bonusDeliveryTopic
    //

    try
      {
        bonusDeliveryTopic = JSONUtilities.decodeString(jsonRoot, "bonusDeliveryTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  offerDeliveryTopic
    //

    try
      {
        offerDeliveryTopic = JSONUtilities.decodeString(jsonRoot, "offerDeliveryTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  messageDeliveryTopic
    //

    try
      {
        messageDeliveryTopic = JSONUtilities.decodeString(jsonRoot, "messageDeliveryTopic", true);
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
        
        /*****************************************
        *
        *  add Profile Change Event
        *
        *****************************************/

        // This is added when creating the profileCriterionFields
        
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
        NotificationDailyWindows notificationDailyWindows = new NotificationDailyWindows(JSONUtilities.decodeJSONObject(jsonRoot, "notificationDailyWindows", true));
        notificationTimeWindowsMap.put("0", notificationDailyWindows);
      }
    catch (GUIManagerException | JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  emptyTopic
    //

    try
      {
        emptyTopic = JSONUtilities.decodeString(jsonRoot, "emptyTopic", true);
      }
    catch (JSONUtilitiesException e)
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
    //  pointFulfillmentRequestTopic
    //

    try
      {
        pointFulfillmentRequestTopic = JSONUtilities.decodeString(jsonRoot, "pointFulfillmentRequestTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  pointFulfillmentResponseTopic
    //

    try
      {
        pointFulfillmentResponseTopic = JSONUtilities.decodeString(jsonRoot, "pointFulfillmentResponseTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  elasticSearchDateFormat
    //

    try
      {
        elasticSearchDateFormat = JSONUtilities.decodeString(jsonRoot, "elasticSearchDateFormat", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  elasticSearchScrollSize
    //

    try
      {
        elasticSearchScrollSize = JSONUtilities.decodeInteger(jsonRoot, "elasticSearchScrollSize", 0);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  pointFufillmentRepartitioningTopic
    //

    try
      {
        pointFufillmentRepartitioningTopic = JSONUtilities.decodeString(jsonRoot, "pointFufillmentRepartitioningTopic", true);
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
    //  partnerTopic
    //

    try
      {
        partnerTopic = JSONUtilities.decodeString(jsonRoot, "partnerTopic", true);
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
    //  journeyRequestTopic
    //

    try
      {
        journeyRequestTopic = JSONUtilities.decodeString(jsonRoot, "journeyRequestTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  journeyResponseTopic
    //

    try
      {
        journeyResponseTopic = JSONUtilities.decodeString(jsonRoot, "journeyResponseTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  loyaltyProgramRequestTopic
    //

    try
      {
        loyaltyProgramRequestTopic = JSONUtilities.decodeString(jsonRoot, "loyaltyProgramRequestTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  loyaltyProgramResponseTopic
    //

    try
      {
        loyaltyProgramResponseTopic = JSONUtilities.decodeString(jsonRoot, "loyaltyProgramResponseTopic", true);
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
    //  deliverableSourceTopic
    //

    try
      {
        deliverableSourceTopic = JSONUtilities.decodeString(jsonRoot, "deliverableSourceTopic", true);
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
    //  propensityLogTopic
    //

    try
      {
        propensityLogTopic = JSONUtilities.decodeString(jsonRoot, "propensityLogTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  propensityStateChangeLog
    //

    try
      {
        propensityStateChangeLog = JSONUtilities.decodeString(jsonRoot, "propensityStateChangeLog", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  propensityStateChangeLogTopic
    //

    try
      {
        propensityStateChangeLogTopic = JSONUtilities.decodeString(jsonRoot, "propensityStateChangeLogTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  propensityRepartitioningTopic
    //

    try
      {
        propensityRepartitioningTopic = JSONUtilities.decodeString(jsonRoot, "propensityRepartitioningTopic", true);
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
    //  journeyTrafficChangeLog
    //

    try
      {
        journeyTrafficChangeLog = JSONUtilities.decodeString(jsonRoot, "journeyTrafficChangeLog", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  journeyTrafficChangeLogTopic
    //

    try
      {
        journeyTrafficChangeLogTopic = JSONUtilities.decodeString(jsonRoot, "journeyTrafficChangeLogTopic", true);
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
    //  subscriberProfileCompressionType
    //

    try
      {
        subscriberProfileCompressionType = CompressionType.fromStringRepresentation(JSONUtilities.decodeString(jsonRoot, "subscriberProfileCompressionType", "unknown"));
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    if (subscriberProfileCompressionType == CompressionType.Unknown) throw new ServerRuntimeException("unsupported compression type");

    //
    //  journeyTrafficArchivePeriodInSeconds
    //

    try
      {
        journeyTrafficArchivePeriodInSeconds = JSONUtilities.decodeInteger(jsonRoot, "journeyTrafficArchivePeriodInSeconds", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  journeyTrafficArchiveMaxNumberOfPeriods
    //

    try
      {
        journeyTrafficArchiveMaxNumberOfPeriods = JSONUtilities.decodeInteger(jsonRoot, "journeyTrafficArchiveMaxNumberOfPeriods", true);
      }
    catch (JSONUtilitiesException e)
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
    //  baseLanguageID
    //

    baseLanguageID = getSupportedLanguageID(Deployment.getBaseLanguage());

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
    //  serviceTypes
    //

    try
      {
        JSONArray serviceTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "serviceTypes", true);
        for (int i=0; i<serviceTypeValues.size(); i++)
          {
            JSONObject serviceTypeJSON = (JSONObject) serviceTypeValues.get(i);
            ServiceType serviceType = new ServiceType(serviceTypeJSON);
            serviceTypes.put(serviceType.getID(), serviceType);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  supportedShortCodes
    //

    try
      {
        JSONArray supportedShortCodeValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedShortCodes", new JSONArray());
        for (int i=0; i<supportedShortCodeValues.size(); i++)
          {
            JSONObject supportedShortCodeJSON = (JSONObject) supportedShortCodeValues.get(i);
            SupportedShortCode supportedShortCode = new SupportedShortCode(supportedShortCodeJSON);
            supportedShortCodes.put(supportedShortCode.getID(), supportedShortCode);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  supportedEmailAddresses
    //

    try
      {
        JSONArray supportedEmailAddressValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedEmailAddresses", new JSONArray());
        for (int i=0; i<supportedEmailAddressValues.size(); i++)
          {
            JSONObject supportedEmailAddressJSON = (JSONObject) supportedEmailAddressValues.get(i);
            SupportedEmailAddress supportedEmailAddress = new SupportedEmailAddress(supportedEmailAddressJSON);
            supportedEmailAddresses.put(supportedEmailAddress.getID(), supportedEmailAddress);
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
    //  initialSegmentationDimensionsJSONArray
    //

    initialSegmentationDimensionsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialSegmentationDimensions", new JSONArray());
    
    //
    // initialCommunicationChannelsJSONArray
    // 
    initialCommunicationChannelsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "communicationChannels", new JSONArray());

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
    //  journeyMetricDeclarations
    //

    try
      {
        JSONArray journeyMetricDeclarationValues = JSONUtilities.decodeJSONArray(jsonRoot, "journeyMetrics", new JSONArray());
        for (int i=0; i<journeyMetricDeclarationValues.size(); i++)
          {
            JSONObject journeyMetricDeclarationJSON = (JSONObject) journeyMetricDeclarationValues.get(i);
            JourneyMetricDeclaration journeyMetricDeclaration = new JourneyMetricDeclaration(journeyMetricDeclarationJSON);
            journeyMetricDeclarations.put(journeyMetricDeclaration.getID(), journeyMetricDeclaration);
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
        JSONArray criterionFieldValues = JSONUtilities.decodeJSONArray(jsonRoot, "profileCriterionFields", new JSONArray());
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

    //
    //  universalControlGroupCriteria
    //

    try
      {
        JSONArray evaluationCriterionValues = JSONUtilities.decodeJSONArray(jsonRoot, "universalControlGroupCriteria", new JSONArray());
        for (int i=0; i<evaluationCriterionValues.size(); i++)
          {
            JSONObject evaluationCriterionJSON = (JSONObject) evaluationCriterionValues.get(i);
            EvaluationCriterion evaluationCriterion = new EvaluationCriterion(evaluationCriterionJSON, CriterionContext.Profile);
            universalControlGroupCriteria.add(evaluationCriterion);
          }
      }
    catch (GUIManagerException | JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  controlGroupCriteria
    //

    try
      {
        JSONArray evaluationCriterionValues = JSONUtilities.decodeJSONArray(jsonRoot, "controlGroupCriteria", new JSONArray());
        for (int i=0; i<evaluationCriterionValues.size(); i++)
          {
            JSONObject evaluationCriterionJSON = (JSONObject) evaluationCriterionValues.get(i);
            EvaluationCriterion evaluationCriterion = new EvaluationCriterion(evaluationCriterionJSON, CriterionContext.Profile);
            controlGroupCriteria.add(evaluationCriterion);
          }
      }
    catch (GUIManagerException | JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  offerCategories
    //

    try
      {
        JSONArray offerCategoryValues = JSONUtilities.decodeJSONArray(jsonRoot, "offerCategories", new JSONArray());
        for (int i=0; i<offerCategoryValues.size(); i++)
          {
            JSONObject offerCategoryJSON = (JSONObject) offerCategoryValues.get(i);
            OfferCategory offerCategory = new OfferCategory(offerCategoryJSON);
            offerCategories.put(offerCategory.getID(), offerCategory);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

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
    //  journeyDefaultTargetingWindowDuration
    //

    journeyDefaultTargetingWindowDuration = JSONUtilities.decodeInteger(jsonRoot, "journeyDefaultTargetingWindowDuration", 3);

    //
    //  journeyDefaultTargetingWindowUnit
    //

    journeyDefaultTargetingWindowUnit = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "journeyDefaultTargetingWindowUnit", "month"));

    //
    //  journeyDefaultTargetingWindowRoundUp
    //

    journeyDefaultTargetingWindowRoundUp = JSONUtilities.decodeBoolean(jsonRoot, "journeyDefaultTargetingWindowRoundUp", Boolean.FALSE);
    
    //
    //  journeyUniversalEligibilityCriteria
    //

    try
      {
        JSONArray evaluationCriterionValues = JSONUtilities.decodeJSONArray(jsonRoot, "journeyUniversalEligibilityCriteria", new JSONArray());
        for (int i=0; i<evaluationCriterionValues.size(); i++)
          {
            JSONObject evaluationCriterionJSON = (JSONObject) evaluationCriterionValues.get(i);
            EvaluationCriterion evaluationCriterion = new EvaluationCriterion(evaluationCriterionJSON, CriterionContext.Profile);
            journeyUniversalEligibilityCriteria.add(evaluationCriterion);
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
      }
    catch (GUIManagerException | JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
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

    //
    //  stockRefreshPeriod
    //

    stockRefreshPeriod = JSONUtilities.decodeInteger(jsonRoot, "stockRefreshPeriod", 30);
    
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
            reportManagerOutputPath = JSONUtilities.decodeString(reportManager, "reportManagerOutputPath", false);
            reportManagerDateFormat = JSONUtilities.decodeString(reportManager, "reportManagerDateFormat", false);
            reportManagerFileExtension = JSONUtilities.decodeString(reportManager, "reportManagerFileExtension", false);
            reportManagerCsvSeparator = JSONUtilities.decodeString(reportManager, "reportManagerCsvSeparator", false);
            reportManagerStreamsTempDir = JSONUtilities.decodeString(reportManager, "reportManagerStreamsTempDir", false);
          }
        else
          {
            reportManagerZookeeperDir = Deployment.getZookeeperRoot() + File.separator + "reports";
            reportManagerOutputPath = "/app/reports";
            reportManagerDateFormat = "yyyy-MM-dd_HH-mm-ss_SSSS";
            reportManagerFileExtension = "csv";
            reportManagerCsvSeparator = ";";
            reportManagerStreamsTempDir = System.getProperty("java.io.tmpdir");
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
    //  ucgEngineESConnectTimeout
    //

    try
      {
        Integer ucgEngineESConnectTimeoutJSON = JSONUtilities.decodeInteger(jsonRoot,"ucgEngineESConnectTimeout",false);
        ucgEngineESConnectTimeout = ucgEngineESConnectTimeoutJSON == null ? 30000:ucgEngineESConnectTimeoutJSON;
      }
    catch(JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment",e);
      }

    //
    //  ucgEngineESSocketTimeout
    //

    try
      {
        Integer ucgEngineESSocketTimeoutJSON = JSONUtilities.decodeInteger(jsonRoot,"ucgEngineESSocketTimeout",false);
        ucgEngineESSocketTimeout = ucgEngineESSocketTimeoutJSON == null ? 60000:ucgEngineESSocketTimeoutJSON;
      }
    catch(JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment",e);
      }

    //
    //  ucgEngineESMasRetryTimeout
    //

    try
      {
        Integer ucgEngineESMasRetryTimeoutJSON = JSONUtilities.decodeInteger(jsonRoot,"ucgEngineESMasRetryTimeout",false);
        ucgEngineESMasRetryTimeout = ucgEngineESMasRetryTimeoutJSON == null ? 60000:ucgEngineESMasRetryTimeoutJSON;
      }
    catch(JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment",e);
      }
  }

  /*****************************************
  *
  *  main (for validation)
  *
  *****************************************/

  public static void main(String[] args)
  {
    System.out.println("zookeeper root: " + getZookeeperRoot());
  }
}
