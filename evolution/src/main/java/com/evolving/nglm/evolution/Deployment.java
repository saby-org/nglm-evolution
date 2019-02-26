/*****************************************************************************
*
*  Deployment.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SuspenseProcessEventConfiguration;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.SubscriberProfile.CompressionType;

public class Deployment
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private static String subscriberGroupLoaderAlternateID;
  private static String getCustomerAlternateID;
  private static boolean subscriberGroupLoaderAutoProvision;
  private static String criterionFieldRetrieverClassName;
  private static String evolutionEngineExtensionClassName;
  private static String guiManagerExtensionClassName;
  private static String subscriberProfileClassName;
  private static Map<String,EvolutionEngineEventDeclaration> evolutionEngineEvents = new LinkedHashMap<String,EvolutionEngineEventDeclaration>();
  private static String emptyTopic;
  private static String journeyTopic;
  private static String segmentationDimensionTopic;
  private static String offerTopic;
  private static String reportTopic;
  private static String presentationStrategyTopic;
  private static String scoringStrategyTopic;
  private static String callingChannelTopic;
  private static String salesChannelTopic;
  private static String supplierTopic;
  private static String productTopic;
  private static String catalogCharacteristicTopic;
  private static String journeyObjectiveTopic;
  private static String offerObjectiveTopic;
  private static String productTypeTopic;
  private static String ucgRuleTopic;
  private static String deliverableTopic;
  private static String mailTemplateTopic;
  private static String smsTemplateTopic;
  private static String guiAuditTopic;
  private static String subscriberUpdateTopic;
  private static String subscriberGroupTopic;
  private static String subscriberGroupAssignSubscriberIDTopic;
  private static String subscriberGroupEpochTopic;
  private static String ucgStateTopic;
  private static String timedEvaluationTopic;
  private static String subscriberStateChangeLog;
  private static String subscriberStateChangeLogTopic;
  private static String subscriberHistoryChangeLog;
  private static String subscriberHistoryChangeLogTopic;
  private static String journeyRequestTopic;
  private static String journeyStatisticTopic;
  private static String deliverableSourceTopic;
  private static String presentationLogTopic;
  private static String acceptanceLogTopic;
  private static String propensityLogTopic;
  private static String propensityStateChangeLog;
  private static String propensityStateChangeLogTopic;
  private static String subscriberProfileRegistrySubject;
  private static CompressionType subscriberProfileCompressionType;
  private static Map<String,SupportedLanguage> supportedLanguages = new LinkedHashMap<String,SupportedLanguage>();
  private static Map<String,SupportedCurrency> supportedCurrencies = new LinkedHashMap<String,SupportedCurrency>();
  private static Map<String,SupportedTimeUnit> supportedTimeUnits = new LinkedHashMap<String,SupportedTimeUnit>();
  private static Map<String,ServiceType> serviceTypes = new LinkedHashMap<String,ServiceType>();
  private static Map<String,CallingChannelProperty> callingChannelProperties = new LinkedHashMap<String,CallingChannelProperty>();
  private static JSONArray initialCallingChannelsJSONArray = null;
  private static JSONArray initialSalesChannelsJSONArray = null;
  private static JSONArray initialSuppliersJSONArray = null;
  private static JSONArray initialProductsJSONArray = null;
  private static JSONArray initialCatalogCharacteristicsJSONArray = null;
  private static JSONArray initialJourneyObjectivesJSONArray = null;
  private static JSONArray initialOfferObjectivesJSONArray = null;
  private static JSONArray initialProductTypesJSONArray = null;  
  private static JSONArray initialDeliverablesJSONArray = null;
  private static JSONArray initialSegmentationDimensionsJSONArray = null;
  private static Map<String,FulfillmentProvider> fulfillmentProviders = new LinkedHashMap<String,FulfillmentProvider>();
  private static Map<String,PaymentInstrument> paymentMeans = new LinkedHashMap<String,PaymentInstrument>();
  private static Map<String,SupportedDataType> supportedDataTypes = new LinkedHashMap<String,SupportedDataType>();
  private static Map<String,CriterionField> profileCriterionFields = new LinkedHashMap<String,CriterionField>();
  private static Map<String,CriterionField> presentationCriterionFields = new LinkedHashMap<String,CriterionField>();
  private static List<EvaluationCriterion> universalControlGroupCriteria = new ArrayList<EvaluationCriterion>();
  private static List<EvaluationCriterion> controlGroupCriteria = new ArrayList<EvaluationCriterion>();
  private static Map<String,OfferCategory> offerCategories = new LinkedHashMap<String,OfferCategory>();
  private static Map<String,OfferType> offerTypes = new LinkedHashMap<String,OfferType>();
  private static Map<String,OfferOptimizationAlgorithm> offerOptimizationAlgorithms = new LinkedHashMap<String,OfferOptimizationAlgorithm>();
  private static Map<String,DeliveryManagerDeclaration> deliveryManagers = new LinkedHashMap<String,DeliveryManagerDeclaration>();
  private static boolean journeyDefaultAutoTarget;
  private static int journeyDefaultTargetingWindowDuration;
  private static String journeyDefaultTargetingWindowUnit;
  private static boolean journeyDefaultTargetingWindowRoundUp;
  private static List<EvaluationCriterion> journeyUniversalTargetingCriteria = new ArrayList<EvaluationCriterion>();
  private static Map<String,NodeType> nodeTypes = new LinkedHashMap<String,NodeType>();
  private static Map<String,ToolboxSection> journeyToolbox = new LinkedHashMap<String,ToolboxSection>();
  private static Map<String,ToolboxSection> campaignToolbox = new LinkedHashMap<String,ToolboxSection>();
  private static Map<String,ThirdPartyMethodAccessLevel> thirdPartyMethodPermissionsMap = new LinkedHashMap<String,ThirdPartyMethodAccessLevel>();
  private static Integer authResponseCacheLifetimeInMinutes = null;
  private static int stockRefreshPeriod;
  private static String periodicEvaluationCronEntry;
  private static Map<String,ReportConfiguration> reportsConfiguration = new LinkedHashMap<String,ReportConfiguration>();
  private static String reportManagerZookeeperDir;
  private static String reportManagerOutputPath;
  private static String reportManagerDateFormat;
  private static String reportManagerFileExtension;
  private static String reportManagerStreamsTempDir;
  private static String reportManagerCsvSeparator;
  private static JSONArray reportsConfigValues;
  private static CustomerMetaData customerMetaData = null;
  private static String APIresponseDateFormat;

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
  
  //
  //  evolution accessors
  //

  public static String getSubscriberProfileEndpoints() { return System.getProperty("subscriberprofile.endpoints",""); }
  public static String getSubscriberGroupLoaderAlternateID() { return subscriberGroupLoaderAlternateID; }
  public static String getGetCustomerAlternateID() { return getCustomerAlternateID; }
  public static boolean getSubscriberGroupLoaderAutoProvision() { return subscriberGroupLoaderAutoProvision; }
  public static String getCriterionFieldRetrieverClassName() { return criterionFieldRetrieverClassName; }
  public static String getEvolutionEngineExtensionClassName() { return evolutionEngineExtensionClassName; }
  public static String getGUIManagerExtensionClassName() { return guiManagerExtensionClassName; }
  public static String getSubscriberProfileClassName() { return subscriberProfileClassName; }
  public static Map<String,EvolutionEngineEventDeclaration> getEvolutionEngineEvents() { return evolutionEngineEvents; }
  public static String getEmptyTopic() { return emptyTopic; }
  public static String getJourneyTopic() { return journeyTopic; }
  public static String getSegmentationDimensionTopic() { return segmentationDimensionTopic; }
  public static String getOfferTopic() { return offerTopic; }
  public static String getReportTopic() { return reportTopic; }
  public static String getPresentationStrategyTopic() { return presentationStrategyTopic; }
  public static String getScoringStrategyTopic() { return scoringStrategyTopic; }
  public static String getCallingChannelTopic() { return callingChannelTopic; }
  public static String getSalesChannelTopic() { return salesChannelTopic; }
  public static String getSupplierTopic() { return supplierTopic; }
  public static String getProductTopic() { return productTopic; }
  public static String getCatalogCharacteristicTopic() { return catalogCharacteristicTopic; }
  public static String getJourneyObjectiveTopic() { return journeyObjectiveTopic; }
  public static String getOfferObjectiveTopic() { return offerObjectiveTopic; }
  public static String getProductTypeTopic() { return productTypeTopic; }
  public static String getUCGRuleTopic() { return ucgRuleTopic; }
  public static String getDeliverableTopic() { return deliverableTopic; }
  public static String getMailTemplateTopic() { return mailTemplateTopic; }
  public static String getSMSTemplateTopic() { return smsTemplateTopic; }
  public static String getGUIAuditTopic() { return guiAuditTopic; }
  public static String getSubscriberUpdateTopic() { return subscriberUpdateTopic; }
  public static String getSubscriberGroupTopic() { return subscriberGroupTopic; }
  public static String getSubscriberGroupAssignSubscriberIDTopic() { return subscriberGroupAssignSubscriberIDTopic; }
  public static String getSubscriberGroupEpochTopic() { return subscriberGroupEpochTopic; }
  public static String getUCGStateTopic() { return ucgStateTopic; }
  public static String getTimedEvaluationTopic() { return timedEvaluationTopic; }
  public static String getSubscriberStateChangeLog() { return subscriberStateChangeLog; }
  public static String getSubscriberStateChangeLogTopic() { return subscriberStateChangeLogTopic; }
  public static String getSubscriberHistoryChangeLog() { return subscriberHistoryChangeLog; }
  public static String getSubscriberHistoryChangeLogTopic() { return subscriberHistoryChangeLogTopic; }
  public static String getJourneyRequestTopic() { return journeyRequestTopic; }
  public static String getJourneyStatisticTopic() { return journeyStatisticTopic; }
  public static String getDeliverableSourceTopic() { return deliverableSourceTopic; }
  public static String getPresentationLogTopic() { return presentationLogTopic; }
  public static String getAcceptanceLogTopic() { return acceptanceLogTopic; }
  public static String getPropensityLogTopic() { return propensityLogTopic; }
  public static String getPropensityStateChangeLog() { return propensityStateChangeLog; }
  public static String getPropensityStateChangeLogTopic() { return propensityStateChangeLogTopic; }
  public static String getSubscriberProfileRegistrySubject() { return subscriberProfileRegistrySubject; }
  public static CompressionType getSubscriberProfileCompressionType() { return subscriberProfileCompressionType; }
  public static Map<String,SupportedLanguage> getSupportedLanguages() { return supportedLanguages; }
  public static Map<String,SupportedCurrency> getSupportedCurrencies() { return supportedCurrencies; }
  public static Map<String,SupportedTimeUnit> getSupportedTimeUnits() { return supportedTimeUnits; }
  public static Map<String,ServiceType> getServiceTypes() { return serviceTypes; }
  public static Map<String,CallingChannelProperty> getCallingChannelProperties() { return callingChannelProperties; }
  public static JSONArray getInitialCallingChannelsJSONArray() { return initialCallingChannelsJSONArray; }
  public static JSONArray getInitialSalesChannelsJSONArray() { return initialSalesChannelsJSONArray; }
  public static JSONArray getInitialSuppliersJSONArray() { return initialSuppliersJSONArray; }
  public static JSONArray getInitialProductsJSONArray() { return initialProductsJSONArray; }
  public static JSONArray getInitialCatalogCharacteristicsJSONArray() { return initialCatalogCharacteristicsJSONArray; }
  public static JSONArray getInitialJourneyObjectivesJSONArray() { return initialJourneyObjectivesJSONArray; }
  public static JSONArray getInitialOfferObjectivesJSONArray() { return initialOfferObjectivesJSONArray; }
  public static JSONArray getInitialProductTypesJSONArray() { return initialProductTypesJSONArray; }
  public static JSONArray getInitialDeliverablesJSONArray() { return initialDeliverablesJSONArray; }
  public static JSONArray getInitialSegmentationDimensionsJSONArray() { return initialSegmentationDimensionsJSONArray; }
  public static Map<String,FulfillmentProvider> getFulfillmentProviders() { return fulfillmentProviders; }
  public static Map<String,PaymentInstrument> getPaymentMeans() { return paymentMeans; }
  public static Map<String,SupportedDataType> getSupportedDataTypes() { return supportedDataTypes; }
  public static Map<String,CriterionField> getProfileCriterionFields() { return profileCriterionFields; }
  public static Map<String,CriterionField> getPresentationCriterionFields() { return presentationCriterionFields; }
  public static List<EvaluationCriterion> getUniversalControlGroupCriteria() { return universalControlGroupCriteria; }
  public static List<EvaluationCriterion> getControlGroupCriteria() { return controlGroupCriteria; }
  public static Map<String,OfferCategory> getOfferCategories() { return offerCategories; }
  public static Map<String,OfferType> getOfferTypes() { return offerTypes; }
  public static Map<String,OfferOptimizationAlgorithm> getOfferOptimizationAlgorithms() { return offerOptimizationAlgorithms; }
  public static Map<String,DeliveryManagerDeclaration> getDeliveryManagers() { return deliveryManagers; }
  public static boolean getJourneyDefaultAutoTarget() { return journeyDefaultAutoTarget; }
  public static int getJourneyDefaultTargetingWindowDuration() { return journeyDefaultTargetingWindowDuration; }
  public static String getJourneyDefaultTargetingWindowUnit() { return journeyDefaultTargetingWindowUnit; }
  public static boolean getJourneyDefaultTargetingWindowRoundUp() { return journeyDefaultTargetingWindowRoundUp; }
  public static List<EvaluationCriterion> getJourneyUniversalTargetingCriteria() { return journeyUniversalTargetingCriteria; }
  public static Map<String,NodeType> getNodeTypes() { return nodeTypes; }
  public static Map<String,ToolboxSection> getJourneyToolbox() { return journeyToolbox; }
  public static Map<String,ToolboxSection> getCampaignToolbox() { return campaignToolbox; }
  public static Map<String,ThirdPartyMethodAccessLevel> getThirdPartyMethodPermissionsMap() { return thirdPartyMethodPermissionsMap; }
  public static Integer getAuthResponseCacheLifetimeInMinutes() { return authResponseCacheLifetimeInMinutes; }
  public static int getStockRefreshPeriod() { return stockRefreshPeriod; }
  public static String getPeriodicEvaluationCronEntry() { return periodicEvaluationCronEntry; }
  public static Map<String,ReportConfiguration> getReportsConfiguration() { return reportsConfiguration; }
  public static JSONArray getReportsConfigJSon() { return reportsConfigValues; }
  public static String getReportManagerZookeeperDir() { return reportManagerZookeeperDir; }
  public static String getReportManagerOutputPath() { return reportManagerOutputPath; }
  public static String getReportManagerDateFormat() { return reportManagerDateFormat; }
  public static String getReportManagerFileExtension() { return reportManagerFileExtension; }
  public static String getReportManagerCsvSeparator() { return reportManagerCsvSeparator; }
  public static String getReportManagerStreamsTempDir() { return reportManagerStreamsTempDir; }
  public static CustomerMetaData getCustomerMetaData() { return customerMetaData; }
  public static String getAPIresponseDateFormat() { return APIresponseDateFormat; }

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
        throw new RuntimeException(e);
      }
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
    //  mailTemplateTopic
    //

    try
      {
        mailTemplateTopic = JSONUtilities.decodeString(jsonRoot, "mailTemplateTopic", true);
      }
    catch (JSONUtilitiesException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  smsTemplateTopic
    //

    try
      {
        smsTemplateTopic = JSONUtilities.decodeString(jsonRoot, "smsTemplateTopic", true);
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
    //  subscriberUpdateTopic
    //

    try
      {
        subscriberUpdateTopic = JSONUtilities.decodeString(jsonRoot, "subscriberUpdateTopic", true);
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
    //  initialProductsJSONArray
    //

    initialProductsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialProducts", new JSONArray());

    //
    //  initialCatalogCharacteristicsJSONArray
    //

    initialCatalogCharacteristicsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialCatalogCharacteristics", new JSONArray());

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
    //  initialDeliverablesJSONArray
    //

    initialDeliverablesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialDeliverables", new JSONArray());
    
    //
    //  initialSegmentationDimensionsJSONArray
    //

    initialSegmentationDimensionsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialSegmentationDimensions", new JSONArray());

    //
    //  fulfillmentProviders
    //

    try
      {
        JSONArray fulfillmentProviderValues = JSONUtilities.decodeJSONArray(jsonRoot, "fulfillmentProviders", new JSONArray());
        for (int i=0; i<fulfillmentProviderValues.size(); i++)
          {
            JSONObject fulfillmentProviderJSON = (JSONObject) fulfillmentProviderValues.get(i);
            FulfillmentProvider fulfillmentProvider = new FulfillmentProvider(fulfillmentProviderJSON);
            fulfillmentProviders.put(fulfillmentProvider.getID(), fulfillmentProvider);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  paymentMeans
    //

    try
      {
        JSONArray paymentInstrumentValues = JSONUtilities.decodeJSONArray(jsonRoot, "paymentMeans", new JSONArray());
        for (int i=0; i<paymentInstrumentValues.size(); i++)
          {
            JSONObject paymentInstrumentJSON = (JSONObject) paymentInstrumentValues.get(i);
            PaymentInstrument paymentInstrument = new PaymentInstrument(paymentInstrumentJSON);
            paymentMeans.put(paymentInstrument.getID(), paymentInstrument);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
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
    //  offerTypes
    //

    try
      {
        JSONArray offerTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "offerTypes", new JSONArray());
        for (int i=0; i<offerTypeValues.size(); i++)
          {
            JSONObject offerTypeJSON = (JSONObject) offerTypeValues.get(i);
            OfferType offerType = new OfferType(offerTypeJSON);
            offerTypes.put(offerType.getID(), offerType);
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
        JSONArray offerOptimizationAlgorithmValues = JSONUtilities.decodeJSONArray(jsonRoot, "offerOptimizationAlgorithms", new JSONArray());
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
    //  deliveryManagers
    //

    try
      {
        JSONArray deliveryManagerValues = JSONUtilities.decodeJSONArray(jsonRoot, "deliveryManagers", new JSONArray());
        for (int i=0; i<deliveryManagerValues.size(); i++)
          {
            JSONObject deliveryManagerJSON = (JSONObject) deliveryManagerValues.get(i);
            DeliveryManagerDeclaration deliveryManagerDeclaration = new DeliveryManagerDeclaration(deliveryManagerJSON);
            deliveryManagers.put(deliveryManagerDeclaration.getDeliveryType(), deliveryManagerDeclaration);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }
    
    //
    //  journeyDefaultAutoTarget
    //

    journeyDefaultAutoTarget = JSONUtilities.decodeBoolean(jsonRoot, "journeyDefaultAutoTarget", Boolean.FALSE);

    //
    //  journeyDefaultTargetingWindowDuration
    //

    journeyDefaultTargetingWindowDuration = JSONUtilities.decodeInteger(jsonRoot, "journeyDefaultTargetingWindowDuration", 3);

    //
    //  journeyDefaultTargetingWindowUnit
    //

    journeyDefaultTargetingWindowUnit = JSONUtilities.decodeString(jsonRoot, "journeyDefaultTargetingWindowUnit", "month");

    //
    //  journeyDefaultTargetingWindowRoundUp
    //

    journeyDefaultTargetingWindowRoundUp = JSONUtilities.decodeBoolean(jsonRoot, "journeyDefaultTargetingWindowRoundUp", Boolean.FALSE);
    
    //
    //  journeyUniversalTargetingCriteria
    //

    try
      {
        JSONArray evaluationCriterionValues = JSONUtilities.decodeJSONArray(jsonRoot, "journeyUniversalTargetingCriteria", new JSONArray());
        for (int i=0; i<evaluationCriterionValues.size(); i++)
          {
            JSONObject evaluationCriterionJSON = (JSONObject) evaluationCriterionValues.get(i);
            EvaluationCriterion evaluationCriterion = new EvaluationCriterion(evaluationCriterionJSON, CriterionContext.Profile);
            journeyUniversalTargetingCriteria.add(evaluationCriterion);
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
    //  Reports
    //

    try
      {
        JSONObject reportManager = JSONUtilities.decodeJSONObject(jsonRoot, "reportManager", false);
        if (reportManager != null)
          {
            JSONObject globalConfig = JSONUtilities.decodeJSONObject(reportManager, "globalConfiguration", true);
            reportManagerZookeeperDir = JSONUtilities.decodeString(globalConfig, "reportManagerZookeeperDir", true);
            reportManagerOutputPath = JSONUtilities.decodeString(globalConfig, "reportManagerOutputPath", false);
            reportManagerDateFormat = JSONUtilities.decodeString(globalConfig, "reportManagerDateFormat", false);
            reportManagerFileExtension = JSONUtilities.decodeString(globalConfig, "reportManagerFileExtension", false);
            reportManagerCsvSeparator = JSONUtilities.decodeString(globalConfig, "reportManagerCsvSeparator", false);
            reportManagerStreamsTempDir = JSONUtilities.decodeString(globalConfig, "reportManagerStreamsTempDir", false);
            reportsConfigValues = JSONUtilities.decodeJSONArray(reportManager, "reportsConfiguration", true);
            for (int i=0; i<reportsConfigValues.size(); i++)
              {
                ReportConfiguration reportConfig = new ReportConfiguration((JSONObject) reportsConfigValues.get(i));
                String reportName = reportConfig.getReportName();
                if (reportsConfiguration.containsKey(reportName)) throw new ServerRuntimeException("reportsConfiguration entry already exists for "+reportName);
                reportsConfiguration.put(reportName, reportConfig);
              }
          }
        else
          {
            reportsConfiguration = null;
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
