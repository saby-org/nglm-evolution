/*****************************************************************************
*
*  Deployment.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.SubscriberProfileService.CompressionType;

import com.evolving.nglm.core.ServerRuntimeException;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Deployment
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private static String subscriberGroupLoaderAlternateID;
  private static boolean subscriberGroupLoaderAutoProvision;
  private static String criterionFieldRetrieverClassName;
  private static String evolutionEngineExtensionClassName;
  private static String subscriberProfileClassName;
  private static Map<String,EvolutionEngineEventDeclaration> evolutionEngineEvents = new LinkedHashMap<String,EvolutionEngineEventDeclaration>();
  private static String journeyTopic;
  private static String segmentationRuleTopic;
  private static String offerTopic;
  private static String presentationStrategyTopic;
  private static String scoringStrategyTopic;
  private static String callingChannelTopic;
  private static String supplierTopic;
  private static String productTopic;
  private static String catalogCharacteristicTopic;
  private static String catalogObjectiveTopic;
  private static String subscriberUpdateTopic;
  private static String subscriberGroupTopic;
  private static String subscriberGroupAssignSubscriberIDTopic;
  private static String subscriberGroupEpochTopic;
  private static String subscriberStateChangeLog;
  private static String subscriberStateChangeLogTopic;
  private static String journeyStatisticTopic;
  private static String subscriberProfileRegistrySubject;
  private static CompressionType subscriberProfileCompressionType;
  private static Map<String,SupportedLanguage> supportedLanguages = new LinkedHashMap<String,SupportedLanguage>();
  private static Map<String,SupportedCurrency> supportedCurrencies = new LinkedHashMap<String,SupportedCurrency>();
  private static Map<String,SupportedTimeUnit> supportedTimeUnits = new LinkedHashMap<String,SupportedTimeUnit>();
  private static Map<String,ServiceType> serviceTypes = new LinkedHashMap<String,ServiceType>();
  private static Map<String,CallingChannelProperty> callingChannelProperties = new LinkedHashMap<String,CallingChannelProperty>();
  private static JSONArray initialCallingChannelsJSONArray = null;
  private static JSONArray initialSuppliersJSONArray = null;
  private static JSONArray initialProductsJSONArray = null;
  private static JSONArray initialCatalogCharacteristicsJSONArray = null;
  private static JSONArray initialCatalogObjectivesJSONArray = null;
  private static JSONArray fulfillmentProvidersJSONArray = null;
  private static JSONArray offerDeliverablesJSONArray = null;
  private static JSONArray paymentMeansJSONArray = null;
  private static Map<String,SalesChannel> salesChannels = new LinkedHashMap<String,SalesChannel>();
  private static Map<String,CatalogObjectiveSection> catalogObjectiveSections = new LinkedHashMap<String,CatalogObjectiveSection>();
  private static Map<String,SupportedDataType> supportedDataTypes = new LinkedHashMap<String,SupportedDataType>();
  private static Map<String,CriterionField> profileCriterionFields = new LinkedHashMap<String,CriterionField>();
  private static Map<String,CriterionField> presentationCriterionFields = new LinkedHashMap<String,CriterionField>();
  private static Map<String,OfferCategory> offerCategories = new LinkedHashMap<String,OfferCategory>();
  private static Map<String,OfferType> offerTypes = new LinkedHashMap<String,OfferType>();
  private static Map<String,OfferOptimizationAlgorithm> offerOptimizationAlgorithms = new LinkedHashMap<String,OfferOptimizationAlgorithm>();
  private static Map<String,DeliveryManagerDeclaration> deliveryManagers = new LinkedHashMap<String,DeliveryManagerDeclaration>();
  private static Map<String,NodeType> nodeTypes = new LinkedHashMap<String,NodeType>();
  private static Map<String,ToolboxSection> journeyToolbox = new LinkedHashMap<String,ToolboxSection>();
  private static Map<String,ToolboxSection> campaignToolbox = new LinkedHashMap<String,ToolboxSection>();

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
  
  //
  //  deployment accessors
  //

  public static String getSubscriberGroupLoaderAlternateID() { return subscriberGroupLoaderAlternateID; }
  public static boolean getSubscriberGroupLoaderAutoProvision() { return subscriberGroupLoaderAutoProvision; }
  public static String getCriterionFieldRetrieverClassName() { return criterionFieldRetrieverClassName; }
  public static String getEvolutionEngineExtensionClassName() { return evolutionEngineExtensionClassName; }
  public static String getSubscriberProfileClassName() { return subscriberProfileClassName; }
  public static Map<String,EvolutionEngineEventDeclaration> getEvolutionEngineEvents() { return evolutionEngineEvents; }
  public static String getJourneyTopic() { return journeyTopic; }
  public static String getSegmentationRuleTopic() { return segmentationRuleTopic; }
  public static String getOfferTopic() { return offerTopic; }
  public static String getPresentationStrategyTopic() { return presentationStrategyTopic; }
  public static String getScoringStrategyTopic() { return scoringStrategyTopic; }
  public static String getCallingChannelTopic() { return callingChannelTopic; }
  public static String getSupplierTopic() { return supplierTopic; }
  public static String getProductTopic() { return productTopic; }
  public static String getCatalogCharacteristicTopic() { return catalogCharacteristicTopic; }
  public static String getCatalogObjectiveTopic() { return catalogObjectiveTopic; }
  public static String getSubscriberUpdateTopic() { return subscriberUpdateTopic; }
  public static String getSubscriberGroupTopic() { return subscriberGroupTopic; }
  public static String getSubscriberGroupAssignSubscriberIDTopic() { return subscriberGroupAssignSubscriberIDTopic; }
  public static String getSubscriberGroupEpochTopic() { return subscriberGroupEpochTopic; }
  public static String getSubscriberStateChangeLog() { return subscriberStateChangeLog; }
  public static String getSubscriberStateChangeLogTopic() { return subscriberStateChangeLogTopic; }
  public static String getJourneyStatisticTopic() { return journeyStatisticTopic; }
  public static String getSubscriberProfileRegistrySubject() { return subscriberProfileRegistrySubject; }
  public static CompressionType getSubscriberProfileCompressionType() { return subscriberProfileCompressionType; }
  public static Map<String,SupportedLanguage> getSupportedLanguages() { return supportedLanguages; }
  public static Map<String,SupportedCurrency> getSupportedCurrencies() { return supportedCurrencies; }
  public static Map<String,SupportedTimeUnit> getSupportedTimeUnits() { return supportedTimeUnits; }
  public static Map<String,ServiceType> getServiceTypes() { return serviceTypes; }
  public static Map<String,CallingChannelProperty> getCallingChannelProperties() { return callingChannelProperties; }
  public static JSONArray getInitialCallingChannelsJSONArray() { return initialCallingChannelsJSONArray; }
  public static JSONArray getInitialSuppliersJSONArray() { return initialSuppliersJSONArray; }
  public static JSONArray getInitialProductsJSONArray() { return initialProductsJSONArray; }
  public static JSONArray getInitialCatalogCharacteristicsJSONArray() { return initialCatalogCharacteristicsJSONArray; }
  public static JSONArray getInitialCatalogObjectivesJSONArray() { return initialCatalogObjectivesJSONArray; }
  public static JSONArray getFulfillmentProvidersJSONArray() { return fulfillmentProvidersJSONArray; }
  public static JSONArray getOfferDeliverablesJSONArray() { return offerDeliverablesJSONArray; }
  public static JSONArray getPaymentMeansJSONArray() { return paymentMeansJSONArray; }
  public static Map<String,SalesChannel> getSalesChannels() { return salesChannels; }
  public static Map<String,CatalogObjectiveSection> getCatalogObjectiveSections() { return catalogObjectiveSections; }
  public static Map<String,SupportedDataType> getSupportedDataTypes() { return supportedDataTypes; }
  public static Map<String,CriterionField> getProfileCriterionFields() { return profileCriterionFields; }
  public static Map<String,CriterionField> getPresentationCriterionFields() { return presentationCriterionFields; }
  public static Map<String,OfferCategory> getOfferCategories() { return offerCategories; }
  public static Map<String,OfferType> getOfferTypes() { return offerTypes; }
  public static Map<String,OfferOptimizationAlgorithm> getOfferOptimizationAlgorithms() { return offerOptimizationAlgorithms; }
  public static Map<String,DeliveryManagerDeclaration> getDeliveryManagers() { return deliveryManagers; }
  public static Map<String,NodeType> getNodeTypes() { return nodeTypes; }
  public static Map<String,ToolboxSection> getJourneyToolbox() { return journeyToolbox; }
  public static Map<String,ToolboxSection> getCampaignToolbox() { return campaignToolbox; }

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
    //  segmentationRuleTopic
    //

    try
      {
    	segmentationRuleTopic = JSONUtilities.decodeString(jsonRoot, "segmentationRuleTopic", true);
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
    //  catalogObjectiveTopic
    //

    try
      {
        catalogObjectiveTopic = JSONUtilities.decodeString(jsonRoot, "catalogObjectiveTopic", true);
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

    initialCallingChannelsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialCallingChannels", true);

    //
    //  initialSuppliersJSONArray
    //

    initialSuppliersJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialSuppliers", true);

    //
    //  initialProductsJSONArray
    //

    initialProductsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialProducts", true);

    //
    //  initialCatalogCharacteristicsJSONArray
    //

    initialCatalogCharacteristicsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialCatalogCharacteristics", true);

    //
    //  initialCatalogObjectivesJSONArray
    //

    initialCatalogObjectivesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "initialCatalogObjectives", true);
    
    //
    //  fulfillmentProvidersJSONArray
    //

    fulfillmentProvidersJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "fulfillmentProviders", true);
    
    //
    //  offerDeliverablesJSONArray
    //

    offerDeliverablesJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "offerDeliverables", true);
    
    //
    //  paymentMeansJSONArray
    //

    paymentMeansJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "paymentMeans", true);
    
    //
    //  salesChannels
    //

    try
      {
        JSONArray salesChannelValues = JSONUtilities.decodeJSONArray(jsonRoot, "salesChannels", true);
        for (int i=0; i<salesChannelValues.size(); i++)
          {
            JSONObject salesChannelJSON = (JSONObject) salesChannelValues.get(i);
            SalesChannel salesChannel = new SalesChannel(salesChannelJSON);
            salesChannels.put(salesChannel.getID(), salesChannel);
          }
      }
    catch (JSONUtilitiesException | NoSuchMethodException | IllegalAccessException e)
      {
        throw new ServerRuntimeException("deployment", e);
      }

    //
    //  catalogObjectiveSections
    //

    try
      {
        JSONArray catalogObjectiveSectionValues = JSONUtilities.decodeJSONArray(jsonRoot, "catalogObjectiveSections", true);
        for (int i=0; i<catalogObjectiveSectionValues.size(); i++)
          {
            JSONObject catalogObjectiveSectionJSON = (JSONObject) catalogObjectiveSectionValues.get(i);
            CatalogObjectiveSection catalogObjectiveSection = new CatalogObjectiveSection(catalogObjectiveSectionJSON);
            catalogObjectiveSections.put(catalogObjectiveSection.getID(), catalogObjectiveSection);
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
        JSONArray supportedDataTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "supportedDataTypes", true);
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
        JSONArray criterionFieldValues = JSONUtilities.decodeJSONArray(jsonRoot, "profileCriterionFields", true);
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
        JSONArray criterionFieldValues = JSONUtilities.decodeJSONArray(jsonRoot, "presentationCriterionFields", false);
        if (criterionFieldValues == null) criterionFieldValues = new JSONArray();
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
    //  offerCategories
    //

    try
      {
        JSONArray offerCategoryValues = JSONUtilities.decodeJSONArray(jsonRoot, "offerCategories", true);
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
        JSONArray offerTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "offerTypes", true);
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
        JSONArray offerOptimizationAlgorithmValues = JSONUtilities.decodeJSONArray(jsonRoot, "offerOptimizationAlgorithms", false);
        if (offerOptimizationAlgorithmValues == null) offerOptimizationAlgorithmValues = new JSONArray();
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
        JSONArray deliveryManagerValues = JSONUtilities.decodeJSONArray(jsonRoot, "deliveryManagers", true);
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
    //  nodeTypes
    //

    try
      {
        JSONArray nodeTypeValues = JSONUtilities.decodeJSONArray(jsonRoot, "nodeTypes", true);
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
        JSONArray journeyToolboxSectionValues = JSONUtilities.decodeJSONArray(jsonRoot, "journeyToolbox", true);
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
        JSONArray campaignToolboxSectionValues = JSONUtilities.decodeJSONArray(jsonRoot, "campaignToolbox", true);
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
