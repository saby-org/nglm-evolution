/*****************************************************************************
*
*  GUIManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Alarm;
import com.evolving.nglm.core.Alarm.AlarmLevel;
import com.evolving.nglm.core.Alarm.AlarmType;
import com.evolving.nglm.core.LicenseChecker;
import com.evolving.nglm.core.LicenseChecker.LicenseState;
import com.evolving.nglm.core.SubscriberIDService.SubscriberIDServiceException;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberIDService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.UniqueKeyServer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
   
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

  public enum API
  {
    getStaticConfiguration("getStaticConfiguration"),
    getSupportedLanguages("getSupportedLanguages"),
    getSupportedCurrencies("getSupportedCurrencies"),
    getSupportedTimeUnits("getSupportedTimeUnits"),
    getServiceTypes("getServiceTypes"),
    getCallingChannelProperties("getCallingChannelProperties"),
    getSalesChannels("getSalesChannels"),
    getSupportedDataTypes("getSupportedDataTypes"),
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
    getCampaignToolbox("getCampaignToolbox"),
    getCampaignList("getCampaignList"),
    getCampaignSummaryList("getCampaignSummaryList"),
    getCampaign("getCampaign"),
    putCampaign("putCampaign"),
    removeCampaign("removeCampaign"),
    getSegmentationRuleList("getSegmentationRuleList"),
    getSegmentationRuleSummaryList("getSegmentationRuleSummaryList"),
    getSegmentationRule("getSegmentationRule"),
    putSegmentationRule("putSegmentationRule"),
    removeSegmentationRule("removeSegmentationRule"),
    getOfferList("getOfferList"),
    getOfferSummaryList("getOfferSummaryList"),
    getOffer("getOffer"),
    putOffer("putOffer"),
    removeOffer("removeOffer"),
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
    getDeliverableList("getDeliverableList"),
    getDeliverableSummaryList("getDeliverableSummaryList"),
    getDeliverable("getDeliverable"),
    getFulfillmentProviders("getFulfillmentProviders"),
    getPaymentMeans("getPaymentMeans"),
    getDashboardCounts("getDashboardCounts"),
    getCustomer("getCustomer"),
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

  private static final int RESTAPIVersion = 1;
  private HttpServer restServer;
  private JourneyService journeyService;
  private SegmentationRuleService segmentationRuleService;
  private OfferService offerService;
  private ScoringStrategyService scoringStrategyService;
  private PresentationStrategyService presentationStrategyService;
  private CallingChannelService callingChannelService;
  private SupplierService supplierService;
  private ProductService productService;
  private CatalogCharacteristicService catalogCharacteristicService;
  private OfferObjectiveService offerObjectiveService;
  private ProductTypeService productTypeService;
  private DeliverableService deliverableService;
  private SubscriberProfileService subscriberProfileService;
  private SubscriberIDService subscriberIDService;
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private DeliverableSourceService deliverableSourceService;
  private String subscriberTraceControlAlternateID;

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
    String nodeID = System.getProperty("nglm.license.nodeid");
    String journeyTopic = Deployment.getJourneyTopic();
    String segmentationRuleTopic = Deployment.getSegmentationRuleTopic();
    String offerTopic = Deployment.getOfferTopic();
    String presentationStrategyTopic = Deployment.getPresentationStrategyTopic();
    String scoringStrategyTopic = Deployment.getScoringStrategyTopic();
    String callingChannelTopic = Deployment.getCallingChannelTopic();
    String supplierTopic = Deployment.getSupplierTopic();
    String productTopic = Deployment.getProductTopic();
    String catalogCharacteristicTopic = Deployment.getCatalogCharacteristicTopic();
    String offerObjectiveTopic = Deployment.getOfferObjectiveTopic();
    String productTypeTopic = Deployment.getProductTypeTopic();
    String deliverableTopic = Deployment.getDeliverableTopic();
    String subscriberUpdateTopic = Deployment.getSubscriberUpdateTopic();
    String subscriberGroupEpochTopic = Deployment.getSubscriberGroupEpochTopic();
    String deliverableSourceTopic = Deployment.getDeliverableSourceTopic();
    String redisServer = Deployment.getRedisSentinels();
    String subscriberProfileEndpoints = Deployment.getSubscriberProfileEndpoints();
    subscriberTraceControlAlternateID = Deployment.getSubscriberTraceControlAlternateID();
    
    //
    //  log
    //

    log.info("main START: {} {} {} {} {} {} {} {} {}", apiProcessKey, bootstrapServers, apiRestPort, nodeID, journeyTopic, segmentationRuleTopic, offerTopic, presentationStrategyTopic, scoringStrategyTopic, subscriberGroupEpochTopic);

    //
    //  license
    //

    licenseChecker = new LicenseChecker(ProductID, nodeID, Deployment.getZookeeperRoot(), Deployment.getZookeeperConnect());
    
    /*****************************************
    *
    *  services - construct
    *
    *****************************************/

    journeyService = new JourneyService(bootstrapServers, "guimanager-journeyservice-" + apiProcessKey, journeyTopic, true);
    segmentationRuleService = new SegmentationRuleService(bootstrapServers, "guimanager-segmentationruleservice-" + apiProcessKey, segmentationRuleTopic, true);
    offerService = new OfferService(bootstrapServers, "guimanager-offerservice-" + apiProcessKey, offerTopic, true);
    scoringStrategyService = new ScoringStrategyService(bootstrapServers, "guimanager-scoringstrategyservice-" + apiProcessKey, scoringStrategyTopic, true);
    presentationStrategyService = new PresentationStrategyService(bootstrapServers, "guimanager-presentationstrategyservice-" + apiProcessKey, presentationStrategyTopic, true);
    callingChannelService = new CallingChannelService(bootstrapServers, "guimanager-callingchannelservice-" + apiProcessKey, callingChannelTopic, true);
    supplierService = new SupplierService(bootstrapServers, "guimanager-supplierservice-" + apiProcessKey, supplierTopic, true);
    productService = new ProductService(bootstrapServers, "guimanager-productservice-" + apiProcessKey, productTopic, true);
    catalogCharacteristicService = new CatalogCharacteristicService(bootstrapServers, "guimanager-catalogcharacteristicservice-" + apiProcessKey, catalogCharacteristicTopic, true);
    offerObjectiveService = new OfferObjectiveService(bootstrapServers, "guimanager-offerobjectiveservice-" + apiProcessKey, offerObjectiveTopic, true);
    productTypeService = new ProductTypeService(bootstrapServers, "guimanager-producttypeservice-" + apiProcessKey, productTypeTopic, true);
    deliverableService = new DeliverableService(bootstrapServers, "guimanager-deliverableservice-" + apiProcessKey, deliverableTopic, true);
    subscriberProfileService = new EngineSubscriberProfileService(bootstrapServers, "guimanager-subscriberprofileservice-001", subscriberUpdateTopic, subscriberProfileEndpoints);
    subscriberIDService = new SubscriberIDService(redisServer);
    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("guimanager-subscribergroupepoch", apiProcessKey, bootstrapServers, subscriberGroupEpochTopic, SubscriberGroupEpoch::unpack);
    deliverableSourceService = new DeliverableSourceService(bootstrapServers, "guimanager-deliverablesourceservice-" + apiProcessKey, deliverableSourceTopic);

    /*****************************************
    *
    *  services - initialize
    *
    *****************************************/

    //
    //  deliverables
    //

    if (deliverableService.getStoredDeliverables().size() == 0)
      {
        try
          {
            JSONArray initialDeliverablesJSONArray = Deployment.getInitialDeliverablesJSONArray();
            for (int i=0; i<initialDeliverablesJSONArray.size(); i++)
              {
                JSONObject deliverableJSON = (JSONObject) initialDeliverablesJSONArray.get(i);
                processPutDeliverable("0", deliverableJSON);
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
    
    /*****************************************
    *
    *  services - start
    *
    *****************************************/

    journeyService.start();
    segmentationRuleService.start();
    offerService.start();
    scoringStrategyService.start();
    presentationStrategyService.start();
    callingChannelService.start();
    supplierService.start();
    productService.start();
    catalogCharacteristicService.start();
    offerObjectiveService.start();
    productTypeService.start();
    deliverableService.start();
    subscriberProfileService.start();
    deliverableSourceService.start();

    /*****************************************
    *
    *  REST interface -- server and handlers
    *
    *****************************************/

    try
      {
        InetSocketAddress addr = new InetSocketAddress(apiRestPort);
        restServer = HttpServer.create(addr, 0);
        restServer.createContext("/nglm-guimanager/getStaticConfiguration", new APIHandler(API.getStaticConfiguration));
        restServer.createContext("/nglm-guimanager/getSupportedLanguages", new APIHandler(API.getSupportedLanguages));
        restServer.createContext("/nglm-guimanager/getSupportedCurrencies", new APIHandler(API.getSupportedCurrencies));
        restServer.createContext("/nglm-guimanager/getSupportedTimeUnits", new APIHandler(API.getSupportedTimeUnits));
        restServer.createContext("/nglm-guimanager/getServiceTypes", new APIHandler(API.getServiceTypes));
        restServer.createContext("/nglm-guimanager/getCallingChannelProperties", new APIHandler(API.getCallingChannelProperties));
        restServer.createContext("/nglm-guimanager/getSalesChannels", new APIHandler(API.getSalesChannels));
        restServer.createContext("/nglm-guimanager/getSupportedDataTypes", new APIHandler(API.getSupportedDataTypes));
        restServer.createContext("/nglm-guimanager/getProfileCriterionFields", new APIHandler(API.getProfileCriterionFields));
        restServer.createContext("/nglm-guimanager/getProfileCriterionFieldIDs", new APIHandler(API.getProfileCriterionFieldIDs));
        restServer.createContext("/nglm-guimanager/getProfileCriterionField", new APIHandler(API.getProfileCriterionField));
        restServer.createContext("/nglm-guimanager/getPresentationCriterionFields", new APIHandler(API.getPresentationCriterionFields));
        restServer.createContext("/nglm-guimanager/getPresentationCriterionFieldIDs", new APIHandler(API.getPresentationCriterionFieldIDs));
        restServer.createContext("/nglm-guimanager/getPresentationCriterionField", new APIHandler(API.getPresentationCriterionField));
        restServer.createContext("/nglm-guimanager/getJourneyCriterionFields", new APIHandler(API.getJourneyCriterionFields));
        restServer.createContext("/nglm-guimanager/getJourneyCriterionFieldIDs", new APIHandler(API.getJourneyCriterionFieldIDs));
        restServer.createContext("/nglm-guimanager/getJourneyCriterionField", new APIHandler(API.getJourneyCriterionField));
        restServer.createContext("/nglm-guimanager/getOfferCategories", new APIHandler(API.getOfferCategories));
        restServer.createContext("/nglm-guimanager/getOfferTypes", new APIHandler(API.getOfferTypes));
        restServer.createContext("/nglm-guimanager/getOfferOptimizationAlgorithms", new APIHandler(API.getOfferOptimizationAlgorithms));
        restServer.createContext("/nglm-guimanager/getNodeTypes", new APIHandler(API.getNodeTypes));
        restServer.createContext("/nglm-guimanager/getJourneyToolbox", new APIHandler(API.getJourneyToolbox));
        restServer.createContext("/nglm-guimanager/getJourneyList", new APIHandler(API.getJourneyList));
        restServer.createContext("/nglm-guimanager/getJourneySummaryList", new APIHandler(API.getJourneySummaryList));
        restServer.createContext("/nglm-guimanager/getJourney", new APIHandler(API.getJourney));
        restServer.createContext("/nglm-guimanager/putJourney", new APIHandler(API.putJourney));
        restServer.createContext("/nglm-guimanager/removeJourney", new APIHandler(API.removeJourney));
        restServer.createContext("/nglm-guimanager/getCampaignToolbox", new APIHandler(API.getCampaignToolbox));
        restServer.createContext("/nglm-guimanager/getCampaignList", new APIHandler(API.getCampaignList));
        restServer.createContext("/nglm-guimanager/getCampaignSummaryList", new APIHandler(API.getCampaignSummaryList));
        restServer.createContext("/nglm-guimanager/getCampaign", new APIHandler(API.getCampaign));
        restServer.createContext("/nglm-guimanager/putCampaign", new APIHandler(API.putCampaign));
        restServer.createContext("/nglm-guimanager/removeCampaign", new APIHandler(API.removeCampaign));
        restServer.createContext("/nglm-guimanager/getSegmentationRuleList", new APIHandler(API.getSegmentationRuleList));
        restServer.createContext("/nglm-guimanager/getSegmentationRuleSummaryList", new APIHandler(API.getSegmentationRuleSummaryList));
        restServer.createContext("/nglm-guimanager/getSegmentationRule", new APIHandler(API.getSegmentationRule));
        restServer.createContext("/nglm-guimanager/putSegmentationRule", new APIHandler(API.putSegmentationRule));
        restServer.createContext("/nglm-guimanager/removeSegmentationRule", new APIHandler(API.removeSegmentationRule));
        restServer.createContext("/nglm-guimanager/getOfferList", new APIHandler(API.getOfferList));
        restServer.createContext("/nglm-guimanager/getOfferSummaryList", new APIHandler(API.getOfferSummaryList));
        restServer.createContext("/nglm-guimanager/getOffer", new APIHandler(API.getOffer));
        restServer.createContext("/nglm-guimanager/putOffer", new APIHandler(API.putOffer));
        restServer.createContext("/nglm-guimanager/removeOffer", new APIHandler(API.removeOffer));
        restServer.createContext("/nglm-guimanager/getPresentationStrategyList", new APIHandler(API.getPresentationStrategyList));
        restServer.createContext("/nglm-guimanager/getPresentationStrategySummaryList", new APIHandler(API.getPresentationStrategySummaryList));
        restServer.createContext("/nglm-guimanager/getPresentationStrategy", new APIHandler(API.getPresentationStrategy));
        restServer.createContext("/nglm-guimanager/putPresentationStrategy", new APIHandler(API.putPresentationStrategy));
        restServer.createContext("/nglm-guimanager/removePresentationStrategy", new APIHandler(API.removePresentationStrategy));
        restServer.createContext("/nglm-guimanager/getScoringStrategyList", new APIHandler(API.getScoringStrategyList));
        restServer.createContext("/nglm-guimanager/getScoringStrategySummaryList", new APIHandler(API.getScoringStrategySummaryList));
        restServer.createContext("/nglm-guimanager/getScoringStrategy", new APIHandler(API.getScoringStrategy));
        restServer.createContext("/nglm-guimanager/putScoringStrategy", new APIHandler(API.putScoringStrategy));
        restServer.createContext("/nglm-guimanager/removeScoringStrategy", new APIHandler(API.removeScoringStrategy));
        restServer.createContext("/nglm-guimanager/getCallingChannelList", new APIHandler(API.getCallingChannelList));
        restServer.createContext("/nglm-guimanager/getCallingChannelSummaryList", new APIHandler(API.getCallingChannelSummaryList));
        restServer.createContext("/nglm-guimanager/getCallingChannel", new APIHandler(API.getCallingChannel));
        restServer.createContext("/nglm-guimanager/putCallingChannel", new APIHandler(API.putCallingChannel));
        restServer.createContext("/nglm-guimanager/removeCallingChannel", new APIHandler(API.removeCallingChannel));
        restServer.createContext("/nglm-guimanager/getSupplierList", new APIHandler(API.getSupplierList));
        restServer.createContext("/nglm-guimanager/getSupplierSummaryList", new APIHandler(API.getSupplierSummaryList));
        restServer.createContext("/nglm-guimanager/getSupplier", new APIHandler(API.getSupplier));
        restServer.createContext("/nglm-guimanager/putSupplier", new APIHandler(API.putSupplier));
        restServer.createContext("/nglm-guimanager/removeSupplier", new APIHandler(API.removeSupplier));
        restServer.createContext("/nglm-guimanager/getProductList", new APIHandler(API.getProductList));
        restServer.createContext("/nglm-guimanager/getProductSummaryList", new APIHandler(API.getProductSummaryList));
        restServer.createContext("/nglm-guimanager/getProduct", new APIHandler(API.getProduct));
        restServer.createContext("/nglm-guimanager/putProduct", new APIHandler(API.putProduct));
        restServer.createContext("/nglm-guimanager/removeProduct", new APIHandler(API.removeProduct));
        restServer.createContext("/nglm-guimanager/getCatalogCharacteristicList", new APIHandler(API.getCatalogCharacteristicList));
        restServer.createContext("/nglm-guimanager/getCatalogCharacteristicSummaryList", new APIHandler(API.getCatalogCharacteristicSummaryList));
        restServer.createContext("/nglm-guimanager/getCatalogCharacteristic", new APIHandler(API.getCatalogCharacteristic));
        restServer.createContext("/nglm-guimanager/putCatalogCharacteristic", new APIHandler(API.putCatalogCharacteristic));
        restServer.createContext("/nglm-guimanager/removeCatalogCharacteristic", new APIHandler(API.removeCatalogCharacteristic));
        restServer.createContext("/nglm-guimanager/getOfferObjectiveList", new APIHandler(API.getOfferObjectiveList));
        restServer.createContext("/nglm-guimanager/getOfferObjectiveSummaryList", new APIHandler(API.getOfferObjectiveSummaryList));
        restServer.createContext("/nglm-guimanager/getOfferObjective", new APIHandler(API.getOfferObjective));
        restServer.createContext("/nglm-guimanager/putOfferObjective", new APIHandler(API.putOfferObjective));
        restServer.createContext("/nglm-guimanager/removeOfferObjective", new APIHandler(API.removeOfferObjective));
        restServer.createContext("/nglm-guimanager/getProductTypeList", new APIHandler(API.getProductTypeList));
        restServer.createContext("/nglm-guimanager/getProductTypeSummaryList", new APIHandler(API.getProductTypeSummaryList));
        restServer.createContext("/nglm-guimanager/getProductType", new APIHandler(API.getProductType));
        restServer.createContext("/nglm-guimanager/putProductType", new APIHandler(API.putProductType));
        restServer.createContext("/nglm-guimanager/removeProductType", new APIHandler(API.removeProductType));
        restServer.createContext("/nglm-guimanager/getDeliverableList", new APIHandler(API.getDeliverableList));
        restServer.createContext("/nglm-guimanager/getDeliverableSummaryList", new APIHandler(API.getDeliverableSummaryList));
        restServer.createContext("/nglm-guimanager/getDeliverable", new APIHandler(API.getDeliverable));
        restServer.createContext("/nglm-guimanager/getFulfillmentProviders", new APIHandler(API.getFulfillmentProviders));
        restServer.createContext("/nglm-guimanager/getPaymentMeans", new APIHandler(API.getPaymentMeans));
        restServer.createContext("/nglm-guimanager/getDashboardCounts", new APIHandler(API.getDashboardCounts));
        restServer.createContext("/nglm-guimanager/getCustomer", new APIHandler(API.getCustomer));
        restServer.setExecutor(Executors.newFixedThreadPool(10));
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
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(restServer, journeyService, segmentationRuleService, offerService, scoringStrategyService, presentationStrategyService, callingChannelService, supplierService, productService, catalogCharacteristicService, offerObjectiveService, productTypeService, deliverableService, subscriberProfileService, subscriberIDService, subscriberGroupEpochReader, deliverableSourceService));
    
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

    private HttpServer restServer;
    private JourneyService journeyService;
    private SegmentationRuleService segmentationRuleService;
    private OfferService offerService;
    private ScoringStrategyService scoringStrategyService;
    private PresentationStrategyService presentationStrategyService;
    private CallingChannelService callingChannelService;
    private SupplierService supplierService;
    private ProductService productService;
    private CatalogCharacteristicService catalogCharacteristicService;
    private OfferObjectiveService offerObjectiveService;
    private ProductTypeService productTypeService;
    private DeliverableService deliverableService;
    private SubscriberProfileService subscriberProfileService;
    private SubscriberIDService subscriberIDService;
    private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
    private DeliverableSourceService deliverableSourceService;

    //
    //  constructor
    //

    private ShutdownHook(HttpServer restServer, JourneyService journeyService, SegmentationRuleService segmentationRuleService, OfferService offerService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, CallingChannelService callingChannelService, SupplierService supplierService, ProductService productService, CatalogCharacteristicService catalogCharacteristicService, OfferObjectiveService offerObjectiveService, ProductTypeService productTypeService, DeliverableService deliverableService, SubscriberProfileService subscriberProfileService, SubscriberIDService subscriberIDService, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, DeliverableSourceService deliverableSourceService)
    {
      this.restServer = restServer;
      this.journeyService = journeyService;
      this.segmentationRuleService = segmentationRuleService;
      this.offerService = offerService;
      this.scoringStrategyService = scoringStrategyService;
      this.presentationStrategyService = presentationStrategyService;
      this.callingChannelService = callingChannelService;
      this.supplierService = supplierService;
      this.productService = productService;
      this.catalogCharacteristicService = catalogCharacteristicService;
      this.offerObjectiveService = offerObjectiveService;
      this.productTypeService = productTypeService;
      this.deliverableService = deliverableService;
      this.subscriberProfileService = subscriberProfileService;
      this.subscriberIDService = subscriberIDService;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
      this.deliverableSourceService = deliverableSourceService;
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
      if (segmentationRuleService != null) segmentationRuleService.stop();
      if (offerService != null) offerService.stop();
      if (scoringStrategyService != null) scoringStrategyService.stop();
      if (presentationStrategyService != null) presentationStrategyService.stop();
      if (callingChannelService != null) callingChannelService.stop();
      if (supplierService != null) supplierService.stop();
      if (productService != null) productService.stop();
      if (catalogCharacteristicService != null) catalogCharacteristicService.stop();
      if (offerObjectiveService != null) offerObjectiveService.stop();
      if (productTypeService != null) productTypeService.stop();
      if (deliverableService != null) deliverableService.stop();
      if (subscriberProfileService != null) subscriberProfileService.stop(); 
      if (subscriberIDService != null) subscriberIDService.stop();
      if (deliverableSourceService != null) deliverableSourceService.stop();

      //
      //  rest server
      //

      if (restServer != null) restServer.stop(1);
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

  private synchronized void handleAPI(API api, HttpExchange exchange) throws IOException
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
                  
                case getCallingChannelProperties:
                  jsonResponse = processGetCallingChannelProperties(userID, jsonRoot);
                  break;

                case getSalesChannels:
                  jsonResponse = processGetSalesChannels(userID, jsonRoot);
                  break;

                case getSupportedDataTypes:
                  jsonResponse = processGetSupportedDataTypes(userID, jsonRoot);
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

                case getSegmentationRuleList:
                  jsonResponse = processGetSegmentationRuleList(userID, jsonRoot, true);
                  break;

                case getSegmentationRuleSummaryList:
                  jsonResponse = processGetSegmentationRuleList(userID, jsonRoot, false);
                  break;

                case getSegmentationRule:
                    jsonResponse = processGetSegmentationRule(userID, jsonRoot);
                    break;

                case putSegmentationRule:
                    jsonResponse = processPutSegmentationRule(userID, jsonRoot);
                    break;

                case removeSegmentationRule:
                    jsonResponse = processRemoveSegmentationRule(userID, jsonRoot);
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

                case getDeliverableList:
                  jsonResponse = processGetDeliverableList(userID, jsonRoot, true);
                  break;

                case getDeliverableSummaryList:
                  jsonResponse = processGetDeliverableList(userID, jsonRoot, false);
                  break;

                case getDeliverable:
                  jsonResponse = processGetDeliverable(userID, jsonRoot);
                  break;

                case getFulfillmentProviders:
                  jsonResponse = processGetFulfillmentProviders(userID, jsonRoot);
                  break;

                case getPaymentMeans:
                  jsonResponse = processGetPaymentMeans(userID, jsonRoot);
                  break;
                  
                case getDashboardCounts:
                  jsonResponse = processGetDashboardCounts(userID, jsonRoot);
                  break;
                
               case getCustomer:
                 jsonResponse = processGetCustomer(userID, jsonRoot);
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
    *  retrieve salesChannels
    *
    *****************************************/

    List<JSONObject> salesChannels = new ArrayList<JSONObject>();
    for (SalesChannel salesChannel : Deployment.getSalesChannels().values())
      {
        JSONObject salesChannelJSON = salesChannel.getJSONRepresentation();
        salesChannels.add(salesChannelJSON);
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

    List<JSONObject> profileCriterionFields = processCriterionFields(CriterionContext.Profile.getCriterionFields());

    /*****************************************
    *
    *  retrieve presentation criterion fields
    *
    *****************************************/

    List<JSONObject> presentationCriterionFields = processCriterionFields(CriterionContext.Presentation.getCriterionFields());
    
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

    List<JSONObject> nodeTypes = processNodeTypes(Deployment.getNodeTypes());

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
    response.put("salesChannels", JSONUtilities.encodeArray(salesChannels));
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

    /*****************************************
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
  *  getSalesChannels
  *
  *****************************************/

  private JSONObject processGetSalesChannels(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve salesChannels
    *
    *****************************************/

    List<JSONObject> salesChannels = new ArrayList<JSONObject>();
    for (SalesChannel salesChannel : Deployment.getSalesChannels().values())
      {
        JSONObject salesChannelJSON = salesChannel.getJSONRepresentation();
        salesChannels.add(salesChannelJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("salesChannels", JSONUtilities.encodeArray(salesChannels));
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

    List<JSONObject> profileCriterionFields = processCriterionFields(CriterionContext.Profile.getCriterionFields());

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

    List<JSONObject> profileCriterionFields = processCriterionFields(CriterionContext.Profile.getCriterionFields());

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

        List<JSONObject> profileCriterionFields = processCriterionFields(CriterionContext.Profile.getCriterionFields());

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

    List<JSONObject> presentationCriterionFields = processCriterionFields(CriterionContext.Presentation.getCriterionFields());

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

    List<JSONObject> presentationCriterionFields = processCriterionFields(CriterionContext.Presentation.getCriterionFields());

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

        List<JSONObject> presentationCriterionFields = processCriterionFields(CriterionContext.Presentation.getCriterionFields());

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

    Map<CriterionField,CriterionField> journeyMetrics = (JSONUtilities.decodeJSONArray(jsonRoot,"journeyMetrics", false) != null) ? Journey.decodeJourneyMetrics(JSONUtilities.decodeJSONArray(jsonRoot,"journeyMetrics", false)) : Collections.<CriterionField,CriterionField>emptyMap();
    Map<String,CriterionField> journeyParameters = (JSONUtilities.decodeJSONArray(jsonRoot,"journeyParameters", false) != null) ? Journey.decodeJourneyParameters(JSONUtilities.decodeJSONArray(jsonRoot,"journeyParameters", false)) : Collections.<String,CriterionField>emptyMap();
    NodeType journeyNodeType = Deployment.getNodeTypes().get(JSONUtilities.decodeString(jsonRoot, "nodeTypeID", true));
    EvolutionEngineEventDeclaration journeyNodeEvent = (JSONUtilities.decodeString(jsonRoot, "eventName", false) != null) ? Deployment.getEvolutionEngineEvents().get(JSONUtilities.decodeString(jsonRoot, "eventName", true)) : null;

    /*****************************************
    *
    *  retrieve journey criterion fields
    *
    *****************************************/

    List<JSONObject> journeyCriterionFields = Collections.<JSONObject>emptyList();
    if (journeyNodeType != null)
      {
        CriterionContext criterionContext = new CriterionContext(journeyMetrics, journeyParameters, journeyNodeType, journeyNodeEvent, false);
        journeyCriterionFields = processCriterionFields(criterionContext.getCriterionFields());
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

    Map<CriterionField,CriterionField> journeyMetrics = (JSONUtilities.decodeJSONArray(jsonRoot,"journeyMetrics", false) != null) ? Journey.decodeJourneyMetrics(JSONUtilities.decodeJSONArray(jsonRoot,"journeyMetrics", false)) : Collections.<CriterionField,CriterionField>emptyMap();
    Map<String,CriterionField> journeyParameters = (JSONUtilities.decodeJSONArray(jsonRoot,"journeyParameters", false) != null) ? Journey.decodeJourneyParameters(JSONUtilities.decodeJSONArray(jsonRoot,"journeyParameters", false)) : Collections.<String,CriterionField>emptyMap();
    NodeType journeyNodeType = Deployment.getNodeTypes().get(JSONUtilities.decodeString(jsonRoot, "nodeTypeID", true));
    EvolutionEngineEventDeclaration journeyNodeEvent = (JSONUtilities.decodeString(jsonRoot, "eventName", false) != null) ? Deployment.getEvolutionEngineEvents().get(JSONUtilities.decodeString(jsonRoot, "eventName", true)) : null;

    /*****************************************
    *
    *  retrieve journey criterion fields
    *
    *****************************************/

    List<JSONObject> journeyCriterionFields = Collections.<JSONObject>emptyList();
    if (journeyNodeType != null)
      {
        CriterionContext criterionContext = new CriterionContext(journeyMetrics, journeyParameters, journeyNodeType, journeyNodeEvent, false);
        journeyCriterionFields = processCriterionFields(criterionContext.getCriterionFields());
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

    Map<CriterionField,CriterionField> journeyMetrics = (JSONUtilities.decodeJSONArray(jsonRoot,"journeyMetrics", false) != null) ? Journey.decodeJourneyMetrics(JSONUtilities.decodeJSONArray(jsonRoot,"journeyMetrics", false)) : Collections.<CriterionField,CriterionField>emptyMap();
    Map<String,CriterionField> journeyParameters = (JSONUtilities.decodeJSONArray(jsonRoot,"journeyParameters", false) != null) ? Journey.decodeJourneyParameters(JSONUtilities.decodeJSONArray(jsonRoot,"journeyParameters", false)) : Collections.<String,CriterionField>emptyMap();
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
            CriterionContext criterionContext = new CriterionContext(journeyMetrics, journeyParameters, journeyNodeType, journeyNodeEvent, false);
            journeyCriterionFields = processCriterionFields(criterionContext.getCriterionFields());
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

  private JSONObject processGetNodeTypes(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve nodeTypes
    *
    *****************************************/

    List<JSONObject> nodeTypes = processNodeTypes(Deployment.getNodeTypes());

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

  private List<JSONObject> processCriterionFields(Map<String,CriterionField> baseCriterionFields)
  {
    /*****************************************
    *
    *  filter out parameter-only data types
    *
    *****************************************/

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
            case StringSetCriterion:
              criterionFields.put(criterionField.getID(), criterionField);
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
        JSONObject criterionFieldJSON = (JSONObject) criterionField.getJSONRepresentation();
        List<JSONObject> availableValues = evaluateAvailableValues(JSONUtilities.decodeJSONArray(criterionFieldJSON, "availableValues", false));
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
        ResolvedFieldType resolvedFieldType = resolvedFieldTypes.get(criterionField.getID());
        List<CriterionField> fields = defaultFieldsForResolvedType.get(resolvedFieldType);
        if (fields == null)
          {
            fields = new ArrayList<CriterionField>();
            defaultFieldsForResolvedType.put(resolvedFieldType, fields);
          }
        fields.add(criterionField);
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

  private List<JSONObject> evaluateComparableFields(String criterionFieldID, JSONObject criterionFieldJSON, List<CriterionField> allFields, boolean singleton)
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
    Set<String> excludedComparableFieldIDs = requestedExcludedComparableFieldIDs != null ? new LinkedHashSet<String>(requestedExcludedComparableFieldIDs) : Collections.<String>emptySet();

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

  private List<JSONObject> processNodeTypes(Map<String,NodeType> nodeTypes)
  {
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
            JSONObject parameterJSON = (JSONObject) ((JSONObject) parameters.get(i)).clone();
            List<JSONObject> availableValues = evaluateAvailableValues(JSONUtilities.decodeJSONArray(parameterJSON, "availableValues", false));
            parameterJSON.put("availableValues", (availableValues != null) ? JSONUtilities.encodeArray(availableValues) : null);
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
                List<JSONObject> availableValues = evaluateAvailableValues(JSONUtilities.decodeJSONArray(parameterJSON, "availableValues", false));
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

  /****************************************
  *
  *  evaluateAvailableValues
  *
  ****************************************/

  private List<JSONObject> evaluateAvailableValues(JSONArray availableValues)
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
                    result.addAll(evaluateEnumeratedValues(matcher.group(1)));
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

  private List<JSONObject> evaluateEnumeratedValues(String reference)
  {
    List<JSONObject> result = new ArrayList<JSONObject>();
    switch (reference)
      {
        case "ratePlans":
          List<String> ratePlans = new ArrayList<String>();
          ratePlans.add("tariff001");
          ratePlans.add("tariff002");
          ratePlans.add("tariff003");
          for (String ratePlan : ratePlans)
            {
              HashMap<String,Object> availableValue = new HashMap<String,Object>();
              availableValue.put("id", ratePlan);
              availableValue.put("display", ratePlan + " (display)");
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

        case "segments":
          Map<String,SubscriberGroupEpoch> subscriberGroupEpochs = subscriberGroupEpochReader.getAll();
          for (String groupName : subscriberGroupEpochs.keySet())
            {
              SubscriberGroupEpoch subscriberGroupEpoch = subscriberGroupEpochs.get(groupName);
              if (subscriberGroupEpoch.getActive() && ! groupName.equals(SubscriberProfile.UniversalControlGroup))
                {
                  HashMap<String,Object> availableValue = new HashMap<String,Object>();
                  availableValue.put("id", groupName);
                  availableValue.put("display", subscriberGroupEpoch.getDisplay());
                  result.add(JSONUtilities.encodeObject(availableValue));
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

        default:
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

    HashMap<String,Object> response = new HashMap<String,Object>();;
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

        Journey journey = new Journey(jsonRoot, GUIManagedObjectType.Journey, epoch, existingJourney);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyService.putJourney(journey, (existingJourney == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", journey.getJourneyID());
        response.put("accepted", journey.getAccepted());
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

        journeyService.putJourney(incompleteObject, (existingJourney == null), userID);

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

        Journey campaign = new Journey(jsonRoot, GUIManagedObjectType.Campaign, epoch, existingCampaign);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyService.putJourney(campaign, (existingCampaign == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", campaign.getJourneyID());
        response.put("accepted", campaign.getAccepted());
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

        journeyService.putJourney(incompleteObject, (existingCampaign == null), userID);

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
  *  processGetSegmentationRuleList
  *
  *****************************************/

  private JSONObject processGetSegmentationRuleList(String userID, JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert segmentationRules
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> segmentationRules = new ArrayList<JSONObject>();
    for (GUIManagedObject segmentationRule : segmentationRuleService.getStoredSegmentationRules())
      {
        segmentationRules.add(segmentationRuleService.generateResponseJSON(segmentationRule, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("segmentationRules", JSONUtilities.encodeArray(segmentationRules));
    return JSONUtilities.encodeObject(response);
  }
                 
  /*****************************************
  *
  *  processGetSegmentationRule
  *
  *****************************************/

  private JSONObject processGetSegmentationRule(String userID, JSONObject jsonRoot)
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

    String segmentationRuleID = JSONUtilities.decodeString(jsonRoot, "id", true);
    
    /*****************************************
    *
    *  retrieve and decorate segmentationRule
    *
    *****************************************/

    GUIManagedObject segmentationRule = segmentationRuleService.getStoredSegmentationRule(segmentationRuleID);
    JSONObject segmentationRuleJSON = segmentationRuleService.generateResponseJSON(segmentationRule, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (segmentationRule != null) ? "ok" : "segmentationRuleNotFound");
    if (segmentationRule != null) response.put("segmentationRule", segmentationRuleJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutSegmentationRule
  *
  *****************************************/

  private JSONObject processPutSegmentationRule(String userID, JSONObject jsonRoot)
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
    *  segmentationRuleID
    *
    *****************************************/
    
    String segmentationRuleID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (segmentationRuleID == null)
      {
        segmentationRuleID = segmentationRuleService.generateSegmentationRuleID();
        jsonRoot.put("id", segmentationRuleID);
      }
    
    /*****************************************
    *
    *  existing segmentationRule
    *
    *****************************************/

    GUIManagedObject existingSegmentationRule = segmentationRuleService.getStoredSegmentationRule(segmentationRuleID);

    /*****************************************
    *
    *  read-only
    *
    *****************************************/

    if (existingSegmentationRule != null && existingSegmentationRule.getReadOnly())
      {
        response.put("id", existingSegmentationRule.getGUIManagedObjectID());
        response.put("accepted", existingSegmentationRule.getAccepted());
        response.put("processing", segmentationRuleService.isActiveSegmentationRule(existingSegmentationRule, now));
        response.put("responseCode", "failedReadOnly");
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  process segmentationRule
    *
    *****************************************/

    long epoch = epochServer.getKey();
    try
      {
        /****************************************
        *
        *  instantiate segmentationRule
        *
        ****************************************/

        SegmentationRule segmentationRule = new SegmentationRule(jsonRoot, epoch, existingSegmentationRule);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        segmentationRuleService.putSegmentationRule(segmentationRule, (existingSegmentationRule == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", segmentationRule.getSegmentationRuleID());
        response.put("accepted", segmentationRule.getAccepted());
        response.put("processing", segmentationRuleService.isActiveSegmentationRule(segmentationRule, now));
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

        segmentationRuleService.putSegmentationRule(incompleteObject, (existingSegmentationRule == null), userID);

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
        
        //
        //  response
        //

        response.put("segmentationRuleID", incompleteObject.getGUIManagedObjectID());
        response.put("responseCode", "segmentationRuleNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  *  processRemoveSegmentationRule
  *
  *****************************************/

  private JSONObject processRemoveSegmentationRule(String userID, JSONObject jsonRoot)
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
    
    String segmentationRuleID = JSONUtilities.decodeString(jsonRoot, "id", true);
    
    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject segmentationRule = segmentationRuleService.getStoredSegmentationRule(segmentationRuleID);
    if (segmentationRule != null && ! segmentationRule.getReadOnly()) segmentationRuleService.removeSegmentationRule(segmentationRuleID, userID);

    /*****************************************
    *
    *  responseCode
    *
    *****************************************/

    String responseCode;
    if (segmentationRule != null && ! segmentationRule.getReadOnly())
      responseCode = "ok";
    else if (segmentationRule != null)
      responseCode = "failedReadOnly";
    else
      responseCode = "segmentationRuleNotFound";

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

        offerService.putOffer(offer, callingChannelService, productService, (existingOffer == null), userID);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", offer.getOfferID());
        response.put("accepted", offer.getAccepted());
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

        offerService.putIncompleteOffer(incompleteObject, (existingOffer == null), userID);

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

        presentationStrategyService.putIncompletePresentationStrategy(incompleteObject, (existingPresentationStrategy == null), userID);

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

        revalidateOffers(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", callingChannel.getCallingChannelID());
        response.put("accepted", callingChannel.getAccepted());
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

        productService.putIncompleteProduct(incompleteObject, (existingProduct == null), userID);

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

        catalogCharacteristicService.putIncompleteCatalogCharacteristic(incompleteObject, (existingCatalogCharacteristic == null), userID);

        //
        //  revalidate dependent objects
        //

        revalidateOffers(now);
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

        offerObjectiveService.putIncompleteOfferObjective(incompleteObject, (existingOfferObjective == null), userID);

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
    *  retrieve and decorate scoring strategy
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

        productTypeService.putIncompleteProductType(incompleteObject, (existingProductType == null), userID);

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

        deliverableService.putIncompleteDeliverable(incompleteObject, (existingDeliverable == null), userID);

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
    *  revalidate offers
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
            offer.validate(callingChannelService, productService, date);
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
    revalidateOfferObjectives(date);
    revalidateProductTypes(date);
    revalidateProducts(date);
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
  *  getFulfillmentProviders
  *
  *****************************************/

  private JSONObject processGetFulfillmentProviders(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve fulfillment providers
    *
    *****************************************/
    
    JSONArray fulfillmentProviders = Deployment.getFulfillmentProvidersJSONArray();
    ArrayList<JSONObject> fulfillmentProvidersList = new ArrayList<>();
    for (int i=0; i<fulfillmentProviders.size(); i++)
      {
        JSONObject fulfillmentProvider = (JSONObject) fulfillmentProviders.get(i);
        fulfillmentProvidersList.add(fulfillmentProvider);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("fulfillmentProviders", JSONUtilities.encodeArray(fulfillmentProvidersList));
    return JSONUtilities.encodeObject(response);
  }  
  
  /*****************************************
  *
  *  getPaymentMeans
  *
  *****************************************/

  private JSONObject processGetPaymentMeans(String userID, JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve payment means
    *
    *****************************************/
    
    JSONArray paymentMeans = Deployment.getPaymentMeansJSONArray();
    ArrayList<JSONObject> paymentMeanList = new ArrayList<>();
    for (int i=0; i<paymentMeans.size(); i++)
      {
        JSONObject paymentMean = (JSONObject) paymentMeans.get(i);
        paymentMeanList.add(paymentMean);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("paymentMeans", JSONUtilities.encodeArray(paymentMeanList));
    return JSONUtilities.encodeObject(response);
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
    response.put("segmentationRuleCount", segmentationRuleService.getStoredSegmentationRules().size());
    response.put("offerCount", offerService.getStoredOffers().size());
    response.put("scoringStrategyCount", scoringStrategyService.getStoredScoringStrategies().size());
    response.put("presentationStrategyCount", presentationStrategyService.getStoredPresentationStrategies().size());
    response.put("callingChannelCount", callingChannelService.getStoredCallingChannels().size());
    response.put("supplierCount", supplierService.getStoredSuppliers().size());
    response.put("productCount", productService.getStoredProducts().size());
    response.put("catalogCharacteristicCount", catalogCharacteristicService.getStoredCatalogCharacteristics().size());
    response.put("offerObjectiveCount", offerObjectiveService.getStoredOfferObjectives().size());
    response.put("productTypeCount", productTypeService.getStoredProductTypes().size());
    response.put("deliverableCount", deliverableService.getStoredDeliverables().size());
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
    
    Map<String, Object> response = new HashMap<String, Object>();
    
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
        log.info("unable to resolve SubscriberID for subscriberTraceControlAlternateID {} and customerID ", subscriberTraceControlAlternateID, customerID);
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
            if (null == baseSubscriberProfile)
              {
                response.put("responseCode", "CustomerNotFound");
              }
            else
              {
                response = baseSubscriberProfile.getProfileMapForGUIPresentation(subscriberGroupEpochReader);
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
        result = subscriberIDService.getSubscriberID(subscriberTraceControlAlternateID, customerID);
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
  *  class GUIManagerException
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

              String deliverableName =  stringKeySerde.deserializer().deserialize(deliverableSourceRecord.topic(), deliverableSourceRecord.key()).getKey();
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
}
