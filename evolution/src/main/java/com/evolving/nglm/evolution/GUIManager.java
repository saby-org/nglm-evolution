/*****************************************************************************
*
*  GUIManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Alarm;
import com.evolving.nglm.core.Alarm.AlarmLevel;
import com.evolving.nglm.core.Alarm.AlarmType;
import com.evolving.nglm.core.LicenseChecker;
import com.evolving.nglm.core.LicenseChecker.LicenseState;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import com.rii.utilities.SystemTime;
import com.rii.utilities.UniqueKeyServer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

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

  private enum API
  {
    getStaticConfiguration,
    getSupportedLanguages,
    getSupportedCurrencies,
    getSupportedTimeUnits,
    getServiceTypes,
    getCallingChannelProperties,
    getSalesChannels,
    getCatalogObjectiveSections,
    getSupportedDataTypes,
    getProfileCriterionFields,
    getProfileCriterionFieldIDs,
    getProfileCriterionField,
    getPresentationCriterionFields,
    getPresentationCriterionFieldIDs,
    getPresentationCriterionField,
    getOfferCategories,
    getOfferTypes,
    getOfferOptimizationAlgorithms,
    getJourneyList,
    getJourneySummaryList,
    getJourney,
    putJourney,
    removeJourney,
    getSegmentationRuleList,
    getSegmentationRuleSummaryList,
    getSegmentationRule,
    putSegmentationRule,
    removeSegmentationRule,
    getOfferList,
    getOfferSummaryList,
    getOffer,
    putOffer,
    removeOffer,
    getPresentationStrategyList,
    getPresentationStrategySummaryList,
    getPresentationStrategy,
    putPresentationStrategy,
    removePresentationStrategy,
    getScoringStrategyList,
    getScoringStrategySummaryList,
    getScoringStrategy,
    putScoringStrategy,
    removeScoringStrategy,
    getCallingChannelList,
    getCallingChannelSummaryList,
    getCallingChannel,
    putCallingChannel,
    removeCallingChannel,
    getSupplierList,
    getSupplierSummaryList,
    getSupplier,
    putSupplier,
    removeSupplier,
    getProductList,
    getProductSummaryList,
    getProduct,
    putProduct,
    removeProduct,
    getCatalogCharacteristicList,
    getCatalogCharacteristicSummaryList,
    getCatalogCharacteristic,
    putCatalogCharacteristic,
    removeCatalogCharacteristic,
    getCatalogObjectiveList,
    getCatalogObjectiveSummaryList,
    getCatalogObjective,
    putCatalogObjective,
    removeCatalogObjective,
    getFulfillmentProviders,
    getOfferDeliverables,
    getPaymentMeans,
    getDashboardCounts;
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
  private CatalogObjectiveService catalogObjectiveService;
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;

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
    String catalogObjectiveTopic = Deployment.getCatalogObjectiveTopic();
    String subscriberGroupEpochTopic = Deployment.getSubscriberGroupEpochTopic();
    
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
    catalogObjectiveService = new CatalogObjectiveService(bootstrapServers, "guimanager-catalogobjectiveservice-" + apiProcessKey, catalogObjectiveTopic, true);
    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("guimanager-subscribergroupepoch", apiProcessKey, bootstrapServers, subscriberGroupEpochTopic, SubscriberGroupEpoch::unpack);

    /*****************************************
    *
    *  services - initialize
    *
    *****************************************/

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
                processPutCallingChannel(callingChannelJSON);
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
                processPutSupplier(supplierJSON);
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
                processPutProduct(productJSON);
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
                processPutCatalogCharacteristic(catalogCharacteristicJSON);
              }
          }
        catch (JSONUtilitiesException e)
          {
            throw new ServerRuntimeException("deployment", e);
          }
      }
    
    //
    //  catalogObjectives
    //

    if (catalogObjectiveService.getStoredCatalogObjectives().size() == 0)
      {
        try
          {
            JSONArray initialCatalogObjectivesJSONArray = Deployment.getInitialCatalogObjectivesJSONArray();
            for (int i=0; i<initialCatalogObjectivesJSONArray.size(); i++)
              {
                JSONObject catalogObjectiveJSON = (JSONObject) initialCatalogObjectivesJSONArray.get(i);
                processPutCatalogObjective(catalogObjectiveJSON);
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
    catalogObjectiveService.start();

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
        restServer.createContext("/nglm-guimanager/getCatalogObjectiveSections", new APIHandler(API.getCatalogObjectiveSections));
        restServer.createContext("/nglm-guimanager/getSupportedDataTypes", new APIHandler(API.getSupportedDataTypes));
        restServer.createContext("/nglm-guimanager/getProfileCriterionFields", new APIHandler(API.getProfileCriterionFields));
        restServer.createContext("/nglm-guimanager/getProfileCriterionFieldIDs", new APIHandler(API.getProfileCriterionFieldIDs));
        restServer.createContext("/nglm-guimanager/getProfileCriterionField", new APIHandler(API.getProfileCriterionField));
        restServer.createContext("/nglm-guimanager/getPresentationCriterionFields", new APIHandler(API.getPresentationCriterionFields));
        restServer.createContext("/nglm-guimanager/getPresentationCriterionFieldIDs", new APIHandler(API.getPresentationCriterionFieldIDs));
        restServer.createContext("/nglm-guimanager/getPresentationCriterionField", new APIHandler(API.getPresentationCriterionField));
        restServer.createContext("/nglm-guimanager/getOfferCategories", new APIHandler(API.getOfferCategories));
        restServer.createContext("/nglm-guimanager/getOfferTypes", new APIHandler(API.getOfferTypes));
        restServer.createContext("/nglm-guimanager/getOfferOptimizationAlgorithms", new APIHandler(API.getOfferOptimizationAlgorithms));
        restServer.createContext("/nglm-guimanager/getJourneyList", new APIHandler(API.getJourneyList));
        restServer.createContext("/nglm-guimanager/getJourneySummaryList", new APIHandler(API.getJourneySummaryList));
        restServer.createContext("/nglm-guimanager/getJourney", new APIHandler(API.getJourney));
        restServer.createContext("/nglm-guimanager/putJourney", new APIHandler(API.putJourney));
        restServer.createContext("/nglm-guimanager/removeJourney", new APIHandler(API.removeJourney));
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
        restServer.createContext("/nglm-guimanager/getCatalogObjectiveList", new APIHandler(API.getCatalogObjectiveList));
        restServer.createContext("/nglm-guimanager/getCatalogObjectiveSummaryList", new APIHandler(API.getCatalogObjectiveSummaryList));
        restServer.createContext("/nglm-guimanager/getCatalogObjective", new APIHandler(API.getCatalogObjective));
        restServer.createContext("/nglm-guimanager/putCatalogObjective", new APIHandler(API.putCatalogObjective));
        restServer.createContext("/nglm-guimanager/removeCatalogObjective", new APIHandler(API.removeCatalogObjective));
        restServer.createContext("/nglm-guimanager/getFulfillmentProviders", new APIHandler(API.getFulfillmentProviders));
        restServer.createContext("/nglm-guimanager/getOfferDeliverables", new APIHandler(API.getOfferDeliverables));
        restServer.createContext("/nglm-guimanager/getPaymentMeans", new APIHandler(API.getPaymentMeans));
        restServer.createContext("/nglm-guimanager/getDashboardCounts", new APIHandler(API.getDashboardCounts));
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
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(restServer, journeyService, segmentationRuleService, offerService, scoringStrategyService, presentationStrategyService, callingChannelService, supplierService, productService, catalogCharacteristicService, catalogObjectiveService, subscriberGroupEpochReader));
    
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
    private CatalogObjectiveService catalogObjectiveService;
    private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;

    //
    //  constructor
    //

    private ShutdownHook(HttpServer restServer, JourneyService journeyService, SegmentationRuleService segmentationRuleService, OfferService offerService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, CallingChannelService callingChannelService, SupplierService supplierService, ProductService productService, CatalogCharacteristicService catalogCharacteristicService, CatalogObjectiveService catalogObjectiveService, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
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
      this.catalogObjectiveService = catalogObjectiveService;
      this.subscriberGroupEpochReader = subscriberGroupEpochReader;
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
      if (catalogObjectiveService != null) catalogObjectiveService.stop();

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
                  jsonResponse = processGetStaticConfiguration(jsonRoot);
                  break;

                case getSupportedLanguages:
                  jsonResponse = processGetSupportedLanguages(jsonRoot);
                  break;

                case getSupportedCurrencies:
                  jsonResponse = processGetSupportedCurrencies(jsonRoot);
                  break;

                case getSupportedTimeUnits:
                  jsonResponse = processGetSupportedTimeUnits(jsonRoot);
                  break;

                case getServiceTypes:
                  jsonResponse = processGetServiceTypes(jsonRoot);
                  break;
                  
                case getCallingChannelProperties:
                  jsonResponse = processGetCallingChannelProperties(jsonRoot);
                  break;

                case getSalesChannels:
                  jsonResponse = processGetSalesChannels(jsonRoot);
                  break;

                case getCatalogObjectiveSections:
                  jsonResponse = processGetCatalogObjectiveSections(jsonRoot);
                  break;

                case getSupportedDataTypes:
                  jsonResponse = processGetSupportedDataTypes(jsonRoot);
                  break;

                case getProfileCriterionFields:
                  jsonResponse = processGetProfileCriterionFields(jsonRoot);
                  break;

                case getProfileCriterionFieldIDs:
                  jsonResponse = processGetProfileCriterionFieldIDs(jsonRoot);
                  break;

                case getProfileCriterionField:
                  jsonResponse = processGetProfileCriterionField(jsonRoot);
                  break;

                case getPresentationCriterionFields:
                  jsonResponse = processGetPresentationCriterionFields(jsonRoot);
                  break;

                case getPresentationCriterionFieldIDs:
                  jsonResponse = processGetPresentationCriterionFieldIDs(jsonRoot);
                  break;

                case getPresentationCriterionField:
                  jsonResponse = processGetPresentationCriterionField(jsonRoot);
                  break;

                case getOfferCategories:
                  jsonResponse = processGetOfferCategories(jsonRoot);
                  break;

                case getOfferTypes:
                  jsonResponse = processGetOfferTypes(jsonRoot);
                  break;

                case getOfferOptimizationAlgorithms:
                  jsonResponse = processGetOfferOptimizationAlgorithms(jsonRoot);
                  break;

                case getJourneyList:
                  jsonResponse = processGetJourneyList(jsonRoot, true);
                  break;

                case getJourneySummaryList:
                  jsonResponse = processGetJourneyList(jsonRoot, false);
                  break;

                case getJourney:
                  jsonResponse = processGetJourney(jsonRoot);
                  break;

                case putJourney:
                  jsonResponse = processPutJourney(jsonRoot);
                  break;

                case removeJourney:
                  jsonResponse = processRemoveJourney(jsonRoot);
                  break;

                case getSegmentationRuleList:
                  jsonResponse = processGetSegmentationRuleList(jsonRoot, true);
                  break;

                case getSegmentationRuleSummaryList:
                  jsonResponse = processGetSegmentationRuleList(jsonRoot, false);
                  break;

                case getSegmentationRule:
                    jsonResponse = processGetSegmentationRule(jsonRoot);
                    break;

                case putSegmentationRule:
                    jsonResponse = processPutSegmentationRule(jsonRoot);
                    break;

                case removeSegmentationRule:
                    jsonResponse = processRemoveSegmentationRule(jsonRoot);
                    break;
                
                case getOfferList:
                  jsonResponse = processGetOfferList(jsonRoot, true);
                  break;

                case getOfferSummaryList:
                  jsonResponse = processGetOfferList(jsonRoot, false);
                  break;

                case getOffer:
                  jsonResponse = processGetOffer(jsonRoot);
                  break;

                case putOffer:
                  jsonResponse = processPutOffer(jsonRoot);
                  break;

                case removeOffer:
                  jsonResponse = processRemoveOffer(jsonRoot);
                  break;

                case getPresentationStrategyList:
                  jsonResponse = processGetPresentationStrategyList(jsonRoot, true);
                  break;

                case getPresentationStrategySummaryList:
                  jsonResponse = processGetPresentationStrategyList(jsonRoot, false);
                  break;

                case getPresentationStrategy:
                  jsonResponse = processGetPresentationStrategy(jsonRoot);
                  break;

                case putPresentationStrategy:
                  jsonResponse = processPutPresentationStrategy(jsonRoot);
                  break;

                case removePresentationStrategy:
                  jsonResponse = processRemovePresentationStrategy(jsonRoot);
                  break;

                case getScoringStrategyList:
                  jsonResponse = processGetScoringStrategyList(jsonRoot, true);
                  break;

                case getScoringStrategySummaryList:
                  jsonResponse = processGetScoringStrategyList(jsonRoot, false);
                  break;

                case getScoringStrategy:
                  jsonResponse = processGetScoringStrategy(jsonRoot);
                  break;

                case putScoringStrategy:
                  jsonResponse = processPutScoringStrategy(jsonRoot);
                  break;

                case removeScoringStrategy:
                  jsonResponse = processRemoveScoringStrategy(jsonRoot);
                  break;

                case getCallingChannelList:
                  jsonResponse = processGetCallingChannelList(jsonRoot, true);
                  break;

                case getCallingChannelSummaryList:
                  jsonResponse = processGetCallingChannelList(jsonRoot, false);
                  break;

                case getCallingChannel:
                  jsonResponse = processGetCallingChannel(jsonRoot);
                  break;

                case putCallingChannel:
                  jsonResponse = processPutCallingChannel(jsonRoot);
                  break;

                case removeCallingChannel:
                  jsonResponse = processRemoveCallingChannel(jsonRoot);
                  break;

                case getSupplierList:
                  jsonResponse = processGetSupplierList(jsonRoot, true);
                  break;

                case getSupplierSummaryList:
                  jsonResponse = processGetSupplierList(jsonRoot, false);
                  break;

                case getSupplier:
                  jsonResponse = processGetSupplier(jsonRoot);
                  break;

                case putSupplier:
                  jsonResponse = processPutSupplier(jsonRoot);
                  break;

                case removeSupplier:
                  jsonResponse = processRemoveSupplier(jsonRoot);
                  break;
                  
                case getProductList:
                  jsonResponse = processGetProductList(jsonRoot, true);
                  break;

                case getProductSummaryList:
                  jsonResponse = processGetProductList(jsonRoot, false);
                  break;

                case getProduct:
                  jsonResponse = processGetProduct(jsonRoot);
                  break;

                case putProduct:
                  jsonResponse = processPutProduct(jsonRoot);
                  break;

                case removeProduct:
                  jsonResponse = processRemoveProduct(jsonRoot);
                  break;
                  
                case getCatalogCharacteristicList:
                  jsonResponse = processGetCatalogCharacteristicList(jsonRoot, true);
                  break;

                case getCatalogCharacteristicSummaryList:
                  jsonResponse = processGetCatalogCharacteristicList(jsonRoot, false);
                  break;

                case getCatalogCharacteristic:
                  jsonResponse = processGetCatalogCharacteristic(jsonRoot);
                  break;

                case putCatalogCharacteristic:
                  jsonResponse = processPutCatalogCharacteristic(jsonRoot);
                  break;

                case removeCatalogCharacteristic:
                  jsonResponse = processRemoveCatalogCharacteristic(jsonRoot);
                  break;
                  
                case getCatalogObjectiveList:
                  jsonResponse = processGetCatalogObjectiveList(jsonRoot, true);
                  break;

                case getCatalogObjectiveSummaryList:
                  jsonResponse = processGetCatalogObjectiveList(jsonRoot, false);
                  break;

                case getCatalogObjective:
                  jsonResponse = processGetCatalogObjective(jsonRoot);
                  break;

                case putCatalogObjective:
                  jsonResponse = processPutCatalogObjective(jsonRoot);
                  break;

                case removeCatalogObjective:
                  jsonResponse = processRemoveCatalogObjective(jsonRoot);
                  break;

                case getFulfillmentProviders:
                  jsonResponse = processGetFulfillmentProviders(jsonRoot);
                  break;

                case getOfferDeliverables:
                  jsonResponse = processGetOfferDeliverables(jsonRoot);
                  break;

                case getPaymentMeans:
                  jsonResponse = processGetPaymentMeans(jsonRoot);
                  break;
                  
                case getDashboardCounts:
                  jsonResponse = processGetDashboardCounts(jsonRoot);
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
    catch (org.json.simple.parser.ParseException | IOException | ServerException | RuntimeException e )
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

  private JSONObject processGetStaticConfiguration(JSONObject jsonRoot)
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
    *  retrieve catalogObjectiveSections
    *
    *****************************************/

    List<JSONObject> catalogObjectiveSections = new ArrayList<JSONObject>();
    for (CatalogObjectiveSection catalogObjectiveSection : Deployment.getCatalogObjectiveSections().values())
      {
        JSONObject catalogObjectiveSectionJSON = catalogObjectiveSection.getJSONRepresentation();
        catalogObjectiveSections.add(catalogObjectiveSectionJSON);
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

    List<JSONObject> profileCriterionFields = processCriterionFields(Deployment.getProfileCriterionFields());

    /*****************************************
    *
    *  retrieve presentation criterion fields
    *
    *****************************************/

    List<JSONObject> presentationCriterionFields = processCriterionFields(Deployment.getPresentationCriterionFields());
    
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
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("supportedLanguages", JSONUtilities.encodeArray(supportedLanguages));
    response.put("supportedCurrencies", JSONUtilities.encodeArray(supportedCurrencies));
    response.put("callingChannelProperties", JSONUtilities.encodeArray(callingChannelProperties));
    response.put("salesChannels", JSONUtilities.encodeArray(salesChannels));
    response.put("catalogObjectiveSections", JSONUtilities.encodeArray(catalogObjectiveSections));
    response.put("supportedDataTypes", JSONUtilities.encodeArray(supportedDataTypes));
    response.put("profileCriterionFields", JSONUtilities.encodeArray(profileCriterionFields));
    response.put("presentationCriterionFields", JSONUtilities.encodeArray(presentationCriterionFields));
    response.put("offerCategories", JSONUtilities.encodeArray(offerCategories));
    response.put("offerTypes", JSONUtilities.encodeArray(offerTypes));
    response.put("offerOptimizationAlgorithms", JSONUtilities.encodeArray(offerOptimizationAlgorithms));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedLanguages
  *
  *****************************************/

  private JSONObject processGetSupportedLanguages(JSONObject jsonRoot)
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

  private JSONObject processGetSupportedCurrencies(JSONObject jsonRoot)
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

  private JSONObject processGetSupportedTimeUnits(JSONObject jsonRoot)
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

  private JSONObject processGetServiceTypes(JSONObject jsonRoot)
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

  private JSONObject processGetCallingChannelProperties(JSONObject jsonRoot)
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

  private JSONObject processGetSalesChannels(JSONObject jsonRoot)
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
  *  getCatalogObjectiveSections
  *
  *****************************************/

  private JSONObject processGetCatalogObjectiveSections(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve catalogObjectiveSections
    *
    *****************************************/

    List<JSONObject> catalogObjectiveSections = new ArrayList<JSONObject>();
    for (CatalogObjectiveSection catalogObjectiveSection : Deployment.getCatalogObjectiveSections().values())
      {
        JSONObject catalogObjectiveSectionJSON = catalogObjectiveSection.getJSONRepresentation();
        catalogObjectiveSections.add(catalogObjectiveSectionJSON);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("catalogObjectiveSections", JSONUtilities.encodeArray(catalogObjectiveSections));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getSupportedDataTypes
  *
  *****************************************/

  private JSONObject processGetSupportedDataTypes(JSONObject jsonRoot)
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

  private JSONObject processGetProfileCriterionFields(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve profile criterion fields
    *
    *****************************************/

    List<JSONObject> profileCriterionFields = processCriterionFields(Deployment.getProfileCriterionFields());

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

  private JSONObject processGetProfileCriterionFieldIDs(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve profile criterion fields
    *
    *****************************************/

    List<JSONObject> profileCriterionFields = processCriterionFields(Deployment.getProfileCriterionFields());

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

  private JSONObject processGetProfileCriterionField(JSONObject jsonRoot)
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

        List<JSONObject> profileCriterionFields = processCriterionFields(Deployment.getProfileCriterionFields());

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

  private JSONObject processGetPresentationCriterionFields(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve presentation criterion fields
    *
    *****************************************/

    List<JSONObject> presentationCriterionFields = processCriterionFields(Deployment.getPresentationCriterionFields());

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

  private JSONObject processGetPresentationCriterionFieldIDs(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve presentation criterion fields
    *
    *****************************************/

    List<JSONObject> presentationCriterionFields = processCriterionFields(Deployment.getPresentationCriterionFields());

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

  private JSONObject processGetPresentationCriterionField(JSONObject jsonRoot)
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

        List<JSONObject> presentationCriterionFields = processCriterionFields(Deployment.getPresentationCriterionFields());

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
  *  getOfferCategories
  *
  *****************************************/

  private JSONObject processGetOfferCategories(JSONObject jsonRoot)
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

  private JSONObject processGetOfferTypes(JSONObject jsonRoot)
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

  private JSONObject processGetOfferOptimizationAlgorithms(JSONObject jsonRoot)
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
  *  processCriterionFields
  *
  *****************************************/

  private List<JSONObject> processCriterionFields(Map<String,CriterionField> criterionFields)
  {
    /****************************************
    *
    *  resolve field data types
    *
    ****************************************/

    Map<String, ResolvedFieldType> resolvedFieldTypes = new LinkedHashMap<String, ResolvedFieldType>();
    Map<String, List<JSONObject>> resolvedAvailableValues = new LinkedHashMap<String, List<JSONObject>>();
    for (CriterionField criterionField : criterionFields.values())
      {
        List<JSONObject> availableValues = evaluateAvailableValues(criterionField);
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

  /****************************************
  *
  *  evaluateAvailableValues
  *
  ****************************************/

  private List<JSONObject> evaluateAvailableValues(CriterionField criterionField)
  {
    JSONObject criterionFieldJSON = (JSONObject) criterionField.getJSONRepresentation();
    JSONArray availableValues = JSONUtilities.decodeJSONArray(criterionFieldJSON, "availableValues", false);
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

  private JSONObject processGetJourneyList(JSONObject jsonRoot, boolean fullDetails)
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
        journeys.add(journeyService.generateResponseJSON(journey, fullDetails, now));
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

  private JSONObject processGetJourney(JSONObject jsonRoot)
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

  private JSONObject processPutJourney(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
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
    *  process journey
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    long epoch = epochServer.getKey();
    try
      {
        /*****************************************
        *
        *  existing journey
        *
        *****************************************/

        GUIManagedObject existingJourney = journeyService.getStoredJourney(journeyID);

        /****************************************
        *
        *  instantiate journey
        *
        ****************************************/

        Journey journey = new Journey(jsonRoot, epoch, existingJourney);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        journeyService.putJourney(journey);

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

        IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch);

        //
        //  store
        //

        journeyService.putJourney(incompleteObject);

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

  private JSONObject processRemoveJourney(JSONObject jsonRoot)
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
    if (journey != null) journeyService.removeJourney(journeyID);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (journey != null) ? "ok" : "journeyNotFound");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetSegmentationRuleList
  *
  *****************************************/

  private JSONObject processGetSegmentationRuleList(JSONObject jsonRoot, boolean fullDetails)
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

  private JSONObject processGetSegmentationRule(JSONObject jsonRoot)
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

  private JSONObject processPutSegmentationRule(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
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
    *  process segmentationRule
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    long epoch = epochServer.getKey();
    try
      {
        /*****************************************
        *
        *  existing segmentationRule
        *
        *****************************************/

        GUIManagedObject existingSegmentationRule = segmentationRuleService.getStoredSegmentationRule(segmentationRuleID);

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

        segmentationRuleService.putSegmentationRule(segmentationRule);

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

        segmentationRuleService.putSegmentationRule(incompleteObject);

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

  private JSONObject processRemoveSegmentationRule(JSONObject jsonRoot)
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
    if (segmentationRule != null) segmentationRuleService.removeSegmentationRule(segmentationRuleID);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (segmentationRule != null) ? "ok" : "segmentationRuleNotFound");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetOfferList
  *
  *****************************************/

  private JSONObject processGetOfferList(JSONObject jsonRoot, boolean fullDetails)
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

  private JSONObject processGetOffer(JSONObject jsonRoot)
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

  private JSONObject processPutOffer(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
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
    *  process offer
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    long epoch = epochServer.getKey();
    try
      {
        /*****************************************
        *
        *  existing offer
        *
        *****************************************/

        GUIManagedObject existingOffer = offerService.getStoredOffer(offerID);

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

        offerService.putOffer(offer, callingChannelService, productService);

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

        offerService.putIncompleteOffer(incompleteObject);

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

  private JSONObject processRemoveOffer(JSONObject jsonRoot)
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
    if (offer != null) offerService.removeOffer(offerID);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (offer != null) ? "ok" : "offerNotFound");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetPresentationStrategyList
  *
  *****************************************/

  private JSONObject processGetPresentationStrategyList(JSONObject jsonRoot, boolean fullDetails)
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

  private JSONObject processGetPresentationStrategy(JSONObject jsonRoot)
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

  private JSONObject processPutPresentationStrategy(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
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
    *  process presentationStrategy
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    long epoch = epochServer.getKey();
    try
      {
        /*****************************************
        *
        *  existing presentationStrategy
        *
        *****************************************/

        GUIManagedObject existingPresentationStrategy = presentationStrategyService.getStoredPresentationStrategy(presentationStrategyID);

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

        presentationStrategyService.putPresentationStrategy(presentationStrategy, scoringStrategyService);

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

        presentationStrategyService.putIncompletePresentationStrategy(incompleteObject);

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

  private JSONObject processRemovePresentationStrategy(JSONObject jsonRoot)
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
    if (presentationStrategy != null) presentationStrategyService.removePresentationStrategy(presentationStrategyID);

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (presentationStrategy != null) ? "ok" : "presentationStrategyNotFound");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetScoringStrategyList
  *
  *****************************************/

  private JSONObject processGetScoringStrategyList(JSONObject jsonRoot, boolean fullDetails)
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

  private JSONObject processGetScoringStrategy(JSONObject jsonRoot)
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

  private JSONObject processPutScoringStrategy(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
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
    *  process scoringStrategy
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    long epoch = epochServer.getKey();
    try
      {
        /*****************************************
        *
        *  existing scoringStrategy
        *
        *****************************************/

        GUIManagedObject existingScoringStrategy = scoringStrategyService.getStoredScoringStrategy(scoringStrategyID);

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

        scoringStrategyService.putScoringStrategy(scoringStrategy);

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

        scoringStrategyService.putScoringStrategy(incompleteObject);

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

  private JSONObject processRemoveScoringStrategy(JSONObject jsonRoot)
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
    if (scoringStrategy != null) scoringStrategyService.removeScoringStrategy(scoringStrategyID);

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

    response.put("responseCode", (scoringStrategy != null) ? "ok" : "scoringStrategyNotFound");
    return JSONUtilities.encodeObject(response);
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
            presentationStrategy.validateScoringStrategies(scoringStrategyService, date);
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
        presentationStrategyService.putGUIManagedObject(modifiedPresentationStrategy, date);
      }
  }

  /*****************************************
  *
  *  processGetCallingChannelList
  *
  *****************************************/

  private JSONObject processGetCallingChannelList(JSONObject jsonRoot, boolean fullDetails)
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

  private JSONObject processGetCallingChannel(JSONObject jsonRoot)
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

  private JSONObject processPutCallingChannel(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
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
    *  process callingChannel
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    long epoch = epochServer.getKey();
    try
      {
        /*****************************************
        *
        *  existing callingChannel
        *
        *****************************************/

        GUIManagedObject existingCallingChannel = callingChannelService.getStoredCallingChannel(callingChannelID);

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

        callingChannelService.putCallingChannel(callingChannel);

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

        callingChannelService.putCallingChannel(incompleteObject);

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

  private JSONObject processRemoveCallingChannel(JSONObject jsonRoot)
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
    if (callingChannel != null) callingChannelService.removeCallingChannel(callingChannelID);

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

    response.put("responseCode", (callingChannel != null) ? "ok" : "callingChannelNotFound");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetSupplierList
  *
  *****************************************/

  private JSONObject processGetSupplierList(JSONObject jsonRoot, boolean fullDetails)
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

  private JSONObject processGetSupplier(JSONObject jsonRoot)
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

  private JSONObject processPutSupplier(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
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
    *  process supplier
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    long epoch = epochServer.getKey();
    try
      {
        /*****************************************
        *
        *  existing supplier
        *
        *****************************************/

        GUIManagedObject existingSupplier = supplierService.getStoredSupplier(supplierID);

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

        supplierService.putSupplier(supplier);

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

        supplierService.putSupplier(incompleteObject);

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

  private JSONObject processRemoveSupplier(JSONObject jsonRoot)
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
    if (supplier != null) supplierService.removeSupplier(supplierID);

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

    response.put("responseCode", (supplier != null) ? "ok" : "supplierNotFound");
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processGetProductList
  *
  *****************************************/

  private JSONObject processGetProductList(JSONObject jsonRoot, boolean fullDetails)
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

  private JSONObject processGetProduct(JSONObject jsonRoot)
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

  private JSONObject processPutProduct(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
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
    *  process product
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    long epoch = epochServer.getKey();
    try
      {
        /*****************************************
        *
        *  existing product
        *
        *****************************************/

        GUIManagedObject existingProduct = productService.getStoredProduct(productID);

        /****************************************
        *
        *  instantiate product
        *
        ****************************************/

        Product product = new Product(jsonRoot, epoch, existingProduct);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        productService.putProduct(product, supplierService);

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

        productService.putIncompleteProduct(incompleteObject);

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

  private JSONObject processRemoveProduct(JSONObject jsonRoot)
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
    if (product != null) productService.removeProduct(productID);

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

    response.put("responseCode", (product != null) ? "ok" : "productNotFound");
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processGetCatalogCharacteristicList
  *
  *****************************************/

  private JSONObject processGetCatalogCharacteristicList(JSONObject jsonRoot, boolean fullDetails)
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

  private JSONObject processGetCatalogCharacteristic(JSONObject jsonRoot)
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

  private JSONObject processPutCatalogCharacteristic(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
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
    *  process catalogCharacteristic
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    long epoch = epochServer.getKey();
    try
      {
        /*****************************************
        *
        *  existing catalogCharacteristic
        *
        *****************************************/

        GUIManagedObject existingCatalogCharacteristic = catalogCharacteristicService.getStoredCatalogCharacteristic(catalogCharacteristicID);

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

        catalogCharacteristicService.putCatalogCharacteristic(catalogCharacteristic);

        /*****************************************
        *
        *  revalidateOffers
        *
        *****************************************/

        // DEW TBD - revalidateOffers(now);

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

        catalogCharacteristicService.putIncompleteCatalogCharacteristic(incompleteObject);

        //
        //  revalidateOffers
        //

        // DEW TBD - revalidateOffers(now);

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

  private JSONObject processRemoveCatalogCharacteristic(JSONObject jsonRoot)
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
    if (catalogCharacteristic != null) catalogCharacteristicService.removeCatalogCharacteristic(catalogCharacteristicID);

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

    response.put("responseCode", (catalogCharacteristic != null) ? "ok" : "catalogCharacterisicNotFound");
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  processGetCatalogObjectiveList
  *
  *****************************************/

  private JSONObject processGetCatalogObjectiveList(JSONObject jsonRoot, boolean fullDetails)
  {
    /*****************************************
    *
    *  retrieve and convert catalogObjectives
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    List<JSONObject> catalogObjectives = new ArrayList<JSONObject>();
    for (GUIManagedObject catalogObjective : catalogObjectiveService.getStoredCatalogObjectives())
      {
        catalogObjectives.add(catalogObjectiveService.generateResponseJSON(catalogObjective, fullDetails, now));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("catalogObjectives", JSONUtilities.encodeArray(catalogObjectives));
    return JSONUtilities.encodeObject(response);
  }
                 
  /*****************************************
  *
  *  processGetCatalogObjective
  *
  *****************************************/

  private JSONObject processGetCatalogObjective(JSONObject jsonRoot)
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

    String catalogObjectiveID = JSONUtilities.decodeString(jsonRoot, "id", true);
    
    /*****************************************
    *
    *  retrieve and decorate scoring strategy
    *
    *****************************************/

    GUIManagedObject catalogObjective = catalogObjectiveService.getStoredCatalogObjective(catalogObjectiveID);
    JSONObject catalogObjectiveJSON = catalogObjectiveService.generateResponseJSON(catalogObjective, true, SystemTime.getCurrentTime());

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("responseCode", (catalogObjective != null) ? "ok" : "catalogObjectiveNotFound");
    if (catalogObjective != null) response.put("catalogObjective", catalogObjectiveJSON);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  processPutCatalogObjective
  *
  *****************************************/

  private JSONObject processPutCatalogObjective(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
    HashMap<String,Object> response = new HashMap<String,Object>();
    
    /*****************************************
    *
    *  catalogObjectiveID
    *
    *****************************************/
    
    String catalogObjectiveID = JSONUtilities.decodeString(jsonRoot, "id", false);
    if (catalogObjectiveID == null)
      {
        catalogObjectiveID = catalogObjectiveService.generateCatalogObjectiveID();
        jsonRoot.put("id", catalogObjectiveID);
      }
    
    /*****************************************
    *
    *  process catalogObjective
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    long epoch = epochServer.getKey();
    try
      {
        /*****************************************
        *
        *  existing catalogObjective
        *
        *****************************************/

        GUIManagedObject existingCatalogObjective = catalogObjectiveService.getStoredCatalogObjective(catalogObjectiveID);

        /****************************************
        *
        *  instantiate catalogObjective
        *
        ****************************************/

        CatalogObjective catalogObjective = new CatalogObjective(jsonRoot, epoch, existingCatalogObjective);

        /*****************************************
        *
        *  store
        *
        *****************************************/

        catalogObjectiveService.putCatalogObjective(catalogObjective);

        /*****************************************
        *
        *  revalidateOffers
        *
        *****************************************/

        // DEW TBD - revalidateOffers(now);

        /*****************************************
        *
        *  response
        *
        *****************************************/

        response.put("id", catalogObjective.getCatalogObjectiveID());
        response.put("accepted", catalogObjective.getAccepted());
        response.put("processing", catalogObjectiveService.isActiveCatalogObjective(catalogObjective, now));
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

        catalogObjectiveService.putIncompleteCatalogObjective(incompleteObject);

        //
        //  revalidateOffers
        //

        // DEW TBD - revalidateOffers(now);

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
        response.put("responseCode", "catalogObjectiveNotValid");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }
  }
  
  /*****************************************
  *
  *  processRemoveCatalogObjective
  *
  *****************************************/

  private JSONObject processRemoveCatalogObjective(JSONObject jsonRoot)
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
    
    String catalogObjectiveID = JSONUtilities.decodeString(jsonRoot, "id", true);
    
    /*****************************************
    *
    *  remove
    *
    *****************************************/

    GUIManagedObject catalogObjective = catalogObjectiveService.getStoredCatalogObjective(catalogObjectiveID);
    if (catalogObjective != null) catalogObjectiveService.removeCatalogObjective(catalogObjectiveID);

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

    response.put("responseCode", (catalogObjective != null) ? "ok" : "catalogObjectiveNotFound");
    return JSONUtilities.encodeObject(response);
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
            offer.validateCallingChannels(callingChannelService, date);
            offer.validateProducts(productService, date);
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
        offerService.putGUIManagedObject(modifiedOffer, date);
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
            Product product = new Product(existingProduct.getJSONRepresentation(), epoch, existingProduct);
            product.validateSupplier(supplierService, date);
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
        productService.putGUIManagedObject(modifiedProduct, date);
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
  *  getFulfillmentProviders
  *
  *****************************************/

  private JSONObject processGetFulfillmentProviders(JSONObject jsonRoot)
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
  *  getOfferDeliverables
  *
  *****************************************/

  private JSONObject processGetOfferDeliverables(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  retrieve offerDeliverables 
    *
    *****************************************/
    JSONArray offerDeliverables = Deployment.getOfferDeliverablesJSONArray();
    ArrayList<JSONObject> offerDeliverableList = new ArrayList<>();
    for (int i=0; i<offerDeliverables.size(); i++)
      {
        JSONObject deliverable = (JSONObject) offerDeliverables.get(i);
        offerDeliverableList.add(deliverable);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("offerDeliverables", JSONUtilities.encodeArray(offerDeliverableList));
    return JSONUtilities.encodeObject(response);
  }
  
  /*****************************************
  *
  *  getPaymentMeans
  *
  *****************************************/

  private JSONObject processGetPaymentMeans(JSONObject jsonRoot)
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

  private JSONObject processGetDashboardCounts(JSONObject jsonRoot)
  {
    HashMap<String,Object> response = new HashMap<String,Object>();;
    response.put("responseCode", "ok");
    response.put("journeyCount", journeyService.getStoredJourneys().size());
    response.put("segmentationRuleCount", segmentationRuleService.getStoredSegmentationRules().size());
    response.put("offerCount", offerService.getStoredOffers().size());
    response.put("scoringStrategyCount", scoringStrategyService.getStoredScoringStrategies().size());
    response.put("presentationStrategyCount", presentationStrategyService.getStoredPresentationStrategies().size());
    response.put("callingChannelCount", callingChannelService.getStoredCallingChannels().size());
    response.put("supplierCount", supplierService.getStoredSuppliers().size());
    response.put("productCount", productService.getStoredProducts().size());
    response.put("catalogCharacteristicCount", catalogCharacteristicService.getStoredCatalogCharacteristics().size());
    response.put("catalogObjectiveCount", catalogObjectiveService.getStoredCatalogObjectives().size());
    return JSONUtilities.encodeObject(response);
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
    *  constructor - excpetion
    *
    *****************************************/

    public GUIManagerException(Throwable e)
    {
      super(e.getMessage(), e);
      this.responseParameter = null;
    }
  }
}
