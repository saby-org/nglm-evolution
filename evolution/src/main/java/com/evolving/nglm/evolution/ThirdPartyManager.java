/*****************************************************************************
 *
 *  ThirdPartyManager.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Alarm;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.KStreamsUniqueKeyServer;
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
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryRequest.ActivityType;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManager.CustomerStatusInJourney;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.Journey.TargetingType;
import com.evolving.nglm.evolution.SubscriberProfile.ValidateUpdateProfileRequestException;
import com.evolving.nglm.evolution.JourneyHistory.NodeHistory;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;
import com.evolving.nglm.evolution.Token.TokenStatus;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class ThirdPartyManager 
{
  /*****************************************
   *
   *  ProductID
   *
   *****************************************/

  public static String ProductID = "Evolution-ThirdPartyManager";

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ThirdPartyManager.class);

  /*****************************************
   *
   *  data
   *
   *****************************************/

  private KafkaProducer<byte[], byte[]> kafkaProducer;
  private OfferService offerService;
  private SubscriberProfileService subscriberProfileService;
  private SegmentationDimensionService segmentationDimensionService;
  private JourneyService journeyService;
  private JourneyObjectiveService journeyObjectiveService;
  private OfferObjectiveService offerObjectiveService;
  private SubscriberMessageTemplateService subscriberMessageTemplateService;
  private SalesChannelService salesChannelService;
  private SubscriberIDService subscriberIDService;
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private static final int RESTAPIVersion = 1;
  private HttpServer restServer;
  private static Map<String, ThirdPartyMethodAccessLevel> methodPermissionsMapper = new LinkedHashMap<String,ThirdPartyMethodAccessLevel>();
  private static Map<String, Constructor<? extends EvolutionEngineEvent>> JSON3rdPartyEventsConstructor = new HashMap<>();
  private static Integer authResponseCacheLifetimeInMinutes = null;
  private static final String GENERIC_RESPONSE_CODE = "responseCode";
  private static final String GENERIC_RESPONSE_MSG = "responseMessage";
  private String getCustomerAlternateID;
  private static final String REQUEST_DATE_PATTERN = "\\d{4}-\\d{2}-\\d{2}"; //Represents exact yyyy-MM-dd
  private static final String REQUEST_DATE_FORMAT= "yyyy-MM-dd";

  /*****************************************
   *
   *  configuration
   *
   *****************************************/

  private int httpTimeout = 5000;
  private String fwkServer = null;
  RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(httpTimeout).setSocketTimeout(httpTimeout).setConnectionRequestTimeout(httpTimeout).build();

  /*****************************************
   *
   *  enum
   *
   *****************************************/

  private enum API
  {
    ping,
    getCustomer,
    getCustomerBDRs,
    getCustomerODRs,
    getCustomerMessages,
    getCustomerJourneys,
    getCustomerCampaigns,
    getOffersList,
    getActiveOffer,
    getActiveOffers,
    getCustomerAvailableCampaigns,
    updateCustomer,
    getCustomerNBOs,
    getCustomerNBOsTokens,
    getTokensCodesList,
    acceptOffer,
    triggerEvent,
    enterCampaign;
  }

  //
  //  license
  //

  private LicenseChecker licenseChecker = null;

  //
  //  statistics
  //

  ThirdPartyAccessStatistics accessStatistics = null;

  //
  //  authCache
  //

  TimebasedCache<ThirdPartyCredential, AuthenticatedResponse> authCache = null;

  /*****************************************
   *
   *  main
   *
   *****************************************/

  public static void main(String[] args)
  {
    NGLMRuntime.initialize();
    ThirdPartyManager thirdPartyManager = new ThirdPartyManager();
    thirdPartyManager.start(args);
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
    String fwkServer = args[3];
    int threadPoolSize = parseInteger("threadPoolSize", args[4]);
    String nodeID = System.getProperty("nglm.license.nodeid");
    String offerTopic = Deployment.getOfferTopic();
    String subscriberGroupEpochTopic = Deployment.getSubscriberGroupEpochTopic();
    String journeyTopic = Deployment.getJourneyTopic();
    String journeyObjectiveTopic = Deployment.getJourneyObjectiveTopic();
    String offerObjectiveTopic = Deployment.getOfferObjectiveTopic();
    String segmentationDimensionTopic = Deployment.getSegmentationDimensionTopic();
    String redisServer = Deployment.getRedisSentinels();
    String subscriberProfileEndpoints = Deployment.getSubscriberProfileEndpoints();
    methodPermissionsMapper = Deployment.getThirdPartyMethodPermissionsMap();
    authResponseCacheLifetimeInMinutes = Deployment.getAuthResponseCacheLifetimeInMinutes() == null ? new Integer(0) : Deployment.getAuthResponseCacheLifetimeInMinutes();
    getCustomerAlternateID = Deployment.getGetCustomerAlternateID();

    //
    //  log
    //

    log.info("main START: {} {} {} {} {} {}", apiProcessKey, bootstrapServers, apiRestPort, fwkServer, threadPoolSize, authResponseCacheLifetimeInMinutes);

    /*****************************************
     *
     *  FWK Server
     *
     *****************************************/

    this.fwkServer = fwkServer;

    //
    //  license
    //

    licenseChecker = new LicenseChecker(ProductID, nodeID, Deployment.getZookeeperRoot(), Deployment.getZookeeperConnect());

    //
    //  statistics
    //

    try
    {
      accessStatistics = new ThirdPartyAccessStatistics("thirdpartymanager-" + apiProcessKey);
    }
    catch (ServerException e)
    {
      throw new ServerRuntimeException("could not initialize access statistics", e);
    }

    //
    // authCache
    //

    authCache = TimebasedCache.getInstance(60000*authResponseCacheLifetimeInMinutes);

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
     *  services
     *
     *****************************************/

    //
    //  construct
    //

    offerService = new OfferService(bootstrapServers, "thirdpartymanager-offerservice-" + apiProcessKey, offerTopic, false);
    subscriberProfileService = new EngineSubscriberProfileService(subscriberProfileEndpoints);
    segmentationDimensionService = new SegmentationDimensionService(bootstrapServers, "thirdpartymanager-segmentationDimensionservice-001", segmentationDimensionTopic, false);
    journeyService = new JourneyService(bootstrapServers, "thirdpartymanager-journeyservice-" + apiProcessKey, journeyTopic, false);
    journeyObjectiveService = new JourneyObjectiveService(bootstrapServers, "thirdpartymanager-journeyObjectiveService-" + apiProcessKey, journeyObjectiveTopic, false);
    offerObjectiveService = new OfferObjectiveService(bootstrapServers, "thirdpartymanager-offerObjectiveService-" + apiProcessKey, offerObjectiveTopic, false);
    subscriberMessageTemplateService = new SubscriberMessageTemplateService(bootstrapServers, "thirdpartymanager-subscribermessagetemplateservice-" + apiProcessKey, Deployment.getSubscriberMessageTemplateTopic(), false);
    salesChannelService = new SalesChannelService(bootstrapServers, "thirdpartymanager-salesChannelService-" + apiProcessKey, Deployment.getSalesChannelTopic(), false);
    subscriberIDService = new SubscriberIDService(redisServer, "thirdpartymanager-" + apiProcessKey);
    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("thirdpartymanager-subscribergroupepoch", apiProcessKey, bootstrapServers, subscriberGroupEpochTopic, SubscriberGroupEpoch::unpack);

    //
    //  start
    //

    offerService.start();
    subscriberProfileService.start();
    segmentationDimensionService.start();
    journeyService.start();
    journeyObjectiveService.start();
    offerObjectiveService.start();
    subscriberMessageTemplateService.start();
    salesChannelService.start();

    /*****************************************
     *
     *  REST interface -- server and handlers
     *
     *****************************************/

    try
    {
      InetSocketAddress addr = new InetSocketAddress(apiRestPort);
      restServer = HttpServer.create(addr, 0);
      restServer.createContext("/nglm-thirdpartymanager/ping", new APIHandler(API.ping));
      restServer.createContext("/nglm-thirdpartymanager/getCustomer", new APIHandler(API.getCustomer));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerBDRs", new APIHandler(API.getCustomerBDRs));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerODRs", new APIHandler(API.getCustomerODRs));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerMessages", new APIHandler(API.getCustomerMessages));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerJourneys", new APIHandler(API.getCustomerJourneys));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerCampaigns", new APIHandler(API.getCustomerCampaigns));
      restServer.createContext("/nglm-thirdpartymanager/getOffersList", new APIHandler(API.getOffersList));
      restServer.createContext("/nglm-thirdpartymanager/getActiveOffer", new APIHandler(API.getActiveOffer));
      restServer.createContext("/nglm-thirdpartymanager/getActiveOffers", new APIHandler(API.getActiveOffers));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerAvailableCampaigns", new APIHandler(API.getCustomerAvailableCampaigns));
      restServer.createContext("/nglm-thirdpartymanager/updateCustomer", new APIHandler(API.updateCustomer));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerNBOs", new APIHandler(API.getCustomerNBOs));
      restServer.createContext("/nglm-thirdpartymanager/getCustomerNBOsTokens", new APIHandler(API.getCustomerNBOsTokens));
      restServer.createContext("/nglm-thirdpartymanager/getTokensCodesList", new APIHandler(API.getTokensCodesList));
      restServer.createContext("/nglm-thirdpartymanager/acceptOffer", new APIHandler(API.acceptOffer));
      restServer.createContext("/nglm-thirdpartymanager/triggerEvent", new APIHandler(API.triggerEvent));
      restServer.createContext("/nglm-thirdpartymanager/enterCampaign", new APIHandler(API.enterCampaign));
      restServer.setExecutor(Executors.newFixedThreadPool(threadPoolSize));
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

    NGLMRuntime.addShutdownHook(new ShutdownHook(kafkaProducer, restServer, offerService, subscriberProfileService, segmentationDimensionService, journeyService, journeyObjectiveService, offerObjectiveService, subscriberMessageTemplateService, salesChannelService, subscriberIDService, subscriberGroupEpochReader));

    /*****************************************
     *
     *  log restServerStarted
     *
     *****************************************/

    log.info("main restServerStarted");
    log.info("methodPermissionsMapper : {} ", methodPermissionsMapper);
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
    private OfferService offerService;
    private SubscriberProfileService subscriberProfileService;
    private SegmentationDimensionService segmentationDimensionService;
    private JourneyService journeyService;
    private JourneyObjectiveService journeyObjectiveService;
    private OfferObjectiveService offerObjectiveService;
    private SubscriberMessageTemplateService subscriberMessageTemplateService;
    private SalesChannelService salesChannelService;
    private SubscriberIDService subscriberIDService;
    private ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader;

    //
    //  constructor
    //

    private ShutdownHook(KafkaProducer<byte[], byte[]> kafkaProducer, HttpServer restServer, OfferService offerService, SubscriberProfileService subscriberProfileService, SegmentationDimensionService segmentationDimensionService, JourneyService journeyService, JourneyObjectiveService journeyObjectiveService, OfferObjectiveService offerObjectiveService, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, SubscriberIDService subscriberIDService, ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader)
    {
      this.kafkaProducer = kafkaProducer;
      this.restServer = restServer;
      this.offerService = offerService;
      this.subscriberProfileService = subscriberProfileService;
      this.segmentationDimensionService = segmentationDimensionService;
      this.journeyService = journeyService;
      this.journeyObjectiveService = journeyObjectiveService;
      this.offerObjectiveService = offerObjectiveService;
      this.subscriberMessageTemplateService = subscriberMessageTemplateService;
      this.salesChannelService = salesChannelService;
      this.subscriberIDService = subscriberIDService;
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

      if (offerService != null) offerService.stop();
      if (subscriberProfileService != null) subscriberProfileService.stop();
      if (segmentationDimensionService != null) segmentationDimensionService.stop();
      if (journeyService != null) journeyService.stop();
      if (journeyObjectiveService != null) journeyObjectiveService.stop();
      if (offerObjectiveService != null ) offerObjectiveService.stop();
      if (subscriberMessageTemplateService != null) subscriberMessageTemplateService.stop();
      if (salesChannelService != null) salesChannelService.stop();
      if (subscriberIDService != null) subscriberIDService.stop();

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

  private void handleAPI(API api, HttpExchange exchange) throws IOException
  {
    try
    {
      /*****************************************
      *
      *  validateURIandContext
      *
      *****************************************/
      
      validateURIandContext(exchange);
      
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
      if (log.isDebugEnabled()) log.debug("API (raw request): {} {}",api,requestBodyStringBuilder.toString());
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
       *  authenticate and accessCheck
       *
       *****************************************/

      authenticateAndCheckAccess(jsonRoot, api.name());


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
            case ping:
              jsonResponse = processPing(jsonRoot);
              break;
            case getCustomer:
              jsonResponse = processGetCustomer(jsonRoot);
              break;
            case getCustomerBDRs:
              jsonResponse = processGetCustomerBDRs(jsonRoot);
              break;
            case getCustomerODRs:
              jsonResponse = processGetCustomerODRs(jsonRoot);
              break;
            case getCustomerMessages:
              jsonResponse = processGetCustomerMessages(jsonRoot);
              break;
            case getCustomerJourneys:
              jsonResponse = processGetCustomerJourneys(jsonRoot);
              break;
            case getCustomerCampaigns:
              jsonResponse = processGetCustomerCampaigns(jsonRoot);
              break;
            case getOffersList:
              jsonResponse = processGetOffersList(jsonRoot);
              break;
            case getActiveOffer:
              jsonResponse = processGetActiveOffer(jsonRoot);
              break;
            case getActiveOffers:
              jsonResponse = processGetActiveOffers(jsonRoot);
              break;
            case getCustomerAvailableCampaigns:
              jsonResponse = processGetCustomerAvailableCampaigns(jsonRoot);
              break;
            case updateCustomer:
              jsonResponse = processUpdateCustomer(jsonRoot);
              break;
            case getCustomerNBOs:
              jsonResponse = processGetCustomerNBOs(jsonRoot);
              break;
            case getCustomerNBOsTokens:
              jsonResponse = processGetCustomerNBOsTokens(jsonRoot);
              break;
            case getTokensCodesList:
              jsonResponse = processGetTokensCodesList(jsonRoot);
              break;
            case acceptOffer:
              jsonResponse = processAcceptOffer(jsonRoot);
              break;
            case triggerEvent:
              jsonResponse = processTriggerEvent(jsonRoot);
              break;
            case enterCampaign:
              jsonResponse = processEnterCampaign(jsonRoot);
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

      //
      //  log
      //

      if (log.isDebugEnabled()) log.debug("API (raw response): {}", jsonResponse.toString());
      updateStatistics(api);

      //
      //  send
      //

      //
      // headers
      //

      exchange.sendResponseHeaders(200, 0);
      exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
      writer.write(jsonResponse.toString());
      writer.close();
      exchange.close();

    }
    catch (ThirdPartyManagerException ex )
    {
      //
      //  log
      //

      StringWriter stackTraceWriter = new StringWriter();
      ex.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.error("Exception processing REST api: {}", stackTraceWriter.toString());

      //
      //  statistics
      //

      updateStatistics(api, ex);

      //
      //  send error response
      //

      HashMap<String,Object> response = new HashMap<String,Object>();
      response.put(GENERIC_RESPONSE_CODE, ex.getResponseCode());
      response.put(GENERIC_RESPONSE_MSG, ex.getMessage());

      //
      //  standard response fields
      //

      response.put("apiVersion", RESTAPIVersion);
      JSONObject jsonResponse = JSONUtilities.encodeObject(response);
      
      int headerValue = 200;
      String result = jsonResponse.toString();
      
      if (ex.getResponseCode() == -404)
        {
          headerValue = 404;
          result = "<h1>404 Not Found</h1>No context found for request";
        }

      //
      // headers
      //

      exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
      exchange.sendResponseHeaders(headerValue, 0);

      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
      writer.write(result);
      writer.close();
      exchange.close();
    }
    catch (org.json.simple.parser.ParseException exception)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        exception.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("ParseException processing REST api: {}", stackTraceWriter.toString());

        //
        //  statistics
        //

        updateStatistics(api, exception);

        //
        //  send error response
        //

        HashMap<String,Object> response = new HashMap<String,Object>();
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MALFORMED_REQUEST.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MALFORMED_REQUEST.getGenericResponseMessage());

        //
        //  standard response fields
        //

        response.put("apiVersion", RESTAPIVersion);
        JSONObject jsonResponse = JSONUtilities.encodeObject(response);

        //
        // headers
        //

        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
    catch (IOException | ServerException | RuntimeException e )
      {

        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  statistics
        //

        updateStatistics(api, e);

        //
        //  send error response
        //

        HashMap<String,Object> response = new HashMap<String,Object>();
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage());

        //
        //  standard response fields
        //

        response.put("apiVersion", RESTAPIVersion);
        JSONObject jsonResponse = JSONUtilities.encodeObject(response);

        //
        // headers
        //

        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
  }


  /*****************************************
  *
  *  validateURIandContext
  *
  *****************************************/
  
  private void validateURIandContext(HttpExchange exchange) throws ThirdPartyManagerException
  {
    String path = exchange.getRequestURI().getPath();
    if (path.endsWith("/")) path = path.substring(0, path.length()-1);
    
    //
    //  validate
    //
    
    if (! path.equals(exchange.getHttpContext().getPath()))
      {
        log.warn("invalid url {} should be {}", path, exchange.getHttpContext().getPath());
        throw new ThirdPartyManagerException("invalid URL", -404);
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
    response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.LICENSE_RESTRICTION.getGenericResponseCode());
    response.put(GENERIC_RESPONSE_MSG, licenseState.getOutcome().name());
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processPing
   *
   *****************************************/

  private JSONObject processPing(JSONObject jsonRoot)
  {
    /*****************************************
     *
     *  ping
     *
     *****************************************/

    String responseStr = "success";

    /*****************************************
     *
     *  response
     *
     *****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("ping", responseStr);
    response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
    response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processGetCustomer
   *
   *****************************************/

  private JSONObject processGetCustomer(JSONObject jsonRoot) throws ThirdPartyManagerException
  {

    /****************************************
     *
     *  response
     *
     ****************************************/

    Map<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String customerID = readString(jsonRoot, "customerID", true);

    if (customerID == null)
      {
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage()+"-{customerID is missing}");
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
        return JSONUtilities.encodeObject(response);
      }

    String subscriberID = resolveSubscriberID(customerID);

    //
    // process
    //

    if (subscriberID == null)
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
        log.warn("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID ",getCustomerAlternateID, customerID);
      }
    else
      {
        try
        {
          SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true, false);
          if (baseSubscriberProfile == null)
            {
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
            }
          else
            {
              response = baseSubscriberProfile.getProfileMapForThirdPartyPresentation(segmentationDimensionService, subscriberGroupEpochReader);
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
            }
        } 
        catch (SubscriberProfileServiceException e)
        {
          log.error("SubscriberProfileServiceException ", e.getMessage());
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
        }
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processGetCustomerBDRs
   *
   *****************************************/

  private JSONObject processGetCustomerBDRs(JSONObject jsonRoot) throws ThirdPartyManagerException
  {

    /****************************************
     *
     *  response
     *
     ****************************************/

    Map<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String customerID = readString(jsonRoot, "customerID", true);
    String startDateReq = readString(jsonRoot, "startDate", true);

    if (customerID == null)
      {
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage()+"-{customerID is missing}");
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
        return JSONUtilities.encodeObject(response);
      }

    String subscriberID = resolveSubscriberID(customerID);

    //
    // process
    //

    if (subscriberID == null)
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
        log.warn("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID ", getCustomerAlternateID, customerID);
      }
    else
      {
        try
        {

          //
          // include history
          //

          SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, true);
          if (baseSubscriberProfile == null)
            {
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
            }
          else
            {

              //
              // read history
              //

              SubscriberHistory subscriberHistory = baseSubscriberProfile.getSubscriberHistory();
              List<JSONObject> BDRsJson = new ArrayList<JSONObject>();
              if (subscriberHistory != null && subscriberHistory.getDeliveryRequests() != null) 
                {
                  List<DeliveryRequest> activities = subscriberHistory.getDeliveryRequests();

                  //
                  // filter BDRs
                  //

                  List<DeliveryRequest> BDRs = activities.stream().filter(activity -> activity.getActivityType().equals(ActivityType.BDR.getExternalRepresentation())).collect(Collectors.toList()); 

                  //
                  // prepare dates
                  //

                  Date startDate = null;
                  Date now = SystemTime.getCurrentTime();

                  if (startDateReq == null) 
                    {
                      startDate = RLMDateUtils.addDays(now, -7, Deployment.getBaseTimeZone());
                    }
                  else
                    {
                      startDate = getDateFromString(startDateReq, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN);
                    }

                  //
                  // filter and prepare JSON
                  //

                  for (DeliveryRequest bdr : BDRs) 
                    {
                      if (bdr.getEventDate().after(startDate) || bdr.getEventDate().equals(startDate))
                        {
                          Map<String, Object> bdrMap = bdr.getThirdPartyPresentationMap(subscriberMessageTemplateService, salesChannelService);
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
              response.put("BDRs", JSONUtilities.encodeArray(BDRsJson));
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
            }
        } 
        catch (SubscriberProfileServiceException e)
        {
          log.error("SubscriberProfileServiceException ", e.getMessage());
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
        }
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processGetCustomerODRs
   *
   *****************************************/

  private JSONObject processGetCustomerODRs(JSONObject jsonRoot) throws ThirdPartyManagerException
  {

    /****************************************
     *
     *  response
     *
     ****************************************/

    Map<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String customerID = readString(jsonRoot, "customerID", true);
    String startDateReq = readString(jsonRoot, "startDate", true);

    if (customerID == null)
      {
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage()+"-{customerID is missing}");
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
        return JSONUtilities.encodeObject(response);
      }

    String subscriberID = resolveSubscriberID(customerID);

    //
    // process
    //

    if (subscriberID == null)
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
        log.warn("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID ", getCustomerAlternateID, customerID);
      }
    else
      {
        try
        {

          //
          // include history
          //

          SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, true);
          if (baseSubscriberProfile == null)
            {
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
            }
          else
            {

              //
              // read history
              //

              SubscriberHistory subscriberHistory = baseSubscriberProfile.getSubscriberHistory();
              List<JSONObject> ODRsJson = new ArrayList<JSONObject>();
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

                  if (startDateReq == null) 
                    {
                      startDate = RLMDateUtils.addDays(now, -7, Deployment.getBaseTimeZone());
                    }
                  else
                    {
                      startDate = getDateFromString(startDateReq, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN);
                    }

                  //
                  // filter
                  //

                  for (DeliveryRequest odr : ODRs) 
                    {
                      if (odr.getEventDate().after(startDate) || odr.getEventDate().equals(startDate))
                        {
                          Map<String, Object> presentationMap =  odr.getThirdPartyPresentationMap(subscriberMessageTemplateService, salesChannelService);
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
              response.put("ODRs", JSONUtilities.encodeArray(ODRsJson));
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
            }
        } 
        catch (SubscriberProfileServiceException e)
        {
          log.error("SubscriberProfileServiceException ", e.getMessage());
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
        }
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processGetCustomerMessages
   *
   *****************************************/

  private JSONObject processGetCustomerMessages(JSONObject jsonRoot) throws ThirdPartyManagerException
  {

    /****************************************
     *
     *  response
     *
     ****************************************/

    Map<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String customerID = readString(jsonRoot, "customerID", true);
    String startDateReq = readString(jsonRoot, "startDate", true);

    if (customerID == null)
      {
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage()+"-{customerID is missing}");
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
        return JSONUtilities.encodeObject(response);
      }

    String subscriberID = resolveSubscriberID(customerID);

    //
    // process
    //

    if (subscriberID == null)
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
        log.warn("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID ", getCustomerAlternateID, customerID);
      }
    else
      {
        try
        {

          //
          // include history
          //

          SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, true);
          if (baseSubscriberProfile == null)
            {
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
            }
          else
            {

              //
              // read history
              //

              SubscriberHistory subscriberHistory = baseSubscriberProfile.getSubscriberHistory();
              List<JSONObject> messagesJson = new ArrayList<JSONObject>();
              if (subscriberHistory != null && subscriberHistory.getDeliveryRequests() != null) 
                {
                  List<DeliveryRequest> activities = subscriberHistory.getDeliveryRequests();

                  //
                  // filter messages
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
                      startDate = getDateFromString(startDateReq, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN);
                    }

                  //
                  // filter and prepare json
                  //

                  for (DeliveryRequest message : messages) 
                    {
                      if (message.getEventDate().after(startDate) || message.getEventDate().equals(startDate))
                        {
                          messagesJson.add(JSONUtilities.encodeObject(message.getThirdPartyPresentationMap(subscriberMessageTemplateService, salesChannelService)));
                        }
                    }
                }
              response.put("messages", JSONUtilities.encodeArray(messagesJson));
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
            }
        } 
        catch (SubscriberProfileServiceException e)
        {
          log.error("SubscriberProfileServiceException ", e.getMessage());
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
        }
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   * processGetCustomerJourneys
   *
   *****************************************/

  private JSONObject processGetCustomerJourneys(JSONObject jsonRoot) throws ThirdPartyManagerException
  {

    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String customerID = readString(jsonRoot, "customerID", true);
    String journeyObjectiveName = readString(jsonRoot, "objectiveName", true);
    String journeyState = readString(jsonRoot, "journeyState", true);
    String customerStatus = readString(jsonRoot, "customerStatus", true);
    String journeyStartDateStr = readString(jsonRoot, "journeyStartDate", true);
    String journeyEndDateStr = readString(jsonRoot, "journeyEndDate", true);

    Date journeyStartDate = prepareStartDate(getDateFromString(journeyStartDateStr, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN));
    Date journeyEndDate = prepareEndDate(getDateFromString(journeyEndDateStr, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN));

    if (customerID == null)
      {
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage()+"-{customerID is missing}");
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
        return JSONUtilities.encodeObject(response);
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
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
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
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
              if (log.isDebugEnabled()) log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
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
                  //  read campaign statistics 
                  //

                  List<JourneyHistory> journeyHistory = subscriberHistory.getJourneyHistory();

                  //
                  // change data structure to map
                  //

                  Map<String, List<JourneyHistory>> journeyHistorysMap = journeyHistory.stream().collect(Collectors.groupingBy(JourneyHistory::getJourneyID));

                  for (Journey storeJourney : storeJourneys)
                    {

                      //
                      //  thisJourneyStatistics
                      //

                      List<JourneyHistory> thisJourneyHistory = journeyHistorysMap.get(storeJourney.getJourneyID());

                      //
                      //  continue if not in stat
                      //

                      if (thisJourneyHistory == null || thisJourneyHistory.isEmpty()) continue;

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
                      // reverse sort
                      //

                      Collections.sort(thisJourneyHistory, Collections.reverseOrder());

                      //
                      // prepare current node
                      //

                      JourneyHistory subsLatestStatistic = thisJourneyHistory.get(0);

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
                              criteriaSatisfied = statusConverted && !statusNotified && !journeyComplete;
                              break;
                            case CONTROL:
                              criteriaSatisfied = statusControlGroup && !statusConverted && !journeyComplete;
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
                          if (! criteriaSatisfied) continue;
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
                      currentState.put("nodeName", nodeHistory.getToNodeID() == null ? null : storeJourney.getJourneyNode(nodeHistory.getToNodeID()).getNodeName());
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
                          nodeHistoriesMap.put("fromNode", journeyHistories.getFromNodeID() == null ? null : storeJourney.getJourneyNode(journeyHistories.getFromNodeID()).getNodeName());
                          nodeHistoriesMap.put("toNode", journeyHistories.getToNodeID() == null ? null : storeJourney.getJourneyNode(journeyHistories.getToNodeID()).getNodeName());
                          nodeHistoriesMap.put("transitionDate", getDateString(journeyHistories.getTransitionDate()));
                          nodeHistoriesMap.put("linkID", journeyHistories.getLinkID());
                          nodeHistoriesMap.put("deliveryRequestID", journeyHistories.getDeliveryRequestID());
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
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
            }
        } 
        catch (SubscriberProfileServiceException e)
        {
          log.error("SubscriberProfileServiceException ", e.getMessage());
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
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
   * processGetCustomerCampaigns
   *
   *****************************************/

  private JSONObject processGetCustomerCampaigns(JSONObject jsonRoot) throws ThirdPartyManagerException
  {

    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String customerID = readString(jsonRoot, "customerID", true);
    String campaignObjectiveName = readString(jsonRoot, "objectiveName", true);
    String campaignState = readString(jsonRoot, "campaignState", true);
    String customerStatus = readString(jsonRoot, "customerStatus", true);
    String campaignStartDateStr = readString(jsonRoot, "campaignStartDate", true);
    String campaignEndDateStr = readString(jsonRoot, "campaignEndDate", true);


    Date campaignStartDate = prepareStartDate(getDateFromString(campaignStartDateStr, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN));
    Date campaignEndDate = prepareEndDate(getDateFromString(campaignEndDateStr, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN));

    if (customerID == null)
      {
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage()+"-{customerID is missing}");
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
     *
     *  resolve subscriberID
     *
     *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        log.info("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID {}", getCustomerAlternateID, customerID);
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
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
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
              if (log.isDebugEnabled()) log.debug("SubscriberProfile is null for subscriberID {}" , subscriberID);
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

                      log.info("[CHECKPOINT START] journey objectives from active campaigns...");
                      activecampaignObjectives.stream().forEach(journeyObj -> log.info("journey objectives {} ",journeyObj.getJourneyObjectiveName()));
                      log.info("[CHECKPOINT END] journey objectives from active campaigns...");
                      
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

                  List<JourneyHistory> subscribersCampaignStatistics = subscriberHistory.getJourneyHistory();

                  //
                  // change data structure to map
                  //

                  Map<String, List<JourneyHistory>> campaignStatisticsMap = subscribersCampaignStatistics.stream().collect(Collectors.groupingBy(JourneyHistory::getJourneyID));

                  for (Journey storeCampaign : storeCampaigns)
                    {

                      //
                      //  thisCampaignStatistics
                      //

                      List<JourneyHistory> thisCampaignStatistics = campaignStatisticsMap.get(storeCampaign.getJourneyID());

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
                      // reverse sort
                      //

                      Collections.sort(thisCampaignStatistics, Collections.reverseOrder());

                      //
                      // prepare current node
                      //

                      JourneyHistory subsLatestStatistic = thisCampaignStatistics.get(0);


                      //
                      // filter on customerStatus
                      //
                      boolean statusNotified = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> campaignStat.getStatusNotified()).count() > 0L ;
                      boolean statusConverted = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> campaignStat.getStatusConverted()).count() > 0L ;
                      boolean statusControlGroup = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> campaignStat.getStatusControlGroup()).count() > 0L ;
                      boolean statusUniversalControlGroup = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> campaignStat.getStatusUniversalControlGroup()).count() > 0L ;
                      boolean campaignComplete = subsLatestStatistic.getStatusHistory().stream().filter(campaignStat -> campaignStat.getJourneyComplete()).count() > 0L ;

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
                              criteriaSatisfied = statusConverted && !statusNotified && !campaignComplete;
                              break;
                            case CONTROL:
                              criteriaSatisfied = statusControlGroup && !statusConverted && !campaignComplete;
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
                      campaignResponseMap.put("campaignName", journeyService.generateResponseJSON(storeCampaign, true, SystemTime.getCurrentTime()).get("display"));
                      campaignResponseMap.put("description", journeyService.generateResponseJSON(storeCampaign, true, SystemTime.getCurrentTime()).get("description"));
                      campaignResponseMap.put("startDate", getDateString(storeCampaign.getEffectiveStartDate()));
                      campaignResponseMap.put("endDate", getDateString(storeCampaign.getEffectiveEndDate()));
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

                      NodeHistory nodeHistory = subsLatestStatistic.getLastNodeEntered();
                      Map<String, Object> currentState = new HashMap<String, Object>();
                      currentState.put("nodeID", nodeHistory.getToNodeID());
                      currentState.put("nodeName", nodeHistory.getToNodeID() == null ? null : storeCampaign.getJourneyNode(nodeHistory.getToNodeID()).getNodeName());
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
                          nodeHistoriesMap.put("fromNode", journeyHistories.getFromNodeID() == null ? null : storeCampaign.getJourneyNode(journeyHistories.getFromNodeID()).getNodeName());
                          nodeHistoriesMap.put("toNode", journeyHistories.getToNodeID() == null ? null : storeCampaign.getJourneyNode(journeyHistories.getToNodeID()).getNodeName());
                          nodeHistoriesMap.put("transitionDate", getDateString(journeyHistories.getTransitionDate()));
                          nodeHistoriesMap.put("linkID", journeyHistories.getLinkID());
                          nodeHistoriesMap.put("deliveryRequestID", journeyHistories.getDeliveryRequestID());
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
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
            }
        } 
        catch (SubscriberProfileServiceException e)
        {
          log.error("SubscriberProfileServiceException ", e.getMessage());
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
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
   *  processGetOffersList
   *
   *****************************************/

  private JSONObject processGetOffersList(JSONObject jsonRoot) throws ThirdPartyManagerException
  {

    /****************************************
     *
     *  response
     *
     ****************************************/

    Map<String,Object> response = new HashMap<String,Object>();

    /****************************************
     *
     *  argument
     *
     ****************************************/

    String customerID = readString(jsonRoot, "customerID", true);
    String offerState = readString(jsonRoot, "state", true);
    String startDateString = readString(jsonRoot, "startDate", true);
    String endDateString = readString(jsonRoot, "endDate", true);
    String offerObjectiveName = readString(jsonRoot, "objectiveName", true);

    Date offerStartDate = prepareStartDate(getDateFromString(startDateString, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN));
    Date offerEndDate = prepareEndDate(getDateFromString(endDateString, REQUEST_DATE_FORMAT, REQUEST_DATE_PATTERN));

    try
    {
      //
      // resolveSubscriberID when customerID is not null
      //

      String subscriberID = null;
      SubscriberProfile subscriberProfile = null;
      if (customerID != null && ((subscriberID = resolveSubscriberID(customerID)) == null || (subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID)) == null))
        {
          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
          response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
        }
      else if (offerState != null && !offerState.isEmpty() && !offerState.equalsIgnoreCase("ACTIVE"))
        {
          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
          response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage()+"-(state)");
        }
      else
        {
          Collection<Offer> offers = new ArrayList<>();
          if (offerState == null || offerState.isEmpty())
            {
              //
              // retrieve stored offers
              //

              for (GUIManagedObject ofr : offerService.getStoredOffers())
                {
                  if (ofr instanceof Offer) offers.add( (Offer) ofr);
                }
            }
          else
            {
              //
              // retrieve active offers
              //

              offers = offerService.getActiveOffers(SystemTime.getCurrentTime());
            }

          //
          // filter using customerID
          //

          if (customerID != null)
            {
              SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime());
              offers = offers.stream().filter(offer -> offer.evaluateProfileCriteria(evaluationRequest)).collect(Collectors.toList());
            }

          //
          // filter using startDate
          //

          if (offerStartDate != null)
            {
              offers = offers.stream().filter(offer -> (offer.getEffectiveStartDate() == null || offer.getEffectiveStartDate().compareTo(offerStartDate) >= 0)).collect(Collectors.toList()); 
            }

          //
          // filter using endDate
          //

          if (offerEndDate != null)
            {
              offers = offers.stream().filter(campaign -> (campaign.getEffectiveEndDate() == null || campaign.getEffectiveEndDate().compareTo(offerEndDate) <= 0)).collect(Collectors.toList());
            }


          if (offerObjectiveName != null && !offerObjectiveName.isEmpty())
            {

              //
              //  read objective
              //

              Collection<OfferObjective> activeOfferObjectives = offerObjectiveService.getActiveOfferObjectives(SystemTime.getCurrentTime());

              //
              //  filter activejourneyObjective by name
              //

              List<OfferObjective> offerObjectives = activeOfferObjectives.stream().filter(offerObj -> offerObj.getOfferObjectiveName().equals(offerObjectiveName)).collect(Collectors.toList());
              OfferObjective exactOfferObjectives = offerObjectives.size() > 0 ? offerObjectives.get(0) : null;

              //
              //  filter
              //

              if (exactOfferObjectives == null)
                offers = new ArrayList<Offer>();
              else
                offers = offers.stream().filter(offer -> (offer.getOfferObjectives() != null && (offer.getOfferObjectives().stream().filter(obj -> obj.getOfferObjectiveID().equals(exactOfferObjectives.getOfferObjectiveID())).count() > 0L))).collect(Collectors.toList());

            }

          /*****************************************
           *
           *  decorate offers response
           *
           *****************************************/

          List<JSONObject> offersJson = offers.stream().map(offer -> ThirdPartyJSONGenerator.generateOfferJSONForThirdParty(offer, offerService, offerObjectiveService)).collect(Collectors.toList());
          response.put("offers", JSONUtilities.encodeArray(offersJson));
          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
          response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
        }
    }
    catch(SubscriberProfileServiceException spe)
    {
      response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
      response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage());
      log.error("SubscriberProfileServiceException {}", spe);
    }

    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processGetActiveOffer
   *
   *****************************************/

  private JSONObject processGetActiveOffer(JSONObject jsonRoot) throws ThirdPartyManagerException
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

    String offerID = readString(jsonRoot, "id", true);

    /*****************************************
     *
     *  retrieve offer
     *
     *****************************************/

    Offer offer = offerService.getActiveOffer(offerID, SystemTime.getCurrentTime());

    /*****************************************
     *
     *  decorate and response
     *
     *****************************************/

    if (offer == null)
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.OFFER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.OFFER_NOT_FOUND.getGenericResponseMessage());
        return JSONUtilities.encodeObject(response);
      }
    else 
      {
        response.put("offer", ThirdPartyJSONGenerator.generateOfferJSONForThirdParty(offer));
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
        return JSONUtilities.encodeObject(response);
      }
  }

  /*****************************************
   *
   *  processGetActiveOffers
   *
   *****************************************/

  private JSONObject processGetActiveOffers(JSONObject jsonRoot)
  {
    /****************************************
     *
     *  response
     *
     ****************************************/

    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
     *
     *  retrieve offer
     *
     *****************************************/

    Collection<Offer> offers = offerService.getActiveOffers(SystemTime.getCurrentTime());

    /*****************************************
     *
     *  decorate and response
     *
     *****************************************/

    List<JSONObject> offersJson = offers.stream().map(offer -> ThirdPartyJSONGenerator.generateOfferJSONForThirdParty(offer)).collect(Collectors.toList());
    response.put("offers", JSONUtilities.encodeArray(offersJson));
    response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
    response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  processGetCustomerAvailableCampaigns
   *
   *****************************************/

  private JSONObject processGetCustomerAvailableCampaigns(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
     *
     * argument
     *
     ****************************************/

    String customerID = readString(jsonRoot, "customerID", true);

    if (customerID == null)
      {
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage() + "-{customerID is missing}");
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
     *
     * resolve subscriberID
     *
     *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        log.info("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID {}", getCustomerAlternateID, customerID);
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
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
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
              if (log.isDebugEnabled()) log.debug("SubscriberProfile is null for subscriberID {}", subscriberID);
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
                      campaignMap.put("campaignName", journeyService.generateResponseJSON(elgibleActiveCampaign, true, SystemTime.getCurrentTime()).get("display"));
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
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
            }
        }
        catch (SubscriberProfileServiceException e)
        {
          log.error("SubscriberProfileServiceException ", e.getMessage());
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
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

  private JSONObject processUpdateCustomer(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    Map<String, Object> response = new HashMap<String, Object>();

    /****************************************
     *
     * argument
     *
     ****************************************/

    String customerID = readString(jsonRoot, "customerID", true);

    /*****************************************
     *
     * resolve subscriberID
     *
     *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
      } 
    else
      {
        try 
        {
          SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true, false);
          baseSubscriberProfile.validateUpdateProfileRequest(jsonRoot);
          
          jsonRoot.put("subscriberID", subscriberID);
          SubscriberProfileForceUpdate subscriberProfileForceUpdate = new SubscriberProfileForceUpdate(jsonRoot);

          //
          //  submit to kafka
          //

          kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getSubscriberProfileForceUpdateTopic(), StringKey.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), new StringKey(subscriberProfileForceUpdate.getSubscriberID())), SubscriberProfileForceUpdate.serde().serializer().serialize(Deployment.getSubscriberProfileForceUpdateTopic(), subscriberProfileForceUpdate)));

          response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
          response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
        }
        catch (GUIManagerException | SubscriberProfileServiceException e) 
        {
          log.error("unable to process request updateCustomer {} ", e.getMessage());
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode()) ;
        } 
        catch (ValidateUpdateProfileRequestException e)
        {
          throw new ThirdPartyManagerException(e.getMessage(), e.getResponseCode()) ;
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
   *  processGetCustomerNBOs
   *
   *****************************************/

  private JSONObject processGetCustomerNBOs(JSONObject jsonRoot)
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

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", false);
    String strategy = JSONUtilities.decodeString(jsonRoot, "strategy", false);


    // TODO

    /*****************************************
     *
     *  retrieve offer
     *
     *****************************************/

    Collection<Offer> offers = offerService.getActiveOffers(SystemTime.getCurrentTime());

    /*****************************************
     *
     *  decorate and response
     *
     *****************************************/

    List<JSONObject> offersJson = offers.stream().map(offer -> ThirdPartyJSONGenerator.generateOfferJSONForThirdParty(offer)).collect(Collectors.toList());
    response.put("offers", JSONUtilities.encodeArray(offersJson));
    response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
    response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
    return JSONUtilities.encodeObject(response);
  }


  /*****************************************
   *
   *  processGetCustomerNBOsTokens
   *
   *****************************************/

  private JSONObject processGetCustomerNBOsTokens(JSONObject jsonRoot)
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

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", false);
    String token_code = JSONUtilities.decodeString(jsonRoot, "token_code", false);

    // TODO

    /*****************************************
     *
     *  retrieve offer
     *
     *****************************************/

    Collection<Offer> offers = offerService.getActiveOffers(SystemTime.getCurrentTime());

    /*****************************************
     *
     *  decorate and response
     *
     *****************************************/

    List<JSONObject> offersJson = offers.stream().map(offer -> ThirdPartyJSONGenerator.generateOfferJSONForThirdParty(offer)).collect(Collectors.toList());
    response.put("offers", JSONUtilities.encodeArray(offersJson));
    response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
    response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
    return JSONUtilities.encodeObject(response);
  }


  /*****************************************
   *
   *  processGetTokensCodesList
   *
   *****************************************/

  private JSONObject processGetTokensCodesList(JSONObject jsonRoot) throws ThirdPartyManagerException
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

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", false);
    String tokenStatus = JSONUtilities.decodeString(jsonRoot, "tokenStatus", false);

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
            log.info("tokenStatus provided is invalid : {}, will return all statuses (no filter applied)", tokenStatus);
            tokenStatus = null;
          }
      }
    String tokenStatusForStreams = tokenStatus; // We need a 'final-like' variable to process streams later
    boolean hasFilter = (tokenStatusForStreams != null);
    
    /*****************************************
     *
     *  resolve subscriberID
     *
     *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        log.info("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID {}", getCustomerAlternateID, customerID);
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
      } 
    else
      {

        /*****************************************
         *
         *  getSubscriberProfile
         *
         *****************************************/

        if (subscriberID != null)
          {
            try
            {
              SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
              if (baseSubscriberProfile == null)
                {
                  response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
                  response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
                }
              else
                {
                  List<JSONObject> tokensJson;
                  List<Token> tokens = baseSubscriberProfile.getTokens();
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
                          .map(token -> ThirdPartyJSONGenerator.generateTokenJSONForThirdParty(token))
                          .collect(Collectors.toList());
                    }

                  /*****************************************
                   *
                   *  decorate and response
                   *
                   *****************************************/

                  response.put("tokens", JSONUtilities.encodeArray(tokensJson));
                  response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
                  response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
                }
            }
            catch (SubscriberProfileServiceException e)
            {
              log.error("SubscriberProfileServiceException ", e.getMessage());
              throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
            }
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
  *  processTriggerEvent
  *
  *****************************************/

  private JSONObject processTriggerEvent(JSONObject jsonRoot) throws ThirdPartyManagerException
  {
    Map<String, Object> response = new HashMap<String, Object>();
    
    /****************************************
     *
     * argument
     *
     ****************************************/

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", true);
    String eventName = JSONUtilities.decodeString(jsonRoot, "eventName", true);
    JSONObject eventBody = JSONUtilities.decodeJSONObject(jsonRoot, "eventBody");

    if (customerID == null)
      {
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage() + "-{customerID is missing}");
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
        return JSONUtilities.encodeObject(response);
      }

    if (eventName == null || eventName.isEmpty())
      {
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage() + "-{eventName is missing}");
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
        return JSONUtilities.encodeObject(response);
      }

    if (eventBody == null)
      {
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage() + "-{eventBody is missing}");
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
     *
     * resolve subscriberID
     *
     *****************************************/

    String subscriberID = resolveSubscriberID(customerID);
    if (subscriberID == null)
      {
        log.info("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID {}", getCustomerAlternateID, customerID);
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
        return JSONUtilities.encodeObject(response);
      }
    else
      {
        /*****************************************
         *
         * Create Event object and push it its topic
         *
         *****************************************/
        EvolutionEngineEventDeclaration eventDeclaration = Deployment.getEvolutionEngineEvents().get(eventName);
        if (eventDeclaration == null)
          {
            response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.EVENT_NAME_UNKNOWN.getGenericResponseCode());
            response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.EVENT_NAME_UNKNOWN.getGenericResponseMessage() + "-{" + eventName + "}");
            return JSONUtilities.encodeObject(response);
          }
        else
          {
            Constructor<? extends EvolutionEngineEvent> constructor = JSON3rdPartyEventsConstructor.get(eventName);
            Class<? extends EvolutionEngineEvent> eventClass;
            if (constructor == null)
              {
                try
                  {
                    eventClass = (Class<? extends EvolutionEngineEvent>) Class.forName(eventDeclaration.getEventClassName());
                    constructor = eventClass.getConstructor(new Class<?>[]{String.class, Date.class, JSONObject.class });
                    JSON3rdPartyEventsConstructor.put(eventName, constructor);
                  }
                catch (Exception e)
                  {
                    response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.BAD_3RD_PARTY_EVENT_CLASS_DEFINITION.getGenericResponseCode());
                    response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.BAD_3RD_PARTY_EVENT_CLASS_DEFINITION.getGenericResponseMessage() + "-{" + eventDeclaration.getEventClassName() + "(1) Exception " + e.getClass().getName() + "}");
                    return JSONUtilities.encodeObject(response);
                  }
              }
            EvolutionEngineEvent eev = null;
            try
              {
                eev = (EvolutionEngineEvent) constructor.newInstance(new Object[]{subscriberID, new Date(), eventBody });
              }
            catch (Exception e)
              {
                response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.BAD_3RD_PARTY_EVENT_CLASS_DEFINITION.getGenericResponseCode());
                response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.BAD_3RD_PARTY_EVENT_CLASS_DEFINITION.getGenericResponseMessage() + "-{" + eventDeclaration.getEventClassName() + "(2) Exception " + e.getClass().getName() + "}");
                return JSONUtilities.encodeObject(response);
              }
            
            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(eventDeclaration.getEventTopic(), StringKey.serde().serializer().serialize(eventDeclaration.getEventTopic(), new StringKey(subscriberID)), eventDeclaration.getEventSerde().serializer().serialize(eventDeclaration.getEventTopic(), eev)));
            response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
            response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage() + "{event triggered}");
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
  *  processEnterCampaign
   * @throws ThirdPartyManagerException 
  *
  *****************************************/
  
  private JSONObject processEnterCampaign(JSONObject jsonRoot) throws ThirdPartyManagerException
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
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
        return JSONUtilities.encodeObject(response);
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
                response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
                response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
                return JSONUtilities.encodeObject(response);
              }
          }
        catch (SubscriberProfileServiceException e)
          {
            log.error("SubscriberProfileServiceException ", e.getMessage());
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
          }
      }
    
    Journey journey = null;
    String campaignName = JSONUtilities.decodeString(jsonRoot, "campaignName", true);
    Collection<Journey> allActiveJourneys = journeyService.getActiveJourneys(SystemTime.getCurrentTime());
    if(allActiveJourneys != null)
      {
        for(Journey activeJourney : allActiveJourneys)
          {
            if(activeJourney.getJourneyName().equals(campaignName))
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
      }

    if(journey != null)
      {
        String uniqueKey = UUID.randomUUID().toString();
        JourneyRequest journeyRequest = new JourneyRequest(uniqueKey, subscriberID, journey.getJourneyID(), baseSubscriberProfile.getUniversalControlGroup());
        DeliveryManagerDeclaration journeyManagerDeclaration = Deployment.getDeliveryManagers().get(journeyRequest.getDeliveryType());
        String journeyRequestTopic = journeyManagerDeclaration.getDefaultRequestTopic();
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getJourneyRequestTopic(), StringKey.serde().serializer().serialize(journeyRequestTopic, new StringKey(journeyRequest.getDeliveryRequestID())), ((ConnectSerde<DeliveryRequest>)journeyManagerDeclaration.getRequestSerde()).serializer().serialize(journeyRequestTopic, journeyRequest)));
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
   *  processAcceptOffer
   *
   *****************************************/

  private JSONObject processAcceptOffer(JSONObject jsonRoot)
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

    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", false);
    String token_code = JSONUtilities.decodeString(jsonRoot, "token_code", false);
    String offer_id = JSONUtilities.decodeString(jsonRoot, "offer_id", false);
    String origin = JSONUtilities.decodeString(jsonRoot, "origin", false);

    // TODO
    /*****************************************
     *
     *  retrieve offer
     *
     *****************************************/

    Collection<Offer> offers = offerService.getActiveOffers(SystemTime.getCurrentTime());

    /*****************************************
     *
     *  decorate and response
     *
     *****************************************/

    List<JSONObject> offersJson = offers.stream().map(offer -> ThirdPartyJSONGenerator.generateOfferJSONForThirdParty(offer)).collect(Collectors.toList());
    response.put("offers", JSONUtilities.encodeArray(offersJson));
    response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseCode());
    response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.SUCCESS.getGenericResponseMessage());
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
   *
   *  updateStatistics
   *
   *****************************************/

  private void updateStatistics(API api)
  {
    updateStatistics(api, null);
  }

  private void updateStatistics(API api, Exception exception)
  {
    synchronized (accessStatistics)
    {
      accessStatistics.updateTotalAPIRequestCount(1);
      if (exception == null)
        {
          accessStatistics.updateSuccessfulAPIRequestCount(1);
          switch (api)
          {
            case ping:
              accessStatistics.updatePingCount(1);
              break;

            case getCustomer:
              accessStatistics.updateGetCustomerCount(1);
              break;

            case getOffersList:
              accessStatistics.updateGetOffersListCount(1);
              break;

            case getActiveOffer:
              accessStatistics.updateGetActiveOfferCount(1);
              break;

            case getActiveOffers:
              accessStatistics.updateGetActiveOffersCount(1);
              break;
          }
        }
      else
        {
          accessStatistics.updateFailedAPIRequestCount(1);
        }
    }
  }

  /*****************************************
   *
   *  getDateFromString
   *
   *****************************************/

  private Date getDateFromString(String dateString, String dateFormat, String pattern) throws ThirdPartyManagerException
  {
    Date result = null;
    if (dateString != null)
      {
        if (pattern != null && !dateString.matches(pattern))
          {
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage()+"(invalid date/format expected in "+dateFormat+" and found "+dateString+")", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
          }
        try 
          {
            result = RLMDateUtils.parseDate(dateString, dateFormat, Deployment.getBaseTimeZone(), false);
          }
        catch(Exception ex)
          {
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage()+"(invalid date/format expected in "+dateFormat+" and found "+dateString+")", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
          }
        
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
   *  authenticateAndCheckAccess
   *
   *****************************************/

  private void authenticateAndCheckAccess(JSONObject jsonRoot, String api) throws ThirdPartyManagerException, ParseException, IOException
  {

    ThirdPartyCredential thirdPartyCredential = new ThirdPartyCredential(jsonRoot);

    //
    // confirm thirdPartyCredential format is ok
    //

    if (thirdPartyCredential.getLoginName() == null || thirdPartyCredential.getPassword() == null || thirdPartyCredential.getLoginName().isEmpty() || thirdPartyCredential.getPassword().isEmpty())
      {
        log.error("invalid request {}", "credential is missing");
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage() + "-{credential is missing}", RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
      }

    //
    // look up methodAccess configuration from deployment
    //

    ThirdPartyMethodAccessLevel methodAccessLevel = methodPermissionsMapper.get(api);

    //
    // access hack(dev purpose)
    //

    if (methodAccessLevel != null && methodAccessLevel.isByPassAuth()) return ;

    //
    // lookup from authCache
    //

    AuthenticatedResponse authResponse = null;
    synchronized (authCache)
    {
      authResponse = authCache.get(thirdPartyCredential);
    }

    //
    //  cache miss - reauthenticate
    //

    if (authResponse == null)
      {
        authResponse = authenticate(thirdPartyCredential);
        log.info("(Re)Authenticated: credential {} response {}", thirdPartyCredential, authResponse);
      }

    //
    //  hasAccess
    //

    if (! hasAccess(authResponse, methodAccessLevel, api))
      {
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.INSUFFICIENT_USER_RIGHTS.getGenericResponseMessage(), RESTAPIGenericReturnCodes.INSUFFICIENT_USER_RIGHTS.getGenericResponseCode());
      }

  }

  /*****************************************
   *
   *  authenticate
   *
   *****************************************/

  private AuthenticatedResponse authenticate(ThirdPartyCredential thirdPartyCredential) throws IOException, ParseException, ThirdPartyManagerException
  {
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build())
    {
      //
      // create request
      //

      StringEntity stringEntity = new StringEntity(thirdPartyCredential.getJSONString(), ContentType.create("application/json"));
      HttpPost httpPost = new HttpPost("http://" + fwkServer + "/api/account/login");
      httpPost.setEntity(stringEntity);

      //
      // submit request
      //

      HttpResponse httpResponse = httpClient.execute(httpPost);

      //
      // process response
      //

      if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 200)
        {
          String jsonResponse = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
          log.info("FWK raw response : {}", jsonResponse);

          //
          // parse JSON response from FWK
          //

          JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(jsonResponse);

          //
          // prepare response
          //

          AuthenticatedResponse authResponse = new AuthenticatedResponse(jsonRoot);


          //
          // update cache
          //

          synchronized (authCache)
          {
            authCache.put(thirdPartyCredential, authResponse);
          }

          //
          // return
          //

          return authResponse;
        }
      else if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 401)
        {
          log.error("FWK server HTTP reponse code {} message {} ", httpResponse.getStatusLine().getStatusCode(), EntityUtils.toString(httpResponse.getEntity(), "UTF-8"));
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.AUTHENTICATION_FAILURE.getGenericResponseMessage(), RESTAPIGenericReturnCodes.AUTHENTICATION_FAILURE.getGenericResponseCode());
        }
      else if (httpResponse != null && httpResponse.getStatusLine() != null)
        {
          log.error("FWK server HTTP reponse code is invalid {}", httpResponse.getStatusLine().getStatusCode());
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
        }
      else
        {
          log.error("FWK server error httpResponse or httpResponse.getStatusLine() is null {} {} ", httpResponse, httpResponse.getStatusLine());
          throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseMessage(), RESTAPIGenericReturnCodes.SYSTEM_ERROR.getGenericResponseCode());
        }
    }
    catch(ParseException pe) 
    {
      log.error("failed to Parse ParseException {} ", pe.getMessage());
      throw pe;
    }
    catch(IOException e) 
    {
      log.error("failed to authenticate in FWK server");
      log.error("IOException: {}", e.getMessage());
      throw e;
    }
  }

  /*****************************************
   *
   *  hasAccess
   *
   *****************************************/

  private boolean hasAccess(AuthenticatedResponse authResponse, ThirdPartyMethodAccessLevel methodAccessLevel, String api)
  {
    boolean result = true;

    //
    //  check method access
    //

    if (methodAccessLevel == null || (methodAccessLevel.getPermissions().isEmpty() && methodAccessLevel.getWorkgroups().isEmpty()))
      {
        result = false;
        log.warn("No permission/workgroup is configured for method {} ", api);
      }
    else
      {
        //
        //  check workgroup
        //

        result = methodAccessLevel.getWorkgroups().contains(authResponse.getWorkgroupHierarchy().getWorkgroup().getName());

        //
        //  check permissions
        //

        if (result)
          {
            for (String userPermission : authResponse.getPermissions())
              {
                result = methodAccessLevel.getPermissions().contains(userPermission);
                if (result) break;
              }
          }
      }

    //
    //  result
    //

    return result;
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
    } catch (SubscriberIDServiceException e)
    {
      log.error("SubscriberIDServiceException can not resolve subscriberID for {} error is {}", customerID, e.getMessage());
    }
    return result;
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
      case Journey_Manager:
        GUIManagedObject journey = journeyService.getStoredJourney(featureId);
        journey = (journey != null && journey.getGUIManagedObjectType() == GUIManagedObjectType.Journey) ? journey : null;
        featureName = journey == null ? null : journey.getGUIManagedObjectName();
        break;

      case Offer_Catalog:
        featureName = offerService.getStoredOffer(featureId).getGUIManagedObjectName();
        break;

      case Delivery_Manager:
        featureName = "Delivery_Manager-its temp"; //TO DO
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
   *  class ThirdPartyManagerException
   *
   *****************************************/

  private static class ThirdPartyManagerException extends Exception
  {
    /*****************************************
     *
     *  data
     *
     *****************************************/

    private int responseCode;

    /*****************************************
     *
     *  accessors
     *
     *****************************************/

    public int getResponseCode() { return responseCode; }

    /*****************************************
     *
     *  constructor
     *
     *****************************************/

    public ThirdPartyManagerException(String responseMessage, int responseCode)
    {
      super(responseMessage);
      this.responseCode = responseCode;
    }

    /*****************************************
     *
     *  constructor - excpetion
     *
     *****************************************/

    public ThirdPartyManagerException(Throwable e)
    {
      super(e.getMessage(), e);
      this.responseCode = -1;
    }
  }

  /*****************************************
   *
   *  class ThirdPartyCredential
   *
   *****************************************/

  private class ThirdPartyCredential
  {

    //
    // data
    //

    private String loginName;
    private String password;
    private JSONObject jsonRepresentation;

    //
    //  accessors
    //

    public String getLoginName() { return loginName; }
    public String getPassword() { return password; }
    public String getJSONString() { return jsonRepresentation.toString(); }

    /****************************
     *
     *  constructor
     *
     *****************************/

    public ThirdPartyCredential(JSONObject jsonRoot) throws ThirdPartyManagerException
    {
      this.loginName = readString(jsonRoot, "loginName", true);
      this.password = readString(jsonRoot, "password", true);

      //
      //  jsonRepresentation
      //

      jsonRepresentation = new JSONObject();
      jsonRepresentation.put("LoginName", loginName);
      jsonRepresentation.put("Password", password);
    }

    /*****************************************
     *
     *  equals/hashCode
     *
     *****************************************/

    @Override public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof ThirdPartyCredential)
        {
          ThirdPartyCredential credential = (ThirdPartyCredential) obj;
          result = loginName.equals(credential.getLoginName()) && password.equals(credential.getPassword());
        }
      return result;
    }

    @Override public int hashCode()
    {
      return loginName.hashCode() + password.hashCode();
    }
  }

  /*****************************************
   *
   *  class AuthenticatedResponse
   *
   *****************************************/

  private class AuthenticatedResponse
  {
    //
    // data
    //

    private int userId;
    private String loginName;
    private String token;
    private String languageIso;
    private List<String> permissions;
    private String clientIP;
    private WorkgroupHierarchy workgroupHierarchy;
    private String tokenCreationDate;
    private String additionalInfo;

    //
    // accessors
    //

    public int getUserId() { return userId; }
    public String getLoginName() { return loginName; }  
    public String getToken() { return token; }  
    public String getLanguageIso() { return languageIso; }  
    public List<String> getPermissions() { return permissions; }  
    public String getClientIP() { return clientIP; }  
    public WorkgroupHierarchy getWorkgroupHierarchy() { return workgroupHierarchy; }  
    public String getTokenCreationDate() { return tokenCreationDate; }  
    public String getAdditionalInfo() { return additionalInfo; }

    /*****************************************
     *
     *  constructor
     *
     *****************************************/

    private AuthenticatedResponse(JSONObject jsonRoot)
    {
      this.userId = JSONUtilities.decodeInteger(jsonRoot, "UserId", true);
      this.loginName = JSONUtilities.decodeString(jsonRoot, "LoginName", true);
      this.token = JSONUtilities.decodeString(jsonRoot, "Token", true);
      this.languageIso = JSONUtilities.decodeString(jsonRoot, "LanguageIso", false);
      JSONArray userPermissionArray = JSONUtilities.decodeJSONArray(jsonRoot, "Permissions", true);
      permissions = new ArrayList<String>();
      for (int i=0; i<userPermissionArray.size(); i++)
        {
          permissions.add((String) userPermissionArray.get(i));
        }
      this.clientIP = JSONUtilities.decodeString(jsonRoot, "ClientIP", true);
      this.workgroupHierarchy = new WorkgroupHierarchy(JSONUtilities.decodeJSONObject(jsonRoot, "WorkgroupHierarchy", true));
      this.tokenCreationDate = JSONUtilities.decodeString(jsonRoot, "TokenCreationDate", true);
      this.additionalInfo = JSONUtilities.decodeString(jsonRoot, "AdditionalInfo", false);
    }

    /*****************************************
     *
     *  class WorkgroupHierarchy
     *
     *****************************************/

    private class WorkgroupHierarchy
    {
      //
      // data
      //

      private Workgroup workgroup;
      private String parents;
      private String children;

      //
      // accessors
      //

      public Workgroup getWorkgroup() { return workgroup; }
      public String getParents() { return parents; }
      public String getChildren() { return children; }

      /*****************************************
       *
       *  constructor
       *
       *****************************************/

      public WorkgroupHierarchy(JSONObject workgroupHierarchyJSONObject)
      {
        this.workgroup = new Workgroup(JSONUtilities.decodeJSONObject(workgroupHierarchyJSONObject, "Workgroup", true));
        this.parents = JSONUtilities.decodeString(workgroupHierarchyJSONObject, "Parents", false);
        this.children = JSONUtilities.decodeString(workgroupHierarchyJSONObject, "Children", false);
      }

      /*****************************************
       *
       *  class Workgroup
       *
       *****************************************/
      private class Workgroup
      {
        //
        // data
        //

        private int id;
        private String key;
        private String name;

        //
        // accessors
        //

        public int getId() { return id; }
        public String getKey() { return key; }
        public String getName() { return name; }

        /*****************************************
         *
         *  constructor
         *
         *****************************************/

        public Workgroup(JSONObject workgroupJSONObject)
        {
          this.id = JSONUtilities.decodeInteger(workgroupJSONObject, "Id", true);
          this.key = JSONUtilities.decodeString(workgroupJSONObject, "Key", true);
          this.name = JSONUtilities.decodeString(workgroupJSONObject, "Name", true);
        }

      }
    }
  }

  /*****************************************
   *
   *  getDateString
   *
   *****************************************/

  public String getDateString(Date date)
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
  *  readString
  *
  *****************************************/
  
  private String readString(JSONObject jsonRoot, String key, boolean validateNotEmpty) throws ThirdPartyManagerException
  {
    String result = readString(jsonRoot, key);
    if (validateNotEmpty && (result == null || result.trim().isEmpty()) && jsonRoot.containsKey(key))
      {
        log.error("readString validation error");
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
      }
    return result;
  }
  
  /*****************************************
  *
  *  readString
  *
  *****************************************/
  
  private String readString(JSONObject jsonRoot, String key) throws ThirdPartyManagerException
  {
    String result = null;
    try 
      {
        result = JSONUtilities.decodeString(jsonRoot, key, false);
      }
    catch (JSONUtilitiesException e) 
      {
        log.error("readString JSONUtilitiesException "+e.getMessage());
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
      }
    return result;
  }
  
  /*****************************************
  *
  *  readInteger
  *
  *****************************************/
  
  private Integer readInteger(JSONObject jsonRoot, String key, boolean required) throws ThirdPartyManagerException
  {
    try 
      {
        return JSONUtilities.decodeInteger(jsonRoot, key, required);
      }
    catch (JSONUtilitiesException e) 
      {
        log.error("JSONUtilitiesException "+e.getMessage());
        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
      }
  }
}
