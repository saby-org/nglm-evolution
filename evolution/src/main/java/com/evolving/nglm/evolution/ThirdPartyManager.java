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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Alarm;
import com.evolving.nglm.core.LicenseChecker;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.core.SubscriberIDService.SubscriberIDServiceException;
import com.evolving.nglm.core.LicenseChecker.LicenseState;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryRequest.ActivityType;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;
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
  
  private OfferService offerService;
  private SubscriberProfileService subscriberProfileService;
  private SubscriberIDService subscriberIDService;
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader;
  private static final int RESTAPIVersion = 1;
  private HttpServer restServer;
  private static Map<String, ThirdPartyMethodAccessLevel> methodPermissionsMapper = new LinkedHashMap<String,ThirdPartyMethodAccessLevel>();
  private static Integer authResponseCacheLifetimeInMinutes = null;
  private static final String GENERIC_RESPONSE_CODE = "responseCode";
  private static final String GENERIC_RESPONSE_MSG = "responseMessage";
  private String getCustomerAlternateID;
  
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
    getOffersList,
    getActiveOffer,
    getActiveOffers;
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
  
  public static void main(String[] args) throws Exception
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
    int threadPoolSize = parseInteger("apiRestPort", args[4]);
    String nodeID = System.getProperty("nglm.license.nodeid");
    String offerTopic = Deployment.getOfferTopic();
    String subscriberUpdateTopic = Deployment.getSubscriberUpdateTopic();
    String subscriberGroupEpochTopic = Deployment.getSubscriberGroupEpochTopic();
    String redisServer = Deployment.getRedisSentinels();
    String subscriberProfileEndpoints = Deployment.getSubscriberProfileEndpoints();
    methodPermissionsMapper = Deployment.getThirdPartyMethodPermissionsMap();
    authResponseCacheLifetimeInMinutes = Deployment.getAuthResponseCacheLifetimeInMinutes() == null ? new Integer(0) : Deployment.getAuthResponseCacheLifetimeInMinutes();
    getCustomerAlternateID = Deployment.getSubscriberTraceControlAlternateID();
    
    //
    //  log
    //

    log.info("main START: {} {} {} {} {}", apiProcessKey, bootstrapServers, apiRestPort, fwkServer, authResponseCacheLifetimeInMinutes);
    
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
    *  services
    *
    *****************************************/
    
    //
    //  construct
    //
    
    offerService = new OfferService(bootstrapServers, "thirdpartymanager-offerservice-" + apiProcessKey, offerTopic, false);
    subscriberProfileService = new EngineSubscriberProfileService(bootstrapServers, "thirdpartymanager-subscriberprofileservice-001", subscriberUpdateTopic, subscriberProfileEndpoints);
    subscriberIDService = new SubscriberIDService(redisServer);
    subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("thirdpartymanager-subscribergroupepoch", apiProcessKey, bootstrapServers, subscriberGroupEpochTopic, SubscriberGroupEpoch::unpack);
    
    //
    //  start
    //
    
    offerService.start();
    subscriberProfileService.start();
    
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
        restServer.createContext("/nglm-thirdpartymanager/getOffersList", new APIHandler(API.getOffersList));
        restServer.createContext("/nglm-thirdpartymanager/getActiveOffer", new APIHandler(API.getActiveOffer));
        restServer.createContext("/nglm-thirdpartymanager/getActiveOffers", new APIHandler(API.getActiveOffers));
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
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(restServer, offerService, subscriberProfileService, subscriberIDService, subscriberGroupEpochReader));
    
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

    private HttpServer restServer;
    private OfferService offerService;
    private SubscriberProfileService subscriberProfileService;
    private SubscriberIDService subscriberIDService;
    private ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader;
    
    //
    //  constructor
    //

    private ShutdownHook(HttpServer restServer, OfferService offerService, SubscriberProfileService subscriberProfileService, SubscriberIDService subscriberIDService, ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader)
    {
      this.restServer = restServer;
      this.offerService = offerService;
      this.subscriberProfileService = subscriberProfileService;
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
      if (subscriberIDService != null) subscriberIDService.stop();
      
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
  
  private void handleAPI(API api, HttpExchange exchange) throws IOException
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
                case getOffersList:
                  jsonResponse = processGetOffersList(jsonRoot);
                  break;
                case getActiveOffer:
                  jsonResponse = processGetActiveOffer(jsonRoot);
                  break;
                case getActiveOffers:
                  jsonResponse = processGetActiveOffers(jsonRoot);
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

        log.debug("API (raw response): {}", jsonResponse.toString());
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
    catch (org.json.simple.parser.ParseException | IOException | ServerException | RuntimeException e )
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
   * @throws ThirdPartyManagerException 
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
    
    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", false);
    
    if (null == customerID || customerID.isEmpty())
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage()+"-{customerID is missing}");
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage());
        return JSONUtilities.encodeObject(response);
      }
    
    String subscriberID = resolveSubscriberID(customerID);
    
    //
    // process
    //
    
    if (null == subscriberID)
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
        log.warn("unable to resolve SubscriberID for getCustomerAlternateID {} and customerID ",getCustomerAlternateID, customerID);
      }
    else
      {
        try
        {
          SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID);
          if (null == baseSubscriberProfile)
            {
              response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
              response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
            }
          else
            {
              response = baseSubscriberProfile.getProfileMapForGUIPresentation(subscriberGroupEpochReader);
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
  * @throws ThirdPartyManagerException 
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
    
    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", false);
    String startDateReq = JSONUtilities.decodeString(jsonRoot, "startDate", false);
    
    //
    // yyyy-MM-dd -- date format
    //
    
    String dateFormat = "yyyy-MM-dd";
    
    if (null == customerID || customerID.isEmpty())
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage()+"-{customerID is missing}");
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage());
        return JSONUtilities.encodeObject(response);
      }
    
    String subscriberID = resolveSubscriberID(customerID);
    
    //
    // process
    //
    
    if (null == subscriberID)
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
          
          SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true);
          if (null == baseSubscriberProfile)
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
              if (null != subscriberHistory && null != subscriberHistory.getDeliveryRequests()) 
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
                  
                  if (startDateReq == null || startDateReq.isEmpty()) 
                    {
                      startDate = RLMDateUtils.addDays(now, -7, Deployment.getBaseTimeZone());
                    }
                  else
                    {
                      startDate = RLMDateUtils.parseDate(startDateReq, dateFormat, Deployment.getBaseTimeZone());
                    }
                  
                  //
                  // filter and prepare JSON
                  //
                  
                  for (DeliveryRequest bdr : BDRs) 
                    {
                      if (bdr.getEventDate().after(startDate) || bdr.getEventDate().equals(startDate))
                        {
                          BDRsJson.add(JSONUtilities.encodeObject(bdr.getThirdPartyPresentationMap()));
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
  * @throws ThirdPartyManagerException 
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
    
    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", false);
    String startDateReq = JSONUtilities.decodeString(jsonRoot, "startDate", false);
    
    //
    // yyyy-MM-dd -- date format
    //
    
    String dateFormat = "yyyy-MM-dd";
    
    if (null == customerID || customerID.isEmpty())
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage()+"-{customerID is missing}");
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage());
        return JSONUtilities.encodeObject(response);
      }
    
    String subscriberID = resolveSubscriberID(customerID);
    
    //
    // process
    //
    
    if (null == subscriberID)
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
          
          SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true);
          if (null == baseSubscriberProfile)
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
              if (null != subscriberHistory && null != subscriberHistory.getDeliveryRequests()) 
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
                  
                  if (startDateReq == null || startDateReq.isEmpty()) 
                    {
                      startDate = RLMDateUtils.addDays(now, -7, Deployment.getBaseTimeZone());
                    }
                  else
                    {
                      startDate = RLMDateUtils.parseDate(startDateReq, dateFormat, Deployment.getBaseTimeZone());
                    }
                  
                  //
                  // filter
                  //
                  
                  for (DeliveryRequest odr : ODRs) 
                    {
                      if (odr.getEventDate().after(startDate) || odr.getEventDate().equals(startDate))
                        {
                          Map<String, Object> presentationMap =  odr.getGUIPresentationMap();
                          String offerID = presentationMap.get(DeliveryRequest.OFFERID) == null ? null : presentationMap.get(DeliveryRequest.OFFERID).toString();
                          if (null != offerID)
                            {
                              Offer offer = (Offer) offerService.getStoredOffer(offerID);
                              if (null != offer)
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
  * @throws ThirdPartyManagerException 
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
    
    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", false);
    String startDateReq = JSONUtilities.decodeString(jsonRoot, "startDate", false);
    
    //
    // yyyy-MM-dd -- date format
    //
    
    String dateFormat = "yyyy-MM-dd";
    
    if (null == customerID || customerID.isEmpty())
      {
        response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage()+"-{customerID is missing}");
        response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage());
        return JSONUtilities.encodeObject(response);
      }
    
    String subscriberID = resolveSubscriberID(customerID);
    
    //
    // process
    //
    
    if (null == subscriberID)
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
          
          SubscriberProfile baseSubscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true);
          if (null == baseSubscriberProfile)
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
              if (null != subscriberHistory && null != subscriberHistory.getDeliveryRequests()) 
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
                      startDate = RLMDateUtils.parseDate(startDateReq, dateFormat, Deployment.getBaseTimeZone());
                    }
                  
                  //
                  // filter and prepare json
                  //
                  
                  for (DeliveryRequest message : messages) 
                    {
                      if (message.getEventDate().after(startDate) || message.getEventDate().equals(startDate))
                        {
                          messagesJson.add(JSONUtilities.encodeObject(message.getThirdPartyPresentationMap()));
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
    
    String customerID = JSONUtilities.decodeString(jsonRoot, "customerID", false);
    
    try
      {
        //
        // resolveSubscriberID when customerID is not null
        //
        
        String subscriberID = null;
        SubscriberProfile subscriberProfile = null;
        if (null != customerID && ((subscriberID = resolveSubscriberID(customerID)) == null || (subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID)) == null))
          {
            response.put(GENERIC_RESPONSE_CODE, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseCode());
            response.put(GENERIC_RESPONSE_MSG, RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND.getGenericResponseMessage());
          }
        else
          {
            //
            // retrieve offers
            //
            
            Collection<Offer> offers = offerService.getActiveOffers(SystemTime.getCurrentTime());
            
            //
            // filter using customerID
            //
            
            if (null != customerID)
              {
                SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime());
                offers = offers.stream().filter(offer -> offer.evaluateProfileCriteria(evaluationRequest)).collect(Collectors.toList());
              }
            
            /*****************************************
            *
            *  decorate offers response
            *
            *****************************************/
            
            List<JSONObject> offersJson = offers.stream().map(offer -> ThirdPartyJSONGenerator.generateOfferJSONForThirdParty(offer)).collect(Collectors.toList());
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

  private JSONObject processGetActiveOffer(JSONObject jsonRoot)
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
    *  retrieve offer
    *
    *****************************************/

    Offer offer = offerService.getActiveOffer(offerID, SystemTime.getCurrentTime());
    
    /*****************************************
    *
    *  decorate and response
    *
    *****************************************/
    
    if ( null == offer )
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
    
    if (null == thirdPartyCredential.getLoginName() || null == thirdPartyCredential.getPassword() || thirdPartyCredential.getLoginName().isEmpty() || thirdPartyCredential.getPassword().isEmpty())
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
    
    if (null != methodAccessLevel && methodAccessLevel.isByPassAuth()) return ;
    
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
    
    if (null == authResponse)
      {
        authResponse = authenticate(thirdPartyCredential);
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
        log.error("failed to FWK server with data {} ", thirdPartyCredential.getJSONString());
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
    
    if (null == methodAccessLevel || (methodAccessLevel.getPermissions().isEmpty() && methodAccessLevel.getWorkgroups().isEmpty()))
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
    
    public ThirdPartyCredential(JSONObject jsonRoot)
    {
      this.loginName = JSONUtilities.decodeString(jsonRoot, "loginName", false);
      this.password = JSONUtilities.decodeString(jsonRoot, "password", false);
      
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
  
}
