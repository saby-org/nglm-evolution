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
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.core.SubscriberIDService.SubscriberIDServiceException;
import com.evolving.nglm.core.LicenseChecker.LicenseState;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
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
  private static final String GENERIC_RESPONSE_CODE = "responseCode";
  private static final String GENERIC_RESPONSE_MSG = "responseMessage";
  private String subscriberTraceControlAlternateID;
  
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
    methodPermissionsMapper = Deployment.getThirdPartyMethodPermissionsMap();
    subscriberTraceControlAlternateID = Deployment.getSubscriberTraceControlAlternateID();
    
    //
    //  log
    //

    log.info("main START: {} {} {} {}", apiProcessKey, bootstrapServers, apiRestPort, fwkServer);
    
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
    
    /*****************************************
    *
    *  services
    *
    *****************************************/
    
    //
    //  construct
    //
    
    offerService = new OfferService(bootstrapServers, "thirdpartymanager-offerservice-" + apiProcessKey, offerTopic, false);
    subscriberProfileService = new SubscriberProfileService(bootstrapServers, "thirdpartymanager-subscriberprofileservice-" + apiProcessKey, subscriberUpdateTopic, redisServer);
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
        *  accessCheck - HACK - trusting the token if provided
        *
        *****************************************/
        
        ThirdPartyCredential thirdPartyCredential = new ThirdPartyCredential(jsonRoot);
        String token = null;
        if (thirdPartyCredential.getToken() == null)
          {
            token = validateCredentialAndGetToken(thirdPartyCredential, api.name());
          }
        else
          {
            token = thirdPartyCredential.getToken();
          }
        
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
        jsonResponse.put("token", token);
        
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
        log.warn("unable to resolve SubscriberID for subscriberTraceControlAlternateID {} and customerID ",subscriberTraceControlAlternateID, customerID);
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
  *  validateCredential
   * @throws ThirdPartyManagerException 
   * @throws ParseException 
   * @throws IOException 
  *
  *****************************************/
  
  private String validateCredentialAndGetToken(ThirdPartyCredential thirdPartyCredential, String api) throws ThirdPartyManagerException, ParseException, IOException
  {
    String token = null;
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build())
      {
        //
        // create request
        //

        HttpResponse httpResponse = null;

        if ((null == thirdPartyCredential.getLoginName() || null == thirdPartyCredential.getPassword() || thirdPartyCredential.getLoginName().isEmpty() || thirdPartyCredential.getPassword().isEmpty()) && (null == thirdPartyCredential.getToken() || thirdPartyCredential.getToken().isEmpty()))
          {
            log.error("invalid request {}", "credential is missing");
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseMessage() + "-{credential is missing}", RESTAPIGenericReturnCodes.MISSING_PARAMETERS.getGenericResponseCode());
          }
        else if (null == thirdPartyCredential.getToken() || thirdPartyCredential.getToken().isEmpty())
          {
            //
            // call FWK api/account/login
            //
            
            log.debug("request data for login {} ", thirdPartyCredential.getJSONString());
            StringEntity stringEntity = new StringEntity(thirdPartyCredential.getJSONString(), ContentType.create("application/json"));
            HttpPost httpPost = new HttpPost("http://" + fwkServer + "/api/account/login");
            httpPost.setEntity(stringEntity);
            
            //
            // submit request
            //
            
            httpResponse = httpClient.execute(httpPost);
          } 
        else
          {
            //
            // call FWK api/account/checktoken
            //
            
            log.debug("request checktoken : {}", "http://" + fwkServer + "/api/account/checktoken?token=" + thirdPartyCredential.getToken());
            HttpGet httpGet = new HttpGet("http://" + fwkServer + "/api/account/checktoken?token=" + thirdPartyCredential.getToken());
            
            
            //
            // submit request
            //
            
            httpResponse = httpClient.execute(httpGet);
          }

        //
        // process response
        //
        
        if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 200)
          {
            String jsonResponse = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
            log.info("FWK raw response : {}", jsonResponse);
            
            //
            //  parse JSON response from FWK
            //
            
            JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(jsonResponse);
            
            //
            // check privileges
            //
            
            boolean hasAccess = hasAccess(jsonRoot, api);
            if (hasAccess)
              {
                token = JSONUtilities.decodeString(jsonRoot, "Token", true);
                
                //
                //  return correct token
                //
                
                return token;
              }
            else 
              {
                throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.INSUFFICIENT_USER_RIGHTS.getGenericResponseMessage(), RESTAPIGenericReturnCodes.INSUFFICIENT_USER_RIGHTS.getGenericResponseCode());
              }
          }
        else if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 401)
          {
            log.error("FWK server HTTP reponse code {} message {} ", httpResponse.getStatusLine().getStatusCode(), EntityUtils.toString(httpResponse.getEntity(), "UTF-8"));
            throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.AUTHENTICATION_FAILURE.getGenericResponseMessage(), RESTAPIGenericReturnCodes.AUTHENTICATION_FAILURE.getGenericResponseCode());
          }
        else if(httpResponse != null && httpResponse.getStatusLine() != null)
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
    catch (IOException e)
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
  
  private boolean hasAccess(JSONObject jsonRoot, String api)
  {
    boolean result = true;

    //
    //  build user permissions
    //

    List<String> userPermissions = new ArrayList<String>();
    JSONArray userPermissionArray = JSONUtilities.decodeJSONArray(jsonRoot, "Permissions", true);
    for (int i=0; i<userPermissionArray.size(); i++)
      {
        userPermissions.add((String) userPermissionArray.get(i));
      }

    //
    //  check method access
    //
    
    ThirdPartyMethodAccessLevel methodAccessLevel = methodPermissionsMapper.get(api);
    if (null == methodAccessLevel || (methodAccessLevel.getPermissions().isEmpty() && methodAccessLevel.getWorkgroups().isEmpty()))
      {
        result = false;
        log.warn("No permission/workgroup is configured for method {} ", api);
      }
    else
      {
        //
        //  check workgroup (if specified)
        //
        
        JSONObject userWorkgroupHierarchyJSON = JSONUtilities.decodeJSONObject(jsonRoot, "WorkgroupHierarchy", false);
        if (null != userWorkgroupHierarchyJSON)
          {
            JSONObject userWorkgroupJSON = JSONUtilities.decodeJSONObject(userWorkgroupHierarchyJSON, "Workgroup", true);
            String userWorkgroup = JSONUtilities.decodeString(userWorkgroupJSON, "Name", true);
            result = methodAccessLevel.getWorkgroups().contains(userWorkgroup);
          }
        
        //
        //  check permissions
        //
        
        if (result)
          {
            for (String userPermission : userPermissions)
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
        result = subscriberIDService.getSubscriberID(subscriberTraceControlAlternateID, customerID);
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
    private String token;
    private JSONObject jsonRepresentation;
    
    //
    //  accessors
    //
    
    public String getLoginName() { return loginName; }
    public String getPassword() { return password; }
    public String getToken() { return token; }
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
      this.token = JSONUtilities.decodeString(jsonRoot, "token", false);
      
      //
      //  jsonRepresentation
      //
          
      jsonRepresentation = new JSONObject();
      if (token == null || token.isEmpty()) 
        {
          jsonRepresentation.put("LoginName", loginName);
          jsonRepresentation.put("Password", password);
        }
    }
  }
  
}
