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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Alarm;
import com.evolving.nglm.core.LicenseChecker;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.LicenseChecker.LicenseState;
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
  
  /*****************************************
  *
  *  enum
  *
  *****************************************/
  
  private enum API
  {
    ping,
    getActiveOffer,
    getActiveOffers;
  }
  
  /*****************************************
  *
  *  configuration
  *
  *****************************************/
  
  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ThirdPartyManager.class);
  
  //
  //  license
  //

  private LicenseChecker licenseChecker = null;
  
  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private OfferService offerService;

  private static final int RESTAPIVersion = 1;
  private HttpServer restServer;
  
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
    String nodeID = System.getProperty("nglm.license.nodeid");
    String offerTopic = Deployment.getOfferTopic();
    
    //
    //  log
    //

    log.info("main START: {} {} {}", apiProcessKey, bootstrapServers, apiRestPort);
    
    //
    //  license
    //

    licenseChecker = new LicenseChecker(ProductID, nodeID, Deployment.getZookeeperRoot(), Deployment.getZookeeperConnect());
    
    /*****************************************
    *
    *  services
    *
    *****************************************/
    
    //
    //  construct
    //
    
    offerService = new OfferService(bootstrapServers, "thirdpartymanager-offerservice-" + apiProcessKey, offerTopic, false); //thirdpartymanager-offerservice? Yes, I believe its correct as its the consumer grouId
    
    //
    //  start
    //
    
    offerService.start();
    
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
        restServer.createContext("/nglm-thirdpartymanager/getActiveOffer", new APIHandler(API.getActiveOffer));
        restServer.createContext("/nglm-thirdpartymanager/getActiveOffers", new APIHandler(API.getActiveOffers));
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
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(restServer, offerService));
    
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
    private OfferService offerService;
    
    //
    //  constructor
    //

    private ShutdownHook(HttpServer restServer, OfferService offerService)
    {
      this.restServer = restServer;
      this.offerService = offerService;
    }
    
    //
    //  shutdown
    //
    
    @Override public void shutdown(boolean normalShutdown)
    {
      //
      //  reference data reader
      //

      //
      //  services
      //
      
      if (offerService != null) offerService.stop();
      
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
        
        //
        //  standard response fields
        //

        response.put("apiVersion", RESTAPIVersion);
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
  *  ping
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
    response.put("responseCode", "ok");
    response.put("ping", responseStr);
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
        response.put("responseCode", "offerNotFound");
        return JSONUtilities.encodeObject(response);
      }
    else 
      {
        response.put("responseCode", "ok");
        response.put("offer", ThirdPartyJSONGenerator.generateOfferJSONForThirdParty(offer));
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
    response.put("responseCode", "ok");
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
  *  class ThirdPartyManagerException
  *
  *****************************************/
  
  public static class ThirdPartyManagerException extends Exception
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
    
    public ThirdPartyManagerException(String responseMessage, String responseParameter)
    {
      super(responseMessage);
      this.responseParameter = responseParameter;
    }

    /*****************************************
    *
    *  constructor - excpetion
    *
    *****************************************/
    
    public ThirdPartyManagerException(Throwable e)
    {
      super(e.getMessage(), e);
      this.responseParameter = null;
    }
  }
}
