/*****************************************************************************
*
*  DNBOProxy.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DNBOProxy
{
  /*****************************************
  *
  *  ProductID
  *
  *****************************************/

  public static String ProductID = "Evolution-DNBOProxy";

  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum API
  {
    getSubscriberOffers("getSubscriberOffers"),
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

  private static final Logger log = LoggerFactory.getLogger(DNBOProxy.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  static
  //

  private static final int RESTAPIVersion = 1;

  //
  //  instance
  //

  private HttpServer restServer;
  private OfferService offerService;
  private ScoringStrategyService scoringStrategyService;
  private SalesChannelService salesChannelService;
  private SubscriberProfileService subscriberProfileService;

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    NGLMRuntime.initialize();
    DNBOProxy dnboProxy = new DNBOProxy();
    dnboProxy.start(args);
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
    int apiRestPort = parseInteger("apiRestPort", args[1]);

    //
    //  log
    //

    log.info("main START: {} {}", apiProcessKey, apiRestPort);

    /*****************************************
    *
    *  services - construct
    *
    *****************************************/

    offerService = new OfferService(Deployment.getBrokerServers(), "dnboproxy-offerservice-" + apiProcessKey, Deployment.getOfferTopic(), false);
    scoringStrategyService = new ScoringStrategyService(Deployment.getBrokerServers(), "dnboproxy-scoringstrategyservice-" + apiProcessKey, Deployment.getScoringStrategyTopic(), false);
    salesChannelService = new SalesChannelService(Deployment.getBrokerServers(), "dnboproxy-saleschannelservice-" + apiProcessKey, Deployment.getSalesChannelTopic(), false);
    subscriberProfileService = new EngineSubscriberProfileService(Deployment.getSubscriberProfileEndpoints());

    /*****************************************
    *
    *  services - start
    *
    *****************************************/

    offerService.start();
    scoringStrategyService.start();
    salesChannelService.start();
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
        restServer.createContext("/nglm-dnboproxy/getSubscriberOffers", new APIHandler(API.getSubscriberOffers));
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

    NGLMRuntime.addShutdownHook(new ShutdownHook(restServer, offerService, scoringStrategyService, salesChannelService, subscriberProfileService));

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
    private ScoringStrategyService scoringStrategyService;
    private SalesChannelService salesChannelService;
    private SubscriberProfileService subscriberProfileService;

    //
    //  constructor
    //

    private ShutdownHook(HttpServer restServer, OfferService offerService, ScoringStrategyService scoringStrategyService, SalesChannelService salesChannelService, SubscriberProfileService subscriberProfileService)
    {
      this.restServer = restServer;
      this.offerService = offerService;
      this.scoringStrategyService = scoringStrategyService;
      this.salesChannelService = salesChannelService;
      this.subscriberProfileService = subscriberProfileService;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      //
      //  services
      //

      if (offerService != null) offerService.stop();
      if (scoringStrategyService != null) scoringStrategyService.stop();
      if (salesChannelService != null) salesChannelService.stop();
      if (subscriberProfileService != null) subscriberProfileService.stop();

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
        JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse((requestBodyStringBuilder.length()) > 0 ? requestBodyStringBuilder.toString() : "{ }");

        /*****************************************
        *
        *  backward compatibility (move arguments from the URI to the body)
        *
        *****************************************/

        //
        //  apiVersion
        //

        if (JSONUtilities.decodeInteger(jsonRoot, "apiVersion", false) == null) 
          {
            jsonRoot.put("apiVersion", RESTAPIVersion);
          }

        //
        //  scoringStrategy
        //

        if (JSONUtilities.decodeString(jsonRoot, "scoringStrategyID", false) == null && exchange.getRequestURI().getQuery() != null)
          {
            Pattern pattern = Pattern.compile("^(.*\\&strategy|strategy)=(.*?)(\\&.*$|$)");
            Matcher matcher = pattern.matcher(exchange.getRequestURI().getQuery());
            if (matcher.matches())
              {
                jsonRoot.put("scoringStrategyID", matcher.group(2));
              }
          }

        //
        //  subscriberID
        //

        if (JSONUtilities.decodeString(jsonRoot, "subscriberID", false) == null && exchange.getRequestURI().getQuery() != null)
          {
            Pattern pattern = Pattern.compile("^(.*\\&msisdn|msisdn)=(.*?)(\\&.*$|$)");
            Matcher matcher = pattern.matcher(exchange.getRequestURI().getQuery());
            if (matcher.matches())
              {
                jsonRoot.put("subscriberID", matcher.group(2));
              }
          }

        //
        //  salesChannelID
        //

        if (JSONUtilities.decodeString(jsonRoot, "salesChannelID", false) == null && exchange.getRequestURI().getQuery() != null)
          {
            Pattern pattern = Pattern.compile("^(.*\\&salesChannel|salesChannel)=(.*?)(\\&.*$|$)");
            Matcher matcher = pattern.matcher(exchange.getRequestURI().getQuery());
            if (matcher.matches())
              {
                jsonRoot.put("salesChannelID", matcher.group(2));
              }
          }
        
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
        *  process
        *
        *****************************************/

        JSONObject jsonResponse = null;
        switch (api)
          {
            case getSubscriberOffers:
              jsonResponse = processGetOffers(userID, jsonRoot);
              break;
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
    catch (org.json.simple.parser.ParseException | DNBOProxyException | SubscriberProfileServiceException | IOException | ServerException | RuntimeException e )
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
        response.put("statusCode", 1);
        response.put("status", "systemError");
        response.put("message", e.getMessage());
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
  *  processGetOffers
  *
  *****************************************/

  private JSONObject processGetOffers(String userID, JSONObject jsonRoot) throws DNBOProxyException, SubscriberProfileServiceException
  {
    /*****************************************
    *
    *  arguments
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    String subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    String scoringStrategyID = JSONUtilities.decodeString(jsonRoot, "scoringStrategyID", true);
    String salesChannelID = JSONUtilities.decodeString(jsonRoot, "salesChannelID", true);
    
    /*****************************************
    *
    *  evaluate arguments
    *
    *****************************************/

    SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID);
    ScoringStrategy scoringStrategy = scoringStrategyService.getActiveScoringStrategy(scoringStrategyID, now);
    SalesChannel salesChannel = salesChannelService.getActiveSalesChannel(salesChannelID, now);

    /*****************************************
    *
    *  validate arguments
    *
    *****************************************/

    if (subscriberProfile == null) throw new DNBOProxyException("unknown subscriber", subscriberID);
    if (scoringStrategy == null) throw new DNBOProxyException("unknown scoring strategy", scoringStrategyID);
    if (salesChannel == null) throw new DNBOProxyException("unknown sales channel", salesChannelID);

    /*****************************************
    *
    *  score (replace this default section with real code)
    *
    *****************************************/

    String log = "(dummy log)";
    List<ScoredOffer> scoredOffers = new ArrayList<ScoredOffer>();
    for (Offer offer : offerService.getActiveOffers(now))
      {
        scoredOffers.add(new ScoredOffer(offer, salesChannel, 7, 1));
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    //
    //  offers
    //

    List<JSONObject> scoredOffersJSON = new ArrayList<JSONObject>();
    for (ScoredOffer scoredOffer : scoredOffers)
      {
        scoredOffersJSON.add(scoredOffer.getJSONRepresentation());
      }

    //
    //  value
    //

    HashMap<String,Object> value = new HashMap<String,Object>();
    value.put("subscriberID", subscriberID);
    value.put("scoringStrategyID", scoringStrategyID);
    value.put("msisdn", subscriberID);
    value.put("strategy", scoringStrategyID);
    value.put("offers", scoredOffersJSON);
    value.put("log", log);
    
    //
    //  response
    //

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("statusCode", 0);
    response.put("status", "SUCCESS");
    response.put("message", "GetOffer well executed");
    response.put("value", JSONUtilities.encodeObject(value));
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  class ScoredOffer
  *
  *****************************************/

  private static class ScoredOffer
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private Offer offer;
    private SalesChannel salesChannel;
    private int offerScore;
    private int offerRank;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ScoredOffer(Offer offer, SalesChannel salesChannel, int offerScore, int offerRank)
    {
      this.offer = offer;
      this.salesChannel = salesChannel;
      this.offerScore = offerScore;
      this.offerRank = offerRank;
    }

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public Offer getOffer() { return offer; }
    public SalesChannel getSalesChannel() { return salesChannel; }
    public int getOfferScore() { return offerScore; }
    public int getOfferRank() { return offerRank; }

    /*****************************************
    *
    *  getJSONRepresentation
    *
    *****************************************/

    public JSONObject getJSONRepresentation()
    {
      HashMap<String,Object> jsonRepresentation = new HashMap<String,Object>();
      jsonRepresentation.put("offerID", offer.getOfferID());
      jsonRepresentation.put("salesChannelId", salesChannel.getSalesChannelID());
      jsonRepresentation.put("offerScore", offerScore);
      jsonRepresentation.put("offerRank", offerRank);
      return JSONUtilities.encodeObject(jsonRepresentation);
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
  *  class DNBOProxyException
  *
  *****************************************/

  public static class DNBOProxyException extends Exception
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

    public DNBOProxyException(String responseMessage, String responseParameter)
    {
      super(responseMessage);
      this.responseParameter = responseParameter;
    }

    /*****************************************
    *
    *  constructor - exception
    *
    *****************************************/

    public DNBOProxyException(Throwable e)
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
