/*****************************************************************************
*
*  DNBOProxy.java
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
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;
import com.evolving.nglm.evolution.offeroptimizer.GetOfferException;
import com.evolving.nglm.evolution.offeroptimizer.OfferOptimizerAlgoManager;
import com.evolving.nglm.evolution.offeroptimizer.ProposedOfferDetails;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

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
  private ProductService productService;
  private ProductTypeService productTypeService;
  private CatalogCharacteristicService catalogCharacteristicService;
  private ScoringStrategyService scoringStrategyService;
  private SalesChannelService salesChannelService;
  private SubscriberProfileService subscriberProfileService;
  private ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader = null;
  private ReferenceDataReader<PropensityKey, PropensityState> propensityDataReader = null;

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
    productService = new ProductService(Deployment.getBrokerServers(), "dnboproxy-productservice-" + apiProcessKey, Deployment.getProductTopic(), false);
    productTypeService = new ProductTypeService(Deployment.getBrokerServers(), "dnboproxy-producttypeservice-" + apiProcessKey, Deployment.getProductTypeTopic(), false);
    catalogCharacteristicService = new CatalogCharacteristicService(Deployment.getBrokerServers(), "dnboproxy-catalogcharacteristicservice-" + apiProcessKey, Deployment.getCatalogCharacteristicTopic(), false);
    scoringStrategyService = new ScoringStrategyService(Deployment.getBrokerServers(), "dnboproxy-scoringstrategyservice-" + apiProcessKey, Deployment.getScoringStrategyTopic(), false);
    salesChannelService = new SalesChannelService(Deployment.getBrokerServers(), "dnboproxy-saleschannelservice-" + apiProcessKey, Deployment.getSalesChannelTopic(), false);
    subscriberProfileService = new EngineSubscriberProfileService(Deployment.getSubscriberProfileEndpoints());
    subscriberGroupEpochReader = ReferenceDataReader.<String, SubscriberGroupEpoch>startReader("dnboproxy-subscribergroupepoch", "dnboproxy-subscribergroupepochreader-"+apiProcessKey,
        Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
    propensityDataReader = ReferenceDataReader.<PropensityKey, PropensityState>startReader("dnboproxy-propensitystate", "dnboproxy-propensityreader-"+apiProcessKey,
        Deployment.getBrokerServers(), Deployment.getPropensityLogTopic(), PropensityState::unpack);
    
    /*****************************************
    *
    *  services - start
    *
    *****************************************/

    offerService.start();
    productService.start();
    productTypeService.start();
    catalogCharacteristicService.start();
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

    NGLMRuntime.addShutdownHook(new ShutdownHook(restServer, offerService, productService, productTypeService, catalogCharacteristicService, scoringStrategyService, salesChannelService, subscriberProfileService));

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
    private ProductService productService;   
    private ProductTypeService productTypeService;
    private CatalogCharacteristicService catalogCharacteristicService;
    private ScoringStrategyService scoringStrategyService;
    private SalesChannelService salesChannelService;
    private SubscriberProfileService subscriberProfileService;

    //
    //  constructor
    //

    private ShutdownHook(HttpServer restServer, OfferService offerService, ProductService productService,
        ProductTypeService productTypeService, CatalogCharacteristicService catalogCharacteristicService,
        ScoringStrategyService scoringStrategyService, SalesChannelService salesChannelService,
        SubscriberProfileService subscriberProfileService)
    {
      this.restServer = restServer;
      this.offerService = offerService;
      this.productService = productService;
      this.productTypeService = productTypeService;
      this.catalogCharacteristicService = catalogCharacteristicService;
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
      if (productService != null) productService.stop();
      if (productTypeService != null) productTypeService.stop();
      if (catalogCharacteristicService != null) catalogCharacteristicService.stop();
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
        
        if (jsonRoot.get("scoringStrategyID") == null && exchange.getRequestURI().getQuery() != null)
          {
            Pattern pattern = Pattern.compile("^(.*\\&strategy|strategy)=(.*?)(\\&.*$|$)");
            Matcher matcher = pattern.matcher(exchange.getRequestURI().getQuery());
            if (matcher.matches())
              {
                String scoringStrategyID = matcher.group(2);
                Object objectToAdd;
                if (scoringStrategyID != null && scoringStrategyID.indexOf(",") == -1)
                  {
                    //
                    // single value
                    //
                    objectToAdd = scoringStrategyID;
                  }
                else
                  {
                    //
                    // We got a list : 21,22,23 -> convert it to a JSON array
                    //
                    List<String> items = Arrays.asList(scoringStrategyID.split("\\s*,\\s*"));
                    objectToAdd = JSONUtilities.encodeArray(items);
                    
                  }
                jsonRoot.put("scoringStrategyID", objectToAdd);
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
              jsonResponse = processGetSubscriberOffers(userID, jsonRoot);
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
  *  processGetSubscriberOffers
  *
  *****************************************/

  private JSONObject processGetSubscriberOffers(String userID, JSONObject jsonRoot) throws DNBOProxyException, SubscriberProfileServiceException
  {
    /*****************************************
    *
    *  arguments
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    String subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    List<String> scoringStrategyIDList;
    boolean multipleStrategies = true;
    try
    {
      scoringStrategyIDList = JSONUtilities.decodeJSONArray(jsonRoot, "scoringStrategyID", true);
    }
    catch (JSONUtilitiesException e)
    {
      //
      // We may have received a single parameter; In this case, we construct a list with one element
      //
      String scoringStrategyID = JSONUtilities.decodeString(jsonRoot, "scoringStrategyID", true);
      scoringStrategyIDList = new ArrayList<>();
      scoringStrategyIDList.add(scoringStrategyID);
      multipleStrategies = false;
    }
    String salesChannelID = JSONUtilities.decodeString(jsonRoot, "salesChannelID", true);
    
    /*****************************************
    *
    *  evaluate arguments
    *
    *****************************************/
    
    SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID);
    SalesChannel salesChannel = salesChannelService.getActiveSalesChannel(salesChannelID, now); 
    
    /*****************************************
    *
    *  validate arguments
    *
    *****************************************/

    if (subscriberProfile == null) throw new DNBOProxyException("unknown subscriber", subscriberID);
    if (salesChannel == null) throw new DNBOProxyException("unknown sales channel", salesChannelID);

    //
    // Process list of ScoringStrategies
    //
    
    List<JSONObject> resArray = new ArrayList<>();
    boolean allScoringStrategiesBad = true;
    boolean atLeastOneError = false;
    DNBOProxyException lastException = null;
    for (String scoringStrategyID : scoringStrategyIDList ) // process all scoringStrategies
      {
        if (log.isDebugEnabled())
          {
            log.debug("DNBOproxy.processGetSubscriberOffers Processing "+scoringStrategyID+" ...");
          }
        ScoringStrategy scoringStrategy = scoringStrategyService.getActiveScoringStrategy(scoringStrategyID, now);
        if (scoringStrategy == null)
          {
            log.info("DNBOproxy.processGetSubscriberOffers Unknown scoring strategy in list : " + scoringStrategyID);
            atLeastOneError = true;
            continue;
          }
        try
        {
          JSONObject valueRes = getOffers(now, subscriberID, scoringStrategyID, salesChannelID, subscriberProfile, scoringStrategy, salesChannel);
          resArray.add(valueRes);
          allScoringStrategiesBad = false;
        }
        catch (DNBOProxyException e)
        {
          log.warn("DNBOProxy.processGetSubscriberOffers Exception " + e.getClass().getName() + " while processing " + scoringStrategyID, e);
          lastException = e;
          atLeastOneError = true;
        }
     
      }
    //
    //  response
    //

    if (allScoringStrategiesBad)
      {
        StringBuilder res = new StringBuilder();
        for (String scoringStrategyID : scoringStrategyIDList)
          {
            res.append(scoringStrategyID+" ");
          }
        log.info("DNBOproxy.processGetSubscriberOffers All scoring strategies are bad : " + res.toString());
        if (lastException == null)
          {
            throw new DNBOProxyException("All scoring strategies are bad : "+ res.toString(), "");
          }
        else
          {
            throw lastException;
          }
      }

    HashMap<String,Object> response = new HashMap<String,Object>();
    response.put("responseCode", "ok");
    response.put("statusCode", 0);
    response.put("status", "SUCCESS");
    String msg = "getSubscriberOffers ";
    if (atLeastOneError)
      {
        msg += "partially executed";
      }
    else
      {
        msg += "well executed";
      }
    response.put("message", msg);
    if (multipleStrategies)
      {
        response.put("value", JSONUtilities.encodeArray(resArray));        
      }
    else if (resArray.size() == 1)
      {
        response.put("value", resArray.get(0));
      }
    else if (resArray.size() == 0)
      {
        log.info("DNBOproxy.processGetSubscriberOffers Internal error, list is empty");  
        throw new DNBOProxyException("Unexpected error : list is empty", "");
      }
    else
      {
        log.warn("DNBOproxy.processGetSubscriberOffers Internal error, list should contain exactly 1 element, not " + resArray.size() + ", taking first one");
        response.put("value", resArray.get(0));
      }
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  getOffers
  *
  *****************************************/

  private JSONObject getOffers(Date now, String subscriberID, String scoringStrategyID, String salesChannelID,
      SubscriberProfile subscriberProfile, ScoringStrategy scoringStrategy, SalesChannel salesChannel)
      throws DNBOProxyException
  {

    /*****************************************
    *
    *  score
    *
    *****************************************/
    
    StringBuffer returnedLog = new StringBuffer();
    String logFragment;

    // ###################################### SCORING GROUP
    // ####################################################################

    String msisdn = subscriberID;

    JSONObject valueRes;
    
    try {
      ScoringGroup selectedScoringGroup = getScoringGroup(scoringStrategy, subscriberProfile);
      logFragment = "DNBOProxy.getOffers " + scoringStrategy.getScoringStrategyID() + " Selected ScoringGroup for " + msisdn + " " + selectedScoringGroup;
      returnedLog.append(logFragment+", ");
      if (log.isDebugEnabled())
      {
        log.debug(logFragment);
      }

      // ##############################" ScoringSplit
      // ##############################

      ScoringSplit selectedScoringSplit = getScoringSplit(msisdn, selectedScoringGroup);
      logFragment = "DNBOProxy.getOffers ScoringStrategy : " + selectedScoringSplit+ ", selectedScoringSplit : "+selectedScoringSplit.getOfferObjectiveIDs();
      returnedLog.append(logFragment+", ");
      if (log.isDebugEnabled())
      {
        log.debug(logFragment);
      }

      Set<Offer> offersForAlgo = getOffersToOptimize(msisdn, selectedScoringSplit.getOfferObjectiveIDs(), subscriberProfile);

      OfferOptimizationAlgorithm algo = selectedScoringSplit.getOfferOptimizationAlgorithm();
      if (algo == null)
      {
        log.warn("DNBOProxy.getOffers No Algo returned for selectedScoringSplit " + scoringStrategy.getScoringStrategyID());
        throw new DNBOProxyException("No Algo returned for selectedScoringSplit ", scoringStrategy.getScoringStrategyID());
      }

      OfferOptimizationAlgorithmParameter thresholdParameter = new OfferOptimizationAlgorithmParameter("thresholdValue");

      OfferOptimizationAlgorithmParameter valueModeParameter = new OfferOptimizationAlgorithmParameter("valueMode");

      double threshold = 0;
      String thresholdString = selectedScoringSplit.getParameters().get(thresholdParameter);
      if (thresholdString != null)
      {
        threshold = Double.parseDouble(thresholdString);
      }
      logFragment = "DNBOProxy.getOffers Threshold value " + threshold;
      returnedLog.append(logFragment+", ");
      if (log.isDebugEnabled())
      {
        log.debug(logFragment);
      }

      String valueMode = selectedScoringSplit.getParameters().get(valueModeParameter);
      if (valueMode == null || valueMode.equals(""))
      {
        logFragment = "DNBOProxy.getOffers no value mode";
        if (log.isDebugEnabled())
          {
            log.debug(logFragment);
          }
        returnedLog.append(logFragment+", ");
      }
      
      // This returns an ordered Collection (and sorted by offerScore)
      Collection<ProposedOfferDetails> offerAvailabilityFromPropensityAlgo =
          OfferOptimizerAlgoManager.getInstance().applyScoreAndSort(
              algo, valueMode, offersForAlgo, subscriberProfile, threshold, salesChannelID,
              productService, productTypeService, catalogCharacteristicService,
              propensityDataReader, subscriberGroupEpochReader, returnedLog);

      if (offerAvailabilityFromPropensityAlgo == null)
        {
          offerAvailabilityFromPropensityAlgo = new ArrayList<>();
        }

      //
      // Now add some predefined offers based on alwaysAppendOfferObjectiveIDs of ScoringSplit
      //
      Set<String> offerObjectiveIds = selectedScoringSplit.getAlwaysAppendOfferObjectiveIDs();
      for(Offer offer : offerService.getActiveOffers(now))
        {
          boolean inList = true;
          for (OfferObjectiveInstance offerObjective : offer.getOfferObjectives()) 
            {
              if (!offerObjectiveIds.contains(offerObjective.getOfferObjectiveID())) 
                {
                  inList = false;
                  break;
                }
            }
          if (!inList) 
            {
              //
              // not a single objective of this offer is in the list of the scoringSplit -> skip it
              //
              continue;
            }
          for (OfferSalesChannelsAndPrice salesChannelAndPrice : offer.getOfferSalesChannelsAndPrices())
            {
              for (String loopSalesChannelID : salesChannelAndPrice.getSalesChannelIDs()) 
                {
                  if (loopSalesChannelID.equals(salesChannelID)) 
                    {
                      String offerId = offer.getOfferID();
                      boolean offerIsAlreadyInList = false;
                      for (ProposedOfferDetails offerAvail : offerAvailabilityFromPropensityAlgo)
                        {
                          if (offerAvail.getOfferId().equals(offerId))
                            {
                              offerIsAlreadyInList = true;
                              if (log.isTraceEnabled()) log.trace("DNBOProxy.getOffers offer "+offerId+" already in list, skip it");
                              break;
                            }
                        }
                      if (!offerIsAlreadyInList)
                        {
                          if (log.isTraceEnabled()) log.trace("DNBOProxy.getOffers offer "+offerId+" added to the list because its objective is in alwaysAppendOfferObjectiveIDs of ScoringStrategy");
                          ProposedOfferDetails additionalDetails = new ProposedOfferDetails(offerId, salesChannelID, 0);
                          offerAvailabilityFromPropensityAlgo.add(additionalDetails);
                        }
                    }
                }
            }
        }

      if (offerAvailabilityFromPropensityAlgo.isEmpty())
        {
          log.warn("DNBOProxy.getOffers Return empty list of offers");
          throw new DNBOProxyException("No Offer available while executing getOffer ", scoringStrategy.getScoringStrategyID());
        }
      
      int index = 1;
      for (ProposedOfferDetails current : offerAvailabilityFromPropensityAlgo)
      {
        current.setOfferRank(index);
        index++;
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
      for (ProposedOfferDetails proposedOffer : offerAvailabilityFromPropensityAlgo)
      {
        scoredOffersJSON.add(proposedOffer.getJSONRepresentation());
      }

      //
      //  value
      //

      HashMap<String,Object> value = new HashMap<String,Object>();
      value.put("subscriberID", subscriberID);
      value.put("msisdn", subscriberID); // subscriberID expected by caller here (for compatibility reasons)
      value.put("scoringStrategyID", scoringStrategyID);
      value.put("strategy", scoringStrategyID);
      value.put("offers", scoredOffersJSON);
      value.put("log", returnedLog.toString());
      valueRes = JSONUtilities.encodeObject(value);
    }
    catch (Exception e)
    {
      log.warn("DNBOProxy.getOffers Exception " + e.getClass().getName() + " " + e.getMessage() + " while retrieving offers " + msisdn + " " + scoringStrategyID, e);
      throw new DNBOProxyException("Exception while retrieving offers ", scoringStrategyID);
    }
    return valueRes;
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
  
  private Set<Offer> getOffersToOptimize(String msisdn, Set<String> catalogObjectiveIDs, SubscriberProfile subscriberProfile)
  {
    // Browse all offers:
    // - filter by offer objective coming from the split strategy
    // - filter by profile of subscriber
    // Return a set of offers that can be optimised
    Collection<Offer> offers = offerService.getActiveOffers(Calendar.getInstance().getTime());
    Set<Offer> result = new HashSet<>();
    for (String currentSplitObjectiveID : catalogObjectiveIDs)
    {
      log.trace("currentSplitObjectiveID : "+currentSplitObjectiveID);
      for (Offer currentOffer : offers)
      {
        for (OfferObjectiveInstance currentOfferObjective : currentOffer.getOfferObjectives())
        {
          log.trace("    offerID : "+currentOffer.getOfferID()+" offerObjectiveID : "+currentOfferObjective.getOfferObjectiveID());
          if (currentOfferObjective.getOfferObjectiveID().equals(currentSplitObjectiveID))
          {
            // this offer is a good candidate for the moment, let's check the profile
            SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(
                subscriberProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime());
            if (currentOffer.evaluateProfileCriteria(evaluationRequest))
            {
              log.trace("        add offer : "+currentOffer.getOfferID());
              result.add(currentOffer);
            }
          }
        }
      }
    }
    return result;
  }

  private ScoringSplit getScoringSplit(String msisdn, ScoringGroup selectedScoringGroup) throws GetOfferException
  {
    // now let evaluate the split testing...
    // let compute the split bashed on hash...
    int nbSamples = selectedScoringGroup.getScoringSplits().size();
    // let get the same sample for the subscriber for the whole strategy
    String hashed = msisdn + nbSamples;
    int hash = hashed.hashCode();
    if (hash < 0)
      {
        hash = hash * -1; // can happen because a hashcode is a SIGNED
        // integer
      }
    int sampleId = hash % nbSamples; // modulo

    // now retrieve the good split strategy for this user:
    ScoringSplit selectedScoringSplit = selectedScoringGroup.getScoringSplits().get(sampleId);

    if (selectedScoringSplit == null)
      {
        // should never happen since modulo plays on the number of samples
        log.warn("DNBOProxy.getScoringSplit Split Testing modulo problem for " + msisdn + " and subStrategy " + selectedScoringGroup);
        throw new GetOfferException("Split Testing modulo problem for " + msisdn + " and subStrategy " + selectedScoringGroup);
      }
    return selectedScoringSplit;
  }

  private ScoringGroup getScoringGroup(ScoringStrategy strategy, SubscriberProfile subscriberProfile) throws GetOfferException
  {
    // let retrieve the first sub strategy that maps this user:
    Date now = SystemTime.getCurrentTime();
    SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(
        subscriberProfile, subscriberGroupEpochReader, now);

    ScoringGroup selectedScoringGroup = strategy.evaluateScoringGroups(evaluationRequest);
    if (log.isDebugEnabled())
      {
        log.debug("DNBOProxy.getScoringGroup Retrieved matching scoringGroup " + (selectedScoringGroup != null ? display(selectedScoringGroup.getProfileCriteria()) : null));
      }

    if (selectedScoringGroup == null)
      {
        throw new GetOfferException("Can't retrieve ScoringGroup for strategy " + strategy.getScoringStrategyID() + " and msisdn " + subscriberProfile.getSubscriberID());
      }
    return selectedScoringGroup;
  }

  public static String display(List<EvaluationCriterion> profileCriteria) {
    StringBuffer res = new StringBuffer();
    for (EvaluationCriterion ec : profileCriteria) {
      res.append("["+ec.getCriterionContext()+","+ec.getCriterionField()+","+ec.getCriterionOperator()+","+ec.getArgumentExpression()+","+ec.getStoryReference()+"]");
    }
    return res.toString();
  }
  
}