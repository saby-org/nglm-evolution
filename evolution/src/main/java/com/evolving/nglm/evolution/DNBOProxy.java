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

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
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
    subscriberGroupEpochReader = ReferenceDataReader.<String, SubscriberGroupEpoch>startReader("dnboproxy", "dnboproxy-subscribergroupepochreader-"+apiProcessKey,
        Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
    propensityDataReader = ReferenceDataReader.<PropensityKey, PropensityState>startReader("dnboproxy", "dnboproxy-propensityreader-"+apiProcessKey,
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
    *  score
    *
    *****************************************/
    
    StringBuffer returnedLog = new StringBuffer();
    String logFragment;

    // ###################################### SCORING GROUP
    // ####################################################################

    // String msisdn = subscriberProfile.getMSISDN();
    String msisdn = subscriberID; // TODO : check this

    try {
      ScoringGroup selectedScoringGroup = getScoringGroup(scoringStrategy, subscriberProfile);
      logFragment = "DNBOProxy.processGetOffers " + scoringStrategy.getScoringStrategyID() + " Selected ScoringGroup for " + msisdn + " " + selectedScoringGroup;
      returnedLog.append(logFragment+", ");
      if (log.isDebugEnabled())
      {
        log.debug(logFragment);
      }

      // ##############################" ScoringSplit
      // ##############################

      ScoringSplit selectedScoringSplit = getScoringSplit(msisdn, selectedScoringGroup);
      logFragment = "DNBOProxy.processGetOffers ScoringStrategy : " + selectedScoringSplit+ ", selectedScoringSplit : "+selectedScoringSplit.getOfferObjectiveIDs();
      returnedLog.append(logFragment+", ");
      if (log.isDebugEnabled())
      {
        log.debug(logFragment);
      }

      Set<Offer> offersForAlgo = getOffersToOptimize(msisdn, selectedScoringSplit.getOfferObjectiveIDs(), subscriberProfile);

      OfferOptimizationAlgorithm algo = selectedScoringSplit.getOfferOptimizationAlgorithm();
      if (algo == null)
      {
        log.warn("DNBOProxy.processGetOffers No Algo returned for selectedScoringSplit " + scoringStrategy.getScoringStrategyID());
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
      logFragment = "DNBOProxy.processGetOffers Threshold value " + threshold;
      returnedLog.append(logFragment+", ");
      if (log.isDebugEnabled())
      {
        log.debug(logFragment);
      }

      String valueMode = selectedScoringSplit.getParameters().get(valueModeParameter);
      logFragment = "DNBOProxy.processGetOffers no value mode";
      returnedLog.append(logFragment+", ");
      if (valueMode == null || valueMode.equals(""))
      {
        log.warn(logFragment);

      }
      
      log.trace("Subscriber status : "+subscriberProfile.getEvolutionSubscriberStatus());

      // This returns an ordered Collection (and sorted by offerScore)
      Collection<ProposedOfferDetails> offerAvailabilityFromPropensityAlgo =
          OfferOptimizerAlgoManager.getInstance().applyScoreAndSort(
              algo, valueMode, offersForAlgo, subscriberProfile, threshold, salesChannelID,
              productService, productTypeService, catalogCharacteristicService,
              propensityDataReader, returnedLog);

      if(offerAvailabilityFromPropensityAlgo == null){
        log.warn("DNBOProxy.processGetOffers Return empty list of offer");
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
      value.put("scoringStrategyID", scoringStrategyID);
      value.put("msisdn", subscriberID);
      value.put("strategy", scoringStrategyID);
      value.put("offers", scoredOffersJSON);
      value.put("log", returnedLog.toString());

      //
      //  response
      //

      HashMap<String,Object> response = new HashMap<String,Object>();
      response.put("responseCode", "ok");
      response.put("statusCode", 0);
      response.put("status", "SUCCESS");
      response.put("message", "getSubscriberOffers well executed");
      response.put("value", JSONUtilities.encodeObject(value));
      return JSONUtilities.encodeObject(response);
    } catch (Exception e)
    {
      log.warn("DNBOProxy.processGetOffers Exception " + e.getClass().getName() + " " + e.getMessage() + " while retrieving offers " + msisdn + " " + scoringStrategyID, e);
      throw new DNBOProxyException("Exception while retrieving offers ", scoringStrategyID);
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
