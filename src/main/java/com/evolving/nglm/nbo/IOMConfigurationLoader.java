/*****************************************************************************
*
*  IOMConfigurationLoader.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.OfferService.OfferListener;
import com.evolving.nglm.evolution.PresentationStrategyService.PresentationStrategyListener;
import com.evolving.nglm.evolution.ScoringStrategyService.ScoringStrategyListener;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
   
public class IOMConfigurationLoader
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(IOMConfigurationLoader.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Map<String,Long> configuredPresentationStrategies;
  private Map<String,Long> configuredScoringStrategies;
  private Map<String,Long> configuredOffers;
  private OfferService offerService;
  private PresentationStrategyService presentationStrategyService;
  private ScoringStrategyService scoringStrategyService;

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    NGLMRuntime.initialize();
    IOMConfigurationLoader iomConfigurationLoader = new IOMConfigurationLoader();
    iomConfigurationLoader.start(args);
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
    String presentationStrategyTopic = Deployment.getPresentationStrategyTopic();
    String scoringStrategyTopic = Deployment.getScoringStrategyTopic();
    String offerTopic = Deployment.getOfferTopic();
    //
    //  log
    //

    log.info("main START: {} {} {} {}", apiProcessKey, bootstrapServers, offerTopic, presentationStrategyTopic, scoringStrategyTopic);

    /*****************************************
    *
    *  presentation strategy service 
    *
    *****************************************/

    //
    //  retrieve previously configured strategies
    //
    
    configuredPresentationStrategies = retrieveConfiguredPresentationStrategies();

    //
    //  presentationStrategyListener
    //

    PresentationStrategyListener presentationStrategyListener = new PresentationStrategyListener()
    {
      //
      //  activated
      //

      @Override public void presentationStrategyActivated(PresentationStrategy presentationStrategy)
      {
        if (configuredPresentationStrategies.get(presentationStrategy.getPresentationStrategyID()) == null)
          {
            activatePresentationStrategy(presentationStrategy);
            configuredPresentationStrategies.put(presentationStrategy.getPresentationStrategyID(), presentationStrategy.getEpoch());
          }
        else if (presentationStrategy.getEpoch() > configuredPresentationStrategies.get(presentationStrategy.getPresentationStrategyID()))
          {
            updatePresentationStrategy(presentationStrategy);
            configuredPresentationStrategies.put(presentationStrategy.getPresentationStrategyID(), presentationStrategy.getEpoch());
          }
      }

      //
      //  deactivated
      //

      @Override public void presentationStrategyDeactivated(PresentationStrategy presentationStrategy)
      {
        if (configuredPresentationStrategies.containsKey(presentationStrategy.getPresentationStrategyID()))
          {
            deactivatePresentationStrategy(presentationStrategy.getPresentationStrategyID());
            configuredPresentationStrategies.remove(presentationStrategy.getPresentationStrategyID());
          }
      }
    };

    //
    //  load service 
    //

    presentationStrategyService = new PresentationStrategyService(bootstrapServers, "iomconfigurationloader-presentationstrategyservice-" + apiProcessKey, presentationStrategyTopic, false, presentationStrategyListener);

    //
    //  restart active
    //

    for (PresentationStrategy presentationStrategy : presentationStrategyService.getActivePresentationStrategies(presentationStrategyService.getLastUpdate()))
      {
        presentationStrategyListener.presentationStrategyActivated(presentationStrategy);
      }

    //
    //  restart inactive
    //

    Set<String> deactivatedPresentationStrategies = new HashSet<String>();
    for (String presentationStrategyID : configuredPresentationStrategies.keySet())
      {
        if (presentationStrategyService.getActivePresentationStrategy(presentationStrategyID, presentationStrategyService.getLastUpdate()) == null)
          {
            deactivatePresentationStrategy(presentationStrategyID);
          }
      }

    /*****************************************
    *
    *  scoring strategy service 
    *
    *****************************************/

    //
    //  retrieve previously configured strategies
    //
    
    configuredScoringStrategies = retrieveConfiguredScoringStrategies();

    //
    //  scoringStrategyListener
    //

    ScoringStrategyListener scoringStrategyListener = new ScoringStrategyListener()
    {
      //
      //  activated
      //

      @Override public void scoringStrategyActivated(ScoringStrategy scoringStrategy)
      {
        if (configuredScoringStrategies.get(scoringStrategy.getScoringStrategyID()) == null)
          {
            activateScoringStrategy(scoringStrategy);
            configuredScoringStrategies.put(scoringStrategy.getScoringStrategyID(), scoringStrategy.getEpoch());
          }
        else if (scoringStrategy.getEpoch() > configuredScoringStrategies.get(scoringStrategy.getScoringStrategyID()))
          {
            updateScoringStrategy(scoringStrategy);
            configuredScoringStrategies.put(scoringStrategy.getScoringStrategyID(), scoringStrategy.getEpoch());
          }
      }

      //
      //  deactivated
      //

      @Override public void scoringStrategyDeactivated(ScoringStrategy scoringStrategy)
      {
        if (configuredScoringStrategies.containsKey(scoringStrategy.getScoringStrategyID()))
          {
            deactivateScoringStrategy(scoringStrategy.getScoringStrategyID());
            configuredScoringStrategies.remove(scoringStrategy.getScoringStrategyID());
          }
      }
    };

    //
    //  load service 
    //

    scoringStrategyService = new ScoringStrategyService(bootstrapServers, "iomconfigurationloader-scoringstrategyservice-" + apiProcessKey, scoringStrategyTopic, false, scoringStrategyListener);

    //
    //  restart active
    //

    for (ScoringStrategy scoringStrategy : scoringStrategyService.getActiveScoringStrategies(scoringStrategyService.getLastUpdate()))
      {
        scoringStrategyListener.scoringStrategyActivated(scoringStrategy);
      }

    //
    //  restart inactive
    //

    Set<String> deactivatedScoringStrategies = new HashSet<String>();
    for (String scoringStrategyID : configuredScoringStrategies.keySet())
      {
        if (scoringStrategyService.getActiveScoringStrategy(scoringStrategyID, scoringStrategyService.getLastUpdate()) == null)
          {
            deactivateScoringStrategy(scoringStrategyID);
          }
      }

    /*****************************************
    *
    *  offer service 
    *
    *****************************************/

    //
    //  retrieve previously configured strategies
    //
    
    configuredOffers = retrieveConfiguredOffers();

    //
    //  offerListener
    //

    OfferListener offerListener = new OfferListener()
    {
      //
      //  activated
      //

      @Override public void offerActivated(Offer offer)
      {
        if (configuredOffers.get(offer.getOfferID()) == null)
          {
            activateOffer(offer);
            configuredOffers.put(offer.getOfferID(), offer.getEpoch());
          }
        else if (offer.getEpoch() > configuredOffers.get(offer.getOfferID()))
          {
            updateOffer(offer);
            configuredOffers.put(offer.getOfferID(), offer.getEpoch());
          }
      }

      //
      //  deactivated
      //

      @Override public void offerDeactivated(Offer offer)
      {
        if (configuredOffers.containsKey(offer.getOfferID()))
          {
            deactivateOffer(offer.getOfferID());
            configuredOffers.remove(offer.getOfferID());
          }
      }
    };

    //
    //  load service 
    //

    offerService = new OfferService(bootstrapServers, "iomconfigurationloader-offerservice-" + apiProcessKey, offerTopic, false, offerListener);

    //
    //  restart active
    //

    for (Offer offer : offerService.getActiveOffers(offerService.getLastUpdate()))
      {
        offerListener.offerActivated(offer);
      }

    //
    //  restart inactive
    //

    Set<String> deactivatedOffers = new HashSet<String>();
    for (String offerID : configuredOffers.keySet())
      {
        if (offerService.getActiveOffer(offerID, offerService.getLastUpdate()) == null)
          {
            deactivateOffer(offerID);
          }
      }

    /*****************************************
    *
    *  start listeners
    *
    *****************************************/

    presentationStrategyService.start();
    scoringStrategyService.start();
    offerService.start();
  }

  /*****************************************
  *
  *  retrieveConfiguredPresentationStrategies
  *    -- called at startup
  *    -- returns all presentation strategies that are currently configured/active in IOM
  *    -- must return a Map from presentationStrategyID to the epoch for that presentationStrategy
  *
  *****************************************/

  private Map<String,Long> retrieveConfiguredPresentationStrategies()
  {
    return new HashMap<String,Long>();
  }

  /*****************************************
  *
  *  activatePresentationStrategy
  *    -- called when a presentationStrategy is first activated
  *    -- NOTE:  epoch must be stored (see retrieveConfiguredPresentationStrategies)
  *
  *****************************************/

  private void activatePresentationStrategy(PresentationStrategy presentationStrategy)
  {
    System.out.println("presentationStrategy activated: " + presentationStrategy.getPresentationStrategyID());
  }

  /*****************************************
  *
  *  updatePresentationStrategy
  *    -- called when a previously configured presentationStrategy is updated
  *    -- NOTE:  epoch must be updated to the new epoch (see retrieveConfiguredPresentationStrategies)
  *
  *****************************************/

  private void updatePresentationStrategy(PresentationStrategy presentationStrategy)
  {
    System.out.println("presentationStrategy updated: " + presentationStrategy.getPresentationStrategyID());
  }

  /*****************************************
  *
  *  deactivatePresentationStrategy
  *    -- called when a previously configured presentationStrategy is deactivated/removed/invalidated
  *
  *****************************************/

  private void deactivatePresentationStrategy(String presentationStrategyID)
  {
    System.out.println("presentationStrategy deactivated: " + presentationStrategyID);
  }

  /*****************************************
  *
  *  retrieveConfiguredScoringStrategies
  *    -- called at startup
  *    -- returns all scoring strategies that are currently configured/active in IOM
  *    -- must return a Map from scoringStrategyID to the epoch for that scoringStrategy
  *
  *****************************************/

  private Map<String,Long> retrieveConfiguredScoringStrategies()
  {
    return new HashMap<String,Long>();
  }

  /*****************************************
  *
  *  activateScoringStrategy
  *    -- called when a scoringStrategy is first activated
  *    -- NOTE:  epoch must be stored (see retrieveConfiguredScoringStrategies)
  *
  *****************************************/

  private void activateScoringStrategy(ScoringStrategy scoringStrategy)
  {
    System.out.println("scoringStrategy activated: " + scoringStrategy.getScoringStrategyID());
  }

  /*****************************************
  *
  *  updateScoringStrategy
  *    -- called when a previously configured scoringStrategy is updated
  *    -- NOTE:  epoch must be updated to the new epoch (see retrieveConfiguredScoringStrategies)
  *
  *****************************************/

  private void updateScoringStrategy(ScoringStrategy scoringStrategy)
  {
    System.out.println("scoringStrategy updated: " + scoringStrategy.getScoringStrategyID());
  }

  /*****************************************
  *
  *  deactivateScoringStrategy
  *    -- called when a previously configured scoringStrategy is deactivated/removed/invalidated
  *
  *****************************************/

  private void deactivateScoringStrategy(String scoringStrategyID)
  {
    System.out.println("scoringStrategy deactivated: " + scoringStrategyID);
  }

  /*****************************************
  *
  *  retrieveConfiguredOffers
  *    -- called at startup
  *    -- returns all offers that are currently configured/active in IOM
  *    -- must return a Map from offerID to the epoch for that offer
  *
  *****************************************/

  private Map<String,Long> retrieveConfiguredOffers()
  {
    return new HashMap<String,Long>();
  }

  /*****************************************
  *
  *  activateOffer
  *    -- called when an offer is first activated
  *    -- NOTE:  epoch must be stored (see retrieveConfiguredOffers)
  *
  *****************************************/

  private void activateOffer(Offer offer)
  {
    System.out.println("offer activated: " + offer.getOfferID());
  }

  /*****************************************
  *
  *  updateOffer
  *    -- called when a previously configured offer is updated
  *    -- NOTE:  epoch must be updated to the new epoch (see retrieveConfiguredOffers)
  *
  *****************************************/

  private void updateOffer(Offer offer)
  {
    System.out.println("offer updated: " + offer.getOfferID());
  }

  /*****************************************
  *
  *  deactivateOffer
  *    -- called when a previously configured offer is deactivated/removed/invalidated
  *
  *****************************************/

  private void deactivateOffer(String offerID)
  {
    System.out.println("offer deactivated: " + offerID);
  }
}
