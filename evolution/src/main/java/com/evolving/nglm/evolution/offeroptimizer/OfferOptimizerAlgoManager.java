package com.evolving.nglm.evolution.offeroptimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.CatalogCharacteristicService;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;
import com.evolving.nglm.evolution.OfferSalesChannelsAndPrice;
import com.evolving.nglm.evolution.ProductService;
import com.evolving.nglm.evolution.ProductTypeService;
import com.evolving.nglm.evolution.PropensityKey;
import com.evolving.nglm.evolution.PropensityState;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.SubscriberEvaluationRequest;
import com.evolving.nglm.evolution.SubscriberGroupEpoch;
import com.evolving.nglm.evolution.SubscriberProfile;


/**
 * This class is the reference in term of repository of Offer Optimizer
 * algorithm
 * 
 * @author fduclos
 *
 */
public class OfferOptimizerAlgoManager {
  private static Logger logger = Logger.getLogger(OfferOptimizerAlgoManager.class);
  private static OfferOptimizerAlgoManager instance;
  private static Object lock = new Object();
  private HashMap<String, IOfferOptimizerAlgorithm> algorithmInstances = new HashMap<>();

  public static OfferOptimizerAlgoManager getInstance() {
    if (instance == null) 
      {
        synchronized (lock) 
        {
          {
            if (instance == null) 
              {
                instance = new OfferOptimizerAlgoManager();
              }
          }
        }
      }
    return instance;
  }

  public Collection<ProposedOfferDetails> applyScoreAndSort(OfferOptimizationAlgorithm algoDefinitions,
      Map<OfferOptimizationAlgorithmParameter, String> algoParameters, Set<Offer> offers, SubscriberProfile subscriberProfile, double minScoreThreshold,
      String requestedSalesChannelId, ProductService productService, ProductTypeService productTypeService,
      CatalogCharacteristicService catalogCharacteristicService,
      ReferenceDataReader<PropensityKey, PropensityState> propensityDataReader,
      ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, 
      SegmentationDimensionService segmentationDimensionService, StringBuffer returnedLog) {

    if (offers == null || offers.size() == 0) 
      {
        returnedLog.append("No offer to sort");
        logger.warn("OfferOptimizerAlgoManager.applyScoreAndSort No offer to sort, ");
        return new ArrayList<>();
      }

    // let retreive the algo class
    IOfferOptimizerAlgorithm algo = algorithmInstances.get(algoDefinitions.getID());
    if (algo == null) 
      {
        // must create it
        synchronized (algorithmInstances) 
        {
          algo = algorithmInstances.get(algoDefinitions.getID());
          if (algo == null) 
            {
              try 
              {
                algo = (IOfferOptimizerAlgorithm) Class.forName(algoDefinitions.getJavaClass()).newInstance();
              } 
              catch (Exception e) 
              {
                logger.warn(
                    "OfferOptimizerAlgoManager.applyScoreAndSort Can't instanciate algo with ID "
                        + algoDefinitions.getID() + " and class name " + algoDefinitions.getJavaClass(),
                        e);
              }
            }
        }
      }

    if (algo == null) 
      {
        logger.warn("OfferOptimizerAlgoManager.applyScoreAndSort Can't retrieve algo " + algoDefinitions.getID());
        returnedLog.append("No Algo " + algoDefinitions.getID() + ", ");
        return new ArrayList<>();
      }

    List<ProposedOfferDetails> result = new ArrayList<>();
    for (Offer o : offers) 
      {
        // 
        // Validate propensity rule before using it
        //
        
        PropensityState propensityState = null;
        
        if(Deployment.getPropensityRule().validate(segmentationDimensionService))
          {
            // retrieve the current propensity for this offer
            PropensityKey pk = new PropensityKey(o.getOfferID(), subscriberProfile, subscriberGroupEpochReader);
            propensityState = propensityDataReader.get(pk);
          }
        
        double currentPropensity = 0.5; // TODO: In the future, use: o.getInitialPropensity();
        if (propensityState != null) 
          {
            int presentationThreshold = Deployment.getPropensityInitialisationPresentationThreshold();
            int daysThreshold = Deployment.getPropensityInitialisationDurationInDaysThreshold();
            currentPropensity = propensityState.getPropensity(0.50d, o.getEffectiveStartDate(), presentationThreshold, daysThreshold); // TODO: In the future, use: o.getInitialPropensity();
          } 
        else 
          {
            // just log a warn and keep initial propensity
            logger.warn("OfferOptimizerAlgoManager.applyScoreAndSort Could not retrieve propensity for offer "
                + o.getOfferID());
          }
        logger.trace("OfferOptimizerAlgoManager.applyScoreAndSort Propensity for offer "
            + o.getOfferID()+ " = " + currentPropensity);

        // TODO TRACE TO REMOVE
        for (OfferSalesChannelsAndPrice salesChannelAndPrice : o.getOfferSalesChannelsAndPrices()) 
          {
            for (String salesChannelID : salesChannelAndPrice.getSalesChannelIDs()) 
              {
                logger.trace("--> salesChannelID : "+salesChannelID);
              }
          }
        // TODO END TRACE TO REMOVE

        for (OfferSalesChannelsAndPrice salesChannelAndPrice : o.getOfferSalesChannelsAndPrices()) 
          {
            for (String salesChannelID : salesChannelAndPrice.getSalesChannelIDs()) 
              {
                if (salesChannelID.equals(requestedSalesChannelId)) 
                  {
                    SubscriberEvaluationRequest subscriberEvaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime());
                    ProposedOfferDetails scorePerChannel = algo.getOfferPropensityScore(algoParameters, o,
                        salesChannelID,
                        currentPropensity, salesChannelAndPrice.getPrice() != null
                        ? salesChannelAndPrice.getPrice().getAmount() : 0,
                            subscriberEvaluationRequest, algoDefinitions, productService, productTypeService, catalogCharacteristicService);

                    if (logger.isDebugEnabled()) 
                      {
                        logger.debug("OfferOptimizerAlgoManager.applyScoreAndSort scorePerChannel "
                            + (scorePerChannel != null
                            ? (scorePerChannel.getOfferId() + " " + scorePerChannel.getOfferScore()
                            + " " + scorePerChannel.getSalesChannelId())
                                : null));
                      }

                    if (scorePerChannel == null) 
                      {
                        logger.warn(
                            "OfferOptimizerAlgoManager.applyScoreAndSort Could not evaluate score for offer "
                                + o.getOfferID() + " sales Channel " + salesChannelID);
                        returnedLog.append("[Skipped No Score, Offer:" + o.getOfferID() + ",channel:" + salesChannelID + ",score:" + scorePerChannel + ",threshold:" + minScoreThreshold +"], ");

                      } 
                    else if (scorePerChannel.getOfferScore() >= minScoreThreshold) 
                      {
                        if (logger.isTraceEnabled()) logger.trace("*** Add offer " + o.getOfferID() + " with score " + scorePerChannel.getOfferScore() + " for channel " + requestedSalesChannelId);
                        returnedLog.append("[Proposed Offer:" + o.getOfferID() + ",channel:" + salesChannelID + ",score:" + scorePerChannel + ",threshold:" + minScoreThreshold +"], ");
                        result.add(scorePerChannel);
                      } 
                    else 
                      {
                        returnedLog.append("[Skipped under threshold, Offer:" + o.getOfferID() + ",channel:" + salesChannelID + ",score:" + scorePerChannel.getOfferScore() + ",threshold:" + minScoreThreshold +"], ");
                        if (logger.isDebugEnabled()) 
                          {
                            logger.debug("OfferOptimizerAlgoManager.applyScoreAndSort "
                                + scorePerChannel.getOfferId() + " " + scorePerChannel.getSalesChannelId()
                                + " has a score lower than the threashold " + minScoreThreshold);
                          }
                      }
                  }
                else 
                  {
                    if (logger.isTraceEnabled()) 
                      {
                        logger.trace("OfferOptimizerAlgoManager.applyScoreAndSort Ignore offer " + o.getOfferID() +" with sales channel " + salesChannelID);
                      }
                  }
              }
          }
      }
    // now sort it...
    if(result.size() > 0)
      {
        Collections.sort(result);
      }

    return result;
  }
}
