package com.evolving.nglm.evolution.offeroptimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.propensity.PropensityService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;


/**
 * This class is the reference in term of repository of Offer Optimizer
 * algorithm
 * 
 * @author fduclos
 *
 */
public class OfferOptimizerAlgoManager {
  private static Logger logger = LoggerFactory.getLogger(OfferOptimizerAlgoManager.class);
  private static OfferOptimizerAlgoManager instance;
  private static PropensityService propensityService;
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
      String requestedSalesChannelId, ProductService productService, ProductTypeService productTypeService, VoucherService voucherService, VoucherTypeService voucherTypeService,
      CatalogCharacteristicService catalogCharacteristicService,
      ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader,
      SegmentationDimensionService segmentationDimensionService, DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameters,StringBuffer returnedLog, int tenantID) {

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
        double currentPropensity = o.getInitialPropensity();
        if(Deployment.getDeployment(tenantID).getPropensityRule().validate(segmentationDimensionService))
          {
            // lazy init of the propensityService if needed
            if(propensityService==null)
              {
                synchronized (lock)
                  {
                    if(propensityService==null)
                      {
                        propensityService=new PropensityService(subscriberGroupEpochReader);
                      }
                  }
              }
            int presentationThreshold = Deployment.getDeployment(tenantID).getPropensityInitialisationPresentationThreshold();
            int daysThreshold = Deployment.getDeployment(tenantID).getPropensityInitialisationDurationInDaysThreshold();
            currentPropensity = propensityService.getPropensity(o.getOfferID(), subscriberProfile, o.getInitialPropensity(),o.getEffectiveStartDate(),presentationThreshold,daysThreshold);
          }
        else 
          {
            // just log a warn and keep initial propensity
            logger.warn("OfferOptimizerAlgoManager.applyScoreAndSort Could not retrieve propensity for offer "
                + o.getOfferID());
          }
        if(logger.isDebugEnabled()) logger.debug("OfferOptimizerAlgoManager.applyScoreAndSort Propensity for offer "
            + o.getOfferID()+ " = " + currentPropensity);

        if(logger.isDebugEnabled())
          {
            for (OfferSalesChannelsAndPrice salesChannelAndPrice : o.getOfferSalesChannelsAndPrices())
              {
                for (String salesChannelID : salesChannelAndPrice.getSalesChannelIDs())
                  {
                    logger.debug("--> salesChannelID : "+salesChannelID);
                  }
              }
          }


        boolean skipRemaining = false;
        for (OfferSalesChannelsAndPrice salesChannelAndPrice : o.getOfferSalesChannelsAndPrices()) 
          {
            for (String salesChannelID : salesChannelAndPrice.getSalesChannelIDs()) 
              {
                if ((requestedSalesChannelId == null) || (salesChannelID.equals(requestedSalesChannelId))) 
                  {
                    SubscriberEvaluationRequest subscriberEvaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime(), tenantID);
                    ProposedOfferDetails scorePerChannel = algo.getOfferPropensityScore(algoParameters, o,
                        salesChannelID,
                        currentPropensity, salesChannelAndPrice.getPrice() != null
                        ? salesChannelAndPrice.getPrice().getAmount() : 0,
                            subscriberEvaluationRequest, algoDefinitions, productService, productTypeService, voucherService, voucherTypeService, catalogCharacteristicService,dnboMatrixAlgorithmParameters);

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
                        // TODO Temporary : when scoring through multiple channels, only consider first channel, and skip all other ones 
                        if (requestedSalesChannelId == null)
                          {
                            skipRemaining = true;
                            break;
                          }
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
            if (skipRemaining) break;
          }
      }
    // now sort it...
    if(result.size() > 0)
      {
        Collections.sort(result);
        OfferOptimizationAlgorithmParameter reversedParameter = new OfferOptimizationAlgorithmParameter("reversed");
        String isReversedString = algoParameters.get(reversedParameter);
        if (isReversedString != null)
        {
          if (Boolean.parseBoolean(isReversedString))
          {
            Collections.reverse(result);
          }
        }
      }

    return result;
  }
}
