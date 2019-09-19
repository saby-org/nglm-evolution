package com.evolving.nglm.evolution.offeroptimizer;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import com.evolving.nglm.evolution.CatalogCharacteristicService;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;
import com.evolving.nglm.evolution.ProductService;
import com.evolving.nglm.evolution.ProductTypeService;
import com.evolving.nglm.evolution.SubscriberEvaluationRequest;

public class RandomAlgo implements IOfferOptimizerAlgorithm {

  private static Logger logger = Logger.getLogger(RandomAlgo.class);

  @Override
  public ProposedOfferDetails getOfferPropensityScore(
      Map<OfferOptimizationAlgorithmParameter,
      String> algoParameters,
      Offer o,
      String salesChannelId,
      double offerCurrentPropensity,
      long offerPrice, SubscriberEvaluationRequest subscriberEvaluationRequest,
      OfferOptimizationAlgorithm algoDefinition,
      ProductService productService, 
      ProductTypeService productTypeService,
      CatalogCharacteristicService catalogCharacteristicService,
      DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameterser)
  {
    if (logger.isTraceEnabled())
      {
        logger.trace("RandomAlgo.getOfferPropensityScore Entered "
          + algoParameters + " " + o.getOfferID() + " " + salesChannelId + " "  + offerCurrentPropensity + " "
          + offerPrice + " " + subscriberEvaluationRequest.getSubscriberProfile() + " " + algoDefinition);
      }
    // note : tried LocalDateTime.now().getNano() but does not provide true nanosec granularity on my JVM
    int random = (new Random()).nextInt(1000); // [ 0 .. 999 ]
    double score = ((double) random) / 1000d; // { 0.000, 0.001, 0.002, ... , 0.999 }
    ProposedOfferDetails pod = new ProposedOfferDetails(o.getOfferID(), salesChannelId, score);
    return pod;
  }
}
