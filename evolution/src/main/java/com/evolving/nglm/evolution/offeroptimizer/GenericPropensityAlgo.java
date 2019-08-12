package com.evolving.nglm.evolution.offeroptimizer;

import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import com.evolving.nglm.evolution.CatalogCharacteristicService;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;
import com.evolving.nglm.evolution.ProductService;
import com.evolving.nglm.evolution.ProductTypeService;
import com.evolving.nglm.evolution.SubscriberEvaluationRequest;

public class GenericPropensityAlgo implements IOfferOptimizerAlgorithm {

  private static Logger logger = Logger.getLogger(GenericPropensityAlgo.class);

  /**
   * @param valueMode
   * @param o
   * @param offerCurrentPropensity
   * @param subscriberProfile
   * @param tContext
   * @return
   */
  @Override
  public ProposedOfferDetails getOfferPropensityScore(Map<OfferOptimizationAlgorithmParameter, String> algoParameters,
      Offer o, String salesChannelId,
      double offerCurrentPropensity, long offerPrice, SubscriberEvaluationRequest subscriberEvaluationRequest,
      OfferOptimizationAlgorithm algoDefinition, ProductService productService, 
      ProductTypeService productTypeService, CatalogCharacteristicService catalogCharacteristicService) {
    if (logger.isTraceEnabled()) {
      logger.trace("GenericPropensityAlgo.getOfferPropensityScore Entered "
          + algoParameters + " " + o.getOfferID() + " " + salesChannelId + " "  + offerCurrentPropensity + " "
          + offerPrice + " " + subscriberEvaluationRequest.getSubscriberProfile() + " " + algoDefinition);
    }
    if (o.getOfferProducts().size() == 0) {
      // error, should have 1 product...
      logger.warn(
          "GenericPropensityAlgo.getOfferPropensityScore Could not find product into offer " + o.getOfferID());
      return null;
    }
    long random = (new Date().getTime()) % 100; // [ 0 .. 99 ]
    double score = ((double) random) / 100d;
    ProposedOfferDetails opd = new ProposedOfferDetails(o.getOfferID(), salesChannelId, score);
    return opd;
  }

}
