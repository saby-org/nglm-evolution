package com.evolving.nglm.evolution.offeroptimizer;

import java.util.Map;

import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;

public interface IOfferOptimizerAlgorithm
{

  /**
   * 
   * @param valueMode : 
   * @param o
   * @param salesChannelId
   * @param offerCurrentPropensity
   * @param offerPrice
   * @param subscriberProfile
   * @return
   */
  public ProposedOfferDetails getOfferPropensityScore(
    Map<OfferOptimizationAlgorithmParameter, String> algoParameters, 
		Offer o,
		String salesChannelId, 
		double offerCurrentPropensity,
		long offerPrice,
		SubscriberEvaluationRequest subscriberEvaluationRequest,
		OfferOptimizationAlgorithm algoDefinition,
		ProductService productService,
	    ProductTypeService productTypeService,
	    CatalogCharacteristicService catalogCharacteristicService,
        DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameters);
}
