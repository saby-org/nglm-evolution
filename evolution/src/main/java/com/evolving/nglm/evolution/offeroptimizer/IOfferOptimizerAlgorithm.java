package com.evolving.nglm.evolution.offeroptimizer;

import com.evolving.nglm.evolution.CatalogCharacteristicService;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm;
import com.evolving.nglm.evolution.ProductService;
import com.evolving.nglm.evolution.ProductTypeService;
import com.evolving.nglm.evolution.SubscriberEvaluationRequest;
import com.evolving.nglm.evolution.SubscriberProfile;

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
		String valueMode, 
		Offer o,
		String salesChannelId, 
		int offerCurrentPropensity,
		long offerPrice,
		SubscriberEvaluationRequest subscriberEvaluationRequest,
		OfferOptimizationAlgorithm algoDefinition,
		ProductService productService,
	    ProductTypeService productTypeService,
	    CatalogCharacteristicService catalogCharacteristicService);   
}
