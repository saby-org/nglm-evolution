package com.evolving.nglm.evolution.offeroptimizer;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.CatalogCharacteristicService;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm;
import com.evolving.nglm.evolution.ProductService;
import com.evolving.nglm.evolution.ProductTypeService;
import com.evolving.nglm.evolution.SubscriberEvaluationRequest;
import com.evolving.nglm.evolution.VoucherService;
import com.evolving.nglm.evolution.VoucherTypeService;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;

public class DNBOMatrixOfferOptimizerAlgo implements IOfferOptimizerAlgorithm
{
  private static final Logger log = LoggerFactory.getLogger(DNBOMatrixOfferOptimizerAlgo.class);
  
  @Override
  public ProposedOfferDetails getOfferPropensityScore(Map<OfferOptimizationAlgorithmParameter, String> algoParameters, Offer offer, String salesChannelId, double offerCurrentPropensity, long offerPrice, SubscriberEvaluationRequest subscriberEvaluationRequest, OfferOptimizationAlgorithm algoDefinition, ProductService productService, ProductTypeService productTypeService, VoucherService voucherService, VoucherTypeService voucherTypeService, CatalogCharacteristicService catalogCharacteristicService, DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameters)
  {
    //
    //  fake ProposedOfferDetails
    //
    
    ProposedOfferDetails result = new ProposedOfferDetails(offer.getOfferID(), salesChannelId, offerCurrentPropensity);
    log.info("[DNBOMatrixOfferOptimizerAlgo] OfferID[{}] : offerCurrentPropensity[{}]", result.getOfferId(), result.getOfferScore());
    
    return result;
  }

}
