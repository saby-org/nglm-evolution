package com.evolving.nglm.evolution.offeroptimizer;

import java.util.Map;
import java.util.Random;

import com.evolving.nglm.evolution.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;

public class RandomAlgo implements IOfferOptimizerAlgorithm {

  private static Logger logger = LoggerFactory.getLogger(RandomAlgo.class);

  @Override
  public ProposedOfferDetails getOfferPropensityScore(
      Map<OfferOptimizationAlgorithmParameter,String> algoParameters,
      Offer o,
      String salesChannelId,
      double offerCurrentPropensity,
      long offerPrice, SubscriberEvaluationRequest subscriberEvaluationRequest,
      OfferOptimizationAlgorithm algoDefinition,
      ProductService productService, 
      ProductTypeService productTypeService,
      VoucherService voucherService,
      VoucherTypeService voucherTypeService,
      CatalogCharacteristicService catalogCharacteristicService,
      DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameterser)
  {
    if (logger.isTraceEnabled())
      {
        logger.trace("RandomAlgo.getOfferPropensityScore Entered "
          + algoParameters + " " + o.getOfferID() + " " + salesChannelId + " "  + offerCurrentPropensity + " "
          + offerPrice + " " + subscriberEvaluationRequest.getSubscriberProfile() + " " + algoDefinition);
      }
    
    OfferOptimizationAlgorithmParameter predictableParameter = new OfferOptimizationAlgorithmParameter("predictable");
    String predictableString = algoParameters.get(predictableParameter);
    Boolean predictable = false; // default value
    if (predictableString != null)
    {
      predictable = Boolean.parseBoolean(predictableString);
    }
    double score;
    if (predictable)
      {
        // Offers will sort according to creation date (created last -> highest score)
        // This formula works with dates from 2019 to 2025, and for offers created within 1 second of each other
        score = ((((o.getCreatedDate().getTime()-1_550_000_000_000L) / (1_750_000_000_000D-1_550_000_000_000D)*(double)Integer.MAX_VALUE)));
      }
    else
      {
        // note : tried LocalDateTime.now().getNano() but does not provide true nanosec granularity on my JVM
        int random = (new Random()).nextInt(1000); // [ 0 .. 999 ]
        score = ((double) random) / 1000d; // { 0.000, 0.001, 0.002, ... , 0.999 }
      }
    ProposedOfferDetails pod = new ProposedOfferDetails(o.getOfferID(), salesChannelId, score);
    return pod;
  }
}
