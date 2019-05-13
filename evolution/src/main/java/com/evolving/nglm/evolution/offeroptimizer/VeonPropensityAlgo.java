package com.evolving.nglm.evolution.offeroptimizer;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.CatalogCharacteristic;
import com.evolving.nglm.evolution.CatalogCharacteristicInstance;
import com.evolving.nglm.evolution.CatalogCharacteristicService;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.MetricHistory;
import com.evolving.nglm.evolution.OfferProduct;
import com.evolving.nglm.evolution.Product;
import com.evolving.nglm.evolution.ProductService;
import com.evolving.nglm.evolution.ProductType;
import com.evolving.nglm.evolution.ProductTypeInstance;
import com.evolving.nglm.evolution.ProductTypeService;
import com.evolving.nglm.evolution.Offer;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm;
import com.evolving.nglm.evolution.SubscriberProfile;

public class VeonPropensityAlgo implements IOfferOptimizerAlgorithm {

  private static Logger logger = Logger.getLogger(VeonPropensityAlgo.class);

  /**
   * @param valueMode
   * @param o
   * @param offerCurrentPropensity
   * @param subscriberProfile
   * @param tContext
   * @return
   */
  @Override
  public ProposedOfferDetails getOfferPropensityScore(String valueMode, Offer o, String salesChannelId,
      int offerCurrentPropensity, long offerPrice, SubscriberProfile subscriberProfile,
      OfferOptimizationAlgorithm algoDefinition, ProductService productService, 
      ProductTypeService productTypeService, CatalogCharacteristicService catalogCharacteristicService) {
    if (logger.isTraceEnabled()) {
      logger.trace("VeonPropensityAlgo.getOfferPropensityScore Entered " + valueMode + " " + o.getOfferID() + " " + salesChannelId + " "  + offerCurrentPropensity + " " + offerPrice + " " + subscriberProfile + " " + algoDefinition);
    }
    if (o.getOfferProducts().size() == 0) {
      // error, should have 1 product...
      logger.warn(
          "VeonPropensityAlgo.getOfferPropensityScore Could not find product into offer " + o.getOfferID());
      return null;
    }

    // Algo:
    // Check the type of Algo to know if it is P, V or PxV
    // Based on the type of Product, we know which characteristics to read
    // and also which consumption to get from profile
    switch (algoDefinition.getName()) {
    case "P":
      double score = (double) (offerCurrentPropensity) / 100d;
      ProposedOfferDetails opd = new ProposedOfferDetails(o.getOfferID(), salesChannelId, score);
      return opd;
    case "V":
      score = computeV(valueMode, o, salesChannelId, offerCurrentPropensity, offerPrice, subscriberProfile,
          algoDefinition, productService, productTypeService, catalogCharacteristicService);
      opd = new ProposedOfferDetails(o.getOfferID(), salesChannelId, score);
      return opd;
    case "PxV":
      double propensity = (double) (offerCurrentPropensity) / 100d;
      Double v = computeV(valueMode, o, salesChannelId, offerCurrentPropensity, offerPrice, subscriberProfile,
          algoDefinition, productService, productTypeService, catalogCharacteristicService);
      if(v == null){
        logger.warn("VeonPropensityAlgo.getOfferPropensityScore Could not get a valid score for " + valueMode + " " + o.getOfferID() + " " + salesChannelId + " "  + offerCurrentPropensity + " " + offerPrice + " " + subscriberProfile + " " + algoDefinition);
        return null;
      }
      score = propensity * v;
      opd = new ProposedOfferDetails(o.getOfferID(), salesChannelId, score);
      return opd;
    default:
      logger.warn("VeonPropensityAlgo2.getOfferPropensityScore Could not interpret algo with name "
          + algoDefinition.getName());
      return null;
    }
  }

  private Double computeV(String valueMode, Offer o, String salesChannelId, int offerCurrentPropensity,
      long offerPrice, SubscriberProfile subscriberProfile, OfferOptimizationAlgorithm algoDefinition,
      ProductService productService, ProductTypeService productTypeService, CatalogCharacteristicService catalogCharacteristicService) {

    // Check the type of product
    if (o.getOfferProducts() == null || o.getOfferProducts().size() != 1) {
      // just log a Warn
      logger.warn("VeonPropensityAlgo2.computePxV Offer " + o.getOfferID() + " does not have exactly 1 product "
          + (o.getOfferProducts() != null ? o.getOfferProducts().size() : null));
    }
    double v = 0;
    for (OfferProduct op : o.getOfferProducts()) { // there will have only 1
      // product normally...
      // let retrieve the characteristic related to the nb of days of
      // expiration
      if (logger.isTraceEnabled()) {
        logger.trace("VeonPropensityAlgo.computeV Focus on product " + op.getProductID());
      }
      Product p = productService.getActiveProduct(op.getProductID(),
          Calendar.getInstance().getTime());

      if (p == null) {
        // this is not coherent, let raise a warning and return null;
        logger.warn("VeonPropensityAlgo.getOfferPropensityScore No product with ID " + op.getProductID()
        + " is currently active, let ignore the current offer's propensity scoring");
        return null;
      }
      if (p.getProductTypes() == null || p.getProductTypes().size() != 1) {
        logger.warn("VeonPropensityAlgo2.computePxV Product " + p.getProductID()
        + " does not have exactly 1 productType "
        + (p.getProductTypes() != null ? p.getProductTypes().size() : null));
      }
      // now let retrieve the characteristic...
      ArrayList<Double> consumptions = new ArrayList<>();
      ArrayList<Double> expirations = new ArrayList<>();
      ArrayList<Double> weights = new ArrayList<>();

      if (logger.isTraceEnabled()) {
        logger.trace("VeonPropensityAlgo.computeV Ready to get product types of product " + p.getProductID());
      }

      for (ProductTypeInstance productType : p.getProductTypes()) { 
        // get the productType definition
        ProductType pt = productTypeService
            .getActiveProductType(productType.getProductTypeID(), new Date());
        switch (pt.getProductTypeName()) {
        case "data":
          MetricHistory mh = subscriberProfile.getDataRevenueAmountMetricHistory();
          boolean ok = enrichInfos(mh, "dataExpiration", null, productType, consumptions, expirations,
              weights, catalogCharacteristicService);
          if (!ok) {
            logger.warn(
                "VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration DATA Pack OfferID "
                    + o.getOfferID());
            return null;
          }
          break;
        case "voice":
          mh = subscriberProfile.getVoiceRevenueAmountMetricHistory();
          ok = enrichInfos(mh, "voiceExpiration", null, productType, consumptions, expirations, weights, catalogCharacteristicService);
          if (!ok) {
            logger.warn(
                "VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration Voice Pack OfferID "
                    + o.getOfferID());
            return null;
          }
          break;

        case "sms":
          mh = subscriberProfile.getVoiceRevenueAmountMetricHistory(); // TODO
          // move
          // to
          // SMS
          ok = enrichInfos(mh, "voiceExpiration", null, productType, consumptions, expirations, weights, catalogCharacteristicService); // TODO
          if (!ok) {
            logger.warn(
                "VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration SMS Pack OfferID "
                    + o.getOfferID());
            return null;
          }
          break;

        case "dataAndVoice":
          mh = subscriberProfile.getDataRevenueAmountMetricHistory();
          ok = enrichInfos(mh, "dataExpiration", "Mixt bundle- Data Weight", productType, consumptions,
              expirations, weights, catalogCharacteristicService);
          if (!ok) {
            logger.warn(
                "VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration Mixt Bundle - Data & Voice DATA Pack OfferID "
                    + o.getOfferID());
            return null;
          }
          mh = subscriberProfile.getVoiceRevenueAmountMetricHistory();
          ok = enrichInfos(mh, "voiceExpiration", "Mixt bundle-Voice Weight", productType, consumptions,
              expirations, weights, catalogCharacteristicService);
          if (!ok) {
            logger.warn(
                "VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration Mixt Bundle - Data & Voice Voice Pack OfferID "
                    + o.getOfferID());
            return null;
          }
          break;

        case "dataAndSMS":
          mh = subscriberProfile.getDataRevenueAmountMetricHistory();
          ok = enrichInfos(mh, "dataExpiration", "Mixt bundle- Data Weight", productType, consumptions,
              expirations, weights, catalogCharacteristicService);
          if (!ok) {
            logger.warn(
                "VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration Mixt Bundle - Data & SMS DATA Pack OfferID "
                    + o.getOfferID());
            return null;
          }
          mh = subscriberProfile.getVoiceRevenueAmountMetricHistory(); // TODO
          // change
          // to
          // SMS
          ok = enrichInfos(mh, "SMSExpiration", "Mixt bundle- SMS Weight", productType, consumptions,
              expirations, weights, catalogCharacteristicService); // TODO
          if (!ok) {
            logger.warn(
                "VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration Mixt Bundle - Data & SMS SMS Pack OfferID "
                    + o.getOfferID());
            return null;
          }
          break;

        case "voiceAndSMS":
          mh = subscriberProfile.getVoiceRevenueAmountMetricHistory();
          ok = enrichInfos(mh, "voiceExpiration", "Mixt bundle-Voice Weight", productType, consumptions,
              expirations, weights, catalogCharacteristicService);
          if (!ok) {
            logger.warn(
                "VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration Mixt Bundle - Voice & SMS Voice Pack OfferID "
                    + o.getOfferID());
            return null;
          }
          mh = subscriberProfile.getVoiceRevenueAmountMetricHistory(); // TODO
          // move
          // to
          // SMS
          ok = enrichInfos(mh, "SMSExpiration", "Mixt bundle- SMS Weight", productType, consumptions,
              expirations, weights, catalogCharacteristicService);
          if (!ok) {
            logger.warn(
                "VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration Mixt Bundle - Voice & SMS SMS Pack OfferID "
                    + o.getOfferID());
            return null;
          }
          break;

        case "dataVoiceAndSMS":
          mh = subscriberProfile.getDataRevenueAmountMetricHistory();
          ok = enrichInfos(mh, "dataExpiration", "Mixt bundle- Data Weight", productType, consumptions,
              expirations, weights, catalogCharacteristicService);
          if (!ok) {
            logger.warn(
                "VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration Mixt Bundle - Data & Voice & SMS DATA Pack OfferID "
                    + o.getOfferID());
            return null;
          }
          mh = subscriberProfile.getVoiceRevenueAmountMetricHistory();
          ok = enrichInfos(mh, "voiceExpiration", "Mixt bundle-Voice Weight", productType, consumptions,
              expirations, weights, catalogCharacteristicService);
          if (!ok) {
            logger.warn(
                "VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration Mixt Bundle - Data & Voice & SMS Voice Pack OfferID "
                    + o.getOfferID());
            return null;
          }
          mh = subscriberProfile.getVoiceRevenueAmountMetricHistory(); // TODO
          // move
          // to
          // SMS
          ok = enrichInfos(mh, "SMSExpiration", "Mixt bundle- SMS Weight", productType, consumptions,
              expirations, weights, catalogCharacteristicService); // TODO
          if (!ok) {
            logger.warn(
                "VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration Mixt Bundle - Data & Voice & SMS SMS Pack OfferID "
                    + o.getOfferID());
            return null;
          }
          break;

        default:
          logger.warn("VeonPropensityAlgo2.computePxV Cound not understand product type "
              + pt.getProductTypeName());
          return null;
        }

        // now all data is retrieved, let compute the Value
        // v = offerprice - (((c1/90*d1)*w1 + (c2/90*d2)*w2) / (w1 +
        // w2))
        double weightSum = 0;
        double consumptionRatioSum = 0;
        for (int i = 0; i < consumptions.size(); i++) {
          weightSum = weightSum + weights.get(i);
          consumptionRatioSum = consumptionRatioSum
              + ((consumptions.get(i) / 90) * expirations.get(i) * weights.get(i));
          if (logger.isTraceEnabled()) {
            logger.trace("VeonPropensityAlgo.computeV ProductType=" + productType.getProductTypeID() + " INDEX=" + i + " weightSum=" + weightSum + " consumptionRatioSum=" + consumptionRatioSum);
          }
        }
        v = offerPrice - (consumptionRatioSum / weightSum);
        if (logger.isTraceEnabled()) {
          logger.trace("VeonPropensityAlgo.computeV ProductType=" + productType.getProductTypeID() + " Computed Value offerPrice - (consumptionRatioSum / weightSum) " 
              + offerPrice + " - (" + consumptionRatioSum + " / " + weightSum + ") = " + v);
        }

      }
    }
    return v;
  }

  private boolean enrichInfos(MetricHistory mh, String expirationCharacteristicName,
      String weightCharacteristicName /* may be null */, ProductTypeInstance productType,
      ArrayList<Double> consumptions, ArrayList<Double> expirations, ArrayList<Double> weights,
      CatalogCharacteristicService catalogCharacteristicService) {

    if (logger.isTraceEnabled()) {
      logger.trace("VeonPropensityAlgo.enrichInfos Called " + mh + " " + expirationCharacteristicName + " " + weightCharacteristicName + 
          productType + " " + consumptions + " " + expirations + " " + weights);
    }
    Date day = RLMDateUtils.truncate(Calendar.getInstance().getTime(), Calendar.DATE, Calendar.SUNDAY,
        Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, 90 * -1, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    Double consumption = new Double(mh.getValue(startDay, endDay));
    Double expiration = null;
    Double weight = null;
    // let try to extract the good characteristic...
    for (CatalogCharacteristicInstance pcc : productType.getCatalogCharacteristics()) {
      CatalogCharacteristic cc = catalogCharacteristicService.getActiveCatalogCharacteristic(pcc.getCatalogCharacteristicID(), new Date());
      if (cc.getCatalogCharacteristicName().equals(expirationCharacteristicName)) {
        // this is the good characteristic
        Object o = pcc.getValue();
        if (o instanceof Integer)
          expiration = Double.parseDouble(((Integer) pcc.getValue()).toString());
        else
          expiration = Double.parseDouble((String) pcc.getValue());
      }
      if (weightCharacteristicName != null
          && cc.getCatalogCharacteristicName().equals(weightCharacteristicName)) {
        // this is the good characteristic
        Object o = pcc.getValue();
        if (o instanceof Integer)
          weight = Double.parseDouble(((Integer) pcc.getValue()).toString());
        else
          weight = Double.parseDouble((String) pcc.getValue());
      } else {
        // no need of weight here so let set it to 1
        weight = 1d;
      }
    }
    if (consumption == null || expiration == null) {
      logger.warn("VeonPropensityAlgo2.computePxV Can't retrieve either consumption, either expiration "
          + consumption + " " + expiration);
      return false;
    } else {
      if (logger.isTraceEnabled()) {
        logger.trace("VeonPropensityAlgo.enrichInfos Add infos " + consumption + " " + expiration + " " + weight);
      }
      consumptions.add(consumption);
      expirations.add(expiration);
      weights.add(weight);
      return true;
    }
  }
}
