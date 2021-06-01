/*****************************************************************************
 *
 *  TokenUtils.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;
import com.evolving.nglm.evolution.offeroptimizer.DNBOMatrixAlgorithmParameters;
import com.evolving.nglm.evolution.offeroptimizer.GetOfferException;
import com.evolving.nglm.evolution.offeroptimizer.OfferOptimizerAlgoManager;
import com.evolving.nglm.evolution.offeroptimizer.ProposedOfferDetails;

public class TokenUtils
{
  private static final int HOW_MANY_TIMES_TO_TRY_TO_GENERATE_A_TOKEN_CODE = 100;

  private static Random random = new Random();
  private static final Logger log = LoggerFactory.getLogger(TokenUtils.class);

  /*****************************************
   *
   *    Finite-state machine State
   *
   *****************************************/

  private enum State
  {
    REGULAR_CHAR,
    REGULAR_CHAR_AFTER_BRACKET,
    LEFT_BRACKET,
    LEFT_BRACE,
    ERROR;
  }

  /*****************************************
   *
   *  generateFromRegex
   *
   *****************************************/
  /**
   * Generate a string that matches the regex passed in parameter.
   * Code supports simple patterns, such as :
   *   [list of chars]{numbers}      ( "{numbers}" can be omitted and defaults to 1)
   * There can be as many of these patterns as needed, mixed with regular chars that are not interpreted.
   * Example of valid regex :
   *   gl-[0123456789abcdef]{5}-[01234]
   *   [ACDEFGHJKMNPQRTWXY34679]{5}
   *   tc-[ 123456789][0123456789]{7}-tc
   * @param regex regex to generate from.
   * @return generated string, or an error message if regex has an invalid syntax.
   */
  public static String generateFromRegex(String regex)
  {
    StringBuilder output = new StringBuilder();
    StringBuilder chooseFrom = new StringBuilder();
    StringBuilder numOccurences = new StringBuilder();
    State currentState = State.REGULAR_CHAR;

    for (char c : regex.toCharArray())
      {
        switch (currentState)
        {

          case REGULAR_CHAR:
            if (c == '[')
              {
                currentState = State.LEFT_BRACKET;
              }
            else if (c == '{')
              {
                currentState = State.LEFT_BRACE;
              } 
            else
              {
                output.append(String.valueOf(c)); // Regular char goes to output string
              }
            break;

          case REGULAR_CHAR_AFTER_BRACKET:
            if (c == '{')
              {
                currentState = State.LEFT_BRACE;
              } 
            else
              {
                generateRandomString(chooseFrom, 1, output); // No braces after [...] means 1 occurrence
                chooseFrom = new StringBuilder();
                if (c == '[')
                  {
                    currentState = State.LEFT_BRACKET;
                  } 
                else
                  {
                    output.append(String.valueOf(c));
                    currentState = State.REGULAR_CHAR;
                  }
              }
            break;

          case LEFT_BRACKET:
            if (c == ']')
              {
                currentState = State.REGULAR_CHAR_AFTER_BRACKET;
              } 
            else if (c == '{')
              {
                output.append("-INVALID cannot have '{' inside brackets");
                currentState = State.ERROR;
              } 
            else
              {
                chooseFrom.append(String.valueOf(c));
              }
            break;

          case LEFT_BRACE:
            if (c == '}')
              {
                if (numOccurences.length() == 0)
                  { 
                    output.append("-INVALID cannot have '{}'");
                    currentState = State.ERROR;
                  } 
                else if (chooseFrom.length() == 0)
                  {
                    output.append("-INVALID cannot have []{n}");
                    currentState = State.ERROR;
                  }
                else
                  {
                    int numberOfOccurrences = Integer.parseInt(numOccurences.toString());
                    generateRandomString(chooseFrom, numberOfOccurrences, output);
                    chooseFrom = new StringBuilder();
                    numOccurences = new StringBuilder();
                    currentState = State.REGULAR_CHAR;
                  }
              } 
            else if (c == '[')
              {
                output.append("-INVALID cannot have '[' inside braces");
                currentState = State.ERROR;
              } 
            else
              {
                numOccurences.append(String.valueOf(c));
              }
            break;

          case ERROR:
            return output.toString();
        }
      }

    //
    // Check final state (after processing all input)
    //
    switch (currentState)
    {
      case REGULAR_CHAR_AFTER_BRACKET:
        generateRandomString(chooseFrom, 1, output); // No braces after [...] means 1 occurrence
        break;
      case LEFT_BRACKET:
      case LEFT_BRACE:
        output.append("-INVALID cannot end with pending '{' or '['");
        break;
      case REGULAR_CHAR:
      case ERROR:
        break;
    }

    return output.toString();
  }
  /*****************************************
   *
   *  generateRandomString
   *
   *****************************************/

  private static void generateRandomString(StringBuilder possibleValues, int length, StringBuilder result)
  {
    // System.out.println("possibleValues = "+possibleValues+" length = "+length);
    for (int i=0; i<length; i++)
      {
        result.append(chooseRandomOne(possibleValues.toString()));
      }
  }

  /*****************************************
   *
   *  chooseRandomOne
   *
   *****************************************/

  private static String chooseRandomOne(String values)
  {
    int index = random.nextInt(values.length());
    String result = values.substring(index, index+1);
    return result;
  }

  /*****************************************
   *
   *  isValidRegex
   *
   *****************************************/

  /**
   * Checks is the given regex is valid or not.
   * @param regex
   * @return true iif the regex has a valid syntax.
   */
  public static boolean isValidRegex(String regex)
  {
    boolean res = true;
    try
    {
      Pattern.compile(regex);
    } 
    catch (PatternSyntaxException ex)
    {
      res = false;
    }
    return res;
  }

  /*****************************************
   *
   *  generateTokenCode
   *
   *****************************************/
  
  public static DNBOToken generateTokenCode(SubscriberProfile subscriberProfile, TokenType tokenType)
  {
    List<String> currentTokens = subscriberProfile.getTokens().stream().map(token->token.getTokenCode()).collect(Collectors.toList());
    String tokenCode = null;
    boolean newTokenGenerated = false;
    String codeFormat = tokenType.getCodeFormat();
    for (int i=0; i<HOW_MANY_TIMES_TO_TRY_TO_GENERATE_A_TOKEN_CODE; i++)
    {
      tokenCode = generateFromRegex(codeFormat);
      if (!currentTokens.contains(tokenCode))
        {
          newTokenGenerated = true;
          break;
        }
    }
    if (!newTokenGenerated)
      {
        log.info("After " + HOW_MANY_TIMES_TO_TRY_TO_GENERATE_A_TOKEN_CODE + " tries, unable to generate a new token code with pattern " + codeFormat);
        return null;
      }
    log.info("TokenUtils.generateTokenCode : " + tokenCode);
    DNBOToken token = new DNBOToken(tokenCode, subscriberProfile.getSubscriberID(), tokenType);
    return token;
  }

  /*****************************************
   *
   *  getOffers
   *
   *****************************************/
  
  public static Collection<ProposedOfferDetails> getOffers(Date now, DNBOToken token, SubscriberEvaluationRequest evaluationRequest,
      SubscriberProfile subscriberProfile, PresentationStrategy presentationStrategy,
      ProductService productService, ProductTypeService productTypeService,
      VoucherService voucherService, VoucherTypeService voucherTypeService,
      CatalogCharacteristicService catalogCharacteristicService,
      ScoringStrategyService scoringStrategyService,
      ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader,
      SegmentationDimensionService segmentationDimensionService,
      DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameters, OfferService offerService, StringBuffer returnedLog,
      String msisdn, Supplier supplier) throws GetOfferException
  {
    // check if we can call this PS
    int maximumPresentationsPeriodDays = presentationStrategy.getMaximumPresentationsPeriodDays();
    Date earliestDateToKeep = RLMDateUtils.addDays(now, -maximumPresentationsPeriodDays, Deployment.getBaseTimeZone());
    List<Date> presentationDates = token.getPresentationDates();
    List<Date> newPresentationDates = new ArrayList<>();
    for (Date date : presentationDates)
      {
        if (date.after(earliestDateToKeep))
          {
            newPresentationDates.add(date);
          }
      }
    int nbPresentationSoFar = newPresentationDates.size();
    int nbPresentationMax = presentationStrategy.getMaximumPresentations();
    if (nbPresentationSoFar >= nbPresentationMax)
      {
        returnedLog.append("token has been presented " + nbPresentationSoFar + " times in the past " + maximumPresentationsPeriodDays + " days, no more presentation allowed (max : " + nbPresentationMax + " )");
        token.setPresentationDates(new ArrayList<>()); // indicates that bound has failed
        return new ArrayList<>();
      }
    newPresentationDates.add(now);
    token.setPresentationDates(newPresentationDates);
    
    Set<String> salesChannelIDs = presentationStrategy.getSalesChannelIDs();
    // TODO : which sales channel to take ?
    String salesChannelID = salesChannelIDs.iterator().next();
    PositionSet setA = presentationStrategy.getSetA();
    List<EvaluationCriterion> setAEligibility = setA.getEligibility();
    PositionSet setToUse;
    if (setAEligibility == null || setAEligibility.isEmpty())
      {
        setToUse = setA;
      }
    else
      {
        setToUse = presentationStrategy.getSetB(); // default one
        for (EvaluationCriterion criterion : setAEligibility)
          {
            if (criterion.evaluate(evaluationRequest))
              {
                setToUse = presentationStrategy.getSetA();
                break;
              }
          }
      }
    Map<String, Collection<ProposedOfferDetails>> scoringCache = new HashMap<>(); // indexed by scoringStrategyID
    List<ProposedOfferDetails> res = new ArrayList<>();
    int indexResult = 0;
    for (int positionIndex=0; positionIndex < setToUse.getPositions().size(); positionIndex++)
      {
        PositionElement position = setToUse.getPositions().get(positionIndex);
        if (position.getAdditionalCriteria() != null && !position.getAdditionalCriteria().isEmpty())
        {
          boolean valid = false;
          for (EvaluationCriterion criterion : position.getAdditionalCriteria())
            {
              if (criterion.evaluate(evaluationRequest))
                {
                  valid = true;
                  break;
                }
            }
          if (!valid)
            {
              log.trace("For positionIndex " + (positionIndex+1) + " skip element because criteria not true");
              continue; // skip this position in the result            
            }
        }
        String scoringStrategyID = position.getScoringStrategyID();
        ScoringStrategy scoringStrategy = scoringStrategyService.getActiveScoringStrategy(scoringStrategyID, now);
        if (scoringStrategy == null)
          {
            log.warn("For positionIndex " + (positionIndex+1) + " invalid scoring strategy " + scoringStrategyID);
            continue; // skip this position in the result
          }
        Collection<ProposedOfferDetails> localScoring = scoringCache.get(scoringStrategyID);
        if (localScoring == null) // cache miss
          {
            localScoring = getOffersWithScoringStrategy(now, salesChannelID, subscriberProfile, scoringStrategy, productService, productTypeService, voucherService, voucherTypeService, catalogCharacteristicService, subscriberGroupEpochReader, segmentationDimensionService, dnboMatrixAlgorithmParameters, offerService, returnedLog, msisdn, supplier);
            scoringCache.put(scoringStrategyID, localScoring);
          }
        if (localScoring.size() < indexResult+1)
          {
            log.warn("For positionIndex " + (positionIndex+1) + " result does not have enough elements : " + localScoring.size());
            continue; // skip this position in the result            
          }
        res.add(indexResult, localScoring.toArray(new ProposedOfferDetails[0])[positionIndex]);
        indexResult++;
      }
    log.trace("Finished scoring, got " + indexResult + " elements, max possible " + setToUse.getPositions().size());
    return res;
  }
  
  public static Collection<ProposedOfferDetails> getOffersWithScoringStrategy(Date now, String salesChannelID,
    SubscriberProfile subscriberProfile, ScoringStrategy scoringStrategy,
    ProductService productService, ProductTypeService productTypeService,
    VoucherService voucherService, VoucherTypeService voucherTypeService,
    CatalogCharacteristicService catalogCharacteristicService,
    ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader,
    SegmentationDimensionService segmentationDimensionService, DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameters,
    OfferService offerService, StringBuffer returnedLog, String msisdn, Supplier supplier) throws GetOfferException
  {
    String logFragment;
    ScoringSegment selectedScoringSegment = getScoringSegment(scoringStrategy, subscriberProfile, subscriberGroupEpochReader);
    logFragment = "getOffers " + scoringStrategy.getScoringStrategyID() + " Selected ScoringSegment for " + msisdn + " " + selectedScoringSegment;
    returnedLog.append(logFragment+", ");
    if (log.isDebugEnabled())
    {
      log.debug(logFragment);
    }

    Set<Offer> offersForAlgo = getOffersToOptimize(now, selectedScoringSegment.getOfferObjectiveIDs(), subscriberProfile, offerService, subscriberGroupEpochReader, supplier, productService, voucherService);

    OfferOptimizationAlgorithm algo = selectedScoringSegment.getOfferOptimizationAlgorithm();
    if (algo == null)
    {
      logFragment = "getOffers No Algo returned for selectedScoringSplit " + scoringStrategy.getScoringStrategyID();
      returnedLog.append(logFragment+", ");
      log.warn(logFragment);
      return null;
    }

    OfferOptimizationAlgorithmParameter thresholdParameter = new OfferOptimizationAlgorithmParameter("thresholdValue");

    Map<OfferOptimizationAlgorithmParameter, String> algoParameters = selectedScoringSegment.getParameters(); 

    double threshold = 0;
    String thresholdString = selectedScoringSegment.getParameters().get(thresholdParameter);
    if (thresholdString != null)
    {
      threshold = Double.parseDouble(thresholdString);
    }
    logFragment = "DNBOProxy.getOffers Threshold value " + threshold;
    returnedLog.append(logFragment+", ");
    if (log.isDebugEnabled())
    {
      log.debug(logFragment);
    }
    
    // This returns an ordered Collection (and sorted by offerScore)
    Collection<ProposedOfferDetails> offerAvailabilityFromPropensityAlgo =
        OfferOptimizerAlgoManager.getInstance().applyScoreAndSort(
            algo, algoParameters, offersForAlgo, subscriberProfile, threshold, salesChannelID,
            productService, productTypeService, voucherService, voucherTypeService, catalogCharacteristicService,
            subscriberGroupEpochReader,
            segmentationDimensionService, dnboMatrixAlgorithmParameters, returnedLog);

    if (offerAvailabilityFromPropensityAlgo == null)
      {
        offerAvailabilityFromPropensityAlgo = new ArrayList<>();
      }

    //
    // Now add some predefined offers based on alwaysAppendOfferObjectiveIDs of ScoringSplit
    //
    Set<String> offerObjectiveIds = selectedScoringSegment.getAlwaysAppendOfferObjectiveIDs();
    for(Offer offer : offerService.getActiveOffers(now))
      {
        boolean inList = true;
        for (OfferObjectiveInstance offerObjective : offer.getOfferObjectives()) 
          {
            if (!offerObjectiveIds.contains(offerObjective.getOfferObjectiveID())) 
              {
                inList = false;
                break;
              }
          }
        if (!inList) 
          {
            //
            // not a single objective of this offer is in the list of the scoringSegment -> skip it
            //
            continue;
          }
        for (OfferSalesChannelsAndPrice salesChannelAndPrice : offer.getOfferSalesChannelsAndPrices())
          {
            for (String loopSalesChannelID : salesChannelAndPrice.getSalesChannelIDs()) 
              {
                if ((salesChannelID == null) || (loopSalesChannelID.equals(salesChannelID)))
                 {
                    String offerId = offer.getOfferID();
                    boolean offerIsAlreadyInList = false;
                    for (ProposedOfferDetails offerAvail : offerAvailabilityFromPropensityAlgo)
                      {
                        if (offerAvail.getOfferId().equals(offerId))
                          {
                            offerIsAlreadyInList = true;
                            if (log.isTraceEnabled()) log.trace("DNBOProxy.getOffers offer "+offerId+" already in list, skip it");
                            break;
                          }
                      }
                    if (!offerIsAlreadyInList)
                      {
                        if (log.isTraceEnabled()) log.trace("DNBOProxy.getOffers offer "+offerId+" added to the list because its objective is in alwaysAppendOfferObjectiveIDs of ScoringStrategy");
                        ProposedOfferDetails additionalDetails = new ProposedOfferDetails(offerId, salesChannelID, 0);
                        offerAvailabilityFromPropensityAlgo.add(additionalDetails);
                      }
                  }
              }
          }
      }

    if (offerAvailabilityFromPropensityAlgo.isEmpty())
      {
        log.warn("DNBOProxy.getOffers Return empty list of offers");
      }
    
    int index = 1;
    for (ProposedOfferDetails current : offerAvailabilityFromPropensityAlgo)
    {
      current.setOfferRank(index);
      index++;
    }
    return offerAvailabilityFromPropensityAlgo;
  }
  
  private static Set<Offer> getOffersToOptimize(Date now, Set<String> catalogObjectiveIDs,
      SubscriberProfile subscriberProfile, OfferService offerService, ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader, Supplier supplier, ProductService productService, VoucherService voucherService)
  {
    // Browse all offers:
    // - filter by offer objective coming from the split strategy
    // - filter by profile of subscriber
    // Return a set of offers that can be optimised
    Collection<Offer> offers = offerService.getActiveOffers(SystemTime.getCurrentTime());
    Set<Offer> result = new HashSet<>();
    List<Token> tokens = subscriberProfile.getTokens();
    Collection<Offer>filteredOffers = new ArrayList<>(); 
    /**
     * 
     * filter offer based on the supplier
     * 
     */
    if (supplier != null)
      {
        for (Offer offer : offers)
          {
            Set<OfferProduct> offerProducts = offer.getOfferProducts();
            Set<OfferVoucher> offerVouchers = offer.getOfferVouchers();

            if (offerProducts != null && offerProducts.size() != 0)
              {
                for (OfferProduct offerproduct : offerProducts)
                  {
                    String productID = offerproduct.getProductID();
                    GUIManagedObject productObject = productService.getStoredProduct(productID);
                    if (productObject != null && productObject instanceof Product)
                      {
                        Product product = (Product) productObject;
                        if (product.getSupplierID().equals(supplier.getSupplierID()))
                          {
                            filteredOffers.add(offer);
                            break;
                          }
                      }
                    else {
                      if(log.isDebugEnabled())
                        log.debug(productObject + "is not a complete product");
                    }
                  }
              }
            if (offerVouchers != null && offerVouchers.size() != 0)
              {
                for (OfferVoucher offerVoucher : offerVouchers)
                  {
                    String voucherID = offerVoucher.getVoucherID();
                    GUIManagedObject voucherObject = voucherService.getStoredVoucher(voucherID);
                    if (voucherObject != null && voucherObject instanceof Voucher)
                      {
                        Voucher voucher = (Voucher) voucherObject;
                        if (voucher.getSupplierID().equals(supplier.getSupplierID()))
                          {
                            filteredOffers.add(offer);
                            break;
                          }
                      }

                    else
                      {
                        if (log.isDebugEnabled())
                          log.debug(voucherObject + "is not a complete voucher");
                      }
                  }
              }
          }
      }
    else {
      filteredOffers = offers;
    }
    for (Offer offer : filteredOffers)
    {
      boolean nextOffer = false;
      
      Long maximumPresentationsStr = (Long) offer.getJSONRepresentation().get("maximumPresentations");
      long maximumPresentations = maximumPresentationsStr != null ? maximumPresentationsStr : Long.MAX_VALUE;  // default value

      Long maximumPresentationsPeriodDaysStr = (Long) offer.getJSONRepresentation().get("maximumPresentationsPeriodDays");
      long maximumPresentationsPeriodDays = maximumPresentationsPeriodDaysStr != null ? maximumPresentationsPeriodDaysStr : 365L;  // default value
      
      Date earliestDateToKeep = RLMDateUtils.addDays(now, -((int)maximumPresentationsPeriodDays), Deployment.getBaseTimeZone());

      for (String catalogObjectiveID : catalogObjectiveIDs)
      {
        log.trace("catalogObjectiveID : "+catalogObjectiveID);
        for (OfferObjectiveInstance offerObjective : offer.getOfferObjectives())
        {
          log.trace("    offerID : "+offer.getOfferID()+" offerObjectiveID : "+offerObjective.getOfferObjectiveID());
          if (offerObjective.getOfferObjectiveID().equals(catalogObjectiveID))
          {
            SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, now);
            if (offer.evaluateProfileCriteria(evaluationRequest))
            {
              // check if we can still present this offer
              long nbPresentations = 0;
              for (Token token : tokens)
                {
                  if (token instanceof DNBOToken)  {
                      DNBOToken dnboToken = (DNBOToken) token;
                      List<Presentation> presentationHistory = dnboToken.getPresentationHistory();
                      log.info("We got " + presentationHistory.size() + " presentations of offer " + offer.getOfferID() + " for " + token.getTokenCode());
                      for (Presentation presentation : presentationHistory) {
                        if (presentation.getOfferIDs().contains(offer.getOfferID()) && presentation.getDate().after(earliestDateToKeep)) {
                          log.info("Got a presentation for offer " + offer.getOfferID() + " in " + token.getTokenCode() + " on " + presentation.getDate());
                          nbPresentations++;
                        }
                      }
                    }
                }
              log.debug("offer " + offer.getOfferID() + " presented " + nbPresentations + " compared to max " + maximumPresentations);
              if (nbPresentations < maximumPresentations)
                {
                  log.trace("add offer : "+offer.getOfferID());
                  result.add(offer);
                  nextOffer = true; // do not consider this offer again
                  break;
                }
              else
                {
                  log.debug("offer " + offer.getOfferID() + " has been presented " + nbPresentations + " times in the past " + maximumPresentationsPeriodDays + " days, skip it (max : " + maximumPresentations + " )");
                }
            }
          }
        }
        if (nextOffer) break;
      }
    }
    return result;
  }
  
  private static ScoringSegment getScoringSegment(ScoringStrategy strategy, SubscriberProfile subscriberProfile,
      ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader) throws GetOfferException
  {
    // let retrieve the first sub strategy that maps this user:
    Date now = SystemTime.getCurrentTime();
    SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, now);
    ScoringSegment selectedScoringSegment = strategy.evaluateScoringSegments(evaluationRequest);
    if (log.isDebugEnabled())
      {
        log.debug("DNBOProxy.getScoringSegment Retrieved matching scoringSegment " + (selectedScoringSegment != null ? selectedScoringSegment : null));
      }

    if (selectedScoringSegment == null)
      {
        throw new GetOfferException("Can't retrieve ScoringSegment for strategy " + strategy.getScoringStrategyID() + " and msisdn " + subscriberProfile.getSubscriberID());
      }
    return selectedScoringSegment;
  }
  

  /*****************************************
   *
   *  main
   *
   *****************************************/

  public static void main(String[] args)
  {
    String[] testStrings = new String[]
        {
            // Token formats from E4O
            "br-[0123456789abcdef]{5}",
            "sl-[0123456789abcdef]{5}",
            "gl-[0123456789abcdef]{5}",
            "[ACDEFGHJKMNPQRTWXY34679]{5}",
            "[ACDEFGHJKMNPQRTWXY34679]{6}",
            "[ACDEFGHJKMNPQRTWXY34679]{7}",

            // Test values
            "gl-[0123456789abcdef]{5}-[01234]",
            "0x[ABCDEF0123456789]{4}-0x[ABCDEF0123456789]{2}--0x[ABCDEF0123456789]{8}-0x[ABCDEF0123456789]{2}",
            "tc-[ 123456789][0123456789]{7}-tc",
            "A-[1]{3}-B",
            "A-[1",
            "A-{",
            "A-{[[[[[",
            "A-[123]{a[",
            "I am [ 1][123456789][0123456789] years old"
        };
    for (String regex : testStrings)
      {
        if (isValidRegex(regex))
          {
            try
            {
              String result = TokenUtils.generateFromRegex(regex);
              System.out.println("Generated \""+result+"\" from regex \""+regex+"\"");
              //
              // Check that the generated string matches the regex
              //
              Pattern pattern = Pattern.compile("^"+regex+"$");
              Matcher matcher = pattern.matcher(result);
              if (!matcher.find())
                {
                  System.out.println("PROBLEM : should match");
                }
            }
            catch (PatternSyntaxException ex)
            {
              System.out.println("PROBLEM : unexpected issue with regex "+regex);
            }
          }
        else
          {
            System.out.println("We should get an issue generating from regex "+regex);
            String result = TokenUtils.generateFromRegex(regex);
            System.out.println("    ----> Generated \""+result+"\" from regex \""+regex+"\"");
          }
      }
    System.out.println("All tests finished");
  }
}
