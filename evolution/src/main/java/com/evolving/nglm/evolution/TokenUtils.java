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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DNBOProxy.DNBOProxyException;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;
import com.evolving.nglm.evolution.offeroptimizer.DNBOMatrixAlgorithmParameters;
import com.evolving.nglm.evolution.offeroptimizer.GetOfferException;
import com.evolving.nglm.evolution.offeroptimizer.OfferOptimizerAlgoManager;
import com.evolving.nglm.evolution.offeroptimizer.ProposedOfferDetails;

public class TokenUtils
{

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

  
  public static Collection<ProposedOfferDetails> getOffers(Date now, String salesChannelID,
      SubscriberProfile subscriberProfile, ScoringStrategy scoringStrategy, ProductService productService,
      ProductTypeService productTypeService, CatalogCharacteristicService catalogCharacteristicService,
      ReferenceDataReader<PropensityKey, PropensityState> propensityDataReader,
      ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader,
      SegmentationDimensionService segmentationDimensionService, DNBOMatrixAlgorithmParameters dnboMatrixAlgorithmParameters, OfferService offerService, StringBuffer returnedLog,
      String msisdn) throws GetOfferException
  {
    String logFragment;
    ScoringSegment selectedScoringSegment = getScoringSegment(scoringStrategy, subscriberProfile, subscriberGroupEpochReader);
    logFragment = "getOffers " + scoringStrategy.getScoringStrategyID() + " Selected ScoringSegment for " + msisdn + " " + selectedScoringSegment;
    returnedLog.append(logFragment+", ");
    if (log.isDebugEnabled())
    {
      log.debug(logFragment);
    }

    Set<Offer> offersForAlgo = getOffersToOptimize(selectedScoringSegment.getOfferObjectiveIDs(), subscriberProfile, offerService, subscriberGroupEpochReader);

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
            productService, productTypeService, catalogCharacteristicService,
            propensityDataReader, subscriberGroupEpochReader,
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
                if (loopSalesChannelID.equals(salesChannelID)) 
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
  
  private static Set<Offer> getOffersToOptimize(Set<String> catalogObjectiveIDs,
      SubscriberProfile subscriberProfile, OfferService offerService, ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    // Browse all offers:
    // - filter by offer objective coming from the split strategy
    // - filter by profile of subscriber
    // Return a set of offers that can be optimised
    Collection<Offer> offers = offerService.getActiveOffers(Calendar.getInstance().getTime());
    Set<Offer> result = new HashSet<>();
    for (String currentSplitObjectiveID : catalogObjectiveIDs)
    {
      log.trace("currentSplitObjectiveID : "+currentSplitObjectiveID);
      for (Offer currentOffer : offers)
      {
        for (OfferObjectiveInstance currentOfferObjective : currentOffer.getOfferObjectives())
        {
          log.trace("    offerID : "+currentOffer.getOfferID()+" offerObjectiveID : "+currentOfferObjective.getOfferObjectiveID());
          if (currentOfferObjective.getOfferObjectiveID().equals(currentSplitObjectiveID))
          {
            // this offer is a good candidate for the moment, let's check the profile
            SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime());
            if (currentOffer.evaluateProfileCriteria(evaluationRequest))
            {
              log.trace("        add offer : "+currentOffer.getOfferID());
              result.add(currentOffer);
            }
          }
        }
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
