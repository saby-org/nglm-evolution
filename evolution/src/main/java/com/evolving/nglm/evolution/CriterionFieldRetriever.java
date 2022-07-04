/****************************************************************************
*
*  CriterionFieldRetriever.java
*
****************************************************************************/

package com.evolving.nglm.evolution;


import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.LoyaltyProgramHistory.TierHistory;
import com.evolving.nglm.evolution.LoyaltyProgramMission.MissionStep;
import com.evolving.nglm.evolution.LoyaltyProgramMissionHistory.StepHistory;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;
import com.evolving.nglm.evolution.SubscriberPredictions.Prediction;
import com.evolving.nglm.evolution.complexobjects.ComplexObjectException;
import com.evolving.nglm.evolution.complexobjects.ComplexObjectInstance;
import com.evolving.nglm.evolution.complexobjects.ComplexObjectUtils;
import com.evolving.nglm.evolution.datamodel.DataModelFieldValue;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.Badge.BadgeType;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.LoyaltyProgramChallengeHistory.LevelHistory;

public abstract class CriterionFieldRetriever
{
  
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CriterionFieldRetriever.class);
  
  /*****************************************
  *
  *  adv criteria - with sub criteria
  *
  *****************************************/
  
  public static Object getNumberOfOfferPurchasedForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    long result = 0;
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    String offerID = (String) subcriteriaVal.get(0);
    String period = (String) subcriteriaVal.get(1);
    List<Date> purchaseDates = new ArrayList<Date>();
    
    //
    //  offerID
    //
    
    if (offerID != null)
      {
        if (subscriberProfile.getOfferPurchaseHistory().get(offerID) != null) purchaseDates.addAll(subscriberProfile.getOfferPurchaseHistory().get(offerID));
        if (subscriberProfile.getOfferPurchaseSalesChannelHistory().get(offerID) != null) purchaseDates.addAll(subscriberProfile.getOfferPurchaseSalesChannelHistory().get(offerID).stream().map(pair -> pair.getSecondElement()).collect(Collectors.toList()));
      }
    else
      {
        purchaseDates = subscriberProfile.getOfferPurchaseHistory().values().stream().flatMap(vl -> vl.stream()).collect(Collectors.toList());
        for (List<Pair<String, Date>> pairList : subscriberProfile.getOfferPurchaseSalesChannelHistory().values())
          {
            purchaseDates.addAll(pairList.stream().map(pair -> pair.getSecondElement()).collect(Collectors.toList()));
          }
      }
    
    //
    //  period
    //
    
    if (period != null)
      {
        String timeZone = Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone();
        Pair<Date, Date> startEndDatePair = getStartAndEndDate(period, timeZone);
        Date startDate = startEndDatePair.getFirstElement();
        Date endDate = startEndDatePair.getSecondElement();
        result = purchaseDates.stream().filter(purchaseDate -> purchaseDate.after(startDate) && purchaseDate.before(endDate)).count();
      }
    else
      {
        result = purchaseDates.size();
      }
    return result;
  }
  
  public static Object getNumberOfOfferPurchasedFromSalesChnlForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    long result = 0;
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    String salesChannelID = (String) subcriteriaVal.get(0);
    String period = (String) subcriteriaVal.get(1);
    List<Date> purchaseDates = new ArrayList<Date>();
    
    //
    //  salesChannelID
    //
    
    if (salesChannelID != null)
      {
        for (List<Pair<String, Date>> pairList : subscriberProfile.getOfferPurchaseSalesChannelHistory().values())
          {
            purchaseDates.addAll(pairList.stream().filter(element -> element.getFirstElement().equals(salesChannelID)).map(pair -> pair.getSecondElement()).collect(Collectors.toList()));
          }
      }
    else
      {
        for (List<Pair<String, Date>> pairList : subscriberProfile.getOfferPurchaseSalesChannelHistory().values())
          {
            purchaseDates.addAll(pairList.stream().map(pair -> pair.getSecondElement()).collect(Collectors.toList()));
          }
      }
    
    //
    //  period
    //
    
    if (period != null)
      {
        String timeZone = Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone();
        Pair<Date, Date> startEndDatePair = getStartAndEndDate(period, timeZone);
        Date startDate = startEndDatePair.getFirstElement();
        Date endDate = startEndDatePair.getSecondElement();
        result = purchaseDates.stream().filter(purchaseDate -> purchaseDate.after(startDate) && purchaseDate.before(endDate)).count();
      }
    else
      {
        result = purchaseDates.size();
      }
    return result;
  }
  
  public static Object getNumberOfVoucherDeliveredForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    int result = 0;
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    List<VoucherProfileStored> vouchers = subscriberProfile.getVouchers();
    
    //
    //  args
    //
    
    String voucherID = (String) subcriteriaVal.get(0);
    String period = (String) subcriteriaVal.get(1);
    String timeZone = Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone();
    Pair<Date, Date> startEndDatePair = getStartAndEndDate(period, timeZone);
    Date startDate = startEndDatePair.getFirstElement();
    Date endDate = startEndDatePair.getSecondElement();
    
    if (vouchers != null && !vouchers.isEmpty())
      {
        if (voucherID != null) vouchers = vouchers.stream().filter(voucher -> voucher.getVoucherID().equals(voucherID)).collect(Collectors.toList());
        vouchers = vouchers.stream().filter(voucher -> voucher.getVoucherDeliveryDate() != null && voucher.getVoucherDeliveryDate().after(startDate) && voucher.getVoucherDeliveryDate().before(endDate)).collect(Collectors.toList());
        result = vouchers.size();
      }
    
    return result;
  }
  
  public static Object getNumberOfVoucherRedeemedForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    int result = 0;
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    List<VoucherProfileStored> vouchers = subscriberProfile.getVouchers();
    
    //
    //  args
    //
    
    String voucherID = (String) subcriteriaVal.get(0);
    String period = (String) subcriteriaVal.get(1);
    String timeZone = Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone();
    Pair<Date, Date> startEndDatePair = getStartAndEndDate(period, timeZone);
    Date startDate = startEndDatePair.getFirstElement();
    Date endDate = startEndDatePair.getSecondElement();
    
    if (vouchers != null && !vouchers.isEmpty())
      {
        if (voucherID != null) vouchers = vouchers.stream().filter(voucher -> voucher.getVoucherID().equals(voucherID)).collect(Collectors.toList());
        vouchers = vouchers.stream().filter(voucher -> voucher.getVoucherRedeemDate() != null && voucher.getVoucherRedeemDate().after(startDate) && voucher.getVoucherRedeemDate().before(endDate)).collect(Collectors.toList());
        result = vouchers.size();
      }
    
    return result;
  }
  
  public static Object getNumberOfVoucherExpiredForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    int result = 0;
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    List<VoucherProfileStored> vouchers = subscriberProfile.getVouchers();
    
    //
    //  args
    //
    
    String voucherID = (String) subcriteriaVal.get(0);
    String period = (String) subcriteriaVal.get(1);
    String timeZone = Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone();
    Pair<Date, Date> startEndDatePair = getStartAndEndDate(period, timeZone);
    Date startDate = startEndDatePair.getFirstElement();
    Date endDate = startEndDatePair.getSecondElement();
    
    if (vouchers != null && !vouchers.isEmpty())
      {
        if (voucherID != null) vouchers = vouchers.stream().filter(voucher -> voucher.getVoucherID().equals(voucherID)).collect(Collectors.toList());
        vouchers = vouchers.stream().filter(voucher -> voucher.getVoucherExpiryDate() != null && voucher.getVoucherExpiryDate().after(startDate) && voucher.getVoucherExpiryDate().before(endDate)).collect(Collectors.toList());
        result = vouchers.size();
      }
    
    return result;
  }
  
  public static Object getNumberOfBadgesAwardedForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    int result = 0;
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    List<BadgeState> badges = subscriberProfile.getBadges();
    
    //
    //  args
    //
    
    String badgeID = (String) subcriteriaVal.get(0);
    String period = (String) subcriteriaVal.get(1);
    String timeZone = Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone();
    Pair<Date, Date> startEndDatePair = getStartAndEndDate(period, timeZone);
    Date startDate = startEndDatePair.getFirstElement();
    Date endDate = startEndDatePair.getSecondElement();
    
    if (badges != null && !badges.isEmpty())
      {
        if (badgeID != null) badges = badges.stream().filter(badge -> badge.getBadgeID().equals(badgeID)).collect(Collectors.toList());
        badges = badges.stream().filter(badge -> badge.getBadgeAwardDate() != null && badge.getBadgeAwardDate().after(startDate) && badge.getBadgeAwardDate().before(endDate)).collect(Collectors.toList());
        result = badges.size();
      }
    if (log.isTraceEnabled()) log.trace("BadgeCriteria getNumberOfBadgesAwardedForPeriod subcriteriaVal {}, startEndDatePair {}, result {}", subcriteriaVal, startEndDatePair, result);
    return result;
  }
  
  public static Object getNumberOfBadgeTypesAwardedForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    int result = 0;
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    List<BadgeState> badges = subscriberProfile.getBadges();
    
    //
    //  args
    //
    
    String badgeTypeStr = (String) subcriteriaVal.get(0);
    String period = (String) subcriteriaVal.get(1);
    String timeZone = Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone();
    Pair<Date, Date> startEndDatePair = getStartAndEndDate(period, timeZone);
    Date startDate = startEndDatePair.getFirstElement();
    Date endDate = startEndDatePair.getSecondElement();
    
    if (badges != null && !badges.isEmpty())
      {
        if (badgeTypeStr != null) badges = badges.stream().filter(badge -> badge.getBadgeType().equals(BadgeType.fromExternalRepresentation(badgeTypeStr))).collect(Collectors.toList());
        badges = badges.stream().filter(badge -> badge.getBadgeAwardDate() != null && badge.getBadgeAwardDate().after(startDate) && badge.getBadgeAwardDate().before(endDate)).collect(Collectors.toList());
        result = badges.size();
      }
    if (log.isTraceEnabled()) log.trace("BadgeCriteria getNumberOfBadgeTypesAwardedForPeriod subcriteriaVal {}, startEndDatePair {}, result {}", subcriteriaVal, startEndDatePair, result);
    return result;
  }
  
  public static Object getNumberOfBadgeObjectivesAwardedForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    int result = 0;
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    List<BadgeState> badges = subscriberProfile.getBadges();
    
    //
    //  args
    //
    
    String badgeObjID = (String) subcriteriaVal.get(0);
    String period = (String) subcriteriaVal.get(1);
    String timeZone = Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone();
    Pair<Date, Date> startEndDatePair = getStartAndEndDate(period, timeZone);
    Date startDate = startEndDatePair.getFirstElement();
    Date endDate = startEndDatePair.getSecondElement();
    
    if (badges != null && !badges.isEmpty())
      {
        if (badgeObjID != null) badges = badges.stream().filter(badge -> badge.getBadgeObjectiveIDs().contains(badgeObjID)).collect(Collectors.toList());
        badges = badges.stream().filter(badge -> badge.getBadgeAwardDate() != null && badge.getBadgeAwardDate().after(startDate) && badge.getBadgeAwardDate().before(endDate)).collect(Collectors.toList());
        result = badges.size();
      }
    if (log.isTraceEnabled()) log.trace("BadgeCriteria getNumberOfBadgeObjectivesAwardedForPeriod subcriteriaVal {}, startEndDatePair {}, result {}", subcriteriaVal, startEndDatePair, result);
    return result;
  }
  
  public static Object getNumberOfBadgesRemovedForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    int result = 0;
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    List<BadgeState> badges = subscriberProfile.getBadges();
    
    //
    //  args
    //
    
    String badgeID = (String) subcriteriaVal.get(0);
    String period = (String) subcriteriaVal.get(1);
    String timeZone = Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone();
    Pair<Date, Date> startEndDatePair = getStartAndEndDate(period, timeZone);
    Date startDate = startEndDatePair.getFirstElement();
    Date endDate = startEndDatePair.getSecondElement();
    
    if (badges != null && !badges.isEmpty())
      {
        if (badgeID != null) badges = badges.stream().filter(badge -> badge.getBadgeID().equals(badgeID)).collect(Collectors.toList());
        badges = badges.stream().filter(badge -> badge.getBadgeRemoveDate() != null && badge.getBadgeRemoveDate().after(startDate) && badge.getBadgeRemoveDate().before(endDate)).collect(Collectors.toList());
        result = badges.size();
      }
    if (log.isTraceEnabled()) log.trace("BadgeCriteria getNumberOfBadgesRemovedForPeriod subcriteriaVal {}, startEndDatePair {}, result {}", subcriteriaVal, startEndDatePair, result);
    return result;
  }
  
  public static Object getNumberOfBadgeTypesRemovedForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    int result = 0;
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    List<BadgeState> badges = subscriberProfile.getBadges();
    
    //
    //  args
    //
    
    String badgeTypeStr = (String) subcriteriaVal.get(0);
    String period = (String) subcriteriaVal.get(1);
    String timeZone = Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone();
    Pair<Date, Date> startEndDatePair = getStartAndEndDate(period, timeZone);
    Date startDate = startEndDatePair.getFirstElement();
    Date endDate = startEndDatePair.getSecondElement();
    
    if (badges != null && !badges.isEmpty())
      {
        if (badgeTypeStr != null) badges = badges.stream().filter(badge -> badge.getBadgeType().equals(BadgeType.fromExternalRepresentation(badgeTypeStr))).collect(Collectors.toList());
        badges = badges.stream().filter(badge -> badge.getBadgeRemoveDate() != null && badge.getBadgeRemoveDate().after(startDate) && badge.getBadgeRemoveDate().before(endDate)).collect(Collectors.toList());
        result = badges.size();
      }
    if (log.isTraceEnabled()) log.trace("BadgeCriteria getNumberOfBadgeTypesRemovedForPeriod subcriteriaVal {}, startEndDatePair {}, result {}", subcriteriaVal, startEndDatePair, result);
    return result;
  }
  
  public static Object getNumberOfBadgeObjectivesRemovedForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    int result = 0;
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    List<BadgeState> badges = subscriberProfile.getBadges();
    
    //
    //  args
    //
    
    String badgeObjID = (String) subcriteriaVal.get(0);
    String period = (String) subcriteriaVal.get(1);
    String timeZone = Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone();
    Pair<Date, Date> startEndDatePair = getStartAndEndDate(period, timeZone);
    Date startDate = startEndDatePair.getFirstElement();
    Date endDate = startEndDatePair.getSecondElement();
    
    if (badges != null && !badges.isEmpty())
      {
        if (badgeObjID != null) badges = badges.stream().filter(badge -> badge.getBadgeObjectiveIDs().contains(badgeObjID)).collect(Collectors.toList());
        badges = badges.stream().filter(badge -> badge.getBadgeRemoveDate() != null && badge.getBadgeRemoveDate().after(startDate) && badge.getBadgeRemoveDate().before(endDate)).collect(Collectors.toList());
        result = badges.size();
      }
    if (log.isTraceEnabled()) log.trace("BadgeCriteria getNumberOfBadgesRemovedForPeriod subcriteriaVal {}, startEndDatePair {}, result {}", subcriteriaVal, startEndDatePair, result);
    return result;
  }
  
  public static Object getBadgeBalanceForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    int result = (int) getNumberOfBadgesAwardedForPeriod(evaluationRequest, fieldName, subcriteriaVal) - (int) getNumberOfBadgesRemovedForPeriod(evaluationRequest, fieldName, subcriteriaVal);
    if (result < 0) log.error("bad badge balance value {}, should not be negative and must be checked for subcriteria {}", result, subcriteriaVal);
    return result;
  }
  
  public static Object getBadgeTypeBalanceForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    int result = (int) getNumberOfBadgeTypesAwardedForPeriod(evaluationRequest, fieldName, subcriteriaVal) - (int) getNumberOfBadgeTypesRemovedForPeriod(evaluationRequest, fieldName, subcriteriaVal);
    if (result < 0) log.error("bad badge type balance value {}, should not be negative and must be checked for subcriteria {}", result, subcriteriaVal);
    return result;
  }
  
  public static Object getBadgeObjectiveBalanceForPeriod(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    int result = (int) getNumberOfBadgeObjectivesAwardedForPeriod(evaluationRequest, fieldName, subcriteriaVal) - (int) getNumberOfBadgeObjectivesRemovedForPeriod(evaluationRequest, fieldName, subcriteriaVal);
    if (result < 0) log.error("bad badge objective balance value {}, should not be negative and must be checked for subcriteria {}", result, subcriteriaVal);
    return result;
  }
  
  protected static Pair<Date, Date> getStartAndEndDate(String period, String timeZone)
  {
    Date now = SystemTime.getCurrentTime();
    Date startDate = now, endDate = now;
    switch (period)
    {
      case "today":
        startDate = RLMDateUtils.truncate(now, Calendar.DATE, timeZone);
        break;
        
      case "yesterday":
        startDate = RLMDateUtils.addDays(startDate, -1, timeZone);
        endDate = RLMDateUtils.ceiling(startDate, Calendar.DATE, timeZone);
        startDate = RLMDateUtils.truncate(startDate, Calendar.DATE, timeZone);
        break;
        
      case "this.month":
        startDate = RLMDateUtils.truncate(now, Calendar.MONTH, timeZone);
        break;
        
      case "last.1.month":
        startDate = RLMDateUtils.addMonths(startDate, -1, timeZone);
        endDate = RLMDateUtils.ceiling(startDate, Calendar.MONTH, timeZone);
        startDate = RLMDateUtils.truncate(startDate, Calendar.MONTH, timeZone);
        break;
        
      case "last.3.month":
        startDate = RLMDateUtils.addMonths(startDate, -1, timeZone);
        endDate = RLMDateUtils.ceiling(startDate, Calendar.MONTH, timeZone);
        startDate = RLMDateUtils.addMonths(startDate, -2, timeZone);
        startDate = RLMDateUtils.truncate(startDate, Calendar.MONTH, timeZone);
        break;
        
      case "this.week":
        startDate = RLMDateUtils.truncate(now, Calendar.DAY_OF_WEEK, timeZone);
        break;
        
      case "last.7.days":
        startDate = RLMDateUtils.addDays(startDate, -1, timeZone);
        endDate = RLMDateUtils.ceiling(startDate, Calendar.DATE, timeZone);
        startDate = RLMDateUtils.addDays(startDate, -6, timeZone);
        startDate = RLMDateUtils.truncate(startDate, Calendar.DATE, timeZone);
        break;
        
      case "last.week":
        startDate = RLMDateUtils.addWeeks(startDate, -1, timeZone);
        endDate = RLMDateUtils.ceiling(startDate, Calendar.DAY_OF_WEEK, timeZone);
        startDate =RLMDateUtils.truncate(startDate, Calendar.DAY_OF_WEEK, timeZone);
        break;

      default:
        break;
    }
    Pair<Date, Date> result = new Pair<Date, Date>(startDate, endDate);
    if (log.isTraceEnabled()) log.trace("BadgeCriteria result getStartAndEndDate {}", result);
    return result;
  }

  /**
   * getPredictionCurrentRank
   * 
   * @param evaluationRequest
   * @param fieldName           UNUSED - filled with null in deployment-product-evolution settings (see: evolutionProfileCriterionFields).
   * @param subcriteriaVal      Here we are expecting a singleton list with predictionID value (String)
   * @return
   */
  public static Object getPredictionCurrentRank(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    String predictionID = (String) subcriteriaVal.get(0);
    
    Prediction prediction = subscriberProfile.getPredictions().getCurrent().get(predictionID);
    if(prediction != null) {
      return prediction.getNcileInterval(100);
    }
    else {
      return null;
    }
  }

  /**
   * getPredictionCurrentScore
   * 
   * @param evaluationRequest
   * @param fieldName           UNUSED - filled with null in deployment-product-evolution settings (see: evolutionProfileCriterionFields).
   * @param subcriteriaVal      Here we are expecting a singleton list with predictionID value (String)
   * @return
   */
  public static Object getPredictionCurrentScore(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    String predictionID = (String) subcriteriaVal.get(0);
    
    Prediction prediction = subscriberProfile.getPredictions().getCurrent().get(predictionID);
    if(prediction != null) {
      return prediction.score;
    }
    else {
      return null;
    }
  }

  /**
   * getPredictionRankEvolution
   * 
   * @param evaluationRequest
   * @param fieldName           UNUSED - filled with null in deployment-product-evolution settings (see: evolutionProfileCriterionFields).
   * @param subcriteriaVal      Here we are expecting a singleton list with predictionID value (String)
   * @return
   */
  public static Object getPredictionRankEvolution(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) 
  {
    SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
    String predictionID = (String) subcriteriaVal.get(0);
    
    Prediction current = subscriberProfile.getPredictions().getCurrent().get(predictionID);
    Prediction previous = subscriberProfile.getPredictions().getPrevious().get(predictionID);
    if(previous != null && current != null) {
      return current.getNcileInterval(100) - previous.getNcileInterval(100);
    }
    else {
      return null;
    }
  }
  
  /*****************************************
  *
  *  simple
  *
  *****************************************/

  //
  //  system
  //

  public static Object getEvaluationDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getEvaluationDate(); }
  public static Object getJourneyEvaluationEventName(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return (evaluationRequest.getSubscriberStreamEvent() != null && evaluationRequest.getSubscriberStreamEvent() instanceof EvolutionEngineEvent) ? ((EvolutionEngineEvent) evaluationRequest.getSubscriberStreamEvent()).getEventName() : null; }
  public static Object isUnknownRelationship(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
    { 
      List<Pair<String, String>> unknownRelationships = evaluationRequest.getSubscriberProfile().getUnknownRelationships();
      Pair<String, String> p = new Pair<>(evaluationRequest.getJourneyState().getJourneyID(), evaluationRequest.getJourneyNode().getNodeID());
      if(unknownRelationships != null) {
        for(Pair<String, String> current : unknownRelationships)
          {
            if(p.getFirstElement().equals(current.getFirstElement()) && p.getSecondElement().equals(current.getSecondElement())) {
              return true;
            }
            else {
              return false;
            }
          }
      }
      return false;
    }
  
  
  
  public static Object getEvaluationAniversary(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return getEvaluationDate(evaluationRequest, fieldName);}
  
  public static Object getSubscriberTmpSuccessVouchers(SubscriberEvaluationRequest evaluationRequest, String fieldName)
  {
    Set<String> res = evaluationRequest.getJourneyState().getVoucherChanges().stream().filter(vc -> vc.getReturnStatus() == RESTAPIGenericReturnCodes.SUCCESS).map(VoucherChange::getVoucherCode).collect(Collectors.toSet());
    return res;
  }
  public static Object getSubscriberTmpSuccessBadges(SubscriberEvaluationRequest evaluationRequest, String fieldName)
  {
    Set<String> res = evaluationRequest.getJourneyState().getBadgeChanges().stream().filter(bc -> bc.getReturnStatus() == RESTAPIGenericReturnCodes.SUCCESS).map(BadgeChange::getBadgeID).collect(Collectors.toSet());
    return res;
  }
  
  public static Object getRandom100(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return ThreadLocalRandom.current().nextInt(100); }
  public static Object getTrue(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return Boolean.TRUE; }
  public static Object getFalse(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return Boolean.FALSE; }
  public static Object getUnsupportedField(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return null; }
  public static Object getEvaluationWeekDay(SubscriberEvaluationRequest evaluationRequest, String fieldName) { int weekDay = RLMDateUtils.getField(evaluationRequest.getEvaluationDate(), Calendar.DAY_OF_WEEK, Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone()); Set<String> evaluationWeekDay = new HashSet<String>(); evaluationWeekDay.add(getDay(weekDay)); return evaluationWeekDay; }
  public static Object getEvaluationMonth(SubscriberEvaluationRequest evaluationRequest, String fieldName) { int month = RLMDateUtils.getField(evaluationRequest.getEvaluationDate(), Calendar.MONTH, Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone()); Set<String> evaluationMonth = new HashSet<String>(); evaluationMonth.add(getMonth(month)); return evaluationMonth; }
  public static Object getEvaluationDayOfMonth(SubscriberEvaluationRequest evaluationRequest, String fieldName) {  int dayOfMonth = RLMDateUtils.getField(evaluationRequest.getEvaluationDate(), Calendar.DAY_OF_MONTH, Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone()); return dayOfMonth; }
  public static Object getEvaluationTime(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
  { 
    Date evaluationDate = evaluationRequest.getEvaluationDate();
    StringBuilder evaluationTimeBuilder = new StringBuilder();
    evaluationTimeBuilder.append(RLMDateUtils.getField(evaluationDate, Calendar.HOUR_OF_DAY, com.evolving.nglm.core.Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone())).append(":");
    evaluationTimeBuilder.append(RLMDateUtils.getField(evaluationDate, Calendar.MINUTE, com.evolving.nglm.core.Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone())).append(":");
    evaluationTimeBuilder.append(RLMDateUtils.getField(evaluationDate, Calendar.SECOND, com.evolving.nglm.core.Deployment.getDeployment(evaluationRequest.getTenantID()).getTimeZone()));
    return evaluationTimeBuilder.toString(); 
  }

  //
  //  journey
  //

  public static Object getJourneyEntryDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getJourneyState().getJourneyEntryDate(); }
  public static Object getJourneyEndDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getJourneyState().getJourneyEndDate(); }
  public static Object getJourneyParameter(SubscriberEvaluationRequest evaluationRequest, String fieldName) {
    Object value = evaluationRequest.getJourneyState().getJourneyParameters().get(fieldName);
    return evaluateParameter(evaluationRequest, value); }
  public static Object getJourneyNodeEntryDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getJourneyState().getJourneyNodeEntryDate(); }
  public static Object getJourneyNodeParameter(SubscriberEvaluationRequest evaluationRequest, String fieldName) { 
    ParameterMap parameters = evaluationRequest.getJourneyNode().getNodeParameters();
    return evaluateParameter(evaluationRequest, parameters.get(fieldName)); 
    }
  public static Object getJourneyLinkParameter(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluateParameter(evaluationRequest, evaluationRequest.getJourneyLink().getLinkParameters().get(fieldName)); }
  public static Object getActionAttribute(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluateParameter(evaluationRequest, evaluationRequest.getJourneyState().getJourneyActionManagerContext().get(fieldName)); }

  //
  //  subscriberMessages
  //

  public static Object getSubscriberMessageParameterTag(SubscriberEvaluationRequest evaluationRequest, String fieldName) {
    Object result = null;
    String tagJourneyNodeParameterName = evaluationRequest.getMiscData().get("tagJourneyNodeParameterName");
    if(tagJourneyNodeParameterName == null) {
      tagJourneyNodeParameterName = "node.parameter.message"; // compatibility with OLD SMS
    }
    // OLD Way to retrieve subscriberMessage
    SubscriberMessage subscriberMessage = (SubscriberMessage) CriterionFieldRetriever.getJourneyNodeParameter(evaluationRequest, tagJourneyNodeParameterName);
    if(subscriberMessage == null) {
      // compatibility with OLD MAIL
      subscriberMessage = (SubscriberMessage) CriterionFieldRetriever.getJourneyNodeParameter(evaluationRequest, "node.parameter.message");
    }
    if(subscriberMessage == null) {
      // GENERIC WAY to retrieve subscriberMessage
      subscriberMessage = (SubscriberMessage) CriterionFieldRetriever.getJourneyNodeParameter(evaluationRequest, "node.parameter.dialog_template");
    }
    SimpleParameterMap parameterMap = subscriberMessage.getParameterTags();

    result = evaluateParameter(evaluationRequest, parameterMap.get(fieldName)); 
    return result;
    }
  
  //
  //  simple
  //
  
  public static Object getEvolutionSubscriberStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return (evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatus() != null) ? evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatus().getExternalRepresentation() : null; }
  public static Object getPreviousEvolutionSubscriberStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return (evaluationRequest.getSubscriberProfile().getPreviousEvolutionSubscriberStatus() != null) ? evaluationRequest.getSubscriberProfile().getPreviousEvolutionSubscriberStatus().getExternalRepresentation() : null; }
  public static Object getEvolutionSubscriberStatusChangeDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatusChangeDate(); }
  public static Object getUniversalControlGroup(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getUniversalControlGroup(); }
  public static Object getLanguageID(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getLanguageID() != null ? evaluationRequest.getSubscriberProfile().getLanguageID() : Deployment.getDeployment(evaluationRequest.getTenantID()).getLanguageID(); }

  //
  //  segments membership
  //

  public static Object getSegments(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getSegments(evaluationRequest.getSubscriberGroupEpochReader()); }

  //
  //  targets membership
  //

  public static Object getTargets(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getTargets(evaluationRequest.getSubscriberGroupEpochReader()); }

  
  //
  //  exclusionInclusionTarget membership
  //

  public static Object getExclusionInclusionTargets(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getExclusionInclusionTargets(evaluationRequest.getSubscriberGroupEpochReader()); }

  //
  //  for profileChange old and new value access by type.
  //
  
  public static Object getProfileChangeFieldOldValue(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return ((ProfileChangeEvent)(evaluationRequest.getSubscriberStreamEvent())).getOldValue(fieldName); }
  public static Object getProfileChangeFieldNewValue(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return ((ProfileChangeEvent)(evaluationRequest.getSubscriberStreamEvent())).getNewValue(fieldName); }
  public static Object getProfileChangeFieldsUpdated(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return ((ProfileChangeEvent)(evaluationRequest.getSubscriberStreamEvent())).getIsProfileFieldUpdated(fieldName); }
  

  //
  //  for segmentProfileChange old and new value access by type.
  //
  
  public static Object getProfileSegmentChangeDimensionOldValue(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return ((ProfileSegmentChangeEvent)(evaluationRequest.getSubscriberStreamEvent())).getOldSegment(fieldName); }
  public static Object getProfileSegmentChangeDimensionNewValue(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return ((ProfileSegmentChangeEvent)(evaluationRequest.getSubscriberStreamEvent())).getNewSegment(fieldName); }
  public static Object getProfileSegmentChangeDimensionUpdated(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return ((ProfileSegmentChangeEvent)(evaluationRequest.getSubscriberStreamEvent())).isDimensionUpdated(fieldName); }

  //
  //  for loyaltyProgramPointsProfileChange old and new tier access by type.
  //
  
  public static Object getProfilePointLoyaltyProgramChangeTierOldValue(SubscriberEvaluationRequest evaluationRequest, String fieldName) {
    
    // Check if this is for the good loyalty program...
    ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent)(evaluationRequest.getSubscriberStreamEvent());
    String[] fields = fieldName.split("\\.");
    if(! fields[2].equals(event.getLoyaltyProgramID())) { return null; }
    
    if(event.getInfos().get(LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.ENTERING.getExternalRepresentation()) != null)
      {
        return LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.ENTERING.name();
      }
    else 
      {
        return event.getInfos().get(LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation());
      }
  }
  
  public static Object getProfileChallengeLoyaltyProgramChangeLevelOldValue(SubscriberEvaluationRequest evaluationRequest, String fieldName)
  {
    ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent) (evaluationRequest.getSubscriberStreamEvent());
    String[] fields = fieldName.split("\\.");
    if (!fields[2].equals(event.getLoyaltyProgramID()))
      {
        return null;
      }
    if (event.getInfos().get(LoyaltyProgramChallenge.LoyaltyProgramChallengeEventInfos.ENTERING.getExternalRepresentation()) != null)
      {
        return LoyaltyProgramChallenge.LoyaltyProgramChallengeEventInfos.ENTERING.name();
      } 
    else
      {
        return event.getInfos().get(LoyaltyProgramChallenge.LoyaltyProgramChallengeEventInfos.OLD_LEVEL.getExternalRepresentation());
      }
  }
  
  public static Object getProfileMissionLoyaltyProgramChangeStepOldValue(SubscriberEvaluationRequest evaluationRequest, String fieldName)
  {
    ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent) (evaluationRequest.getSubscriberStreamEvent());
    String[] fields = fieldName.split("\\.");
    if (!fields[2].equals(event.getLoyaltyProgramID()))
      {
        return null;
      }
    if (event.getInfos().get(LoyaltyProgramMission.LoyaltyProgramMissionEventInfos.ENTERING.getExternalRepresentation()) != null)
      {
        return LoyaltyProgramMission.LoyaltyProgramMissionEventInfos.ENTERING.name();
      } 
    else
      {
        return event.getInfos().get(LoyaltyProgramMission.LoyaltyProgramMissionEventInfos.OLD_STEP.getExternalRepresentation());
      }
  }
  
  public static Object getProfilePointLoyaltyProgramChangeTierNewValue(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
    {
      // Check if this is for the good loyalty program...
      ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent)(evaluationRequest.getSubscriberStreamEvent());
      String[] fields = fieldName.split("\\.");
      if(! fields[2].equals(event.getLoyaltyProgramID())) { return null; }
      
      if(event.getInfos().get(LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.LEAVING.getExternalRepresentation()) != null)
        {
          return LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.LEAVING.name();
        }
      else 
        {
          return event.getInfos().get(LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation());
        }
    }
  
  public static Object getProfileChallengeLoyaltyProgramChangeLevelNewValue(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
  {
    ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent) (evaluationRequest.getSubscriberStreamEvent());
    String[] fields = fieldName.split("\\.");
    if (!fields[2].equals(event.getLoyaltyProgramID()))
      {
        return null;
      }
    if (event.getInfos().get(LoyaltyProgramChallenge.LoyaltyProgramChallengeEventInfos.LEAVING.getExternalRepresentation()) != null)
      {
        return LoyaltyProgramChallenge.LoyaltyProgramChallengeEventInfos.LEAVING.name();
      } 
    else
      {
        return event.getInfos().get(LoyaltyProgramChallenge.LoyaltyProgramChallengeEventInfos.NEW_LEVEL.getExternalRepresentation());
      }
  }
  
  public static Object getProfileMissionLoyaltyProgramChangeStepNewValue(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
  {
    ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent) (evaluationRequest.getSubscriberStreamEvent());
    String[] fields = fieldName.split("\\.");
    if (!fields[2].equals(event.getLoyaltyProgramID()))
      {
        return null;
      }
    if (event.getInfos().get(LoyaltyProgramMission.LoyaltyProgramMissionEventInfos.LEAVING.getExternalRepresentation()) != null)
      {
        return LoyaltyProgramMission.LoyaltyProgramMissionEventInfos.LEAVING.name();
      } 
    else
      {
        return event.getInfos().get(LoyaltyProgramMission.LoyaltyProgramMissionEventInfos.NEW_STEP.getExternalRepresentation());
      }
  }
  
  public static Object getProfilePointLoyaltyProgramUpdated(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
    {
      // Check if this is for the good loyalty program...
      ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent)(evaluationRequest.getSubscriberStreamEvent());
      String[] fields = fieldName.split("\\.");
      if(! fields[2].equals(event.getLoyaltyProgramID())) { return null; }
      return event.getLoyaltyProgramID().equals(fieldName.substring(LoyaltyProgramPoints.CRITERION_FIELD_NAME_IS_UPDATED_PREFIX.length()));
    }
  
  public static Object getProfileChallengeLoyaltyProgramUpdated(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
  {
    ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent)(evaluationRequest.getSubscriberStreamEvent());
    String[] fields = fieldName.split("\\.");
    if(! fields[2].equals(event.getLoyaltyProgramID())) { return null; }
    return event.getLoyaltyProgramID().equals(fieldName.substring(LoyaltyProgramChallenge.CRITERION_FIELD_NAME_IS_UPDATED_PREFIX.length()));
  }
  
  public static Object getProfileMissionLoyaltyProgramUpdated(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
  {
    ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent)(evaluationRequest.getSubscriberStreamEvent());
    String[] fields = fieldName.split("\\.");
    if(! fields[2].equals(event.getLoyaltyProgramID())) { return null; }
    return event.getLoyaltyProgramID().equals(fieldName.substring(LoyaltyProgramMission.CRITERION_FIELD_NAME_IS_UPDATED_PREFIX.length()));
  }
  
  public static Object getProfilePointLoyaltyProgramTierUpdateType(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
  {
    
    // Check if this is for the good loyalty program...
    ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent)(evaluationRequest.getSubscriberStreamEvent());
    String[] fields = fieldName.split("\\.");
    if(! fields[1].equals(event.getLoyaltyProgramID())) { return null; }
    
    return event.getInfos().get(LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.TIER_UPDATE_TYPE.getExternalRepresentation());
  }
  
  public static Object getProfileChallengeLoyaltyProgramLevelUpdateType(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
  {
    ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent) (evaluationRequest.getSubscriberStreamEvent());
    String[] fields = fieldName.split("\\.");
    if (!fields[1].equals(event.getLoyaltyProgramID()))
      {
        return null;
      }
    return event.getInfos().get(LoyaltyProgramChallenge.LoyaltyProgramChallengeEventInfos.LEVEL_UPDATE_TYPE.getExternalRepresentation());
  }
  
  public static Object getProfileMissionLoyaltyProgramStepUpdateType(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
  {
    ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent) (evaluationRequest.getSubscriberStreamEvent());
    String[] fields = fieldName.split("\\.");
    if (!fields[1].equals(event.getLoyaltyProgramID()))
      {
        return null;
      }
    return event.getInfos().get(LoyaltyProgramMission.LoyaltyProgramMissionEventInfos.STEP_UPDATE_TYPE.getExternalRepresentation());
  }

  //
  //  TokenRedeemed
  //
  
  public static Object getTokenType(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((TokenRedeemed) evaluationRequest.getSubscriberStreamEvent()).getTokenType(); }
  public static Object getAcceptedOfferId(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((TokenRedeemed) evaluationRequest.getSubscriberStreamEvent()).getAcceptedOfferId(); }    

  //
  //  BonusDelivery
  //

  public static Object getModuleId_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getModuleID(); }
  public static Object getFeatureId_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getFeatureID(); }
  public static Object getReturnCode_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryReturnCode(); }
  public static Object getDeliveryStatus_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getDeliveryStatus() != null ? ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getDeliveryStatus().getExternalRepresentation() : null; }
  public static Object getReturnCodeDetails_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryReturnCodeDetails(); }
  public static Object getOrigin_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryOrigin(); }
  public static Object getProviderId_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryProviderId(); }
  public static Object getDeliverableId_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryDeliverableId(); }
  public static Object getDeliverableName_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryDeliverableName(); }
  public static Object getDeliverableQty_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryDeliverableQty(); }
  public static Object getDeliverableExpiration_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getTimeout(); }
  public static Object getOperation_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryOperation(); }

  //
  //  OfferDelivery
  //

  public static Object getModuleId_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryModuleId(); }
  public static Object getFeatureId_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryFeatureId(); }    
  public static Object getReturnCode_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryReturnCode(); }
  public static Object getDeliveryStatus_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryDeliveryStatus() != null ? ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryDeliveryStatus().getExternalRepresentation() : null; }    
  public static Object getReturnCodeDetails_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryReturnCodeDetails(); }
  public static Object getOrigin_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryOrigin(); }
  public static Object getOfferDisplay_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryOfferDisplay(); }
  public static Object getOfferID_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryOfferID(); }
  public static Object getOfferQty_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryOfferQty(); }    
  public static Object getSalesChannelId_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliverySalesChannelId(); }
  public static Object getOfferPrice_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryOfferPrice(); }    
  public static Object getMeanOfPayment_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryMeanOfPayment(); }
  public static Object getOfferContent_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryOfferContent(); }
  public static Object getVoucherCode_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryVoucherCode(); }
  public static Object getVoucherPartnerId_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryVoucherPartnerId(); }
  public static Object getResellerName_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getResellerName_OfferDelivery(); }
  public static Object getSupplierName_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getSupplierName_OfferDelivery(); }
  
  //
  //  MessageDelivery
  //

  public static Object getModuleId_MessageDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((MessageDelivery) evaluationRequest.getSubscriberStreamEvent()).getMessageDeliveryModuleId(); }
  public static Object getFeatureId_MessageDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((MessageDelivery) evaluationRequest.getSubscriberStreamEvent()).getMessageDeliveryFeatureId(); }    
  public static Object getReturnCode_MessageDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((MessageDelivery) evaluationRequest.getSubscriberStreamEvent()).getMessageDeliveryReturnCode(); }
  public static Object getDeliveryStatus_MessageDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((MessageDelivery) evaluationRequest.getSubscriberStreamEvent()).getMessageDeliveryDeliveryStatus() != null ? ((MessageDelivery) evaluationRequest.getSubscriberStreamEvent()).getMessageDeliveryDeliveryStatus().getExternalRepresentation() : null; }    
  public static Object getReturnCodeDetails_MessageDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((MessageDelivery) evaluationRequest.getSubscriberStreamEvent()).getMessageDeliveryReturnCodeDetails(); }
  public static Object getOrigin_MessageDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((MessageDelivery) evaluationRequest.getSubscriberStreamEvent()).getMessageDeliveryOrigin(); }
  public static Object getMessageId_MessageDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((MessageDelivery) evaluationRequest.getSubscriberStreamEvent()).getMessageDeliveryMessageId(); }
  
  //
  //  voucher
  //
  
  public static Object getVoucherValidationVoucherCode(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException 
  { String result = ((VoucherValidation) evaluationRequest.getSubscriberStreamEvent()).getVoucherValidationVoucherCode(); return result; }
  public static Object getVoucherValidationStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException 
  { String result =  ((VoucherValidation) evaluationRequest.getSubscriberStreamEvent()).getVoucherValidationStatus(); return result; }
  public static Object getVoucherValidationStatusCode(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException 
  { Integer result =  ((VoucherValidation) evaluationRequest.getSubscriberStreamEvent()).getVoucherValidationStatusCode(); return result; }
  
  public static Object getVoucherRedemptionVoucherCode(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((VoucherRedemption) evaluationRequest.getSubscriberStreamEvent()).getVoucherRedemptionVoucherCode(); }
  public static Object getVoucherRedemptionStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((VoucherRedemption) evaluationRequest.getSubscriberStreamEvent()).getVoucherRedemptionStatus(); }
  public static Object getVoucherRedemptionStatusCode(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((VoucherRedemption) evaluationRequest.getSubscriberStreamEvent()).getVoucherRedemptionStatusCode(); }
  
  //
  //  FileWithVariableEvent
  //
  
  public static Object getFileWithVariableID(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((FileWithVariableEvent) evaluationRequest.getSubscriberStreamEvent()).getFileID(); }

  

  /*****************************************
  *
  *  complex
  *
  *****************************************/

  //
  // getLoyaltyProgramsTiers
  //
  
  public static Object getLoyaltyProgramsTiers(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException {
    Set<String> res = evaluationRequest.getSubscriberProfile().getLoyaltyPrograms().values().stream()
        .filter(lps -> (lps.getLoyaltyProgramExitDate() == null))
        .filter(lps -> (lps instanceof LoyaltyProgramPointsState))
        .map(lps -> (LoyaltyProgramPointsState) lps)
        .map(lps -> ((LoyaltyProgramPointsState) lps).getTierName())
        .collect(Collectors.toSet());
    return res;
  }

  //
  // getLoyaltyPrograms
  //
  
  public static Object getLoyaltyPrograms(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException {
    Set<String> res = evaluationRequest.getSubscriberProfile().getLoyaltyPrograms().values().stream()
        .filter(lps -> (lps.getLoyaltyProgramExitDate() == null))
        .map(lps -> lps.getLoyaltyProgramName())
        .collect(Collectors.toSet());
    return res;
  }
  
  //
  // getLoyaltyChallengesLevels
  //
  
  public static Object getLoyaltyChallengesLevels(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException
  {
    Set<String> res = evaluationRequest.getSubscriberProfile().getLoyaltyPrograms().values().stream().filter(lps -> (lps.getLoyaltyProgramExitDate() == null)).filter(lps -> (lps instanceof LoyaltyProgramChallengeState)).map(lps -> (LoyaltyProgramChallengeState) lps).map(lps -> ((LoyaltyProgramChallengeState) lps).getLevelName()).collect(Collectors.toSet());
    return res;
  }
  
  //
  // getLoyaltyMissionsSteps
  //
  
  public static Object getLoyaltyMissionsSteps(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException
  {
    Set<String> res = evaluationRequest.getSubscriberProfile().getLoyaltyPrograms().values().stream().filter(lps -> (lps.getLoyaltyProgramExitDate() == null)).filter(lps -> (lps instanceof LoyaltyProgramMissionState)).map(lps -> (LoyaltyProgramMissionState) lps).map(lps -> ((LoyaltyProgramMissionState) lps).getStepName()).collect(Collectors.toSet());
    return res;
  }
  
  // getVoucherCodes
  //
  
  public static Object getVoucherCodes(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException {
   
   List <VoucherProfileStored> vouchers = evaluationRequest.getSubscriberProfile().getVouchers();
   Set<String> res = new HashSet<>();
   for (VoucherProfileStored voucher : vouchers) {
     res.add(voucher.getVoucherCode());
   }
   return res;
  }

  //
  //  getCustomCriterionField (dynamic)
  //

  public static Object getCustomCriterionField(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException
  {
    Object result = null;
    
    Pattern criterionIDPattern = Pattern.compile("^customCriteria\\.(.*)$");
    Matcher criterionIDMatcher = criterionIDPattern.matcher(fieldName);
    if (! criterionIDMatcher.find()) throw new CriterionException("invalid custom criterion field " + fieldName);
    String criterionID = criterionIDMatcher.group(1);

    // retrieve customCriteria
    EvolutionEventContext context = evaluationRequest.geEvolutionEventContext();
    if (context != null) {
      CustomCriteriaService customCriteriaService = context.getCustomCriteriaService();
      if (customCriteriaService != null) {
        CustomCriteria customCriteria = customCriteriaService.getActiveCustomCriteria(criterionID, evaluationRequest.getEvaluationDate());
        if (customCriteria == null) {
          log.info("Refering to customCriteria " + criterionID + " which does not exist");
        } else {
          result = customCriteria.getExpression().evaluate(evaluationRequest, null);
        }
      }
    }
    return result;
  }

  //
  //  getLoyaltyProgramCriterionField (dynamic)
  //

  public static Object getLoyaltyProgramCriterionField(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException
  {
    //
    //  extract program and variable name
    //

    Pattern fieldNamePattern = Pattern.compile("^loyaltyprogram\\.([^.]+)\\.(.+)$");
    Matcher fieldNameMatcher = fieldNamePattern.matcher(fieldName);
    if (! fieldNameMatcher.find()) throw new CriterionException("invalid loyaltyprogram field " + fieldName);
    String loyaltyProgramID = fieldNameMatcher.group(1);
    String criterionFieldBaseName = fieldNameMatcher.group(2);
    String tierUpdateType = null;
    Object result = null;
    Date optInDate = null;
    Date optOutDate = null; 
    boolean loyaltyProgramStateAvailable = false;
    
    //
    //  loyaltyProgramState
    //

    LoyaltyProgramState loyaltyProgramState = evaluationRequest.getSubscriberProfile().getLoyaltyPrograms().get(loyaltyProgramID);
    
    ///
    // optin and optout should be valid even if the customer left the program already
    //
    
    if (loyaltyProgramState != null)
      {
        if (loyaltyProgramState instanceof LoyaltyProgramPointsState)
          {
            LoyaltyProgramPointsState loyaltyProgramPointsStateOptInOptOut = (LoyaltyProgramPointsState) loyaltyProgramState;
            optInDate = loyaltyProgramPointsStateOptInOptOut.getLoyaltyProgramEnrollmentDate();
            optOutDate = loyaltyProgramPointsStateOptInOptOut.getLoyaltyProgramExitDate();            
          }
        else if (loyaltyProgramState instanceof LoyaltyProgramChallengeState)
          {
            LoyaltyProgramChallengeState loyaltyProgramChallengeState = (LoyaltyProgramChallengeState) loyaltyProgramState;
            optInDate = loyaltyProgramChallengeState.getLoyaltyProgramEnrollmentDate();
            optOutDate = loyaltyProgramChallengeState.getLoyaltyProgramExitDate(); 
          }
        else if (loyaltyProgramState instanceof LoyaltyProgramMissionState)
          {
            LoyaltyProgramMissionState loyaltyProgramMissionState = (LoyaltyProgramMissionState) loyaltyProgramState;
            optInDate = loyaltyProgramMissionState.getLoyaltyProgramEnrollmentDate();
            optOutDate = loyaltyProgramMissionState.getLoyaltyProgramExitDate(); 
          }
      }
    
    //
    // opted out previously?
    //

    if (loyaltyProgramState != null && loyaltyProgramState.getLoyaltyProgramExitDate() != null)
      {
        loyaltyProgramState = null;
        loyaltyProgramStateAvailable = true;
      }

    //
    // in program?
    //

    if (loyaltyProgramState == null && !loyaltyProgramStateAvailable)
      return null;

    else if (loyaltyProgramState == null && loyaltyProgramStateAvailable)
      {
        switch (criterionFieldBaseName)
        {
          case "optindate":
            result = optInDate;
            break;

          case "optoutdate":
            result = optOutDate;
            break;
            
          default:
            result = null;
            break;
        }
      }
    else
      {
        if (loyaltyProgramState instanceof LoyaltyProgramPointsState)
          {
            //
            //  retrieve
            //
            
            LoyaltyProgramPointsState loyaltyProgramPointsState = (LoyaltyProgramPointsState) loyaltyProgramState;
            TierHistory tierHistory = loyaltyProgramPointsState.getLoyaltyProgramHistory().getLastTierEntered(); 
            tierUpdateType = tierHistory.getTierUpdateType().getExternalRepresentation();

            switch (criterionFieldBaseName)
              {
                case "tier":
                  result = loyaltyProgramPointsState.getTierName();
                  break;

                case "statuspoint.balance":
                  result = loyaltyProgramPointsState.getStatusPoints();
                  break;

                case "rewardpoint.balance":
                  result = loyaltyProgramPointsState.getRewardPoints();
                  break;

                case "tierupdatedate":
                  result = loyaltyProgramPointsState.getTierEnrollmentDate();
                  break;

                case "optindate":
                  result = optInDate;
                  break;

                case "optoutdate":
                  result = optOutDate;
                  break;
                  
                // Deprecated, i.e should not be used since EVPRO-665  
                case "tierupdatetype":
                  result = tierUpdateType;
                  break;

                default:
                  fieldNamePattern = Pattern.compile("^([^.]+)\\.([^.]+)\\.([^.]+)$");
                  fieldNameMatcher = fieldNamePattern.matcher(criterionFieldBaseName);
                  if (!fieldNameMatcher.find())
                    throw new CriterionException("invalid criterionFieldBaseName field " + criterionFieldBaseName);
                  String pointName = fieldNameMatcher.group(1);
                  String pointID = fieldNameMatcher.group(2);
                  String request = fieldNameMatcher.group(3);
                  Date evaluationDate = evaluationRequest.getEvaluationDate();
                  Map<String, PointBalance> pointBalances = evaluationRequest.getSubscriberProfile().getPointBalances();

                  Date earliestExpiration = evaluationDate;
                  int earliestExpiryQuantity = 0;

                  if (pointBalances == null)
                    {
                      log.info("Error evaluating " + fieldName + " no pointBalances for subscriber " + evaluationRequest.getSubscriberProfile().getSubscriberID() + " on LP " + loyaltyProgramPointsState.getLoyaltyProgramName() + " in tier " + loyaltyProgramPointsState.getTierName());
                    }
                  else
                    {
                      PointBalance pointBalance = pointBalances.get(pointID);
                      if (pointBalance == null)
                        {
                          log.info("Error evaluating " + fieldName + " no pointBalance for subscriber " + evaluationRequest.getSubscriberProfile().getSubscriberID() + " for point " + pointID + " on LP " + loyaltyProgramPointsState.getLoyaltyProgramName() + " in tier " + loyaltyProgramPointsState.getTierName());
                        }
                      else
                        {
                          earliestExpiration = pointBalance.getFirstExpirationDate(evaluationDate);
                          earliestExpiryQuantity = pointBalance.getBalance(earliestExpiration);
                          log.info("[TEST] earliestExpiry => date: {}, quantity: {}", earliestExpiration, earliestExpiryQuantity);
                        }
                    }

                  switch (request)
                    {
                      case "earliestexpirydate":
                        result = earliestExpiration;
                        break;

                      case "earliestexpiryquantity":
                        result = earliestExpiryQuantity;
                        break;

                      default:
                        throw new CriterionException("Invalid criteria " + criterionFieldBaseName + " " + request + " for subscriber " + evaluationRequest.getSubscriberProfile().getSubscriberID() + " for point " + pointID + " on LP " + loyaltyProgramPointsState.getLoyaltyProgramName() + " in tier " + loyaltyProgramPointsState.getTierName());
                    }
              }
          }
        else if (loyaltyProgramState instanceof LoyaltyProgramChallengeState)      
          {
            //
            //  retrieve
            //
            
            LoyaltyProgramChallengeState loyaltyProgramChallengeState = (LoyaltyProgramChallengeState) loyaltyProgramState;
            LevelHistory levelHistory = loyaltyProgramChallengeState.getLoyaltyProgramChallengeHistory().getLastLevelEntered(); 
            String levelUpdateType = levelHistory.getLevelUpdateType().getExternalRepresentation();

            switch (criterionFieldBaseName)
              {
                case "level":
                  result = loyaltyProgramChallengeState.getLevelName();
                  break;

                case "score":
                  result = loyaltyProgramChallengeState.getCurrentScore();
                  break;
                  
                case "lastScoreChangeDate":
                  result = loyaltyProgramChallengeState.getLastScoreChangeDate();
                  break;

                case "levelupdatedate":
                  result = loyaltyProgramChallengeState.getLevelEnrollmentDate();
                  break;

                case "optindate":
                  result = optInDate;
                  break;

                case "optoutdate":
                  result = optOutDate;
                  break;
                  
                // Deprecated, i.e should not be used since EVPRO-665  
                case "levelUpdateType":
                  result = levelUpdateType;
                  break;

                default:
                  throw new CriterionException("Invalid criteria " + criterionFieldBaseName);
              }
          }
        else if (loyaltyProgramState instanceof LoyaltyProgramMissionState)      
          {
            //
            //  retrieve
            //
            
            LoyaltyProgramMissionState loyaltyProgramMissionState = (LoyaltyProgramMissionState) loyaltyProgramState;
            StepHistory stepHistory = loyaltyProgramMissionState.getLoyaltyProgramMissionHistory().getLastStepEntered();
            String stepUpdateType = stepHistory.getStepUpdateType().getExternalRepresentation();

            switch (criterionFieldBaseName)
              {
                case "step":
                  result = loyaltyProgramMissionState.getStepName();
                  break;

                case "stepupdatedate":
                  result = loyaltyProgramMissionState.getStepEnrollmentDate();
                  break;
                  
                case "currentProgression":
                  result = loyaltyProgramMissionState.getCurrentProgression();
                  break;
                  
                case "isMissionCompleted":
                  result = loyaltyProgramMissionState.isMissionCompleted();
                  break;

                case "optindate":
                  result = optInDate;
                  break;

                case "optoutdate":
                  result = optOutDate;
                  break;
                  
                default:
                  throw new CriterionException("Invalid criteria " + criterionFieldBaseName);
              }
          }
      }
        
    //
    // return
    //

    return result;
  }

  //
  //  getPointCriterionField (dynamic)
  //

  public static Object getPointCriterionField(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException
  {
    //
    //  extract point and variable name
    //

    Pattern fieldNamePattern = Pattern.compile("^point\\.([^.]+)\\.(.+)$");
    Matcher fieldNameMatcher = fieldNamePattern.matcher(fieldName);
    if (! fieldNameMatcher.find()) throw new CriterionException("invalid point field " + fieldName);
    String pointID = fieldNameMatcher.group(1);
    String criterionFieldBaseName = fieldNameMatcher.group(2);
    
    Object result = 0;
    Date evaluationDate = evaluationRequest.getEvaluationDate();
    Date earliestExpiration = evaluationDate;
    int earliestExpiryQuantity = 0;
    Integer balance = 0;
    Map<String, PointBalance> pointBalances = evaluationRequest.getSubscriberProfile().getPointBalances();
    PointBalance pointBalance = null;
    if (pointBalances == null)
      {
        log.info("Error evaluating " + fieldName + " no pointBalances for subscriber " + evaluationRequest.getSubscriberProfile().getSubscriberID());
        return null; // so that the calling criteria will return false
      }
    else
      {
        pointBalance = pointBalances.get(pointID);
        if (pointBalance == null)
          {
            log.info("Error evaluating " + fieldName + " no pointBalance for subscriber " + evaluationRequest.getSubscriberProfile().getSubscriberID() + " for point " + pointID);
            return null; // so that the calling criteria will return false
          }
        else
          {
            balance = new Integer(pointBalance.getBalance(evaluationDate));
            earliestExpiration = pointBalance.getFirstExpirationDate(evaluationDate);
            earliestExpiryQuantity = pointBalance.getBalance(earliestExpiration);
            log.info("[TEST] earliestExpiry => date: {}, quantity: {}", earliestExpiration, earliestExpiryQuantity);
          }
      }
      switch (criterionFieldBaseName)
        {
          case "balance":
            result = balance;
            break;
            
          case "earliestexpirydate":
            result = earliestExpiration;
            break;

          case "earliestexpiryquantity":
            result = earliestExpiryQuantity;
            break;

          default:
            // earned.yesterday,...
            fieldNamePattern = Pattern.compile("^([^.]+)\\.([^.]+)$");
            fieldNameMatcher = fieldNamePattern.matcher(criterionFieldBaseName);
            if (! fieldNameMatcher.find()) throw new CriterionException("invalid criterionFieldBaseName field " + criterionFieldBaseName);
            String nature = fieldNameMatcher.group(1); // earned, consumed, expired
            String interval = fieldNameMatcher.group(2); // yesterday, last7days, last30days
            if (pointBalance != null)
              {
                MetricHistory metric;
                switch (nature)
                {
                  case "earned"   : metric = pointBalance.getEarnedHistory(); break;
                  case "consumed" : metric = pointBalance.getConsumedHistory(); break;
                  case "expired"  : metric = pointBalance.getExpiredHistory(); break;
                  default: throw new CriterionException("invalid criterionField nature " + nature + " (should be earned, consumed, expired)");
                }
                if (metric == null)
                  {
                    log.info("null metric evaluating " + fieldName + " and " + criterionFieldBaseName + "for subscriber " + evaluationRequest.getSubscriberProfile().getSubscriberID() + " returning default value " + result);
                  }
                else
                  {
                    Long value = 0L;
                    switch (interval)
                    {
                      case "yesterday"  : value = metric.getYesterday(evaluationDate); break;
                      case "last7days"  : value = metric.getPrevious7Days(evaluationDate); break;
                      case "last30days" : value = metric.getPrevious30Days(evaluationDate); break;
                      case "today"      : value = metric.getToday(evaluationDate); break;
                      case "thisWeek"   : value = metric.getThisWeek(evaluationDate); break;
                      case "thisMonth"  : value = metric.getThisMonth(evaluationDate); break;
                      
                      default: throw new CriterionException("invalid criterionField interval " + interval + " (should be yesterday, last7days, last30days, thisWeek, thisMonth)");
                    }
                    if (value > Integer.MAX_VALUE && value < Integer.MIN_VALUE)
                      {
                        log.debug("Value for " + fieldName + " is outside of integer range : " + value + ", truncating");
                        result = ((value > 0) ? Integer.MAX_VALUE : Integer.MIN_VALUE);
                      }
                    else
                      {
                        result = (int) value.longValue();
                      }
                    break;
                  }
              }
        }
    return result;
  }
  
  //
  //  getJourneyActionDeliveryStatus
  //

  public static Object getJourneyActionDeliveryStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName)
  {
    /*****************************************
    *
    *  awaited response?
    *
    *****************************************/

    boolean awaitedResponse = true;
    awaitedResponse = awaitedResponse && evaluationRequest.getJourneyState().getJourneyOutstandingDeliveryRequestID() != null;
    awaitedResponse = awaitedResponse && evaluationRequest.getSubscriberStreamEvent() instanceof DeliveryRequest;
    awaitedResponse = awaitedResponse && ((DeliveryRequest) evaluationRequest.getSubscriberStreamEvent()).getDeliveryRequestID().equals(evaluationRequest.getJourneyState().getJourneyOutstandingDeliveryRequestID());

    /*****************************************
    *
    *  result
    *
    *****************************************/

    Object result = DeliveryStatus.Pending.getExternalRepresentation();
    if (awaitedResponse)
      {
        result = ((DeliveryRequest) evaluationRequest.getSubscriberStreamEvent()).getDeliveryStatus().getExternalRepresentation();
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return result;
  }

  //
  //  getJourneyActionJourneyStatus
  //

  public static Object getJourneyActionJourneyStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException
  {
    /*****************************************
    *
    *  awaited journey response
    *
    *****************************************/

    //
    //  journey response?
    //

    boolean isJourneyResponse = true;
    isJourneyResponse = isJourneyResponse && evaluationRequest.getSubscriberStreamEvent() instanceof JourneyRequest;
    isJourneyResponse = isJourneyResponse && ! ((JourneyRequest) evaluationRequest.getSubscriberStreamEvent()).isPending();

    //
    //  awaited journey response
    //

    JourneyRequest journeyResponse = isJourneyResponse ? (JourneyRequest) evaluationRequest.getSubscriberStreamEvent() : null;
    boolean awaitedJourneyResponse = false;
    if (isJourneyResponse)
      {
        JourneyState journeyState = evaluationRequest.getJourneyState();
        String journeyInstanceID = (journeyState != null) ? journeyState.getJourneyInstanceID() : null;
        awaitedJourneyResponse = journeyResponse.getCallingJourneyInstanceID() != null && journeyInstanceID != null && journeyResponse.getCallingJourneyInstanceID().equals(journeyInstanceID);
      }

    /*****************************************
    *
    *  result
    *
    *****************************************/

    if (awaitedJourneyResponse && ! journeyResponse.getEligible())
      return SubscriberJourneyStatus.NotEligible.getExternalRepresentation();
    else if (awaitedJourneyResponse)
      return journeyResponse.getJourneyStatus();
    else
      return null;
  }

  /*****************************************
  *
  *  evaluation variables
  *
  *****************************************/

  //
  //  getEvaluationJourney
  //

  public static Object getEvaluationJourney(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
  { 
    return "evaluation.variable.journey"; 
  }

  //
  //  getEvaluationJourneyStatus
  //
  
  public static Object getEvaluationJourneyStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException
  {
    Set<String> journeyIDs = ( Set<String>) evaluationRequest.getEvaluationVariables().get("evaluation.variable.journey");
    Set<String> status=new HashSet<String>();
    if (journeyIDs != null && !journeyIDs.isEmpty())
      {
        for (String journeyID : journeyIDs)
          {
            if (journeyID == null) throw new CriterionException("invalid journey status request");
            SubscriberJourneyStatus currentStatus = evaluationRequest.getSubscriberProfile().getSubscriberJourneys().get(journeyID);
            if(currentStatus != null) { status.add(currentStatus.getExternalRepresentation()); };
          }
      }
    else
      {
        if (evaluationRequest.getJourneyState() != null)
          {
            status.add(evaluationRequest.getSubscriberProfile().getSubscriberJourneys().get(evaluationRequest.getJourneyState().getJourneyID()).getExternalRepresentation());
          }
      }
     return status;
  }
  /*
  // migration start EVPRO-1185
  @Deprecated // should use getComplexObjectXXXX // must be removed when all the customer using adv criteria when using complex fields
  public static Object getComplexObjectFieldValue(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException
  {
    // parse the field name to retrieve the good value...
    // complexObject.<objectTypeID>.<elementID>.<subfieldName>;
    String[] split = fieldName.split("\\.");
    if (split.length != 4 || !split[0].equals("complexObject"))
      {
        throw new CriterionException("field " + fieldName + " can't be handled");
      }
    String objectTypeID = split[1];
    String elementID = split[2];
    String subfieldName = split[3];
    List<ComplexObjectInstance> complexObjectInstances = evaluationRequest.getSubscriberProfile().getComplexObjectInstances();
    if (complexObjectInstances == null)
      {
        return null;
      }
    ComplexObjectInstance instance = null;
    for (ComplexObjectInstance current : complexObjectInstances)
      {
        if (current.getComplexObjectTypeID().equals(objectTypeID) && current.getElementID().equals(elementID))
          {
            instance = current;
            break;
          }
      }
    if (instance == null)
      {
        return null;
      }
    Map<String, DataModelFieldValue> values = instance.getFieldValuesReadOnly();
    if (values == null)
      {
        return null;
      }
    DataModelFieldValue elementValue = values.get(subfieldName);
    if (elementValue == null)
      {
        return null;
      }
    return elementValue.getValue();
  }
  // migration end EVPRO-1185
   */
  
  public static Object getComplexObjectLong(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) throws CriterionException
  {
    Long result = null;
    
    //
    //  fieldName = complex.ExampleObjName.subfieldprivateID.subfieldName.complexObjectTypeID
    //
    
    String[] split = fieldName.split("\\.");
    boolean invalidFieldName = split.length != 5 || !split[0].equals("complex");
    if (invalidFieldName)
      {
        throw new CriterionException("field " + fieldName + " can't be handled");
      }
    else
      {
        String complexObjectTypeName = split[1];
        String elementID = (String) subcriteriaVal.get(0);
        String subfieldName = split[3];
        SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
        try
          {
            result = ComplexObjectUtils.getComplexObjectLong(subscriberProfile, complexObjectTypeName, elementID, subfieldName);
          } 
        catch (ComplexObjectException e)
          {
            log.error("ComplexObjectException for {}", fieldName);
          }
      }
    return result;   
  }
  
  public static Object getComplexObjectString(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) throws CriterionException
  {
    String result = null;
    
    //
    //  fieldName = complex.ExampleObjName.subfieldprivateID.subfieldName.complexObjectTypeID
    //
    
    String[] split = fieldName.split("\\.");
    boolean invalidFieldName = split.length != 5 || !split[0].equals("complex");
    if (invalidFieldName)
      {
        throw new CriterionException("field " + fieldName + " can't be handled");
      }
    else
      {
        String complexObjectTypeName = split[1];
        String elementID = (String) subcriteriaVal.get(0);
        String subfieldName = split[3];
        SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
        try
          {
            result = ComplexObjectUtils.getComplexObjectString(subscriberProfile, complexObjectTypeName, elementID, subfieldName);
          } 
        catch (ComplexObjectException e)
          {
            log.error("ComplexObjectException for {}", fieldName);
          }
      }
    return result;   
  }
  
  public static Object getComplexObjectBoolean(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) throws CriterionException
  {
    Boolean result = null;
    
    //
    //  fieldName = complex.ExampleObjName.subfieldprivateID.subfieldName.complexObjectTypeID
    //
    
    String[] split = fieldName.split("\\.");
    boolean invalidFieldName = split.length != 5 || !split[0].equals("complex");
    if (invalidFieldName)
      {
        throw new CriterionException("field " + fieldName + " can't be handled");
      }
    else
      {
        String complexObjectTypeName = split[1];
        String elementID = (String) subcriteriaVal.get(0);
        String subfieldName = split[3];
        SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
        try
          {
            result = ComplexObjectUtils.getComplexObjectBoolean(subscriberProfile, complexObjectTypeName, elementID, subfieldName);
          } 
        catch (ComplexObjectException e)
          {
            log.error("ComplexObjectException for {}", fieldName);
          }
      }
    return result;   
  }
  
  public static Object getComplexObjectDate(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) throws CriterionException
  {
    Date result = null;
    
    //
    //  fieldName = complex.ExampleObjName.subfieldprivateID.subfieldName.complexObjectTypeID
    //
    
    String[] split = fieldName.split("\\.");
    boolean invalidFieldName = split.length != 5 || !split[0].equals("complex");
    if (invalidFieldName)
      {
        throw new CriterionException("field " + fieldName + " can't be handled");
      }
    else
      {
        String complexObjectTypeName = split[1];
        String elementID = (String) subcriteriaVal.get(0);
        String subfieldName = split[3];
        SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
        try
          {
            result = ComplexObjectUtils.getComplexObjectDate(subscriberProfile, complexObjectTypeName, elementID, subfieldName);
          } 
        catch (ComplexObjectException e)
          {
            log.error("ComplexObjectException for {}", fieldName);
          }
      }
    return result;   
  }
  
  public static Object getComplexObjectStringSet(SubscriberEvaluationRequest evaluationRequest, String fieldName, List<Object> subcriteriaVal) throws CriterionException
  {
    Set<String> result = new HashSet<String>();
    
    //
    //  fieldName = complex.ExampleObjName.subfieldprivateID.subfieldName.complexObjectTypeID
    //
    
    String[] split = fieldName.split("\\.");
    boolean invalidFieldName = split.length != 5 || !split[0].equals("complex");
    if (invalidFieldName)
      {
        throw new CriterionException("field " + fieldName + " can't be handled");
      }
    else
      {
        String complexObjectTypeName = split[1];
        String elementID = (String) subcriteriaVal.get(0);
        String subfieldName = split[3];
        SubscriberProfile subscriberProfile = evaluationRequest.getSubscriberProfile();
        try
          {
            List<String> complexResult = ComplexObjectUtils.getComplexObjectStringSet(subscriberProfile, complexObjectTypeName, elementID, subfieldName);
            if (complexResult != null) result.addAll(complexResult);
          } 
        catch (ComplexObjectException e)
          {
            log.error("ComplexObjectException for {}", fieldName);
          }
      }
    return result;   
  }

  /*****************************************
  *
  *   getJourneyResult
  *
  *****************************************/

  public static Object getJourneyResult(SubscriberEvaluationRequest evaluationRequest, String fieldName)
  {
    /*****************************************
    *
    *  awaited journey response
    *
    *****************************************/

    //
    //  journey response?
    //

    boolean isJourneyResponse = true;
    isJourneyResponse = isJourneyResponse && evaluationRequest.getSubscriberStreamEvent() instanceof JourneyRequest;
    isJourneyResponse = isJourneyResponse && ! ((JourneyRequest) evaluationRequest.getSubscriberStreamEvent()).isPending();

    //
    //  awaited journey response
    //

    JourneyRequest journeyResponse = isJourneyResponse ? (JourneyRequest) evaluationRequest.getSubscriberStreamEvent() : null;
    boolean awaitedJourneyResponse = false;
    if (isJourneyResponse)
      {
        JourneyState journeyState = evaluationRequest.getJourneyState();
        String journeyInstanceID = (journeyState != null) ? journeyState.getJourneyInstanceID() : null;
        awaitedJourneyResponse = journeyResponse.getCallingJourneyInstanceID() != null && journeyInstanceID != null && journeyResponse.getCallingJourneyInstanceID().equals(journeyInstanceID);
      }

    /*****************************************
    *
    *  result
    *
    *****************************************/

    if (awaitedJourneyResponse)
      return journeyResponse.getJourneyResults().get(fieldName);
    else
      return null;
  }
  
  

  /*****************************************
  *
  *  evaluateParameter
  *
  *****************************************/

  private static Object evaluateParameter(SubscriberEvaluationRequest evaluationRequest, Object parameterValue)
  {
    Object result = parameterValue;
    if (parameterValue instanceof ParameterExpression)
      {
        ParameterExpression parameterExpression = (ParameterExpression) parameterValue;
        Expression expression = parameterExpression.getExpression();
        TimeUnit baseTimeUnit = parameterExpression.getBaseTimeUnit();
        result = expression.evaluateExpression(evaluationRequest, baseTimeUnit);
      }
    return result;
  }
  
  
  
  /*****************************************
  *
  *  getDay
  *
  *****************************************/
  
  private static String getDay(int today)
  {
    String result = null;
    
    switch(today)
    {
      case Calendar.SUNDAY:
        result = "SUNDAY";
        break;
      case Calendar.MONDAY:
        result = "MONDAY";
        break;
      case Calendar.TUESDAY:
        result = "TUESDAY";
        break;
      case Calendar.WEDNESDAY:
        result = "WEDNESDAY";
        break;
      case Calendar.THURSDAY:
        result = "THURSDAY";
        break;
      case Calendar.FRIDAY:
        result = "FRIDAY";
        break;
      case Calendar.SATURDAY:
        result = "SATURDAY";
        break;
    }
    return result.toLowerCase();
  }
  
  /*****************************************
  *
  *  getMonth
  *
  *****************************************/
  
  private static String getMonth(int today)
  {
    String result = null;
    
    switch(today)
    {
      case Calendar.JANUARY:
        result = "JANUARY";
        break;
      case Calendar.FEBRUARY:
        result = "FEBRUARY";
        break;
      case Calendar.MARCH:
        result = "MARCH";
        break;
      case Calendar.APRIL:
        result = "APRIL";
        break;
      case Calendar.MAY:
        result = "MAY";
        break;
      case Calendar.JUNE:
        result = "JUNE";
        break;
      case Calendar.JULY:
        result = "JULY";
        break;
      case Calendar.AUGUST:
        result = "AUGUST";
        break;
      case Calendar.SEPTEMBER:
        result = "SEPTEMBER";
        break;
      case Calendar.OCTOBER:
        result = "OCTOBER";
        break;
      case Calendar.NOVEMBER:
        result = "NOVEMBER";
        break;
      case Calendar.DECEMBER:
        result = "DECEMBER";
        break;
    }
    return result.toLowerCase();
  }
}
