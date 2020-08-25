/****************************************************************************
*
*  CriterionFieldRetriever.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.LoyaltyProgramHistory.TierHistory;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

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
  *  simple
  *
  *****************************************/

  //
  //  system
  //

  public static Object getEvaluationDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getEvaluationDate(); }
  public static Object getEvaluationAniversary(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return getEvaluationDate(evaluationRequest, fieldName);}
  public static Object getJourneyEvaluationEventName(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return (evaluationRequest.getSubscriberStreamEvent() != null && evaluationRequest.getSubscriberStreamEvent() instanceof EvolutionEngineEvent) ? ((EvolutionEngineEvent) evaluationRequest.getSubscriberStreamEvent()).getEventName() : null; } 
  public static Object getRandom100(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return ThreadLocalRandom.current().nextInt(100); }
  public static Object getTrue(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return Boolean.TRUE; }
  public static Object getFalse(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return Boolean.FALSE; }
  public static Object getUnsupportedField(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return null; }
  public static Object getEvaluationWeekDay(SubscriberEvaluationRequest evaluationRequest, String fieldName) { int weekDay = RLMDateUtils.getField(evaluationRequest.getEvaluationDate(), Calendar.DAY_OF_WEEK, Deployment.getBaseTimeZone()); Set<String> evaluationWeekDay = new HashSet<String>(); evaluationWeekDay.add(getDay(weekDay)); return evaluationWeekDay; }
  public static Object getEvaluationMonth(SubscriberEvaluationRequest evaluationRequest, String fieldName) { int month = RLMDateUtils.getField(evaluationRequest.getEvaluationDate(), Calendar.MONTH, Deployment.getBaseTimeZone()); Set<String> evaluationMonth = new HashSet<String>(); evaluationMonth.add(getMonth(month)); return evaluationMonth; }
  public static Object getEvaluationDayOfMonth(SubscriberEvaluationRequest evaluationRequest, String fieldName) {  int dayOfMonth = RLMDateUtils.getField(evaluationRequest.getEvaluationDate(), Calendar.DAY_OF_MONTH, Deployment.getBaseTimeZone()); return dayOfMonth; }
  public static Object getEvaluationTime(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
  { 
    Date evaluationDate = evaluationRequest.getEvaluationDate();
    StringBuilder evaluationTimeBuilder = new StringBuilder();
    evaluationTimeBuilder.append(RLMDateUtils.getField(evaluationDate, Calendar.HOUR_OF_DAY, com.evolving.nglm.core.Deployment.getBaseTimeZone())).append(":");
    evaluationTimeBuilder.append(RLMDateUtils.getField(evaluationDate, Calendar.MINUTE, com.evolving.nglm.core.Deployment.getBaseTimeZone())).append(":");
    evaluationTimeBuilder.append(RLMDateUtils.getField(evaluationDate, Calendar.SECOND, com.evolving.nglm.core.Deployment.getBaseTimeZone()));
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

    return evaluateParameter(evaluationRequest, parameterMap.get(fieldName)); 
    }
  
  //
  //  simple
  //
  
  public static Object getEvolutionSubscriberStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return (evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatus() != null) ? evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatus().getExternalRepresentation() : null; }
  public static Object getPreviousEvolutionSubscriberStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return (evaluationRequest.getSubscriberProfile().getPreviousEvolutionSubscriberStatus() != null) ? evaluationRequest.getSubscriberProfile().getPreviousEvolutionSubscriberStatus().getExternalRepresentation() : null; }
  public static Object getEvolutionSubscriberStatusChangeDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatusChangeDate(); }
  public static Object getUniversalControlGroup(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getUniversalControlGroup(); }
  public static Object getLanguageID(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getLanguageID() != null ? evaluationRequest.getSubscriberProfile().getLanguageID() : Deployment.getBaseLanguageID(); }

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
    ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent)(evaluationRequest.getSubscriberStreamEvent());
    if(event.getInfos().get(LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.ENTERING.getExternalRepresentation()) != null)
      {
        return LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.ENTERING.name();
      }
    else 
      {
        return event.getInfos().get(LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.OLD_TIER.getExternalRepresentation());
      }
  }
  
  public static Object getProfilePointLoyaltyProgramChangeTierNewValue(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
    {
      ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent)(evaluationRequest.getSubscriberStreamEvent());
      if(event.getInfos().get(LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.LEAVING.getExternalRepresentation()) != null)
        {
          return LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.LEAVING.name();
        }
      else 
        {
          return event.getInfos().get(LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.NEW_TIER.getExternalRepresentation());
        }
    }
  
  public static Object getProfilePointLoyaltyProgramUpdated(SubscriberEvaluationRequest evaluationRequest, String fieldName) 
    {
      ProfileLoyaltyProgramChangeEvent event = (ProfileLoyaltyProgramChangeEvent)(evaluationRequest.getSubscriberStreamEvent());
      return event.getLoyaltyProgramID().equals(fieldName.substring(LoyaltyProgramPoints.CRITERION_FIELD_NAME_IS_UPDATED_PREFIX.length()));
    }

  //
  //  TokenRedeemed
  //
  
  public static Object getTokenType(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((TokenRedeemed) evaluationRequest.getSubscriberStreamEvent()).getTokenType(); }
  public static Object getAcceptedOfferId(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((TokenRedeemed) evaluationRequest.getSubscriberStreamEvent()).getAcceptedOfferId(); }    

  //
  //  BonusDelivery
  //

  public static Object getModuleId_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryModuleId(); }
  public static Object getFeatureId_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryFeatureId(); }    
  public static Object getReturnCode_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryReturnCode(); }
  public static Object getDeliveryStatus_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryDeliveryStatus() != null ? ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryDeliveryStatus().getExternalRepresentation() : null; }
  public static Object getReturnCodeDetails_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryReturnCodeDetails(); }
  public static Object getOrigin_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryOrigin(); }    
  public static Object getProviderId_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryProviderId(); }
  public static Object getDeliverableId_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryDeliverableId(); }    
  public static Object getDeliverableName_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryDeliverableName(); }    
  public static Object getDeliverableQty_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryDeliverableQty(); }
  public static Object getDeliverableExpiration_BonusDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((BonusDelivery) evaluationRequest.getSubscriberStreamEvent()).getBonusDeliveryDeliverableExpiration(); }    
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

    //
    //  loyaltyProgramState
    //

    LoyaltyProgramState loyaltyProgramState = evaluationRequest.getSubscriberProfile().getLoyaltyPrograms().get(loyaltyProgramID);
    //
    //  opted out previously?
    //

    if (loyaltyProgramState != null && loyaltyProgramState.getLoyaltyProgramExitDate() != null)
      {
        loyaltyProgramState = null;
      }

    //
    //  in program?
    //

    if (loyaltyProgramState == null) return null;
    if (! (loyaltyProgramState instanceof LoyaltyProgramPointsState)) return null;

    //
    //  retrieve
    //
    LoyaltyProgramPointsState loyaltyProgramPointsState = (LoyaltyProgramPointsState) loyaltyProgramState;
    Object result = null;
    
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
          
        case "tierUpdateDate":
          result = loyaltyProgramPointsState.getTierEnrollmentDate();
          break;
          
        case "optInDate":
          result = loyaltyProgramPointsState.getLoyaltyProgramEnrollmentDate();
          break;
          
        case "optOutDate":
          result = loyaltyProgramPointsState.getLoyaltyProgramExitDate();
          break;        
         
          
        default:
          fieldNamePattern = Pattern.compile("^([^.]+)\\.([^.]+)\\.([^.]+)$");
          fieldNameMatcher = fieldNamePattern.matcher(criterionFieldBaseName);
          if (! fieldNameMatcher.find()) throw new CriterionException("invalid criterionFieldBaseName field " + criterionFieldBaseName);
          String pointName = fieldNameMatcher.group(1);
          String pointID = fieldNameMatcher.group(2);
          String request = fieldNameMatcher.group(3);
          Date evaluationDate = evaluationRequest.getEvaluationDate();
          Map<String, PointBalance> pointBalances = evaluationRequest.getSubscriberProfile().getPointBalances();
          
          Date earliestExpiration = evaluationDate;
          int earliestExpiryQuantity = 0;
          
          if (pointBalances == null)
            {
              log.info("invalid point balances, using default value");
            }
          else
            {
              PointBalance pointBalance = pointBalances.get(pointID);
              if (pointBalance == null)
                {
                  log.info("invalid point balance, using default value");
                }
              else
                {
                  earliestExpiration = pointBalance.getFirstExpirationDate(evaluationDate);
                  earliestExpiryQuantity = pointBalance.getBalance(earliestExpiration);
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
              throw new CriterionException("invalid request " + criterionFieldBaseName + " " + request);
          }
          
      }

    //
    //  return
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
    
    Object result = null;
    Date evaluationDate = evaluationRequest.getEvaluationDate();
    Date earliestExpiration = evaluationDate;
    int earliestExpiryQuantity = 0;
    Integer balance = 0;
    Map<String, PointBalance> pointBalances = evaluationRequest.getSubscriberProfile().getPointBalances();
    if (pointBalances == null)
      {
        log.info("invalid point balances, using default value");
      }
    else
      {
        PointBalance pointBalance = pointBalances.get(pointID);
        if (pointBalance == null)
          {
            log.info("invalid point balance, using default value");
          }
        else
          {
            balance = new Integer(pointBalance.getBalance(evaluationDate));
            earliestExpiration = pointBalance.getFirstExpirationDate(evaluationDate);
            earliestExpiryQuantity = pointBalance.getBalance(earliestExpiration);
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
            throw new CriterionException("invalid criterionFieldBaseName " + criterionFieldBaseName);
        }

    //
    //  return
    //

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

  public static Object getEvaluationJourney(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return "evaluation.variable.journey"; }

  //
  //  getEvaluationJourneyStatus
  //
  
  public static Object getEvaluationJourneyStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException
  {
    String journeyID = (String) evaluationRequest.getEvaluationVariables().get("evaluation.variable.journey");
    if (journeyID == null) throw new CriterionException("invalid journey status request");
    return (evaluationRequest.getSubscriberProfile().getSubscriberJourneys().get(journeyID) != null) ? evaluationRequest.getSubscriberProfile().getSubscriberJourneys().get(journeyID).getExternalRepresentation() : null;
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
