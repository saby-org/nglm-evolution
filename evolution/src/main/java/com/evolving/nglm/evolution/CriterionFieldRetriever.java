/****************************************************************************
*
*  CriterionFieldRetriever.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public abstract class CriterionFieldRetriever
{
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
  public static Object getRandom100(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return ThreadLocalRandom.current().nextInt(100); }
  public static Object getFalse(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return Boolean.FALSE; }
  public static Object getUnsupportedField(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return null; }

  //
  //  journey
  //

  public static Object getJourneyEntryDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getJourneyState().getJourneyEntryDate(); }
  public static Object getJourneyParameter(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluateParameter(evaluationRequest, evaluationRequest.getJourneyState().getJourneyParameters().get(fieldName)); }
  public static Object getJourneyNodeEntryDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getJourneyState().getJourneyNodeEntryDate(); }
  public static Object getJourneyNodeParameter(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluateParameter(evaluationRequest, evaluationRequest.getJourneyNode().getNodeParameters().get(fieldName)); }
  public static Object getJourneyLinkParameter(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluateParameter(evaluationRequest, evaluationRequest.getJourneyLink().getLinkParameters().get(fieldName)); }
  public static Object getActionAttribute(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluateParameter(evaluationRequest, evaluationRequest.getJourneyState().getJourneyActionManagerContext().get(fieldName)); }

  //
  //  subscriberMessages
  //

  public static Object getSubscriberMessageParameterTag(SubscriberEvaluationRequest evaluationRequest, String fieldName) {  return evaluateParameter(evaluationRequest, ((SubscriberMessage) CriterionFieldRetriever.getJourneyNodeParameter(evaluationRequest,"node.parameter.message")).getParameterTags().get(fieldName)); }
  
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
  public static Object getOfferId_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryOfferId(); }
  public static Object getOfferQty_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryOfferQty(); }    
  public static Object getSalesChannelId_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliverySalesChannelId(); }
  public static Object getOfferPrice_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryOfferPrice(); }    
  public static Object getMeanOfPayment_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryMeanOfPayment(); }
  public static Object getOfferStock_OfferDelivery(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return ((OfferDelivery) evaluationRequest.getSubscriberStreamEvent()).getOfferDeliveryOfferStock(); }
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
  // getLoyaltyPrograms
  //
  
  public static Object getLoyaltyPrograms(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException {
    Set<String> res = new HashSet<>();
    for (LoyaltyProgramState lps : evaluationRequest.getSubscriberProfile().getLoyaltyPrograms().values())
      {
        res.add(lps.getLoyaltyProgramName());
      }
    return res;
  }

  //
  // getLoyaltyProgramTier
  //

  public static Object getLoyaltyProgramTier(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException {
    String res = "";
    String programID = fieldName.substring("loyaltyprogram.".length(), fieldName.length()-".tier".length()); // "loyaltyprogram."+loyaltyProgramName+".tier"
    for (LoyaltyProgramState lps : evaluationRequest.getSubscriberProfile().getLoyaltyPrograms().values())
      {
        if (lps.getLoyaltyProgramID() != null)
          {
          if (lps.getLoyaltyProgramID().equals(programID))
              {
                if (lps instanceof LoyaltyProgramPointsState)
                  {
                    res = ((LoyaltyProgramPointsState) lps).getTierName();
                    break;
                  }
              }
          }
      }
    return res;
  }
  
  
  //
  //  getJourneyActionDeliveryStatus
  //

  public static Object getJourneyActionDeliveryStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException
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
    *  awaited journey entry?
    *
    *****************************************/

    boolean awaitedJourneyEntry = true;
    awaitedJourneyEntry = awaitedJourneyEntry && evaluationRequest.getJourneyState().getJourneyOutstandingJourneyRequestID() != null;
    awaitedJourneyEntry = awaitedJourneyEntry && evaluationRequest.getSubscriberStreamEvent() instanceof JourneyRequest;
    awaitedJourneyEntry = awaitedJourneyEntry && ((JourneyRequest) evaluationRequest.getSubscriberStreamEvent()).getJourneyRequestID().equals(evaluationRequest.getJourneyState().getJourneyOutstandingJourneyRequestID());

    /*****************************************
    *
    *  awaited journey transition?
    *
    *****************************************/

    boolean awaitedJourneyTransition = true;
    awaitedJourneyTransition = awaitedJourneyTransition && evaluationRequest.getJourneyState().getJourneyOutstandingJourneyInstanceID() != null;
    awaitedJourneyTransition = awaitedJourneyTransition && evaluationRequest.getSubscriberStreamEvent() instanceof JourneyStatistic;
    awaitedJourneyTransition = awaitedJourneyTransition && ((JourneyStatistic) evaluationRequest.getSubscriberStreamEvent()).getJourneyInstanceID().equals(evaluationRequest.getJourneyState().getJourneyOutstandingJourneyInstanceID());

    /*****************************************
    *
    *  result
    *
    *****************************************/

    if (awaitedJourneyEntry && ! ((JourneyRequest) evaluationRequest.getSubscriberStreamEvent()).getEligible())
      return SubscriberJourneyStatus.NotEligible.getExternalRepresentation();
    else if (awaitedJourneyEntry && ((JourneyRequest) evaluationRequest.getSubscriberStreamEvent()).getEligible())
      return SubscriberJourneyStatus.Entered.getExternalRepresentation();
    else if (awaitedJourneyTransition)
      return ((JourneyStatistic) evaluationRequest.getSubscriberStreamEvent()).getSubscriberJourneyStatus().getExternalRepresentation();
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
        result = expression.evaluate(evaluationRequest, baseTimeUnit);
      }
    return result;
  }
}
