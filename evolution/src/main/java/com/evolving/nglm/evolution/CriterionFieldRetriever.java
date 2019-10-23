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

import java.util.concurrent.ThreadLocalRandom;

public abstract class CriterionFieldRetriever
{
  /*****************************************
  *
  *  simle
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
  //  for profileChange old and new value access by type.
  //
  
  public static Object getProfileSegmentChangeDimensionOldValue(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return ((ProfileSegmentChangeEvent)(evaluationRequest.getSubscriberStreamEvent())).getOldSegment(fieldName); }
  public static Object getProfileSegmentChangeDimensionNewValue(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return ((ProfileSegmentChangeEvent)(evaluationRequest.getSubscriberStreamEvent())).getNewSegment(fieldName); }
  public static Object getProfileSegmentChangeDimensionUpdated(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return ((ProfileSegmentChangeEvent)(evaluationRequest.getSubscriberStreamEvent())).isDimensionUpdated(fieldName); }

  
  
  /*****************************************
  *
  *  complex
  *
  *****************************************/

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
