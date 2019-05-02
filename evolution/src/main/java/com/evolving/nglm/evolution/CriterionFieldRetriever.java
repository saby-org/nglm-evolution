/****************************************************************************
*
*  CriterionFieldRetriever.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.EvaluationCriterion.TimeUnit;
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
  public static Object getUnsupportedField(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return null; }

  //
  //  journey
  //

  public static Object getJourneyEntryDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getJourneyState().getJourneyEntryDate(); }
  public static Object getJourneyParameter(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluateParameter(evaluationRequest, evaluationRequest.getJourneyState().getJourneyParameters().get(fieldName)); }
  public static Object getJourneyNodeEntryDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getJourneyState().getJourneyNodeEntryDate(); }
  public static Object getJourneyNodeParameter(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluateParameter(evaluationRequest, evaluationRequest.getJourneyNode().getNodeParameters().get(fieldName)); }
  public static Object getJourneyLinkParameter(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluateParameter(evaluationRequest, evaluationRequest.getJourneyLink().getLinkParameters().get(fieldName)); }
  
  //
  //  simple
  //
  
  public static Object getEvolutionSubscriberStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return (evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatus() != null) ? evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatus().getExternalRepresentation() : null; }
  public static Object getPreviousEvolutionSubscriberStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return (evaluationRequest.getSubscriberProfile().getPreviousEvolutionSubscriberStatus() != null) ? evaluationRequest.getSubscriberProfile().getPreviousEvolutionSubscriberStatus().getExternalRepresentation() : null; }
  public static Object getEvolutionSubscriberStatusChangeDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatusChangeDate(); }
  public static Object getUniversalControlGroup(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getUniversalControlGroup(); }
  public static Object getLanguage(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getLanguage() != null ? evaluationRequest.getSubscriberProfile().getLanguage() : Deployment.getBaseLanguage(); }

  //
  //  subscriber group membership
  //

  public static Object getSubscriberGroups(SubscriberEvaluationRequest evaluationRequest, String fieldName) { return evaluationRequest.getSubscriberProfile().getSubscriberGroups(evaluationRequest.getSubscriberGroupEpochReader()); }

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
      return SubscriberJourneyStatus.Eligible.getExternalRepresentation();
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
