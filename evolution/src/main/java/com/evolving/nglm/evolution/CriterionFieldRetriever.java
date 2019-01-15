/****************************************************************************
*
*  CriterionFieldRetriever.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;

import java.util.Random;

public abstract class CriterionFieldRetriever
{
  /*****************************************
  *
  *  support
  *
  *****************************************/

  private static Random random = new Random();

  /*****************************************
  *
  *  simle
  *
  *****************************************/

  //
  //  system
  //

  public static Object getEvaluationDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return evaluationRequest.getEvaluationDate(); }
  public static Object getJourneyEvaluationEventName(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return (evaluationRequest.getSubscriberStreamEvent() != null && evaluationRequest.getSubscriberStreamEvent() instanceof EvolutionEngineEvent) ? ((EvolutionEngineEvent) evaluationRequest.getSubscriberStreamEvent()).getEventName() : null; } 
  public static Object getRandom100(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return random.nextInt(100); }
  public static Object getUnsupportedField(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return null; }

  //
  //  journey
  //

  public static Object getJourneyEntryDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return evaluationRequest.getJourneyState().getJourneyEntryDate(); }
  public static Object getJourneyMetric(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return evaluationRequest.getJourneyState().getJourneyMetrics().containsKey(fieldName) ? evaluationRequest.getJourneyState().getJourneyMetrics().get(fieldName) : new Integer(0); }
  public static Object getJourneyParameter(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return evaluationRequest.getJourneyState().getJourneyParameters().get(fieldName); }
  public static Object getJourneyNodeEntryDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return evaluationRequest.getJourneyState().getJourneyNodeEntryDate(); }
  public static Object getJourneyNodeParameter(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return evaluationRequest.getJourneyNode().getNodeParameters().get(fieldName); }
  public static Object getJourneyLinkParameter(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return evaluationRequest.getJourneyLink().getLinkParameters().get(fieldName); }

  //
  //  simple
  //
  
  public static Object getEvolutionSubscriberStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return (evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatus() != null) ? evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatus().getExternalRepresentation() : null; }
  public static Object getPreviousEvolutionSubscriberStatus(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return evaluationRequest.getSubscriberProfile().getPreviousEvolutionSubscriberStatus().getExternalRepresentation(); }
  public static Object getEvolutionSubscriberStatusChangeDate(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatusChangeDate(); }
  public static Object getUniversalControlGroup(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return evaluationRequest.getSubscriberProfile().getUniversalControlGroup(evaluationRequest.getSubscriberGroupEpochReader()); }
  public static Object getLanguage(SubscriberEvaluationRequest evaluationRequest, String fieldName) throws CriterionException { return evaluationRequest.getSubscriberProfile().getLanguage() != null ? evaluationRequest.getSubscriberProfile().getLanguage() : Deployment.getBaseLanguage(); }

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
    return null;
  }
}
