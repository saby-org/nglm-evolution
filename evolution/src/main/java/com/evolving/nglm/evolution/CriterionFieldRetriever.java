/****************************************************************************
*
*  CriterionFieldRetriever.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;

public abstract class CriterionFieldRetriever
{
  //
  //  system
  //

  public static Object getEvaluationDate(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getEvaluationDate(); }
  public static Object getUnsupportedField(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return null; }

  //
  //  simple
  //
  
  public static Object getEvolutionSubscriberStatus(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return (evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatus() != null) ? evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatus().getExternalRepresentation() : null; }
  public static Object getPreviousEvolutionSubscriberStatus(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getPreviousEvolutionSubscriberStatus().getExternalRepresentation(); }
  public static Object getEvolutionSubscriberStatusChangeDate(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getEvolutionSubscriberStatusChangeDate(); }
  public static Object getUniversalControlGroup(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getUniversalControlGroup(evaluationRequest.getSubscriberGroupEpochReader()); }
  public static Object getLanguage(SubscriberEvaluationRequest evaluationRequest) throws CriterionException { return evaluationRequest.getSubscriberProfile().getLanguage() != null ? evaluationRequest.getSubscriberProfile().getLanguage() : Deployment.getBaseLanguage(); }

  //
  //  subscriber group membership
  //

  public static Object getSubscriberGroups(SubscriberEvaluationRequest evaluationRequest) { return evaluationRequest.getSubscriberProfile().getSubscriberGroups(evaluationRequest.getSubscriberGroupEpochReader()); }
}
