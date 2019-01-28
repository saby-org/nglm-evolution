/*****************************************************************************
*
*  ActionManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;

import org.json.simple.JSONObject;

public abstract class ActionManager
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum ActionType
  {
    DeliveryRequest,
    JourneyRequest;
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  protected ActionManager(JSONObject configuration)  { }

  /*****************************************
  *
  *  executeOnEntry
  *
  *****************************************/

  public Action executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
  {
    //
    //  default implementation (empty)
    //

    return null;
  }

  /*****************************************
  *
  *  executeOnExit
  *
  *****************************************/

  public void executeOnExit(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest, JourneyLink journeyLink)
  {
    //
    //  default implementation (empty)
    //
  }

  /*****************************************
  *
  *  interface Action
  *
  *****************************************/

  public interface Action
  {
    public ActionType getActionType();
  }
}
