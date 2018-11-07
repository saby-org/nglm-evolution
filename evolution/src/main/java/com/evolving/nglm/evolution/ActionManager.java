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
  *  constructor
  *
  *****************************************/

  protected ActionManager(JSONObject configuration)  { }

  /*****************************************
  *
  *  executeOnEntry
  *
  *****************************************/

  public DeliveryRequest executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
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
}
