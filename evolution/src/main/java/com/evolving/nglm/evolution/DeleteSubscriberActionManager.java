package com.evolving.nglm.evolution;

import java.util.Collections;
import java.util.List;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.AssignSubscriberIDs;
import com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class DeleteSubscriberActionManager extends ActionManager
{

  /*****************************************
  *
  *  data
  *
  *****************************************/

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DeleteSubscriberActionManager(JSONObject configuration) throws GUIManagerException
  {
    super(configuration);
  }

  /*****************************************
  *
  *  execute
  *
  *****************************************/

  @Override public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
  {
    // immediate or normal delete ?
    Boolean immediate = (Boolean) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.isdeleteimmediate");
    
    SubscriberAction subscriberAction = null;
    if(immediate)
      {
        subscriberAction = SubscriberAction.DeleteImmediate;
      }
    else 
      {
        subscriberAction = SubscriberAction.Delete;
      }
    
    AssignSubscriberIDs assignSubscriberIDs = new AssignSubscriberIDs(evolutionEventContext.getSubscriberState().getSubscriberID(), SystemTime.getCurrentTime(), subscriberAction, null, subscriberEvaluationRequest.getTenantID());
    evolutionEventContext.getSubscriberState().getDeleteActions().add(assignSubscriberIDs);
    
    return Collections.emptyList();    
  }
}
