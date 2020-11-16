/*****************************************************************************
*
*  ActionManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

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
    ExecuteActionOtherSubscriber,
    JourneyRequest,
    JourneyContextUpdate,
    ActionManagerContextUpdate,
    TokenUpdate,
    TokenChange,
    TriggerEvent;
  }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Map<String,CriterionField> outputAttributes;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  protected ActionManager(JSONObject configuration) throws GUIManagerException
  {
    this.outputAttributes = decodeOutputAttributes(JSONUtilities.decodeJSONArray(configuration, "outputAttributes", new JSONArray()));
  }

  /*****************************************
  *
  *  decodeOutputAttributes
  *
  *****************************************/

  public static Map<String,CriterionField> decodeOutputAttributes(JSONArray jsonArray) throws GUIManagerException
  {
    Map<String,CriterionField> outputAttributes = new LinkedHashMap<String,CriterionField>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject outputAttributeJSON = (JSONObject) jsonArray.get(i);
        CriterionField outputAttribute = new CriterionField(outputAttributeJSON);
        outputAttributes.put(outputAttribute.getID(), outputAttribute);
      }
    return outputAttributes;
  }

  /*****************************************
  *
  *  executeOnEntry
  *
  *****************************************/

  public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
  {
    //
    //  default implementation (empty)
    //

    return Collections.<Action>emptyList();
  }

  /*****************************************
  *
  *  executeOnExit
  *
  *****************************************/

  public List<Action> executeOnExit(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest, JourneyLink journeyLink)
  {
    //
    //  default implementation (empty)
    //

    return Collections.<Action>emptyList();
  }

  /*****************************************
  *
  *  getOutputAttributes
  *
  *****************************************/

  public Map<String,CriterionField> getOutputAttributes() { return outputAttributes; }

  /*****************************************
  *
  *  interface Action
  *
  *****************************************/

  public interface Action
  {
    public ActionType getActionType();
  }
  
  
  /*****************************************
  *
  *  utils
  *
  *****************************************/
  
  public static String extractWorkflowFeatureID(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest, String deliveryRequestSource)
  {
    Journey sourceJourney = (Journey) evolutionEventContext.getJourneyService().getStoredJourney(deliveryRequestSource);
    if(sourceJourney != null && (sourceJourney.getGUIManagedObjectType() == GUIManagedObjectType.Workflow || sourceJourney.getGUIManagedObjectType() == GUIManagedObjectType.LoyaltyWorkflow))
      {
        if(subscriberEvaluationRequest.getJourneyState() != null && subscriberEvaluationRequest.getJourneyState().getsourceFeatureID() != null)
          {
            deliveryRequestSource = subscriberEvaluationRequest.getJourneyState().getsourceFeatureID();
          }
        
      }
    return deliveryRequestSource;
  }
  
  public Map<String, String> getGUIDependencies(JourneyNode journeyNode)
  {
    return new HashMap<String, String>();
  }

}
