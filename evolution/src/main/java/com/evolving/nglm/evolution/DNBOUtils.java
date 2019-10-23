/*****************************************************************************
*
*  DNBOUtils.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.ContextUpdate;

public class DNBOUtils
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DNBOUtils.class);
  
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum ChoiceField
  {
    ManualAllocation("manualAllocation", "journey.status.manual"),
    AutomaticAllocation("automaticAllocation", "journey.status.automaticallocation"),
    AutomaticRedeem("automaticRedeem", "journey.status.automaticredeem"),
    Unknown("(unknown)", "(unknown)");
    private String externalRepresentation;
    private String choiceParameterName;
    private ChoiceField(String externalRepresentation, String choiceParameterName) { this.externalRepresentation = externalRepresentation; this.choiceParameterName = choiceParameterName; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getChoiceParameterName() { return choiceParameterName; }
    public static ChoiceField fromExternalRepresentation(String externalRepresentation) { for (ChoiceField enumeratedValue : ChoiceField.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
  /*****************************************
  *
  *  class AllocationAction
  *
  *****************************************/

  public static class ActionManager extends com.evolving.nglm.evolution.ActionManager
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ActionManager(JSONObject configuration) throws GUIManagerException
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
      /*****************************************
      *
      *  parameters
      *
      *****************************************/

      String scoringStrategyID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest, "node.parameter.strategy");
      String tokenTypeID = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest, "node.parameter.tokentype"); 
      ChoiceField choiceField = ChoiceField.fromExternalRepresentation((String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.choice"));
      
      /*****************************************
      *
      *  scoring strategy
      *
      *****************************************/

      ScoringStrategy scoringStrategy = evolutionEventContext.getScoringStrategyService().getActiveScoringStrategy(scoringStrategyID, evolutionEventContext.now());
      if (scoringStrategy == null)
        {
          log.error("invalid scoring strategy {}", scoringStrategyID);
          return Collections.<Action>emptyList();
        }

      /*****************************************
      *
      *  token type
      *
      *****************************************/

      TokenType tokenType = evolutionEventContext.getTokenTypeService().getActiveTokenType(tokenTypeID, evolutionEventContext.now());
      if (tokenType == null)
        {
          log.error("no token type {}", tokenTypeID);
          return Collections.<Action>emptyList();
        }
        
      /*****************************************
      *
      *  action -- generate token
      *
      *****************************************/

      DNBOToken token = new DNBOToken(TokenUtils.generateFromRegex(tokenType.getCodeFormat()), subscriberEvaluationRequest.getSubscriberProfile().getSubscriberID(), tokenType);
      token.setScoringStrategyIDs(Collections.<String>singletonList(scoringStrategy.getScoringStrategyID()));
      token.setCreationDate(evolutionEventContext.now());

      /*****************************************
      *
      *  action -- token code
      *
      *****************************************/

      ContextUpdate tokenCodeUpdate = new ContextUpdate(ActionType.ActionManagerContextUpdate);
      tokenCodeUpdate.getParameters().put("action.token.code", token.getTokenCode());

      /*****************************************
      *
      *  return
      *
      *****************************************/

      List<Action> result = new ArrayList<Action>();
      result.add(token);
      result.add(tokenCodeUpdate);
      return result;
    }
  }
}
