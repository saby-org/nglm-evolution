/*****************************************************************************
*
*  DNBOUtils.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class DNBOUtils
{

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DNBOUtils.class);


  //
  //  ChoiceField
  //

  public enum ChoiceField
  {
    ManualAllocation("manualAllocation", "journey.status.notified"),
    AutomaticAllocation("automaticAllocation", "journey.status.converted"),
    AutomaticRedeem("automaticRedeem", "journey.status.controlgroup"),
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

  public static class Action extends ActionManager
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public Action(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
    }
        
    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public DeliveryRequest executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      ParameterMap map = subscriberEvaluationRequest.getJourneyNode().getNodeParameters();

      String field = "node.parameter.presentationStrategy";
      String presentationStrategy = map.containsKey(field) ? (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,field) : null;
      if (presentationStrategy == null) throw new ServerRuntimeException("No Presentation Strategy");
      if (log.isTraceEnabled()) log.trace("presentationStrategy : "+presentationStrategy);

      field = "node.parameter.tokenType";
      String tokenTypeId = map.containsKey(field) ? (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,field) : null;
      if (tokenTypeId == null) throw new ServerRuntimeException("No Token Type Id");
      String codeFormat = tokenTypeId; // the ID passed by the gui is actually the regexp
      if (log.isTraceEnabled()) log.trace("codeFormat : "+codeFormat);

      field = "node.parameter.choice";
      String choiceFieldStr = (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,field);
      ChoiceField choiceField = map.containsKey(field) ? ChoiceField.fromExternalRepresentation(choiceFieldStr) : null;
      if (choiceField == null) throw new ServerRuntimeException("No choice field");
      if (choiceField == ChoiceField.Unknown) throw new ServerRuntimeException("Unknown choice field: " + choiceFieldStr);
      if (log.isTraceEnabled()) log.trace("choiceField : "+choiceField.getExternalRepresentation());

      switch (choiceField)
      {
        case ManualAllocation :
          {
            String tokenCode = TokenUtils.generateFromRegex(codeFormat);
            log.trace("We generated "+tokenCode);
            // TODO set tokenCode in subscriber profile
            break;
          }
        case AutomaticAllocation :
        case AutomaticRedeem :
          {
            // TODO do these cases
            break;
          }
          default :
            {
              throw new ServerRuntimeException("unknown choice field: " + choiceField.getExternalRepresentation());        
            }
      }
      return null;
    }
  }

}