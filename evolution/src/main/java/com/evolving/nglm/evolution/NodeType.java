/*****************************************************************************
*
*  NodeType.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;


import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class NodeType extends DeploymentManagedObject
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  OutputType
  //

  public enum OutputType
  {
    None("none"),
    Static("static"),
    Dynamic("dynamic"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private OutputType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static OutputType fromExternalRepresentation(String externalRepresentation) { for (OutputType enumeratedValue : OutputType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private boolean startNode;
  private boolean endNode;
  private boolean scheduleNode;
  private boolean enableCycle;
  private boolean allowContextVariables;
  private OutputType outputType;
  private LinkedHashMap<String,CriterionField> parameters = new LinkedHashMap<String,CriterionField>();
  private LinkedHashMap<String,CriterionField> outputConnectorParameters = new LinkedHashMap<String,CriterionField>();
  private ActionManager actionManager = null;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public boolean getStartNode() { return startNode; }
  public boolean getEndNode() { return endNode; }
  public boolean getScheduleNode() { return scheduleNode; }
  public boolean getEnableCycle() { return enableCycle; }
  public boolean getAllowContextVariables() { return allowContextVariables; }
  public OutputType getOutputType() { return  outputType; }
  public Map<String,CriterionField> getParameters() { return parameters; }
  public Map<String,CriterionField> getOutputConnectorParameters() { return outputConnectorParameters; }
  public ActionManager getActionManager() { return actionManager; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public NodeType(JSONObject jsonRoot) throws GUIManagerException
  {
    //
    //  super
    //

    super(jsonRoot);

    //
    //  simple
    //

    this.startNode = JSONUtilities.decodeBoolean(jsonRoot, "startNode", Boolean.FALSE);
    this.endNode = JSONUtilities.decodeBoolean(jsonRoot, "endNode", Boolean.FALSE);
    this.scheduleNode = JSONUtilities.decodeBoolean(jsonRoot, "scheduleNode", Boolean.FALSE);
    this.enableCycle = JSONUtilities.decodeBoolean(jsonRoot, "enableCycle", Boolean.FALSE);
    this.allowContextVariables = JSONUtilities.decodeBoolean(jsonRoot, "allowContextVariables", Boolean.FALSE);
    this.outputType = OutputType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "outputType", true));

    //
    //  parameters
    //

    JSONArray parametersJSON = JSONUtilities.decodeJSONArray(jsonRoot, "parameters", true);
    for (int i=0; i<parametersJSON.size(); i++)
      {
        JSONObject parameterJSON = (JSONObject) parametersJSON.get(i);
        CriterionField originalParameter = new CriterionField(parameterJSON);
        CriterionField enhancedParameter = new CriterionField(originalParameter, originalParameter.getID(), "getJourneyNodeParameter", originalParameter.getInternalOnly(), originalParameter.getTagFormat(), originalParameter.getTagMaxLength());
        parameters.put(enhancedParameter.getID(), enhancedParameter);
      }

    //
    //  outputConnectorParameters
    //

    JSONObject dynamicOutputConnector = JSONUtilities.decodeJSONObject(jsonRoot, "dynamicOutputConnector", false);
    if (dynamicOutputConnector != null)
      {
        JSONArray outputConnectorParametersJSON = JSONUtilities.decodeJSONArray(dynamicOutputConnector, "parameters", true);
        for (int i=0; i<outputConnectorParametersJSON.size(); i++)
          {
            JSONObject outputConnectorParameterJSON = (JSONObject) outputConnectorParametersJSON.get(i);
            CriterionField originalParameter = new CriterionField(outputConnectorParameterJSON);
            CriterionField enhancedParameter = new CriterionField(originalParameter, originalParameter.getID(), "getJourneyLinkParameter", originalParameter.getInternalOnly(), originalParameter.getTagFormat(), originalParameter.getTagMaxLength());
            outputConnectorParameters.put(enhancedParameter.getID(), enhancedParameter);
          }
      }

    //
    //  actionManager
    //
        
    JSONObject action = JSONUtilities.decodeJSONObject(jsonRoot, "action", false);
    if (action != null)
      {
        try
          {
            String actionManagerClassName = JSONUtilities.decodeString(action, "actionManagerClass", true);
            Class<ActionManager> actionManagerClass = (Class<ActionManager>) Class.forName(actionManagerClassName);
            Constructor actionManagerConstructor = actionManagerClass.getDeclaredConstructor(JSONObject.class);
            this.actionManager = (ActionManager) actionManagerConstructor.newInstance(action);
          }
        catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e)
          {
            throw new GUIManagerException(e);
          }
      }
  }
}
