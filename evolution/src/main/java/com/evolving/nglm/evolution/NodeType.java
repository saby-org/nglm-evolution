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
  private OutputType outputType;
  private LinkedHashMap<String,CriterionField> parameters = new LinkedHashMap<String,CriterionField>();
  private ActionManager actionManager = null;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public boolean getStartNode() { return startNode; }
  public OutputType getOutputType() { return  outputType; }
  public Map<String,CriterionField> getParameters() { return parameters; }
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
    this.outputType = OutputType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "outputType", true));

    //
    //  parameters
    //

    JSONArray parametersJSON = JSONUtilities.decodeJSONArray(jsonRoot, "parameters", true);
    for (int i=0; i<parametersJSON.size(); i++)
      {
        JSONObject parameterJSON = (JSONObject) parametersJSON.get(i);
        CriterionField originalParameter = new CriterionField(parameterJSON);
        CriterionField enhancedParameter = new CriterionField(originalParameter, originalParameter.getID(), "getJourneyNodeParameter", originalParameter.getInternalOnly());
        parameters.put(enhancedParameter.getID(), enhancedParameter);
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
