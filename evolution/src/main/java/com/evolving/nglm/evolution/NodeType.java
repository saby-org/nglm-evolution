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

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public boolean getStartNode() { return startNode; }
  public OutputType getOutputType() { return  outputType; }
  public Map<String,CriterionField> getParameters() { return parameters; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public NodeType(JSONObject jsonRoot) throws GUIManagerException
  {
    super(jsonRoot);
    this.startNode = JSONUtilities.decodeBoolean(jsonRoot, "startNode", Boolean.FALSE);
    this.outputType = OutputType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "outputType", true));
    JSONArray parametersJSON = JSONUtilities.decodeJSONArray(jsonRoot, "parameters", true);
    for (int i=0; i<parametersJSON.size(); i++)
      {
        JSONObject parameterJSON = (JSONObject) parametersJSON.get(i);
        CriterionField originalParameter = new CriterionField(parameterJSON);
        CriterionField enhancedParameter = new CriterionField(originalParameter, originalParameter.getID(), "getJourneyNodeParameter");
        parameters.put(enhancedParameter.getID(), enhancedParameter);
      }
  }
}
