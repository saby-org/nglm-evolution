/*****************************************************************************
*
*  NodeType.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

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

  //
  //  ParameterType
  //

  public enum ParameterType
  {
    IntegerParameter("integer"),
    DoubleParameter("double"),
    StringParameter("string"),
    BooleanParameter("boolean"),
    DateParameter("date"),
    EvaluationCriteriaParameter("evaluationCriteria"),
    SMSMessageParameter("smsMessage"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private ParameterType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static ParameterType fromExternalRepresentation(String externalRepresentation) { for (ParameterType enumeratedValue : ParameterType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private OutputType outputType;
  private LinkedHashMap<String,Parameter> parameters = new LinkedHashMap<String,Parameter>();

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public OutputType getOutputType() { return  outputType; }
  public Map<String,Parameter> getParameters() { return parameters; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public NodeType(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
    this.outputType = OutputType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "outputType", true));
    JSONArray parametersJSON = JSONUtilities.decodeJSONArray(jsonRoot, "parameters", true);
    for (int i=0; i<parametersJSON.size(); i++)
      {
        JSONObject parameterJSON = (JSONObject) parametersJSON.get(i);
        Parameter parameter = new Parameter(parameterJSON);
        parameters.put(parameter.getParameterName(), parameter);
      }
  }

  /****************************************************************************
  *
  *  class Parameter
  *
  ****************************************************************************/

  public static class Parameter
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String parameterName;
    private ParameterType parameterType;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getParameterName() { return parameterName; }
    public ParameterType getParameterType() { return parameterType; }

    /*****************************************
    *
    *  constructor (JSON)
    *
    *****************************************/

    public Parameter(JSONObject jsonRoot)
    {
      this.parameterName = JSONUtilities.decodeString(jsonRoot, "parameterName", true);
      this.parameterType = ParameterType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "parameterType", true));
    }
    
    /*****************************************
    *
    *  constructor (simple)
    *
    *****************************************/

    public Parameter(String parameterName, ParameterType parameterType)
    {
      this.parameterName = parameterName;
      this.parameterType = parameterType;
    }
    
    /*****************************************
    *
    *  equals
    *
    *****************************************/

    public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof Parameter)
        {
          Parameter Parameter = (Parameter) obj;
          result = true;
          result = result && Objects.equals(parameterName, Parameter.getParameterName());
        }
      return result;
    }

    /*****************************************
    *
    *  hashCode
    *
    *****************************************/

    public int hashCode()
    {
      return parameterName.hashCode();
    }
  }
}
