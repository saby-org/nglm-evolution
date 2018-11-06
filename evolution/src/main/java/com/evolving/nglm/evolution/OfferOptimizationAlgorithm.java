/*****************************************************************************
*
*  OfferOptimizationAlgorithm.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;


import com.evolving.nglm.core.DeploymentManagedObject;

import com.evolving.nglm.core.JSONUtilities;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class OfferOptimizationAlgorithm extends DeploymentManagedObject
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Set<OfferOptimizationAlgorithmParameter> parameters;
  private String javaClass;
  private HashMap<String, String> javaClassParameters;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Set<OfferOptimizationAlgorithmParameter> getParameters() { return parameters; }
  public String getJavaClass() { return javaClass; }
  public HashMap<String, String> getJavaClassParameters() { return javaClassParameters; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public OfferOptimizationAlgorithm(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
    this.parameters = decodeParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", true));
    this.javaClass = JSONUtilities.decodeString(jsonRoot, "javaClass", true);
    this.javaClassParameters = decodeJavaClassParameters(JSONUtilities.decodeJSONObject(jsonRoot, "javaClassParameters"));
  }

  /*****************************************
  *
  *  decodeParameters
  *
  *****************************************/

  private Set<OfferOptimizationAlgorithmParameter> decodeParameters(JSONArray jsonArray)
  {
    Set<OfferOptimizationAlgorithmParameter> result = new HashSet<OfferOptimizationAlgorithmParameter>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new OfferOptimizationAlgorithmParameter((JSONObject) jsonArray.get(i)));
      }
    return result;
  }
  
  /*****************************************
  *
  *  decodeJavaClassParameters
  *
  *****************************************/

  private HashMap<String, String> decodeJavaClassParameters(JSONObject jsonRoot)
  {
    HashMap<String, String> result = new HashMap<>();
    for (Object keyObject : jsonRoot.keySet())
      {
        String key = (String)keyObject;
        String value = (String)jsonRoot.get(key);
        result.put(key, value);
      }
    return result;
  }

  /****************************************************************************
  *
  *  class OfferOptimizationAlgorithmParameter
  *
  ****************************************************************************/

  public static class OfferOptimizationAlgorithmParameter
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String name;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getParameterName() { return getName(); }
    public String getName() { return name; }

    /*****************************************
    *
    *  constructor (JSON)
    *
    *****************************************/

    public OfferOptimizationAlgorithmParameter(JSONObject jsonRoot)
    {
      this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
    }
    
    /*****************************************
    *
    *  constructor (simple)
    *
    *****************************************/

    public OfferOptimizationAlgorithmParameter(String name)
    {
      this.name = name;
    }
    
    /*****************************************
    *
    *  equals
    *
    *****************************************/

    public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof OfferOptimizationAlgorithmParameter)
        {
          OfferOptimizationAlgorithmParameter offerOptimizationAlgorithmParameter = (OfferOptimizationAlgorithmParameter) obj;
          result = true;
          result = result && Objects.equals(name, offerOptimizationAlgorithmParameter.getName());
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
      return name.hashCode();
    }
  }
}
