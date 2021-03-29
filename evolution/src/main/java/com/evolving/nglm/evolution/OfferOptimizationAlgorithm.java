/*****************************************************************************
*
*  OfferOptimizationAlgorithm.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.DeploymentManagedObject;
import com.evolving.nglm.core.JSONUtilities;

/* IF COPY/PASTE FROM ANOTHER DeploymentManagedObject OBJECT
 * DO NOT FORGET TO ADD IT TO THE DeploymentManagedObject FACTORY
 * SEE DeploymentManagedObject.create */
public class OfferOptimizationAlgorithm extends DeploymentManagedObject
{
  @Override
  public String toString() {
    String s1 = "", s2="";
    for (OfferOptimizationAlgorithmParameter ooap : parameters)
      s1+="{"+ooap+"}";

    return "OfferOptimizationAlgorithm [parameters=" + s1 + ", javaClass=" + javaClass
        + ", Parameters=" + s2 + "]";
  }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Set<OfferOptimizationAlgorithmParameter> parameters;
  private String javaClass;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Set<OfferOptimizationAlgorithmParameter> getParameters() { return parameters; }
  public String getJavaClass() { return javaClass; }
  
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

  /****************************************************************************
  *
  *  class OfferOptimizationAlgorithmParameter
  *
  ****************************************************************************/

  public static class OfferOptimizationAlgorithmParameter
  {
    @Override
    public String toString() {
      return "OfferOptimizationAlgorithmParameter [name=" + name + "]";
    }

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
