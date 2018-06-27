/*****************************************************************************
*
*  SupportedDataType.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class SupportedDataType extends DeploymentManagedObject
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Map<String, SupportedOperator> operators = new LinkedHashMap<String, SupportedOperator>();

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getCriterionDataTypeName() { return getName(); }
  public Map<String, SupportedOperator> getOperators() { return operators; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SupportedDataType(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
    JSONArray operatorsJSON = JSONUtilities.decodeJSONArray(jsonRoot, "operators", true);
    for (int i=0; i<operatorsJSON.size(); i++)
      {
        JSONObject supportedOperatorJSON = (JSONObject) operatorsJSON.get(i);
        SupportedOperator supportedOperator = new SupportedOperator(supportedOperatorJSON);
        operators.put(supportedOperator.getOperatorName(), supportedOperator);
      }
  }
}
