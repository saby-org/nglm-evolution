/*****************************************************************************
*
*  SupportedDataType.java
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

/* IF COPY/PASTE FROM ANOTHER DeploymentManagedObject OBJECT
 * DO NOT FORGET TO ADD IT TO THE DeploymentManagedObject FACTORY
 * SEE DeploymentManagedObject.create */
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
        operators.put(supportedOperator.getID(), supportedOperator);
      }
  }
}
