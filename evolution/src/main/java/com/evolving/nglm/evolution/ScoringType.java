/*****************************************************************************
*
*  ScoringType.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;


import org.json.simple.JSONObject;

import com.evolving.nglm.core.DeploymentManagedObject;

public class ScoringType extends DeploymentManagedObject
{

  /*****************************************
  *
  *  data
  *
  *****************************************/

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ScoringType(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
  }

}
