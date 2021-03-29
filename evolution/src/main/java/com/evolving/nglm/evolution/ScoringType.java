/*****************************************************************************
*
*  ScoringType.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;


import org.json.simple.JSONObject;

import com.evolving.nglm.core.DeploymentManagedObject;

/* IF COPY/PASTE FROM ANOTHER DeploymentManagedObject OBJECT
 * DO NOT FORGET TO ADD IT TO THE DeploymentManagedObject FACTORY
 * SEE DeploymentManagedObject.create */
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
