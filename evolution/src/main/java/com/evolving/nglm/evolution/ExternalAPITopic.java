/*****************************************************************************
*
*  ExternalAPITopic.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.DeploymentManagedObject;

public class ExternalAPITopic extends DeploymentManagedObject
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

  public ExternalAPITopic(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
  }
}
