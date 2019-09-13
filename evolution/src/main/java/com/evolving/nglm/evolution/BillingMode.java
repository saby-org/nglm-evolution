package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

import org.json.simple.JSONObject;

public class BillingMode extends DeploymentManagedObject
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

  public BillingMode(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
  }
}