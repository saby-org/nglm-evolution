package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.DeploymentManagedObject;

public class CustomerMetaData extends DeploymentManagedObject
{
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public CustomerMetaData(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
  }

}
