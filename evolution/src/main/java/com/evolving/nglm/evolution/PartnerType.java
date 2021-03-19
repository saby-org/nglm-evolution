package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

import org.json.simple.JSONObject;

/* IF COPY/PASTE FROM ANOTHER DeploymentManagedObject OBJECT
 * DO NOT FORGET TO ADD IT TO THE DeploymentManagedObject FACTORY
 * SEE DeploymentManagedObject.create */
public class PartnerType extends DeploymentManagedObject
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

  public PartnerType(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
  }
}
