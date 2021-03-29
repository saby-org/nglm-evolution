/*****************************************************************************
*
*  SupportedTokenCodesFormat.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;


import com.evolving.nglm.core.DeploymentManagedObject;
/* IF COPY/PASTE FROM ANOTHER DeploymentManagedObject OBJECT
 * DO NOT FORGET TO ADD IT TO THE DeploymentManagedObject FACTORY
 * SEE DeploymentManagedObject.create */
public class SupportedTokenCodesFormat extends DeploymentManagedObject
{
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SupportedTokenCodesFormat(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
  }
}
