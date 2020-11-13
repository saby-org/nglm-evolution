/*****************************************************************************
*
*  SupportedTokenCodesFormat.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.DeploymentManagedObject;

public class SupportedVoucherCodePattern extends DeploymentManagedObject
{
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SupportedVoucherCodePattern(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
  }
}
