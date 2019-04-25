/*****************************************************************************
*
*  CatalogCharacteristicUnit.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

import org.json.simple.JSONObject;

public class CatalogCharacteristicUnit extends DeploymentManagedObject
{
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CatalogCharacteristicUnit(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
  }
}
