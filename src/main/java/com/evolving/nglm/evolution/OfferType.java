/*****************************************************************************
*
*  OfferType.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONObject;

import java.util.Objects;

public class OfferType extends DeploymentManagedObject
{
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getOfferTypeName() { return getName(); }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public OfferType(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
  }
}
