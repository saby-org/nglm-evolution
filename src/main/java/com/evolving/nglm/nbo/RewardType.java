/*****************************************************************************
*
*  RewardType.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONObject;

import java.util.Objects;

public class RewardType extends DeploymentManagedObject
{
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getRewardTypeName() { return getName(); }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public RewardType(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
  }
}
