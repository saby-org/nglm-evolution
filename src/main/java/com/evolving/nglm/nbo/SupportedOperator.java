/*****************************************************************************
*
*  SupportedOperator.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONObject;

import java.util.Objects;

public class SupportedOperator extends DeploymentManagedObject
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private boolean argumentSet;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getOperatorName() { return getName(); }
  public boolean getArgumentSet() { return argumentSet; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SupportedOperator(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
    this.argumentSet = JSONUtilities.decodeBoolean(jsonRoot, "argumentSet", true);
  }
}
