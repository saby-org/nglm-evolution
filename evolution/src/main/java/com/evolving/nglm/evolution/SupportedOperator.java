/*****************************************************************************
*
*  SupportedOperator.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONObject;

import java.util.Objects;

/* IF COPY/PASTE FROM ANOTHER DeploymentManagedObject OBJECT
 * DO NOT FORGET TO ADD IT TO THE DeploymentManagedObject FACTORY
 * SEE DeploymentManagedObject.create */
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
