/*****************************************************************************
*
*  SupportedLanguage.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONObject;

import java.util.Objects;

public class SupportedLanguage extends DeploymentManagedObject
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private boolean defaultLanguage;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public boolean getDefaultLanguage() { return defaultLanguage; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SupportedLanguage(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
    this.defaultLanguage = JSONUtilities.decodeBoolean(jsonRoot, "defaultLanguage", Boolean.FALSE);
  }
}
