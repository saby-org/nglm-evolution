/*****************************************************************************
*
*  SupportedLanguage.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

import com.evolving.nglm.core.JSONUtilities;
import org.json.simple.JSONObject;

/* IF COPY/PASTE FROM ANOTHER DeploymentManagedObject OBJECT
 * DO NOT FORGET TO ADD IT TO THE DeploymentManagedObject FACTORY
 * SEE DeploymentManagedObject.create */
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
