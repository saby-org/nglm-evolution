/*****************************************************************************
*
*  DNBOMatrixVariable.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;


import org.json.simple.JSONObject;

import com.evolving.nglm.core.DeploymentManagedObject;

public class DNBOMatrixVariable extends DeploymentManagedObject
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

  public DNBOMatrixVariable(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
  }

 }
