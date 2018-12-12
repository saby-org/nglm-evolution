/*****************************************************************************
*
*  PaymentInstrument.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONObject;

import java.util.Objects;

public class PaymentInstrument extends DeploymentManagedObject
{
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PaymentInstrument(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
  }
}
