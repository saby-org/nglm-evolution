package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;

@GUIDependencyDef(objectType = "workflow", serviceClass = JourneyService.class, dependencies = {"offer", "mailtemplate", "pushtemplate" , "dialogtemplate", "target", "voucher", "loyaltyProgramPoints", "loyaltyprogramchallenge", "loyaltyprogrammission", "saleschannel", "point", "sourceaddress", "tokentype", "presentationstrategy"})
public class Workflow extends GUIManagedObject
{

  protected Workflow(String guiManagedObjectID, int tenantID)
  {
    super(guiManagedObjectID, tenantID);
    throw new ServerRuntimeException("Bulk Campaign is marker only - not allowed to create an object");
  }

}
