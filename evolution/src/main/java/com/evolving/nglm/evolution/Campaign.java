package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;

@GUIDependencyDef(objectType = "campaign", serviceClass = JourneyService.class, dependencies = { "offer", "journeyobjective" , "point" , "target" , "workflow" , "mailtemplate" , "pushtemplate" , "dialogtemplate", "voucher", "loyaltyProgramPoints", "loyaltyprogramchallenge", "loyaltyprogrammission", "saleschannel", "sourceaddress"})
public class Campaign extends GUIManagedObject
{

  protected Campaign(String guiManagedObjectID, int tenantID)
  {
    super(guiManagedObjectID, tenantID);
    throw new ServerRuntimeException("Campaign is marker only - not allowed to create an object");
  }

}
