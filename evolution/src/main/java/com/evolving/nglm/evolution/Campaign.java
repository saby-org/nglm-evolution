package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;

@GUIDependencyDef(objectType = "campaign", serviceClass = JourneyService.class, dependencies = { "offer", "journeyobjective" , "point" })
public class Campaign extends GUIManagedObject
{

  protected Campaign(String guiManagedObjectID)
  {
    super(guiManagedObjectID);
    throw new ServerRuntimeException("Campaign is marker only - not allowed to create an object");
  }

}
