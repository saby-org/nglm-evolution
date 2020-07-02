package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;

@GUIDependencyDef(objectType = "bulkcampaign", serviceClass = JourneyService.class, dependencies = {"point" })
public class BulkCampaign extends GUIManagedObject
{

  protected BulkCampaign(String guiManagedObjectID)
  {
    super(guiManagedObjectID);
    throw new ServerRuntimeException("Bulk Campaign is marker only - not allowed to create an object");
  }

}
