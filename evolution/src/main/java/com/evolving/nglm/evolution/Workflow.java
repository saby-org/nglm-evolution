package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;

@GUIDependencyDef(objectType = "workflow", serviceClass = JourneyService.class, dependencies = {"mailtemplate" , "pushtemplate" , "dialogtemplate"})
public class Workflow extends GUIManagedObject
{

  protected Workflow(String guiManagedObjectID)
  {
    super(guiManagedObjectID);
    throw new ServerRuntimeException("Bulk Campaign is marker only - not allowed to create an object");
  }

}
