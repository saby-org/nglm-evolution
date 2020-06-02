package com.evolving.nglm.evolution;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyModel;

/****************************************
*
*  GUIDependencyModelTree.java
*
*****************************************/

public class GUIDependencyModelTree
{
  //
  //  data
  //
  
  private String guiManagedObjectTypeID;
  private Class<? extends GUIManagedObject> guiManagedObjectTypeClass;
  private List<Class<? extends GUIManagedObject>> dependencyList = new LinkedList<Class<? extends GUIManagedObject>>();
  private Class<? extends GUIService> serviceClass;
  
  //
  // accessors
  //
  
  public String getGuiManagedObjectTypeID() { return guiManagedObjectTypeID; }
  public Class<? extends GUIManagedObject> getGuiManagedObjectTypeClass() { return guiManagedObjectTypeClass; }
  public List<Class<? extends GUIManagedObject>> getDependencyList() { return dependencyList; }
  public Class<? extends GUIService> getServiceClass() { return serviceClass; }
  
  /*********************************************
   * 
   * GUIDependencyModelTree Class constructor
   * 
   *********************************************/
  
  public GUIDependencyModelTree(Class guiDependencyModelClass)
  {
    GUIDependencyModel guiDependencyModel = (GUIDependencyModel) guiDependencyModelClass.getAnnotation(GUIDependencyModel.class);
    if (guiDependencyModel != null)
      {
        this.guiManagedObjectTypeID = guiDependencyModel.objectType().toLowerCase();
        this.guiManagedObjectTypeClass = guiDependencyModelClass;
        this.serviceClass = guiDependencyModel.serviceClass();
        
        //
        //  depdencyList
        //
        
        if (guiDependencyModel.attachableIN() != null)
          {
            for (Class<? extends GUIManagedObject> attachableIN : guiDependencyModel.attachableIN())
              {
                dependencyList.add(attachableIN);
              }
          }
      }
  }
}
