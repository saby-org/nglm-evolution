package com.evolving.nglm.evolution;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;

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
  
  private String guiManagedObjectType;
  private Set<String> dependencyList = new HashSet<String>();
  private Class<? extends GUIService> serviceClass;
  
  //
  // accessors
  //
  
  public String getGuiManagedObjectType() { return guiManagedObjectType; }
  public Set<String> getDependencyList() { return dependencyList; }
  public Class<? extends GUIService> getServiceClass() { return serviceClass; }
  
  /*********************************************
   * 
   * constructor
   * 
   *********************************************/
  
  public GUIDependencyModelTree(Class guiDependencyDefClass, final Set<Class<?>> guiDependencyDefClassList)
  {
    GUIDependencyDef guiDependencyDef = (GUIDependencyDef) guiDependencyDefClass.getAnnotation(GUIDependencyDef.class);
    if (guiDependencyDef != null)
      {
        this.guiManagedObjectType = guiDependencyDef.objectType().toLowerCase();
        this.serviceClass = guiDependencyDef.serviceClass();
        this.dependencyList = prepareDependencyList(guiManagedObjectType, guiDependencyDefClassList);
        
      }
  }
  
  /*********************************************
   * 
   * prepareDependencyList
   * 
   *********************************************/
  
  private Set<String> prepareDependencyList(String guiManagedObjectType, final Set<Class<?>> guiDependencyDefClassList)
  {
    Set<String> result = new HashSet<String>();
    for (Class guiDependencyDefClass : guiDependencyDefClassList)
      { System.out.println(GUIDependencyModelTree.class.getName());
        GUIDependencyDef guiDependencyDef = (GUIDependencyDef) guiDependencyDefClass.getAnnotation(GUIDependencyDef.class);
        if (guiDependencyDef.dependencies().length > 0)
          {
            Set<String> thisDependencies = new HashSet<>(Arrays.asList(guiDependencyDef.dependencies()));
            if (thisDependencies.contains(guiManagedObjectType)) result.add(guiDependencyDef.objectType());
          }
      }
    return result;
  }
}
