package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
      { List<String> dependencyList=new ArrayList<>();
    	System.out.println(guiDependencyDefClass +" ====for obj type"+ guiManagedObjectType);
        GUIDependencyDef guiDependencyDef = (GUIDependencyDef) guiDependencyDefClass.getAnnotation(GUIDependencyDef.class);
        if (guiDependencyDef.dependencies().length > 0)
          {
        	for(String dep : guiDependencyDef.dependencies()) {
        		dependencyList.add(dep.toLowerCase());
        	}
        	for(String test: dependencyList) {
        	    System.out.println("1:"+test);  // Will invoke overrided `toString()` method
        	}
            Set<String> thisDependencies = new HashSet<>(dependencyList);
            for(String test1: thisDependencies) {
        	    System.out.println("2:"+test1);  // Will invoke overrided `toString()` method
        	}

            if (thisDependencies.contains(guiManagedObjectType))
            	{System.out.println("inside if");
            	result.add(guiDependencyDef.objectType().toLowerCase());
            	            	}
          }
      }
    return result;
  }
}
