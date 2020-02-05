package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;

public abstract class GUIManagedObjectMap<T extends GUIManagedObject>
{
  private static final Logger log = LoggerFactory.getLogger(GUIManagedObjectMap.class);
  
  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private Class<T> typeOfT;
  protected Map<String, T> guiManagedObjects;
  private Set<String> warnings; // TODO: factorize with ESObjectList later 
  
  public GUIManagedObjectMap(Class<T> typeOfT) 
  {
    this.typeOfT = typeOfT;
    this.guiManagedObjects = Collections.emptyMap();
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  protected abstract Collection<GUIManagedObject> getCollection();
  
  public Set<String> keySet() { return this.guiManagedObjects.keySet(); }
  public T get(String id) { return this.guiManagedObjects.get(id); }
  
  /*****************************************
  *
  *  reset
  *
  *****************************************/
  
  protected void reset() 
  {
    this.guiManagedObjects = new HashMap<String, T>(); 
    this.warnings = new HashSet<String>();
  }
  
  /*****************************************
  *
  *  logWarningOnlyOnce
  *
  *****************************************/

  protected void logWarningOnlyOnce(String msg)
  {
    if(!this.warnings.contains(msg))
      {
        this.warnings.add(msg);
        log.warn(msg);
      }
  }
  
  /*****************************************
  *
  *  update
  *
  *****************************************/
  
  public void update() 
  {
    this.reset();
    
    for(GUIManagedObject object : getCollection())
      {
        if(this.typeOfT.isInstance(object))
          {
            this.guiManagedObjects.put(object.getGUIManagedObjectID(), (T) object);
          }
        else
          {
            log.warn("Unable to cast {} into {}. It will be discarded from the final map.", object.getGUIManagedObjectID(), typeOfT.toString());
          }
      }
  }

  /*****************************************
  *
  *  getDisplay
  *
  *****************************************/
  
  public String getDisplay(String id, String fieldName)
  {
    T result = this.guiManagedObjects.get(id);
    if(result != null)
      {
        return result.getGUIManagedObjectDisplay();
      }
    else
      {
        logWarningOnlyOnce("Unable to retrieve " + fieldName + ".display for " + fieldName + ".id: " + id);
        return id; // When missing, return the ID by default.
      }
  }
}