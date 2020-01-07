package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.GUIManagedObject;

public abstract class GUIManagedObjectList<T extends GUIManagedObject>
{
  protected static final Logger log = LoggerFactory.getLogger(GUIManagedObjectList.class);
  
  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  protected Map<String, T> guiManagedObjects;   // (objectID,object)
  protected Set<String> warnings; // TODO: factorize with GUIManagerObjectList later 
  
  public GUIManagedObjectList() 
  {
    this.guiManagedObjects = Collections.emptyMap();
    
    // TODO: we force the init of GUIManagedObject class by calling a static function
    // Otherwise there is a bug with static code calls between GUIManagedObject class and those which extends GUIManagedObject.
    GUIManagedObject.commonSchema();
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public Set<String> keySet() { return this.guiManagedObjects.keySet(); }
  public T get(String id) { return this.guiManagedObjects.get(id); }
  
  /*****************************************
  *
  *  reset
  *
  *****************************************/
  
  public void reset() 
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
  
  /*****************************************
  *
  *  retrieveFromGUIManager
  *
  *****************************************/
  
  public abstract void updateFromGUIManager(GUIManagerClient gmClient);
  
  /*****************************************
  *
  *  getGUIManagedObjectList
  *
  *****************************************/
  
  protected List<JSONObject> getGUIManagedObjectList(GUIManagerClient gmClient, String api, String field)
  {
    Map<String,Object> getRequestBodyJSON = new HashMap<String,Object>();
    getRequestBodyJSON.put("apiVersion",1);
    JSONObject getRequestBody = JSONUtilities.encodeObject(getRequestBodyJSON);

    /*****************************************
    *
    *  call
    *
    *****************************************/

    JSONObject response = gmClient.call(api, getRequestBody);

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (response == null)
      {
        // empty list
        return Collections.emptyList();
      }
    else if(! Objects.equals(JSONUtilities.decodeString(response, "responseCode", false), "ok"))
      {
        log.error("Error on GUIManager call {}: {}", api, JSONUtilities.decodeString(response, "responseCode", false));
        return Collections.emptyList();
      }
    
    return (List<JSONObject>) JSONUtilities.decodeJSONArray(response, field, true);
  }
}