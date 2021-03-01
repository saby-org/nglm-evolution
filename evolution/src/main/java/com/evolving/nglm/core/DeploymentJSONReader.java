package com.evolving.nglm.core;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

/**
 * This class should be used to extract all json values from Deployment.json
 * 
 * TOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOODDDOOOOOOOOOOOOooTOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOODDDOOOOOOOOOOOOooTOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOODDDOOOOOOOOOOOOoo
 * 
 * REQUIRED and DEFAULT_VALUE fields have been removed (compared to JSONUtilities) from all
 * retriever functions for the following reason :
 * - Now that deployment-product-evolution.json act as a "default" deployment that can be override 
 * in the deployment.json of nglm-<project>, it does not make sense anymore to have DEFAULT_VALUE 
 * in the java code. If you want to define a default value for a specific variable, you just have to 
 * define it with its default value in deployment-product-evolution.json 
 * - For REQUIRED, the reason is similar. All variables are REQUIRED, a "not required" variable 
 * would mean that null is the default value if nothing is defined.   TOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOODDDOOOOOOOOOOOOoo
 * 
 * The pro of defining everything in deployment-product-evolution.json is that it act as a 
 * template/sample for projects that need to override them. Plus, it is less subject to weird shady
 * behaviors that could only be understood by reading the way we extract variables in the java code.
 */
public class DeploymentJSONReader
{
  private static final Logger log = LoggerFactory.getLogger(DeploymentJSONReader.class);

  /****************************************
  *
  * Properties
  *
  ****************************************/
  private final JSONObject jsonRoot; // Read only mode
  // This variable will be used to know what has been read already in the jsonRoot. 
  // The goal is to display warning at the end, mentioning all field in jsonRoot that were never used. 
  private Map<String, DeploymentJSONReader> jsonUnused; 
  
  /****************************************
  *
  * Constructor
  * DeploymentJSONReader is used to build a recursive tree structure
  *
  ****************************************/
  // Leaf constructor
  public DeploymentJSONReader() { 
    this.jsonRoot = null;
    this.jsonUnused = null;
  }
  
  // Node constructor (recursive)
  public DeploymentJSONReader(JSONObject jsonObject) {
    this.jsonRoot = jsonObject;
    this.jsonUnused = new LinkedHashMap<String,DeploymentJSONReader>();
    
    for(Object key : jsonObject.keySet()) {
      Object o = jsonRoot.get(key);
      if(o instanceof JSONObject) {
        jsonUnused.put((String) key, new DeploymentJSONReader((JSONObject) o)); // node
      } else {
        jsonUnused.put((String) key, new DeploymentJSONReader()); // leaf
      }
    }
  }

  /****************************************************************************
  *
  *  Decode Methods
  *  - decodeString
  *  - decodeInteger
  *  - decodeLong
  *  - decodeDouble
  *  - decodeDate
  *  - decodeBoolean
  *  - decodeJSONObject
  *  - decodeJSONArray
  *
  ****************************************************************************/
  
  public String decodeString(String key) throws JSONUtilitiesException
  {
    String result = JSONUtilities.decodeString(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonUnused.remove(key);
      return result;
    }
  }
  
  public Integer decodeInteger(String key) throws JSONUtilitiesException
  {
    Integer result = JSONUtilities.decodeInteger(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonUnused.remove(key);
      return result;
    }
  }
  
  public Long decodeLong(String key) throws JSONUtilitiesException
  {
    Long result = JSONUtilities.decodeLong(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonUnused.remove(key);
      return result;
    }
  }
  
  public Double decodeDouble(String key) throws JSONUtilitiesException
  {
    Double result = JSONUtilities.decodeDouble(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonUnused.remove(key);
      return result;
    }
  }
  
  public Date decodeDate(String key) throws JSONUtilitiesException
  {
    Date result = JSONUtilities.decodeDate(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonUnused.remove(key);
      return result;
    }
  }
/*
  public Date decodeDate(JSONArray jsonArray) throws JSONUtilitiesException
  {
    
  }*/
  
  public Boolean decodeBoolean(String key) throws JSONUtilitiesException
  {
    Boolean result = JSONUtilities.decodeBoolean(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonUnused.remove(key);
      return result;
    }
  }

  // TOOOOOOOOOOOOOOOOOODOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
  public JSONObject decodeJSONObject(String key) throws JSONUtilitiesException
  {
    JSONObject result = JSONUtilities.decodeJSONObject(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonUnused.remove(key);
      return result;
    }
  }  
  
  // TOOOOOOOOOOOOOOOOOODOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
  public JSONArray decodeJSONArray(String key) throws JSONUtilitiesException
  {
    JSONArray result = JSONUtilities.decodeJSONArray(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonUnused.remove(key);
      return result;
    }
  }
}
