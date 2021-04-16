package com.evolving.nglm.core;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

/**
 * This class should be used to extract all json values from Deployment.json
 * 
 * TOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOODDDOOOOOOOOOOOOooTOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOODDDOOOOOOOOOOOOooTOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOODDDOOOOOOOOOOOOoo
 * EXPLAIN OPTIONAL 
 * 
 * REQUIRED and DEFAULT_VALUE fields have been removed (compared to JSONUtilities) from all
 * retriever functions for the following reason :
 * - Now that deployment-product-evolution.json act as a "default" deployment that can be override 
 * in the deployment.json of nglm-<project>, it does not make sense anymore to have DEFAULT_VALUE 
 * in the java code. If you want to define a default value for a specific variable, you just have to 
 * define it with its default value in deployment-product-evolution.json 
 * - For REQUIRED, the reason is similar. All variables are REQUIRED, a "not required" variable 
 * would mean that null is the default value if nothing is defined.   TOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOODDDOOOOOOOOOOOOoo
 * if null/empty as no meaning, then it is better to raise an error when the variable cannot be found.
 * if null has a value, then use Optional 
 * For array, it is better to put them, even if empty, to act as a template
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
  private Map<String, DeploymentJSONReader> jsonTree; 
  
  /****************************************
  *
  * Constructor
  * DeploymentJSONReader is built as a recursive tree structure
  *
  ****************************************/
  // Leaf constructor
  public DeploymentJSONReader() { 
    this.jsonRoot = null;
    this.jsonTree = Collections.emptyMap();
  }
  
  // Node constructor (recursive)
  public DeploymentJSONReader(JSONObject jsonObject) {
    this.jsonRoot = jsonObject;
    this.jsonTree = new LinkedHashMap<String,DeploymentJSONReader>();
    
    for(Object key : jsonObject.keySet()) {
      Object o = jsonRoot.get(key);
      if(o instanceof JSONObject) {
        jsonTree.put((String) key, new DeploymentJSONReader((JSONObject) o)); // node
      } else {
        jsonTree.put((String) key, new DeploymentJSONReader()); // leaf
      }
    }
  }

  /****************************************************************************
  * 
  * JSON Object methods 
  * 
  ****************************************************************************/
  public Set<?> keySet() {
    if(jsonRoot != null) {
      return jsonRoot.keySet();
    } else {
      return Collections.emptySet();
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
      jsonTree.remove(key);
      return result;
    }
  }
  
  public String decodeOptionalString(String key) throws JSONUtilitiesException
  {
    String result = JSONUtilities.decodeString(jsonRoot, key); // Not required, NULL as default.
    jsonTree.remove(key);
    return result;
  }
  
  public Integer decodeInteger(String key) throws JSONUtilitiesException
  {
    Integer result = JSONUtilities.decodeInteger(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonTree.remove(key);
      return result;
    }
  }
  
  public Long decodeLong(String key) throws JSONUtilitiesException
  {
    Long result = JSONUtilities.decodeLong(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonTree.remove(key);
      return result;
    }
  }
  
  public Double decodeDouble(String key) throws JSONUtilitiesException
  {
    Double result = JSONUtilities.decodeDouble(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonTree.remove(key);
      return result;
    }
  }
  
  public Date decodeDate(String key) throws JSONUtilitiesException
  {
    Date result = JSONUtilities.decodeDate(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonTree.remove(key);
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
      jsonTree.remove(key);
      return result;
    }
  }

  // TOOOOOOOOOOOOOOOOOODOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
  @Deprecated // utiliser .get !!!
  public JSONObject decodeJSONObject(String key) throws JSONUtilitiesException
  {
    JSONObject result = JSONUtilities.decodeJSONObject(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonTree.remove(key); // TOOOOOOOOOOOOOOOOOODOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
      return result;
    }
  }  
  

  // TOOOOOOOOOOOOOOOOOODOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
  public DeploymentJSONReader get(Object key) throws JSONUtilitiesException
  {
    DeploymentJSONReader result = jsonTree.get(key);
        
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else if (result.jsonRoot == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " is not a JSONObject.");
    } else {
      return result;
    }
  }  
  
  // TOOOOOOOOOOOOOOOOOODOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
  @Deprecated
  public JSONArray decodeJSONArray(String key) throws JSONUtilitiesException
  {
    JSONArray result = JSONUtilities.decodeJSONArray(jsonRoot, key); // Not required, NULL as default.
    
    if(result == null) {
      throw new JSONUtilitiesException("JSON settings extraction: " + key + " could not be found (or is null).");
    } else {
      jsonTree.remove(key);
      return result;
    }
  }
  
  /****************************************************************************
  * 
  * High-level decode Methods// TOOOOOOOOOOOOOOOOOODOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
   * @throws GUIManagerException 
  * 
  ****************************************************************************/
  /**
   * Create a Map(ObjectID -> Object) from a JSONArray of JSONObjects.
   * Each JSONObject is representing an instance of @param Tclass
   */
  public <T extends DeploymentManagedObject> Map<String, T> decodeMapFromArray(Class<T> Tclass, String key) throws NoSuchMethodException, IllegalAccessException, ClassNotFoundException, GUIManagerException{
    Map<String, T> result = new LinkedHashMap<String, T>();
    
    JSONArray jsonArray = this.decodeJSONArray(key);
    for (int i = 0; i < jsonArray.size(); i++) {
      JSONObject jsonObject = (JSONObject) jsonArray.get(i);
      T item = (T) DeploymentManagedObject.create(Tclass, jsonObject);
      result.put(item.getID(), item);
    }
    
    return result;
  }
  
  /****************************************************************************
  * 
  * extractRemaining
  * 
  ****************************************************************************/
  public JSONObject buildRemaining() {
    JSONObject result = null;
    for(String key: jsonTree.keySet()) {
      DeploymentJSONReader node = jsonTree.get(key);
      if(node == null) {
        continue;
      }

      Object nodeValue = null;
      if(node.jsonRoot == null) { // leaf - primitive type
        nodeValue = jsonRoot.get(key);
      } 
      else { // node
        nodeValue = node.buildRemaining();
      }

      if(nodeValue != null) {
        if(result == null) {
          result = new JSONObject();
        }
        result.put(key, nodeValue);
      }
    }
    
    return result;
  }
  
  /****************************************************************************
  * 
  * checkUnusedFields
  * 
  ****************************************************************************/
  private boolean printUnusedError(boolean print) {
    if(print) {
      log.error("The following fields of your Deployment JSON settings were not used. "
          +"Please remove them from your configuration and check if everything is set correctly.");
    }
    return true;
  }
  
  /* return true if an error was displayed */
  private boolean checkUnusedFieldsRec(String prefix, boolean preambuleRaised) {
    boolean errorRaised = preambuleRaised;
    for(String key : this.jsonTree.keySet()) {
      if(this.jsonTree.get(key).jsonRoot == null) { // leaf
        errorRaised = printUnusedError(!errorRaised);
        log.error("- " + prefix + "->" + key);
      } 
      else {
        errorRaised = this.jsonTree.get(key).checkUnusedFieldsRec(prefix + "->" + key, errorRaised);
      }
    }
    
    return errorRaised;
  }
  
  /**
   * This will log errors if there is fields that were never read in the JSON Root object.
   * @param tenantID
   */
  public void checkUnusedFields(String prefix) {
    boolean errorRaised = this.checkUnusedFieldsRec(prefix, false);
    if(errorRaised) {
      throw new ServerRuntimeException("Check your deployment JSON settings.");
    }
  }
}
