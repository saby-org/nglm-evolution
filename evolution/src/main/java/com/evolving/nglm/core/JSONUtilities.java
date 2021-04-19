/*****************************************************************************
*
*  JSONUtilities.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

public class JSONUtilities
{
  /****************************************************************************
  *
  *  Encode Methods
  *  - encodeObject
  *  - encodeArray
  *
  ****************************************************************************/

  /****************************************
  *
  *  encodeObject
  *
  ****************************************/

  private static final Logger log = LoggerFactory.getLogger(JSONUtilities.class);

  public static JSONObject encodeObject(Map<String,? extends Object> entities)
  {
    JSONObject result = new JSONObject();

    //
    //  loop through entities, doing type conversion as necessary
    //
    
    for (String key : entities.keySet())
      {
        //
        //  get Java value
        //

        Object value = entities.get(key);

        //
        //  convert to JSON-assignable object type
        //  - Date to Long
        //
        
        if (value instanceof Date)
          {
            value = new Long(((Date) value).getTime());
          }

        //
        //  add converted object to result
        //

        result.put(key, value);
      }

    //
    //  return
    //
    
    return result;
  }

  /****************************************
  *
  *  encodeArray
  *
  ****************************************/

  public static JSONArray encodeArray(List<? extends Object> elements)
  {
    JSONArray result = new JSONArray();

    //
    //  loop through entities, doing type conversion as necessary
    //
    
    for (Object element : elements)
      {
        //
        //  convert to JSON-assignable object type
        //  - Date to Long
        //
        
        if (element instanceof Date)
          {
            element = new Long(((Date) element).getTime());
          }

        //
        //  add converted object to result
        //

        result.add(element);
      }

    //
    //  return
    //
    
    return result;
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
  
  /****************************************
  *
  *  decodeString
  *
  ****************************************/

  public static String decodeString(JSONObject jsonObject, String key) throws JSONUtilitiesException
  {
    return decodeString(jsonObject, key, false, null);
  }
  
  public static String decodeString(JSONObject jsonObject, String key, String defaultValue) throws JSONUtilitiesException
  {
    return decodeString(jsonObject, key, false, defaultValue);
  }

  public static String decodeString(JSONObject jsonObject, String key, boolean required) throws JSONUtilitiesException
  {
    return decodeString(jsonObject, key, required, null);
  }
  
  private static String decodeString(JSONObject jsonObject, String key, boolean required, String defaultValue) throws JSONUtilitiesException
  {
    String result = null;

    //
    //  get value as String
    //

    try
      {
        result  = (String) jsonObject.get(key);
      }
    catch (ClassCastException e)
      {
        throw new JSONUtilitiesException("value for key " + key + " is not a string", e);
      }

    //
    //  use default as necessary
    //

    result = (result == null || result.equals("")) ? defaultValue : result;

    //
    //  test for required
    //

    if (required && (result == null || result.equals("")))
      {
        throw new JSONUtilitiesException("unexpected null value for key " + key);
      }
    
    //
    //  return
    //
    
    return result;
  }

  /****************************************
  *
  *  decodeInteger
  *
  ****************************************/

  public static Integer decodeInteger(JSONObject jsonObject, String key) throws JSONUtilitiesException
  {
    return decodeInteger(jsonObject, key, false, null);
  }
  
  public static Integer decodeInteger(JSONObject jsonObject, String key, Integer defaultValue) throws JSONUtilitiesException
  {
    return decodeInteger(jsonObject, key, false, defaultValue);
  }

  public static Integer decodeInteger(JSONObject jsonObject, String key, boolean required) throws JSONUtilitiesException
  {
    return decodeInteger(jsonObject, key, required, null);
  }
  
  private static Integer decodeInteger(JSONObject jsonObject, String key, boolean required, Integer defaultValue) throws JSONUtilitiesException
  {
    Integer result = null;

    //
    //  get value as Number
    //
    
    Number number;
    try
      {
        number = (Number) jsonObject.get(key);
      }
    catch (NumberFormatException e)
      {
        throw new JSONUtilitiesException("value for key " + key + " is not a number", e);
      }

    //
    //  convert to Integer, using default as necessary
    //
    
    result = (number != null) ? new Integer(number.intValue()) : defaultValue;

    //
    //  test for required
    //

    if (required && result == null)
      {
        throw new JSONUtilitiesException("unexpected null value for key " + key);
      }
    
    //
    //  return
    //
    
    return result;
  }

  /****************************************
  *
  *  decodeLong
  *
  ****************************************/

  public static Long decodeLong(JSONObject jsonObject, String key) throws JSONUtilitiesException
  {
    return decodeLong(jsonObject, key, false, null);
  }
  
  public static Long decodeLong(JSONObject jsonObject, String key, Long defaultValue) throws JSONUtilitiesException
  {
    return decodeLong(jsonObject, key, false, defaultValue);
  }

  public static Long decodeLong(JSONObject jsonObject, String key, boolean required) throws JSONUtilitiesException
  {
    return decodeLong(jsonObject, key, required, null);
  }
  
  private static Long decodeLong(JSONObject jsonObject, String key, boolean required, Long defaultValue) throws JSONUtilitiesException
  {
    Long result = null;

    //
    //  get value as Number
    //
    
    Number number;
    try
      {
        number = (Number) jsonObject.get(key);
      }
    catch (NumberFormatException e)
      {
        throw new JSONUtilitiesException("value for key " + key + " is not a number", e);
      }

    //
    //  convert to Long, using default as necessary
    //
    
    result = (number != null) ? new Long(number.longValue()) : defaultValue;

    //
    //  test for required
    //

    if (required && result == null)
      {
        throw new JSONUtilitiesException("unexpected null value for key " + key);
      }
    
    //
    //  return
    //
    
    return result;
  }

  /****************************************
  *
  *  decodeDouble
  *
  ****************************************/

  public static Double decodeDouble(JSONObject jsonObject, String key) throws JSONUtilitiesException
  {
    return decodeDouble(jsonObject, key, false, null);
  }
  
  public static Double decodeDouble(JSONObject jsonObject, String key, Double defaultValue) throws JSONUtilitiesException
  {
    return decodeDouble(jsonObject, key, false, defaultValue);
  }

  public static Double decodeDouble(JSONObject jsonObject, String key, boolean required) throws JSONUtilitiesException
  {
    return decodeDouble(jsonObject, key, required, null);
  }
  
  private static Double decodeDouble(JSONObject jsonObject, String key, boolean required, Double defaultValue) throws JSONUtilitiesException
  {
    Double result = null;

    //
    //  get value as Number
    //
    
    Number number;
    try
      {
        number = (Number) jsonObject.get(key);
      }
    catch (NumberFormatException e)
      {
        throw new JSONUtilitiesException("value for key " + key + " is not a number", e);
      }

    //
    //  convert to Double, using default as necessary
    //
    
    result = (number != null) ? new Double(number.doubleValue()) : defaultValue;

    //
    //  test for required
    //

    if (required && result == null)
      {
        throw new JSONUtilitiesException("unexpected null value for key " + key);
      }
    
    //
    //  return
    //
    
    return result;
  }

  /****************************************
  *
  *  decodeDate
  *
  ****************************************/

  public static Date decodeDate(JSONObject jsonObject, String key) throws JSONUtilitiesException
  {
    return decodeDate(jsonObject, key, false, null);
  }
  
  public static Date decodeDate(JSONObject jsonObject, String key, Date defaultValue) throws JSONUtilitiesException
  {
    return decodeDate(jsonObject, key, false, defaultValue);
  }

  public static Date decodeDate(JSONObject jsonObject, String key, boolean required) throws JSONUtilitiesException
  {
    return decodeDate(jsonObject, key, required, null);
  }
  
  private static Date decodeDate(JSONObject jsonObject, String key, boolean required, Date defaultValue) throws JSONUtilitiesException
  {
    Date result = null;

    //
    //  get value as Number
    //
    
    Number number;
    try
      {
        number = (Number) jsonObject.get(key);
      }
    catch (NumberFormatException e)
      {
        throw new JSONUtilitiesException("value for key " + key + " is not a number", e);
      }

    //
    //  convert to Date, using default as necessary
    //
    
    result = (number != null) ? new Date(number.longValue()) : defaultValue;

    //
    //  test for required
    //

    if (required && result == null)
      {
        throw new JSONUtilitiesException("unexpected null value for key " + key);
      }
    
    //
    //  return
    //
    
    return result;
  }

  public static Date decodeDate(JSONArray jsonArray, int index) throws JSONUtilitiesException
  {
    return decodeDate(jsonArray, index, false, null);
  }
  
  public static Date decodeDate(JSONArray jsonArray, int index, Date defaultValue) throws JSONUtilitiesException
  {
    return decodeDate(jsonArray, index, false, defaultValue);
  }

  public static Date decodeDate(JSONArray jsonArray, int index, boolean required) throws JSONUtilitiesException
  {
    return decodeDate(jsonArray, index, required, null);
  }

  public static Date decodeDate(JSONArray jsonArray, int index, boolean required, Date defaultValue) throws JSONUtilitiesException
  {
    Date result = null;

    //
    //  get value as Number
    //
    
    Number number;
    try
      {
        number = (Number) jsonArray.get(index);
      }
    catch (NumberFormatException e)
      {
        throw new JSONUtilitiesException("value for index " + index + " is not a number", e);
      }

    //
    //  convert to Date, using default as necessary
    //
    
    result = (number != null) ? new Date(number.longValue()) : defaultValue;

    //
    //  test for required
    //

    if (required && result == null)
      {
        throw new JSONUtilitiesException("unexpected null value for index " + index);
      }
    
    //
    //  return
    //
    
    return result;
  }
  
  
  /****************************************
  *
  *  decodeBoolean
  *
  ****************************************/

  public static Boolean decodeBoolean(JSONObject jsonObject, String key) throws JSONUtilitiesException
  {
    return decodeBoolean(jsonObject, key, false, null);
  }
  
  // /!\ WARNING the two following overload of decodeBoolean are subject to confusion and mistakes
  // This require a refactoring.
  // In the meantime, they should be used with caution (not really deprecated yet)
  // If possible, add comment when using the first one with Boolean.TRUE / Boolean.False
  @Deprecated
  public static Boolean decodeBoolean(JSONObject jsonObject, String key, Boolean defaultValue) throws JSONUtilitiesException
  {
    return decodeBoolean(jsonObject, key, false, defaultValue);
  }
  @Deprecated
  public static Boolean decodeBoolean(JSONObject jsonObject, String key, boolean required) throws JSONUtilitiesException
  {
    return decodeBoolean(jsonObject, key, required, null);
  }
  
  private static Boolean decodeBoolean(JSONObject jsonObject, String key, boolean required, Boolean defaultValue) throws JSONUtilitiesException
  {
    Boolean result = null;

    //
    //  get value as Boolean
    //

    try
      {
        result  = (Boolean) jsonObject.get(key);
      }
    catch (ClassCastException e)
      {
        throw new JSONUtilitiesException("value for key " + key + " is not a boolean", e);
      }

    //
    //  use default as necessary
    //

    result = (result != null) ? result : defaultValue;

    //
    //  test for required
    //

    if (required && result == null)
      {
        throw new JSONUtilitiesException("unexpected null value for key " + key);
      }
    
    //
    //  return
    //
    
    return result;
  }

  /****************************************
  *
  *  decodeJSONObject
  *
  ****************************************/

  public static JSONObject decodeJSONObject(JSONObject jsonObject, String key) throws JSONUtilitiesException
  {
    return decodeJSONObject(jsonObject, key, false, null);
  }
  
  public static JSONObject decodeJSONObject(JSONObject jsonObject, String key, boolean required) throws JSONUtilitiesException
  {
    return decodeJSONObject(jsonObject, key, required, null);
  }
  
  public static JSONObject decodeJSONObject(JSONObject jsonObject, String key, JSONObject defaultValue) throws JSONUtilitiesException
  {
    return decodeJSONObject(jsonObject, key, false, defaultValue);
  }
  
  private static JSONObject decodeJSONObject(JSONObject jsonObject, String key, boolean required, JSONObject defaultValue) throws JSONUtilitiesException
  {
    JSONObject result = null;

    //
    //  get value
    //

    try
      {
        result  = (JSONObject) jsonObject.get(key);
      }
    catch (ClassCastException e)
      {
        throw new JSONUtilitiesException("value for key " + key + " is not a JSONObject", e);
      }

    //
    //  use default as necessary
    //
    
    result = (result != null) ? result : defaultValue;

    //
    //  test for required
    //

    if (required && (result == null))
      {
        throw new JSONUtilitiesException("unexpected null value for key " + key);
      }
    
    //
    //  return
    //
    
    return result;
  }  
  
  /****************************************
  *
  *  decodeJSONArray
  *
  ****************************************/

  public static JSONArray decodeJSONArray(JSONObject jsonObject, String key) throws JSONUtilitiesException
  {
    return decodeJSONArray(jsonObject, key, false, null);
  }
  
  public static JSONArray decodeJSONArray(JSONObject jsonObject, String key, boolean required) throws JSONUtilitiesException
  {
    return decodeJSONArray(jsonObject, key, required, null);
  }
  
  public static JSONArray decodeJSONArray(JSONObject jsonObject, String key, JSONArray defaultValue) throws JSONUtilitiesException
  {
    return decodeJSONArray(jsonObject, key, false, defaultValue);
  }
  
  private static JSONArray decodeJSONArray(JSONObject jsonObject, String key, boolean required, JSONArray defaultValue) throws JSONUtilitiesException
  {
    JSONArray result = null;

    //
    //  get value
    //

    try
      {
        result  = (JSONArray) jsonObject.get(key);
      }
    catch (ClassCastException e)
      {
        throw new JSONUtilitiesException("value for key " + key + " is not a JSONArray", e);
      }

    //
    //  use default as necessary
    //
    
    result = (result != null) ? result : defaultValue;

    //
    //  test for required
    //

    if (required && (result == null))
      {
        throw new JSONUtilitiesException("unexpected null value for key " + key);
      }
    
    //
    //  return
    //
    
    return result;
  }

  
  /****************************************
  *
  * json copy
  * This copy is intended to be close to a DEEP COPY
  * BUT for "primitive" types (that are still Objects here) it is only a copy by reference
  *
  ****************************************/
  public static JSONObject jsonCopyMap(JSONObject source) 
  {
    JSONObject result = new JSONObject();
    for(Object key : source.keySet()) {
      Object value = source.get(key);
      result.put(key, jsonCopy(value));
    }
    
    return result;
  }
  
  public static JSONArray jsonCopyArray(JSONArray source) {
    JSONArray result = new JSONArray();
    for(Object value : source) {
      result.add(jsonCopy(value));
    }
    
    return result;
  }
  
  private static Object jsonCopy(Object source) {
    if(source instanceof JSONObject) {
      return jsonCopyMap((JSONObject) source);
    }
    else if(source instanceof JSONArray) {
      return jsonCopyArray((JSONArray) source);
    }
    else {
      return source; // WARNING : HERE IT IS ONLY A COPY BY REFERENCE
    }
  }
  
  /****************************************
  *
  *  jsonMerge
  *
  ****************************************/
  // this merge 2 JSON objects :
  // - merging non shared attributes together
  // - overriding shared attributes of 1st arg "initialJson" with ones from 2nd arg "overriderJson"
  // - using 3rd arg method to test if Json objects in Array from both args are the "same" (to be merged)
  // a bit hard to very explain result, just try it, the initial use is to merge/override "product" deployment.json conf with "custo" ones just for needed values
  public static JSONObject jsonMergerOverrideOrAdd(JSONObject initialJson, JSONObject overriderJson, BiPredicate<JSONObject,JSONObject> areJsonEqualsBiPredicate){

    if(log.isDebugEnabled()) log.debug("need to merge json "+initialJson.toJSONString()+ " with overriding one "+overriderJson.toJSONString());

    JSONObject resultJson=new JSONObject();

    // if special field "toDeleteFromConf":true, we skip this object (this is very special case of deleting product conf...)
    if(JSONUtilities.decodeBoolean(overriderJson,"toDeleteFromConf",false,false)){
      if(log.isDebugEnabled()) log.debug("overrider json object contains \"toDeleteFromConf\":true, removing it");
      // return the empty one
      return resultJson;
    }

    resultJson.putAll(initialJson);//deep copy the initial one for the returns
    JSONObject toAdd=new JSONObject();
    toAdd.putAll(overriderJson);// deep copy the one we need to "merge" to

    for(Object key:initialJson.keySet()){

      if(toAdd.get(key)==null) continue;//nothing to do for this key
      if(log.isDebugEnabled()) log.debug("need to merge for key "+key+" object "+toAdd.get(key)+" with object "+initialJson.get(key));

      if(initialJson.get(key) instanceof JSONArray){

        // 2nd arg object does not have same structure, can not merge this part
        if(!(toAdd.get(key) instanceof JSONArray)){
          log.warn("\""+key+"\" in overrider json object is not a json array, will not merge it with initial json");
          continue;
        }
        if(log.isDebugEnabled()) log.debug("array merge for \""+key+"\"");
        resultJson.put(key,jsonArrayMergerOverrideOrAdd((JSONArray)resultJson.get(key),(JSONArray)toAdd.get(key),areJsonEqualsBiPredicate));
        toAdd.remove(key);// removing, we will add all the left ones

      }else if(initialJson.get(key) instanceof JSONObject){

        // 2nd arg object does not have same structure, can not merge this part
        if(!(toAdd.get(key) instanceof JSONObject)){
          log.warn("\""+key+"\" in overrider json object is not a json object, will not merge it with initial json");
          continue;
        }
        if(log.isDebugEnabled()) log.debug("json merge for \""+key+"\"");
        JSONObject mergedJson=jsonMergerOverrideOrAdd((JSONObject)resultJson.get(key),(JSONObject)toAdd.get(key),areJsonEqualsBiPredicate);
        if(!mergedJson.isEmpty()) resultJson.put(key,mergedJson);//do not keep empty one
        toAdd.remove(key);// removing, we will add all the left ones

      }else{
        if((toAdd.get(key) instanceof JSONObject)||(toAdd.get(key) instanceof JSONArray)){
          log.warn("\""+key+"\" in overrider json object is not a simple json key/value attribute, will not merge it with initial json");
          continue;
        }
        if(log.isDebugEnabled()) log.debug("attribute overriding for \""+key+"\", replace initial "+resultJson.get(key)+ "with "+toAdd.get(key));
        resultJson.put(key,toAdd.get(key));
        toAdd.remove(key);// removing, we will add all the left ones
      }
    }
    //adding all the left values/object
    resultJson.putAll(toAdd);

    if(log.isDebugEnabled()) log.debug("Json merge result : "+resultJson.toJSONString());
    return resultJson;

  }
  // only meant for internal use with jsonMergerOverrideOrAdd
  private static JSONArray jsonArrayMergerOverrideOrAdd(JSONArray initialJson, JSONArray toAdd, BiPredicate<JSONObject,JSONObject> areJsonEqualsBiPredicate){
    if(log.isDebugEnabled()) log.debug("need to merge json array  "+initialJson.toJSONString()+ " with  "+toAdd.toJSONString());

    // JSONArray might be of "primitive" type, in that case returning the overriding one completely
    if(initialJson!=null && !initialJson.isEmpty() && !(initialJson.get(0) instanceof JSONObject)){
      if(log.isDebugEnabled()) log.debug("array of primitive type, returning the overriding one : "+toAdd.toJSONString());
      return toAdd;
    }

    JSONArray resultArray= new JSONArray();

    mainLoop:
    for(Object initialObject:initialJson.toArray()){
      JSONObject initialToCheck=(JSONObject)initialObject;
      if(log.isDebugEnabled()) log.debug("checking "+initialToCheck.toJSONString());
      Iterator<JSONObject> toAddIterator=toAdd.iterator();
      while(toAddIterator.hasNext()){
        JSONObject toAddToCheck=toAddIterator.next();
        if(areJsonEqualsBiPredicate.test(initialToCheck,toAddToCheck)){
          // we considered those 2 json object to be merged
          if(log.isDebugEnabled()) log.debug("need to merge matching objects "+initialToCheck.toJSONString()+" and "+toAddToCheck.toJSONString());
          JSONObject mergedJson=jsonMergerOverrideOrAdd(initialToCheck,toAddToCheck,areJsonEqualsBiPredicate);
          if(!mergedJson.isEmpty()) resultArray.add(mergedJson);//do not keep empty one
          toAddIterator.remove();//removing, we will add all the left ones (NOT A DEEP COPY!!, main reason this function is private)
          continue mainLoop;
        }
      }
      // no merge to do on that object
      resultArray.add(initialToCheck);
    }
    // the left values are to add
    resultArray.addAll(toAdd);

    if(log.isDebugEnabled()) log.debug("Array merge result : "+resultArray.toJSONString());
    return resultArray;
  }

  /****************************************************************************
  *
  *  JSONUtilitiesException
  *
  ****************************************************************************/

  public static class JSONUtilitiesException extends RuntimeException
  {
    public JSONUtilitiesException(String message)
    {
      super(message);
    }
    
    public JSONUtilitiesException(String message, Throwable cause)
    {
      super(message, cause);
    }
  }
}