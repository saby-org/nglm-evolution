package com.evolving.nglm.evolution.complexobjects;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.SubscriberProfile;
import com.evolving.nglm.evolution.complexobjects.ComplexObjectException.ComplexObjectUtilsReturnCodes;

public class ComplexObjectUtils
{  
  private static ComplexObjectTypeService complexObjectTypeService;
  
  static
  {
    complexObjectTypeService = new ComplexObjectTypeService(System.getProperty("broker.servers"), "complexobjectinstance-complexobjecttypeservice", Deployment.getComplexObjectTypeTopic(), false);
    complexObjectTypeService.start();
  }
  
  public static void setComplexObjectValue(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName, Object value) throws ComplexObjectException
  {
    Collection<ComplexObjectType> types = complexObjectTypeService.getActiveComplexObjectTypes(SystemTime.getCurrentTime(), profile.getTenantID());
    ComplexObjectType type = null;
    for(ComplexObjectType current : types) { if(current.getComplexObjectTypeName().equals(complexTypeName)) { type = current; break; } }
    if(type == null) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE, "Unknown " + complexTypeName); }
    
    // retrieve the subfield declaration
    ComplexObjectTypeSubfield subfieldType = null;
    for(ComplexObjectTypeSubfield currentField : type.getSubfields().values()){ if(currentField.getSubfieldName().equals(subfieldName)){ subfieldType = currentField; break; }} 
    if(subfieldType == null) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_SUBFIELD, "Unknown " + subfieldName + " into " + complexTypeName);}
    
    // ensure the value's type is ok
    if(value != null) {
      switch (subfieldType.getCriterionDataType())
        {
        case IntegerCriterion:
          // must be an Integer or a Long
          if(!(value instanceof Integer) && !(value instanceof Long)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE.BAD_SUBFIELD_TYPE, "Provided value " + value + " should be of type Integer or Long"); }
          break;
          
        case DateCriterion:
          // must be of type Date
          if(!(value instanceof Date)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE.BAD_SUBFIELD_TYPE, "Provided value " + value + " should be of type Date"); }
          break;
          
        case StringCriterion:
          // must be a String
          if(!(value instanceof String)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE.BAD_SUBFIELD_TYPE, "Provided value " + value + " should be of type String"); }
          
          break;
        case BooleanCriterion:
          // must be a Boolean
          if(!(value instanceof Boolean)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE.BAD_SUBFIELD_TYPE, "Provided value " + value + " should be of type Boolean"); }
          
          break;
        case StringSetCriterion:
          // must be a List<String>
          if(!(value instanceof List)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE.BAD_SUBFIELD_TYPE, "Provided value " + value + " should be of type List<String>"); }
        default:
          break;
        }
    }
    
    // check if the element ID exists
    if(!type.getAvailableElements().contains(elementID)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_ELEMENT, "Unknown " + elementID + " into " + complexTypeName);}
    
    // check if the profile already has an instance for this type / element
    List<ComplexObjectInstance> instances = profile.getComplexObjectInstances();
    if(instances == null) { instances = new ArrayList<>(); profile.setComplexObjectInstances(instances);}
    
    ComplexObjectInstance instance = null;
    for(ComplexObjectInstance current : instances) { if(current.getComplexObjectTypeID().equals(type.getComplexObjectTypeID()) && current.getElementID().equals(elementID)) { instance = current; break; }}
    if(instance == null)
      {
        instance = new ComplexObjectInstance(type.getComplexObjectTypeID(), elementID);
        instances.add(instance);        
      }
    
    Map<String, ComplexObjectinstanceSubfieldValue> valueSubFields = instance.getFieldValues();
    if(value == null)
      {
        valueSubFields.remove(subfieldType.getSubfieldName());
      }
    else 
      {
        ComplexObjectinstanceSubfieldValue valueSubField = new ComplexObjectinstanceSubfieldValue(subfieldType.getSubfieldName(), subfieldType.getPrivateID(), value);
        if(valueSubFields == null) { valueSubFields = new HashMap<>(); instance.setFieldValues(valueSubFields); }
        valueSubFields.put(subfieldType.getSubfieldName(), valueSubField);
      }
  }
  
  public static String getComplexObjectString(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
  {
    Object value = getComplexObjectValue(profile, complexTypeName, elementID, subfieldName);
    if(value != null && !(value instanceof String)) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.BAD_SUBFIELD_TYPE, "complexTypeName:" + complexTypeName + " elementID:" + elementID + " subfieldName:" + subfieldName + " is not a String but " + value.getClass().getName());}
    return (String)value;
  }
  
  public static Boolean getComplexObjectBoolean(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
  {
    Object value = getComplexObjectValue(profile, complexTypeName, elementID, subfieldName);
    if(value == null) { return null; }
    if(value != null && !(value instanceof Boolean)) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.BAD_SUBFIELD_TYPE, "complexTypeName:" + complexTypeName + " elementID:" + elementID + " subfieldName:" + subfieldName + " is not a boolean but " + value.getClass().getName());}
    return ((Boolean)value).booleanValue();
  }
  
  public static Date getComplexObjectDate(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
  {
    Object value = getComplexObjectValue(profile, complexTypeName, elementID, subfieldName);
    if(value != null && !(value instanceof Date)) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.BAD_SUBFIELD_TYPE, "complexTypeName:" + complexTypeName + " elementID:" + elementID + " subfieldName:" + subfieldName + " is not a Date but " + value.getClass().getName());}
    return (Date)value;
  }

  public static Long getComplexObjectLong(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
  {
    Object value = getComplexObjectValue(profile, complexTypeName, elementID, subfieldName);
    if(value == null) { return null; }
    if(value != null && !(value instanceof Integer) && !(value instanceof Long)) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.BAD_SUBFIELD_TYPE, "complexTypeName:" + complexTypeName + " elementID:" + elementID + " subfieldName:" + subfieldName + " is not a long but " + value.getClass().getName());}
    if(value instanceof Long) { return ((Long)value); }
    if(value instanceof Integer) { return new Long((int)((Long)value).longValue()); }
    return -1L; // should not happen...    
  }
  
  public static Integer getComplexObjectInteger(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
  {
    Object value = getComplexObjectValue(profile, complexTypeName, elementID, subfieldName);
    if(value == null) { return null; }
    if(value != null && !(value instanceof Integer) && !(value instanceof Long)) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.BAD_SUBFIELD_TYPE, "complexTypeName:" + complexTypeName + " elementID:" + elementID + " subfieldName:" + subfieldName + " is not a long but " + value.getClass().getName());}
    if(value instanceof Integer) { return ((Integer)value); }
    if(value instanceof Long) { return (int)((Long)value).longValue(); }
    return -1; // should not happen... 
  }
  
  public static List<String> getComplexObjectStringSet(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
  {
    Object value = getComplexObjectValue(profile, complexTypeName, elementID, subfieldName);
    if(value != null && !(value instanceof ArrayList)) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.BAD_SUBFIELD_TYPE, "complexTypeName:" + complexTypeName + " elementID:" + elementID + " subfieldName:" + subfieldName + " is not a StringSet but " + value.getClass().getName());}
    return (List<String>) value; 
  }
  
  private static Object getComplexObjectValue(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
  {
    Collection<ComplexObjectType> types = complexObjectTypeService.getActiveComplexObjectTypes(SystemTime.getCurrentTime(), profile.getTenantID());
    ComplexObjectType type = null;
    for(ComplexObjectType current : types) { if(current.getComplexObjectTypeName().equals(complexTypeName)) { type = current; break; } }
    if(type == null) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE, "Unknown " + complexTypeName); }

    // retrieve the subfield declaration
    ComplexObjectTypeSubfield subfieldType = null;
    for(ComplexObjectTypeSubfield currentField : type.getSubfields().values()){ if(currentField.getSubfieldName().equals(subfieldName)){ subfieldType = currentField; break; }} 
    if(subfieldType == null) { return ComplexObjectUtilsReturnCodes.UNKNOWN_SUBFIELD; }

    // check if the element ID exists
    if(!type.getAvailableElements().contains(elementID)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_ELEMENT, "Unknown " + elementID + " into " + complexTypeName);}
    
    // check if the profile already has an instance for this type / element
    List<ComplexObjectInstance> instances = profile.getComplexObjectInstances();
    if(instances == null) { return null;}
    ComplexObjectInstance instance = null;
    for(ComplexObjectInstance current : instances) { if(current.getElementID().equals(elementID)) { instance = current; break; }}
    if(instance == null) { return null; }
    Map<String, ComplexObjectinstanceSubfieldValue> values = instance.getFieldValues();
    if(values == null) { return null; }
    ComplexObjectinstanceSubfieldValue value = values.get(subfieldName);
    if(value == null) { return null; }    
    return value.getValue();
  }

}
