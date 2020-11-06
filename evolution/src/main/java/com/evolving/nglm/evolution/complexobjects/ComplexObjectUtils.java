package com.evolving.nglm.evolution.complexobjects;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
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
    Collection<ComplexObjectType> types = complexObjectTypeService.getActiveComplexObjectTypes(SystemTime.getCurrentTime());
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
          
          break;
          
        case DateCriterion:
          // must be an Integer or a Long
          
          
          break;
          
        case StringCriterion:
          // must be a String
          
          break;
        case BooleanCriterion:
          // must be a Boolean
          
          break;
        case StringSetCriterion:
          // must be a List<String>
        
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
    for(ComplexObjectInstance current : instances) { if(current.getElementID().equals(elementID)) { instance = current; break; }}
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
        ComplexObjectinstanceSubfieldValue valueSubField = new ComplexObjectinstanceSubfieldValue(subfieldType.getPrivateID(), value);
        if(valueSubFields == null) { valueSubFields = new HashMap<>(); instance.setFieldValues(valueSubFields); }
        valueSubFields.put(subfieldType.getSubfieldName(), valueSubField);
      }
  }
  
  public static Object getComplexObjectValue(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
  {
    Collection<ComplexObjectType> types = complexObjectTypeService.getActiveComplexObjectTypes(SystemTime.getCurrentTime());
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
    return values.get(subfieldName);
  }

}
