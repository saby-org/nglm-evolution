package com.evolving.nglm.evolution.complexobjects;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.SubscriberProfile;

public class ComplexObjectUtils
{
  
  public enum ComplexObjectUtilsReturnCodes 
  {
    UNKNOWN_COMPLEX_TYPE,
    UNKNOWN_ELEMENT,
    UNKNOWN_SUBFIELD,
    BAD_SUBFIELD_TYPE,
    OK;
  }
  
  private static ComplexObjectTypeService complexObjectTypeService;
  
  static
  {
    complexObjectTypeService = new ComplexObjectTypeService(System.getProperty("broker.servers"), "complexobjectinstance-complexobjecttypeservice", Deployment.getComplexObjectTypeTopic(), false);
    complexObjectTypeService.start();
  }
  
  public static ComplexObjectUtilsReturnCodes setComplexObjectValue(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName, Object value)
  {
    Collection<ComplexObjectType> types = complexObjectTypeService.getActiveComplexObjectTypes(SystemTime.getCurrentTime());
    ComplexObjectType type = null;
    for(ComplexObjectType current : types) { if(current.getComplexObjectTypeName().equals(complexTypeName)) { type = current; break; } }
    if(type == null) { return ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE; }
    // retrieve the subfield declaration
    ComplexObjectTypeSubfield subfieldType = null;
    for(ComplexObjectTypeSubfield currentField : type.getSubfields().values()){ if(currentField.getSubfieldName().equals(subfieldName)){ subfieldType = currentField; break; }} 
    if(subfieldType == null) { return ComplexObjectUtilsReturnCodes.UNKNOWN_SUBFIELD; }
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
    ComplexObjectinstanceSubfieldValue valueSubField = new ComplexObjectinstanceSubfieldValue(subfieldType.getPrivateID(), value);
    Map<String, ComplexObjectinstanceSubfieldValue> valueSubFields = instance.getFieldValues();
    if(valueSubFields == null) { valueSubFields = new HashMap<>(); instance.setFieldValues(valueSubFields); }
    valueSubFields.put(subfieldType.getSubfieldName(), valueSubField);
    
    return ComplexObjectUtilsReturnCodes.OK;       
  }
  
  public static Object getComplexObjectValue(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName)
  {
    Collection<ComplexObjectType> types = complexObjectTypeService.getActiveComplexObjectTypes(SystemTime.getCurrentTime());
    ComplexObjectType type = null;
    for(ComplexObjectType current : types) { if(current.getComplexObjectTypeName().equals(complexTypeName)) { type = current; break; } }
    if(type == null) { return ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE; }
    // retrieve the subfield declaration
    ComplexObjectTypeSubfield subfieldType = null;
    for(ComplexObjectTypeSubfield currentField : type.getSubfields().values()){ if(currentField.getSubfieldName().equals(subfieldName)){ subfieldType = currentField; break; }} 
    if(subfieldType == null) { return ComplexObjectUtilsReturnCodes.UNKNOWN_SUBFIELD; }
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
