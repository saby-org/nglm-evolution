package com.evolving.nglm.evolution.complexobjects;

import java.util.Collection;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;

public class ComplexObjectUtils
{
  
  public enum ComplexObjectUtilsReturnCodes 
  {
    UNKNOWN_COMPLEX_TYPE,
    UNKNOWN_ELEMENT,
    UNKNOWN_SUBFIELD,
    BAD_SUBFIELD_TYPE;
  }
  
  private static ComplexObjectTypeService complexObjectTypeService;
  
  static
  {
    complexObjectTypeService = new ComplexObjectTypeService(System.getProperty("broker.servers"), "complexobjectinstance-complexobjecttypeservice", Deployment.getComplexObjectTypeTopic(), false);
    complexObjectTypeService.start();
  }
  
  public static ComplexObjectUtilsReturnCodes setComplexObject(String complexTypeName, String elementName, String subFieldName, Object value)
  {
    Collection<ComplexObjectType> types = complexObjectTypeService.getActiveComplexObjectTypes(SystemTime.getCurrentTime());
  }

}
