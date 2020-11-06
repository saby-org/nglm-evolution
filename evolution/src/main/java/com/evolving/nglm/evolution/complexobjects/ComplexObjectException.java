package com.evolving.nglm.evolution.complexobjects;

@SuppressWarnings("serial")
public class ComplexObjectException extends Exception
{

  public enum ComplexObjectUtilsReturnCodes 
  {
    UNKNOWN_COMPLEX_TYPE,
    UNKNOWN_ELEMENT,
    UNKNOWN_SUBFIELD,
    BAD_SUBFIELD_TYPE,
    OK;
  }
  
  private ComplexObjectUtilsReturnCodes code;
  
  public ComplexObjectException(ComplexObjectUtilsReturnCodes code, String message)
  {
    super(code + ": " + message);
    this.code = code;
  }
  
  public ComplexObjectUtilsReturnCodes getCode() { return code; }
    
}
