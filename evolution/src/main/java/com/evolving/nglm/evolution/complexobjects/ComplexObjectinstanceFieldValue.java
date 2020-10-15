package com.evolving.nglm.evolution.complexobjects;

public class ComplexObjectinstanceFieldValue
{
  private int privateFieldID;
  private Object value;
  
  public ComplexObjectinstanceFieldValue(int privateFieldID, Object value)
  {
    this.privateFieldID = privateFieldID;
    this.value = value;
  }
  
  public int getPrivateFieldID()
  {
    return privateFieldID;
  }
  public void setPrivateFieldID(int privateFieldID)
  {
    this.privateFieldID = privateFieldID;
  }
  public Object getValue()
  {
    return value;
  }
  public void setValue(Object value)
  {
    this.value = value;
  }
}
