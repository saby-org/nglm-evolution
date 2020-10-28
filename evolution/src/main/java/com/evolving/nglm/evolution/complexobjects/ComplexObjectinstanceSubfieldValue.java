package com.evolving.nglm.evolution.complexobjects;

public class ComplexObjectinstanceSubfieldValue
{
  private int privateSubfieldID;
  private Object value;
  
  public ComplexObjectinstanceSubfieldValue(int privateSubfieldID, Object value)
  {
    this.privateSubfieldID = privateSubfieldID;
    this.value = value;
  }
  
  public int getPrivateSubfieldID()
  {
    return privateSubfieldID;
  }
  public void setPrivateFieldID(int privateSubfieldID)
  {
    this.privateSubfieldID = privateSubfieldID;
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
