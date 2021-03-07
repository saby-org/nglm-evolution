package com.evolving.nglm.evolution.datamodel;

public class DataModelFieldValue
{
  private String fieldName;
  private int privateFieldID;
  private Object value;
  
  public DataModelFieldValue(String filedName, int privateFiledID, Object value)
  {
    this.fieldName = filedName;
    this.privateFieldID = privateFiledID;
    this.value = value;
  }
  
  public int getPrivateFieldID()
  {
    return privateFieldID;
  }
  public void setPrivateFieldID(int privateFiledID)
  {
    this.privateFieldID = privateFiledID;
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
