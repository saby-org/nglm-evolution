package com.evolving.nglm.evolution.complexobjects;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;

public class ComplexObjectTypeField
{
  
  private String fieldName;
  private CriterionDataType criterionDataType;
  private int privateID;
  private List<String> availableValues;

  /*****************************************
  *
  *  constructor for Avro deserialize
  *
  *****************************************/
  public ComplexObjectTypeField(String fromSerialization) throws Exception
  {
    this.fromSerializedString(fromSerialization);
  }

  public ComplexObjectTypeField(JSONObject field)
  {
    this.fieldName = JSONUtilities.decodeString(field, "fieldName", true);
    this.criterionDataType = CriterionDataType.fromExternalRepresentation(JSONUtilities.decodeString(field, "fieldDataType", true));
    JSONArray availableValues = JSONUtilities.decodeJSONArray(field, "availableValues", false);
    if(availableValues != null)
      {
        for(int i=0; i<availableValues.size(); i++)
          {
            if(this.availableValues == null) {this.availableValues = new ArrayList<>();}
            this.availableValues.add((String)availableValues.get(i));              
          }
      }
    this.privateID = fieldName.hashCode() & 0xFFFF;
  }
  
  public String getFieldName()
  {
    return fieldName;
  }
  public CriterionDataType getCriterionDataType()
  {
    return criterionDataType;
  }
  public int getPrivateID()
  {
    return privateID;
  }
  public List<String> getAvailableValues()
  {
    return availableValues;
  }
  
  public String toSerializedString()
  {
    String ser = criterionDataType + "|||" + privateID + "|||";
    if(availableValues != null) {
      for(int i = 0; i < availableValues.size(); i++)
        {
          ser = ser + availableValues.get(i);
          if(i < availableValues.size() - 1)
            {
              ser = ser + "|$|";
            }
        }
    }
    else 
      {
        ser = ser + "noAvailableValues"; 
      }
    return ser;
  }
  
  private void fromSerializedString(String s) throws Exception
  {
    String[] ss = s.split("\\|\\|\\|");
    if(ss.length != 3)
      {
        throw new Exception("bad number of fields " + ss);
      }
    criterionDataType = CriterionDataType.fromExternalRepresentation(ss[0]);
    privateID = Integer.parseInt(ss[1]);
    String availableValues = ss[2];
    if(!availableValues.equals("noAvailableValues"))
      {
        String[] av = availableValues.split("\\|$\\|");
        for(String currentAv : av) {
          if(this.availableValues == null)
            {
              this.availableValues = new ArrayList<>();
            }
          this.availableValues.add(currentAv);
        }
      }    
  }
  
  @Override
  public boolean equals(Object f)
  {
    ComplexObjectTypeField other = (ComplexObjectTypeField)f;
    if(!fieldName.equals(other.getFieldName())) { return false; }
    if(privateID != other.getPrivateID()) { return false; }
    if(!criterionDataType.equals(other.getCriterionDataType())) { return false; }
    if(availableValues == null && other.getAvailableValues() != null) { return false; }
    if(availableValues != null && other.getAvailableValues() == null) { return false; }
    if(availableValues != null) {
      for(String current : availableValues) {
        if(!other.getAvailableValues().contains(current)) {
          return false;
        }
      }
    }
    return true;    
  }

}
