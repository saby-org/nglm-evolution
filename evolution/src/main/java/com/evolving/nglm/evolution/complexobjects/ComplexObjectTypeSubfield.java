package com.evolving.nglm.evolution.complexobjects;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;

public class ComplexObjectTypeSubfield
{
  
  private String subfieldName;
  private CriterionDataType criterionDataType;
  private int privateID;
  private List<String> availableValues;
  
  /*****************************************
  *
  *  constructor for Avro deserialize
  *
  *****************************************/
  public ComplexObjectTypeSubfield(String fromSerialization) throws Exception
  {
    this.fromSerializedString(fromSerialization);
  }

  public ComplexObjectTypeSubfield(JSONObject subfield)
  {
    this.subfieldName = JSONUtilities.decodeString(subfield, "subfieldName", true);
    this.criterionDataType = CriterionDataType.fromExternalRepresentation(JSONUtilities.decodeString(subfield, "subfieldDataType", true));
    JSONArray availableValues = JSONUtilities.decodeJSONArray(subfield, "availableValues", false);
    if(availableValues != null)
      {
        for(int i=0; i<availableValues.size(); i++)
          {
            if(this.availableValues == null) {this.availableValues = new ArrayList<>();}
            this.availableValues.add((String)availableValues.get(i));              
          }
      }
    this.privateID = subfieldName.hashCode() & 0x7FFF; // avoid the sign bit to be 1
  }
  
  public String getSubfieldName()
  {
    return subfieldName;
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
        throw new Exception("bad number of subfields " + ss);
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
    ComplexObjectTypeSubfield other = (ComplexObjectTypeSubfield)f;
    if(!subfieldName.equals(other.getSubfieldName())) { return false; }
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
