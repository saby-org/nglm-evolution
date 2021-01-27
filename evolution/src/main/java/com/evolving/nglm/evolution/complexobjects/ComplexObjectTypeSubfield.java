package com.evolving.nglm.evolution.complexobjects;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;

public class ComplexObjectTypeSubfield
{

  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  
  
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("ComplexObjectTypeSubfield");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subfieldName", Schema.STRING_SCHEMA);
    schemaBuilder.field("criterionDataType", Schema.STRING_SCHEMA);
    schemaBuilder.field("privateID", Schema.INT32_SCHEMA);
    schemaBuilder.field("availableValues", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().schema());    
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<ComplexObjectTypeSubfield> serde = new ConnectSerde<ComplexObjectTypeSubfield>(schema, false, ComplexObjectTypeSubfield.class, ComplexObjectTypeSubfield::pack, ComplexObjectTypeSubfield::unpack);
  
  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ComplexObjectTypeSubfield> serde() { return serde; }

  
  
  
  private String subfieldName;
  private CriterionDataType criterionDataType;
  private int privateID;
  private List<String> availableValues;
  
  
  /*****************************************
  *
  * pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ComplexObjectTypeSubfield complexObjectTypeSubfield = (ComplexObjectTypeSubfield) value;
    Struct struct = new Struct(schema);

    struct.put("subfieldName", complexObjectTypeSubfield.getSubfieldName());
    struct.put("criterionDataType", complexObjectTypeSubfield.getCriterionDataType().getExternalRepresentation());
    struct.put("privateID", complexObjectTypeSubfield.getPrivateID());
    struct.put("availableValues", complexObjectTypeSubfield.getAvailableValues());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ComplexObjectTypeSubfield unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String subfieldName = valueStruct.getString("subfieldName");
    CriterionDataType criterionDataType = CriterionDataType.fromExternalRepresentation(valueStruct.getString("criterionDataType"));
    Integer privateID = valueStruct.getInt32("privateID");
    List<String> availableValues = (List<String>) valueStruct.get("availableValues");
    //
    //  return
    //

    return new ComplexObjectTypeSubfield(subfieldName, criterionDataType, privateID, availableValues);
  }
  
  public ComplexObjectTypeSubfield(String subfieldName, CriterionDataType criterionDataType, Integer privateID, List<String> availableValues)
  {
    this.subfieldName = subfieldName;
    this.criterionDataType = criterionDataType;
    this.privateID = privateID;
    this.availableValues = availableValues;
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
