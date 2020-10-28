package com.evolving.nglm.evolution.complexobjects;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
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
import com.evolving.nglm.evolution.CriterionField;
import com.evolving.nglm.evolution.EvaluationCriterion;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.JourneyObjectiveInstance;
import com.evolving.nglm.evolution.LoyaltyProgramState;
import com.evolving.nglm.evolution.ParameterMap;
import com.evolving.nglm.evolution.SubscriberRelatives;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class ComplexObjectType extends GUIManagedObject
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
    schemaBuilder.name("complex_object");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("availableElements", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("subfields", SchemaBuilder.map(Schema.INT32_SCHEMA, ComplexObjectTypeSubfield.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ComplexObjectType> serde = new ConnectSerde<ComplexObjectType>(schema, false, ComplexObjectType.class, ComplexObjectType::pack, ComplexObjectType::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ComplexObjectType> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private List<String> availableElements;
  private Map<Integer, ComplexObjectTypeSubfield> subfields;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //
  public String getComplexObjectTypeID() { return getGUIManagedObjectID(); }
  public String getComplexObjectTypeName() { return getGUIManagedObjectName(); }
  public String getDisplay() { return getGUIManagedObjectDisplay(); }
  public List<String> getAvailableElements() { return availableElements; }
  public Map<Integer, ComplexObjectTypeSubfield> getSubfields() { return subfields; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public ComplexObjectType(SchemaAndValue schemaAndValue, List<String> availableElements, Map<Integer, ComplexObjectTypeSubfield> subfields)
  {
    super(schemaAndValue);
    this.availableElements = availableElements;
    this.subfields = subfields;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private ComplexObjectType(ComplexObjectType type)
  {
    super(type.getJSONRepresentation(), type.getEpoch());
    this.availableElements = type.getAvailableElements();
    this.subfields = type.getSubfields();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public ComplexObjectType copy()
  {
    return new ComplexObjectType(this);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ComplexObjectType type = (ComplexObjectType) value;
    Struct struct = new Struct(schema);
    packCommon(struct, type);
    struct.put("availableElements", type.getAvailableElements());
    struct.put("subfields", packSubfields(type.getSubfields()));
    return struct;
  }
  
  /****************************************
  *
  *  packSubfields
  *
  ****************************************/

  private static Map<Integer,Object> packSubfields(Map<Integer,ComplexObjectTypeSubfield> subfields)
  {
    Map<Integer,Object> result = new LinkedHashMap<Integer,Object>();
    for (Integer subfieldPrivateKey : subfields.keySet())
      {
        ComplexObjectTypeSubfield subfield = subfields.get(subfieldPrivateKey);
        result.put(subfieldPrivateKey, ComplexObjectTypeSubfield.pack(subfield));
      }
    return result;
  }
  

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ComplexObjectType unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    List<String> availableElements = (List<String>) valueStruct.get("availableElements");
    Map<Integer, ComplexObjectTypeSubfield> subfields = unpackSubfields(schema.field("subfields").schema(), (Map<Integer,Object>) valueStruct.get("subfields"));
    
    //
    //  return
    //

    return new ComplexObjectType(schemaAndValue, availableElements, subfields);
  }
  
  /*****************************************
  *
  *  unpackLoyaltyPrograms
  *
  *****************************************/

  private static Map<Integer,ComplexObjectTypeSubfield> unpackSubfields(Schema schema, Map<Integer,Object> value)
  {
    //
    //  get schema for LoyaltyProgramState
    //

    Schema subfieldsSchema = schema.valueSchema();

    //
    //  unpack
    //

    Map<Integer,ComplexObjectTypeSubfield> result = new HashMap<Integer,ComplexObjectTypeSubfield>();
    Map<Integer, Object> valueMap = (Map<Integer, Object>) value;
    if(value != null)
      {
        for (Integer key : value.keySet())
          {
            result.put(key, ComplexObjectTypeSubfield.serde().unpack(new SchemaAndValue(subfieldsSchema, valueMap.get(key))));
          }
      }
    
    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public ComplexObjectType(JSONObject jsonRoot, long epoch, GUIManagedObject existingComplexObjectTypeUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingComplexObjectTypeUnchecked != null) ? existingComplexObjectTypeUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingComplexObjectType
    *
    *****************************************/

    ComplexObjectType existingComplexObjectType = (existingComplexObjectTypeUnchecked != null && existingComplexObjectTypeUnchecked instanceof ComplexObjectType) ? (ComplexObjectType) existingComplexObjectTypeUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    // {
    //    "availableElements" : [ "A","B"],
    //    "subfields" : [
    //      
    //      {
    //        "subfieldName" : "Foo",
    //        "subfieldDataType" : "integer",
    //        "availableValues" : "["V1", "V2"]
    //      }
    //    ]
    //  }
    //      

    this.availableElements = JSONUtilities.decodeJSONArray(jsonRoot, "availableElements", true);    
    JSONArray f = JSONUtilities.decodeJSONArray(jsonRoot, "subfields", true);
    this.subfields = new HashMap<>();
    for(int i=0 ; i<f.size(); i++)
      {
        ComplexObjectTypeSubfield subfield = new ComplexObjectTypeSubfield((JSONObject)f.get(i));
        this.subfields.put(subfield.getPrivateID(), subfield);
      }
    
    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingComplexObjectType))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(ComplexObjectType complexObjectType)
  {
    if (complexObjectType != null && complexObjectType.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getAvailableElements(), complexObjectType.getAvailableElements());
        epochChanged = epochChanged || ! Objects.equals(getSubfields(), complexObjectType.getSubfields());
        return epochChanged;
      }
    else
      {
        return true;
      }
   }
}
