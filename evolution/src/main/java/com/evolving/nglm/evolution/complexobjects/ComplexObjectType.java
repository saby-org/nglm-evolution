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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("availableNames", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("fields", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).schema());
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

  private List<String> availableNames;
  private Map<Integer, ComplexObjectTypeField> fields;

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
  public List<String> getAvailableNames() { return availableNames; }
  public Map<Integer, ComplexObjectTypeField> getFields() { return fields; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public ComplexObjectType(SchemaAndValue schemaAndValue, List<String> availableNames, Map<Integer, ComplexObjectTypeField> fields)
  {
    super(schemaAndValue);
    this.availableNames = availableNames;
    this.fields = fields;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private ComplexObjectType(ComplexObjectType type)
  {
    super(type.getJSONRepresentation(), type.getEpoch());
    this.availableNames = type.getAvailableNames();
    this.fields = type.getFields();
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
    struct.put("availableNames", type.getAvailableNames());
    struct.put("fields", packFields(type.getFields()));
    return struct;
  }
  
  /****************************************
  *
  *  packFields
  *
  ****************************************/

  private static Map<Integer,Object> packFields(Map<Integer,ComplexObjectTypeField> fields)
  {
    Map<Integer,Object> result = new LinkedHashMap<Integer,Object>();
    for (Integer fieldPrivateKey : fields.keySet())
      {
        ComplexObjectTypeField field = fields.get(fieldPrivateKey);
        result.put(fieldPrivateKey, field);
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
    List<String> availableNames = (List<String>) valueStruct.get("availableNames");
    Map<Integer, ComplexObjectTypeField> fields = unpackFields(schema.field("fields").schema(), (Map<Integer,Object>) valueStruct.get("fields"));
    
    //
    //  return
    //

    return new ComplexObjectType(schemaAndValue, availableNames, fields);
  }
  
  /*****************************************
  *
  *  unpackLoyaltyPrograms
  *
  *****************************************/

  private static Map<Integer,ComplexObjectTypeField> unpackFields(Schema schema, Map<Integer,Object> value)
  {
    //
    //  get schema for LoyaltyProgramState
    //

    Schema fieldsSchema = schema.valueSchema();

    //
    //  unpack
    //

    Map<Integer,ComplexObjectTypeField> result = new HashMap<Integer,ComplexObjectTypeField>();
    if(value != null)
      {
        for (Integer key : value.keySet())
          {
            result.put(key, (ComplexObjectTypeField) value.get(key));
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
    //    "availableNames" : [ "A","B"],
    //    "fields" : [
    //      
    //      {
    //        "fieldName" : "Foo",
    //        "fieldDataType" : "integer",
    //        "availableValues" : "["V1", "V2"]
    //      }
    //    ]
    //  }
    //      

    this.availableNames = JSONUtilities.decodeJSONArray(jsonRoot, "availableNames", true);    
    JSONArray f = JSONUtilities.decodeJSONArray(jsonRoot, "fields", true);
    this.fields = new HashMap<>();
    for(int i=0 ; i<f.size(); i++)
      {
        ComplexObjectTypeField field = new ComplexObjectTypeField((JSONObject)f.get(i));
        this.fields.put(field.getPrivateID(), field);
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
        epochChanged = epochChanged || ! Objects.equals(getAvailableNames(), complexObjectType.getAvailableNames());
        epochChanged = epochChanged || ! Objects.equals(getFields(), complexObjectType.getFields());
        return epochChanged;
      }
    else
      {
        return true;
      }
   }
}
