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
  private Map<String, CriterionDataType> fields;

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
  public Map<String, CriterionDataType> getFields() { return fields; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public ComplexObjectType(SchemaAndValue schemaAndValue, List<String> availableNames, Map<String, CriterionDataType> fields)
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

  private static Map<String,Object> packFields(Map<String,CriterionDataType> fields)
  {
    Map<String,Object> result = new LinkedHashMap<String,Object>();
    for (String contextVariableName : fields.keySet())
      {
        CriterionDataType dataType = fields.get(contextVariableName);
        result.put(contextVariableName, dataType.getExternalRepresentation());
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
    Map<String, CriterionDataType> fields = unpackFields(schema.field("fields").schema(), (Map<String,Object>) valueStruct.get("fields"));
    
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

  private static Map<String,CriterionDataType> unpackFields(Schema schema, Map<String,Object> value)
  {
    //
    //  get schema for LoyaltyProgramState
    //

    Schema fieldsSchema = schema.valueSchema();

    //
    //  unpack
    //

    Map<String,CriterionDataType> result = new HashMap<String,CriterionDataType>();
    if(value != null)
      {
        for (String key : value.keySet())
          {
            result.put(key, CriterionDataType.fromExternalRepresentation((String)(value.get(key))));
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

    this.availableNames = JSONUtilities.decodeJSONArray(jsonRoot, "availableNames", true);
    Map<String, String> f = JSONUtilities.decodeJSONObject(jsonRoot, "fields", true);
    this.fields = new HashMap<>();
    for(Map.Entry<String,String> current : f.entrySet())
      {
        this.fields.put(current.getKey(), CriterionDataType.fromExternalRepresentation(current.getValue()));
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
