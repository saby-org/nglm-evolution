package com.evolving.nglm.evolution.complexobjects;

import java.util.ArrayList;
import java.util.Collection;
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
import com.evolving.nglm.evolution.ParameterMap;
import com.evolving.nglm.evolution.Point;
import com.evolving.nglm.evolution.PointValidity;
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

  public static Point unpack(SchemaAndValue schemaAndValue)
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
    List<String> availableNames = (List<String>) valueStruct.get("targetID");
    
    boolean debitable = valueStruct.getBoolean("debitable");
    boolean creditable = valueStruct.getBoolean("creditable");
    boolean setable = valueStruct.getBoolean("setable");
    PointValidity validity = PointValidity.unpack(new SchemaAndValue(schema.field("validity").schema(), valueStruct.get("validity")));
    String label = (schemaVersion >= 2) ? valueStruct.getString("label") : "";

    //
    //  return
    //

    return new Point(schemaAndValue, debitable, creditable, setable, validity, label);
  }
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Point(JSONObject jsonRoot, long epoch, GUIManagedObject existingPointUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingPointUnchecked != null) ? existingPointUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingPoint
    *
    *****************************************/

    Point existingPoint = (existingPointUnchecked != null && existingPointUnchecked instanceof Point) ? (Point) existingPointUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.debitable = JSONUtilities.decodeBoolean(jsonRoot, "debitable", Boolean.TRUE);
    this.creditable = JSONUtilities.decodeBoolean(jsonRoot, "creditable", Boolean.TRUE);
    this.setable = JSONUtilities.decodeBoolean(jsonRoot, "setable", Boolean.FALSE);
    this.validity = new PointValidity(JSONUtilities.decodeJSONObject(jsonRoot, "validity", true));
    this.label = JSONUtilities.decodeString(jsonRoot, "label", false);
    
    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingPoint))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Point point)
  {
    if (point != null && point.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getDebitable(), point.getDebitable());
        epochChanged = epochChanged || ! Objects.equals(getCreditable(), point.getCreditable());
        epochChanged = epochChanged || ! Objects.equals(getSetable(), point.getSetable());
        epochChanged = epochChanged || ! Objects.equals(getValidity(), point.getValidity());
        epochChanged = epochChanged || ! Objects.equals(getLabel(), point.getLabel());
        return epochChanged;
      }
    else
      {
        return true;
      }
   }



}
