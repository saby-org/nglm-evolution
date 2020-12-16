/*****************************************************************************
*
*  DynamicCriterionField.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class DynamicCriterionField extends GUIManagedObject
{
  /*****************************************
  *
  *  singletonID
  *
  *****************************************/

  public static final String singletonID = "base";

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
    schemaBuilder.name("dynamic_criterion_fields");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("criterionField", CriterionField.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<DynamicCriterionField> serde = new ConnectSerde<DynamicCriterionField>(schema, false, DynamicCriterionField.class, DynamicCriterionField::pack, DynamicCriterionField::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DynamicCriterionField> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CriterionField criterionField;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public CriterionField getCriterionField() { return criterionField; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DynamicCriterionField(GUIManagedObject parent, JSONObject criterionFieldJSON, int tenantID) throws GUIManagerException
  {
    super(criterionFieldJSON, parent.getEpoch(), tenantID);
    this.criterionField = new CriterionField(criterionFieldJSON);
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public DynamicCriterionField(SchemaAndValue schemaAndValue, CriterionField criterionField)
  {
    super(schemaAndValue);
    this.criterionField = criterionField;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    DynamicCriterionField dynamicCriterionField = (DynamicCriterionField) value;
    Struct struct = new Struct(schema);
    packCommon(struct, dynamicCriterionField);
    struct.put("criterionField", CriterionField.pack(dynamicCriterionField.getCriterionField()));
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static DynamicCriterionField unpack(SchemaAndValue schemaAndValue)
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
    CriterionField criterionField = CriterionField.unpack(new SchemaAndValue(schema.field("criterionField").schema(), valueStruct.get("criterionField")));

    //
    //  return
    //

    return new DynamicCriterionField(schemaAndValue, criterionField);
  }
}
