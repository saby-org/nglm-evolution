/*****************************************************************************
*
*  CustomCriteria.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;

public class CustomCriteria extends GUIManagedObject
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
    schemaBuilder.name("custom_criteria");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("formula", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CustomCriteria> serde = new ConnectSerde<CustomCriteria>(schema, false, CustomCriteria.class, CustomCriteria::pack, CustomCriteria::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CustomCriteria> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/
  
   private String formula;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getCustomCriteriaID() { return getGUIManagedObjectID(); }
  public String getFormula() { return formula; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public CustomCriteria(SchemaAndValue schemaAndValue, String formula)
  {
    super(schemaAndValue);
    this.formula = formula;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CustomCriteria customCriteria = (CustomCriteria) value;
    Struct struct = new Struct(schema);
    packCommon(struct, customCriteria);;
    struct.put("formula", customCriteria.getFormula());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CustomCriteria unpack(SchemaAndValue schemaAndValue)
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
    String formula = valueStruct.getString("formula");
    //
    //  return
    //

    return new CustomCriteria(schemaAndValue, formula);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public CustomCriteria(JSONObject jsonRoot, long epoch, GUIManagedObject existingCustomCriteriaUnchecked, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingCustomCriteriaUnchecked != null) ? existingCustomCriteriaUnchecked.getEpoch() : epoch, tenantID);

    /*****************************************
    *
    *  existingCustomCriteria
    *
    *****************************************/

    CustomCriteria existingCustomCriteria = (existingCustomCriteriaUnchecked != null && existingCustomCriteriaUnchecked instanceof CustomCriteria) ? (CustomCriteria) existingCustomCriteriaUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    this.formula = JSONUtilities.decodeString(jsonRoot, "formula", false);


    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingCustomCriteria))
      {
        this.setEpoch(epoch);
      }
  }
 

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(CustomCriteria existingCustomCriteria)
  {
    if (existingCustomCriteria != null && existingCustomCriteria.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingCustomCriteria.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getFormula(), existingCustomCriteria.getFormula());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

}
