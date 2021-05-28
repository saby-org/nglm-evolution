/*****************************************************************************
*
*  PointValidity.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;

public class ProductValidity 
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
    //
    //  schema
    //
    
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("product_validity");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("validityPeriod", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("validityType", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private TimeUnit validityType;
  private Integer validityPeriod;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private ProductValidity(TimeUnit validityType, Integer validityPeriod)
  {
    this.validityType = validityType;
    this.validityPeriod = validityPeriod;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  ProductValidity(JSONObject jsonRoot) throws GUIManagerException
  {
    this.validityType = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "validityType", "(unknown)"));
    this.validityPeriod = JSONUtilities.decodeInteger(jsonRoot, "validityPeriod", false);
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private ProductValidity(ProductValidity pointValidity)
  {
    this.validityType = pointValidity.getValidityType();
    this.validityPeriod = pointValidity.getValidityPeriod();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public ProductValidity copy()
  {
    return new ProductValidity(this);
  }

  /*****************************************
  *
  *  getters
  *
  *****************************************/

  public TimeUnit getValidityType() { return validityType; }
  public Integer getValidityPeriod() { return validityPeriod; }

  /*****************************************
  *
  *  setters
  *
  *****************************************/

  public void setValidityType(TimeUnit validityType) { this.validityType = validityType; }
  public void setValidityPeriod(int validityPeriod) { this.validityPeriod = validityPeriod; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<ProductValidity> serde()
  {
    return new ConnectSerde<ProductValidity>(schema, false, ProductValidity.class, ProductValidity::pack, ProductValidity::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ProductValidity segment = (ProductValidity) value;
    Struct struct = new Struct(schema);
    struct.put("validityType",segment != null && segment.validityType != null ? segment.validityType.getExternalRepresentation() : null);
    struct.put("validityPeriod", segment != null && segment.validityPeriod != null ? segment.validityPeriod : null);
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ProductValidity unpack(SchemaAndValue schemaAndValue)
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
    Integer validityPeriod = valueStruct.getInt32("validityPeriod");
    TimeUnit validityType = TimeUnit.fromExternalRepresentation(valueStruct.getString("validityType"));

    //
    //  return
    //

    return new ProductValidity(validityType, validityPeriod);
  }
}
