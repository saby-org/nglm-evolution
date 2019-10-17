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

public class PointValidity 
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
    schemaBuilder.name("point_validity");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("periodType", Schema.STRING_SCHEMA);
    schemaBuilder.field("periodQuantity", Schema.INT32_SCHEMA);
    schemaBuilder.field("roundDown", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("validityExtension", Schema.BOOLEAN_SCHEMA);
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

  private TimeUnit periodType;
  private int periodQuantity;
  private boolean roundDown;
  private boolean validityExtension;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private PointValidity(TimeUnit periodType, int periodQuantity, boolean roundDown, boolean validityExtension)
  {
    this.periodType = periodType;
    this.periodQuantity = periodQuantity;
    this.roundDown = roundDown;
    this.validityExtension = validityExtension;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  PointValidity(JSONObject jsonRoot) throws GUIManagerException
  {
    this.periodType = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "periodType", true));
    this.periodQuantity = JSONUtilities.decodeInteger(jsonRoot, "periodQuantity", true);
    this.roundDown = JSONUtilities.decodeBoolean(jsonRoot, "roundDown", true);
    this.validityExtension = JSONUtilities.decodeBoolean(jsonRoot, "validityExtension", true);
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private PointValidity(PointValidity pointValidity)
  {
    this.periodType = pointValidity.getPeriodType();
    this.periodQuantity = pointValidity.getPeriodQuantity();
    this.roundDown = pointValidity.getRoundDown();
    this.validityExtension = pointValidity.getValidityExtension();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public PointValidity copy()
  {
    return new PointValidity(this);
  }

  /*****************************************
  *
  *  getters
  *
  *****************************************/

  public TimeUnit getPeriodType() { return periodType; }
  public int getPeriodQuantity() { return periodQuantity; }
  public boolean getRoundDown() { return roundDown; }
  public boolean getValidityExtension() { return validityExtension; }

  /*****************************************
  *
  *  setters
  *
  *****************************************/

  public void setPeriodType(TimeUnit periodType) { this.periodType = periodType; }
  public void setPeriodQuantity(int periodQuantity) { this.periodQuantity = periodQuantity; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<PointValidity> serde()
  {
    return new ConnectSerde<PointValidity>(schema, false, PointValidity.class, PointValidity::pack, PointValidity::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PointValidity segment = (PointValidity) value;
    Struct struct = new Struct(schema);
    struct.put("periodType", segment.getPeriodType().getExternalRepresentation());
    struct.put("periodQuantity", segment.getPeriodQuantity());
    struct.put("roundDown", segment.getRoundDown());
    struct.put("validityExtension", segment.getValidityExtension());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PointValidity unpack(SchemaAndValue schemaAndValue)
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
    TimeUnit periodType = TimeUnit.fromExternalRepresentation(valueStruct.getString("periodType"));
    int periodQuantity = valueStruct.getInt32("periodQuantity");
    boolean roundDown = valueStruct.getBoolean("roundDown");
    boolean validityExtension = valueStruct.getBoolean("validityExtension");

    //
    //  return
    //

    return new PointValidity(periodType, periodQuantity, roundDown, validityExtension);
  }
}
