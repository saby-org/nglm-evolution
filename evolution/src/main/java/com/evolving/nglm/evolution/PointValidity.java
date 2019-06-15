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
    schemaBuilder.field("roundUp", Schema.BOOLEAN_SCHEMA);
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
  private boolean roundUp;
  private boolean validityExtension;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private PointValidity(TimeUnit periodType, int periodQuantity, boolean roundUp, boolean validityExtension)
  {
    this.periodType = periodType;
    this.periodQuantity = periodQuantity;
    this.roundUp = roundUp;
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
    this.roundUp = JSONUtilities.decodeBoolean(jsonRoot, "roundUp", true);
    this.validityExtension = JSONUtilities.decodeBoolean(jsonRoot, "validityExtension", true);
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public TimeUnit getPeriodType() { return periodType; }
  public int getPeriodQuantity() { return periodQuantity; }
  public boolean getRoundUp() { return roundUp; }
  public boolean getValidityExtension() { return validityExtension; }

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
    struct.put("roundUp", segment.getRoundUp());
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
    boolean roundUp = valueStruct.getBoolean("roundUp");
    boolean validityExtension = valueStruct.getBoolean("validityExtension");

    //
    //  return
    //

    return new PointValidity(periodType, periodQuantity, roundUp, validityExtension);
  }
}
