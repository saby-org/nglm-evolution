/*****************************************************************************
*
*  VoucherValidity.java
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

public class VoucherValidity 
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
    schemaBuilder.name("voucher_validity");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("periodType", Schema.STRING_SCHEMA);
    schemaBuilder.field("periodQuantity", Schema.INT32_SCHEMA);
    schemaBuilder.field("roundDown", Schema.BOOLEAN_SCHEMA);
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

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private VoucherValidity(TimeUnit periodType, int periodQuantity, boolean roundDown)
  {
    this.periodType = periodType;
    this.periodQuantity = periodQuantity;
    this.roundDown = roundDown;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  VoucherValidity(JSONObject jsonRoot) throws GUIManagerException
  {
    this.periodType = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "periodType", true));
    this.periodQuantity = JSONUtilities.decodeInteger(jsonRoot, "periodQuantity", true);
    this.roundDown = JSONUtilities.decodeBoolean(jsonRoot, "roundDown", true);
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private VoucherValidity(VoucherValidity voucherValidity)
  {
    this.periodType = voucherValidity.getPeriodType();
    this.periodQuantity = voucherValidity.getPeriodQuantity();
    this.roundDown = voucherValidity.getRoundDown();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public VoucherValidity copy()
  {
    return new VoucherValidity(this);
  }

  /*****************************************
  *
  *  getters
  *
  *****************************************/

  public TimeUnit getPeriodType() { return periodType; }
  public int getPeriodQuantity() { return periodQuantity; }
  public boolean getRoundDown() { return roundDown; }

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

  public static ConnectSerde<VoucherValidity> serde()
  {
    return new ConnectSerde<VoucherValidity>(schema, false, VoucherValidity.class, VoucherValidity::pack, VoucherValidity::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    VoucherValidity segment = (VoucherValidity) value;
    Struct struct = new Struct(schema);
    struct.put("periodType", segment.getPeriodType().getExternalRepresentation());
    struct.put("periodQuantity", segment.getPeriodQuantity());
    struct.put("roundDown", segment.getRoundDown());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static VoucherValidity unpack(SchemaAndValue schemaAndValue)
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

    //
    //  return
    //

    return new VoucherValidity(periodType, periodQuantity, roundDown);
  }
}
