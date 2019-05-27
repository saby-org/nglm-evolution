/*****************************************************************************
*
*  PropensityState.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.ReferenceDataValue;
import com.evolving.nglm.core.SchemaUtilities;

public class PropensityState implements ReferenceDataValue<PropensityKey>
{

  /*****************************************
  *
  *  static
  *
  *****************************************/

  public static void forceClassLoad() {}

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
    schemaBuilder.name("propensity_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("propensityKey", PropensityKey.schema());
    schemaBuilder.field("acceptanceCount", Schema.INT64_SCHEMA);
    schemaBuilder.field("presentationCount", Schema.INT64_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PropensityState> serde = new ConnectSerde<PropensityState>(schema, false, PropensityState.class, PropensityState::pack, PropensityState::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PropensityState> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private PropensityKey propensityKey;
  private Long acceptanceCount;
  private Long presentationCount;

  /****************************************
  *
  *  accessors - basic
  *
  ****************************************/

  //
  //  Getters
  //
  
  public PropensityKey getPropensityKey() { return propensityKey; }
  public Long getAcceptanceCount() { return acceptanceCount; }
  public Long getPresentationCount() { return presentationCount; }
  
  //
  //  Setters
  //
  
  public void setAcceptanceCount(long acceptanceCount) { this.acceptanceCount = new Long(acceptanceCount); }
  public void setPresentationCount(long presentationCount) { this.presentationCount = new Long(presentationCount); }

  //
  //  Propensity computation
  //
  
  public Double getPropensity()
  {
    if (getPresentationCount().equals(0L)) return null; // TODO return initial propensity for OfferID de pro
    return new Double(getAcceptanceCount() / getPresentationCount());
  }
  
  //
  //  ReferenceDataValue
  //

  @Override public PropensityKey getKey()
  {
    return propensityKey;
  }

  /*****************************************
  *
  *  constructor (simple)
  *
  *****************************************/

  public PropensityState(PropensityKey propensityKey)
  {
    this.propensityKey = propensityKey;
    this.presentationCount = new Long(0L);
    this.acceptanceCount = new Long(0L);
  }

  /*****************************************
  *
  *  constructor (unpack)
  *
  *****************************************/

  private PropensityState(PropensityKey propensityKey, Long acceptanceCount, Long presentationCount)
  {
    this.propensityKey = propensityKey;
    this.acceptanceCount = acceptanceCount;
    this.presentationCount = presentationCount;
  }

  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  public PropensityState(PropensityState propensityState)
  {
    this.propensityKey = propensityState.getPropensityKey();
    this.acceptanceCount = propensityState.getAcceptanceCount();
    this.presentationCount = propensityState.getPresentationCount();
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PropensityState propensityState = (PropensityState) value;
    Struct struct = new Struct(schema);
    struct.put("propensityKey", PropensityKey.pack(propensityState.getPropensityKey()));
    struct.put("acceptanceCount", propensityState.getAcceptanceCount());
    struct.put("presentationCount", propensityState.getPresentationCount());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PropensityState unpack(SchemaAndValue schemaAndValue)
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
    PropensityKey propensityKey = PropensityKey.unpack(new SchemaAndValue(schema.field("propensityKey").schema(), valueStruct.get("propensityKey")));
    Long acceptanceCount = valueStruct.getInt64("acceptanceCount");
    Long presentationCount = valueStruct.getInt64("presentationCount");

    //
    //  return
    //

    return new PropensityState(propensityKey, acceptanceCount, presentationCount);
  }
}
