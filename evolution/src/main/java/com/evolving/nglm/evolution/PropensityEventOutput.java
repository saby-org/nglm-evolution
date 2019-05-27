/*****************************************************************************
*
*  PropensityEventOutput.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;

public class PropensityEventOutput implements SubscriberStreamOutput
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
    schemaBuilder.name("propensityevent_output");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("propensityKey", PropensityKey.schema());
    schemaBuilder.field("accepted", Schema.BOOLEAN_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PropensityEventOutput> serde = new ConnectSerde<PropensityEventOutput>(schema, false, PropensityEventOutput.class, PropensityEventOutput::pack, PropensityEventOutput::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PropensityEventOutput> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private PropensityKey propensityKey;
  private boolean accepted;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public PropensityKey getPropensityKey() { return propensityKey; }
  public boolean isAccepted() { return accepted; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PropensityEventOutput(PropensityKey propensityKey, boolean accepted)
  {
    this.propensityKey = propensityKey;
    this.accepted = accepted;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PropensityEventOutput propensityEventOutput = (PropensityEventOutput) value;
    Struct struct = new Struct(schema);
    struct.put("propensityKey", PropensityKey.pack(propensityEventOutput.getPropensityKey()));
    struct.put("accepted", propensityEventOutput.isAccepted());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PropensityEventOutput unpack(SchemaAndValue schemaAndValue)
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
    boolean accepted = valueStruct.getBoolean("accepted");
    
    //
    //  return
    //

    return new PropensityEventOutput(propensityKey, accepted);
  }

}
