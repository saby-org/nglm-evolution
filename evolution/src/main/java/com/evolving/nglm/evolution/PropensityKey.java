
package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

public class PropensityKey
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
    schemaBuilder.name("propensity_key");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("offerid", Schema.STRING_SCHEMA);
    schemaBuilder.field("segment", Schema.STRING_SCHEMA);
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
  
  private String offerid;
  private String segment;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PropensityKey(String offerid, String segment)
  {
    this.offerid = offerid;
    this.segment = segment;
  }
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getOfferID() { return offerid; }
  public String getSegment() { return segment; }
  
  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<PropensityKey> serde()
  {
    return new ConnectSerde<PropensityKey>(schema, true, PropensityKey.class, PropensityKey::pack, PropensityKey::unpack);
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PropensityKey propensityKey = (PropensityKey) value;
    Struct struct = new Struct(schema);
    struct.put("offerid", propensityKey.getOfferID());
    struct.put("segment", propensityKey.getSegment());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PropensityKey unpack(SchemaAndValue schemaAndValue)
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
    String offerid = valueStruct.getString("offerid");
    String segment = valueStruct.getString("segment");

    //
    //  return
    //

    return new PropensityKey(offerid, segment);
  }
  
  /*****************************************
  *
  *  equals/hashCode
  *
  *****************************************/
  
  //
  //  equals
  //

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof PropensityKey)
      {
        PropensityKey propensityKey = (PropensityKey) obj;
        result = offerid.equals(propensityKey.getOfferID()) && segment.equals(propensityKey.getSegment());
      }
    return result;
  }
  
  //
  //  hashCode
  //
  
  public int hashCode()
  {
    return (offerid+segment).hashCode();
  }
}
