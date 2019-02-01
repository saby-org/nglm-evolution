
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
    schemaBuilder.field("offerID", Schema.STRING_SCHEMA);
    schemaBuilder.field("dimensionID", Schema.STRING_SCHEMA);
    schemaBuilder.field("segmentID", Schema.STRING_SCHEMA);
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
  
  private String offerID;
  private String dimensionID;
  private String segmentID;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PropensityKey(String offerID, String dimensionID, String segmentID)
  {
    this.offerID = offerID;
    this.dimensionID = dimensionID;
    this.segmentID = segmentID;
  }
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getOfferID() { return offerID; }
  public String getDimensionID() { return dimensionID; }
  public String getSegmentID() { return segmentID; }
  
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
    struct.put("offerID", propensityKey.getOfferID());
    struct.put("dimensionID", propensityKey.getDimensionID());
    struct.put("segmentID", propensityKey.getSegmentID());
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
    String offerid = valueStruct.getString("offerID");
    String dimension = valueStruct.getString("dimensionID");
    String segment = valueStruct.getString("segmentID");

    //
    //  return
    //

    return new PropensityKey(offerid, dimension, segment);
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
        result = offerID.equals(propensityKey.getOfferID()) && dimensionID.equals(propensityKey.getDimensionID()) && segmentID.equals(propensityKey.getSegmentID());
      }
    return result;
  }
  
  //
  //  hashCode
  //
  
  public int hashCode()
  {
    return (offerID+dimensionID+segmentID).hashCode();
  }
}
