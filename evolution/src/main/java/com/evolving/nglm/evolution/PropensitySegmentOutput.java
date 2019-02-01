/*****************************************************************************
*
*  PropensitySegmentOutput.java
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

public class PropensitySegmentOutput 
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
    schemaBuilder.name("propensitysegment_output");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("dimensionID", Schema.STRING_SCHEMA);
    schemaBuilder.field("segmentID", Schema.STRING_SCHEMA);
    schemaBuilder.field("offerID", Schema.STRING_SCHEMA);
    schemaBuilder.field("accepted", Schema.BOOLEAN_SCHEMA);
    schema = schemaBuilder.build();
  };
  
  //
  //  serde
  //

  private static ConnectSerde<PropensitySegmentOutput> serde = new ConnectSerde<PropensitySegmentOutput>(schema, false, PropensitySegmentOutput.class, PropensitySegmentOutput::pack, PropensitySegmentOutput::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PropensitySegmentOutput> serde() { return serde; }
  
  /****************************************
  *
  *  data
  *
  ****************************************/

  private String dimensionID;
  private String segmentID;
  private String offerID;
  private boolean accepted;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getDimensionID() { return dimensionID; }
  public String getSegmentID() { return segmentID; }
  public String getOfferID() { return offerID; }
  public boolean isAccepted() { return accepted; }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PropensitySegmentOutput(String offerID, String dimensionID, String segmentID, boolean accepted)
  {
    this.offerID = offerID;
    this.dimensionID = dimensionID;
    this.segmentID = segmentID;
    this.accepted = accepted;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PropensitySegmentOutput propensitySegmentOutput = (PropensitySegmentOutput) value;
    Struct struct = new Struct(schema);
    struct.put("offerID", propensitySegmentOutput.getOfferID());
    struct.put("dimensionID", propensitySegmentOutput.getDimensionID());
    struct.put("segmentID", propensitySegmentOutput.getSegmentID());
    struct.put("accepted", propensitySegmentOutput.isAccepted());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PropensitySegmentOutput unpack(SchemaAndValue schemaAndValue)
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
    String dimensionID = valueStruct.getString("dimensionID");
    String segmentID = valueStruct.getString("segmentID");
    String offerID = valueStruct.getString("offerID");
    boolean accepted = valueStruct.getBoolean("accepted");
    
    //
    //  return
    //

    return new PropensitySegmentOutput(offerID, dimensionID, segmentID, accepted);
  }

}
