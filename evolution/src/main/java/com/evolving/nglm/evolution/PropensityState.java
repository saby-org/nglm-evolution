/*****************************************************************************
*
*  PropensityState.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.evolving.nglm.core.ConnectSerde;
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
    schemaBuilder.field("offerid", Schema.STRING_SCHEMA);
    schemaBuilder.field("segment", Schema.STRING_SCHEMA);
    schemaBuilder.field("acceptanceCount", Schema.OPTIONAL_INT64_SCHEMA);
    schemaBuilder.field("presentationCount", Schema.OPTIONAL_INT64_SCHEMA);
    schemaBuilder.field("propensity", Schema.OPTIONAL_FLOAT64_SCHEMA);
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

  private String offerid;
  private String segment;
  private Long acceptanceCount;
  private Long presentationCount;
  private Double propensity;
  
  /****************************************
  *
  *  accessors - basic
  *
  ****************************************/

  public String getOfferID() { return offerid; }
  public String getSegment() { return segment; }
  public Long getAcceptanceCount() { return null == acceptanceCount ? new Long(0) : acceptanceCount; }
  public Long getPresentationCount() { return null == presentationCount ? new Long(0) : presentationCount; }
  public Double getPropensity() 
  { 
    if (getPresentationCount().equals(0L)) return new Double(0.0);
    return new Double(getAcceptanceCount() / getPresentationCount());
  }
  
  public void setAcceptanceCount(Long acceptanceCount) { this.acceptanceCount = acceptanceCount; }
  public void setPresentationCount(Long presentationCount) { this.presentationCount = presentationCount; }
  
  //
  //  ReferenceDataValue
  //

  @Override public PropensityKey getKey() 
  {
    return new PropensityKey(offerid, segment);
  }
  
  /*****************************************
  *
  *  constructor (simple)
  *
  *****************************************/

  public PropensityState(String offerid, String segment)
  {
    this.offerid = offerid;
    this.segment = segment;
  }
  
  /*****************************************
  *
  *  constructor (unpack)
  *
  *****************************************/

  private PropensityState(String offerid, String segment, Long acceptanceCount, Long presentationCount, Double propensity)
  {
    this.offerid = offerid;
    this.segment = segment;
    this.acceptanceCount = acceptanceCount;
    this.presentationCount = presentationCount;
    this.propensity = propensity;
  }
  
  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  public PropensityState(PropensityState propensityState)
  {
    this.offerid = propensityState.getOfferID();
    this.segment = propensityState.getSegment();
    this.acceptanceCount = propensityState.getAcceptanceCount();
    this.presentationCount = propensityState.getPresentationCount();
    this.propensity = propensityState.getPropensity();
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
    struct.put("offerid", propensityState.getOfferID());
    struct.put("segment", propensityState.getSegment());
    struct.put("acceptanceCount", propensityState.getAcceptanceCount());
    struct.put("presentationCount", propensityState.getPresentationCount());
    struct.put("propensity", propensityState.getPropensity());
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
    String offerid = valueStruct.getString("offerid");
    String segment = valueStruct.getString("segment");
    Long acceptanceCount = valueStruct.getInt64("acceptanceCount");
    Long presentationCount = valueStruct.getInt64("presentationCount");
    Double propensity = valueStruct.getFloat64("propensity");

    //
    //  return
    //

    return new PropensityState(offerid, segment, acceptanceCount, presentationCount, propensity);
  }
}
