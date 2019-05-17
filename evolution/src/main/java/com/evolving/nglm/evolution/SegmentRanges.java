/*****************************************************************************
*
*  SegmentRanges.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class SegmentRanges implements Segment
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SegmentRanges.class);

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
    schemaBuilder.name("segment_ranges");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("id", Schema.STRING_SCHEMA);
    schemaBuilder.field("name", Schema.STRING_SCHEMA);
    schemaBuilder.field("range_min", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("range_max", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("contactPolicyID", Schema.OPTIONAL_STRING_SCHEMA);
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

  private String id;
  private String name;
  private Integer range_min;
  private Integer range_max;
  private String contactPolicyID;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private SegmentRanges(String id, String name, Integer range_min, Integer range_max, String contactPolicyID)
  {
    this.id = id;
    this.name = name;
    this.range_min = range_min;
    this.range_max = range_max;
    this.contactPolicyID = contactPolicyID;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  SegmentRanges(JSONObject jsonRoot) throws GUIManagerException
  {
    this.id = JSONUtilities.decodeString(jsonRoot, "id", true);
    this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
    this.range_min = JSONUtilities.decodeInteger(jsonRoot, "range_min", false);
    this.range_max = JSONUtilities.decodeInteger(jsonRoot, "range_max", false);
    this.contactPolicyID = JSONUtilities.decodeString(jsonRoot, "contactPolicyID", false);
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getID() { return id; }
  public String getName() { return name; }
  public Integer getRangeMin() { return range_min; }
  public Integer getRangeMax() { return range_max; }
  public String getContactPolicyID() { return contactPolicyID; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<SegmentRanges> serde()
  {
    return new ConnectSerde<SegmentRanges>(schema, false, SegmentRanges.class, SegmentRanges::pack, SegmentRanges::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SegmentRanges segment = (SegmentRanges) value;
    Struct struct = new Struct(schema);
    struct.put("id", segment.getID());
    struct.put("name", segment.getName());
    struct.put("range_min", segment.getRangeMin());
    struct.put("range_max", segment.getRangeMax());
    struct.put("contactPolicyID", segment.getContactPolicyID());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SegmentRanges unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack all but argument
    //

    if(value == null){
      return null;
    }
    Struct valueStruct = (Struct) value;
    String id = valueStruct.getString("id");
    String name = valueStruct.getString("name");
    Integer range_min = valueStruct.getInt32("range_min");
    Integer range_max = valueStruct.getInt32("range_max");
    String contactPolicyID = valueStruct.getString("contactPolicyID");

    //
    //  construct
    //

    SegmentRanges result = new SegmentRanges(id, name, range_min, range_max, contactPolicyID);

    //
    //  return
    //

    return result;
  }

}
