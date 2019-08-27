/*****************************************************************************
*
*  DNBOMatrixRange.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class DNBOMatrixRange
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
    schemaBuilder.name("dnbo_matrixrange");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("rangeId", Schema.STRING_SCHEMA);
    schemaBuilder.field("rangeStartValue", Schema.STRING_SCHEMA);
    schemaBuilder.field("rangeEndValue", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("segments", SchemaBuilder.array(DNBOMatrixSegment.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<DNBOMatrixRange> serde = new ConnectSerde<DNBOMatrixRange>(schema, false, DNBOMatrixRange.class, DNBOMatrixRange::pack, DNBOMatrixRange::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DNBOMatrixRange> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String rangeId;
  private String rangeStartValue;
  private String rangeEndValue;
  private List<DNBOMatrixSegment> segments;
  

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getRangeId()                   {return rangeId; }
  public String getRangeStartValue()           {return rangeStartValue; }
  public String getRangeEndValue()             {return rangeEndValue; }
  public List<DNBOMatrixSegment> getSegments() {return segments; }
  

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DNBOMatrixRange(String rangeId, String rangeStartValue, String rangeEndValue, List<DNBOMatrixSegment> segments)
  {
    this.rangeId = rangeId;
    this.rangeStartValue = rangeStartValue;
    this.rangeEndValue = rangeEndValue;
    this.segments = segments;
  }
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    DNBOMatrixRange dnboMatrixRange = (DNBOMatrixRange) value;
    Struct struct = new Struct(schema);
    struct.put("rangeId", dnboMatrixRange.getRangeId());
    struct.put("rangeStartValue", dnboMatrixRange.getRangeStartValue());
    struct.put("rangeEndValue", dnboMatrixRange.getRangeEndValue());
    struct.put("segments", packSegments(dnboMatrixRange.getSegments()));
    return struct;
  }

  /****************************************
  *
  *  packSegments
  *
  ****************************************/

  private static List<Object> packSegments(List<DNBOMatrixSegment> dnboMatrixSegments)
  {
    List<Object> result = new ArrayList<Object>();
    for (DNBOMatrixSegment dnboMatrixSegment : dnboMatrixSegments)
      {
        result.add(DNBOMatrixSegment.pack(dnboMatrixSegment));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static DNBOMatrixRange unpack(SchemaAndValue schemaAndValue)
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
    String rangeId = (String) valueStruct.get("rangeId");
    String rangeStartValue = (String) valueStruct.get("rangeStartValue");
    String rangeEndValue = (String) valueStruct.get("rangeEndValue");

    List<DNBOMatrixSegment> segments = unpackSegments(schema.field("segments").schema(), valueStruct.get("segments"));

    //
    //  return
    //

    return new DNBOMatrixRange(rangeId, rangeStartValue, rangeEndValue, segments);
  }

  /*****************************************
  *
  *  unpackSegments
  *
  *****************************************/

  private static List<DNBOMatrixSegment> unpackSegments(Schema schema, Object value)
  {
    //
    //  get schema for DNBOMatrixSegment
    //

    Schema dnboMatrixSegmentSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<DNBOMatrixSegment> result = new ArrayList<DNBOMatrixSegment>();
    List<Object> valueArray = (List<Object>) value;
    for (Object dnboMatrixSegment : valueArray)
      {
        result.add(DNBOMatrixSegment.unpack(new SchemaAndValue(dnboMatrixSegmentSchema, dnboMatrixSegment)));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DNBOMatrixRange(JSONObject jsonRoot) throws GUIManagerException
  {
    this.rangeId = JSONUtilities.decodeString(jsonRoot, "rangeId", true);
    this.rangeStartValue = JSONUtilities.decodeString(jsonRoot, "rangeStartValue", true);
    this.rangeEndValue = JSONUtilities.decodeString(jsonRoot, "rangeEndValue", false); // optional : means unlimited
    this.segments = decodeSegments(JSONUtilities.decodeJSONArray(jsonRoot, "segments", true));
  }

  /*****************************************
  *
  *  decodeSegments
  *
  *****************************************/

  private List<DNBOMatrixSegment> decodeSegments(JSONArray jsonArray) throws GUIManagerException
  {
    List<DNBOMatrixSegment> result = new ArrayList<DNBOMatrixSegment>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new DNBOMatrixSegment((JSONObject) jsonArray.get(i)));
      }
    return result;
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof DNBOMatrixRange)
      {
        DNBOMatrixRange dnboMatrixRange = (DNBOMatrixRange) obj;
        result = true;
        result = result && Objects.equals(rangeId, dnboMatrixRange.getRangeId());
        result = result && Objects.equals(rangeStartValue, dnboMatrixRange.getRangeStartValue());
        result = result && Objects.equals(rangeEndValue, dnboMatrixRange.getRangeEndValue());
        result = result && Objects.equals(segments, dnboMatrixRange.getSegments());
      }
    return result;
  }
}
