/*****************************************************************************
*
*  Alarm.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.rii.utilities.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONObject;

import java.util.Date;
import java.util.HashMap;

public class Alarm 
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/
  
  //
  //  AlarmLevel
  //
  
  public enum AlarmLevel
  {
    None(0),
    Alert(1),
    Alarm(2),
    Limit(3),
    Block(4),
    Unknown(-1);
    private int externalRepresentation;
    public int getExternalRepresentation() { return externalRepresentation; }
    AlarmLevel(int externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public static AlarmLevel fromExternalRepresentation(int externalRepresentation) { for (AlarmLevel enumeratedValue : AlarmLevel.values()) { if (enumeratedValue.getExternalRepresentation() == externalRepresentation) return enumeratedValue; } return AlarmLevel.Unknown; }
  }
  //
  //  AlarmType
  //
  
  public enum AlarmType
  {
    License_TimeLimit("license_timelimit"),
    License_ComponentLimit("license_componentlimit"),
    License_NodeLimit("license_nodelimit"),
    License_CapacityLimit("license_capacitylimit"),
    General("general");
    private String stringRepresentation;
    public String getStringRepresentation() { return stringRepresentation; }
    AlarmType(String stringRepresentation) { this.stringRepresentation = stringRepresentation; }
    public static AlarmType fromStringRepresentation(String stringRepresentation) { for (AlarmType enumeratedValue : AlarmType.values()) { if (enumeratedValue.getStringRepresentation().equals(stringRepresentation)) return enumeratedValue; } return AlarmType.General; }
  }

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
    schemaBuilder.name("alarm");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("source", Schema.STRING_SCHEMA);
    schemaBuilder.field("type", Schema.STRING_SCHEMA);
    schemaBuilder.field("level", Schema.INT32_SCHEMA);
    schemaBuilder.field("context", Schema.STRING_SCHEMA);
    schemaBuilder.field("raisedDate", Timestamp.SCHEMA);
    schemaBuilder.field("detail", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  }

  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  String source;
  AlarmType type;
  AlarmLevel level;
  String context;
  Date raisedDate;
  String detail;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSource() { return source; }
  public AlarmType getType() { return type; }
  public AlarmLevel getLevel() { return level; }
  public String getContext() { return context; }
  public Date getRaisedDate() { return raisedDate; }
  public String getDetail() { return detail; }

  /*****************************************
  *
  *  constructor - basic
  *
  *****************************************/

  public Alarm(String source, AlarmType type, AlarmLevel level, String context, Date raisedDate, String detail)
  {
    this.source = source;
    this.type = type;
    this.level = level;
    this.context = context;
    this.raisedDate = raisedDate;
    this.detail = detail;
  }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<Alarm> serde()
  {
    return new ConnectSerde<Alarm>(schema, false, Alarm.class, Alarm::pack, Alarm::unpack);
  }

  /****************************************
  *
  *  pack
  *
  ****************************************/
  
  public static Object pack(Object value)
  {
    Alarm alarm = (Alarm) value;
    Struct struct = new Struct(schema);
    struct.put("source", alarm.getSource());
    struct.put("type", alarm.getType().getStringRepresentation());
    struct.put("level", alarm.getLevel().getExternalRepresentation());
    struct.put("context", alarm.getContext());
    struct.put("raisedDate", alarm.getRaisedDate());
    struct.put("detail", alarm.getDetail());
    return struct;
  }

  /****************************************
  *
  *  constructor -- unpack
  *
  ****************************************/

  public static Alarm unpack(SchemaAndValue schemaAndValue)
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
    String source = valueStruct.getString("source");
    AlarmType type = AlarmType.fromStringRepresentation(valueStruct.getString("type"));
    AlarmLevel level = AlarmLevel.fromExternalRepresentation(valueStruct.getInt32("level"));
    String context = valueStruct.getString("context");
    Date raisedDate = (Date) valueStruct.get("raisedDate");
    String detail = valueStruct.getString("detail");
    
    //
    //  return
    //

    return new Alarm(source, type, level, context, raisedDate, detail);
  }

  public Alarm (JSONObject jsonRoot) throws JSONUtilitiesException
  {
    this.source = JSONUtilities.decodeString(jsonRoot, "source", true);
    this.type = AlarmType.fromStringRepresentation(JSONUtilities.decodeString(jsonRoot, "type", true));
    this.level = AlarmLevel.fromExternalRepresentation(JSONUtilities.decodeInteger(jsonRoot, "level", true));
    this.context = JSONUtilities.decodeString(jsonRoot, "context", true);
    this.raisedDate = JSONUtilities.decodeDate(jsonRoot, "raisedDate", true);
    this.detail = JSONUtilities.decodeString(jsonRoot, "detail", false);
  }

  /*****************************************
  *
  *  getJSONRepresentation
  *
  *****************************************/
    
  public JSONObject getJSONRepresentation()
  {
    HashMap<String,Object> json = new HashMap<String,Object>();
    json.put("source", source);
    json.put("type", type.getStringRepresentation());
    json.put("level", new Integer(level.getExternalRepresentation()));
    json.put("context", context);
    json.put("raisedDate", raisedDate.getTime());
    json.put("detail", detail);
    return JSONUtilities.encodeObject(json);
  }
}
