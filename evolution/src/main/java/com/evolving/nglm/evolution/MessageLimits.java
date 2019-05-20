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

public class MessageLimits
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
    schemaBuilder.name("message_limits");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("maxMessages", Schema.INT32_SCHEMA);
    schemaBuilder.field("duration", Schema.INT32_SCHEMA);
    schemaBuilder.field("timeUnit", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<MessageLimits> serde = new ConnectSerde<MessageLimits>(schema, false, MessageLimits.class, MessageLimits::pack, MessageLimits::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<MessageLimits> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private Integer maxMessages;
  private Integer duration;
  private String timeUnit;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public Integer getMaxMessages() { return maxMessages; }
  public Integer getDuration() { return duration; }
  public String getTimeUnit() { return timeUnit; }
  

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public MessageLimits(Integer maxMessages, Integer duration, String timeUnit)
  {
    this.maxMessages = maxMessages;
    this.duration = duration;
    this.timeUnit = timeUnit;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    MessageLimits messageLimit = (MessageLimits) value;
    Struct struct = new Struct(schema);
    struct.put("maxMessages", messageLimit.getMaxMessages());
    struct.put("duration", messageLimit.getDuration());
    struct.put("timeUnit", messageLimit.getTimeUnit());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static MessageLimits unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    Integer maxMessages = valueStruct.getInt32("maxMessages");
    Integer duration = valueStruct.getInt32("duration");
    String timeUnit = valueStruct.getString("timeUnit");

    //
    //  return
    //

    return new MessageLimits(maxMessages, duration, timeUnit);
  }
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public MessageLimits(JSONObject jsonRoot) throws GUIManagerException
  {
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    this.maxMessages = JSONUtilities.decodeInteger(jsonRoot, "maxMessages", true);
    this.duration = JSONUtilities.decodeInteger(jsonRoot, "duration", true);
    this.timeUnit = JSONUtilities.decodeString(jsonRoot, "timeUnit", true);
  }
}
