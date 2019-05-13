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
    schemaBuilder.field("maxMessagesPerDay", Schema.INT32_SCHEMA);
    schemaBuilder.field("maxMessagesPerWeek", Schema.INT32_SCHEMA);
    schemaBuilder.field("maxMessagesBiWeekly", Schema.INT32_SCHEMA);
    schemaBuilder.field("maxMessagesPerMonth", Schema.INT32_SCHEMA);
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
  
  private Integer maxMessagesPerDay;
  private Integer maxMessagesPerWeek;
  private Integer maxMessagesBiWeekly;
  private Integer maxMessagesPerMonth;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public Integer getMaxMessagesPerDay() { return maxMessagesPerDay; }
  public Integer getMaxMessagesPerWeek() { return maxMessagesPerWeek; }
  public Integer getMaxMessagesBiWeekly() { return maxMessagesBiWeekly; }
  public Integer getMaxMessagesPerMonth() { return maxMessagesPerMonth; }
  

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public MessageLimits(Integer maxMessagesPerDay, Integer maxMessagesPerWeek, Integer maxMessagesBiWeekly, Integer maxMessagesPerMonth)
  {
    this.maxMessagesPerDay = maxMessagesPerDay;
    this.maxMessagesPerWeek = maxMessagesPerWeek;
    this.maxMessagesBiWeekly = maxMessagesBiWeekly;
    this.maxMessagesPerMonth = maxMessagesPerMonth;
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
    struct.put("maxMessagesPerDay", messageLimit.getMaxMessagesPerDay());
    struct.put("maxMessagesPerWeek", messageLimit.getMaxMessagesPerWeek());
    struct.put("maxMessagesBiWeekly", messageLimit.getMaxMessagesBiWeekly());
    struct.put("maxMessagesPerMonth", messageLimit.getMaxMessagesPerMonth());
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
    Integer maxMessagesPerDay = valueStruct.getInt32("maxMessagesPerDay");
    Integer maxMessagesPerWeek = valueStruct.getInt32("maxMessagesPerWeek");
    Integer maxMessagesBiWeekly = valueStruct.getInt32("maxMessagesBiWeekly");
    Integer maxMessagesPerMonth = valueStruct.getInt32("maxMessagesPerMonth");

    //
    //  return
    //

    return new MessageLimits(maxMessagesPerDay, maxMessagesPerWeek, maxMessagesBiWeekly, maxMessagesPerMonth);
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
    
    this.maxMessagesPerDay = JSONUtilities.decodeInteger(jsonRoot, "maxMessagesPerDay", true);
    this.maxMessagesPerWeek = JSONUtilities.decodeInteger(jsonRoot, "maxMessagesPerWeek", true);
    this.maxMessagesBiWeekly = JSONUtilities.decodeInteger(jsonRoot, "maxMessagesBiWeekly", true);
    this.maxMessagesPerMonth = JSONUtilities.decodeInteger(jsonRoot, "maxMessagesPerMonth", true);
  }
}
