/*****************************************************************************
*
*  SubscriberTraffic.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;

public class SubscriberTraffic
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
    schemaBuilder.name("SubscriberTraffic");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(0));
    schemaBuilder.field("subscriberInflow", Schema.INT32_SCHEMA);
    schemaBuilder.field("subscriberOutflow", Schema.INT32_SCHEMA);
    schemaBuilder.field("rewardsInflow", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).name("map_string_integer").schema());
    schemaBuilder.field("rewardsOutflow", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).name("map_string_integer").schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SubscriberTraffic> serde = new ConnectSerde<SubscriberTraffic>(schema, false, SubscriberTraffic.class, SubscriberTraffic::pack, SubscriberTraffic::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberTraffic> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/
  
  private Integer subscriberInflow;
  private Integer subscriberOutflow;
  private Map<String,Integer> rewardsInflow;
  private Map<String,Integer> rewardsOutflow;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public Integer getSubscriberInflow() { return subscriberInflow; }
  public Integer getSubscriberOutflow() { return subscriberOutflow; }
  public Map<String,Integer> getRewardsInflow() { return rewardsInflow; }
  public Map<String,Integer> getRewardsOutflow() { return rewardsOutflow; }

  /****************************************
  *
  *  setters
  *
  ****************************************/

  public void addInflow() { this.subscriberInflow++; }
  public void addOutflow() { this.subscriberOutflow++; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SubscriberTraffic(Integer subscriberInflow, Integer subscriberOutflow, Map<String,Integer> rewardsInflow, Map<String,Integer> rewardsOutflow)
  {
    this.subscriberInflow = subscriberInflow;
    this.subscriberOutflow = subscriberOutflow;
    this.rewardsInflow = rewardsInflow;
    this.rewardsOutflow = rewardsOutflow;
  }

  /*****************************************
  *
  *  constructor -- empty
  *
  *****************************************/

  public SubscriberTraffic()
  {
    this(0, 0, new HashMap<String, Integer>(), new HashMap<String, Integer>());
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public SubscriberTraffic(SubscriberTraffic copy)
  {
    this.subscriberInflow = new Integer(copy.getSubscriberInflow());
    this.subscriberOutflow = new Integer(copy.getSubscriberOutflow());
    this.rewardsInflow = new HashMap<String,Integer>();
    this.rewardsOutflow = new HashMap<String,Integer>();
    // deep copy
    for(String key : copy.getRewardsInflow().keySet()) {
      this.rewardsInflow.put(key, new Integer(copy.getRewardsInflow().get(key)));
    }
    // deep copy
    for(String key : copy.getRewardsOutflow().keySet()) {
      this.rewardsOutflow.put(key, new Integer(copy.getRewardsOutflow().get(key)));
    }
  }
  
  /*****************************************
  *
  *  getJSONRepresentation
  *
  *****************************************/
    
  public JSONObject getJSONRepresentation()
  {
    HashMap<String,Object> json = new HashMap<String,Object>();
    json.put("subscriberInflow", subscriberInflow);
    json.put("subscriberOutflow", subscriberOutflow);
    json.put("rewardsInflow", JSONUtilities.encodeObject(rewardsInflow));
    json.put("rewardsOutflow", JSONUtilities.encodeObject(rewardsOutflow));
    return JSONUtilities.encodeObject(json);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SubscriberTraffic obj = (SubscriberTraffic) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberInflow", obj.getSubscriberInflow());
    struct.put("subscriberOutflow", obj.getSubscriberOutflow());
    struct.put("rewardsInflow", obj.getRewardsInflow());
    struct.put("rewardsOutflow", obj.getRewardsOutflow());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SubscriberTraffic unpack(SchemaAndValue schemaAndValue)
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
    Integer subscriberInflow = valueStruct.getInt32("subscriberInflow");
    Integer subscriberOutflow = valueStruct.getInt32("subscriberOutflow");
    Map<String,Integer> rewardsInflow = (Map<String,Integer>) valueStruct.get("rewardsInflow");
    Map<String,Integer> rewardsOutflow = (Map<String,Integer>) valueStruct.get("rewardsOutflow");
    
    //
    //  return
    //

    return new SubscriberTraffic(subscriberInflow, subscriberOutflow, rewardsInflow, rewardsOutflow);
  }

}