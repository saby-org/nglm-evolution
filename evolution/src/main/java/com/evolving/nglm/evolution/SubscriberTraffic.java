/*****************************************************************************
*
*  SubscriberTraffic.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
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
    schemaBuilder.field("distributedRewards", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).name("map_string_integer").optional().schema());
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
  private Map<String,Integer> distributedRewards;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public Integer getSubscriberInflow() { return subscriberInflow; }
  public Integer getSubscriberOutflow() { return subscriberOutflow; }
  public Map<String,Integer> getDistributedRewards() { return distributedRewards; }

  /****************************************
  *
  *  setters
  *
  ****************************************/

  public void addInflow() { this.subscriberInflow++; }
  public void addOutflow() { this.subscriberOutflow++; }
  public void setEmptyRewardsMap() { this.distributedRewards = new HashMap<String, Integer>(); }
  public void addRewards(String rewardID, int amount) {
    if(this.distributedRewards == null) 
      {
        this.distributedRewards = new HashMap<String, Integer>();
      }
    
    int currentCount = (this.distributedRewards.get(rewardID) != null)? this.distributedRewards.get(rewardID) : 0;
    this.distributedRewards.put(rewardID, new Integer(currentCount + amount));
  }
  
  /*****************************************
  *
  *  getSubscriberCount
  *
  *****************************************/
  public int getSubscriberCount() { return subscriberInflow - subscriberOutflow; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SubscriberTraffic(Integer subscriberInflow, Integer subscriberOutflow, Map<String,Integer> distributedRewards)
  {
    this.subscriberInflow = subscriberInflow;
    this.subscriberOutflow = subscriberOutflow;
    this.distributedRewards = distributedRewards;
  }

  /*****************************************
  *
  *  constructor -- empty
  *
  *****************************************/

  public SubscriberTraffic()
  {
    this(0, 0, null);
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
    this.distributedRewards = null;
    
    if(copy.getDistributedRewards() != null)
      {
        this.distributedRewards = new HashMap<String,Integer>();
        // deep copy
        for(String key : copy.getDistributedRewards().keySet()) 
          {
            this.distributedRewards.put(key, new Integer(copy.getDistributedRewards().get(key)));
          }
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
    if(distributedRewards != null)
      {
        json.put("distributedRewards", JSONUtilities.encodeObject(distributedRewards));
      }
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
    struct.put("distributedRewards", obj.getDistributedRewards());
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
    Map<String,Integer> distributedRewards = (Map<String,Integer>) valueStruct.get("distributedRewards");
    
    //
    //  return
    //

    return new SubscriberTraffic(subscriberInflow, subscriberOutflow, distributedRewards);
  }

}