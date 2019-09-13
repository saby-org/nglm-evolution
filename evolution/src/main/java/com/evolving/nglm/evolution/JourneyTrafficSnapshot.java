/*****************************************************************************
*
*  JourneyTrafficSnapshot.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;

public class JourneyTrafficSnapshot
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
    //
    //  groupID schema
    //
    SchemaBuilder journeyTrafficMapSchemaBuilder = SchemaBuilder.map(Schema.STRING_SCHEMA, SubscriberTraffic.schema());
    journeyTrafficMapSchemaBuilder.name("journey_traffic_map");
    Schema journeyTrafficMapSchema = journeyTrafficMapSchemaBuilder.build();
    
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("JourneyTrafficSnapshot");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(0));
    schemaBuilder.field("global", SubscriberTraffic.schema());
    schemaBuilder.field("byNode", journeyTrafficMapSchema.schema());
    schemaBuilder.field("byStratum", journeyTrafficMapSchema.schema());
    schemaBuilder.field("byStatus", journeyTrafficMapSchema.schema());
    schemaBuilder.field("byStatusByStratum", SchemaBuilder.map(Schema.STRING_SCHEMA, journeyTrafficMapSchema.schema()).name("by_stratum_journey_traffic_map").schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyTrafficSnapshot> serde = new ConnectSerde<JourneyTrafficSnapshot>(schema, false, JourneyTrafficSnapshot.class, JourneyTrafficSnapshot::pack, JourneyTrafficSnapshot::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyTrafficSnapshot> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/
  
  private SubscriberTraffic global;                                             // Traffic & Rewards
  private Map<String,SubscriberTraffic> byNode;                                 // Traffic only
  private Map<List<String>,SubscriberTraffic> byStratum;                        // Traffic & Rewards
  private Map<String,SubscriberTraffic> byStatus;                               // Traffic only
  private Map<List<String>,Map<String,SubscriberTraffic>> byStatusByStratum;    // Traffic only -- Map<Stratum, Map<Status, SubscriberTraffic>>

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public SubscriberTraffic getGlobal() { return global; }
  public Map<String,SubscriberTraffic> getByNode() { return byNode; }
  public Map<List<String>, SubscriberTraffic> getByStratum() { return byStratum; }
  public Map<String,SubscriberTraffic> getByStatus() { return byStatus; }
  public Map<List<String>,Map<String,SubscriberTraffic>> getByStatusByStratum() { return byStatusByStratum; }
  
  public int getStatusSubscribersCount(SubscriberJourneyStatus status)
  {
    SubscriberTraffic traffic = byStatus.get(status.getExternalRepresentation());
    if (traffic != null) 
      {
        return traffic.getSubscriberCount();
      }
    return 0;
  }
  
  public int getNodeSubscribersCount(String nodeID)
  {
    SubscriberTraffic traffic = byNode.get(nodeID);
    if (traffic != null) 
      {
        return traffic.getSubscriberCount();
      }
    return 0;
  }
  
  public int getDistributedRewardsCount(String rewardID)
  {
    Map<String, Integer> rewards = global.getDistributedRewards();
    if (rewards != null) 
      {
        Integer result = rewards.get(rewardID);
        return (result != null) ? result : 0 ;
      }
    return 0;
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public JourneyTrafficSnapshot(SubscriberTraffic global, Map<String,SubscriberTraffic> byNode, Map<List<String>,SubscriberTraffic> byStratum, Map<String,SubscriberTraffic> byStatus, Map<List<String>,Map<String,SubscriberTraffic>> byStatusByStratum)
  {
    this.global = global;
    this.byNode = byNode;
    this.byStratum = byStratum;
    this.byStatus = byStatus;
    this.byStatusByStratum = byStatusByStratum;
  }
  
  /*****************************************
  *
  *  constructor -- empty
  *
  *****************************************/

  public JourneyTrafficSnapshot()
  {
    this(new SubscriberTraffic(),                                       // global
        new HashMap<String, SubscriberTraffic>(),                       // byNode
        new HashMap<List<String>, SubscriberTraffic>(),                 // byStratum
        new HashMap<String, SubscriberTraffic>(),                       // byStatus
        new HashMap<List<String>, Map<String, SubscriberTraffic>>());   // byStatusByStratum
    
    this.global.setEmptyRewardsMap();
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public JourneyTrafficSnapshot(JourneyTrafficSnapshot copy)
  {
    this.global = new SubscriberTraffic(copy.getGlobal());
    this.byNode = new HashMap<String,SubscriberTraffic>();
    this.byStratum = new HashMap<List<String>,SubscriberTraffic>();
    this.byStatus = new HashMap<String,SubscriberTraffic>();
    this.byStatusByStratum = new HashMap<List<String>, Map<String, SubscriberTraffic>>();
    
    // deep copy
    for(String key : copy.getByNode().keySet()) 
      {
        this.byNode.put(key, new SubscriberTraffic(copy.getByNode().get(key)));
      }
    
    // deep copy
    for(List<String> stratum : copy.getByStratum().keySet()) 
      {
        // Warning: we do not copy the (List<String>) stratum object. Should we ?
        this.byStratum.put(stratum, new SubscriberTraffic(copy.getByStratum().get(stratum)));
      }
    
    // deep copy
    for(String key : copy.getByStatus().keySet()) 
      {
        this.byStatus.put(key, new SubscriberTraffic(copy.getByStatus().get(key)));
      }
    
    // deep copy
    for(List<String> stratum : copy.getByStatusByStratum().keySet()) 
      {
        Map<String, SubscriberTraffic> copyMapForStratum = copy.getByStatusByStratum().get(stratum);
        Map<String, SubscriberTraffic> byStatusMap = new HashMap<String,SubscriberTraffic>();
        
        for(String key : copyMapForStratum.keySet()) 
          {
            byStatusMap.put(key, new SubscriberTraffic(copyMapForStratum.get(key)));
          }
  
        // Warning: we do not copy the (List<String>) stratum object. Should we ?
        this.byStatusByStratum.put(stratum,  byStatusMap);
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
    json.put("global", global.getJSONRepresentation());
    json.put("byNode", getJSONSubscriberTrafficMap(byNode));
    json.put("byStratum", getJSONByStratum(byStratum));
    json.put("byStatus", getJSONSubscriberTrafficMap(byStatus));
    json.put("byStatusByStratum", getJSONByStatusByStratum(byStatusByStratum));
    return JSONUtilities.encodeObject(json);
  }

  /****************************************
  *
  *  getJSONSubscriberTrafficMap
  *
  ****************************************/

  private static JSONObject getJSONSubscriberTrafficMap(Map<String,SubscriberTraffic> javaObject)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (String key : javaObject.keySet())
      {
        result.put(key, javaObject.get(key).getJSONRepresentation());
      }
    
    return JSONUtilities.encodeObject(result);
  }

  /****************************************
  *
  *  getJSONByStratum
  *
  ****************************************/

  private static JSONObject getJSONByStratum(Map<List<String>, SubscriberTraffic> javaObject)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (List<String> stratum : javaObject.keySet())
      {
        result.put(JSONUtilities.encodeArray(stratum).toJSONString(), javaObject.get(stratum).getJSONRepresentation());
      }
    
    return JSONUtilities.encodeObject(result);
  }

  /****************************************
  *
  *  getJSONByStatusByStratum
  *
  ****************************************/

  private static JSONObject getJSONByStatusByStratum(Map<List<String>, Map<String,SubscriberTraffic>> javaObject)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (List<String> stratum : javaObject.keySet())
      {
        result.put(JSONUtilities.encodeArray(stratum).toJSONString(), getJSONSubscriberTrafficMap(javaObject.get(stratum)));
      }
    
    return JSONUtilities.encodeObject(result);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyTrafficSnapshot obj = (JourneyTrafficSnapshot) value;
    Struct struct = new Struct(schema);
    struct.put("global", SubscriberTraffic.serde().pack(obj.getGlobal()));
    struct.put("byNode", packSubscriberTrafficMap(obj.getByNode()));
    struct.put("byStratum", packByStratum(obj.getByStratum()));
    struct.put("byStatus", packSubscriberTrafficMap(obj.getByStatus()));
    struct.put("byStatusByStratum", packByStatusByStratum(obj.getByStatusByStratum()));
    return struct;
  }

  /****************************************
  *
  *  packSubscriberTrafficMap
  *
  ****************************************/

  private static Object packSubscriberTrafficMap(Map<String,SubscriberTraffic> javaObject)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (String key : javaObject.keySet())
      {
        result.put(key, SubscriberTraffic.serde().pack(javaObject.get(key)));
      }

    return result;
  }

  /****************************************
  *
  *  packByStratum
  *
  ****************************************/

  private static Object packByStratum(Map<List<String>, SubscriberTraffic> javaObject)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (List<String> stratum : javaObject.keySet())
      {
        result.put(JSONArray.toJSONString(stratum), SubscriberTraffic.serde().pack(javaObject.get(stratum)));
      }

    return result;
  }

  /****************************************
  *
  *  packByStatusByStratum
  *
  ****************************************/

  private static Object packByStatusByStratum(Map<List<String>,Map<String,SubscriberTraffic>> javaObject)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (List<String> stratum : javaObject.keySet())
      {
        result.put(JSONArray.toJSONString(stratum), packSubscriberTrafficMap(javaObject.get(stratum)));
      }

    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static JourneyTrafficSnapshot unpack(SchemaAndValue schemaAndValue)
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
    SubscriberTraffic global = SubscriberTraffic.serde().unpack(new SchemaAndValue(schema.field("global").schema(), valueStruct.get("global")));
    Map<String,SubscriberTraffic> byNode = unpackSubscriberTrafficMap(schema.field("byNode").schema(), valueStruct.get("byNode"));
    Map<List<String>,SubscriberTraffic> byStratum = unpackByStratum(schema.field("byStratum").schema(), valueStruct.get("byStratum"));
    Map<String,SubscriberTraffic> byStatus = unpackSubscriberTrafficMap(schema.field("byStatus").schema(), valueStruct.get("byStatus"));
    Map<List<String>,Map<String,SubscriberTraffic>> byStatusByStratum = unpackByStatusByStratum(schema.field("byStatusByStratum").schema(), valueStruct.get("byStatusByStratum"));
    
    //
    //  return
    //

    return new JourneyTrafficSnapshot(global, byNode, byStratum, byStatus, byStatusByStratum);
  }

  /****************************************
  *
  *  unpackSubscriberTrafficMap
  *
  ****************************************/

  private static Map<String,SubscriberTraffic> unpackSubscriberTrafficMap(Schema schema, Object value)
  {
    Schema mapSchema = schema.valueSchema();
    Map<String,SubscriberTraffic> result = new HashMap<String,SubscriberTraffic>();
    Map<String,Object> valueMap = (Map<String,Object>) value;
    for (String key : valueMap.keySet())
      {
        result.put(key, SubscriberTraffic.unpack(new SchemaAndValue(mapSchema, valueMap.get(key))));
      }

    return result;
  }

  /****************************************
  *
  *  unpackByStratum
  *
  ****************************************/

  private static Map<List<String>,SubscriberTraffic> unpackByStratum(Schema schema, Object value)
  {
    JSONParser jsonParser = new JSONParser();
    
    Schema mapSchema = schema.valueSchema();
    Map<List<String>,SubscriberTraffic> result = new HashMap<List<String>,SubscriberTraffic>();
    Map<String,Object> valueMap = (Map<String,Object>) value;
    for (String key : valueMap.keySet())
      {
        try
          {
            JSONArray stratum = (JSONArray) jsonParser.parse(key);
            result.put((List<String>) stratum, SubscriberTraffic.unpack(new SchemaAndValue(mapSchema, valueMap.get(key))));
          } 
        catch (ParseException e)
          {
            e.printStackTrace();
          }
      }

    return result;
  }

  /****************************************
  *
  *  unpackByStatusByStratum
  *
  ****************************************/

  private static Map<List<String>,Map<String,SubscriberTraffic>> unpackByStatusByStratum(Schema schema, Object value)
  {
    JSONParser jsonParser = new JSONParser();
    
    Schema mapSchema = schema.valueSchema();
    Map<List<String>,Map<String,SubscriberTraffic>> result = new HashMap<List<String>,Map<String,SubscriberTraffic>>();
    Map<String,Object> valueMap = (Map<String,Object>) value;
    for (String key : valueMap.keySet())
      {
        try
          {
            JSONArray stratum = (JSONArray) jsonParser.parse(key);
            result.put((List<String>) stratum, unpackSubscriberTrafficMap(mapSchema, valueMap.get(key)));
          } 
        catch (ParseException e)
          {
            e.printStackTrace();
          }
      }

    return result;
  }

}