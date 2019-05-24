/*****************************************************************************
*
*  SubscriberGroupEpoch.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.ReferenceDataValue;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;
import java.util.HashMap;

public class SubscriberGroupEpoch implements ReferenceDataValue<String>
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
    schemaBuilder.name("subscriber_group_epoch");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("primaryID", SchemaBuilder.string().defaultValue("").schema());
    schemaBuilder.field("epoch", Schema.INT32_SCHEMA);
    schemaBuilder.field("display", Schema.STRING_SCHEMA);
    schemaBuilder.field("active", Schema.BOOLEAN_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SubscriberGroupEpoch> serde = new ConnectSerde<SubscriberGroupEpoch>(schema, false, SubscriberGroupEpoch.class, SubscriberGroupEpoch::pack, SubscriberGroupEpoch::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberGroupEpoch> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String primaryID;
  private int epoch;
  private String display;
  private boolean active;
  private int zookeeperVersion;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getPrimaryID() { return primaryID; }
  public int getEpoch() { return epoch; }
  public String getDisplay() { return display; }
  public boolean getActive() { return active; }
  public int getZookeeperVersion() { return zookeeperVersion; }

  //
  //  ReferenceDataValue
  //
  
  @Override public String getKey() { return primaryID; }
  
  /*****************************************
  *
  *  constructor (trivial)
  *
  *****************************************/

  public SubscriberGroupEpoch(String primaryID)
  {
    this.primaryID = primaryID;
    this.epoch = 0;
    this.display = primaryID;
    this.active = true;
    this.zookeeperVersion = -1;
  }

  /*****************************************
  *
  *  constructor (update)
  *
  *****************************************/

  public SubscriberGroupEpoch(SubscriberGroupEpoch currentSubscriberGroupEpoch)
  {
    this.primaryID = currentSubscriberGroupEpoch.getPrimaryID();
    this.epoch = currentSubscriberGroupEpoch.getEpoch() + 1;
    this.display = currentSubscriberGroupEpoch.getDisplay();
    this.active = currentSubscriberGroupEpoch.getActive();
    this.zookeeperVersion = -1;
  }

  /*****************************************
  *
  *  constructor (unpack)
  *
  *****************************************/

  private SubscriberGroupEpoch(String primaryID, int epoch, String display, boolean active)
  {
    this.primaryID = primaryID;
    this.epoch = epoch;
    this.display = display;
    this.active = active;
    this.zookeeperVersion = -1;
  }


  /*****************************************
  *
  *  constructor (JSON)
  *
  *****************************************/

  public SubscriberGroupEpoch(JSONObject jsonRoot, int zookeeperVersion)
  {
    this.primaryID = JSONUtilities.decodeString(jsonRoot, "primaryID", true);
    this.epoch = JSONUtilities.decodeInteger(jsonRoot, "epoch", true);
    this.display = JSONUtilities.decodeString(jsonRoot, "display", true);
    this.active = JSONUtilities.decodeBoolean(jsonRoot, "active", true);
    this.zookeeperVersion = zookeeperVersion;
  }

  /*****************************************
  *
  *  getJSONRepresentation
  *
  *****************************************/

  public JSONObject getJSONRepresentation()
  {
    HashMap<String,Object> resultMap = new HashMap<String,Object>();;
    resultMap.put("primaryID", primaryID);
    resultMap.put("epoch", epoch);
    resultMap.put("display", display);
    resultMap.put("active", active);
    return JSONUtilities.encodeObject(resultMap);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SubscriberGroupEpoch subscriberGroupEpoch = (SubscriberGroupEpoch) value;
    Struct struct = new Struct(schema);
    struct.put("primaryID", subscriberGroupEpoch.getPrimaryID());
    struct.put("epoch", subscriberGroupEpoch.getEpoch());
    struct.put("display", subscriberGroupEpoch.getDisplay());
    struct.put("active", subscriberGroupEpoch.getActive());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SubscriberGroupEpoch unpack(SchemaAndValue schemaAndValue)
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
    String primaryID = (schemaVersion >= 2) ? valueStruct.getString("primaryID") : valueStruct.getString("dimensionID");
    int epoch = valueStruct.getInt32("epoch");
    String display = valueStruct.getString("display");
    boolean active = valueStruct.getBoolean("active");
    
    //
    //  return
    //

    return new SubscriberGroupEpoch(primaryID, epoch, display, active);
  }
}
