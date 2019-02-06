/*****************************************************************************
*
*  UCGState.java
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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class UCGState implements ReferenceDataValue<String>
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
    schemaBuilder.name("ucg_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("ucgGroups", SchemaBuilder.array(UCGGroup.schema()).schema());
    schemaBuilder.field("ucgPercentage", Schema.INT32_SCHEMA);
    schemaBuilder.field("refreshPercentage", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("epoch", Schema.INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<UCGState> serde = new ConnectSerde<UCGState>(schema, false, UCGState.class, UCGState::pack, UCGState::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<UCGState> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private Set<UCGGroup> ucgGroups;
  private int ucgPercentage;
  private Integer refreshPercentage;
  private int epoch;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public Set<UCGGroup> getUCGGroups() { return ucgGroups; }
  public int getUCGPercentage() { return ucgPercentage; }
  public Integer getRefreshPercentage() { return refreshPercentage; }
  public int getEpoch() { return epoch; }

  //
  //  ReferenceDataValue
  //
  
  @Override public String getKey() { return "ucgState"; }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public UCGState(Set<UCGGroup> ucgGroups, int ucgPercentage, Integer refreshPercentage, int epoch)
  {
    this.ucgGroups = ucgGroups;
    this.ucgPercentage = ucgPercentage;
    this.refreshPercentage = refreshPercentage;
    this.epoch = epoch;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    UCGState ucgState = (UCGState) value;
    Struct struct = new Struct(schema);
    struct.put("ucgGroups", packUCGGroups(ucgState.getUCGGroups()));
    struct.put("ucgPercentage", ucgState.getUCGPercentage());
    struct.put("refreshPercentage" , ucgState.getRefreshPercentage());
    struct.put("epoch", ucgState.getEpoch());
    return struct;
  }

  /****************************************
  *
  *  packUCGGroups
  *
  ****************************************/

  private static List<Object> packUCGGroups(Set<UCGGroup> ucgGroups)
  {
    List<Object> result = new ArrayList<Object>();
    for (UCGGroup ucgGroup : ucgGroups)
      {
        result.add(UCGGroup.pack(ucgGroup));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static UCGState unpack(SchemaAndValue schemaAndValue)
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
    Set<UCGGroup> ucgGroups = unpackUCGGroups(schema.field("ucgGroups").schema(), valueStruct.get("ucgGroups"));
    int ucgPercentage = valueStruct.getInt32("ucgPercentage");
    Integer refreshPercentage = valueStruct.getInt32("refreshPercentage");
    int epoch = valueStruct.getInt32("epoch");
    
    //
    //  return
    //

    return new UCGState(ucgGroups, ucgPercentage, refreshPercentage, epoch);
  }

  /*****************************************
  *
  *  unpackUCGGroups
  *
  *****************************************/

  private static Set<UCGGroup> unpackUCGGroups(Schema schema, Object value)
  {
    //
    //  get schema for ProductType
    //

    Schema ucgGroupSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<UCGGroup> result = new HashSet<UCGGroup>();
    List<Object> valueArray = (List<Object>) value;
    for (Object ucgGroup : valueArray)
      {
        result.add(UCGGroup.unpack(new SchemaAndValue(ucgGroupSchema, ucgGroup)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  class UCGGroup
  *
  *****************************************/

  public static class UCGGroup
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
      schemaBuilder.name("ucg_group");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("segmentIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
      schemaBuilder.field("ucgSubscribers", Schema.INT32_SCHEMA);
      schemaBuilder.field("totalSubscribers", Schema.INT32_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<UCGGroup> serde = new ConnectSerde<UCGGroup>(schema, false, UCGGroup.class, UCGGroup::pack, UCGGroup::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<UCGGroup> serde() { return serde; }

    /****************************************
    *
    *  data
    *
    ****************************************/

    private Set<String> segmentIDs;
    private int ucgSubscribers;
    private Integer totalSubscribers;

    /****************************************
    *
    *  accessors
    *
    ****************************************/

    public Set<String> getSegmentIDs() { return segmentIDs; }
    public int getUCGSubscribers() { return ucgSubscribers; }
    public int getTotalSubscribers() { return totalSubscribers; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public UCGGroup(Set<String> segmentIDs, int ucgSubscribers, int totalSubscribers)
    {
      this.segmentIDs = segmentIDs;
      this.ucgSubscribers = ucgSubscribers;
      this.totalSubscribers = totalSubscribers;
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      UCGGroup ucgGroup = (UCGGroup) value;
      Struct struct = new Struct(schema);
      struct.put("segmentIDs", packSegmentIDs(ucgGroup.getSegmentIDs()));
      struct.put("ucgSubscribers", ucgGroup.getUCGSubscribers());
      struct.put("totalSubscribers" , ucgGroup.getTotalSubscribers());
      return struct;
    }

    /****************************************
    *
    *  packSegmentIDs
    *
    ****************************************/

    private static List<Object> packSegmentIDs(Set<String> segmentIDs)
    {
      List<Object> result = new ArrayList<Object>();
      for (String segmentID : segmentIDs)
        {
          result.add(segmentID);
        }
      return result;
    }

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static UCGGroup unpack(SchemaAndValue schemaAndValue)
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
      Set<String> segmentIDs = unpackSegmentIDs((List<String>) valueStruct.get("segmentIDs"));
      int ucgSubscribers = valueStruct.getInt32("ucgSubscribers");
      int totalSubscribers = valueStruct.getInt32("totalSubscribers");

      //
      //  return
      //

      return new UCGGroup(segmentIDs, ucgSubscribers, totalSubscribers);
    }

    /*****************************************
    *
    *  unpackSegmentIDs
    *
    *****************************************/

    private static Set<String> unpackSegmentIDs(List<String> segmentIDs)
    {
      Set<String> result = new LinkedHashSet<String>();
      for (String segmentID : segmentIDs)
        {
          result.add(segmentID);
        }
      return result;
    }
  }
}
