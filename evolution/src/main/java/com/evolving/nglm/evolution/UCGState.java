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
    schemaBuilder.field("ucgRule", UCGRule.schema());
    schemaBuilder.field("ucgGroups", SchemaBuilder.array(UCGGroup.schema()).schema());
    schemaBuilder.field("evaluationDate", Timestamp.SCHEMA);
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

  private UCGRule ucgRule;
  private Set<UCGGroup> ucgGroups;
  private Date evaluationDate;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public UCGRule getUCGRule() { return ucgRule; }
  public Set<UCGGroup> getUCGGroups() { return ucgGroups; }
  public Date getEvaluationDate() { return evaluationDate; }

  //
  //  convenience accessors
  //

  public String getUCGRuleID() { return ucgRule.getUCGRuleID(); }
  public Integer getRefreshEpoch() { return ucgRule.getRefreshEpoch(); }
  public int getRefreshWindowDays() { return ucgRule.getNoOfDaysForStayOut(); }

  //
  //  ReferenceDataValue
  //
  
  public static String getSingletonKey() { return "ucgState"; }
  @Override public String getKey() { return getSingletonKey(); }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public UCGState(UCGRule ucgRule, Set<UCGGroup> ucgGroups, Date evaluationDate)
  {
    this.ucgRule = ucgRule;
    this.ucgGroups = ucgGroups;
    this.evaluationDate = evaluationDate;
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
    struct.put("ucgRule", UCGRule.pack(ucgState.getUCGRule()));
    struct.put("ucgGroups", packUCGGroups(ucgState.getUCGGroups()));
    struct.put("evaluationDate", ucgState.getEvaluationDate());
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
    UCGRule ucgRule = UCGRule.unpack(new SchemaAndValue(schema.field("ucgRule").schema(), valueStruct.get("ucgRule")));
    Set<UCGGroup> ucgGroups = unpackUCGGroups(schema.field("ucgGroups").schema(), valueStruct.get("ucgGroups"));
    Date evaluationDate = (Date) valueStruct.get("evaluationDate");
    
    //
    //  return
    //

    return new UCGState(ucgRule, ucgGroups, evaluationDate);
  }

  /*****************************************
  *
  *  unpackUCGGroups
  *
  *****************************************/

  private static Set<UCGGroup> unpackUCGGroups(Schema schema, Object value)
  {
    //
    //  get schema for ucgGroup
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
