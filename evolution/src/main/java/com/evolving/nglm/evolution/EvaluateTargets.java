/*****************************************************************************
*
*  EvaluateTargets.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class EvaluateTargets
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
    schemaBuilder.name("evaluate_targets");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("campaignIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(new ArrayList<String>()).schema());
    schemaBuilder.field("targetIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<EvaluateTargets> serde = new ConnectSerde<EvaluateTargets>(schema, false, EvaluateTargets.class, EvaluateTargets::pack, EvaluateTargets::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<EvaluateTargets> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Set<String> campaignIDs;
  private Set<String> targetIDs;

  //
  //  in-memory only
  //

  private volatile boolean aborted = false;
  private volatile boolean completed = false;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Set<String> getCampaignIDs() { return campaignIDs; }
  public Set<String> getTargetIDs() { return targetIDs; }
  public boolean getAborted() { return aborted; }
  public boolean getCompleted() { return completed; }

  //
  //  setters
  //

  public void markAborted() { aborted = true; }
  public void markCompleted() { completed = true; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  //
  //  EvaluateTargets
  //  

  public EvaluateTargets(String campaignID, String targetID)
  {
    this(Collections.<String>singleton(campaignID), Collections.<String>singleton(targetID));
  }

  //
  //  EvaluateTargets
  //

  public EvaluateTargets(Collection<String> campaignIDs, Collection<String> targetIDs)
  {
    this.campaignIDs = new HashSet<String>(campaignIDs);
    this.targetIDs = new HashSet<String>(targetIDs);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    EvaluateTargets evaluateTargets = (EvaluateTargets) value;
    Struct struct = new Struct(schema);
    struct.put("campaignIDs", new ArrayList<Object>(evaluateTargets.getCampaignIDs()));
    struct.put("targetIDs", new ArrayList<Object>(evaluateTargets.getTargetIDs()));
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static EvaluateTargets unpack(SchemaAndValue schemaAndValue)
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
    Set<String> campaignIDs = (schemaVersion >= 2) ? new HashSet<String>((List<String>) valueStruct.get("campaignIDs")) : new HashSet<String>();
    Set<String> targetIDs = new HashSet<String>((List<String>) valueStruct.get("targetIDs"));

    //
    //  return
    //

    return new EvaluateTargets(campaignIDs, targetIDs);
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof EvaluateTargets)
      {
        EvaluateTargets entry = (EvaluateTargets) obj;
        result = true;
        result = result && Objects.equals(campaignIDs, entry.getCampaignIDs());
        result = result && Objects.equals(targetIDs, entry.getTargetIDs());
      }
    return result;
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  public String toString()
  {
    StringBuilder builder = new StringBuilder();
    builder.append("EvaluateTargets {");
    builder.append(campaignIDs + ",");
    builder.append(targetIDs);
    builder.append("}");
    return builder.toString();
  }
}
