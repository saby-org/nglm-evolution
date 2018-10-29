/*****************************************************************************
*
*  ScoringGroup.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

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

import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ScoringGroup
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
    schemaBuilder.name("scoring_group");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("profileCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("scoringSplits", SchemaBuilder.array(ScoringSplit.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ScoringGroup> serde = new ConnectSerde<ScoringGroup>(schema, false, ScoringGroup.class, ScoringGroup::pack, ScoringGroup::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ScoringGroup> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private List<EvaluationCriterion> profileCriteria;
  private List<ScoringSplit> scoringSplits;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public List<EvaluationCriterion> getProfileCriteria() { return profileCriteria; }
  public List<ScoringSplit> getScoringSplits() { return scoringSplits; }

  /*****************************************
  *
  *  evaluateProfileCriteria
  *
  *****************************************/

  public boolean evaluateProfileCriteria(SubscriberEvaluationRequest evaluationRequest)
  {
    return EvaluationCriterion.evaluateCriteria(evaluationRequest, profileCriteria);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ScoringGroup(List<EvaluationCriterion> profileCriteria, List<ScoringSplit> scoringSplits)
  {
    this.profileCriteria = profileCriteria;
    this.scoringSplits = scoringSplits;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ScoringGroup scoringGroup = (ScoringGroup) value;
    Struct struct = new Struct(schema);
    struct.put("profileCriteria", packProfileCriteria(scoringGroup.getProfileCriteria()));
    struct.put("scoringSplits", packScoringSplits(scoringGroup.getScoringSplits()));
    return struct;
  }

  /****************************************
  *
  *  packProfileCriteria
  *
  ****************************************/

  private static List<Object> packProfileCriteria(List<EvaluationCriterion> profileCriteria)
  {
    List<Object> result = new ArrayList<Object>();
    for (EvaluationCriterion criterion : profileCriteria)
      {
        result.add(EvaluationCriterion.pack(criterion));
      }
    return result;
  }

  /****************************************
  *
  *  packScoringSplits
  *
  ****************************************/

  private static List<Object> packScoringSplits(List<ScoringSplit> scoringSplits)
  {
    List<Object> result = new ArrayList<Object>();
    for (ScoringSplit scoringSplit : scoringSplits)
      {
        result.add(ScoringSplit.pack(scoringSplit));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ScoringGroup unpack(SchemaAndValue schemaAndValue)
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
    List<EvaluationCriterion> profileCriteria = unpackProfileCriteria(schema.field("profileCriteria").schema(), valueStruct.get("profileCriteria"));
    List<ScoringSplit> scoringSplits = unpackScoringSplits(schema.field("scoringSplits").schema(), valueStruct.get("scoringSplits"));
    
    //
    //  return
    //

    return new ScoringGroup(profileCriteria, scoringSplits);
  }

  /*****************************************
  *
  *  unpackProfileCriteria
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackProfileCriteria(Schema schema, Object value)
  {
    //
    //  get schema for EvaluationCriterion
    //

    Schema evaluationCriterionSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    List<Object> valueArray = (List<Object>) value;
    for (Object criterion : valueArray)
      {
        result.add(EvaluationCriterion.unpack(new SchemaAndValue(evaluationCriterionSchema, criterion)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  unpackScoringSplits
  *
  *****************************************/

  private static List<ScoringSplit> unpackScoringSplits(Schema schema, Object value)
  {
    //
    //  get schema for ScoringSplit
    //

    Schema scoringSplitSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<ScoringSplit> result = new ArrayList<ScoringSplit>();
    List<Object> valueArray = (List<Object>) value;
    for (Object scoringSplit : valueArray)
      {
        result.add(ScoringSplit.unpack(new SchemaAndValue(scoringSplitSchema, scoringSplit)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ScoringGroup(JSONObject jsonRoot) throws GUIManagerException
  {
    this.profileCriteria = decodeProfileCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", true));
    this.scoringSplits = decodeScoringSplits(JSONUtilities.decodeJSONArray(jsonRoot, "scoringSplits", true));
  }

  /*****************************************
  *
  *  decodeProfileCriteria
  *
  *****************************************/

  private List<EvaluationCriterion> decodeProfileCriteria(JSONArray jsonArray)  throws GUIManagerException, JSONUtilitiesException
  {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.Profile));
      }
    return result;
  }

  /*****************************************
  *
  *  decodeScoringSplit
  *
  *****************************************/

  private List<ScoringSplit> decodeScoringSplits(JSONArray jsonArray) throws GUIManagerException, JSONUtilitiesException
  {
    List<ScoringSplit> result = new ArrayList<ScoringSplit>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new ScoringSplit((JSONObject) jsonArray.get(i)));
      }
    return result;
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof ScoringGroup)
      {
        ScoringGroup scoringGroup = (ScoringGroup) obj;
        result = true;
        result = result && Objects.equals(profileCriteria, scoringGroup.getProfileCriteria());
        result = result && Objects.equals(scoringSplits, scoringGroup.getScoringSplits());
      }
    return result;
  }
}
