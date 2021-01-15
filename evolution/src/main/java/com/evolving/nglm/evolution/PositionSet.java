/*****************************************************************************
*
*  PositionSet.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class PositionSet
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
    schemaBuilder.name("presentation_positionset");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("positions", SchemaBuilder.array(PositionElement.schema()).schema());
    schemaBuilder.field("eligibility", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PositionSet> serde = new ConnectSerde<PositionSet>(schema, false, PositionSet.class, PositionSet::pack, PositionSet::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PositionSet> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private List<PositionElement> positions;
  private List<EvaluationCriterion> eligibility;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public List<PositionElement> getPositions() { return positions; }
  public List<EvaluationCriterion> getEligibility() { return eligibility; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PositionSet(List<PositionElement> positions, List<EvaluationCriterion> eligibility)
  {
    this.positions = positions;
    this.eligibility = eligibility;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PositionSet positionSet = (PositionSet) value;
    Struct struct = new Struct(schema);
    struct.put("positions", packPositions(positionSet.getPositions()));
    struct.put("eligibility", packEligibility(positionSet.getEligibility()));
    return struct;
  }

  /****************************************
  *
  *  packPositions
  *
  ****************************************/

  private static List<Object> packPositions(List<PositionElement> positions)
  {
    List<Object> result = new ArrayList<Object>();
    for (PositionElement position : positions)
      {
        result.add(PositionElement.pack(position));
      }
    return result;
  }

  /****************************************
  *
  *  packEligibility
  *
  ****************************************/

  private static List<Object> packEligibility(List<EvaluationCriterion> additionalCriteria)
  {
    List<Object> result = new ArrayList<Object>();
    if (additionalCriteria != null)
      {
        for (EvaluationCriterion criterion : additionalCriteria)
          {
            result.add(EvaluationCriterion.pack(criterion));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PositionSet unpack(SchemaAndValue schemaAndValue)
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
    List<PositionElement> positions = unpackPositions(schema.field("positions").schema(), valueStruct.get("positions"));
    List<EvaluationCriterion> eligibility = unpackEligibility(schema.field("eligibility").schema(), valueStruct.get("eligibility"));

    //
    //  return
    //

    return new PositionSet(positions, eligibility);
  }

  /*****************************************
  *
  *  unpackPositions
  *
  *****************************************/

  private static List<PositionElement> unpackPositions(Schema schema, Object value)
  {
    //
    //  get schema for PositionElement
    //

    Schema positionElementSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<PositionElement> result = new ArrayList<PositionElement>();
    List<Object> valueArray = (List<Object>) value;
    for (Object position : valueArray)
      {
        result.add(PositionElement.unpack(new SchemaAndValue(positionElementSchema, position)));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  unpackEligibility
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackEligibility(Schema schema, Object value)
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
  *  constructor
  *
  *****************************************/

  public PositionSet(JSONObject jsonRoot, int tenantID) throws GUIManagerException
  {
    this.positions = decodePositions(JSONUtilities.decodeJSONArray(jsonRoot, "positions", true), tenantID);
    // This object has an extra level that is not necessary.
    // We wanted to keep the same structure to emphasis that the Eligibility Rules are actually Criteria, but placed in a different area.
    JSONObject intermediate = JSONUtilities.decodeJSONObject(jsonRoot, "eligibility", false); // optional for "setB"
    if (intermediate == null)
      {
        this.eligibility = null;
      }
    else
      {
        this.eligibility = decodeEligibility(JSONUtilities.decodeJSONArray(intermediate, "additionalCriteria", true), tenantID);
      }
  }

  /*****************************************
  *
  *  decodePositions
  *
  *****************************************/

  private List<PositionElement> decodePositions(JSONArray jsonArray, int tenantID) throws GUIManagerException
  {
    List<PositionElement> result = new ArrayList<PositionElement>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new PositionElement((JSONObject) jsonArray.get(i), tenantID));
      }
    return result;
  }

  /*****************************************
  *
  *  decodeEligibility
  *
  *****************************************/

  private List<EvaluationCriterion> decodeEligibility(JSONArray jsonArray, int tenantID) throws GUIManagerException
  {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.Presentation.get(tenantID), tenantID));
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
    if (obj instanceof PositionSet)
      {
        PositionSet positionSet = (PositionSet) obj;
        result = true;
        result = result && Objects.equals(positions, positionSet.getPositions());
        result = result && Objects.equals(eligibility, positionSet.getEligibility());
      }
    return result;
  }
}
