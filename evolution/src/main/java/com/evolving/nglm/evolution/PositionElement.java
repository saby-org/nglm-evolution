/*****************************************************************************
*
*  PositionElement.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.common.errors.SerializationException;
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

public class PositionElement
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
    schemaBuilder.name("presentation_positionelement");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("scoringStrategyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("additionalCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PositionElement> serde = new ConnectSerde<PositionElement>(schema, false, PositionElement.class, PositionElement::pack, PositionElement::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PositionElement> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String scoringStrategyID;
  private List<EvaluationCriterion> additionalCriteria;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getScoringStrategyID() { return scoringStrategyID; }
  public List<EvaluationCriterion> getAdditionalCriteria() { return additionalCriteria; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PositionElement(String scoringStrategyID, List<EvaluationCriterion> additionalCriteria)
  {
    this.scoringStrategyID = scoringStrategyID;
    this.additionalCriteria = additionalCriteria;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PositionElement positionElement = (PositionElement) value;
    Struct struct = new Struct(schema);
    struct.put("scoringStrategyID", positionElement.getScoringStrategyID());
    struct.put("additionalCriteria", packAdditionalCriteria(positionElement.getAdditionalCriteria()));
    return struct;
  }

  /****************************************
  *
  *  packAdditionalCriteria
  *
  ****************************************/

  private static List<Object> packAdditionalCriteria(List<EvaluationCriterion> additionalCriteria)
  {
    List<Object> result = new ArrayList<Object>();
    for (EvaluationCriterion criterion : additionalCriteria)
      {
        result.add(EvaluationCriterion.pack(criterion));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PositionElement unpack(SchemaAndValue schemaAndValue)
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
    String scoringStrategy = (String) valueStruct.get("scoringStrategyID");
    List<EvaluationCriterion> additionalCriteria = unpackAdditionalCriteria(schema.field("additionalCriteria").schema(), valueStruct.get("additionalCriteria"));

    //
    //  return
    //

    return new PositionElement(scoringStrategy, additionalCriteria);
  }
  
  /*****************************************
  *
  *  unpackAdditionalCriteria
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackAdditionalCriteria(Schema schema, Object value)
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

  public PositionElement(JSONObject jsonRoot, int tenantID) throws GUIManagerException
  {
    this.scoringStrategyID = JSONUtilities.decodeString(jsonRoot, "scoringStrategyID", true);
    this.additionalCriteria = decodeAdditionalCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "additionalCriteria", true), tenantID);
  }

  /*****************************************
  *
  *  decodeAdditionalCriteria
  *
  *****************************************/

  private List<EvaluationCriterion> decodeAdditionalCriteria(JSONArray jsonArray, int tenantID) throws GUIManagerException
  {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.Presentation.get(tenantID)));
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
    if (obj instanceof PositionElement)
      {
        PositionElement presentationPosition = (PositionElement) obj;
        result = true;
        result = result && Objects.equals(scoringStrategyID, presentationPosition.getScoringStrategyID());
        result = result && Objects.equals(additionalCriteria, presentationPosition.getAdditionalCriteria());
      }
    return result;
  }
}
