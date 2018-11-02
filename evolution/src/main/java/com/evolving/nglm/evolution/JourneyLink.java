/*****************************************************************************
*
*  JourneyLink.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.EvaluationPriority;

import com.evolving.nglm.core.ConnectSerde;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class JourneyLink
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
    schemaBuilder.name("journey_link");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("linkID", Schema.STRING_SCHEMA);
    schemaBuilder.field("sourceReference", Schema.STRING_SCHEMA);
    schemaBuilder.field("destinationReference", Schema.STRING_SCHEMA);
    schemaBuilder.field("evaluationPriority", Schema.STRING_SCHEMA);
    schemaBuilder.field("transitionCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyLink> serde = new ConnectSerde<JourneyLink>(schema, false, JourneyLink.class, JourneyLink::pack, JourneyLink::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyLink> serde() { return serde; }


  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  basic
  //

  private String linkID;
  private String sourceReference;
  private String destinationReference;
  private EvaluationPriority evaluationPriority;
  private List<EvaluationCriterion> transitionCriteria;

  //
  //  derived
  //

  private JourneyNode source;
  private JourneyNode destination;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getLinkID() { return linkID; }
  public String getSourceReference() { return sourceReference; }
  public String getDestinationReference() { return destinationReference; }
  public EvaluationPriority getEvaluationPriority() { return evaluationPriority; }
  public List<EvaluationCriterion> getTransitionCriteria() { return transitionCriteria; }
  public JourneyNode getSource() { return source; }
  public JourneyNode getDestination() { return destination; }

  //
  //  setters
  //

  public void setSource(JourneyNode source) { this.source = source; }
  public void setDestination(JourneyNode destination) { this.destination = destination; }

   /*****************************************
  *
  *  constructor -- standard/unpack
  *
  *****************************************/

  public JourneyLink(String linkID, String sourceReference, String destinationReference, EvaluationPriority evaluationPriority, List<EvaluationCriterion> transitionCriteria)
  {
    this.linkID = linkID;
    this.sourceReference = sourceReference;
    this.destinationReference = destinationReference;
    this.evaluationPriority = evaluationPriority;
    this.transitionCriteria = transitionCriteria;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyLink journeyLink = (JourneyLink) value;
    Struct struct = new Struct(schema);
    struct.put("linkID", journeyLink.getLinkID());
    struct.put("sourceReference", journeyLink.getSourceReference());
    struct.put("destinationReference", journeyLink.getDestinationReference());
    struct.put("evaluationPriority", journeyLink.getEvaluationPriority().getExternalRepresentation());
    struct.put("transitionCriteria", packTransitionCriteria(journeyLink.getTransitionCriteria()));
    return struct;
  }

  /****************************************
  *
  *  packTransitionCriteria
  *
  ****************************************/

  private static List<Object> packTransitionCriteria(List<EvaluationCriterion> transitionCriteria)
  {
    List<Object> result = new ArrayList<Object>();
    for (EvaluationCriterion criterion : transitionCriteria)
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

  public static JourneyLink unpack(SchemaAndValue schemaAndValue)
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
    String linkID = valueStruct.getString("linkID");
    String sourceReference = valueStruct.getString("sourceReference");
    String destinationReference = valueStruct.getString("destinationReference");
    EvaluationPriority evaluationPriority = EvaluationPriority.fromExternalRepresentation(valueStruct.getString("evaluationPriority"));
    List<EvaluationCriterion> transitionCriteria = unpackTransitionCriteria(schema.field("transitionCriteria").schema(), valueStruct.get("transitionCriteria"));

    //
    //  return
    //

    return new JourneyLink(linkID, sourceReference, destinationReference, evaluationPriority, transitionCriteria);
  }

  /*****************************************
  *
  *  unpackTransitionCriteria
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackTransitionCriteria(Schema schema, Object value)
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
}
