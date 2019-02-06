/*****************************************************************************
*
*  CriterionContext.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.GUINode;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CriterionContext
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum CriterionContextType
  {
    Profile("profile"),
    Presentation("presentation"),
    JourneyNode("journeyNode"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private CriterionContextType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CriterionContextType fromExternalRepresentation(String externalRepresentation) { for (CriterionContextType enumeratedValue : CriterionContextType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  constants
  *
  *****************************************/

  public static final CriterionContext Profile = new CriterionContext(CriterionContextType.Profile);
  public static final CriterionContext Presentation = new CriterionContext(CriterionContextType.Presentation);

  /*****************************************
  *
  *  standard CriterionFields
  *
  *****************************************/
  
  private static CriterionField journeyEntryDate;
  private static CriterionField nodeEntryDate;
  private static CriterionField evaluationEventName;
  private static CriterionField journeyActionDeliveryStatus;
  private static CriterionField journeyActionJourneyStatus;
  private static CriterionField internalRandom100;
  static
  {
    //
    //  journeyEntryDate
    //

    try
      {
        Map<String,Object> journeyEntryDateJSON = new LinkedHashMap<String,Object>();
        journeyEntryDateJSON.put("id", "journey.entryDate");
        journeyEntryDateJSON.put("display", "journey.entryDate");
        journeyEntryDateJSON.put("dataType", "date");
        journeyEntryDateJSON.put("retriever", "getJourneyEntryDate");
        journeyEntryDateJSON.put("internalOnly", false);
        journeyEntryDate  = new CriterionField(JSONUtilities.encodeObject(journeyEntryDateJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  nodeEntryDate
    //

    try
      {
        Map<String,Object> nodeEntryDateJSON = new LinkedHashMap<String,Object>();
        nodeEntryDateJSON.put("id", "node.entryDate");
        nodeEntryDateJSON.put("display", "node.entryDate");
        nodeEntryDateJSON.put("dataType", "date");
        nodeEntryDateJSON.put("retriever", "getJourneyNodeEntryDate");
        nodeEntryDateJSON.put("internalOnly", false);
        nodeEntryDate  = new CriterionField(JSONUtilities.encodeObject(nodeEntryDateJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  evaluationEventName
    //

    try
      {
        Map<String,Object> evaluationEventNameJSON = new LinkedHashMap<String,Object>();
        evaluationEventNameJSON.put("id", "evaluation.eventname");
        evaluationEventNameJSON.put("display", "evaluation.eventname");
        evaluationEventNameJSON.put("dataType", "string");
        evaluationEventNameJSON.put("retriever", "getJourneyEvaluationEventName");
        evaluationEventNameJSON.put("internalOnly", false);
        evaluationEventName  = new CriterionField(JSONUtilities.encodeObject(evaluationEventNameJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  journeyActionDeliveryStatus
    //

    try
      {
        Map<String,Object> journeyActionDeliveryStatusJSON = new LinkedHashMap<String,Object>();
        journeyActionDeliveryStatusJSON.put("id", "node.action.deliverystatus");
        journeyActionDeliveryStatusJSON.put("display", "node.action.deliverystatus");
        journeyActionDeliveryStatusJSON.put("dataType", "string");
        journeyActionDeliveryStatusJSON.put("retriever", "getJourneyActionDeliveryStatus");
        journeyActionDeliveryStatusJSON.put("internalOnly", true);
        journeyActionDeliveryStatus  = new CriterionField(JSONUtilities.encodeObject(journeyActionDeliveryStatusJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  journeyActionJourneyStatus
    //

    try
      {
        Map<String,Object> journeyActionJourneyStatusJSON = new LinkedHashMap<String,Object>();
        journeyActionJourneyStatusJSON.put("id", "node.action.journeystatus");
        journeyActionJourneyStatusJSON.put("display", "node.action.journeystatus");
        journeyActionJourneyStatusJSON.put("dataType", "string");
        journeyActionJourneyStatusJSON.put("retriever", "getJourneyActionJourneyStatus");
        journeyActionJourneyStatusJSON.put("internalOnly", true);
        journeyActionJourneyStatus  = new CriterionField(JSONUtilities.encodeObject(journeyActionJourneyStatusJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  internalRandom100
    //

    try
      {
        Map<String,Object> internalRandom100JSON = new LinkedHashMap<String,Object>();
        internalRandom100JSON.put("id", "internal.random100");
        internalRandom100JSON.put("display", "internal.random100");
        internalRandom100JSON.put("dataType", "integer");
        internalRandom100JSON.put("retriever", "getRandom100");
        internalRandom100JSON.put("internalOnly", true);
        internalRandom100  = new CriterionField(JSONUtilities.encodeObject(internalRandom100JSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
  }

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
    schemaBuilder.name("criterion_context");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("criterionContextType", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyCriterionFields", SchemaBuilder.array(CriterionField.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CriterionContext> serde = new ConnectSerde<CriterionContext>(schema, false, CriterionContext.class, CriterionContext::pack, CriterionContext::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CriterionContext> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CriterionContextType criterionContextType;
  private Map<String,CriterionField> journeyCriterionFields;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public CriterionContextType getCriterionContextType() { return criterionContextType; }
  public Map<String,CriterionField> getJourneyCriterionFields() { return journeyCriterionFields; }

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  public CriterionContext(CriterionContextType criterionContextType)
  {
    this.criterionContextType = criterionContextType;
    this.journeyCriterionFields = Collections.<String,CriterionField>emptyMap();
  }
  
  /*****************************************
  *
  *  constructor -- journey node
  *
  *****************************************/

  public CriterionContext(Map<CriterionField,CriterionField> journeyMetrics, Map<String,CriterionField> journeyParameters, NodeType journeyNodeType, EvolutionEngineEventDeclaration journeyEvent, boolean includeLinkParameters)
  {
    /*****************************************
    *
    *  contextType
    *
    *****************************************/

    this.criterionContextType = CriterionContextType.JourneyNode;

    /*****************************************
    *
    *  journeyCriterionFields -- standard node
    *
    *****************************************/

    if (! journeyNodeType.getStartNode())
      {
        this.journeyCriterionFields = new LinkedHashMap<String,CriterionField>();

        //
        //  standard journey top-level fields
        //

        this.journeyCriterionFields.put(journeyEntryDate.getID(), journeyEntryDate);

        //
        //  journeyMetrics
        //

        for (CriterionField journeyMetric : journeyMetrics.keySet())
          {
            this.journeyCriterionFields.put(journeyMetric.getID(), journeyMetric);
          }

        //
        //  journey parameters
        //

        this.journeyCriterionFields.putAll(journeyParameters);

        //
        //  standard journey node-level fields
        //

        this.journeyCriterionFields.put(nodeEntryDate.getID(), nodeEntryDate);
        this.journeyCriterionFields.put(evaluationEventName.getID(), evaluationEventName);
        this.journeyCriterionFields.put(journeyActionDeliveryStatus.getID(), journeyActionDeliveryStatus);
        this.journeyCriterionFields.put(journeyActionJourneyStatus.getID(), journeyActionJourneyStatus);
        this.journeyCriterionFields.put(internalRandom100.getID(), internalRandom100);

        //
        //  node-level parameters
        //

        this.journeyCriterionFields.putAll(journeyNodeType.getParameters());

        //
        //  link-level  parameters
        //

        if (includeLinkParameters)
          {
            this.journeyCriterionFields.putAll(journeyNodeType.getOutputConnectorParameters());
          }

        //
        //  trigger-level fields
        //

        if (journeyEvent != null)
          {
            this.journeyCriterionFields.putAll(journeyEvent.getEventCriterionFields());
          }
      }

    /*****************************************
    *
    *  journeyCriterionFields -- start node
    *
    *****************************************/

    if (journeyNodeType.getStartNode())
      {
        this.journeyCriterionFields = new LinkedHashMap<String,CriterionField>();

        //
        //  journey parameters
        //

        this.journeyCriterionFields.putAll(journeyParameters);

        //
        //  standard journey node-level fields
        //

        this.journeyCriterionFields.put(evaluationEventName.getID(), evaluationEventName);
        this.journeyCriterionFields.put(internalRandom100.getID(), internalRandom100);

        //
        //  link-level  parameters
        //

        if (includeLinkParameters)
          {
            this.journeyCriterionFields.putAll(journeyNodeType.getOutputConnectorParameters());
          }

        //
        //  trigger-level fields
        //

        if (journeyEvent != null)
          {
            this.journeyCriterionFields.putAll(journeyEvent.getEventCriterionFields());
          }
      }
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private CriterionContext(CriterionContextType criterionContextType, Map<String,CriterionField> journeyCriterionFields)
  {
    this.criterionContextType = criterionContextType;
    this.journeyCriterionFields = journeyCriterionFields;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CriterionContext criterionContext = (CriterionContext) value;
    Struct struct = new Struct(schema);
    struct.put("criterionContextType", criterionContext.getCriterionContextType().getExternalRepresentation());
    struct.put("journeyCriterionFields", packJourneyCriterionFields(criterionContext.getJourneyCriterionFields()));
    return struct;
  }

  /****************************************
  *
  *  packJourneyCriterionFields
  *
  ****************************************/

  private static List<Object> packJourneyCriterionFields(Map<String,CriterionField> journeyCriterionFields)
  {
    List<Object> result = new ArrayList<Object>();
    for (CriterionField criterionField : journeyCriterionFields.values())
      {
        result.add(CriterionField.pack(criterionField));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CriterionContext unpack(SchemaAndValue schemaAndValue)
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
    CriterionContextType criterionContextType = CriterionContextType.fromExternalRepresentation(valueStruct.getString("criterionContextType"));
    Map<String,CriterionField> journeyCriterionFields = unpackJourneyCriterionFields(schema.field("journeyCriterionFields").schema(), valueStruct.get("journeyCriterionFields"));

    //
    //  return
    //

    return new CriterionContext(criterionContextType, journeyCriterionFields);
  }

  /*****************************************
  *
  *  unpackJourneyCriterionFields
  *
  *****************************************/

  private static Map<String,CriterionField> unpackJourneyCriterionFields(Schema schema, Object value)
  {
    //
    //  get schema for CriterionField
    //

    Schema criterionFieldSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Map<String,CriterionField> result = new LinkedHashMap<String,CriterionField>();
    List<Object> valueArray = (List<Object>) value;
    for (Object field : valueArray)
      {
        CriterionField criterionField = CriterionField.unpack(new SchemaAndValue(criterionFieldSchema, field));
        result.put(criterionField.getID(), criterionField);
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  getCriterionFields
  *
  *****************************************/
  
  public Map<String,CriterionField> getCriterionFields()
  {
    Map<String,CriterionField> result;
    switch (criterionContextType)
      {
        case Profile:
          result = Deployment.getProfileCriterionFields();
          break;
        case Presentation:
          result = Deployment.getPresentationCriterionFields();
          break;
        case JourneyNode:
          result = new LinkedHashMap<String,CriterionField>();
          result.putAll(journeyCriterionFields);
          result.putAll(Deployment.getProfileCriterionFields());
          break;
        default:
          throw new ServerRuntimeException("unknown criterionContext: " + criterionContextType);
      }
    return result;
  }
}
