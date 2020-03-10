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
import com.evolving.nglm.core.SystemTime;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    FullProfile("fullProfile"),
    Presentation("presentation"),
    Journey("journey"),
    JourneyNode("journeyNode"),
    DynamicProfile("dynamicProfile"),
    FullDynamicProfile("fullDynamicProfile"),
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
  public static final CriterionContext FullProfile = new CriterionContext(CriterionContextType.FullProfile);
  public static final CriterionContext DynamicProfile = new CriterionContext(CriterionContextType.DynamicProfile);
  public static final CriterionContext FullDynamicProfile = new CriterionContext(CriterionContextType.FullDynamicProfile);
  public static final CriterionContext Presentation = new CriterionContext(CriterionContextType.Presentation);

  /*****************************************
  *
  *  standard CriterionFields
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CriterionContext.class);
  
  //
  //  internal
  //

  private static CriterionField evaluationDate;
  private static CriterionField evaluationEventName;
  private static CriterionField internalRandom100;
  private static CriterionField internalFalse;
  private static CriterionField internalTargets;
  static
  {
    //
    //  evaluationDate
    //

    try
      {
        Map<String,Object> evaluationDateJSON = new LinkedHashMap<String,Object>();
        evaluationDateJSON.put("id", "evaluation.date");
        evaluationDateJSON.put("display", "Evaluation Date");
        evaluationDateJSON.put("dataType", "date");
        evaluationDateJSON.put("retriever", "getEvaluationDate");
        evaluationDateJSON.put("esField", "evaluationDate");
        evaluationDateJSON.put("internalOnly", false);
        evaluationDate  = new CriterionField(JSONUtilities.encodeObject(evaluationDateJSON));
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
        evaluationEventNameJSON.put("internalOnly", true);
        evaluationEventName  = new CriterionField(JSONUtilities.encodeObject(evaluationEventNameJSON));
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

    //
    //  internalFalse
    //

    try
      {
        Map<String,Object> internalFalseJSON = new LinkedHashMap<String,Object>();
        internalFalseJSON.put("id", "internal.false");
        internalFalseJSON.put("display", "internal.false");
        internalFalseJSON.put("dataType", "boolean");
        internalFalseJSON.put("retriever", "getFalse");
        internalFalseJSON.put("internalOnly", true);
        internalFalse  = new CriterionField(JSONUtilities.encodeObject(internalFalseJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  internalTargets
    //

    try
      {
        Map<String,Object> internalTargetsJSON = new LinkedHashMap<String,Object>();
        internalTargetsJSON.put("id", "internal.targets");
        internalTargetsJSON.put("display", "Subscriber Targets");
        internalTargetsJSON.put("dataType", "stringSet");
        internalTargetsJSON.put("retriever", "getTargets");
        internalTargetsJSON.put("internalOnly", false);
        internalTargets  = new CriterionField(JSONUtilities.encodeObject(internalTargetsJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
  }

  //
  //  journey
  //

  private static CriterionField journeyEntryDate;
  private static CriterionField nodeEntryDate;
  private static CriterionField journeyActionDeliveryStatus;
  private static CriterionField journeyActionJourneyStatus;
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
  }

  /*****************************************
  *
  *  dynamic criterion fields
  *
  *****************************************/

  private static DynamicCriterionFieldService dynamicCriterionFieldService = null;
  public static void initialize(DynamicCriterionFieldService dynamicCriterionFieldService)
  {
    CriterionContext.dynamicCriterionFieldService = dynamicCriterionFieldService;
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("criterionContextType", Schema.STRING_SCHEMA);
    schemaBuilder.field("additionalCriterionFields", SchemaBuilder.array(CriterionField.schema()).schema());
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
  private Map<String,CriterionField> additionalCriterionFields;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public CriterionContextType getCriterionContextType() { return criterionContextType; }
  public Map<String,CriterionField> getAdditionalCriterionFields() { return additionalCriterionFields; }

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  public CriterionContext(CriterionContextType criterionContextType)
  {
    this.criterionContextType = criterionContextType;
    this.additionalCriterionFields = Collections.<String,CriterionField>emptyMap();
  }
  
  /*****************************************
  *
  *  constructor -- top-level of journey
  *
  *****************************************/
  
  public CriterionContext(Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables)
  {
    this.criterionContextType = CriterionContextType.Journey;
    this.additionalCriterionFields = new LinkedHashMap<String,CriterionField>();
    this.additionalCriterionFields.put(journeyEntryDate.getID(), journeyEntryDate);
    this.additionalCriterionFields.putAll(journeyParameters);
    for (CriterionField contextVariable : contextVariables.values())
      {
        this.additionalCriterionFields.put(contextVariable.getID(), contextVariable);
      }
  }

  /*****************************************
  *
  *  constructor -- journey working context
  *
  *****************************************/

  public CriterionContext(CriterionContext baseCriterionContext, Map<String,CriterionField> additionalCriterionFields)
  {
    this.criterionContextType = CriterionContextType.JourneyNode;
    this.additionalCriterionFields = new LinkedHashMap<String,CriterionField>(baseCriterionContext.getAdditionalCriterionFields());
    this.additionalCriterionFields.putAll(additionalCriterionFields);
  }

  /*****************************************
  *
  *  constructor -- journey node
  *
  *****************************************/

  public CriterionContext(Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables, NodeType journeyNodeType, EvolutionEngineEventDeclaration journeyEvent, Journey selectedJourney) throws GUIManagerException
  {
    /*****************************************
    *
    *  contextType
    *
    *****************************************/

    this.criterionContextType = CriterionContextType.JourneyNode;

    /*****************************************
    *
    *  additionalCriterionFields -- standard node
    *
    *****************************************/

    this.additionalCriterionFields = new LinkedHashMap<String,CriterionField>();
    if (! journeyNodeType.getStartNode())
      {
        //
        //  standard journey top-level fields
        //

        this.additionalCriterionFields.put(journeyEntryDate.getID(), journeyEntryDate);

        //
        //  journey parameters
        //

        this.additionalCriterionFields.putAll(journeyParameters);

        //
        //  context variables
        //

        for (CriterionField contextVariable : contextVariables.values())
          {
            this.additionalCriterionFields.put(contextVariable.getID(), contextVariable);
          }

        //
        //  standard journey node-level fields
        //

        this.additionalCriterionFields.put(nodeEntryDate.getID(), nodeEntryDate);
        this.additionalCriterionFields.put(journeyActionDeliveryStatus.getID(), journeyActionDeliveryStatus);
        this.additionalCriterionFields.put(journeyActionJourneyStatus.getID(), journeyActionJourneyStatus);

        //
        //  node-level parameters
        //

        this.additionalCriterionFields.putAll(journeyNodeType.getParameters());

        //
        //  action-manager parameters
        //

        this.additionalCriterionFields.putAll((journeyNodeType.getActionManager() != null) ? journeyNodeType.getActionManager().getOutputAttributes() : Collections.<String,CriterionField>emptyMap());

        //
        //  trigger-level fields
        //

        if (journeyEvent != null)
          {
            this.additionalCriterionFields.putAll(journeyEvent.getEventCriterionFields());
          }

        //
        //  called-journey fields
        //

        if (selectedJourney != null)
          {
            for (CriterionField contextVariable : selectedJourney.getContextVariables().values())
              {
                String resultFieldID = Journey.generateJourneyResultID(selectedJourney, contextVariable);
                this.additionalCriterionFields.put(resultFieldID, new CriterionField(resultFieldID, selectedJourney, contextVariable));
              }
          }
      }
  }

  /*****************************************
  *
  *  construcxtor -- journey link
  *
  *****************************************/

  public CriterionContext(CriterionContext nodeCriterionContext, NodeType journeyNodeType, EvolutionEngineEventDeclaration journeyEvent)
  {
    this.criterionContextType = CriterionContextType.JourneyNode;
    this.additionalCriterionFields = new LinkedHashMap<String,CriterionField>(nodeCriterionContext.getAdditionalCriterionFields());
    this.additionalCriterionFields.putAll(journeyNodeType.getOutputConnectorParameters());
    if (journeyEvent != null)
      {
        this.additionalCriterionFields.putAll(journeyEvent.getEventCriterionFields());
      }
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private CriterionContext(CriterionContextType criterionContextType, Map<String,CriterionField> additionalCriterionFields)
  {
    this.criterionContextType = criterionContextType;
    this.additionalCriterionFields = additionalCriterionFields;
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
    struct.put("additionalCriterionFields", packAdditionalCriterionFields(criterionContext.getAdditionalCriterionFields()));
    return struct;
  }

  /****************************************
  *
  *  packAdditionalCriterionFields
  *
  ****************************************/

  private static List<Object> packAdditionalCriterionFields(Map<String,CriterionField> additionalCriterionFields)
  {
    List<Object> result = new ArrayList<Object>();
    for (CriterionField criterionField : additionalCriterionFields.values())
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
    Map<String,CriterionField> additionalCriterionFields = (schemaVersion >= 2) ? unpackAdditionalCriterionFields(schema.field("additionalCriterionFields").schema(), valueStruct.get("additionalCriterionFields")) : unpackAdditionalCriterionFields(schema.field("journeyCriterionFields").schema(), valueStruct.get("journeyCriterionFields"));

    //
    //  return
    //

    return new CriterionContext(criterionContextType, additionalCriterionFields);
  }

  /*****************************************
  *
  *  unpackAdditionalCriterionFields
  *
  *****************************************/

  private static Map<String,CriterionField> unpackAdditionalCriterionFields(Schema schema, Object value)
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
          result = new LinkedHashMap<String,CriterionField>();
          result.put(evaluationDate.getID(), evaluationDate);
          result.put(evaluationEventName.getID(), evaluationEventName);
          result.put(internalRandom100.getID(), internalRandom100);
          result.put(internalFalse.getID(), internalFalse);
          result.put(internalTargets.getID(), internalTargets);
          result.putAll(Deployment.getProfileCriterionFields());
          break;

        case FullProfile:
          result = new LinkedHashMap<String,CriterionField>();
          result.putAll(Profile.getCriterionFields());
          result.putAll(Deployment.getExtendedProfileCriterionFields());
          break;

        case DynamicProfile:
          if (dynamicCriterionFieldService == null) throw new ServerRuntimeException("criterion context not initialized");
          result = new LinkedHashMap<String,CriterionField>();
          result.putAll(Profile.getCriterionFields());
          for (DynamicCriterionField dynamicCriterionField : dynamicCriterionFieldService.getActiveDynamicCriterionFields(SystemTime.getCurrentTime()))
            {
              result.put(dynamicCriterionField.getCriterionField().getID(), dynamicCriterionField.getCriterionField());
            }
          break;

        case FullDynamicProfile:
          result = new LinkedHashMap<String,CriterionField>();
          result.putAll(DynamicProfile.getCriterionFields());
          result.putAll(Deployment.getExtendedProfileCriterionFields());
          break;

        case Journey:
        case JourneyNode:
          result = new LinkedHashMap<String,CriterionField>();
          result.putAll(additionalCriterionFields);
          result.putAll(DynamicProfile.getCriterionFields());
          break;

        case Presentation:
          result = Deployment.getPresentationCriterionFields();
          break;

        default:
          throw new ServerRuntimeException("unknown criterionContext: " + criterionContextType);
      }
    return result;
  }
}
