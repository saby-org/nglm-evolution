/*****************************************************************************
*
*  Journey.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.Expression.ReferenceExpression;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.JourneyHistory.StatusHistory;

public class Journey extends GUIManagedObject
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  JourneyStatus
  //

  public enum JourneyStatus
  {
    NotValid("Not Valid"),
    Pending("Pending"),
    Started("Started"),
    Running("Running"),
    Suspended("Suspended"),
    Complete("Complete"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private JourneyStatus(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static JourneyStatus fromExternalRepresentation(String externalRepresentation) { for (JourneyStatus enumeratedValue : JourneyStatus.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
  //
  //  EvaluationPriority
  //

  public enum EvaluationPriority
  {
    First("first"),
    Normal("normal"),
    Last("last"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private EvaluationPriority(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static EvaluationPriority fromExternalRepresentation(String externalRepresentation) { for (EvaluationPriority enumeratedValue : EvaluationPriority.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  SubscriberJourneyStatus
  //

  public enum SubscriberJourneyStatus
  {
    NotEligible("notEligible", "Not Eligible"),
    Entered("entered", "Entered"),
    Notified("notified", "Notified"),
    ConvertedNotNotified("unnotified_converted", "Not-Notified/Converted"),
    ConvertedNotified("notified_converted", "Notified/Converted"),
    ControlGroupEntered("controlGroup_entered", "Entered (control)"),
    ControlGroupConverted("controlGroup_converted", "Converted (control)"),
    Unknown("(unknown)", "(unknown)");
    private String externalRepresentation;
    private String display;
    private SubscriberJourneyStatus(String externalRepresentation, String display) { this.externalRepresentation = externalRepresentation; this.display = display; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getDisplay() { return display; }
    public static SubscriberJourneyStatus fromExternalRepresentation(String externalRepresentation) { for (SubscriberJourneyStatus enumeratedValue : SubscriberJourneyStatus.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  SubscriberJourneyStatusField
  //

  public enum SubscriberJourneyStatusField
  {
    StatusNotified("statusNotified", "journey.status.notified"),
    StatusConverted("statusConverted", "journey.status.converted"),
    StatusControlGroup("statusControlGroup", "journey.status.controlgroup"),
    StatusUniversalControlGroup("statusUniversalControlGroup", "journey.status.universalcontrolgroup"),
    Unknown("(unknown)", "(unknown)");
    private String externalRepresentation;
    private String journeyParameterName;
    private SubscriberJourneyStatusField(String externalRepresentation, String journeyParameterName) { this.externalRepresentation = externalRepresentation; this.journeyParameterName = journeyParameterName; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getJourneyParameterName() { return journeyParameterName; }
    public static SubscriberJourneyStatusField fromExternalRepresentation(String externalRepresentation) { for (SubscriberJourneyStatusField enumeratedValue : SubscriberJourneyStatusField.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
  //
  //  TargetingType
  //

  public enum TargetingType
  {
    Target("criteria", "Target"),
    Event("event", "Trigger"),
    Manual("manual", "Manual"),
    Unknown("(unknown)", "(unknown)");
    private String externalRepresentation;
    private String display;
    private TargetingType(String externalRepresentation, String display) { this.externalRepresentation = externalRepresentation; this.display = display; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getDisplay() { return display; }
    public static TargetingType fromExternalRepresentation(String externalRepresentation) { for (TargetingType enumeratedValue : TargetingType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  BulkType
  //
  
  public enum BulkType
  {
    Bulk_SMS("Bulk_SMS"),
    Bulk_Bonus("Bulk_Bonus"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private BulkType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static BulkType fromExternalRepresentation(String externalRepresentation) { for (BulkType enumeratedValue : BulkType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
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
    schemaBuilder.name("journey");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("effectiveEntryPeriodEndDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("journeyParameters", SchemaBuilder.map(Schema.STRING_SCHEMA, CriterionField.schema()).name("journey_journey_parameters").schema());
    schemaBuilder.field("contextVariables", SchemaBuilder.map(Schema.STRING_SCHEMA, CriterionField.schema()).name("journey_context_variables").schema());
    schemaBuilder.field("targetingType", Schema.STRING_SCHEMA);
    schemaBuilder.field("eligibilityCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("targetingCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("targetID", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().schema());
    schemaBuilder.field("startNodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("endNodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyObjectives", SchemaBuilder.array(JourneyObjectiveInstance.schema()).schema());
    schemaBuilder.field("journeyNodes", SchemaBuilder.array(JourneyNode.schema()).schema());
    schemaBuilder.field("journeyLinks", SchemaBuilder.array(JourneyLink.schema()).schema());
    schemaBuilder.field("boundParameters", ParameterMap.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Journey> serde = new ConnectSerde<Journey>(schema, false, Journey.class, Journey::pack, Journey::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Journey> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private Date effectiveEntryPeriodEndDate;
  private Map<String,CriterionField> journeyParameters;
  private Map<String,CriterionField> contextVariables;
  private TargetingType targetingType;
  private List<EvaluationCriterion> eligibilityCriteria;
  private List<EvaluationCriterion> targetingCriteria;
  private List<String> targetID;
  private String startNodeID;
  private String endNodeID;
  private Set<JourneyObjectiveInstance> journeyObjectiveInstances; 
  private Map<String,JourneyNode> journeyNodes;
  private Map<String,JourneyLink> journeyLinks;
  private ParameterMap boundParameters;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getJourneyID() { return getGUIManagedObjectID(); }
  public String getJourneyName() { return getGUIManagedObjectName(); }
  public Map<String,CriterionField> getJourneyParameters() { return journeyParameters; }
  public Map<String,CriterionField> getContextVariables() { return contextVariables; }
  public TargetingType getTargetingType() { return targetingType; }
  public List<EvaluationCriterion> getEligibilityCriteria() { return eligibilityCriteria; }
  public List<EvaluationCriterion> getTargetingCriteria() { return targetingCriteria; }
  public List<String> getTargetID() { return targetID; }
  public String getStartNodeID() { return startNodeID; }
  public String getEndNodeID() { return endNodeID; }
  public Set<JourneyObjectiveInstance> getJourneyObjectiveInstances() { return journeyObjectiveInstances;  }
  public Map<String,JourneyNode> getJourneyNodes() { return journeyNodes; }
  public Map<String,JourneyLink> getJourneyLinks() { return journeyLinks; }
  public JourneyNode getJourneyNode(String nodeID) { return journeyNodes.get(nodeID); }
  public JourneyLink getJourneyLink(String linkID) { return journeyLinks.get(linkID); }
  public ParameterMap getBoundParameters() { return boundParameters; }

  //
  //  package protected
  //

  Date getEffectiveEntryPeriodEndDate() { return (effectiveEntryPeriodEndDate != null) ? effectiveEntryPeriodEndDate : getEffectiveEndDate(); }

  //
  //  private
  //

  protected Date getRawEffectiveEntryPeriodEndDate() { return effectiveEntryPeriodEndDate; }
  
  //
  //  derived
  //

  public boolean getAutoTargeted()
  {
    boolean result = false;
    switch (targetingType)
      {
        case Target:
        case Event:
          result = true;
          break;
        case Manual:
          result = false;
          break;
      }
    return result;
  }

  //
  //  getAllCriteria
  //

  public List<EvaluationCriterion> getAllCriteria(TargetService targetService, Date now)
  {
    try
      {
        //
        //  result
        //

        List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();

        //
        //  eligibilityCriteria
        //

        result.addAll(eligibilityCriteria);

        //
        //  targetingCriteria
        //

        result.addAll(targetingCriteria);

        //
        //  target
        //

        if (targetID != null && !targetID.isEmpty())
          {
            
            for(String currentTargetID : targetID){
              
              //
              //  get the target
              //

              Target target = targetService.getActiveTarget(currentTargetID, now);

              //
              //  target not active -- automatic false criteria
              //

              if (target == null)
                {
                  Map<String,Object> falseCriterionArgumentJSON = new LinkedHashMap<String,Object>();
                  Map<String,Object> falseCriterionJSON = new LinkedHashMap<String,Object>();
                  falseCriterionArgumentJSON.put("expression", "false");
                  falseCriterionJSON.put("criterionField", "internal.false");
                  falseCriterionJSON.put("criterionOperator", "<>");
                  falseCriterionJSON.put("argument", JSONUtilities.encodeObject(falseCriterionArgumentJSON));
                  result.add(new EvaluationCriterion(JSONUtilities.encodeObject(falseCriterionJSON), CriterionContext.Profile));
                }

              //
              //  target criteria
              //

              if (target != null)
                {
                  result.addAll(target.getTargetingCriteria());
                }

              //
              // target "file" criteria
              //

              if (target != null && target.getTargetFileID() != null)
                {
                  Map<String,Object> targetCriterionArgumentJSON = new LinkedHashMap<String,Object>();
                  Map<String,Object> targetCriterionJSON = new LinkedHashMap<String,Object>();
                  targetCriterionArgumentJSON.put("expression", "'" + currentTargetID + "'");
                  targetCriterionJSON.put("criterionField", "internal.targets");
                  targetCriterionJSON.put("criterionOperator", "contains");
                  targetCriterionJSON.put("argument", JSONUtilities.encodeObject(targetCriterionArgumentJSON));
                  result.add(new EvaluationCriterion(JSONUtilities.encodeObject(targetCriterionJSON), CriterionContext.Profile));
                }
            }
          }

        //
        // return
        //

        return result;
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
  }

  //
  //  getAllObjectives
  //

  public Set<JourneyObjective> getAllObjectives(JourneyObjectiveService journeyObjectiveService, Date now)
  {
    Set<JourneyObjective> result = new HashSet<JourneyObjective>();
    for (JourneyObjectiveInstance journeyObjectiveInstance : journeyObjectiveInstances)
      {
        JourneyObjective journeyObjective = journeyObjectiveService.getActiveJourneyObjective(journeyObjectiveInstance.getJourneyObjectiveID(), now);
        if (journeyObjective != null)
          {
            result.add(journeyObjective);
            JourneyObjective walk = (journeyObjective.getParentJourneyObjectiveID() != null) ? journeyObjectiveService.getActiveJourneyObjective(journeyObjective.getParentJourneyObjectiveID(), now) : null;
            while (walk != null && ! result.contains(walk))
              {
                result.add(walk);
                walk = (walk.getParentJourneyObjectiveID() != null) ? journeyObjectiveService.getActiveJourneyObjective(walk.getParentJourneyObjectiveID(), now) : null;
              }
          }
      }
    return result;
  }

  /*****************************************
  *
  *  getSubscriberJourneyStatus
  *
  *****************************************/

  //
  //  base
  //

  public static SubscriberJourneyStatus getSubscriberJourneyStatus(boolean journeyComplete, boolean statusConverted, boolean statusNotified, boolean statusControlGroup)
  {
    if (! statusControlGroup && ! statusNotified && ! statusConverted)
      return SubscriberJourneyStatus.Entered;
    else if (! statusControlGroup && ! statusNotified && statusConverted)
      return SubscriberJourneyStatus.ConvertedNotNotified;
    else if (! statusControlGroup && statusNotified && ! statusConverted)
      return SubscriberJourneyStatus.Notified;
    else if (! statusControlGroup && statusNotified && statusConverted)
      return SubscriberJourneyStatus.ConvertedNotified;
    else if (statusControlGroup && ! statusConverted)
      return SubscriberJourneyStatus.ControlGroupEntered;
    else if (statusControlGroup && statusConverted)
      return SubscriberJourneyStatus.ControlGroupConverted;
    else
      return SubscriberJourneyStatus.Unknown;
  }

  //
  //  journeyStatistic
  //

  public static SubscriberJourneyStatus getSubscriberJourneyStatus(JourneyStatistic journeyStatistic)
  {
    return getSubscriberJourneyStatus(journeyStatistic.getJourneyComplete(), journeyStatistic.getStatusConverted(), journeyStatistic.getStatusNotified(), journeyStatistic.getStatusControlGroup());
  }

  //
  //  journeyState
  //

  public static SubscriberJourneyStatus getSubscriberJourneyStatus(JourneyState journeyState)
  {
    boolean journeyComplete = journeyState.getJourneyExitDate() != null;
    boolean statusConverted = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
    boolean statusNotified = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
    boolean statusControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    return getSubscriberJourneyStatus(journeyComplete, statusConverted, statusNotified, statusControlGroup);
  }

  //
  // statusHistory
  //

  public static SubscriberJourneyStatus getSubscriberJourneyStatus(StatusHistory statusHistory)
  {
    return getSubscriberJourneyStatus(statusHistory.getJourneyComplete(), statusHistory.getStatusConverted(), statusHistory.getStatusNotified(), statusHistory.getStatusControlGroup());
  }
  
  /*****************************************
  *
  *  evaluateEligibilityCriteria
  *
  *****************************************/

  public boolean evaluateEligibilityCriteria(SubscriberEvaluationRequest evaluationRequest)
  {
    return EvaluationCriterion.evaluateCriteria(evaluationRequest, eligibilityCriteria);
  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Journey(SchemaAndValue schemaAndValue, Date effectiveEntryPeriodEndDate, Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables, TargetingType targetingType, List<EvaluationCriterion> eligibilityCriteria, List<EvaluationCriterion> targetingCriteria, List<String> targetID, String startNodeID, String endNodeID, Set<JourneyObjectiveInstance> journeyObjectiveInstances, Map<String,JourneyNode> journeyNodes, Map<String,JourneyLink> journeyLinks, ParameterMap boundParameters)
  {
    super(schemaAndValue);
    this.effectiveEntryPeriodEndDate = effectiveEntryPeriodEndDate;
    this.journeyParameters = journeyParameters;
    this.contextVariables = contextVariables;
    this.targetingType = targetingType;
    this.eligibilityCriteria = eligibilityCriteria;
    this.targetingCriteria = targetingCriteria;
    this.targetID = targetID;
    this.startNodeID = startNodeID;
    this.endNodeID = endNodeID;
    this.journeyObjectiveInstances = journeyObjectiveInstances;
    this.journeyNodes = journeyNodes;
    this.journeyLinks = journeyLinks;
    this.boundParameters = boundParameters;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Journey journey = (Journey) value;
    Struct struct = new Struct(schema);
    packCommon(struct, journey);
    struct.put("effectiveEntryPeriodEndDate", journey.getRawEffectiveEntryPeriodEndDate());
    struct.put("journeyParameters", packJourneyParameters(journey.getJourneyParameters()));
    struct.put("contextVariables", packContextVariables(journey.getContextVariables()));
    struct.put("targetingType", journey.getTargetingType().getExternalRepresentation());
    struct.put("eligibilityCriteria", packCriteria(journey.getEligibilityCriteria()));
    struct.put("targetingCriteria", packCriteria(journey.getTargetingCriteria()));
    struct.put("targetID", journey.getTargetID());
    struct.put("startNodeID", journey.getStartNodeID());
    struct.put("endNodeID", journey.getEndNodeID());
    struct.put("journeyObjectives", packJourneyObjectiveInstances(journey.getJourneyObjectiveInstances()));
    struct.put("journeyNodes", packJourneyNodes(journey.getJourneyNodes()));
    struct.put("journeyLinks", packJourneyLinks(journey.getJourneyLinks()));
    struct.put("boundParameters", ParameterMap.pack(journey.getBoundParameters()));
    return struct;
  }

  /****************************************
  *
  *  packJourneyParameters
  *
  ****************************************/

  private static Map<String,Object> packJourneyParameters(Map<String,CriterionField> parameters)
  {
    Map<String,Object> result = new LinkedHashMap<String,Object>();
    for (String parameterName : parameters.keySet())
      {
        CriterionField journeyParameter = parameters.get(parameterName);
        result.put(parameterName,CriterionField.pack(journeyParameter));
      }
    return result;
  }

  /****************************************
  *
  *  packContextVariables
  *
  ****************************************/

  private static Map<String,Object> packContextVariables(Map<String,CriterionField> contextVariables)
  {
    Map<String,Object> result = new LinkedHashMap<String,Object>();
    for (String contextVariableName : contextVariables.keySet())
      {
        CriterionField contextVariable = contextVariables.get(contextVariableName);
        result.put(contextVariableName,CriterionField.pack(contextVariable));
      }
    return result;
  }

  /****************************************
  *
  *  packCriteria
  *
  ****************************************/

  private static List<Object> packCriteria(List<EvaluationCriterion> criteria)
  {
    List<Object> result = new ArrayList<Object>();
    for (EvaluationCriterion criterion : criteria)
      {
        result.add(EvaluationCriterion.pack(criterion));
      }
    return result;
  }

  /****************************************
  *
  *  packJourneyObjectiveInstances
  *
  ****************************************/

  private static List<Object> packJourneyObjectiveInstances(Set<JourneyObjectiveInstance> journeyObjectiveInstances)
  {
    List<Object> result = new ArrayList<Object>();
    for (JourneyObjectiveInstance journeyObjectiveInstance : journeyObjectiveInstances)
      {
        result.add(JourneyObjectiveInstance.pack(journeyObjectiveInstance));
      }
    return result;
  }
  
  /****************************************
  *
  *  packJourneyNodes
  *
  ****************************************/

  private static List<Object> packJourneyNodes(Map<String,JourneyNode> journeyNodes)
  {
    List<Object> result = new ArrayList<Object>();
    for (JourneyNode journeyNode : journeyNodes.values())
      {
        result.add(JourneyNode.pack(journeyNode));
      }
    return result;
  }

  /****************************************
  *
  *  packJourneyLinks
  *
  ****************************************/

  private static List<Object> packJourneyLinks(Map<String,JourneyLink> journeyLinks)
  {
    List<Object> result = new ArrayList<Object>();
    for (JourneyLink journeyLink : journeyLinks.values())
      {
        result.add(JourneyLink.pack(journeyLink));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Journey unpack(SchemaAndValue schemaAndValue)
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    Struct valueStruct = (Struct) value;
    Date effectiveEntryPeriodEndDate = (Date) valueStruct.get("effectiveEntryPeriodEndDate");
    Map<String,CriterionField> journeyParameters = unpackJourneyParameters(schema.field("journeyParameters").schema(), (Map<String,Object>) valueStruct.get("journeyParameters"));
    Map<String,CriterionField> contextVariables = unpackContextVariables(schema.field("contextVariables").schema(), (Map<String,Object>) valueStruct.get("contextVariables"));
    TargetingType targetingType = TargetingType.fromExternalRepresentation(valueStruct.getString("targetingType"));
    List<EvaluationCriterion> eligibilityCriteria = unpackCriteria(schema.field("eligibilityCriteria").schema(), valueStruct.get("eligibilityCriteria"));
    List<EvaluationCriterion> targetingCriteria = unpackCriteria(schema.field("targetingCriteria").schema(), valueStruct.get("targetingCriteria"));
    List<String> targetID = (List<String>) valueStruct.get("targetID");
    String startNodeID = valueStruct.getString("startNodeID");
    String endNodeID = valueStruct.getString("endNodeID");
    Set<JourneyObjectiveInstance> journeyObjectiveInstances = unpackJourneyObjectiveInstances(schema.field("journeyObjectives").schema(), valueStruct.get("journeyObjectives"));
    Map<String,JourneyNode> journeyNodes = unpackJourneyNodes(schema.field("journeyNodes").schema(), valueStruct.get("journeyNodes"));
    Map<String,JourneyLink> journeyLinks = unpackJourneyLinks(schema.field("journeyLinks").schema(), valueStruct.get("journeyLinks"));
    ParameterMap boundParameters = (schemaVersion >= 2) ? ParameterMap.unpack(new SchemaAndValue(schema.field("boundParameters").schema(), valueStruct.get("boundParameters"))) : new ParameterMap();

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    for (JourneyNode journeyNode : journeyNodes.values())
      {
        if (journeyNode.getNodeType() == null) throw new SerializationException("unknown nodeType for node " + journeyNode.getNodeID());
      }

    /*****************************************
    *
    *  transform
    *
    *****************************************/

    //
    //  bind links to nodes
    //

    for (JourneyNode journeyNode : journeyNodes.values())
      {
        //
        //  incoming
        //

        for (String incomingLinkReference : journeyNode.getIncomingLinkReferences())
          {
            JourneyLink incomingLink = journeyLinks.get(incomingLinkReference);
            journeyNode.getIncomingLinks().put(incomingLink.getLinkID(), incomingLink);
          }

        //
        //  outgoing
        //

        for (String outgoingLinkReference : journeyNode.getOutgoingLinkReferences())
          {
            JourneyLink outgoingLink = journeyLinks.get(outgoingLinkReference);
            journeyNode.getOutgoingLinks().put(outgoingLink.getLinkID(), outgoingLink);
          }
      }

    //
    //  bind nodes to links
    //

    for (JourneyLink journeyLink : journeyLinks.values())
      {
        journeyLink.setSource(journeyNodes.get(journeyLink.getSourceReference()));
        journeyLink.setDestination(journeyNodes.get(journeyLink.getDestinationReference()));
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return new Journey(schemaAndValue, effectiveEntryPeriodEndDate, journeyParameters, contextVariables, targetingType, eligibilityCriteria, targetingCriteria, targetID, startNodeID, endNodeID, journeyObjectiveInstances, journeyNodes, journeyLinks, boundParameters);
  }
  
  /*****************************************
  *
  *  unpackJourneyParameters
  *
  *****************************************/

  private static Map<String,CriterionField> unpackJourneyParameters(Schema schema, Map<String,Object> parameters)
  {
    Map<String,CriterionField> result = new LinkedHashMap<String,CriterionField>();
    for (String parameterName : parameters.keySet())
      {
        CriterionField journeyParameter = CriterionField.unpack(new SchemaAndValue(schema.valueSchema(), parameters.get(parameterName)));
        result.put(parameterName, journeyParameter);
      }
    return result;
  }

  /*****************************************
  *
  *  unpackContextVariables
  *
  *****************************************/

  private static Map<String,CriterionField> unpackContextVariables(Schema schema, Map<String,Object> contextVariables)
  {
    Map<String,CriterionField> result = new LinkedHashMap<String,CriterionField>();
    for (String contextVariableName : contextVariables.keySet())
      {
        CriterionField contextVariable = CriterionField.unpack(new SchemaAndValue(schema.valueSchema(), contextVariables.get(contextVariableName)));
        result.put(contextVariableName, contextVariable);
      }
    return result;
  }

  /*****************************************
  *
  *  unpackCriteria
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackCriteria(Schema schema, Object value)
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
  *  unpackJourneyObjectiveInstances
  *
  *****************************************/

  private static Set<JourneyObjectiveInstance> unpackJourneyObjectiveInstances(Schema schema, Object value)
  {
    //
    //  get schema for JourneyObjectiveInstance
    //

    Schema journeyObjectiveInstanceSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<JourneyObjectiveInstance> result = new HashSet<JourneyObjectiveInstance>();
    List<Object> valueArray = (List<Object>) value;
    for (Object journeyObjectiveInstance : valueArray)
      {
        result.add(JourneyObjectiveInstance.unpack(new SchemaAndValue(journeyObjectiveInstanceSchema, journeyObjectiveInstance)));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  unpackJourneyNodes
  *
  *****************************************/

  private static Map<String,JourneyNode> unpackJourneyNodes(Schema schema, Object value)
  {
    //
    //  get schema for JourneyNode
    //

    Schema journeyNodeSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Map<String,JourneyNode> result = new LinkedHashMap<String,JourneyNode>();
    List<Object> valueArray = (List<Object>) value;
    for (Object node : valueArray)
      {
        JourneyNode journeyNode = JourneyNode.unpack(new SchemaAndValue(journeyNodeSchema, node));
        result.put(journeyNode.getNodeID(), journeyNode);
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  unpackJourneyLinks
  *
  *****************************************/

  private static Map<String,JourneyLink> unpackJourneyLinks(Schema schema, Object value)
  {
    //
    //  get schema for JourneyLink
    //

    Schema journeyLinkSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Map<String,JourneyLink> result = new LinkedHashMap<String,JourneyLink>();
    List<Object> valueArray = (List<Object>) value;
    for (Object link : valueArray)
      {
        JourneyLink journeyLink = JourneyLink.unpack(new SchemaAndValue(journeyLinkSchema, link));
        result.put(journeyLink.getLinkID(), journeyLink);
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Journey(JSONObject jsonRoot, GUIManagedObjectType journeyType, long epoch, GUIManagedObject existingJourneyUnchecked, CatalogCharacteristicService catalogCharacteristicService, SubscriberMessageTemplateService subscriberMessageTemplateService, DynamicEventDeclarationsService dynamicEventDeclarationsService, CommunicationChannelService communicationChannelService) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, journeyType, (existingJourneyUnchecked != null) ? existingJourneyUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingJourney
    *
    *****************************************/

    Journey existingJourney = (existingJourneyUnchecked != null && existingJourneyUnchecked instanceof Journey) ? (Journey) existingJourneyUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.effectiveEntryPeriodEndDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "effectiveEntryPeriodEndDate", false));
    this.journeyParameters = decodeJourneyParameters(JSONUtilities.decodeJSONArray(jsonRoot, "journeyParameters", false));
    this.targetingType = TargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", "criteria"));
    this.eligibilityCriteria = decodeCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "eligibilityCriteria", false), Deployment.getJourneyUniversalEligibilityCriteria());
    this.targetingCriteria = decodeCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "targetingCriteria", false), new ArrayList<EvaluationCriterion>());
    this.targetID = decodeTargetIDs(JSONUtilities.decodeJSONArray(jsonRoot, "targetID", new JSONArray()));
    this.journeyObjectiveInstances = decodeJourneyObjectiveInstances(JSONUtilities.decodeJSONArray(jsonRoot, "journeyObjectives", false), catalogCharacteristicService);
    Map<String,GUINode> contextVariableNodes = decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot, "nodes", true), this.journeyParameters, Collections.<String,CriterionField>emptyMap(), true, subscriberMessageTemplateService, dynamicEventDeclarationsService);
    List<GUILink> jsonLinks = decodeLinks(JSONUtilities.decodeJSONArray(jsonRoot, "links", true));

    /*****************************************
    *
    *  contextVariables
    *
    *****************************************/

    this.contextVariables = Journey.processContextVariableNodes(contextVariableNodes, journeyParameters);

    /*****************************************
    *
    *  boundParameters
    *
    *****************************************/

    this.boundParameters = decodeBoundParameters(JSONUtilities.decodeJSONArray(jsonRoot, "boundParameters", new JSONArray()), this.journeyParameters, this.contextVariables, subscriberMessageTemplateService);

    /*****************************************
    *
    *  jsonNodes
    *
    *****************************************/

    Map<String,GUINode> jsonNodes = decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot, "nodes", true), this.journeyParameters, contextVariables, false, subscriberMessageTemplateService, dynamicEventDeclarationsService);
    
    /*****************************************
    *
    *  validate
    *
    *****************************************/

    //
    //  autoTargeting and parameters
    //

    switch (this.targetingType)
      {
        case Target:
        case Event:
          switch (journeyType) 
            {
              case Journey:
              case Campaign:
              case BulkCampaign:
                if (this.journeyParameters.size() > 0 && this.journeyParameters.size() != this.boundParameters.size()) throw new GUIManagerException("autoTargeted Journey may not have parameters", this.getJourneyID());
                break;
            }
          break;
      }

    //
    //  nodeTypes
    //

    for (GUINode jsonNode : jsonNodes.values())
      {
        if (jsonNode.getNodeType() == null) throw new GUIManagerException("unknown nodeType", jsonNode.getNodeID());
      }

    /*****************************************
    *
    *  build journeyNodes
    *
    *****************************************/

    this.journeyNodes = new LinkedHashMap<String,JourneyNode>();
    for (GUINode jsonNode : jsonNodes.values())
      {
        journeyNodes.put(jsonNode.getNodeID(), new JourneyNode(jsonNode.getNodeID(), jsonNode.getNodeName(), jsonNode.getNodeType(), jsonNode.getNodeParameters(), jsonNode.getContextVariables(), new ArrayList<String>(), new ArrayList<String>()));
      }

    /*****************************************
    *
    *  startNodeID
    *
    *****************************************/

    this.startNodeID = null;
    for (JourneyNode journeyNode : this.journeyNodes.values())
      {
        if (journeyNode.getNodeType().getStartNode())
          {
            if (this.startNodeID != null) throw new GUIManagerException("multiple start nodes", journeyNode.getNodeID());
            this.startNodeID = journeyNode.getNodeID();
          }
      }
    if (this.startNodeID == null) throw new GUIManagerException("no start node", null);
    if (this.journeyNodes.get(this.startNodeID).getNodeType().getActionManager() != null) throw new GUIManagerException("illegal start node", this.startNodeID);

    /*****************************************
    *
    *  endNodeID
    *
    *****************************************/

    this.endNodeID = null;
    for (JourneyNode journeyNode : this.journeyNodes.values())
      {
        if (journeyNode.getNodeType().getEndNode())
          {
            if (this.endNodeID != null) throw new GUIManagerException("multiple end nodes", journeyNode.getNodeID());
            this.endNodeID = journeyNode.getNodeID();
          }
      }
    if (this.endNodeID == null) throw new GUIManagerException("no end node", null);
    if (this.journeyNodes.get(this.endNodeID).getNodeType().getActionManager() != null) throw new GUIManagerException("illegal end node", this.endNodeID);

    /*****************************************
    *
    *  populate implicit GUILinks
    *
    *****************************************/

    //
    //  build outgoingGUILinksByGUINode
    //

    Map<String,Map<Integer,GUILink>> outgoingGUILinksByGUINode = new HashMap<String,Map<Integer,GUILink>>();
    for (GUILink jsonLink : jsonLinks)
      {
        Map<Integer,GUILink> linksForGUINode = outgoingGUILinksByGUINode.get(jsonLink.getSourceNodeID());
        if (linksForGUINode == null)
          {
            linksForGUINode = new HashMap<Integer,GUILink>();
            outgoingGUILinksByGUINode.put(jsonLink.getSourceNodeID(), linksForGUINode);
          }
        linksForGUINode.put(jsonLink.getSourceConnectionPoint(), jsonLink);
      }

    //
    //  add implicitLinks to jsonLinks
    //

    for (GUINode jsonNode : jsonNodes.values())
      {
        for (int i=0; i<jsonNode.getOutgoingConnectionPoints().size(); i++)
          {
            Map<Integer,GUILink> linksForGUINode = outgoingGUILinksByGUINode.get(jsonNode.getNodeID());
            GUILink outgoingLink = (linksForGUINode != null) ? linksForGUINode.get(i) : null;
            if (outgoingLink == null)
              {
                jsonLinks.add(new GUILink(jsonNode.getNodeID(), i, this.endNodeID));
              }
          }
      }

    /*****************************************
    *
    *  build journeyLinks, incomingLinkReferencesByJourneyNode, outgoingLinkReferencesByJourneyNode
    *
    *****************************************/

    this.journeyLinks = new LinkedHashMap<String,JourneyLink>();
    Map<JourneyNode,SortedMap<Integer,String>> outgoingLinkReferencesByJourneyNode = new HashMap<JourneyNode,SortedMap<Integer,String>>();
    Map<JourneyNode,List<String>> incomingLinkReferencesByJourneyNode = new HashMap<JourneyNode,List<String>>();
    for (GUILink jsonLink : jsonLinks)
      {
        /*****************************************
        *
        *  source/destination
        *
        *****************************************/

        GUINode sourceNode = jsonNodes.get(jsonLink.getSourceNodeID());
        GUINode destinationNode = jsonNodes.get(jsonLink.getDestinationNodeID());

        //
        //  validate
        //

        if (sourceNode == null) throw new GUIManagerException("unknown source node", jsonLink.getSourceNodeID());
        if (destinationNode == null) throw new GUIManagerException("unknown destination node", jsonLink.getDestinationNodeID());

        /*****************************************
        *
        *  source and destination node
        *
        *****************************************/

        JourneyNode sourceJourneyNode = journeyNodes.get(sourceNode.getNodeID());
        JourneyNode destinationJourneyNode = journeyNodes.get(destinationNode.getNodeID());

        /*****************************************
        *
        *  source connectionPoint
        *
        *****************************************/

        OutgoingConnectionPoint outgoingConnectionPoint = (jsonLink.getSourceConnectionPoint() < sourceNode.getOutgoingConnectionPoints().size()) ? sourceNode.getOutgoingConnectionPoints().get(jsonLink.getSourceConnectionPoint()) : null;

        //
        //  validate
        //

        if (outgoingConnectionPoint == null) throw new GUIManagerException("unknown source connection point", Integer.toString(jsonLink.getSourceConnectionPoint()));

        /*****************************************
        *
        *  prepare final list of transition criteria
        *
        *****************************************/

        List<EvaluationCriterion> transitionCriteria = new ArrayList<EvaluationCriterion>(outgoingConnectionPoint.getTransitionCriteria());

        //
        //  additionalCriteria -- node
        //

        if (outgoingConnectionPoint.getAdditionalCriteria() != null && sourceJourneyNode.getNodeParameters().containsKey(outgoingConnectionPoint.getAdditionalCriteria()))
          {
            transitionCriteria.addAll((List<EvaluationCriterion>) sourceJourneyNode.getNodeParameters().get(outgoingConnectionPoint.getAdditionalCriteria()));
          }

        //
        //  additionalCriteria -- link
        //

        if (outgoingConnectionPoint.getAdditionalCriteria() != null && outgoingConnectionPoint.getOutputConnectorParameters().containsKey(outgoingConnectionPoint.getAdditionalCriteria()))
          {
            transitionCriteria.addAll((List<EvaluationCriterion>) outgoingConnectionPoint.getOutputConnectorParameters().get(outgoingConnectionPoint.getAdditionalCriteria()));
          }
        
        /*****************************************
        *
        *  journeyLink
        *
        *****************************************/

        String linkID = jsonLink.getSourceNodeID() + "-" + Integer.toString(jsonLink.getSourceConnectionPoint()) + ":" + jsonLink.getDestinationNodeID();
        JourneyLink journeyLink = new JourneyLink(linkID, outgoingConnectionPoint.getName(), outgoingConnectionPoint.getOutputConnectorParameters(), sourceNode.getNodeID(), destinationNode.getNodeID(), outgoingConnectionPoint.getEvaluationPriority(), outgoingConnectionPoint.getEvaluateContextVariables(), transitionCriteria, outgoingConnectionPoint.getDisplay());
        journeyLink.setSource(sourceJourneyNode);
        journeyLink.setDestination(destinationJourneyNode);
        journeyLinks.put(journeyLink.getLinkID(), journeyLink);

        /*****************************************
        *
        *  outgoingLinkReferencesByJourneyNode
        *
        *****************************************/

        SortedMap<Integer,String> outgoingLinkReferences = outgoingLinkReferencesByJourneyNode.get(sourceJourneyNode);
        if (outgoingLinkReferences == null)
          {
            outgoingLinkReferences = new TreeMap<Integer,String>();
            outgoingLinkReferencesByJourneyNode.put(sourceJourneyNode, outgoingLinkReferences);
          }
        outgoingLinkReferences.put(jsonLink.getSourceConnectionPoint(), journeyLink.getLinkID());

        /*****************************************
        *
        *  incomingLinkReferencesByJourneyNode
        *
        *****************************************/

        List<String> incomingLinkReferences = incomingLinkReferencesByJourneyNode.get(destinationJourneyNode);
        if (incomingLinkReferences == null)
          {
            incomingLinkReferences = new ArrayList<String>();
            incomingLinkReferencesByJourneyNode.put(destinationJourneyNode, incomingLinkReferences);
          }
        incomingLinkReferences.add(journeyLink.getLinkID());
      }

    /*****************************************
    *
    *  build outgoingLinkReferences and outgoingLinks
    *
    *****************************************/

    for (JourneyNode journeyNode : outgoingLinkReferencesByJourneyNode.keySet())
      {
        //
        //  initialize outgoingLinksByEvaluationPriority
        //

        Map<EvaluationPriority,List<JourneyLink>> outgoingLinksByEvaluationPriority = new HashMap<EvaluationPriority,List<JourneyLink>>();
        outgoingLinksByEvaluationPriority.put(EvaluationPriority.First, new ArrayList<JourneyLink>());
        outgoingLinksByEvaluationPriority.put(EvaluationPriority.Normal, new ArrayList<JourneyLink>());
        outgoingLinksByEvaluationPriority.put(EvaluationPriority.Last, new ArrayList<JourneyLink>());

        //
        //  sort by EvaluationPriority
        //

        for (String outgoingLinkReference : outgoingLinkReferencesByJourneyNode.get(journeyNode).values())
          {
            JourneyLink outgoingLink = journeyLinks.get(outgoingLinkReference);
            List<JourneyLink> outgoingLinks = outgoingLinksByEvaluationPriority.get(outgoingLink.getEvaluationPriority());
            if (outgoingLinks == null)
              {
                outgoingLinks = new ArrayList<JourneyLink>();
                outgoingLinksByEvaluationPriority.put(outgoingLink.getEvaluationPriority(), outgoingLinks);
              }
            outgoingLinks.add(outgoingLink);
          }

        //
        //  concatenate outgoingLinks
        //

        List<JourneyLink> sortedOutgoingLinks = new ArrayList<JourneyLink>();
        sortedOutgoingLinks.addAll(outgoingLinksByEvaluationPriority.get(EvaluationPriority.First));
        sortedOutgoingLinks.addAll(outgoingLinksByEvaluationPriority.get(EvaluationPriority.Normal));
        sortedOutgoingLinks.addAll(outgoingLinksByEvaluationPriority.get(EvaluationPriority.Last));

        //
        //  outgoingLinkReferences and outgoingLinks
        //  

        for (JourneyLink journeyLink : sortedOutgoingLinks)
          {
            journeyNode.getOutgoingLinkReferences().add(journeyLink.getLinkID());
            journeyNode.getOutgoingLinks().put(journeyLink.getLinkID(), journeyLink);
          }
      }

    /*****************************************
    *
    *  build incomingLinkReferences and incomingLinks
    *
    *****************************************/
    
    for (JourneyNode journeyNode : incomingLinkReferencesByJourneyNode.keySet())
      {
        for (String incomingLinkReference : incomingLinkReferencesByJourneyNode.get(journeyNode))
          {
            JourneyLink incomingLink = journeyLinks.get(incomingLinkReference);
            journeyNode.getIncomingLinkReferences().add(incomingLink.getLinkID());
            journeyNode.getIncomingLinks().put(incomingLink.getLinkID(), incomingLink);
          }
      }

    /*****************************************
    *
    *  ensure no illegal cycles
    *
    *****************************************/
    
    Set<JourneyNode> visitedNodes = new HashSet<JourneyNode>();
    LinkedList<JourneyNode> walkNodes = new LinkedList<JourneyNode>();
    JourneyNode startNode = journeyNodes.get(startNodeID);
    if (startNode.detectCycle(visitedNodes, walkNodes))
      {
        throw new GUIManagerException("illegal cycle", walkNodes.get(walkNodes.size()-1).getNodeID());
      }

    /*****************************************
    *
    *  validate mandatory parameters
    *
    *****************************************/

    for (JourneyNode journeyNode : this.journeyNodes.values())
      {
        //
        //  node parameters
        //

        for (String parameterName : journeyNode.getNodeType().getParameters().keySet())
          {
            CriterionField parameterDeclaration = journeyNode.getNodeType().getParameters().get(parameterName);
            if (parameterDeclaration.getMandatoryParameter() && journeyNode.getNodeParameters().containsKey(parameterName) && journeyNode.getNodeParameters().get(parameterName) == null)
              {
                throw new GUIManagerException("mandatory parameter not set", parameterName);
              }
          }

        //
        //  link parameters
        //

        for (String parameterName : journeyNode.getNodeType().getOutputConnectorParameters().keySet())
          {
            CriterionField parameterDeclaration = journeyNode.getNodeType().getOutputConnectorParameters().get(parameterName);
            for (JourneyLink journeyLink : journeyNode.getOutgoingLinks().values())
              {
                if (parameterDeclaration.getMandatoryParameter() && journeyLink.getLinkParameters().containsKey(parameterName) && journeyLink.getLinkParameters().get(parameterName) == null)
                  {
                    throw new GUIManagerException("mandatory parameter not set", parameterName);
                  }
              }
          }
      }

    /*****************************************
    *
    *  targeting criteria from start node (TEMPORARY)
    *
    *****************************************/

    //
    //  autoTargeted
    //

    if (startNode.getNodeParameters().containsKey("node.parameter.autotargeted"))
      {
        Boolean autoTargeted = (Boolean) startNode.getNodeParameters().get("node.parameter.autotargeted");
        if (autoTargeted != null && ! autoTargeted)
          {
            this.targetingType = TargetingType.Manual;
          }
      }

    //
    //  targetCriteria
    //

    if (startNode.getNodeParameters().containsKey("node.parameter.targetcriteria"))
      {
        this.targetingCriteria.addAll((List<EvaluationCriterion>) startNode.getNodeParameters().get("node.parameter.targetcriteria"));
      }

    /*****************************************
    *
    *  set evaluateContextVariables
    *
    *****************************************/

    for (JourneyNode journeyNode : journeyNodes.values())
      {
        boolean evaluateContextVariables = journeyNode.getNodeType().getAllowContextVariables() && journeyNode.getContextVariables().size() > 0;
        boolean evaluateContextVariablesOnEntry = evaluateContextVariables;
        for (JourneyLink outgoingLink : journeyNode.getOutgoingLinks().values())
          {
            evaluateContextVariablesOnEntry = evaluateContextVariablesOnEntry && ! outgoingLink.getEvaluateContextVariables();
            outgoingLink.setEvaluateContextVariables(evaluateContextVariables && outgoingLink.getEvaluateContextVariables());
          }
        journeyNode.setEvaluateContextVariables(evaluateContextVariablesOnEntry);
      }

    /*****************************************
    *
    *  resolve hard-coded subscriber messages
    *
    *****************************************/

    Set<SubscriberMessage> hardcodedSubscriberMessages = retrieveHardcodedSubscriberMessages(this);
    Set<SubscriberMessage> existingHardcodedSubscriberMessages = (existingJourney != null) ? retrieveHardcodedSubscriberMessages(existingJourney) : new HashSet<SubscriberMessage>();
    for (SubscriberMessage subscriberMessage : hardcodedSubscriberMessages)
      {
        //
        //  validate -- no parameterTags  
        //

        if (SubscriberMessageTemplate.resolveParameterTags(subscriberMessage.getDialogMessages()).size() > 0)
          {
            throw new GUIManagerException("illegal subscriberMessage", "parameterTags not allowed here");
          }

        //
        //  does this message already exist?
        //

        SubscriberMessage matchingSubscriberMessage = null;
        for (SubscriberMessage existingSubscriberMessage : existingHardcodedSubscriberMessages)
          {
            if (Objects.equals(subscriberMessage.getDialogMessages(), existingSubscriberMessage.getDialogMessages()))
              {
                matchingSubscriberMessage = existingSubscriberMessage;
                break;
              }
          }

        //
        //  resolve
        //

        if (matchingSubscriberMessage == null)
          {
            SubscriberMessageTemplate internalSubscriberMessageTemplate = SubscriberMessageTemplate.newInternalTemplate(subscriberMessage, subscriberMessageTemplateService, communicationChannelService);
            subscriberMessage.setSubscriberMessageTemplateID(internalSubscriberMessageTemplate.getSubscriberMessageTemplateID());
            subscriberMessageTemplateService.putSubscriberMessageTemplate(internalSubscriberMessageTemplate, true, null);
          }
        else
          {
            subscriberMessage.setSubscriberMessageTemplateID(matchingSubscriberMessage.getSubscriberMessageTemplateID());
          }
      }

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingJourney))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  constructor -- JSON without context -- for externals read-only (such as datacubes & reports)
  *
  *****************************************/

  public Journey(JSONObject jsonRoot, GUIManagedObjectType journeyType) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, journeyType, 0);
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.effectiveEntryPeriodEndDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "effectiveEntryPeriodEndDate", false));
    this.journeyParameters = decodeJourneyParameters(JSONUtilities.decodeJSONArray(jsonRoot, "journeyParameters", false));
    this.targetingType = TargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", "criteria"));
    this.eligibilityCriteria = decodeCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "eligibilityCriteria", false), Deployment.getJourneyUniversalEligibilityCriteria());
    this.targetingCriteria = decodeCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "targetingCriteria", false), new ArrayList<EvaluationCriterion>());
    this.targetID = decodeTargetIDs(JSONUtilities.decodeJSONArray(jsonRoot, "targetID", new JSONArray()));

    /*****************************************
    *
    *  jsonNodes
    *
    *****************************************/

    this.journeyNodes = new LinkedHashMap<String,JourneyNode>();
    JSONArray jsonArray = JSONUtilities.decodeJSONArray(jsonRoot, "nodes", true);
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            JSONObject nodeJSON = (JSONObject) jsonArray.get(i);
            String nodeID = JSONUtilities.decodeString(nodeJSON, "id", true);
            String nodeName = JSONUtilities.decodeString(nodeJSON, "name", nodeID);
            NodeType nodeType = Deployment.getNodeTypes().get(JSONUtilities.decodeString(nodeJSON, "nodeTypeID", true));
            
            journeyNodes.put(nodeID, new JourneyNode(nodeID, nodeName, nodeType, null, null, new ArrayList<String>(), new ArrayList<String>()));
          }
      }
    
    /*****************************************
    *
    *  startNodeID
    *
    *****************************************/

    this.startNodeID = null;
    for (JourneyNode journeyNode : this.journeyNodes.values())
      {
        if (journeyNode.getNodeType().getStartNode())
          {
            this.startNodeID = journeyNode.getNodeID();
            break;
          }
      }
    
    /*****************************************
    *
    *  endNodeID
    *
    *****************************************/

    this.endNodeID = null;
    for (JourneyNode journeyNode : this.journeyNodes.values())
      {
        if (journeyNode.getNodeType().getEndNode())
          {
            this.endNodeID = journeyNode.getNodeID();
            break;
          }
      }
  }

  /*****************************************
  *
  *  decodeTargetIDs
  *
  *****************************************/

  private List<String> decodeTargetIDs(JSONArray jsonArray)
  {
    List<String> targetIDs = new ArrayList<String>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        targetIDs.add((String) jsonArray.get(i));
      }
    return targetIDs;
  }

  /*****************************************
  *
  *  decodeJourneyParameters
  *
  *****************************************/

  public static Map<String,CriterionField> decodeJourneyParameters(JSONArray jsonArray) throws GUIManagerException
  {
    Map<String,CriterionField> journeyParameters = new LinkedHashMap<String,CriterionField>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            JSONObject journeyParameterJSON = (JSONObject) jsonArray.get(i);
            CriterionField originalJourneyParameter = new CriterionField(journeyParameterJSON);
            CriterionField enhancedJourneyParameter = new CriterionField(originalJourneyParameter, originalJourneyParameter.getID(), "getJourneyParameter", originalJourneyParameter.getInternalOnly(), originalJourneyParameter.getTagFormat(), originalJourneyParameter.getTagMaxLength());
            journeyParameters.put(enhancedJourneyParameter.getID(), enhancedJourneyParameter);
          }
      }
    return journeyParameters;
  }

  /*****************************************
  *
  *  decodeCriteria
  *
  *****************************************/

  private List<EvaluationCriterion> decodeCriteria(JSONArray jsonArray, List<EvaluationCriterion> universalCriteria) throws GUIManagerException
  {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();

    //
    //  universal criteria
    //

    result.addAll(universalCriteria);

    //
    //  journey-level targeting critera
    //

    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.DynamicProfile));
          }
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  decodeJourneyObjectiveInstances
  *
  *****************************************/

  private Set<JourneyObjectiveInstance> decodeJourneyObjectiveInstances(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    Set<JourneyObjectiveInstance> result = new HashSet<JourneyObjectiveInstance>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new JourneyObjectiveInstance((JSONObject) jsonArray.get(i), catalogCharacteristicService));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  decodeNodes
  *
  *****************************************/

  public static Map<String,GUINode> decodeNodes(JSONArray jsonArray, Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables, boolean contextVariableProcessing, SubscriberMessageTemplateService subscriberMessageTemplateService, DynamicEventDeclarationsService dynamicEventDeclarationsService) throws GUIManagerException
  {
    Map<String,GUINode> nodes = new LinkedHashMap<String,GUINode>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            //
            //  node
            //

            JSONObject nodeJSON = (JSONObject) jsonArray.get(i);
            GUINode node = new GUINode(nodeJSON, journeyParameters, contextVariables, contextVariableProcessing, subscriberMessageTemplateService, dynamicEventDeclarationsService);

            //
            //  validate (if required)
            //

            if (! contextVariableProcessing)
              {
                for (ContextVariable contextVariable : node.getContextVariables())
                  {
                    contextVariable.validate(node.getNodeCriterionContext());
                  }
              }

            //
            //  nodes
            //

            nodes.put(node.getNodeID(), node);
          }
      }
    return nodes;
  }

  /*****************************************
  *
  *  decodeLinks
  *
  *****************************************/

  private static List<GUILink> decodeLinks(JSONArray jsonArray) throws GUIManagerException
  {
    List<GUILink> links = new ArrayList<GUILink>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject linkJSON = (JSONObject) jsonArray.get(i);
        GUILink link = new GUILink(linkJSON);
        links.add(link);
      }
    return links;
  }

  /*****************************************
  *
  *  decodeBoundParameters
  *
  *****************************************/

  private ParameterMap decodeBoundParameters(JSONArray jsonArray, Map<String,CriterionField> journeyParameters, Map<String, CriterionField> contextVariables, SubscriberMessageTemplateService subscriberMessageTemplateService) throws GUIManagerException
  {
    CriterionContext criterionContext = new CriterionContext(journeyParameters, contextVariables);
    ParameterMap boundParameters = new ParameterMap();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
        String parameterName = JSONUtilities.decodeString(parameterJSON, "parameterName", true);
        CriterionField parameter = journeyParameters.get(parameterName);
        if (parameter == null) throw new GUIManagerException("unknown parameter", parameterName);
         switch (parameter.getFieldDataType())
          {
            case IntegerCriterion:
              boundParameters.put(parameterName, JSONUtilities.decodeInteger(parameterJSON, "value", false));
              break;

            case DoubleCriterion:
              boundParameters.put(parameterName, JSONUtilities.decodeDouble(parameterJSON, "value", false));
              break;

            case StringCriterion:
              boundParameters.put(parameterName, JSONUtilities.decodeString(parameterJSON, "value", false));
              break;

            case BooleanCriterion:
              boundParameters.put(parameterName, JSONUtilities.decodeBoolean(parameterJSON, "value", false));
              break;

            case DateCriterion:
              boundParameters.put(parameterName, GUIManagedObject.parseDateField(JSONUtilities.decodeString(parameterJSON, "value", false)));
              break;

            case StringSetCriterion:
              Set<String> stringSetValue = new HashSet<String>();
              JSONArray stringSetArray = JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray());
              for (int j=0; j<stringSetArray.size(); j++)
                {
                  stringSetValue.add((String) stringSetArray.get(j));
                }
              boundParameters.put(parameterName, stringSetValue);
              break;

            case EvaluationCriteriaParameter:
              List<EvaluationCriterion> evaluationCriteriaValue = new ArrayList<EvaluationCriterion>();
              JSONArray evaluationCriteriaArray = JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray());
              for (int j=0; j<evaluationCriteriaArray.size(); j++)
                {
                  evaluationCriteriaValue.add(new EvaluationCriterion((JSONObject) evaluationCriteriaArray.get(j), criterionContext));
                }
              boundParameters.put(parameterName, evaluationCriteriaValue);
              break;

            case SMSMessageParameter:
              SMSMessage smsMessageValue = new SMSMessage(parameterJSON.get("value"), subscriberMessageTemplateService, criterionContext);
              boundParameters.put(parameterName, smsMessageValue);
              break;

            case EmailMessageParameter:
              EmailMessage emailMessageValue = new EmailMessage(parameterJSON.get("value"), subscriberMessageTemplateService, criterionContext);
              boundParameters.put(parameterName, emailMessageValue);
              break;

            case PushMessageParameter:
              PushMessage pushMessageValue = new PushMessage(parameterJSON.get("value"), subscriberMessageTemplateService, criterionContext);
              boundParameters.put(parameterName, pushMessageValue);
              break;
          }
      }
    return boundParameters;
  }

  /*****************************************
  *
  *  retrieve hard-coded subscriber messages (i.e., that do NOT directly reference a template)
  *
  *****************************************/

  private static Set<SubscriberMessage> retrieveHardcodedSubscriberMessages(Journey journey)
  {
    /*****************************************
    *
    *  node/link parameters
    *
    *****************************************/

    Set<SubscriberMessage> result = new HashSet<SubscriberMessage>();
    for (JourneyNode journeyNode : journey.getJourneyNodes().values())
      {
        //
        //  node parameters
        //

        for (Object parameterValue : journeyNode.getNodeParameters().values())
          {
            if (parameterValue instanceof SubscriberMessage)
              {
                SubscriberMessage subscriberMessage = (SubscriberMessage) parameterValue;
                if (subscriberMessage.getDialogMessages().size() > 0)
                  {
                    result.add(subscriberMessage);
                  }
              }
          }

        //
        //  outgoing link parameters
        //

        for (JourneyLink journeyLink : journeyNode.getOutgoingLinks().values())
          {
            for (Object parameterValue : journeyLink.getLinkParameters().values())
              {
                if (parameterValue instanceof SubscriberMessage)
                  {
                    SubscriberMessage subscriberMessage = (SubscriberMessage) parameterValue;
                    if (subscriberMessage.getDialogMessages().size() > 0)
                      {
                        result.add(subscriberMessage);
                      }
                  }
              }
          }
      }

    /*****************************************
    *
    *  boundParameters
    *
    *****************************************/

    for (Object parameterValue : journey.getBoundParameters().values())
      {
        if (parameterValue instanceof SubscriberMessage)
          {
            SubscriberMessage subscriberMessage = (SubscriberMessage) parameterValue;
            if (subscriberMessage.getDialogMessages().size() > 0)
              {
                result.add(subscriberMessage);
              }
          }
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return result;
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(JourneyObjectiveService journeyObjectiveService, CatalogCharacteristicService catalogCharacteristicService, TargetService targetService, Date date) throws GUIManagerException
  {
    /****************************************
    *
    *  ensure valid/active journey objectives
    *
    ****************************************/

    Set<JourneyObjective> validJourneyObjectives = new HashSet<JourneyObjective>();
    for (JourneyObjectiveInstance journeyObjectiveInstance : journeyObjectiveInstances)
      {
        /*****************************************
        *
        *  retrieve journeyObjective
        *
        *****************************************/

        JourneyObjective journeyObjective = journeyObjectiveService.getActiveJourneyObjective(journeyObjectiveInstance.getJourneyObjectiveID(), date);

        /*****************************************
        *
        *  validate the journeyObjective exists and is active
        *
        *****************************************/

        if (journeyObjective == null)
          {
            log.info("journey {} uses unknown journey objective: {}", getJourneyID(), journeyObjectiveInstance.getJourneyObjectiveID());
            throw new GUIManagerException("journey uses unknown journey objective", journeyObjectiveInstance.getJourneyObjectiveID());
          }

        /*****************************************
        *
        *  validate the characteristics
        *
        *****************************************/

        //
        //  set of catalog characteristics defined for this journey objective
        //
            
        Set<String> configuredCatalogCharacteristics = new HashSet<String>();
        for (CatalogCharacteristicInstance catalogCharacteristicInstance : journeyObjectiveInstance.getCatalogCharacteristics())
          {
            configuredCatalogCharacteristics.add(catalogCharacteristicInstance.getCatalogCharacteristicID());
          }

        //
        //  validate against journeyObjective characteristics
        //
            
        if (! configuredCatalogCharacteristics.containsAll(journeyObjective.getCatalogCharacteristics()))
          {
            log.info("journey {}, objective {} does not specify all required catalog characteristics", getJourneyID(), journeyObjectiveInstance.getJourneyObjectiveID());
            throw new GUIManagerException("objective for journey missing required catalog characteristics", journeyObjectiveInstance.getJourneyObjectiveID());
          }
      }
    
    /****************************************
    *
    *  ensure valid/active target
    *
    ****************************************/

    if (targetID != null)
      {
        
        for(String currentTargetID : targetID){
          
          //
          //  retrieve target
          //
          
          Target target = targetService.getActiveTarget(currentTargetID, date);

          //
          //  validate the target exists and is active
          //
          
          if (target == null)
            {
              log.info("journey {} uses unknown/inactive target: {}", getJourneyID(), currentTargetID);
              throw new GUIManagerException("journey uses unknown target", currentTargetID);
            }
          
        }
        
      }
  }

  /*****************************************
  *
  *  processContextVariableNodes
  *
  *****************************************/

  public static Map<String, CriterionField> processContextVariableNodes(Map<String,GUINode> contextVariableNodes, Map<String,CriterionField> journeyParameters) throws GUIManagerException
  {
    /*****************************************
    *
    *  preparation
    *
    *****************************************/

    Map<ContextVariable,CriterionContext> contextVariables = new IdentityHashMap<ContextVariable,CriterionContext>();
    for (GUINode guiNode : contextVariableNodes.values())
      {
        for (ContextVariable contextVariable : guiNode.getContextVariables())
          {
            contextVariables.put(contextVariable, guiNode.getNodeCriterionContext());
          }
      }

    /*****************************************
    *
    *  process
    *
    *****************************************/

    Map<String,CriterionField> contextVariableFields = new HashMap<String,CriterionField>();
    Set<ContextVariable> unvalidatedContextVariables = new HashSet<ContextVariable>(contextVariables.keySet());
    Set<ContextVariable> newlyValidatedContextVariables = new HashSet<ContextVariable>();
    do
      {
        /*****************************************
        *
        *  validate as many contextVariables as possible using workingCriterionContext
        *
        *****************************************/

        //
        //  reset newlyValidatedContextVariables
        //

        newlyValidatedContextVariables.clear();

        //
        //  validate 
        //

        for (ContextVariable contextVariable : unvalidatedContextVariables)
          {
            try
              {
                //
                //  workingCriterionContext
                //

                CriterionContext workingCriterionContext = new CriterionContext(contextVariables.get(contextVariable), contextVariableFields);

                //
                //  validate
                //

                contextVariable.validate(workingCriterionContext);

                //
                //  mark as validated
                //

                newlyValidatedContextVariables.add(contextVariable);
              }
            catch (GUIManagerException e)
              {
                //
                //  ignore failure (remain in unvalidatedContextVariables)
                //
              }
          }

        //
        //  update unvalidated context variables
        //

        unvalidatedContextVariables.removeAll(newlyValidatedContextVariables);

        /*****************************************
        *
        *  find/resolve type conflicts with previously validated context variables
        *
        *****************************************/

        boolean anyFieldTypeModified = false;
        for (ContextVariable contextVariable : newlyValidatedContextVariables)
          {
            CriterionField criterionField = new CriterionField(contextVariable);
            CriterionField existingCriterionField = contextVariableFields.get(criterionField.getID());
            if (existingCriterionField != null)
              {
                //
                //  process
                //

                switch (criterionField.getFieldDataType())
                  {
                    case IntegerCriterion:
                      switch (existingCriterionField.getFieldDataType())
                        {
                          case IntegerCriterion:
                          case DoubleCriterion:
                            break;

                          default:
                            throw new GUIManagerException("inconsistent data types", criterionField.getID());
                        }
                      break;

                    case DoubleCriterion:
                      switch (existingCriterionField.getFieldDataType())
                        {
                          case IntegerCriterion:
                            contextVariableFields.put(criterionField.getID(), criterionField);
                            anyFieldTypeModified = true;
                            break;

                          case DoubleCriterion:
                            break;

                          default:
                            throw new GUIManagerException("inconsistent data types", criterionField.getID());
                        }
                      break;

                    case StringCriterion:
                    case BooleanCriterion:
                    case DateCriterion:
                    case StringSetCriterion:
                      if (contextVariableFields.get(criterionField.getID()).getFieldDataType() != criterionField.getFieldDataType())
                        {
                          throw new GUIManagerException("inconsistent data types", criterionField.getID());
                        }
                      break;

                    default:
                      throw new GUIManagerException("bad data type", criterionField.getFieldDataType().getExternalRepresentation());
                  }
              }
            else
              {
                contextVariableFields.put(criterionField.getID(), criterionField);
              }
          }

        /*****************************************
        *
        *  revalidate all context variables if any field type was modified
        *
        *****************************************/

        if (anyFieldTypeModified)
          {
            unvalidatedContextVariables.addAll(contextVariables.keySet());
          }
      }
    while (unvalidatedContextVariables.size() > 0 && newlyValidatedContextVariables.size() > 0);

    /*****************************************
    *
    *  all context variables validated?
    *
    *****************************************/

    if (unvalidatedContextVariables.size() > 0)
      {
        throw new GUIManagerException("unvalidatedContextVariables", Integer.toString(unvalidatedContextVariables.size()));
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/
    
    return contextVariableFields;
  }
  
  /*****************************************
  *
  *  isExpressionValuedParameterValue
  *
  *****************************************/

  public static boolean isExpressionValuedParameterValue(JSONObject parameterJSON)
  {
    return (parameterJSON.get("value") instanceof JSONObject) && (((JSONObject) parameterJSON.get("value")).get("expression") != null);
  }

  /*****************************************************************************
  *
  *  class GUINode
  *
  *****************************************************************************/
  
  public static class GUINode
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String nodeID;
    private String nodeName;
    private NodeType nodeType;
    private ParameterMap nodeParameters;
    private List<OutgoingConnectionPoint> outgoingConnectionPoints;
    private List<ContextVariable> contextVariables;
    private CriterionContext nodeCriterionContext;
    private CriterionContext linkCriterionContext;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getNodeID() { return nodeID; }
    public String getNodeName() { return nodeName; }
    public NodeType getNodeType() { return nodeType; }
    public ParameterMap getNodeParameters() { return nodeParameters; }
    public List<OutgoingConnectionPoint> getOutgoingConnectionPoints() { return outgoingConnectionPoints; }
    public List<ContextVariable> getContextVariables() { return contextVariables; }
    public CriterionContext getNodeCriterionContext() { return nodeCriterionContext; }
    public CriterionContext getLinkCriterionContext() { return linkCriterionContext; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public GUINode(JSONObject jsonRoot, Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables, boolean contextVariableProcessing, SubscriberMessageTemplateService subscriberMessageTemplateService, DynamicEventDeclarationsService dynamicEventDeclarationsService) throws GUIManagerException
    {
      /*****************************************
      *
      *  process these fields in all situations
      *
      *****************************************/

      //
      //  data
      //

      this.nodeID = JSONUtilities.decodeString(jsonRoot, "id", true);
      this.nodeName = JSONUtilities.decodeString(jsonRoot, "name", this.nodeID);
      this.nodeType = Deployment.getNodeTypes().get(JSONUtilities.decodeString(jsonRoot, "nodeTypeID", true));

      //
      //  validate nodeType
      //

      if (this.nodeType == null) throw new GUIManagerException("unknown nodeType", JSONUtilities.decodeString(jsonRoot, "nodeTypeID"));

      //
      //  nodeParameters (independent, i.e., not EvaluationCriteria or Messages)
      //

      this.nodeParameters = decodeIndependentNodeParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", true), nodeType);

      //
      //  eventName
      //

      String eventName = this.nodeParameters.containsKey("node.parameter.eventname") ? (String) this.nodeParameters.get("node.parameter.eventname") : null;
      EvolutionEngineEventDeclaration nodeEvent = (eventName != null) ? dynamicEventDeclarationsService.getStaticAndDynamicEvolutionEventDeclarations().get(eventName) : null;
      if (eventName != null && nodeEvent == null) throw new GUIManagerException("unknown event", eventName);

      //
      //  criterionContext
      //

      this.nodeCriterionContext = new CriterionContext(journeyParameters, contextVariables, this.nodeType, nodeEvent, false);
      this.linkCriterionContext = new CriterionContext(journeyParameters, contextVariables, this.nodeType, nodeEvent, true);

      //
      //  contextVariables
      //

      this.contextVariables = nodeType.getAllowContextVariables() ? decodeContextVariables(JSONUtilities.decodeJSONArray(jsonRoot, "contextVariables", false)) : Collections.<ContextVariable>emptyList();

      /*****************************************
      *
      *  process these fields only if NOT doing contextVariableProcessing
      *
      *****************************************/

      if (! contextVariableProcessing)
        {
          //
          //  nodeParameters (dependent, ie., EvaluationCriteria and Messages which are dependent on other parameters)
          //

          this.nodeParameters.putAll(decodeDependentNodeParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", true), nodeType, nodeCriterionContext, subscriberMessageTemplateService));
          this.nodeParameters.putAll(decodeExpressionValuedParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", true), nodeType, nodeCriterionContext));

          //
          //  outputConnectors
          //

          this.outgoingConnectionPoints = decodeOutgoingConnectionPoints(JSONUtilities.decodeJSONArray(jsonRoot, "outputConnectors", true), nodeType, linkCriterionContext, subscriberMessageTemplateService);
        }
    }

    /*****************************************
    *
    *  decodeIndependentNodeParameters
    *
    *****************************************/

    private ParameterMap decodeIndependentNodeParameters(JSONArray jsonArray, NodeType nodeType) throws GUIManagerException
    {
      ParameterMap nodeParameters = new ParameterMap();
      for (int i=0; i<jsonArray.size(); i++)
        {
          JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
          String parameterName = JSONUtilities.decodeString(parameterJSON, "parameterName", true);
          CriterionField parameter = nodeType.getParameters().get(parameterName);
          if (parameter == null) throw new GUIManagerException("unknown parameter", parameterName);
          if (Journey.isExpressionValuedParameterValue(parameterJSON)) continue;
          switch (parameter.getFieldDataType())
            {
              case IntegerCriterion:
                nodeParameters.put(parameterName, JSONUtilities.decodeInteger(parameterJSON, "value", false));
                break;

              case DoubleCriterion:
                nodeParameters.put(parameterName, JSONUtilities.decodeDouble(parameterJSON, "value", false));
                break;
                
              case StringCriterion:
                nodeParameters.put(parameterName, JSONUtilities.decodeString(parameterJSON, "value", false));
                break;
                
              case BooleanCriterion:
                nodeParameters.put(parameterName, JSONUtilities.decodeBoolean(parameterJSON, "value", false));
                break;
                
              case DateCriterion:
                nodeParameters.put(parameterName, GUIManagedObject.parseDateField(JSONUtilities.decodeString(parameterJSON, "value", false)));
                break;
                
              case StringSetCriterion:
                Set<String> stringSetValue = new HashSet<String>();
                JSONArray stringSetArray = JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray());
                for (int j=0; j<stringSetArray.size(); j++)
                  {
                    stringSetValue.add((String) stringSetArray.get(j));
                  }
                nodeParameters.put(parameterName, stringSetValue);
                break;
            }
        }
      return nodeParameters;
    }

    /*****************************************
    *
    *  decodeDependentNodeParameters
    *
    *****************************************/

    private ParameterMap decodeDependentNodeParameters(JSONArray jsonArray, NodeType nodeType, CriterionContext criterionContext, SubscriberMessageTemplateService subscriberMessageTemplateService) throws GUIManagerException
    {
      ParameterMap nodeParameters = new ParameterMap();
      for (int i=0; i<jsonArray.size(); i++)
        {
          /*****************************************
          *
          *  parameter
          *
          *****************************************/

          JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
          String parameterName = JSONUtilities.decodeString(parameterJSON, "parameterName", true);
          CriterionField parameter = nodeType.getParameters().get(parameterName);
          if (parameter == null) throw new GUIManagerException("unknown parameter", parameterName);
          if (Journey.isExpressionValuedParameterValue(parameterJSON)) continue;

          /*****************************************
          *
          *  constant
          *
          *****************************************/

          switch (parameter.getFieldDataType())
            {
              case EvaluationCriteriaParameter:
                List<EvaluationCriterion> evaluationCriteriaValue = new ArrayList<EvaluationCriterion>();
                JSONArray evaluationCriteriaArray = JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray());
                for (int j=0; j<evaluationCriteriaArray.size(); j++)
                  {
                    evaluationCriteriaValue.add(new EvaluationCriterion((JSONObject) evaluationCriteriaArray.get(j), criterionContext));
                  }
                nodeParameters.put(parameterName, evaluationCriteriaValue);
                break;

              case SMSMessageParameter:
                SMSMessage smsMessageValue = new SMSMessage(parameterJSON.get("value"), subscriberMessageTemplateService, criterionContext);
                nodeParameters.put(parameterName, smsMessageValue);
                break;

              case EmailMessageParameter:
                EmailMessage emailMessageValue = new EmailMessage(parameterJSON.get("value"), subscriberMessageTemplateService, criterionContext);
                nodeParameters.put(parameterName, emailMessageValue);
                break;

              case PushMessageParameter:
                PushMessage pushMessageValue = new PushMessage(parameterJSON.get("value"), subscriberMessageTemplateService, criterionContext);
                nodeParameters.put(parameterName, pushMessageValue);
                break;
            }
        }
      return nodeParameters;
    }

    /*****************************************
    *
    *  decodeExpressionValuedParameters
    *
    *****************************************/

    private ParameterMap decodeExpressionValuedParameters(JSONArray jsonArray, NodeType nodeType, CriterionContext criterionContext) throws GUIManagerException
    {
      ParameterMap nodeParameters = new ParameterMap();
      for (int i=0; i<jsonArray.size(); i++)
        {
          /*****************************************
          *
          *  parameter
          *
          *****************************************/

          JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
          String parameterName = JSONUtilities.decodeString(parameterJSON, "parameterName", true);
          CriterionField parameter = nodeType.getParameters().get(parameterName);
          if (parameter == null) throw new GUIManagerException("unknown parameter", parameterName);
          if (! isExpressionValuedParameterValue(parameterJSON)) continue;

          /*****************************************
          *
          *  expression
          *
          *****************************************/

          //
          //  parse
          //

          ParameterExpression parameterExpressionValue = new ParameterExpression(JSONUtilities.decodeJSONObject(parameterJSON, "value", true), criterionContext);
          nodeParameters.put(parameterName, parameterExpressionValue);

          //
          //  valid combination
          //

          boolean validCombination = false;
          switch (parameter.getFieldDataType())
            {
              case IntegerCriterion:
              case DoubleCriterion:
                switch (parameterExpressionValue.getType())
                  {
                    case IntegerExpression:
                    case DoubleExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;

              case StringCriterion:
                switch (parameterExpressionValue.getType())
                  {
                    case StringExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;

              case BooleanCriterion:
                switch (parameterExpressionValue.getType())
                  {
                    case BooleanExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;

              case DateCriterion:
                switch (parameterExpressionValue.getType())
                  {
                    case DateExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;


              case EvaluationCriteriaParameter:
              case SMSMessageParameter:
              case EmailMessageParameter:
              case PushMessageParameter:
                switch (parameterExpressionValue.getType())
                  {
                    case OpaqueReferenceExpression:
                      validCombination = ((ReferenceExpression) (parameterExpressionValue.getExpression())).getCriterionDataType() == parameter.getFieldDataType();
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;

              default:
                validCombination = false;
                break;
            }

          //
          //  validate
          //

          if (!validCombination) throw new GUIManagerException("dataType/expression combination", parameter.getFieldDataType().getExternalRepresentation() + "/" + parameterExpressionValue.getType());
        }
      return nodeParameters;
    }

    /*****************************************
    *
    *  decodeOutgoingConnectionPoints
    *
    *****************************************/

    private List<OutgoingConnectionPoint> decodeOutgoingConnectionPoints(JSONArray jsonArray, NodeType nodeType, CriterionContext criterionContext, SubscriberMessageTemplateService subscriberMessageTemplateService) throws GUIManagerException
    {
      List<OutgoingConnectionPoint> outgoingConnectionPoints = new ArrayList<OutgoingConnectionPoint>();
      for (int i=0; i<jsonArray.size(); i++)
        {
          JSONObject connectionPointJSON = (JSONObject) jsonArray.get(i);
          OutgoingConnectionPoint outgoingConnectionPoint = new OutgoingConnectionPoint(connectionPointJSON, nodeType, criterionContext, subscriberMessageTemplateService);
          outgoingConnectionPoints.add(outgoingConnectionPoint);
        }
      return outgoingConnectionPoints;
    }

    /*****************************************
    *
    *  decodeContextVariables
    *
    *****************************************/

    private static List<ContextVariable> decodeContextVariables(JSONArray jsonArray) throws GUIManagerException
    {
      List<ContextVariable> contextVariables = new ArrayList<ContextVariable>();
      if (jsonArray != null)
        {
          for (int i=0; i<jsonArray.size(); i++)
            {
              JSONObject contextVariableJSON = (JSONObject) jsonArray.get(i);
              contextVariables.add(new ContextVariable(contextVariableJSON));
            }
        }
      return contextVariables;
    }
  }

  /*****************************************************************************
  *
  *  class OutgoingConnectionPoint
  *
  *****************************************************************************/

  public static class OutgoingConnectionPoint
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String name;
    private String display;
    private ParameterMap outputConnectorParameters;
    private EvaluationPriority evaluationPriority;
    private boolean evaluateContextVariables;
    private List<EvaluationCriterion> transitionCriteria;
    private String additionalCriteria;
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/
    
    public String getName() { return name; }
    public String getDisplay() { return display; }
    public ParameterMap getOutputConnectorParameters() { return outputConnectorParameters; }
    public EvaluationPriority getEvaluationPriority() { return evaluationPriority; }
    public boolean getEvaluateContextVariables() { return evaluateContextVariables; }
    public List<EvaluationCriterion> getTransitionCriteria() { return transitionCriteria; }
    public String getAdditionalCriteria() { return additionalCriteria; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public OutgoingConnectionPoint(JSONObject jsonRoot, NodeType nodeType, CriterionContext criterionContext, SubscriberMessageTemplateService subscriberMessageTemplateService) throws GUIManagerException
    {
      this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
      this.display = JSONUtilities.decodeString(jsonRoot, "display", true);
      this.outputConnectorParameters = decodeOutputConnectorParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", false), nodeType, criterionContext, subscriberMessageTemplateService);
      this.evaluationPriority = EvaluationPriority.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "evaluationPriority", "normal"));
      this.evaluateContextVariables = JSONUtilities.decodeBoolean(jsonRoot, "evaluateContextVariables", Boolean.FALSE);
      this.transitionCriteria = decodeTransitionCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "transitionCriteria", false), criterionContext);
      this.additionalCriteria = JSONUtilities.decodeString(jsonRoot, "additionalCriteria", false);
    }

    /*****************************************
    *
    *  decodeOutputConnectorParameters
    *
    *****************************************/

    private ParameterMap decodeOutputConnectorParameters(JSONArray jsonArray, NodeType nodeType, CriterionContext criterionContext, SubscriberMessageTemplateService subscriberMessageTemplateService) throws GUIManagerException
    {
      ParameterMap outputConnectorParameters = new ParameterMap();
      if (jsonArray != null)
        {
          for (int i=0; i<jsonArray.size(); i++)
            {
              JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
              String parameterName = JSONUtilities.decodeString(parameterJSON, "parameterName", true);
              CriterionField parameter = nodeType.getOutputConnectorParameters().get(parameterName);
              if (parameter == null) throw new GUIManagerException("unknown parameter", parameterName);
              switch (parameter.getFieldDataType())
                {
                  case IntegerCriterion:
                    outputConnectorParameters.put(parameterName, JSONUtilities.decodeInteger(parameterJSON, "value", false));
                    break;

                  case DoubleCriterion:
                    outputConnectorParameters.put(parameterName, JSONUtilities.decodeDouble(parameterJSON, "value", false));
                    break;

                  case StringCriterion:
                    outputConnectorParameters.put(parameterName, JSONUtilities.decodeString(parameterJSON, "value", false));
                    break;

                  case BooleanCriterion:
                    outputConnectorParameters.put(parameterName, JSONUtilities.decodeBoolean(parameterJSON, "value", false));
                    break;

                  case DateCriterion:
                    outputConnectorParameters.put(parameterName, GUIManagedObject.parseDateField(JSONUtilities.decodeString(parameterJSON, "value", false)));
                    break;

                  case StringSetCriterion:
                    Set<String> stringSetValue = new HashSet<String>();
                    JSONArray stringSetArray = JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray());
                    for (int j=0; j<stringSetArray.size(); j++)
                      {
                        stringSetValue.add((String) stringSetArray.get(j));
                      }
                    outputConnectorParameters.put(parameterName, stringSetValue);
                    break;

                  case EvaluationCriteriaParameter:
                    List<EvaluationCriterion> evaluationCriteriaValue = new ArrayList<EvaluationCriterion>();
                    JSONArray evaluationCriteriaArray = JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray());
                    for (int j=0; j<evaluationCriteriaArray.size(); j++)
                      {
                        evaluationCriteriaValue.add(new EvaluationCriterion((JSONObject) evaluationCriteriaArray.get(j), criterionContext));
                      }
                    outputConnectorParameters.put(parameterName, evaluationCriteriaValue);
                    break;

                  case SMSMessageParameter:
                    SMSMessage smsMessageValue = new SMSMessage(parameterJSON.get("value"), subscriberMessageTemplateService, criterionContext);
                    outputConnectorParameters.put(parameterName, smsMessageValue);
                    break;

                  case EmailMessageParameter:
                    EmailMessage emailMessageValue = new EmailMessage(parameterJSON.get("value"), subscriberMessageTemplateService, criterionContext);
                    outputConnectorParameters.put(parameterName, emailMessageValue);
                    break;

                  case PushMessageParameter:
                    PushMessage pushMessageValue = new PushMessage(parameterJSON.get("value"), subscriberMessageTemplateService, criterionContext);
                    outputConnectorParameters.put(parameterName, pushMessageValue);
                    break;
                }
            }
        }
      return outputConnectorParameters;
    }

    /*****************************************
    *
    *  decodeTransitionCriteria
    *
    *****************************************/

    private List<EvaluationCriterion> decodeTransitionCriteria(JSONArray jsonArray, CriterionContext criterionContext) throws GUIManagerException
    {
      List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
      if (jsonArray != null)
        {
          for (int i=0; i<jsonArray.size(); i++)
            {
              result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), criterionContext));
            }
        }
      return result;
    }
  }

  /*****************************************************************************
  *
  *  class GUILink
  *
  *****************************************************************************/
  
  private static class GUILink
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String sourceNodeID;
    private int sourceConnectionPoint;
    private String destinationNodeID;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getSourceNodeID() { return sourceNodeID; }
    public int getSourceConnectionPoint() { return sourceConnectionPoint; }
    public String getDestinationNodeID() { return destinationNodeID; }

    /*****************************************
    *
    *  constructor -- campaign/journey json
    *
    *****************************************/

    public GUILink(JSONObject jsonRoot) throws GUIManagerException
    {
      this.sourceNodeID = JSONUtilities.decodeString(jsonRoot, "sourceNodeID", true);
      this.sourceConnectionPoint = JSONUtilities.decodeInteger(jsonRoot, "sourceConnectionPoint", true);
      this.destinationNodeID = JSONUtilities.decodeString(jsonRoot, "destinationNodeID", true);
    }

    /*****************************************
    *
    *  constructor -- implicit link
    *
    *****************************************/

    private GUILink(String sourceNodeID, int sourceConnectionPoint, String destinationNodeID)
    {
      this.sourceNodeID = sourceNodeID;
      this.sourceConnectionPoint = sourceConnectionPoint;
      this.destinationNodeID = destinationNodeID;
    }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Journey existingJourney)
  {
    if (existingJourney != null && existingJourney.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingJourney.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(effectiveEntryPeriodEndDate, existingJourney.getRawEffectiveEntryPeriodEndDate());
        epochChanged = epochChanged || ! Objects.equals(journeyParameters, existingJourney.getJourneyParameters());
        epochChanged = epochChanged || ! (targetingType == existingJourney.getTargetingType());
        epochChanged = epochChanged || ! Objects.equals(eligibilityCriteria, existingJourney.getEligibilityCriteria());
        epochChanged = epochChanged || ! Objects.equals(targetingCriteria, existingJourney.getTargetingCriteria());
        epochChanged = epochChanged || ! Objects.equals(targetID, existingJourney.getTargetID());
        epochChanged = epochChanged || ! Objects.equals(startNodeID, existingJourney.getStartNodeID());
        epochChanged = epochChanged || ! Objects.equals(endNodeID, existingJourney.getEndNodeID());
        epochChanged = epochChanged || ! Objects.equals(journeyObjectiveInstances, existingJourney.getJourneyObjectiveInstances());
        epochChanged = epochChanged || ! Objects.equals(journeyNodes, existingJourney.getJourneyNodes());
        epochChanged = epochChanged || ! Objects.equals(journeyLinks, existingJourney.getJourneyLinks());
        epochChanged = epochChanged || ! Objects.equals(boundParameters, existingJourney.getBoundParameters());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

  /*****************************************
  *
  *  class SetStatusAction
  *
  *****************************************/

  public static class SetStatusAction extends ActionManager
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public SetStatusAction(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
    }
        
    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public List<Action> executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      SubscriberJourneyStatusField statusField = SubscriberJourneyStatusField.fromExternalRepresentation(subscriberEvaluationRequest.getJourneyNode().getNodeParameters().containsKey("node.parameter.journeystatus") ? (String) CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.journeystatus") : "(unknown)");
      if (statusField == null) throw new ServerRuntimeException("unknown status field: " + CriterionFieldRetriever.getJourneyNodeParameter(subscriberEvaluationRequest,"node.parameter.journeystatus"));
      ContextUpdate contextUpdate = new ContextUpdate(ActionType.JourneyContextUpdate);
      contextUpdate.getParameters().put(statusField.getJourneyParameterName(), Boolean.TRUE);
      return Collections.<Action>singletonList(contextUpdate);
    }
  }

  /*****************************************
  *
  *  class ControlGroupAction
  *
  *****************************************/

  public static class ControlGroupAction extends ActionManager
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ControlGroupAction(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
    }
        
    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public List<Action> executeOnExit(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest, JourneyLink journeyLink)
    {
      ContextUpdate contextUpdate = new ContextUpdate(ActionType.JourneyContextUpdate);
      switch (journeyLink.getLinkName())
        {
          case "controlGroup":
            contextUpdate.getParameters().put(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName(), Boolean.TRUE);            
            break;
          case "universalControlGroup":
            contextUpdate.getParameters().put(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName(), Boolean.TRUE);            
            break;
        }
      return Collections.<Action>singletonList(contextUpdate);
    }
  }
  
  /*****************************************
  *
  *  class ABTestingAction
  *
  *****************************************/

  public static class ABTestingAction extends ActionManager
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ABTestingAction(JSONObject configuration) throws GUIManagerException
    {
      super(configuration);
    }
        
    /*****************************************
    *
    *  execute
    *
    *****************************************/

    @Override public List<Action> executeOnExit(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest, JourneyLink journeyLink)
    {
      ContextUpdate contextUpdate = new ContextUpdate(ActionType.JourneyContextUpdate);
      contextUpdate.getParameters().put(journeyLink.getLinkName(), journeyLink.getLinkDisplay());
      return Collections.<Action>singletonList(contextUpdate);
    }
  }

  /*****************************************
  *
  *  class ContextUpdate
  *
  *****************************************/

  public static class ContextUpdate implements Action
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private ActionType actionType;
    private ParameterMap parameters;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public ActionType getActionType() { return actionType; }
    public ParameterMap getParameters() { return parameters; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ContextUpdate(ActionType actionType)
    {
      this.actionType = actionType;
      this.parameters = new ParameterMap();
    }
  }
}
