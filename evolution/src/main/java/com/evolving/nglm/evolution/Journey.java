/*****************************************************************************
*
*  Journey.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.stream.Collectors;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.Expression.ReferenceExpression;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.JourneyHistory.StatusHistory;
import com.evolving.nglm.evolution.StockMonitor.StockableItem;
import com.evolving.nglm.evolution.notification.NotificationTemplateParameters;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

@GUIDependencyDef(objectType = "journey", serviceClass = JourneyService.class, dependencies = { "campaign", "journeyobjective" , "target"})
public class Journey extends GUIManagedObject implements StockableItem
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
    PendingNotApproved("PendingNotApproved"),
    WaitingForApproval("WaitingForApproval"),
    StartedApproved("StartedApproved"),
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
    NotEligible("notEligible", "NotEligible"),
    Excluded("excluded", "Excluded"),
    ObjectiveLimitReached("objective_limitReached", "ObjectiveLimitReached"),
    Entered("entered", "Entered"),
    Targeted("targeted", "Targeted"),
    Notified("notified", "Notified"),
    ConvertedNotNotified("unnotified_converted", "Converted"),
    ConvertedNotified("notified_converted", "ConvertedNotified"),
    ControlGroup("controlGroup", "Control"),
    UniversalControlGroup("UniversalControlGroup", "UCG"),
    ControlGroupConverted("controlGroup_converted", "ControlConverted"),
    UniversalControlGroupConverted("UniversalControlGroup_converted", "UCGConverted"),
    Unknown("(unknown)", "Unknown");
    private String externalRepresentation;
    private String display;
    private SubscriberJourneyStatus(String externalRepresentation, String display) { this.externalRepresentation = externalRepresentation; this.display = display; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getDisplay() { return display; }
    public static SubscriberJourneyStatus fromExternalRepresentation(String externalRepresentation) { for (SubscriberJourneyStatus enumeratedValue : SubscriberJourneyStatus.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
    public boolean in (SubscriberJourneyStatus ... states) {
        return Arrays.asList(states).contains(this);
    }
  }

  //
  //  SubscriberJourneyStatusField
  //

  public enum SubscriberJourneyStatusField
  {
    StatusNotified("statusNotified", "journey.status.notified"),
    StatusConverted("statusConverted", "journey.status.converted"),
    StatusTargetGroup("statusTargetGroup", "journey.status.statustargetgroup"),
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

  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static int currentSchemaVersion = 8;
  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("journey");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(), currentSchemaVersion));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("effectiveEntryPeriodEndDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("templateParameters", SchemaBuilder.map(Schema.STRING_SCHEMA, CriterionField.schema()).name("journey_template_parameters").defaultValue(new HashMap<String,CriterionField>()).schema());
    schemaBuilder.field("journeyParameters", SchemaBuilder.map(Schema.STRING_SCHEMA, CriterionField.schema()).name("journey_journey_parameters").schema());
    schemaBuilder.field("contextVariables", SchemaBuilder.map(Schema.STRING_SCHEMA, CriterionField.schema()).name("journey_context_variables").schema());
    schemaBuilder.field("targetingType", Schema.STRING_SCHEMA);
    schemaBuilder.field("eligibilityCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("targetingCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("targetingEventCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).optional().schema());
    schemaBuilder.field("targetID", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().schema());
    schemaBuilder.field("startNodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("endNodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyObjectives", SchemaBuilder.array(JourneyObjectiveInstance.schema()).schema());
    schemaBuilder.field("journeyNodes", SchemaBuilder.array(JourneyNode.schema()).schema());
    schemaBuilder.field("journeyLinks", SchemaBuilder.array(JourneyLink.schema()).schema());
    schemaBuilder.field("boundParameters", ParameterMap.schema());
    schemaBuilder.field("appendInclusionLists", SchemaBuilder.bool().defaultValue(false).schema());
    schemaBuilder.field("appendExclusionLists", SchemaBuilder.bool().defaultValue(false).schema());
    schemaBuilder.field("appendUCG", SchemaBuilder.bool().defaultValue(false).schema());
    schemaBuilder.field("approval", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("maxNoOfCustomers", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("fullStatistics", SchemaBuilder.bool().defaultValue(false).schema());

    schemaBuilder.field("recurrence", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("recurrenceId", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("occurrenceNumber", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("scheduler", JourneyScheduler.serde().optionalSchema());
    schemaBuilder.field("lastCreatedOccurrenceNumber", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("recurrenceActive", Schema.BOOLEAN_SCHEMA);

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
  private Map<String,CriterionField> templateParameters;
  private Map<String,CriterionField> journeyParameters;
  private Map<String,CriterionField> contextVariables;
  private TargetingType targetingType;
  private List<EvaluationCriterion> eligibilityCriteria;
  private List<EvaluationCriterion> targetingCriteria;
  private List<EvaluationCriterion> targetingEventCriteria;
  private List<String> targetID;
  private String startNodeID;
  private String endNodeID;
  private Set<JourneyObjectiveInstance> journeyObjectiveInstances; 
  private Map<String,JourneyNode> journeyNodes;
  private Map<String,JourneyLink> journeyLinks;
  private ParameterMap boundParameters;
  private boolean appendInclusionLists;
  private boolean appendExclusionLists;
  private boolean appendUCG;
  private JourneyStatus approval;
  private Integer maxNoOfCustomers;
  private boolean fullStatistics;

  //
  //  recurrence
  //

  private boolean recurrence;
  private String recurrenceId;
  private Integer occurrenceNumber;
  private JourneyScheduler journeyScheduler;
  private Integer lastCreatedOccurrenceNumber;
  private boolean recurrenceActive;


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
  public Map<String,CriterionField> getTemplateParameters() { return templateParameters; }
  public Map<String,CriterionField> getJourneyParameters() { return journeyParameters; }
  public Map<String,CriterionField> getContextVariables() { return contextVariables; }
  public TargetingType getTargetingType() { return targetingType; }
  public List<EvaluationCriterion> getEligibilityCriteria() { return eligibilityCriteria; }
  public List<EvaluationCriterion> getTargetingCriteria() { return targetingCriteria; }
  public List<EvaluationCriterion> getTargetingEventCriteria() { return targetingEventCriteria; }
  public List<String> getTargetID() { return targetID; }
  public String getStartNodeID() { return startNodeID; }
  public String getEndNodeID() { return endNodeID; }
  public Set<JourneyObjectiveInstance> getJourneyObjectiveInstances() { return journeyObjectiveInstances;  }
  public Map<String,JourneyNode> getJourneyNodes() { return journeyNodes; }
  public Map<String,JourneyLink> getJourneyLinks() { return journeyLinks; }
  public JourneyNode getJourneyNode(String nodeID) { return journeyNodes.get(nodeID); }
  public JourneyLink getJourneyLink(String linkID) { return journeyLinks.get(linkID); }
  public ParameterMap getBoundParameters() { return boundParameters; }
  public boolean getAppendInclusionLists() { return appendInclusionLists; }
  public boolean getAppendExclusionLists() { return appendExclusionLists; }
  public boolean getAppendUCG() { return appendUCG; }
  public JourneyStatus getApproval() {return JourneyStatus.Unknown == approval ? JourneyStatus.Pending : approval; }
  public void setApproval(JourneyStatus approval) { this.approval = approval; }
  public Integer getMaxNoOfCustomers(){ return maxNoOfCustomers; }
  public boolean getFullStatistics() { return fullStatistics; }

  //
  // journey customers limit implemented thanks to stocks :
  //

  @Override public String getStockableItemID() { return "maxNoOfCustomers-journey-"+getJourneyID(); }
  @Override public Integer getStock() { return getMaxNoOfCustomers(); }

  //
  // recurrence
  //

  public boolean getRecurrence() { return recurrence ; }
  public String getRecurrenceId() { return recurrenceId ; }
  public Integer getOccurrenceNumber() { return occurrenceNumber ; }
  public JourneyScheduler getJourneyScheduler() { return journeyScheduler ; }
  public Integer getLastCreatedOccurrenceNumber() {return lastCreatedOccurrenceNumber; }
  public boolean getRecurrenceActive() { return recurrenceActive; }

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
  //  workflow
  //

  public boolean isWorkflow()
  {
    boolean result = false;
    switch (getGUIManagedObjectType())
      {
        case Workflow:
        case LoyaltyWorkflow:
          result = true;
          break;
        default:
          result = false;
          break;
      }
    return result;
  }

  //
  //  getAllCriteria
  //

  public List<List<EvaluationCriterion>> getAllTargetsCriteria(TargetService targetService, Date now, int tenantID)
  {
    try
      {
        //
        // result
        //

        List<List<EvaluationCriterion>> result = new ArrayList<List<EvaluationCriterion>>();

        //
        // target
        //

        if (targetID != null && !targetID.isEmpty())
          {

            for (String currentTargetID : targetID)
              {
                //
                // get the target
                //

                Target target = targetService.getActiveTarget(currentTargetID, now);

                //
                // target not active -- automatic false criteria
                //

                if (target == null)
                  {
                    Map<String, Object> falseCriterionArgumentJSON = new LinkedHashMap<String, Object>();
                    Map<String, Object> falseCriterionJSON = new LinkedHashMap<String, Object>();
                    falseCriterionArgumentJSON.put("expression", "false");
                    falseCriterionJSON.put("criterionField", "internal.false");
                    falseCriterionJSON.put("criterionOperator", "<>");
                    falseCriterionJSON.put("argument", JSONUtilities.encodeObject(falseCriterionArgumentJSON));
                    List<EvaluationCriterion> toAdd = new ArrayList<>();
                    toAdd.add(new EvaluationCriterion(JSONUtilities.encodeObject(falseCriterionJSON), CriterionContext.Profile(tenantID), tenantID));
                    result.add(toAdd);
                  }

                else
                  {
                    Map<String, Object> targetCriterionArgumentJSON = new LinkedHashMap<String, Object>();
                    Map<String, Object> targetCriterionJSON = new LinkedHashMap<String, Object>();
                    targetCriterionArgumentJSON.put("expression", "'" + currentTargetID + "'");
                    targetCriterionJSON.put("criterionField", "internal.targets");
                    targetCriterionJSON.put("criterionOperator", "contains");
                    targetCriterionJSON.put("argument", JSONUtilities.encodeObject(targetCriterionArgumentJSON));
                    List<EvaluationCriterion> toAdd = new ArrayList<>();
                    toAdd.add(new EvaluationCriterion(JSONUtilities.encodeObject(targetCriterionJSON), CriterionContext.Profile(tenantID), tenantID));
                    result.add(toAdd);
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
  public static SubscriberJourneyStatus getSubscriberJourneyStatus(boolean statusConverted, boolean statusNotified, Boolean statusTargetGroup, Boolean statusControlGroup, Boolean statusUniversalControlGroup)
  {
  // Non UCG
  if (statusUniversalControlGroup == null || statusUniversalControlGroup == Boolean.FALSE)
  {
    // Non CG
    if (statusControlGroup == null || statusControlGroup == Boolean.FALSE) {
      // Status not updated yet
      if (! statusNotified && ! statusConverted) {
        if (statusTargetGroup == null || statusTargetGroup == Boolean.FALSE)
          return SubscriberJourneyStatus.Entered;
        else
          return SubscriberJourneyStatus.Targeted;
      }
      // Status updated
      else {
        if (! statusConverted)
          return SubscriberJourneyStatus.Notified;
        else {
          if (! statusNotified)
            return SubscriberJourneyStatus.ConvertedNotNotified;
          else
            return SubscriberJourneyStatus.ConvertedNotified;
        }
      }
    }
    // CG
    else {
      if (! statusConverted)
        return SubscriberJourneyStatus.ControlGroup;
      else
        return SubscriberJourneyStatus.ControlGroupConverted; 
    }
  }
  // UCG
  else
  {
    if (! statusConverted)
      return SubscriberJourneyStatus.UniversalControlGroup;
    else
      return SubscriberJourneyStatus.UniversalControlGroupConverted;
  }
}
  
  //
  //  journeyStatistic
  //

  public static SubscriberJourneyStatus getSubscriberJourneyStatus(JourneyStatistic journeyStatistic)
  {
	  if(journeyStatistic.getSpecialExitStatus()!=null && !journeyStatistic.getSpecialExitStatus().equalsIgnoreCase("null") && !journeyStatistic.getSpecialExitStatus().isEmpty())
		  return SubscriberJourneyStatus.fromExternalRepresentation(journeyStatistic.getSpecialExitStatus()); 
				  else	    
					  return getSubscriberJourneyStatus(journeyStatistic.getStatusConverted(), journeyStatistic.getStatusNotified(), journeyStatistic.getStatusTargetGroup(), journeyStatistic.getStatusControlGroup(), journeyStatistic.getStatusUniversalControlGroup());
  }

  //
  //  journeyState
  //

  public static SubscriberJourneyStatus getSubscriberJourneyStatus(JourneyState journeyState)
  {
	  if(journeyState.isSpecialExit())
	   return journeyState.getSpecialExitReason();
	  else {
    boolean statusConverted = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
    boolean statusNotified = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
    Boolean statusTargetGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusTargetGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusTargetGroup.getJourneyParameterName()) : null;
    Boolean statusControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) : null;
    Boolean statusUniversalControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) : null;
    
    return getSubscriberJourneyStatus(statusConverted, statusNotified, statusTargetGroup, statusControlGroup, statusUniversalControlGroup);
  }
  }

  //
  // statusHistory
  //

  public static SubscriberJourneyStatus getSubscriberJourneyStatus(StatusHistory statusHistory)
  {
    return getSubscriberJourneyStatus(statusHistory.getStatusConverted(), statusHistory.getStatusNotified(), statusHistory.getStatusTargetGroup(), statusHistory.getStatusControlGroup(), statusHistory.getStatusUniversalControlGroup());
  }
  
  /*****************************************
  *
  *  generateJourneyResultID
  *
  *****************************************/

  public static String generateJourneyResultID(Journey journey, CriterionField contextVariable)
  {
    switch (journey.getGUIManagedObjectType())
      {
        case Journey:
          return "journey.result." + contextVariable.getID();
        case Campaign:
          return "campaign.result." + contextVariable.getID();
        case Workflow:
          return "workflow.result." + contextVariable.getID();
        case LoyaltyWorkflow:
          return "loyaltyworkflow.result." + contextVariable.getID();
        default:
          return "journey.result." + contextVariable.getID();
      }
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
  *  targetCount
  *
  *****************************************/
  private long evaluateTargetCount(ElasticsearchClientAPI elasticsearch, int tenantID) 
  {
    try
      {
        BoolQueryBuilder query = EvaluationCriterion.esCountMatchCriteriaGetQuery(targetingCriteria);
        return EvaluationCriterion.esCountMatchCriteriaExecuteQuery(query, elasticsearch);
      }
    catch (CriterionException|IOException|ElasticsearchStatusException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("evaluateTargetCount: {}", stackTraceWriter.toString());

        return 0;
      }
  }
  
  //
  // targetCount is not store inside the object, only inside the JSON representation to be used by the GUI
  //
  // Like description, it is not used inside the system, only put at creation and pushed in Elasticsearch
  // mapping_journeys index in order to be visible for the GUI (Grafana).
  //
  public void setTargetCount(ElasticsearchClientAPI elasticsearch, int tenantID)
  {
    if(this.getTargetingType() == TargetingType.Target) {
      this.getJSONRepresentation().put("targetCount", new Long(this.evaluateTargetCount(elasticsearch, tenantID)) );
    }
  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Journey(SchemaAndValue schemaAndValue, Date effectiveEntryPeriodEndDate, Map<String,CriterionField> templateParameters, Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables, TargetingType targetingType, List<EvaluationCriterion> eligibilityCriteria, List<EvaluationCriterion> targetingCriteria, List<EvaluationCriterion> targetingEventCriteria, List<String> targetID, String startNodeID, String endNodeID, Set<JourneyObjectiveInstance> journeyObjectiveInstances, Map<String,JourneyNode> journeyNodes, Map<String,JourneyLink> journeyLinks, ParameterMap boundParameters, boolean appendInclusionLists, boolean appendExclusionLists, boolean appendUCG, JourneyStatus approval, Integer maxNoOfCustomers, boolean fullStatistics, boolean recurrence, String recurrenceId, Integer occurrenceNumber, JourneyScheduler scheduler, Integer lastCreatedOccurrenceNumber, boolean recurrenceActive)
  {
    super(schemaAndValue);
    this.effectiveEntryPeriodEndDate = effectiveEntryPeriodEndDate;
    this.templateParameters = templateParameters;
    this.journeyParameters = journeyParameters;
    this.contextVariables = contextVariables;
    this.targetingType = targetingType;
    this.eligibilityCriteria = eligibilityCriteria;
    this.targetingCriteria = targetingCriteria;
    this.targetingEventCriteria = targetingEventCriteria;
    this.targetID = targetID;
    this.startNodeID = startNodeID;
    this.endNodeID = endNodeID;
    this.journeyObjectiveInstances = journeyObjectiveInstances;
    this.journeyNodes = journeyNodes;
    this.journeyLinks = journeyLinks;
    this.boundParameters = boundParameters;
    this.appendInclusionLists = appendInclusionLists;
    this.appendExclusionLists = appendExclusionLists;
    this.appendUCG = appendUCG;
    this.approval = approval;
    this.maxNoOfCustomers = maxNoOfCustomers;
    this.fullStatistics = fullStatistics;
    this.recurrence = recurrence;
    this.recurrenceId = recurrenceId;
    this.occurrenceNumber = occurrenceNumber;
    this.journeyScheduler = scheduler;
    this.lastCreatedOccurrenceNumber = lastCreatedOccurrenceNumber;
    this.recurrenceActive = recurrenceActive;
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
    struct.put("templateParameters", packJourneyParameters(journey.getTemplateParameters()));
    struct.put("journeyParameters", packJourneyParameters(journey.getJourneyParameters()));
    struct.put("contextVariables", packContextVariables(journey.getContextVariables()));
    struct.put("targetingType", journey.getTargetingType().getExternalRepresentation());
    struct.put("eligibilityCriteria", packCriteria(journey.getEligibilityCriteria()));
    struct.put("targetingCriteria", packCriteria(journey.getTargetingCriteria()));
    struct.put("targetingEventCriteria", packCriteria(journey.getTargetingEventCriteria()));
    struct.put("targetID", journey.getTargetID());
    struct.put("startNodeID", journey.getStartNodeID());
    struct.put("endNodeID", journey.getEndNodeID());
    struct.put("journeyObjectives", packJourneyObjectiveInstances(journey.getJourneyObjectiveInstances()));
    struct.put("journeyNodes", packJourneyNodes(journey.getJourneyNodes()));
    struct.put("journeyLinks", packJourneyLinks(journey.getJourneyLinks()));
    struct.put("boundParameters", ParameterMap.pack(journey.getBoundParameters()));
    struct.put("appendInclusionLists", journey.getAppendInclusionLists());
    struct.put("appendExclusionLists", journey.getAppendExclusionLists());
    struct.put("appendUCG", journey.getAppendUCG());
    struct.put("approval", journey.getApproval().getExternalRepresentation());
    struct.put("maxNoOfCustomers", journey.getMaxNoOfCustomers());
    struct.put("fullStatistics", journey.getFullStatistics());
    struct.put("recurrence", journey.getRecurrence());
    struct.put("recurrenceId", journey.getRecurrenceId());
    struct.put("occurrenceNumber", journey.getOccurrenceNumber());
    struct.put("scheduler", JourneyScheduler.serde().packOptional(journey.getJourneyScheduler()));
    struct.put("lastCreatedOccurrenceNumber", journey.getLastCreatedOccurrenceNumber());
    struct.put("recurrenceActive", journey.getRecurrenceActive());
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
    Map<String,CriterionField> templateParameters = (schemaVersion >= 4) ? unpackJourneyParameters(schema.field("journeyParameters").schema(), (Map<String,Object>) valueStruct.get("journeyParameters")) : new HashMap<String,CriterionField>();
    Map<String,CriterionField> journeyParameters = unpackJourneyParameters(schema.field("journeyParameters").schema(), (Map<String,Object>) valueStruct.get("journeyParameters"));
    Map<String,CriterionField> contextVariables = unpackContextVariables(schema.field("contextVariables").schema(), (Map<String,Object>) valueStruct.get("contextVariables"));
    TargetingType targetingType = TargetingType.fromExternalRepresentation(valueStruct.getString("targetingType"));
    List<EvaluationCriterion> eligibilityCriteria = unpackCriteria(schema.field("eligibilityCriteria").schema(), valueStruct.get("eligibilityCriteria"));
    List<EvaluationCriterion> targetingCriteria = unpackCriteria(schema.field("targetingCriteria").schema(), valueStruct.get("targetingCriteria"));
    List<EvaluationCriterion> targetingEventCriteria = (schemaVersion >=8) ? unpackCriteria(schema.field("targetingEventCriteria").schema(), valueStruct.get("targetingEventCriteria")) : new ArrayList<EvaluationCriterion>();
    List<String> targetID = (List<String>) valueStruct.get("targetID");
    String startNodeID = valueStruct.getString("startNodeID");
    String endNodeID = valueStruct.getString("endNodeID");
    Set<JourneyObjectiveInstance> journeyObjectiveInstances = unpackJourneyObjectiveInstances(schema.field("journeyObjectives").schema(), valueStruct.get("journeyObjectives"));
    Map<String,JourneyNode> journeyNodes = unpackJourneyNodes(schema.field("journeyNodes").schema(), valueStruct.get("journeyNodes"));
    Map<String,JourneyLink> journeyLinks = unpackJourneyLinks(schema.field("journeyLinks").schema(), valueStruct.get("journeyLinks"));
    ParameterMap boundParameters = (schemaVersion >= 2) ? ParameterMap.unpack(new SchemaAndValue(schema.field("boundParameters").schema(), valueStruct.get("boundParameters"))) : new ParameterMap();
    boolean appendInclusionLists = (schemaVersion >= 3) ? valueStruct.getBoolean("appendInclusionLists") : false;
    boolean appendExclusionLists = (schemaVersion >= 3) ? valueStruct.getBoolean("appendExclusionLists") : false;
    boolean appendUCG = (schema.field("appendUCG") != null) ? valueStruct.getBoolean("appendUCG") : false;
    JourneyStatus approval = (schemaVersion >= 5) ? JourneyStatus.fromExternalRepresentation(valueStruct.getString("approval")) : JourneyStatus.Pending;
    Integer maxNoOfCustomers = (schemaVersion >=6) ? valueStruct.getInt32("maxNoOfCustomers") : null;
    boolean fullStatistics = (schema.field("fullStatistics") != null) ? valueStruct.getBoolean("fullStatistics") : false;

    boolean recurrence = (schema.field("recurrence") != null) ? valueStruct.getBoolean("recurrence") : false;
    String recurrenceId = (schema.field("recurrenceId") != null) ? valueStruct.getString("recurrenceId") : null;
    Integer occurrenceNumber = (schema.field("occurrenceNumber") != null) ? valueStruct.getInt32("occurrenceNumber") : null;
    JourneyScheduler scheduler = (schema.field("scheduler")!= null) ? JourneyScheduler.serde().unpackOptional(new SchemaAndValue(schema.field("scheduler").schema(),valueStruct.get("scheduler"))) : null;
    Integer lastCreatedOccurrenceNumber = (schema.field("lastCreatedOccurrenceNumber")!= null) ? valueStruct.getInt32("lastCreatedOccurrenceNumber") : null;
    boolean recurrenceActive = (schema.field("recurrenceActive") != null) ? valueStruct.getBoolean("recurrenceActive") : false;
    
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

    return new Journey(schemaAndValue, effectiveEntryPeriodEndDate, templateParameters, journeyParameters, contextVariables, targetingType, eligibilityCriteria, targetingCriteria, targetingEventCriteria, targetID, startNodeID, endNodeID, journeyObjectiveInstances, journeyNodes, journeyLinks, boundParameters, appendInclusionLists, appendExclusionLists, appendUCG, approval, maxNoOfCustomers, fullStatistics, recurrence, recurrenceId, occurrenceNumber, scheduler, lastCreatedOccurrenceNumber, recurrenceActive);
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
  *  constructor -- JSON and Approval
  *
  *****************************************/
  
  public Journey(JSONObject jsonRoot, GUIManagedObjectType journeyType, long epoch, GUIManagedObject existingJourneyUnchecked, JourneyService journeyService, CatalogCharacteristicService catalogCharacteristicService, SubscriberMessageTemplateService subscriberMessageTemplateService, DynamicEventDeclarationsService dynamicEventDeclarationsService, JourneyTemplateService journeyTemplateService, int tenantID) throws GUIManagerException
  {
	  this(jsonRoot, journeyType, epoch, existingJourneyUnchecked, journeyService, catalogCharacteristicService, subscriberMessageTemplateService, dynamicEventDeclarationsService, journeyTemplateService, JourneyStatus.Pending, tenantID);
  }
  
  /*****************************************
  *
  *  constructor -- JSON and Approval
  *
  *****************************************/
  
  public Journey(JSONObject jsonRoot, GUIManagedObjectType journeyType, long epoch, GUIManagedObject existingJourneyUnchecked, JourneyService journeyService, CatalogCharacteristicService catalogCharacteristicService, SubscriberMessageTemplateService subscriberMessageTemplateService, DynamicEventDeclarationsService dynamicEventDeclarationsService, JourneyTemplateService journeyTemplateService, JourneyStatus approval, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, journeyType, (existingJourneyUnchecked != null) ? existingJourneyUnchecked.getEpoch() : epoch, tenantID);

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
    this.templateParameters = decodeJourneyParameters(JSONUtilities.decodeJSONArray(jsonRoot, "templateParameters", false));
    this.targetingType = TargetingType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingType", "criteria"));
    this.eligibilityCriteria = decodeCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "eligibilityCriteria", false), new ArrayList<EvaluationCriterion>(), CriterionContext.DynamicProfile(tenantID), tenantID);
    this.targetingCriteria = decodeCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "targetingCriteria", false), new ArrayList<EvaluationCriterion>(), CriterionContext.DynamicProfile(tenantID), tenantID);
    
    //
    // Targeting Event Criterion mgt
    //
    String targetingEvent = JSONUtilities.decodeString(jsonRoot, "targetingEvent", false);
    if(targetingEvent != null)
      {
        JSONArray arrayEventNameCriterion = new JSONArray();
        JSONObject eventNameCriterionJson = new JSONObject();
        eventNameCriterionJson.put("criterionField", "evaluation.eventname");
        eventNameCriterionJson.put("criterionOperator", "==");
        JSONObject argumentJson = new JSONObject();
        argumentJson.put("expression", "'" + targetingEvent + "'");
        eventNameCriterionJson.put("argument", argumentJson);
        arrayEventNameCriterion.add(eventNameCriterionJson);
        EvolutionEngineEventDeclaration event = dynamicEventDeclarationsService.getStaticAndDynamicEvolutionEventDeclarations().get(targetingEvent);
        CriterionContext criterionContext = new CriterionContext(new HashMap<String,CriterionField>(), new HashMap<String,CriterionField>(), null, event, null, null, tenantID);
        List<EvaluationCriterion> eventNameCriteria = decodeCriteria(arrayEventNameCriterion, new ArrayList<EvaluationCriterion>(), criterionContext, tenantID);        
        this.targetingEventCriteria = decodeCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "targetingEventCriteria", false), eventNameCriteria, criterionContext, tenantID);
      }
    else 
      {
        this.targetingEventCriteria = new ArrayList<>();
      }

    this.targetID = decodeTargetIDs(JSONUtilities.decodeJSONArray(jsonRoot, "targetID", new JSONArray()));
    this.journeyObjectiveInstances = decodeJourneyObjectiveInstances(JSONUtilities.decodeJSONArray(jsonRoot, "journeyObjectives", false), catalogCharacteristicService);
    this.appendInclusionLists = JSONUtilities.decodeBoolean(jsonRoot, "appendInclusionLists", Boolean.FALSE);
    this.appendExclusionLists = JSONUtilities.decodeBoolean(jsonRoot, "appendExclusionLists", Boolean.FALSE);
    this.appendUCG = JSONUtilities.decodeBoolean(jsonRoot, "appendUCG", Boolean.FALSE);
    Map<String,GUINode> contextVariableNodes = decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot, "nodes", true), this.templateParameters, Collections.<String,CriterionField>emptyMap(), true, journeyService, subscriberMessageTemplateService, dynamicEventDeclarationsService, tenantID);
    List<GUILink> jsonLinks = decodeLinks(JSONUtilities.decodeJSONArray(jsonRoot, "links", true));
    this.approval = approval;
    // by now GUI send String, to ask change to be cleaner
    //this.maxNoOfCustomers = JSONUtilities.decodeInteger(jsonRoot,"maxNoOfCustomers",null);
    this.maxNoOfCustomers=null;
    String maxNoOfCustomersString = JSONUtilities.decodeString(jsonRoot,"maxNoOfCustomers",null);
    if(maxNoOfCustomersString!=null){
      try{
        this.maxNoOfCustomers=Integer.parseInt(maxNoOfCustomersString);
      }catch (NumberFormatException ex){
        throw new GUIManagerException("maxNoOfCustomers has bad field value",maxNoOfCustomersString);
      }
    }
    this.fullStatistics = JSONUtilities.decodeBoolean(jsonRoot, "fullStatistics", Boolean.FALSE);

    //
    //  recurrence
    //

    this.recurrence = JSONUtilities.decodeBoolean(jsonRoot, "recurrence", Boolean.FALSE);
    this.recurrenceId = JSONUtilities.decodeString(jsonRoot, "recurrenceId", recurrence);
    this.occurrenceNumber = JSONUtilities.decodeInteger(jsonRoot, "occurrenceNumber", recurrence);
    if (recurrence) this.journeyScheduler = new JourneyScheduler(JSONUtilities.decodeJSONObject(jsonRoot, "scheduler", recurrence));
    this.lastCreatedOccurrenceNumber = JSONUtilities.decodeInteger(jsonRoot, "lastCreatedOccurrenceNumber", recurrence);
    this.recurrenceActive = JSONUtilities.decodeBoolean(jsonRoot, "recurrenceActive", Boolean.FALSE);


    /*****************************************
    *
    *  contextVariables
    *
    *****************************************/

    Map<String,CriterionField> contextVariablesAndParameters = Journey.processContextVariableNodes(contextVariableNodes, templateParameters, tenantID);
    this.contextVariables = new HashMap<String,CriterionField>();
    this.journeyParameters = new LinkedHashMap<String,CriterionField>(this.templateParameters);
    for (CriterionField contextVariable : contextVariablesAndParameters.values())
      {
        switch (contextVariable.getVariableType())
          {
            case Local:
            case JourneyResult:
              this.contextVariables.put(contextVariable.getID(), contextVariable);
              break;
            case Parameter:
              this.journeyParameters.put(contextVariable.getID(), contextVariable);
              break;
          }
      }

    /*****************************************
    *
    *  boundParameters
    *
    *****************************************/

    this.boundParameters = decodeBoundParameters(JSONUtilities.decodeJSONArray(jsonRoot, "boundParameters", new JSONArray()), JSONUtilities.decodeString(jsonRoot,  "journeyTemplateID"), this.journeyParameters, this.contextVariables, journeyService, subscriberMessageTemplateService, journeyTemplateService, tenantID);

    /*****************************************
    *
    *  jsonNodes
    *
    *****************************************/

    Map<String,GUINode> jsonNodes = decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot, "nodes", true), this.journeyParameters, contextVariables, false, journeyService, subscriberMessageTemplateService, dynamicEventDeclarationsService, tenantID);
    
    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if(this.maxNoOfCustomers!=null && this.maxNoOfCustomers<0) throw new GUIManagerException("maxNoOfCustomers has bad field value", this.maxNoOfCustomers+"");

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
                if (this.journeyParameters.size() > 0) throw new GUIManagerException("autoTargeted Journey may not have parameters", this.getJourneyID());
                break;

              case BulkCampaign:
                if (this.journeyParameters.size() > 0 && this.journeyParameters.size() != this.boundParameters.size())
                  {
                    if (log.isTraceEnabled())
                      {
                        log.trace("journeyParameters : " + journeyParameters.size() + " values :");
                        for (CriterionField p1 : this.journeyParameters.values())
                          {
                            log.trace("  " + p1.getID()+ " " + p1.getDisplay() + " " + p1.toString());
                          }
                        log.trace("boundParameters : " + boundParameters.size() + " values :");
                        for (Object p2 : this.boundParameters.values())
                          {
                            log.trace("  " + p2.getClass().getCanonicalName() + " : " + p2.toString());
                          }
                      }
                    throw new GUIManagerException("autoTargeted Journey may not have parameters", this.getJourneyID());
                  }
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

    //
    //  workflows
    //

    switch (journeyType)
      {
        case Workflow:

          //
          //  only "manual" targeting
          //

          switch (this.targetingType)
            {
              case Manual:
                break;

              default:
                throw new GUIManagerException("workflow must have manual targeting", this.getJourneyID());
            }

          //
          //  no eligibility criteria
          //

          if (this.eligibilityCriteria.size() > 0) throw new GUIManagerException("workflow may not have eligibility criteria", this.getJourneyID());
          
          //
          //  no targeting criteria
          //

          if (this.targetingCriteria.size() > 0) throw new GUIManagerException("workflow may not have targeting criteria", this.getJourneyID());
          
          //
          //  no targeting event criteria
          //

          if (this.targetingEventCriteria.size() > 0) throw new GUIManagerException("workflow may not have targeting event criteria", this.getJourneyID());


          //
          //  no start/end dates
          //

          if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
          if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));
          if (getRawEffectiveEntryPeriodEndDate() != null) throw new GUIManagerException("unsupported entry period end date", JSONUtilities.decodeString(jsonRoot, "effectiveEntryPeriodEndDate", false));

          //
          //  no journey objectives
          //

          if (this.journeyObjectiveInstances.size() > 0) throw new GUIManagerException("workflow may not have objectives", this.getJourneyID());

          //
          //  break
          //

          break;
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
    *  add journeyParameters to the jsonRepresentation
    *
    ****************************************/

    List<JSONObject> journeyParametersJSON = new ArrayList<JSONObject>();
    for (CriterionField journeyParameter : journeyParameters.values())
      {
        journeyParametersJSON.add(journeyParameter.getJSONRepresentation());
      }
    this.getJSONRepresentation().put("journeyParameters", JSONUtilities.encodeArray(journeyParametersJSON));

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
  
  public void createOrConsolidateHardcodedMessageTemplates(SubscriberMessageTemplateService subscriberMessageTemplateService, String journeyID, JourneyService journeyService) throws GUIManagerException
  {
    
    GUIManagedObject gmo = journeyService.getStoredJourney(journeyID);
    Journey existingJourney = null;
    if (gmo instanceof Journey)
      {
        existingJourney = (Journey) gmo;    
      }
    
    /*****************************************
    *
    *  resolve hard-coded subscriber messages
    *
    *****************************************/

    Set<Pair<SubscriberMessage, String>> hardcodedSubscriberMessages = retrieveHardcodedSubscriberMessages(this);
    Set<Pair<SubscriberMessage, String>> existingHardcodedSubscriberMessages = (existingJourney != null) ? retrieveHardcodedSubscriberMessages(existingJourney) : new HashSet<Pair<SubscriberMessage, String>>();
    for (Pair<SubscriberMessage, String> subscriberMessagePair : hardcodedSubscriberMessages)
      {
        SubscriberMessage subscriberMessage = subscriberMessagePair.getFirstElement();
        
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
        for (Pair<SubscriberMessage, String> existingSubscriberMessagePair : existingHardcodedSubscriberMessages)
          {
            SubscriberMessage existingSubscriberMessage = existingSubscriberMessagePair.getFirstElement();
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
            SubscriberMessageTemplate internalSubscriberMessageTemplate = SubscriberMessageTemplate.newInternalTemplate(subscriberMessagePair.getSecondElement(), subscriberMessage, subscriberMessageTemplateService);
            subscriberMessage.setSubscriberMessageTemplateID(internalSubscriberMessageTemplate.getSubscriberMessageTemplateID());
            subscriberMessageTemplateService.putSubscriberMessageTemplate(internalSubscriberMessageTemplate, true, null);
          }
        else
          {
            subscriberMessage.setSubscriberMessageTemplateID(matchingSubscriberMessage.getSubscriberMessageTemplateID());
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

  private List<EvaluationCriterion> decodeCriteria(JSONArray jsonArray, List<EvaluationCriterion> additionalCriteria, CriterionContext context, int tenantID) throws GUIManagerException
  {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();

    //
    //  universal criteria
    //

    result.addAll(additionalCriteria);

    //
    //  journey-level targeting critera
    //

    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), context, tenantID));
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

  public static Map<String,GUINode> decodeNodes(JSONArray jsonArray, Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables, boolean contextVariableProcessing, JourneyService journeyService, SubscriberMessageTemplateService subscriberMessageTemplateService, DynamicEventDeclarationsService dynamicEventDeclarationsService, int tenantID) throws GUIManagerException
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
            GUINode node = new GUINode(nodeJSON, journeyParameters, contextVariables, contextVariableProcessing, journeyService, subscriberMessageTemplateService, dynamicEventDeclarationsService, tenantID);

            //
            //  validate (if required)
            //

            if (! contextVariableProcessing)
              {
                for (ContextVariable contextVariable : node.getContextVariables())
                  {
                    contextVariable.validate(node.getNodeOnlyCriterionContext(), node.getNodeWithJourneyResultCriterionContext(), tenantID);
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

  private ParameterMap decodeBoundParameters(JSONArray jsonArray, String journeyTemplateID, Map<String,CriterionField> journeyParameters, Map<String, CriterionField> contextVariables, JourneyService journeyService, SubscriberMessageTemplateService subscriberMessageTemplateService, JourneyTemplateService journeyTemplateService, int tenantID) throws GUIManagerException
  {
    CriterionContext criterionContext = new CriterionContext(journeyParameters, contextVariables, this.getTenantID());
    ParameterMap boundParameters = new ParameterMap();
    for (int i = 0; i < jsonArray.size(); i++)
      {
        JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
        String parameterName = JSONUtilities.decodeString(parameterJSON, "parameterName", true);
        CriterionField parameter = journeyParameters.get(parameterName);
        if (parameter == null) throw new GUIManagerException("unknown parameter", parameterName);
        if (!isExpressionValuedParameterValue(parameterJSON))
          {
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

              case AniversaryCriterion:
              case DateCriterion:
                boundParameters.put(parameterName, GUIManagedObject.parseDateField(JSONUtilities.decodeString(parameterJSON, "value", false)));
                break;
                
              case TimeCriterion:
                boundParameters.put(parameterName, JSONUtilities.decodeString(parameterJSON, "value", false));
                break;

              case StringSetCriterion:
                Set<String> stringSetValue = new HashSet<String>();
                JSONArray stringSetArray = JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray());
                for (int j = 0; j < stringSetArray.size(); j++)
                  {
                    stringSetValue.add((String) stringSetArray.get(j));
                  }
                boundParameters.put(parameterName, stringSetValue);
                break;

              case EvaluationCriteriaParameter:
                List<EvaluationCriterion> evaluationCriteriaValue = new ArrayList<EvaluationCriterion>();
                JSONArray evaluationCriteriaArray = JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray());
                for (int j = 0; j < evaluationCriteriaArray.size(); j++)
                  {
                    evaluationCriteriaValue.add(new EvaluationCriterion((JSONObject) evaluationCriteriaArray.get(j), criterionContext, tenantID));
                  }
                boundParameters.put(parameterName, evaluationCriteriaValue);
                break;

              case SMSMessageParameter:
                SMSMessage smsMessageValue = new SMSMessage(parameterJSON.get("value"), null, subscriberMessageTemplateService, criterionContext);
                boundParameters.put(parameterName, smsMessageValue);
                break;

              case EmailMessageParameter:
                EmailMessage emailMessageValue = new EmailMessage(parameterJSON.get("value"), null, subscriberMessageTemplateService, criterionContext);
                boundParameters.put(parameterName, emailMessageValue);
                break;

              case PushMessageParameter:
                PushMessage pushMessageValue = new PushMessage(parameterJSON.get("value"), null, subscriberMessageTemplateService, criterionContext);
                boundParameters.put(parameterName, pushMessageValue);
                break;
              
              case Dialog:
                JSONObject value = JSONUtilities.decodeJSONObject(parameterJSON, "value", false); //(JSONObject)parameterJSON.get("value");
                if (value != null)
                  {
                    HashMap<String,Boolean> dialogMessageFieldsMandatory = new HashMap<String, Boolean>();
                    Journey journeyTemplate = journeyTemplateService.getActiveJourneyTemplate(journeyTemplateID, SystemTime.getCurrentTime());
                    // this is a f**** hack to retrieve the communication channel ID
                    JSONObject templateJSON = journeyTemplate.getJSONRepresentation();
                    JSONArray templateParametersJSON = JSONUtilities.decodeJSONArray(templateJSON, "templateParameters");
                    String communcationChannelID = null;
                    if(templateParametersJSON != null) {
                      for(int j = 0; j < templateParametersJSON.size(); j++)
                        {
                          JSONObject templateParameterJSON = (JSONObject) templateParametersJSON.get(j);
                          if(parameterName.equals(JSONUtilities.decodeString(templateParameterJSON, "id")))
                            {
                              communcationChannelID = JSONUtilities.decodeString(templateParameterJSON, "communicationChannelID");
                            }
                        }
                    }
                    if(communcationChannelID == null) { throw new GUIManagerException("Can't retrieve communication channel ID", parameterName); }
                    CommunicationChannel channel = Deployment.getCommunicationChannels().get(communcationChannelID);
                    for(CriterionField param : channel.getParameters().values()) {
                      if(param.getFieldDataType().getExternalRepresentation().startsWith("template_")) {
                        dialogMessageFieldsMandatory.put(param.getID(), param.getMandatoryParameter());
                      }
                    }

                    JSONArray message = JSONUtilities.decodeJSONArray(value, "message");
                    /*
                    
                      "value": {
                          "message": [
                            {
                              "languageID": "1",
                              "sms.body": "Bienvenue sur le reseau"
                            },{
                              "languageID": "2",
                              "sms.body": null
                            }
                          ]
                      }
                      
                      or
                      
                       "value": {
                          "templateID": "1",
                          "macros": [
                              {
                                "templateValue": "tag.x",
                                "campaignValue": "subscriber.arpu"
                              },{
                                "templateValue": "tag.y",
                                "campaignValue": "bulkcampaign.customer.status"
                              }
                          ]
                       }
                    */

                    if(message != null) {                
                      // case InLine Template
                      NotificationTemplateParameters templateParameters = new NotificationTemplateParameters(message, communcationChannelID, dialogMessageFieldsMandatory, subscriberMessageTemplateService, criterionContext);
                      boundParameters.put(parameterName, templateParameters);
                    }
                    else {
                      // case referenced Template
                      NotificationTemplateParameters templateParameters = new NotificationTemplateParameters(value, communcationChannelID, dialogMessageFieldsMandatory, subscriberMessageTemplateService, criterionContext);
                      boundParameters.put(parameterName, templateParameters);
                    }
                  }
                else
                  {
                    log.trace("parameter does not have a value : " + parameterJSON.toJSONString());
                  }
                break;
                
              case WorkflowParameter:
                WorkflowParameter workflowParameterValue = new WorkflowParameter((JSONObject) parameterJSON.get("value"), journeyService, criterionContext, tenantID);
                boundParameters.put(parameterName, workflowParameterValue);
                break;
              }
          }
        else
          {
            /*****************************************
             *
             * expression
             *
             *****************************************/

            //
            // parse
            //

            ParameterExpression parameterExpressionValue = new ParameterExpression(JSONUtilities.decodeJSONObject(parameterJSON, "value", true), criterionContext, tenantID);
            boundParameters.put(parameterName, parameterExpressionValue);

            //
            // valid combination
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

              case AniversaryCriterion:
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
                
              case TimeCriterion:
                switch (parameterExpressionValue.getType())
                  {
                  case TimeExpression:
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
              case NotificationStringParameter:
              case NotificationHTMLStringParameter:
              case Dialog:
              case WorkflowParameter:
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
            // validate
            //

            if (!validCombination) throw new GUIManagerException("dataType/expression combination", parameter.getFieldDataType().getExternalRepresentation() + "/" + parameterExpressionValue.getType());
          }
      }

    return boundParameters;
  }

  /*****************************************
  *
  *  retrieve hard-coded subscriber messages (i.e., that do NOT directly reference a template)
  *
  *****************************************/

  private static Set<Pair<SubscriberMessage, String>> /*Element / ChannelId*/retrieveHardcodedSubscriberMessages(Journey journey)
  {
    
    // TODO THIS CODE IS SHITTY: Has to be refactored when hardcoded SMS MAIL and PUSH is removed from the system 
    /*****************************************
    *
    *  node/link parameters
    *
    *****************************************/

    Set<Pair<SubscriberMessage, String>> result = new HashSet();
    for (JourneyNode journeyNode : journey.getJourneyNodes().values())
      {
        //
        //  node parameters
        //
        String channelID = "NotGeneric"; // Generic Channel ID of this parameter 
        for (Object parameterValue : journeyNode.getNodeParameters().values())          {
            
            if (parameterValue instanceof SubscriberMessage)
              {
                NodeType nodeType = journeyNode.getNodeType();
                channelID = JSONUtilities.decodeString(nodeType.getJSONRepresentation(), "communicationChannelID", false); // false because old channels don't provide this information
                // This is for the management of Old channels.... hardcoded SMS, EMAIL and so on...
                SubscriberMessage subscriberMessage = (SubscriberMessage) parameterValue;
                if (subscriberMessage.getDialogMessages().size() > 0)
                  {
                    result.add(new Pair(subscriberMessage, channelID != null ? channelID : "UnknownChannelID"));
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
                    NodeType nodeType = journeyNode.getNodeType();
                    channelID = JSONUtilities.decodeString(nodeType.getJSONRepresentation(), "communicationChannelID", true);
                    SubscriberMessage subscriberMessage = (SubscriberMessage) parameterValue;
                    if (subscriberMessage.getDialogMessages().size() > 0)
                      {
                        result.add(new Pair(subscriberMessage, channelID != null ? channelID : "UnknownChannelID"));
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
                result.add(new Pair(subscriberMessage, subscriberMessage.getCommunicationChannelID()));
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

  public static Map<String, CriterionField> processContextVariableNodes(Map<String,GUINode> contextVariableNodes, Map<String,CriterionField> journeyParameters, int tenantID) throws GUIManagerException
  {
    return processContextVariableNodes(contextVariableNodes, journeyParameters, null, tenantID);
  }

  public static Map<String, CriterionField> processContextVariableNodes(Map<String,GUINode> contextVariableNodes, Map<String,CriterionField> journeyParameters, CriterionDataType expectedDataType, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  preparation
    *
    *****************************************/

    Map<ContextVariable,Pair<CriterionContext,CriterionContext>> contextVariables = new IdentityHashMap<ContextVariable,Pair<CriterionContext,CriterionContext>>();
    for (GUINode guiNode : contextVariableNodes.values())
      {
        for (ContextVariable contextVariable : guiNode.getContextVariables())
          {
            if (expectedDataType == null || (contextVariable.getType().equals(expectedDataType)))
              {
                contextVariables.put(contextVariable, new Pair<CriterionContext,CriterionContext>(guiNode.getNodeOnlyCriterionContext(), guiNode.getNodeWithJourneyResultCriterionContext()));
              }
          }
      }


    /*****************************************
    *
    *  pre-validate parameters (to set correct type)
    *
    *****************************************/
        
    Map<String,CriterionField> contextVariableFields = new HashMap<String,CriterionField>();
    Set<ContextVariable> unvalidatedContextVariables = new HashSet<ContextVariable>();
    for (ContextVariable contextVariable : contextVariables.keySet())
      {
        switch (contextVariable.getVariableType())
          {
            case Parameter:
              CriterionContext nodeOnlyWorkingCriterionContext = new CriterionContext(contextVariables.get(contextVariable).getFirstElement(), contextVariableFields, tenantID);
              CriterionContext nodeWithJourneyResultWorkingCriterionContext = new CriterionContext(contextVariables.get(contextVariable).getSecondElement(), contextVariableFields, tenantID);
              contextVariable.validate(nodeOnlyWorkingCriterionContext, nodeWithJourneyResultWorkingCriterionContext, tenantID);
              CriterionField criterionField = new CriterionField(contextVariable);
              contextVariableFields.put(criterionField.getID(), criterionField);
              break;

            default:
              unvalidatedContextVariables.add(contextVariable);
              break;
          }
      }

    /*****************************************
    *
    *  process
    *
    *****************************************/

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

                CriterionContext nodeOnlyWorkingCriterionContext = new CriterionContext(contextVariables.get(contextVariable).getFirstElement(), contextVariableFields, tenantID);
                CriterionContext nodeWithJourneyResultWorkingCriterionContext = new CriterionContext(contextVariables.get(contextVariable).getSecondElement(), contextVariableFields, tenantID);

                //
                //  validate
                //

                contextVariable.validate(nodeOnlyWorkingCriterionContext, nodeWithJourneyResultWorkingCriterionContext, tenantID);

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
                    case AniversaryCriterion:
                    case StringSetCriterion:
                      if (contextVariableFields.get(criterionField.getID()).getFieldDataType() != criterionField.getFieldDataType())
                        {
                          throw new GUIManagerException("inconsistent data types", criterionField.getID());
                        }
                      break;

                    case TimeCriterion:
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
        StringBuilder buffer = new StringBuilder();
        unvalidatedContextVariables.iterator().forEachRemaining(var -> buffer.append(var.getID()+" "));
        throw new GUIManagerException("unvalidatedContextVariables "+buffer, Integer.toString(unvalidatedContextVariables.size()));
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
    private CriterionContext nodeOnlyCriterionContext;
    private CriterionContext nodeWithJourneyResultCriterionContext;

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
    public CriterionContext getNodeOnlyCriterionContext() { return nodeOnlyCriterionContext; }
    public CriterionContext getNodeWithJourneyResultCriterionContext() { return nodeWithJourneyResultCriterionContext; }


    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public GUINode(JSONObject jsonRoot, Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables, boolean contextVariableProcessing, JourneyService journeyService, SubscriberMessageTemplateService subscriberMessageTemplateService, DynamicEventDeclarationsService dynamicEventDeclarationsService, int tenantID) throws GUIManagerException
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
      //  nodeParameters (independent, i.e., not EvaluationCriteria or messages)
      //

      this.nodeParameters = decodeIndependentNodeParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", new JSONArray()), nodeType);

      //
      //  eventName
      //

      String eventName = this.nodeParameters.containsKey("node.parameter.eventname") ? (String) this.nodeParameters.get("node.parameter.eventname") : null;
      EvolutionEngineEventDeclaration nodeEvent = (eventName != null) ? dynamicEventDeclarationsService.getStaticAndDynamicEvolutionEventDeclarations().get(eventName) : null;
      if (eventName != null && nodeEvent == null) throw new GUIManagerException("unknown event", eventName);

      //
      //  selectedjourney
      //

      Journey workflow = decodeDependentWorkflow(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", new JSONArray()), nodeType, journeyService, tenantID);

      //
      //  criterionContext
      //

      this.nodeOnlyCriterionContext = new CriterionContext(journeyParameters, contextVariables, this.nodeType, nodeEvent, (Journey) null, tenantID);
      this.nodeWithJourneyResultCriterionContext = new CriterionContext(journeyParameters, contextVariables, this.nodeType, nodeEvent, workflow, tenantID);

      //
      //  contextVariables
      //

      this.contextVariables = nodeType.getAllowContextVariables() ? decodeContextVariables(JSONUtilities.decodeJSONArray(jsonRoot, "contextVariables", false), tenantID) : Collections.<ContextVariable>emptyList();

      // add a special internal variables to hold partner
      
      if ("121".equals(nodeType.getID()) && "offerDelivery".equals(eventName)) // event.selection nodetype (defined in src/main/resources/config/deployment-product-toolbox.json)
        {
          this.contextVariables.add(new ContextVariable(buildContextVariableJSON(EvolutionEngine.INTERNAL_VARIABLE_SUPPLIER, "event.supplierName"), tenantID));
          this.contextVariables.add(new ContextVariable(buildContextVariableJSON(EvolutionEngine.INTERNAL_VARIABLE_RESELLER, "event.resellerName"), tenantID));
        }
      
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

          this.nodeParameters.putAll(decodeDependentNodeParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", new JSONArray()), nodeType, nodeOnlyCriterionContext, journeyService, subscriberMessageTemplateService, tenantID));
          this.nodeParameters.putAll(decodeExpressionValuedParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", new JSONArray()), nodeType, nodeOnlyCriterionContext, tenantID));

          //
          //  outputConnectors
          //

          this.outgoingConnectionPoints = decodeOutgoingConnectionPoints(JSONUtilities.decodeJSONArray(jsonRoot, "outputConnectors", true), nodeType, nodeOnlyCriterionContext, journeyService, subscriberMessageTemplateService, dynamicEventDeclarationsService, tenantID);
        }
    }
    
    public JSONObject buildContextVariableJSON(String internalVariableName, String eventField)
    {
      JSONObject contextVariableJSON;
      JSONObject valueJSON = new JSONObject();
      valueJSON.put("expression", eventField);
      valueJSON.put("value", eventField);
      valueJSON.put("expressionType", EvaluationCriterion.CriterionDataType.StringCriterion.getExternalRepresentation());
      valueJSON.put("assignment", ContextVariable.Assignment.Direct.getExternalRepresentation());
      valueJSON.put("valueType", "complex"); // not sure this is required

      contextVariableJSON = new JSONObject();
      contextVariableJSON.put("name", internalVariableName);
      contextVariableJSON.put("value", valueJSON);
      return contextVariableJSON;
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
              case AniversaryCriterion:
                nodeParameters.put(parameterName, GUIManagedObject.parseDateField(JSONUtilities.decodeString(parameterJSON, "value", false)));
                break;
                
              case TimeCriterion:
                nodeParameters.put(parameterName, JSONUtilities.decodeString(parameterJSON, "value", false));
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
    *  decodeDependentWorkflow
    *
    *****************************************/

    private Journey decodeDependentWorkflow(JSONArray jsonArray, NodeType nodeType, JourneyService journeyService, int tenantID) throws GUIManagerException
    {
      Journey workflow = null;
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

          /*****************************************
          *
          *  constant
          *
          *****************************************/

          switch (parameter.getFieldDataType())
            {
              case WorkflowParameter:

                if((JSONObject) parameterJSON.get("value") != null)
                  {
                    JSONObject workflowJSON = (JSONObject) parameterJSON.get("value");
                    if (workflowJSON == null) throw new GUIManagerException("no workflow", "null");
                    String workflowID = JSONUtilities.decodeString(workflowJSON, "workflowID", true);
                    workflow = journeyService.getActiveJourney(workflowID, SystemTime.getCurrentTime());                    
                    if (workflow == null) throw new GUIManagerException("unknown workflow", workflowID);
                  }
                break;
            }
        }
      return workflow;
    }

    /*****************************************
    *
    *  decodeDependentNodeParameters
    *
    *****************************************/

    private ParameterMap decodeDependentNodeParameters(JSONArray jsonArray, NodeType nodeType, CriterionContext criterionContext, JourneyService journeyService, SubscriberMessageTemplateService subscriberMessageTemplateService, int tenantID) throws GUIManagerException
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
                    evaluationCriteriaValue.add(new EvaluationCriterion((JSONObject) evaluationCriteriaArray.get(j), criterionContext, tenantID));
                  }
                nodeParameters.put(parameterName, evaluationCriteriaValue);
                break;

              case SMSMessageParameter:
                SMSMessage smsMessageValue = new SMSMessage(parameterJSON.get("value"), null, subscriberMessageTemplateService, criterionContext);
                nodeParameters.put(parameterName, smsMessageValue);
                break;

              case EmailMessageParameter:
                EmailMessage emailMessageValue = new EmailMessage(parameterJSON.get("value"), null, subscriberMessageTemplateService, criterionContext);
                nodeParameters.put(parameterName, emailMessageValue);
                break;

              case PushMessageParameter:
                PushMessage pushMessageValue = new PushMessage(parameterJSON.get("value"), null, subscriberMessageTemplateService, criterionContext);
                nodeParameters.put(parameterName, pushMessageValue);
                break;
                
              case Dialog:
                HashMap<String,Boolean> dialogMessageFieldsMandatory = new HashMap<String, Boolean>();
                String communicationChannelID = JSONUtilities.decodeString(nodeType.getJSONRepresentation(), "communicationChannelID");
                CommunicationChannel channel = Deployment.getCommunicationChannels().get(communicationChannelID);
                for(CriterionField param : channel.getParameters().values()) {
                  if(param.getFieldDataType().getExternalRepresentation().startsWith("template_")) {
                    dialogMessageFieldsMandatory.put(param.getID(), param.getMandatoryParameter());
                  }
                }
                JSONObject value = JSONUtilities.decodeJSONObject(parameterJSON, "value", false); //(JSONObject)parameterJSON.get("value");
                if (value != null)
                  {
                    JSONArray message = JSONUtilities.decodeJSONArray(value, "message");
                    // in case of TemplateID reference:
                    //{
                    //  "parameterName": "node.parameter.dialog_template",
                    //  "value": {
                    //      "macros": [
                    //          {
                    //              "campaignValue": "subscriber.email",
                    //              "templateValue": "emailAddress"
                    //          }
                    //      ],
                    //      "templateID": "17"
                    //  }
                    // 
                    // In case of InLine template:
                    // {
                    //  "parameterName": "node.parameter.dialog_template",
                    //  "value": {
                    //      "message": [
                    //          {
                    //              "node.parameter.subject": "subj",
                    //              "languageID": "1",
                    //              "node.parameter.body": "<!DOCTYPE html>\n<html>\n<head>\n</head>\n<body>\n<p>html 2{Campaign Name}</p>\n<p>si inca unu {mama}&nbsp;nu are mere</p>\n</body>\n</html>"
                    //          },
                    //          {
                    //              "node.parameter.subject": "sagsgsa",
                    //              "languageID": "3",
                    //              "node.parameter.body": "<!DOCTYPE html>\n<html>\n<head>\n</head>\n<body>\n<p>html 2{Campaign Name}</p>\n<p>si inca unu {mama}&nbsp;nu are mere</p>\n</body>\n</html>"
                    //          }
                    //      ]
                    //   }
                    // }
                    if(message != null) {                
                      // case InLine Template
                      NotificationTemplateParameters templateParameters = new NotificationTemplateParameters(message, communicationChannelID, dialogMessageFieldsMandatory, subscriberMessageTemplateService, criterionContext);
                      nodeParameters.put(parameterName, templateParameters);
                    }
                    else {
                      // case referenced Template
                      NotificationTemplateParameters templateParameters = new NotificationTemplateParameters(value, communicationChannelID, dialogMessageFieldsMandatory, subscriberMessageTemplateService, criterionContext);
                      nodeParameters.put(parameterName, templateParameters);
                    }
                  }
                break;

              case WorkflowParameter:
                WorkflowParameter workflowParameter = new WorkflowParameter((JSONObject) parameterJSON.get("value"), journeyService, criterionContext, tenantID);
                nodeParameters.put(parameterName, workflowParameter);
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

    private ParameterMap decodeExpressionValuedParameters(JSONArray jsonArray, NodeType nodeType, CriterionContext criterionContext, int tenantID) throws GUIManagerException
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

          ParameterExpression parameterExpressionValue = new ParameterExpression(JSONUtilities.decodeJSONObject(parameterJSON, "value", true), criterionContext, tenantID);
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
              case AniversaryCriterion:
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
                
              case TimeCriterion:
                switch (parameterExpressionValue.getType())
                  {
                    case TimeExpression:
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
              case NotificationStringParameter:
              case NotificationHTMLStringParameter:
              case Dialog:
              case WorkflowParameter:
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

    private List<OutgoingConnectionPoint> decodeOutgoingConnectionPoints(JSONArray jsonArray, NodeType nodeType, CriterionContext criterionContext, JourneyService journeyService, SubscriberMessageTemplateService subscriberMessageTemplateService, DynamicEventDeclarationsService dynamicEventDeclarationsService, int tenantID) throws GUIManagerException
    {
      List<OutgoingConnectionPoint> outgoingConnectionPoints = new ArrayList<OutgoingConnectionPoint>();
      for (int i=0; i<jsonArray.size(); i++)
        {
          JSONObject connectionPointJSON = (JSONObject) jsonArray.get(i);
          OutgoingConnectionPoint outgoingConnectionPoint = new OutgoingConnectionPoint(connectionPointJSON, nodeType, criterionContext, journeyService, subscriberMessageTemplateService, dynamicEventDeclarationsService, tenantID);
          outgoingConnectionPoints.add(outgoingConnectionPoint);
        }
      return outgoingConnectionPoints;
    }

    /*****************************************
    *
    *  decodeContextVariables
    *
    *****************************************/

    private static List<ContextVariable> decodeContextVariables(JSONArray jsonArray, int tenantID) throws GUIManagerException
    {
      List<ContextVariable> contextVariables = new ArrayList<ContextVariable>();
      if (jsonArray != null)
        {
          for (int i=0; i<jsonArray.size(); i++)
            {
              JSONObject contextVariableJSON = (JSONObject) jsonArray.get(i);
              contextVariables.add(new ContextVariable(contextVariableJSON, tenantID));
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
    private CriterionContext linkCriterionContext;
    
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
    public CriterionContext getLinkCriterionContext() { return linkCriterionContext; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public OutgoingConnectionPoint(JSONObject jsonRoot, NodeType nodeType, CriterionContext nodeCriterionContext, JourneyService journeyService, SubscriberMessageTemplateService subscriberMessageTemplateService, DynamicEventDeclarationsService dynamicEventDeclarationsService, int tenantID) throws GUIManagerException
    {
      //
      //  data
      //

      this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
      this.display = JSONUtilities.decodeString(jsonRoot, "display", true);
      this.evaluationPriority = EvaluationPriority.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "evaluationPriority", "normal"));
      this.evaluateContextVariables = JSONUtilities.decodeBoolean(jsonRoot, "evaluateContextVariables", Boolean.FALSE);
      this.additionalCriteria = JSONUtilities.decodeString(jsonRoot, "additionalCriteria", false);

      //
      //  outputConnectorParameters (independent, i.e., not EvaluationCriteria or messages)
      //

      this.outputConnectorParameters = decodeIndependentOutputConnectorParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", new JSONArray()), nodeType);

      //
      //  eventName
      //

      String eventName = this.outputConnectorParameters.containsKey("link.parameter.eventname") ? (String) this.outputConnectorParameters.get("link.parameter.eventname") : null;
      EvolutionEngineEventDeclaration linkEvent = (eventName != null) ? dynamicEventDeclarationsService.getStaticAndDynamicEvolutionEventDeclarations().get(eventName) : null;
      if (eventName != null && linkEvent == null) throw new GUIManagerException("unknown event", eventName);

      //
      //  criterionContext
      //

      this.linkCriterionContext = new CriterionContext(nodeCriterionContext, nodeType, linkEvent, tenantID);

      //
      //  additional parameters
      //

      this.outputConnectorParameters.putAll(decodeDependentOutputConnectorParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", new JSONArray()), nodeType, linkCriterionContext, journeyService, subscriberMessageTemplateService, tenantID));
      this.outputConnectorParameters.putAll(decodeExpressionValuedOutputConnectorParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", new JSONArray()), nodeType, linkCriterionContext, tenantID));

      //
      //  transition criteria
      //

      this.transitionCriteria = decodeTransitionCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "transitionCriteria", false), linkCriterionContext, tenantID);
    }

    /*****************************************
    *
    *  decodeIndependentOutputConnectorParameters
    *
    *****************************************/

    private ParameterMap decodeIndependentOutputConnectorParameters(JSONArray jsonArray, NodeType nodeType) throws GUIManagerException
    {
      ParameterMap outputConnectorParameters = new ParameterMap();
      for (int i=0; i<jsonArray.size(); i++)
        {
          JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
          String parameterName = JSONUtilities.decodeString(parameterJSON, "parameterName", true);
          CriterionField parameter = nodeType.getOutputConnectorParameters().get(parameterName);
          if (parameter == null) throw new GUIManagerException("unknown parameter", parameterName);
          if (Journey.isExpressionValuedParameterValue(parameterJSON)) continue;
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

              case AniversaryCriterion:
              case DateCriterion:
                outputConnectorParameters.put(parameterName, GUIManagedObject.parseDateField(JSONUtilities.decodeString(parameterJSON, "value", false)));
                break;
                
              case TimeCriterion:
                outputConnectorParameters.put(parameterName, JSONUtilities.decodeString(parameterJSON, "value", false));
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
            }
        }
      return outputConnectorParameters;
    }

    /*****************************************
    *
    *  decodeDependentOutputConnectorParameters
    *
    *****************************************/

    private ParameterMap decodeDependentOutputConnectorParameters(JSONArray jsonArray, NodeType nodeType, CriterionContext criterionContext, JourneyService journeyService, SubscriberMessageTemplateService subscriberMessageTemplateService, int tenantID) throws GUIManagerException
    {
      ParameterMap outputConnectorParameters = new ParameterMap();
      for (int i=0; i<jsonArray.size(); i++)
        {
          /*****************************************
          *
          *  parameter
          *
          *****************************************/

          JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
          String parameterName = JSONUtilities.decodeString(parameterJSON, "parameterName", true);
          CriterionField parameter = nodeType.getOutputConnectorParameters().get(parameterName);
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
                    evaluationCriteriaValue.add(new EvaluationCriterion((JSONObject) evaluationCriteriaArray.get(j), criterionContext, tenantID));
                  }
                outputConnectorParameters.put(parameterName, evaluationCriteriaValue);
                break;

              case SMSMessageParameter:
                SMSMessage smsMessageValue = new SMSMessage(parameterJSON.get("value"), null, subscriberMessageTemplateService, criterionContext);
                outputConnectorParameters.put(parameterName, smsMessageValue);
                break;

              case EmailMessageParameter:
                EmailMessage emailMessageValue = new EmailMessage(parameterJSON.get("value"), null, subscriberMessageTemplateService, criterionContext);
                outputConnectorParameters.put(parameterName, emailMessageValue);
                break;

              case PushMessageParameter:
                PushMessage pushMessageValue = new PushMessage(parameterJSON.get("value"), null, subscriberMessageTemplateService, criterionContext);
                outputConnectorParameters.put(parameterName, pushMessageValue);
                break;

                
// I keep the code in case of... But it should never be used as Dialog type should be only for DependentNodeParameters                
//              case Dialog:
//                HashMap<String,Boolean> dialogMessageFieldsMandatory = new HashMap<String, Boolean>();
//                for(CriterionField param : nodeType.getParameters().values()) {
//                  if(param.getFieldDataType().getExternalRepresentation().startsWith("template_")) {
//                    dialogMessageFieldsMandatory.put(param.getID(), param.getMandatoryParameter());
//                  }
//                }
//                JSONObject value = (JSONObject)parameterJSON.get("value");
//                JSONArray message = JSONUtilities.decodeJSONArray(value, "message");
//                NotificationTemplateParameters templateParameters = new NotificationTemplateParameters(message, dialogMessageFieldsMandatory, subscriberMessageTemplateService, criterionContext);
//                outputConnectorParameters.put(parameterName, templateParameters);
//                break;

              case WorkflowParameter:
                WorkflowParameter workflowParameter = new WorkflowParameter((JSONObject) parameterJSON.get("value"), journeyService, criterionContext, tenantID);
                outputConnectorParameters.put(parameterName, workflowParameter);
                break;
            }
        }
      return outputConnectorParameters;
    }

    /*****************************************
    *
    *  decodeExpressionValuedParameters
    *
    *****************************************/

    private ParameterMap decodeExpressionValuedOutputConnectorParameters(JSONArray jsonArray, NodeType nodeType, CriterionContext criterionContext, int tenantID) throws GUIManagerException
    {
      ParameterMap outputConnectorParameters = new ParameterMap();
      for (int i=0; i<jsonArray.size(); i++)
        {
          /*****************************************
          *
          *  parameter
          *
          *****************************************/

          JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
          String parameterName = JSONUtilities.decodeString(parameterJSON, "parameterName", true);
          CriterionField parameter = nodeType.getOutputConnectorParameters().get(parameterName);
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

          ParameterExpression parameterExpressionValue = new ParameterExpression(JSONUtilities.decodeJSONObject(parameterJSON, "value", true), criterionContext, tenantID);
          outputConnectorParameters.put(parameterName, parameterExpressionValue);

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

              case AniversaryCriterion:
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
                
              case TimeCriterion:
                switch (parameterExpressionValue.getType())
                  {
                    case TimeExpression:
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
              case NotificationStringParameter:
              case NotificationHTMLStringParameter:
              case Dialog:
              case WorkflowParameter:
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
      return outputConnectorParameters;
    }

    /*****************************************
    *
    *  decodeTransitionCriteria
    *
    *****************************************/

    private List<EvaluationCriterion> decodeTransitionCriteria(JSONArray jsonArray, CriterionContext criterionContext, int tenantID) throws GUIManagerException
    {
      List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
      if (jsonArray != null)
        {
          for (int i=0; i<jsonArray.size(); i++)
            {
              result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), criterionContext, tenantID));
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
        epochChanged = epochChanged || ! Objects.equals(targetingEventCriteria, existingJourney.getTargetingEventCriteria());
        epochChanged = epochChanged || ! Objects.equals(targetID, existingJourney.getTargetID());
        epochChanged = epochChanged || ! Objects.equals(startNodeID, existingJourney.getStartNodeID());
        epochChanged = epochChanged || ! Objects.equals(endNodeID, existingJourney.getEndNodeID());
        epochChanged = epochChanged || ! Objects.equals(journeyObjectiveInstances, existingJourney.getJourneyObjectiveInstances());
        epochChanged = epochChanged || ! Objects.equals(journeyNodes, existingJourney.getJourneyNodes());
        epochChanged = epochChanged || ! Objects.equals(journeyLinks, existingJourney.getJourneyLinks());
        epochChanged = epochChanged || ! Objects.equals(boundParameters, existingJourney.getBoundParameters());
        epochChanged = epochChanged || ! Objects.equals(appendInclusionLists, existingJourney.getAppendInclusionLists());
        epochChanged = epochChanged || ! Objects.equals(appendExclusionLists, existingJourney.getAppendExclusionLists());
        epochChanged = epochChanged || ! Objects.equals(appendUCG, existingJourney.getAppendUCG());
        epochChanged = epochChanged || ! Objects.equals(approval, existingJourney.getApproval());
        epochChanged = epochChanged || ! Objects.equals(maxNoOfCustomers, existingJourney.getMaxNoOfCustomers());
        epochChanged = epochChanged || ! Objects.equals(fullStatistics, existingJourney.getFullStatistics());
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
          case "targetGroup":
            contextUpdate.getParameters().put(SubscriberJourneyStatusField.StatusTargetGroup.getJourneyParameterName(), Boolean.TRUE);
            contextUpdate.getParameters().put(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName(), Boolean.FALSE);
            contextUpdate.getParameters().put(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName(), Boolean.FALSE);
            break;
          case "controlGroup":
            contextUpdate.getParameters().put(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName(), Boolean.TRUE);
            contextUpdate.getParameters().put(SubscriberJourneyStatusField.StatusTargetGroup.getJourneyParameterName(), Boolean.FALSE);
            contextUpdate.getParameters().put(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName(), Boolean.FALSE);
            break;
          case "universalControlGroup":
            contextUpdate.getParameters().put(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName(), Boolean.TRUE);
            contextUpdate.getParameters().put(SubscriberJourneyStatusField.StatusTargetGroup.getJourneyParameterName(), Boolean.FALSE);
            contextUpdate.getParameters().put(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName(), Boolean.FALSE);
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
  
  /*******************************
   * 
   * getGUIDependencies
   * 
   *******************************/
  
  @Override public Map<String, List<String>> getGUIDependencies()
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    List<String> targetIDs = new ArrayList<String>();
    switch (getGUIManagedObjectType())
      {
        case Journey:
          
          //
          //  campaign
          //
          
          List<String> campaignIDs = new ArrayList<String>();
          for (JourneyNode journeyNode : getJourneyNodes().values())
            {
              if (journeyNode.getNodeType().getActionManager() != null)
                {
                  String campaignID = journeyNode.getNodeType().getActionManager().getGUIDependencies(journeyNode).get("journey");
                  if (campaignID != null)campaignIDs.add(campaignID);
                }
            }
          result.put("campaign", campaignIDs);
          
          List<String> journeyObjectiveIDs = getJourneyObjectiveInstances().stream().map(journeyObjective -> journeyObjective.getJourneyObjectiveID()).collect(Collectors.toList());
          result.put("journeyobjective", journeyObjectiveIDs);
       
          targetIDs = getTargetID();
          result.put("target", targetIDs);
          
          
          break;
          
        case Campaign:
          
          //
          //  offer
          //
          List<String> pointIDs = new ArrayList<String>();
          List<String> offerIDs = new ArrayList<String>();
          for (JourneyNode offerNode : getJourneyNodes().values())
            {
              if (offerNode.getNodeType().getActionManager() != null)
                {
                  String offerID = offerNode.getNodeType().getActionManager().getGUIDependencies(offerNode).get("offer");
                  if (offerID != null) 
                	  offerIDs.add(offerID);
                  
                  
                  String pointID = offerNode.getNodeType().getActionManager().getGUIDependencies(offerNode).get("point");
                  if (pointID != null) pointIDs.add(pointID);
                }
            }
          result.put("offer", offerIDs);
          result.put("point", pointIDs);
          
          List<String> journeyObjIDs = getJourneyObjectiveInstances().stream().map(journeyObjective -> journeyObjective.getJourneyObjectiveID()).collect(Collectors.toList());
          result.put("journeyobjective", journeyObjIDs);
          
          targetIDs = getTargetID();
          result.put("target", targetIDs);
            
          
          break;

        case BulkCampaign:
            List<String> blkpointIDs = new ArrayList<String>();
         if (this.boundParameters.containsKey("journey.deliverableID") && boundParameters.get("journey.deliverableID").toString().startsWith(CommodityDeliveryManager.POINT_PREFIX))
        	 blkpointIDs.add(boundParameters.get("journey.deliverableID").toString().replace(CommodityDeliveryManager.POINT_PREFIX, ""));
             result.put("point", blkpointIDs);    
             
            targetIDs = getTargetID();
             result.put("target", targetIDs);
             
            break;
            
        default:
          break;
      }

    return result;
  }
}
