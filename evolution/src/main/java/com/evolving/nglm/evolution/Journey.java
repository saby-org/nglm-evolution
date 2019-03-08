/*****************************************************************************
*
*  Journey.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.TimeUnit;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

public class Journey extends GUIManagedObject
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

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
  //  JourneyStatus
  //

  public enum JourneyStatus
  {
    NotEligible("notEligible"),
    Eligible("eligible"),
    Notified("notified"),
    Converted("converted"),
    NotConverted("notConverted"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private JourneyStatus(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static JourneyStatus fromExternalRepresentation(String externalRepresentation) { for (JourneyStatus enumeratedValue : JourneyStatus.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  JourneyStatusField
  //

  public enum JourneyStatusField
  {
    StatusNotified("statusNotified", "journey.status.notified"),
    StatusConverted("statusConverted", "journey.status.converted"),
    StatusControlGroup("statusControlGroup", "journey.status.controlgroup"),
    StatusUniversalControlGroup("statusUniversalControlGroup", "journey.status.universalcontrolgroup"),
    Unknown("(unknown)", "(unknown)");
    private String externalRepresentation;
    private String journeyParameterName;
    private JourneyStatusField(String externalRepresentation, String journeyParameterName) { this.externalRepresentation = externalRepresentation; this.journeyParameterName = journeyParameterName; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getJourneyParameterName() { return journeyParameterName; }
    public static JourneyStatusField fromExternalRepresentation(String externalRepresentation) { for (JourneyStatusField enumeratedValue : JourneyStatusField.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("journeyMetrics", SchemaBuilder.map(CriterionField.schema(), Schema.STRING_SCHEMA).name("journey_journey_metrics").schema());
    schemaBuilder.field("journeyParameters", SchemaBuilder.map(Schema.STRING_SCHEMA, CriterionField.schema()).name("journey_journey_parameters").schema());
    schemaBuilder.field("autoTargeted", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("targetingWindowDuration", Schema.INT32_SCHEMA);
    schemaBuilder.field("targetingWindowUnit", Schema.STRING_SCHEMA);
    schemaBuilder.field("targetingWindowRoundUp", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("targetingCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("startNodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("endNodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyObjectives", SchemaBuilder.array(JourneyObjectiveInstance.schema()).schema());
    schemaBuilder.field("journeyNodes", SchemaBuilder.array(JourneyNode.schema()).schema());
    schemaBuilder.field("journeyLinks", SchemaBuilder.array(JourneyLink.schema()).schema());
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

  private Map<CriterionField,CriterionField> journeyMetrics;            // TBD:  the value is currently hacked to be CriterionField (i.e., history.totalCharge.yesterday) 
  private Map<String,CriterionField> journeyParameters;
  private boolean autoTargeted;
  private int targetingWindowDuration;
  private TimeUnit targetingWindowUnit;
  private boolean targetingWindowRoundUp;
  private List<EvaluationCriterion> targetingCriteria;
  private String startNodeID;
  private String endNodeID;
  private Set<JourneyObjectiveInstance> journeyObjectiveInstances; 
  private Map<String,JourneyNode> journeyNodes;
  private Map<String,JourneyLink> journeyLinks;

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
  public Map<CriterionField,CriterionField> getJourneyMetrics() { return journeyMetrics; }
  public Map<String,CriterionField> getJourneyParameters() { return journeyParameters; }
  public boolean getAutoTargeted() { return autoTargeted; }
  public int getTargetingWindowDuration() { return targetingWindowDuration; }
  public TimeUnit getTargetingWindowUnit() { return targetingWindowUnit; }
  public boolean getTargetingWindowRoundUp() { return targetingWindowRoundUp; }
  public List<EvaluationCriterion> getTargetingCriteria() { return targetingCriteria; }
  public String getStartNodeID() { return startNodeID; }
  public String getEndNodeID() { return endNodeID; }
  public Set<JourneyObjectiveInstance> getJourneyObjectiveInstances() { return journeyObjectiveInstances;  }
  public Map<String,JourneyNode> getJourneyNodes() { return journeyNodes; }
  public Map<String,JourneyLink> getJourneyLinks() { return journeyLinks; }
  public JourneyNode getJourneyNode(String nodeID) { return journeyNodes.get(nodeID); }
  public JourneyLink getJourneyLink(String linkID) { return journeyLinks.get(linkID); }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Journey(SchemaAndValue schemaAndValue, Map<CriterionField,CriterionField> journeyMetrics, Map<String,CriterionField> journeyParameters, boolean autoTargeted, int targetingWindowDuration, TimeUnit targetingWindowUnit, boolean targetingWindowRoundUp, List<EvaluationCriterion> targetingCriteria, String startNodeID, String endNodeID, Set<JourneyObjectiveInstance> journeyObjectiveInstances, Map<String,JourneyNode> journeyNodes, Map<String,JourneyLink> journeyLinks)
  {
    super(schemaAndValue);
    this.journeyMetrics = journeyMetrics;
    this.journeyParameters = journeyParameters;
    this.autoTargeted = autoTargeted;
    this.targetingWindowDuration = targetingWindowDuration;
    this.targetingWindowUnit = targetingWindowUnit;
    this.targetingWindowRoundUp = targetingWindowRoundUp;
    this.targetingCriteria = targetingCriteria;
    this.startNodeID = startNodeID;
    this.endNodeID = endNodeID;
    this.journeyObjectiveInstances = journeyObjectiveInstances;
    this.journeyNodes = journeyNodes;
    this.journeyLinks = journeyLinks;
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
    struct.put("journeyMetrics", packJourneyMetrics(journey.getJourneyMetrics()));
    struct.put("journeyParameters", packJourneyParameters(journey.getJourneyParameters()));
    struct.put("autoTargeted", journey.getAutoTargeted());
    struct.put("targetingWindowDuration", journey.getTargetingWindowDuration());
    struct.put("targetingWindowUnit", journey.getTargetingWindowUnit().getExternalRepresentation());
    struct.put("targetingWindowRoundUp", journey.getTargetingWindowRoundUp());
    struct.put("targetingCriteria", packTargetingCriteria(journey.getTargetingCriteria()));
    struct.put("startNodeID", journey.getStartNodeID());
    struct.put("endNodeID", journey.getEndNodeID());
    struct.put("journeyObjectives", packJourneyObjectiveInstances(journey.getJourneyObjectiveInstances()));
    struct.put("journeyNodes", packJourneyNodes(journey.getJourneyNodes()));
    struct.put("journeyLinks", packJourneyLinks(journey.getJourneyLinks()));
    return struct;
  }

  /****************************************
  *
  *  packJourneyMetrics
  *
  ****************************************/

  private static Map<Object,String> packJourneyMetrics(Map<CriterionField,CriterionField> journeyMetrics)
  {
    Map<Object,String> result = new LinkedHashMap<Object,String>();
    for (CriterionField criterionField : journeyMetrics.keySet())
      {
        CriterionField baseMetric = journeyMetrics.get(criterionField);
        result.put(CriterionField.pack(criterionField), baseMetric.getID());
      }
    return result;
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
  *  packTargetingCriteria
  *
  ****************************************/

  private static List<Object> packTargetingCriteria(List<EvaluationCriterion> targetingCriteria)
  {
    List<Object> result = new ArrayList<Object>();
    for (EvaluationCriterion criterion : targetingCriteria)
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
    Map<CriterionField,CriterionField> journeyMetrics = unpackJourneyMetrics(schema.field("journeyMetrics").schema(), (Map<Object,String>) valueStruct.get("journeyMetrics"));
    Map<String,CriterionField> journeyParameters = unpackJourneyParameters(schema.field("journeyParameters").schema(), (Map<String,Object>) valueStruct.get("journeyParameters"));
    boolean autoTargeted = valueStruct.getBoolean("autoTargeted");
    int targetingWindowDuration = valueStruct.getInt32("targetingWindowDuration");
    TimeUnit targetingWindowUnit = TimeUnit.fromExternalRepresentation(valueStruct.getString("targetingWindowUnit"));
    boolean targetingWindowRoundUp = valueStruct.getBoolean("targetingWindowRoundUp");
    List<EvaluationCriterion> targetingCriteria = unpackTargetingCriteria(schema.field("targetingCriteria").schema(), valueStruct.get("targetingCriteria"));
    String startNodeID = valueStruct.getString("startNodeID");
    String endNodeID = valueStruct.getString("endNodeID");
    Set<JourneyObjectiveInstance> journeyObjectiveInstances = unpackJourneyObjectiveInstances(schema.field("journeyObjectives").schema(), valueStruct.get("journeyObjectives"));
    Map<String,JourneyNode> journeyNodes = unpackJourneyNodes(schema.field("journeyNodes").schema(), valueStruct.get("journeyNodes"));
    Map<String,JourneyLink> journeyLinks = unpackJourneyLinks(schema.field("journeyLinks").schema(), valueStruct.get("journeyLinks"));

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

    return new Journey(schemaAndValue, journeyMetrics, journeyParameters, autoTargeted, targetingWindowDuration, targetingWindowUnit, targetingWindowRoundUp, targetingCriteria, startNodeID, endNodeID, journeyObjectiveInstances, journeyNodes, journeyLinks);
  }
  
  /*****************************************
  *
  *  unpackJourneyMetrics
  *
  *****************************************/

  private static Map<CriterionField,CriterionField> unpackJourneyMetrics(Schema schema, Map<Object,String> journeyMetrics)
  {
    Map<CriterionField,CriterionField> result = new LinkedHashMap<CriterionField,CriterionField>();
    for (Object packedJourneyMetric : journeyMetrics.keySet())
      {
        CriterionField journeyMetric = CriterionField.unpack(new SchemaAndValue(schema.keySchema(), packedJourneyMetric));
        String baseMetricID = journeyMetrics.get(packedJourneyMetric);
        CriterionField baseMetric = CriterionContext.Profile.getCriterionFields().get(baseMetricID);
        if (baseMetric == null) throw new SerializationException("unknown baseMetric: " + baseMetricID);
        result.put(journeyMetric, baseMetric);
      }
    return result;
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
  *  unpackTargetingCriteria
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackTargetingCriteria(Schema schema, Object value)
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

  public Journey(JSONObject jsonRoot, GUIManagedObjectType journeyType, long epoch, GUIManagedObject existingJourneyUnchecked, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
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

    this.journeyMetrics = decodeJourneyMetrics(JSONUtilities.decodeJSONArray(jsonRoot, "journeyMetrics", false));
    this.journeyParameters = decodeJourneyParameters(JSONUtilities.decodeJSONArray(jsonRoot, "journeyParameters", false));
    this.autoTargeted = JSONUtilities.decodeBoolean(jsonRoot, "autoTargeted", new Boolean(Deployment.getJourneyDefaultAutoTarget()));
    this.targetingWindowDuration = JSONUtilities.decodeInteger(jsonRoot, "targetingWindowDuration", Deployment.getJourneyDefaultTargetingWindowDuration());
    this.targetingWindowUnit = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingWindowUnit", Deployment.getJourneyDefaultTargetingWindowUnit()));
    this.targetingWindowRoundUp = JSONUtilities.decodeBoolean(jsonRoot, "targetingWindowRoundUp", new Boolean(Deployment.getJourneyDefaultTargetingWindowRoundUp()));
    this.targetingCriteria = decodeTargetingCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "targetConditions", false), Deployment.getJourneyUniversalTargetingCriteria());
    this.journeyObjectiveInstances = decodeJourneyObjectiveInstances(JSONUtilities.decodeJSONArray(jsonRoot, "journeyObjectives", false), catalogCharacteristicService);
    Map<String,GUINode> jsonNodes = decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot, "nodes", true), this);
    List<GUILink> jsonLinks = decodeLinks(JSONUtilities.decodeJSONArray(jsonRoot, "links", true));

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    //
    //  autoTargeting and parameters
    //

    if (this.autoTargeted && this.journeyParameters.size() > 0) throw new GUIManagerException("autoTargeted Journey may not have parameters", this.getJourneyID());

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
        journeyNodes.put(jsonNode.getNodeID(), new JourneyNode(jsonNode.getNodeID(), jsonNode.getNodeName(), jsonNode.getNodeType(), jsonNode.getNodeParameters(), new ArrayList<String>(), new ArrayList<String>()));
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
        JourneyLink journeyLink = new JourneyLink(linkID, outgoingConnectionPoint.getName(), outgoingConnectionPoint.getOutputConnectorParameters(), sourceNode.getNodeID(), destinationNode.getNodeID(), outgoingConnectionPoint.getEvaluationPriority(), transitionCriteria);
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
    *  targeting criteria from start node
    *
    *****************************************/

    //
    //  autoTargeted
    //

    if (startNode.getNodeParameters().containsKey("node.parameter.autotargeted"))
      {
        Boolean autoTargeted = (Boolean) startNode.getNodeParameters().get("node.parameter.autotargeted");
        this.autoTargeted = (autoTargeted != null) ? autoTargeted.booleanValue() : false;
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
  *  decodeJourneyMetrics
  *
  *****************************************/

  public static Map<CriterionField,CriterionField> decodeJourneyMetrics(JSONArray jsonArray) throws GUIManagerException
  {
    Map<CriterionField,CriterionField> journeyMetrics = new LinkedHashMap<CriterionField,CriterionField>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            //
            //  parse
            //

            JSONObject journeyMetricJSON = (JSONObject) jsonArray.get(i);
            String journeyMetricName = JSONUtilities.decodeString(journeyMetricJSON, "criterionFieldID", true);
            String baseMetricID = JSONUtilities.decodeString(journeyMetricJSON, "baseMetric", true);
            CriterionField baseMetric = CriterionContext.Profile.getCriterionFields().get(baseMetricID);

            //
            //  validate
            //

            if (baseMetric == null) throw new GUIManagerException("unknown baseMetric", baseMetricID);
            switch (baseMetric.getFieldDataType())
              {
                case IntegerCriterion:
                case DoubleCriterion:
                  break;

                default:
                  throw new GUIManagerException("non-numeric baseMetric", baseMetricID);
              }

            //
            //  journeyMetric
            //

            CriterionField journeyMetric = new CriterionField(baseMetric, journeyMetricName, "getJourneyMetric", false, baseMetric.getTagFormat(), baseMetric.getTagMaxLength());

            //
            //  result
            //

            journeyMetrics.put(journeyMetric, baseMetric);
          }
      }
    return journeyMetrics;
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
  *  decodeTargetingCriteria
  *
  *****************************************/

  private List<EvaluationCriterion> decodeTargetingCriteria(JSONArray jsonArray, List<EvaluationCriterion> universalTargetingCriteria) throws GUIManagerException
  {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();

    //
    //  universal criteria
    //

    result.addAll(universalTargetingCriteria);

    //
    //  journey-level targeting critera
    //

    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.Profile));
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

  private Map<String,GUINode> decodeNodes(JSONArray jsonArray, Journey journey) throws GUIManagerException
  {
    Map<String,GUINode> nodes = new LinkedHashMap<String,GUINode>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject nodeJSON = (JSONObject) jsonArray.get(i);
        GUINode node = new GUINode(nodeJSON, journey);
        nodes.put(node.getNodeID(), node);
      }
    return nodes;
  }

  /*****************************************
  *
  *  decodeLinks
  *
  *****************************************/

  private List<GUILink> decodeLinks(JSONArray jsonArray) throws GUIManagerException
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
  *  validate
  *
  *****************************************/

  public void validate(JourneyObjectiveService journeyObjectiveService, CatalogCharacteristicService catalogCharacteristicService, Date date) throws GUIManagerException
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

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public GUINode(JSONObject jsonRoot, Journey journey) throws GUIManagerException
    {
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
      EvolutionEngineEventDeclaration nodeEvent = (eventName != null) ? Deployment.getEvolutionEngineEvents().get(eventName) : null;
      if (eventName != null && nodeEvent == null) throw new GUIManagerException("unknown event", eventName);

      //
      //  criterionContext
      //

      CriterionContext nodeCriterionContext = new CriterionContext(journey.getJourneyMetrics(), journey.getJourneyParameters(), this.nodeType, nodeEvent, false);
      CriterionContext linkCriterionContext = new CriterionContext(journey.getJourneyMetrics(), journey.getJourneyParameters(), this.nodeType, nodeEvent, true);

      //
      //  nodeParameters (dependent, ie., EvaluationCriteria and Messages which are dependent on other parameters)
      //

      this.nodeParameters.putAll(decodeDependentNodeParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", true), nodeType, nodeCriterionContext));

      //
      //  outputConnectors
      //

      this.outgoingConnectionPoints = decodeOutgoingConnectionPoints(JSONUtilities.decodeJSONArray(jsonRoot, "outputConnectors", true), nodeType, linkCriterionContext);
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
                nodeParameters.put(parameterName, JSONUtilities.decodeDate(parameterJSON, "value", false));  // TBD DEW:  use a string date format
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

    private ParameterMap decodeDependentNodeParameters(JSONArray jsonArray, NodeType nodeType, CriterionContext criterionContext) throws GUIManagerException
    {
      ParameterMap nodeParameters = new ParameterMap();
      for (int i=0; i<jsonArray.size(); i++)
        {
          JSONObject parameterJSON = (JSONObject) jsonArray.get(i);
          String parameterName = JSONUtilities.decodeString(parameterJSON, "parameterName", true);
          CriterionField parameter = nodeType.getParameters().get(parameterName);
          if (parameter == null) throw new GUIManagerException("unknown parameter", parameterName);
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
                SMSMessage smsMessageValue = new SMSMessage(JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray()), criterionContext);
                nodeParameters.put(parameterName, smsMessageValue);
                break;

              case EmailMessageParameter:
                EmailMessage emailMessageValue = new EmailMessage(JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray()), criterionContext);
                nodeParameters.put(parameterName, emailMessageValue);
                break;

              case PushMessageParameter:
                PushMessage pushMessageValue = new PushMessage(JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray()), criterionContext);
                nodeParameters.put(parameterName, pushMessageValue);
                break;
            }
        }
      return nodeParameters;
    }

    /*****************************************
    *
    *  decodeOutgoingConnectionPoints
    *
    *****************************************/

    private List<OutgoingConnectionPoint> decodeOutgoingConnectionPoints(JSONArray jsonArray, NodeType nodeType, CriterionContext criterionContext) throws GUIManagerException
    {
      List<OutgoingConnectionPoint> outgoingConnectionPoints = new ArrayList<OutgoingConnectionPoint>();
      for (int i=0; i<jsonArray.size(); i++)
        {
          JSONObject connectionPointJSON = (JSONObject) jsonArray.get(i);
          OutgoingConnectionPoint outgoingConnectionPoint = new OutgoingConnectionPoint(connectionPointJSON, nodeType, criterionContext);
          outgoingConnectionPoints.add(outgoingConnectionPoint);
        }
      return outgoingConnectionPoints;
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
    private ParameterMap outputConnectorParameters;
    private EvaluationPriority evaluationPriority;
    private List<EvaluationCriterion> transitionCriteria;
    private String additionalCriteria;
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/
    
    public String getName() { return name; }
    public ParameterMap getOutputConnectorParameters() { return outputConnectorParameters; }
    public EvaluationPriority getEvaluationPriority() { return evaluationPriority; }
    public List<EvaluationCriterion> getTransitionCriteria() { return transitionCriteria; }
    public String getAdditionalCriteria() { return additionalCriteria; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public OutgoingConnectionPoint(JSONObject jsonRoot, NodeType nodeType, CriterionContext criterionContext) throws GUIManagerException
    {
      this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
      this.outputConnectorParameters = decodeOutputConnectorParameters(JSONUtilities.decodeJSONArray(jsonRoot, "parameters", false), nodeType, criterionContext);
      this.evaluationPriority = EvaluationPriority.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "evaluationPriority", "normal"));
      this.transitionCriteria = decodeTransitionCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "transitionCriteria", false), criterionContext);
      this.additionalCriteria = JSONUtilities.decodeString(jsonRoot, "additionalCriteria", false);
    }

    /*****************************************
    *
    *  decodeOutputConnectorParameters
    *
    *****************************************/

    private ParameterMap decodeOutputConnectorParameters(JSONArray jsonArray, NodeType nodeType, CriterionContext criterionContext) throws GUIManagerException
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
                    outputConnectorParameters.put(parameterName, JSONUtilities.decodeDate(parameterJSON, "value", false));  // TBD DEW:  use a string date format
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
                    SMSMessage smsMessageValue = new SMSMessage(JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray()), criterionContext);
                    outputConnectorParameters.put(parameterName, smsMessageValue);
                    break;

                  case EmailMessageParameter:
                    EmailMessage emailMessageValue = new EmailMessage(JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray()), criterionContext);
                    outputConnectorParameters.put(parameterName, emailMessageValue);
                    break;

                  case PushMessageParameter:
                    PushMessage pushMessageValue = new PushMessage(JSONUtilities.decodeJSONArray(parameterJSON, "value", new JSONArray()), criterionContext);
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
        epochChanged = epochChanged || ! Objects.equals(journeyMetrics, existingJourney.getJourneyMetrics());
        epochChanged = epochChanged || ! Objects.equals(journeyParameters, existingJourney.getJourneyParameters());
        epochChanged = epochChanged || ! (autoTargeted == existingJourney.getAutoTargeted());
        epochChanged = epochChanged || ! (targetingWindowDuration == existingJourney.getTargetingWindowDuration());
        epochChanged = epochChanged || ! (targetingWindowUnit == existingJourney.getTargetingWindowUnit());
        epochChanged = epochChanged || ! (targetingWindowRoundUp == existingJourney.getTargetingWindowRoundUp());
        epochChanged = epochChanged || ! Objects.equals(targetingCriteria, existingJourney.getTargetingCriteria());
        epochChanged = epochChanged || ! Objects.equals(startNodeID, existingJourney.getStartNodeID());
        epochChanged = epochChanged || ! Objects.equals(endNodeID, existingJourney.getEndNodeID());
        epochChanged = epochChanged || ! Objects.equals(journeyObjectiveInstances, existingJourney.getJourneyObjectiveInstances());
        epochChanged = epochChanged || ! Objects.equals(journeyNodes, existingJourney.getJourneyNodes());
        epochChanged = epochChanged || ! Objects.equals(journeyLinks, existingJourney.getJourneyLinks());
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

    @Override public DeliveryRequest executeOnEntry(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest)
    {
      JourneyStatusField statusField = JourneyStatusField.fromExternalRepresentation(subscriberEvaluationRequest.getJourneyNode().getNodeParameters().containsKey("node.parameter.journeystatus") ? (String) subscriberEvaluationRequest.getJourneyNode().getNodeParameters().get("node.parameter.journeystatus") : "(unknown)");
      if (statusField == null) throw new ServerRuntimeException("unknown status field: " + subscriberEvaluationRequest.getJourneyNode().getNodeParameters().get("node.parameter.journeystatus"));
      subscriberEvaluationRequest.getJourneyState().getJourneyParameters().put(statusField.getJourneyParameterName(), Boolean.TRUE);
      return null;
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

    @Override public void executeOnExit(EvolutionEventContext evolutionEventContext, SubscriberEvaluationRequest subscriberEvaluationRequest, JourneyLink journeyLink)
    {
      switch (journeyLink.getLinkName())
        {
          case "controlGroup":
            subscriberEvaluationRequest.getJourneyState().getJourneyParameters().put(JourneyStatusField.StatusControlGroup.getJourneyParameterName(), Boolean.TRUE);            
            break;
          case "universalControlGroup":
            subscriberEvaluationRequest.getJourneyState().getJourneyParameters().put(JourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName(), Boolean.TRUE);            
            break;
        }
    }
  }
}
