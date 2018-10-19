/*****************************************************************************
*
*  Journey.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionContext;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

public class Journey extends GUIManagedObject
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  JourneyNodeType
  //

  public enum JourneyNodeType
  {
    Start("start"),
    Wait("wait"),
    Action("action"),
    Fork("fork"),
    Join("join"),
    End("end"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private JourneyNodeType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static JourneyNodeType fromExternalRepresentation(String externalRepresentation) { for (JourneyNodeType enumeratedValue : JourneyNodeType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  JourneyLinkType
  //

  public enum JourneyLinkType
  {
    Unconditional("unconditional"),
    Timeout("timeout"),
    VisitNumber("visitNumber"),
    Trigger("trigger"),
    ProfileCriterion("profileCriterion"),
    ActionStatus("actionStatus"),
    MessageStatus("messageStatus"),
    ExitRequested("exitRequested"),  
    Unknown("(unknown)");
    private String externalRepresentation;
    private JourneyLinkType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static JourneyLinkType fromExternalRepresentation(String externalRepresentation) { for (JourneyLinkType enumeratedValue : JourneyLinkType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
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
    schemaBuilder.field("journeyMetrics", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).name("journey_journey_metrics").schema());
    schemaBuilder.field("journeyParameters", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).name("journey_journey_parameters").schema());
    schemaBuilder.field("autoTargeted", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("autoTargetingCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("startNodeID", Schema.STRING_SCHEMA);
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
  private Map<String,CriterionDataType> journeyParameters;
  private boolean autoTargeted;
  private List<EvaluationCriterion> autoTargetingCriteria;
  private String startNodeID;
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
  public Map<CriterionField,CriterionField> getJourneyMetrics() { return journeyMetrics; }
  public Map<String,CriterionDataType> getJourneyParameters() { return journeyParameters; }
  public boolean getAutoTargeted() { return autoTargeted; }
  public List<EvaluationCriterion> getAutoTargetingCriteria() { return autoTargetingCriteria; }
  public String getStartNodeID() { return startNodeID; }
  public Map<String,JourneyNode> getJourneyNodes() { return journeyNodes; }
  public Map<String,JourneyLink> getJourneyLinks() { return journeyLinks; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Journey(SchemaAndValue schemaAndValue, Map<CriterionField,CriterionField> journeyMetrics, Map<String,CriterionDataType> journeyParameters, boolean autoTargeted, List<EvaluationCriterion> autoTargetingCriteria, String startNodeID, Map<String,JourneyNode> journeyNodes, Map<String,JourneyLink> journeyLinks)
  {
    super(schemaAndValue);
    this.journeyMetrics = journeyMetrics;
    this.journeyParameters = journeyParameters;
    this.autoTargeted = autoTargeted;
    this.autoTargetingCriteria = autoTargetingCriteria;
    this.startNodeID = startNodeID;
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
    struct.put("autoTargetingCriteria", packAutoTargetingCriteria(journey.getAutoTargetingCriteria()));
    struct.put("startNodeID", journey.getStartNodeID());
    struct.put("journeyNodes", packJourneyNodes(journey.getJourneyNodes()));
    struct.put("journeyLinks", packJourneyLinks(journey.getJourneyLinks()));
    return struct;
  }

  /****************************************
  *
  *  packJourneyMetrics
  *
  ****************************************/

  private static Map<String,String> packJourneyMetrics(Map<CriterionField,CriterionField> journeyMetrics)
  {
    Map<String,String> result = new LinkedHashMap<String,String>();
    for (CriterionField criterionField : journeyMetrics.keySet())
      {
        CriterionField baseMetric = journeyMetrics.get(criterionField);
        result.put(criterionField.getID(), baseMetric.getID());
      }
    return result;
  }

  /****************************************
  *
  *  packJourneyParameters
  *
  ****************************************/

  private static Map<String,String> packJourneyParameters(Map<String,CriterionDataType> parameters)
  {
    Map<String,String> result = new LinkedHashMap<String,String>();
    for (String parameterName : parameters.keySet())
      {
        CriterionDataType parameterDataType = parameters.get(parameterName);
        result.put(parameterName,parameterDataType.getExternalRepresentation());
      }
    return result;
  }

  /****************************************
  *
  *  packAutoTargetingCriteria
  *
  ****************************************/

  private static List<Object> packAutoTargetingCriteria(List<EvaluationCriterion> autoTargetingCriteria)
  {
    List<Object> result = new ArrayList<Object>();
    for (EvaluationCriterion criterion : autoTargetingCriteria)
      {
        result.add(EvaluationCriterion.pack(criterion));
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
    Map<CriterionField,CriterionField> journeyMetrics = unpackJourneyMetrics((Map<String,String>) valueStruct.get("journeyMetrics"));
    Map<String,CriterionDataType> journeyParameters = unpackJourneyParameters((Map<String,String>) valueStruct.get("journeyParameters"));
    boolean autoTargeted = valueStruct.getBoolean("autoTargeted");
    List<EvaluationCriterion> autoTargetingCriteria = unpackAutoTargetingCriteria(schema.field("autoTargetingCriteria").schema(), valueStruct.get("autoTargetingCriteria"));
    String startNodeID = valueStruct.getString("startNodeID");
    Map<String,JourneyNode> journeyNodes = unpackJourneyNodes(schema.field("journeyNodes").schema(), valueStruct.get("journeyNodes"));
    Map<String,JourneyLink> journeyLinks = unpackJourneyLinks(schema.field("journeyLinks").schema(), valueStruct.get("journeyLinks"));

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

    //
    //  return
    //

    return new Journey(schemaAndValue, journeyMetrics, journeyParameters, autoTargeted, autoTargetingCriteria, startNodeID, journeyNodes, journeyLinks);
  }
  
  /*****************************************
  *
  *  unpackJourneyMetrics
  *
  *****************************************/

  private static Map<CriterionField,CriterionField> unpackJourneyMetrics(Map<String,String> journeyMetrics)
  {
    Map<CriterionField,CriterionField> result = new LinkedHashMap<CriterionField,CriterionField>();
    for (String journeyMetricID : journeyMetrics.keySet())
      {
        String baseMetricID = journeyMetrics.get(journeyMetricID);
        CriterionField baseMetric = Deployment.getProfileCriterionFields().get(baseMetricID);
        if (baseMetric == null) throw new SerializationException("unknown baseMetric: " + baseMetricID);
        CriterionField journeyMetric = baseMetric;                                                              // TBD:  hack hack hack
        result.put(journeyMetric, baseMetric);
      }
    return result;
  }

  /*****************************************
  *
  *  unpackJourneyParameters
  *
  *****************************************/

  private static Map<String,CriterionDataType> unpackJourneyParameters(Map<String,String> parameters)
  {
    Map<String,CriterionDataType> result = new LinkedHashMap<String,CriterionDataType>();
    for (String parameterName : parameters.keySet())
      {
        CriterionDataType criterionDataType = CriterionDataType.fromExternalRepresentation(parameters.get(parameterName));
        result.put(parameterName, criterionDataType);
      }
    return result;
  }

  /*****************************************
  *
  *  unpackAutoTargetingCriteria
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackAutoTargetingCriteria(Schema schema, Object value)
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

    Map<String,JourneyNode> result = new HashMap<String,JourneyNode>();
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

    Map<String,JourneyLink> result = new HashMap<String,JourneyLink>();
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

  public Journey(JSONObject jsonRoot, long epoch, GUIManagedObject existingJourneyUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingJourneyUnchecked != null) ? existingJourneyUnchecked.getEpoch() : epoch);

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

    this.journeyMetrics = decodeJourneyMetrics(JSONUtilities.decodeJSONArray(jsonRoot, "journeyMetrics", true));
    this.journeyParameters = decodeJourneyParameters(JSONUtilities.decodeJSONArray(jsonRoot, "journeyParameters", true));
    this.autoTargeted = JSONUtilities.decodeBoolean(jsonRoot, "autoTargeted", true);
    this.autoTargetingCriteria = decodeAutoTargetingCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "targetConditions", true));
    Map<String,GUINode> jsonNodes = decodeNodes(JSONUtilities.decodeJSONArray(jsonRoot, "nodes", true));
    List<GUILink> jsonLinks = decodeLinks(JSONUtilities.decodeJSONArray(jsonRoot, "links", true));

    /*****************************************
    *
    *  translate jsonNodes/jsonLinks to journeyNodes/journeyLinks
    *
    *****************************************/

    //
    //  build journeyNodes
    //

    this.journeyNodes = new HashMap<String,JourneyNode>();
    for (GUINode jsonNode : jsonNodes.values())
      {
        journeyNodes.put(jsonNode.getNodeID(), new JourneyNode(jsonNode.getNodeID(), jsonNode.getNodeName(), jsonNode.getNodeType(), new ArrayList<String>(), new ArrayList<String>()));
      }

    //
    //  startNodeID
    //

    this.startNodeID = null;
    for (JourneyNode journeyNode : this.journeyNodes.values())
      {
        switch (journeyNode.getNodeType())
          {
            case Start:
              if (this.startNodeID != null) throw new GUIManagerException("multiple start nodes", journeyNode.getNodeID());
              this.startNodeID = journeyNode.getNodeID();
              break;
          }
      }

    //
    //  build journeyLinks
    //

    this.journeyLinks = new HashMap<String,JourneyLink>();
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
        *  journeyLink
        *
        *****************************************/

        String linkID = jsonLink.getSourceNodeID() + "-" + Integer.toString(jsonLink.getSourceConnectionPoint()) + ":" + jsonLink.getDestinationNodeID() + "-" + Integer.toString(jsonLink.getDestinationConnectionPoint());
        JourneyLink journeyLink = new JourneyLink(linkID, outgoingConnectionPoint.getType(), sourceNode.getNodeID(), destinationNode.getNodeID(), outgoingConnectionPoint.getTransitionCriteria());
        journeyLinks.put(journeyLink.getLinkID(), journeyLink);

        /*****************************************
        *
        *  fixup nodes
        *
        *****************************************/

        //
        //  source node
        //

        JourneyNode sourceJourneyNode = journeyNodes.get(jsonLink.getSourceNodeID());
        sourceJourneyNode.getOutgoingLinkReferences().add(journeyLink.getLinkID());
        sourceJourneyNode.getOutgoingLinks().put(journeyLink.getLinkID(), journeyLink);

        //
        //  destination node
        //

        JourneyNode destinationJourneyNode = journeyNodes.get(jsonLink.getDestinationNodeID());
        destinationJourneyNode.getIncomingLinkReferences().add(journeyLink.getLinkID());
        destinationJourneyNode.getIncomingLinks().put(journeyLink.getLinkID(), journeyLink);
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

  private Map<CriterionField,CriterionField> decodeJourneyMetrics(JSONArray jsonArray) throws GUIManagerException
  {
    Map<CriterionField,CriterionField> journeyMetrics = new LinkedHashMap<CriterionField,CriterionField>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject journeyMetricJSON = (JSONObject) jsonArray.get(i);
        String journeyMetricName = JSONUtilities.decodeString(journeyMetricJSON, "criterionFieldID", true);
        String baseMetricName = JSONUtilities.decodeString(journeyMetricJSON, "baseMetric", true);
        CriterionField baseMetric = Deployment.getProfileCriterionFields().get(baseMetricName);
        if (baseMetric == null) throw new GUIManagerException("unknown baseMetric", baseMetricName);
        CriterionField journeyMetric = baseMetric;                                                              // TBD:  hack hack hack
        journeyMetrics.put(journeyMetric, baseMetric);
      }
    return journeyMetrics;
  }

  /*****************************************
  *
  *  decodeJourneyMetrics
  *
  *****************************************/

  private Map<String,CriterionDataType> decodeJourneyParameters(JSONArray jsonArray) throws GUIManagerException
  {
    Map<String,CriterionDataType> journeyParameters = new LinkedHashMap<String,CriterionDataType>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject journeyParameterJSON = (JSONObject) jsonArray.get(i);
        String journeyParameterName = JSONUtilities.decodeString(journeyParameterJSON, "name", true);
        CriterionDataType journeyParameterType = CriterionDataType.fromExternalRepresentation(JSONUtilities.decodeString(journeyParameterJSON, "type", true));
        if (journeyParameterType == CriterionDataType.Unknown) throw new GUIManagerException("unknown journeyParmeterType", JSONUtilities.decodeString(journeyParameterJSON, "type", true));
        journeyParameters.put(journeyParameterName, journeyParameterType);
      }
    return journeyParameters;
  }

  /*****************************************
  *
  *  decodeAutoTargetingCriteria
  *
  *****************************************/

  private List<EvaluationCriterion> decodeAutoTargetingCriteria(JSONArray jsonArray) throws GUIManagerException
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
  *  decodeNodes
  *
  *****************************************/

  private Map<String,GUINode> decodeNodes(JSONArray jsonArray) throws GUIManagerException
  {
    Map<String,GUINode> nodes = new HashMap<String,GUINode>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject nodeJSON = (JSONObject) jsonArray.get(i);
        GUINode node = new GUINode(nodeJSON);
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

  /*****************************************************************************
  *
  *  class GUINode
  *
  *****************************************************************************/
  
  private static class GUINode
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String nodeID;
    private String nodeName;
    private JourneyNodeType nodeType;
    private List<OutgoingConnectionPoint> outgoingConnectionPoints;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getNodeID() { return nodeID; }
    public String getNodeName() { return nodeName; }
    public JourneyNodeType getNodeType() { return nodeType; }
    public List<OutgoingConnectionPoint> getOutgoingConnectionPoints() { return outgoingConnectionPoints; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public GUINode(JSONObject jsonRoot) throws GUIManagerException
    {
      this.nodeID = JSONUtilities.decodeString(jsonRoot, "nodeID", true);
      this.nodeName = JSONUtilities.decodeString(jsonRoot, "nodeName", this.nodeID);
      this.nodeType = JourneyNodeType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "nodeType", true));
      this.outgoingConnectionPoints = decodeOutgoingConnectionPoints(JSONUtilities.decodeJSONArray(jsonRoot, "outgoingConnectionPoints", true));
    }

    /*****************************************
    *
    *  decodeOutgoingConnectionPoints
    *
    *****************************************/

    private List<OutgoingConnectionPoint> decodeOutgoingConnectionPoints(JSONArray jsonArray) throws GUIManagerException
    {
      List<OutgoingConnectionPoint> outgoingConnectionPoints = new ArrayList<OutgoingConnectionPoint>();
      for (int i=0; i<jsonArray.size(); i++)
        {
          JSONObject connectionPointJSON = (JSONObject) jsonArray.get(i);
          OutgoingConnectionPoint outgoingConnectionPoint = new OutgoingConnectionPoint(connectionPointJSON);
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

  private static class OutgoingConnectionPoint
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private JourneyLinkType type;
    private List<EvaluationCriterion> transitionCriteria;
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/
    
    public JourneyLinkType getType() { return type; }
    public List<EvaluationCriterion> getTransitionCriteria() { return transitionCriteria; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public OutgoingConnectionPoint(JSONObject jsonRoot) throws GUIManagerException
    {
      this.type = JourneyLinkType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "type", true));
      this.transitionCriteria = decodeTransitionCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "transitionCriteria", false));
    }

    /*****************************************
    *
    *  decodeTransitionCriteria
    *
    *****************************************/

    private List<EvaluationCriterion> decodeTransitionCriteria(JSONArray jsonArray) throws GUIManagerException
    {
      List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
      if (jsonArray != null)
        {
          for (int i=0; i<jsonArray.size(); i++)
            {
              result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.Profile));
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
    private int destinationConnectionPoint;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getSourceNodeID() { return sourceNodeID; }
    public int getSourceConnectionPoint() { return sourceConnectionPoint; }
    public String getDestinationNodeID() { return destinationNodeID; }
    public int getDestinationConnectionPoint() { return destinationConnectionPoint; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public GUILink(JSONObject jsonRoot) throws GUIManagerException
    {
      this.sourceNodeID = JSONUtilities.decodeString(jsonRoot, "sourceNodeID", true);
      this.sourceConnectionPoint = JSONUtilities.decodeInteger(jsonRoot, "sourceConnectionPoint", true);
      this.destinationNodeID = JSONUtilities.decodeString(jsonRoot, "destinationNodeID", true);
      this.destinationConnectionPoint = JSONUtilities.decodeInteger(jsonRoot, "destinationConnectionPoint", true);
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
        epochChanged = epochChanged || ! Objects.equals(autoTargetingCriteria, existingJourney.getAutoTargetingCriteria());
        epochChanged = epochChanged || ! Objects.equals(startNodeID, existingJourney.getStartNodeID());
        epochChanged = epochChanged || ! Objects.equals(journeyNodes, existingJourney.getJourneyNodes());
        epochChanged = epochChanged || ! Objects.equals(journeyLinks, existingJourney.getJourneyLinks());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
