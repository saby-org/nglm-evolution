/*****************************************************************************
*
*  JourneyNode.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

public class JourneyNode
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
    schemaBuilder.name("journey_node");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("nodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("nodeName", Schema.STRING_SCHEMA);
    schemaBuilder.field("nodeTypeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("nodeParameters", ParameterMap.schema());
    schemaBuilder.field("evaluateContextVariables", SchemaBuilder.bool().defaultValue(false).schema());
    schemaBuilder.field("contextVariables", SchemaBuilder.array(ContextVariable.schema()));
    schemaBuilder.field("incomingLinkReferences", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("outgoingLinkReferences", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyNode> serde = new ConnectSerde<JourneyNode>(schema, false, JourneyNode.class, JourneyNode::pack, JourneyNode::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyNode> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  basic
  //

  private String nodeID;
  private String nodeName;
  private NodeType nodeType;
  private ParameterMap nodeParameters;
  private boolean evaluateContextVariables;
  private List<ContextVariable> contextVariables;
  private List<String> incomingLinkReferences;
  private List<String> outgoingLinkReferences;

  //
  //  derived
  //

  private Map<String,JourneyLink> incomingLinks = new LinkedHashMap<String,JourneyLink>();
  private Map<String,JourneyLink> outgoingLinks = new LinkedHashMap<String,JourneyLink>();

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getNodeID() { return nodeID; }
  public String getNodeName() { return nodeName; }
  public NodeType getNodeType() { return nodeType; }
  public ParameterMap getNodeParameters() { return nodeParameters; }
  public boolean getEvaluateContextVariables() { return evaluateContextVariables; }
  public List<ContextVariable> getContextVariables() { return contextVariables; }
  public List<String> getIncomingLinkReferences() { return incomingLinkReferences; }
  public List<String> getOutgoingLinkReferences() { return outgoingLinkReferences; }
  public Map<String,JourneyLink> getIncomingLinks() { return incomingLinks; }
  public Map<String,JourneyLink> getOutgoingLinks() { return outgoingLinks; }
  public boolean getExitNode() { return outgoingLinks.size() == 0; }

  //
  //  setters
  //

  public void setEvaluateContextVariables(boolean evaluateContextVariables) { this.evaluateContextVariables = evaluateContextVariables; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public JourneyNode(String nodeID, String nodeName, NodeType nodeType, ParameterMap nodeParameters, List<ContextVariable> contextVariables, List<String> incomingLinkReferences, List<String> outgoingLinkReferences)
  {
    this.nodeID = nodeID;
    this.nodeName = nodeName;
    this.nodeType = nodeType;
    this.nodeParameters = nodeParameters;
    this.evaluateContextVariables = false;
    this.incomingLinkReferences = incomingLinkReferences;
    this.outgoingLinkReferences = outgoingLinkReferences;

    //
    //  filter context variables to include local (and exclude parameter declarations)
    //

    this.contextVariables = new ArrayList<ContextVariable>();
    for (ContextVariable contextVariable : contextVariables)
      {
        switch (contextVariable.getVariableType())
          {
            case Local:
            case JourneyResult:

              //
              //  include (allows runtime evaluation)
              //

              this.contextVariables.add(contextVariable);
              break;

            case Parameter:

              //
              //  do NOT include (prevents runtime evaluation)
              //

              break;
          }
      }
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private JourneyNode(String nodeID, String nodeName, NodeType nodeType, ParameterMap nodeParameters, boolean evaluateContextVariables, List<ContextVariable> contextVariables, List<String> incomingLinkReferences, List<String> outgoingLinkReferences)
  {
    this.nodeID = nodeID;
    this.nodeName = nodeName;
    this.nodeType = nodeType;
    this.nodeParameters = nodeParameters;
    this.evaluateContextVariables = evaluateContextVariables;
    this.contextVariables = contextVariables;
    this.incomingLinkReferences = incomingLinkReferences;
    this.outgoingLinkReferences = outgoingLinkReferences;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyNode journeyNode = (JourneyNode) value;
    Struct struct = new Struct(schema);
    struct.put("nodeID", journeyNode.getNodeID());
    struct.put("nodeName", journeyNode.getNodeName());
    struct.put("nodeTypeID", journeyNode.getNodeType().getID());
    struct.put("nodeParameters", ParameterMap.pack(journeyNode.getNodeParameters()));
    struct.put("evaluateContextVariables", journeyNode.getEvaluateContextVariables());
    struct.put("contextVariables", packContextVariables(journeyNode.getContextVariables()));
    struct.put("incomingLinkReferences", journeyNode.getIncomingLinkReferences());
    struct.put("outgoingLinkReferences", journeyNode.getOutgoingLinkReferences());
    return struct;
  }

  /****************************************
  *
  *  packContextVariables
  *
  ****************************************/

  private static List<Object> packContextVariables(List<ContextVariable> contextVariables)
  {
    List<Object> result = new ArrayList<Object>();
    for (ContextVariable contextVariable : contextVariables)
      {
        result.add(ContextVariable.pack(contextVariable));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static JourneyNode unpack(SchemaAndValue schemaAndValue)
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
    String nodeID = valueStruct.getString("nodeID");
    String nodeName = valueStruct.getString("nodeName");
    NodeType nodeType = Deployment.getNodeTypes().get(valueStruct.getString("nodeTypeID"));
    ParameterMap nodeParameters = ParameterMap.unpack(new SchemaAndValue(schema.field("nodeParameters").schema(), valueStruct.get("nodeParameters")));
    boolean evaluateContextVariables = (schemaVersion >= 2) ? valueStruct.getBoolean("evaluateContextVariables") : false;
    List<ContextVariable> contextVariables = unpackContextVariables(schema.field("contextVariables").schema(), valueStruct.get("contextVariables"));
    List<String> incomingLinkReferences = (List<String>) valueStruct.get("incomingLinkReferences");
    List<String> outgoingLinkReferences = (List<String>) valueStruct.get("outgoingLinkReferences");

    //
    //  return
    //

    return new JourneyNode(nodeID, nodeName, nodeType, nodeParameters, evaluateContextVariables, contextVariables, incomingLinkReferences, outgoingLinkReferences);
  }
  
  /*****************************************
  *
  *  unpackContextVariables
  *
  *****************************************/

  private static List<ContextVariable> unpackContextVariables(Schema schema, Object value)
  {
    //
    //  get schema for ContextVariable
    //

    Schema contextVariableSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<ContextVariable> result = new ArrayList<ContextVariable>();
    List<Object> valueArray = (List<Object>) value;
    for (Object contextVariable : valueArray)
      {
        result.add(ContextVariable.unpack(new SchemaAndValue(contextVariableSchema, contextVariable)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  detectCycle
  *
  *****************************************/

  boolean detectCycle(Set<JourneyNode> visitedNodes, LinkedList<JourneyNode> walkNodes)
  {
    //
    //  visited?
    //

    if (visitedNodes.contains(this)) return false;
    
    //
    //  cycle found?
    //

    boolean cycleDetected = walkNodes.contains(this);

    //
    //  add to path
    //

    walkNodes.add(this);
    
    //
    //  walk children
    //

    if (! this.getNodeType().getEnableCycle())
      {
        for (JourneyLink journeyLink : outgoingLinks.values())
          {
            cycleDetected = cycleDetected || journeyLink.getDestination().detectCycle(visitedNodes, walkNodes);
          }
      }

    //
    //  update path, visited
    //

    if (! cycleDetected) walkNodes.remove(this);
    visitedNodes.add(this);

    //
    //  return
    //

    return cycleDetected;
  }
}
