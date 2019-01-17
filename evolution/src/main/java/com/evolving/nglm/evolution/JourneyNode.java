/*****************************************************************************
*
*  JourneyNode.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("nodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("nodeName", Schema.STRING_SCHEMA);
    schemaBuilder.field("nodeTypeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("nodeParameters", ParameterMap.schema());
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
  public List<String> getIncomingLinkReferences() { return incomingLinkReferences; }
  public List<String> getOutgoingLinkReferences() { return outgoingLinkReferences; }
  public Map<String,JourneyLink> getIncomingLinks() { return incomingLinks; }
  public Map<String,JourneyLink> getOutgoingLinks() { return outgoingLinks; }
  public boolean getExitNode() { return outgoingLinks.size() == 0; }

  /*****************************************
  *
  *  constructor -- standard/unpack
  *
  *****************************************/

  public JourneyNode(String nodeID, String nodeName, NodeType nodeType, ParameterMap nodeParameters, List<String> incomingLinkReferences, List<String> outgoingLinkReferences)
  {
    this.nodeID = nodeID;
    this.nodeName = nodeName;
    this.nodeType = nodeType;
    this.nodeParameters = nodeParameters;
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
    struct.put("incomingLinkReferences", journeyNode.getIncomingLinkReferences());
    struct.put("outgoingLinkReferences", journeyNode.getOutgoingLinkReferences());
    return struct;
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
    List<String> incomingLinkReferences = (List<String>) valueStruct.get("incomingLinkReferences");
    List<String> outgoingLinkReferences = (List<String>) valueStruct.get("outgoingLinkReferences");

    //
    //  return
    //

    return new JourneyNode(nodeID, nodeName, nodeType, nodeParameters, incomingLinkReferences, outgoingLinkReferences);
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
