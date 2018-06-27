/*****************************************************************************
*
*  JourneyNode.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.JourneyNodeType;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    schemaBuilder.field("nodeType", Schema.STRING_SCHEMA);
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
  private JourneyNodeType nodeType;
  private List<String> incomingLinkReferences;
  private List<String> outgoingLinkReferences;

  //
  //  derived
  //

  private Map<String,JourneyLink> incomingLinks = new HashMap<String,JourneyLink>();
  private Map<String,JourneyLink> outgoingLinks = new HashMap<String,JourneyLink>();

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getNodeID() { return nodeID; }
  public String getNodeName() { return nodeName; }
  public JourneyNodeType getNodeType() { return nodeType; }
  public List<String> getIncomingLinkReferences() { return incomingLinkReferences; }
  public List<String> getOutgoingLinkReferences() { return outgoingLinkReferences; }
  public Map<String,JourneyLink> getIncomingLinks() { return incomingLinks; }
  public Map<String,JourneyLink> getOutgoingLinks() { return outgoingLinks; }

  /*****************************************
  *
  *  constructor -- standard/unpack
  *
  *****************************************/

  public JourneyNode(String nodeID, String nodeName, JourneyNodeType nodeType, List<String> incomingLinkReferences, List<String> outgoingLinkReferences)
  {
    this.nodeID = nodeID;
    this.nodeName = nodeName;
    this.nodeType = nodeType;
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
    struct.put("nodeType", journeyNode.getNodeType().getExternalRepresentation());
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
    JourneyNodeType nodeType = JourneyNodeType.fromExternalRepresentation(valueStruct.getString("nodeType"));
    List<String> incomingLinkReferences = (List<String>) valueStruct.get("incomingLinkReferences");
    List<String> outgoingLinkReferences = (List<String>) valueStruct.get("outgoingLinkReferences");

    //
    //  return
    //

    return new JourneyNode(nodeID, nodeName, nodeType, incomingLinkReferences, outgoingLinkReferences);
  }
}
