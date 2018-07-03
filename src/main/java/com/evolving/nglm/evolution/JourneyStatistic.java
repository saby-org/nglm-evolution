/*****************************************************************************
*
*  JourneyStatistic.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;

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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JourneyStatistic implements SubscriberStreamOutput
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
    schemaBuilder.name("journey_statistic");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("journeyInstanceID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("transitionDate", Timestamp.SCHEMA);
    schemaBuilder.field("linkID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("fromNodeID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("toNodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("exited", Schema.BOOLEAN_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyStatistic> serde = new ConnectSerde<JourneyStatistic>(schema, false, JourneyStatistic.class, JourneyStatistic::pack, JourneyStatistic::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyStatistic> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String journeyInstanceID;
  private String journeyID;
  private String subscriberID;
  private Date transitionDate;
  private String linkID;
  private String fromNodeID;
  private String toNodeID;
  private boolean exited;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getJourneyInstanceID() { return journeyInstanceID; }
  public String getJourneyID() { return journeyID; }
  public String getSubscriberID() { return subscriberID; }
  public Date getTransitionDate() { return transitionDate; }
  public String getLinkID() { return linkID; }
  public String getFromNodeID() { return fromNodeID; }
  public String getToNodeID() { return toNodeID; }
  public boolean getExited() { return exited; }

  /*****************************************
  *
  *  constructor -- standard/unpack
  *
  *****************************************/

  public JourneyStatistic(String journeyInstanceID, String journeyID, String subscriberID, Date transitionDate, String linkID, String fromNodeID, String toNodeID, boolean exited)
  {
    this.journeyInstanceID = journeyInstanceID;
    this.journeyID = journeyID;
    this.subscriberID = subscriberID;
    this.transitionDate = transitionDate;
    this.linkID = linkID;
    this.fromNodeID = fromNodeID;
    this.toNodeID = toNodeID;
    this.exited = exited;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public JourneyStatistic(JourneyStatistic journeyStatistic)
  {
    this.journeyInstanceID = journeyStatistic.getJourneyInstanceID();
    this.journeyID = journeyStatistic.getJourneyID();
    this.subscriberID = journeyStatistic.getSubscriberID();
    this.transitionDate = journeyStatistic.getTransitionDate();
    this.linkID = journeyStatistic.getLinkID();
    this.fromNodeID = journeyStatistic.getFromNodeID();
    this.toNodeID = journeyStatistic.getToNodeID();
    this.exited = journeyStatistic.getExited();
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyStatistic journeyStatistic = (JourneyStatistic) value;
    Struct struct = new Struct(schema);
    struct.put("journeyInstanceID", journeyStatistic.getJourneyInstanceID());
    struct.put("journeyID", journeyStatistic.getJourneyID());
    struct.put("subscriberID", journeyStatistic.getSubscriberID());
    struct.put("transitionDate", journeyStatistic.getTransitionDate());
    struct.put("linkID", journeyStatistic.getLinkID());
    struct.put("fromNodeID", journeyStatistic.getFromNodeID());
    struct.put("toNodeID", journeyStatistic.getToNodeID());
    struct.put("exited", journeyStatistic.getExited());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static JourneyStatistic unpack(SchemaAndValue schemaAndValue)
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
    String journeyInstanceID = valueStruct.getString("journeyInstanceID");
    String journeyID = valueStruct.getString("journeyID");
    String subscriberID = valueStruct.getString("subscriberID");
    Date transitionDate = (Date) valueStruct.get("transitionDate");
    String linkID = valueStruct.getString("linkID");
    String fromNodeID = valueStruct.getString("fromNodeID");
    String toNodeID = valueStruct.getString("toNodeID");
    boolean exited = valueStruct.getBoolean("exited");
    
    //
    //  return
    //

    return new JourneyStatistic(journeyInstanceID, journeyID, subscriberID, transitionDate, linkID, fromNodeID, toNodeID, exited);
  }
}
