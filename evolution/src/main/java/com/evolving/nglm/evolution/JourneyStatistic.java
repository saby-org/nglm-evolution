/*****************************************************************************
*
*  JourneyStatistic.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.evolution.Journey.JourneyStatus;
import com.evolving.nglm.evolution.Journey.JourneyStatusField;

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

public class JourneyStatistic implements SubscriberStreamEvent, SubscriberStreamOutput
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
    schemaBuilder.field("deliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("statusNotified", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("statusConverted", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("statusControlGroup", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("statusUniversalControlGroup", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("journeyComplete", Schema.BOOLEAN_SCHEMA);
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
  public Schema subscriberStreamEventSchema() { return schema(); }

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
  private String deliveryRequestID;
  private boolean statusNotified;
  private boolean statusConverted;
  private boolean statusControlGroup;
  private boolean statusUniversalControlGroup;
  private boolean journeyComplete;

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
  public String getDeliveryRequestID() { return deliveryRequestID; }
  public boolean getStatusNotified() { return statusNotified; }
  public boolean getStatusConverted() { return statusConverted; }
  public boolean getStatusControlGroup() { return statusControlGroup; }
  public boolean getStatusUniversalControlGroup() { return statusUniversalControlGroup; }
  public boolean getJourneyComplete() { return journeyComplete; }
  public Date getEventDate() { return transitionDate; }

  //
  //  getJourneyStatus
  //

  public JourneyStatus getJourneyStatus()
  {
    if (journeyComplete && statusConverted)
      return JourneyStatus.Converted;
    else if (journeyComplete && ! statusConverted)
      return JourneyStatus.NotConverted;
    else if (statusNotified)
      return JourneyStatus.Notified;
    else
      return JourneyStatus.Eligible;
  }

  /*****************************************
  *
  *  constructor -- enter
  *
  *****************************************/

  public JourneyStatistic(String subscriberID, JourneyState journeyState)
  {
    this.journeyInstanceID = journeyState.getJourneyInstanceID();
    this.journeyID = journeyState.getJourneyID();
    this.subscriberID = subscriberID;
    this.transitionDate = journeyState.getJourneyNodeEntryDate();
    this.linkID = null;
    this.fromNodeID = null;
    this.toNodeID = journeyState.getJourneyNodeID();
    this.deliveryRequestID = null;
    this.statusNotified = false;
    this.statusConverted = false;
    this.statusControlGroup = false;
    this.statusUniversalControlGroup = false;
    this.journeyComplete = false;
  }

  /*****************************************
  *
  *  constructor -- transition
  *
  *****************************************/

  public JourneyStatistic(String subscriberID, JourneyState journeyState, JourneyLink journeyLink)
  {
    this.journeyInstanceID = journeyState.getJourneyInstanceID();
    this.journeyID = journeyState.getJourneyID();
    this.subscriberID = subscriberID;
    this.transitionDate = journeyState.getJourneyNodeEntryDate();
    this.linkID = journeyLink.getLinkID();
    this.fromNodeID = journeyLink.getSourceReference();
    this.toNodeID = journeyLink.getDestinationReference();
    this.deliveryRequestID = journeyState.getJourneyOutstandingDeliveryRequestID();
    this.statusNotified = journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusNotified.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
    this.statusConverted = journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusConverted.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
    this.statusControlGroup = journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusControlGroup.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    this.statusUniversalControlGroup = journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    this.journeyComplete = journeyLink.getDestination().getExitNode();
  }

  /*****************************************
  *
  *  constructor -- abnormal exit
  *
  *****************************************/

  public JourneyStatistic(String subscriberID, JourneyState journeyState, Date exitDate)
  {
    this.journeyInstanceID = journeyState.getJourneyInstanceID();
    this.journeyID = journeyState.getJourneyID();
    this.subscriberID = subscriberID;
    this.transitionDate = exitDate;
    this.linkID = null;
    this.fromNodeID = journeyState.getJourneyNodeID();
    this.toNodeID = null;
    this.deliveryRequestID = null;
    this.statusNotified = journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusNotified.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
    this.statusConverted = journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusConverted.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
    this.statusControlGroup = journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusControlGroup.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    this.statusUniversalControlGroup = journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(JourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    this.journeyComplete = true;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private JourneyStatistic(String journeyInstanceID, String journeyID, String subscriberID, Date transitionDate, String linkID, String fromNodeID, String toNodeID, String deliveryRequestID, boolean statusNotified, boolean statusConverted, boolean statusControlGroup, boolean statusUniversalControlGroup, boolean journeyComplete)
  {
    this.journeyInstanceID = journeyInstanceID;
    this.journeyID = journeyID;
    this.subscriberID = subscriberID;
    this.transitionDate = transitionDate;
    this.linkID = linkID;
    this.fromNodeID = fromNodeID;
    this.toNodeID = toNodeID;
    this.deliveryRequestID = deliveryRequestID;
    this.statusNotified = statusNotified;
    this.statusConverted = statusConverted;
    this.statusControlGroup = statusControlGroup;
    this.statusUniversalControlGroup = statusUniversalControlGroup;
    this.journeyComplete = journeyComplete;
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
    this.deliveryRequestID = journeyStatistic.getDeliveryRequestID();
    this.statusNotified = journeyStatistic.getStatusNotified();
    this.statusConverted = journeyStatistic.getStatusConverted();
    this.statusControlGroup = journeyStatistic.getStatusControlGroup();
    this.statusUniversalControlGroup = journeyStatistic.getStatusUniversalControlGroup();
    this.journeyComplete = journeyStatistic.getJourneyComplete();
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
    struct.put("deliveryRequestID", journeyStatistic.getDeliveryRequestID());
    struct.put("statusNotified", journeyStatistic.getStatusNotified());
    struct.put("statusConverted", journeyStatistic.getStatusConverted());
    struct.put("statusControlGroup", journeyStatistic.getStatusControlGroup());
    struct.put("statusUniversalControlGroup", journeyStatistic.getStatusUniversalControlGroup());
    struct.put("journeyComplete", journeyStatistic.getJourneyComplete());
    return struct;
  }
  
  //
  //  subscriberStreamEventPack
  //

  public Object subscriberStreamEventPack(Object value) { return pack(value); }

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
    String deliveryRequestID = valueStruct.getString("deliveryRequestID");
    boolean statusNotified = valueStruct.getBoolean("statusNotified");
    boolean statusConverted = valueStruct.getBoolean("statusConverted");
    boolean statusControlGroup = valueStruct.getBoolean("statusControlGroup");
    boolean statusUniversalControlGroup = valueStruct.getBoolean("statusUniversalControlGroup");
    boolean journeyComplete = valueStruct.getBoolean("journeyComplete");
    
    //
    //  return
    //

    return new JourneyStatistic(journeyInstanceID, journeyID, subscriberID, transitionDate, linkID, fromNodeID, toNodeID, deliveryRequestID, statusNotified, statusConverted, statusControlGroup, statusUniversalControlGroup, journeyComplete);
  }
}
