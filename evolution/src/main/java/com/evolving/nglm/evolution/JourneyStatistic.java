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
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatusField;

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

public class JourneyStatistic implements SubscriberStreamEvent, SubscriberStreamOutput, Comparable
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("journeyStatisticID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyInstanceID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("transitionDate", Timestamp.SCHEMA);
    schemaBuilder.field("linkID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("fromNodeID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("toNodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("markNotified", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("markConverted", Schema.BOOLEAN_SCHEMA);
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

  private String journeyStatisticID;
  private String journeyInstanceID;
  private String journeyID;
  private String subscriberID;
  private Date transitionDate;
  private String linkID;
  private String fromNodeID;
  private String toNodeID;
  private String deliveryRequestID;
  private boolean markNotified;
  private boolean markConverted;
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

  public String getJourneyStatisticID() { return journeyStatisticID; }
  public String getJourneyInstanceID() { return journeyInstanceID; }
  public String getJourneyID() { return journeyID; }
  public String getSubscriberID() { return subscriberID; }
  public Date getTransitionDate() { return transitionDate; }
  public String getLinkID() { return linkID; }
  public String getFromNodeID() { return fromNodeID; }
  public String getToNodeID() { return toNodeID; }
  public String getDeliveryRequestID() { return deliveryRequestID; }
  public boolean getMarkNotified() { return markNotified; }
  public boolean getMarkConverted() { return markConverted; }
  public boolean getStatusNotified() { return statusNotified; }
  public boolean getStatusConverted() { return statusConverted; }
  public boolean getStatusControlGroup() { return statusControlGroup; }
  public boolean getStatusUniversalControlGroup() { return statusUniversalControlGroup; }
  public boolean getJourneyComplete() { return journeyComplete; }
  public Date getEventDate() { return transitionDate; }

  //
  //  getSubscriberJourneyStatus
  //

  public SubscriberJourneyStatus getSubscriberJourneyStatus()
  {
    if (journeyComplete && statusConverted)
      return SubscriberJourneyStatus.Converted;
    else if (journeyComplete && ! statusConverted)
      return SubscriberJourneyStatus.NotConverted;
    else if (statusNotified)
      return SubscriberJourneyStatus.Notified;
    else
      return SubscriberJourneyStatus.Eligible;
  }

  /*****************************************
  *
  *  constructor -- enter
  *
  *****************************************/

  public JourneyStatistic(EvolutionEventContext context, String subscriberID, JourneyState journeyState)
  {
    this.journeyStatisticID = context.getUniqueKey();
    this.journeyInstanceID = journeyState.getJourneyInstanceID();
    this.journeyID = journeyState.getJourneyID();
    this.subscriberID = subscriberID;
    this.transitionDate = journeyState.getJourneyNodeEntryDate();
    this.linkID = null;
    this.fromNodeID = null;
    this.toNodeID = journeyState.getJourneyNodeID();
    this.deliveryRequestID = null;
    this.markNotified = false;
    this.markConverted = false;
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

  public JourneyStatistic(EvolutionEventContext context, String subscriberID, JourneyState journeyState, JourneyLink journeyLink, boolean markNotified, boolean markConverted)
  {
    this.journeyStatisticID = context.getUniqueKey();
    this.journeyInstanceID = journeyState.getJourneyInstanceID();
    this.journeyID = journeyState.getJourneyID();
    this.subscriberID = subscriberID;
    this.transitionDate = journeyState.getJourneyNodeEntryDate();
    this.linkID = journeyLink.getLinkID();
    this.fromNodeID = journeyLink.getSourceReference();
    this.toNodeID = journeyLink.getDestinationReference();
    this.deliveryRequestID = journeyState.getJourneyOutstandingDeliveryRequestID();
    this.markNotified = markNotified;
    this.markConverted = markConverted;
    this.statusNotified = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
    this.statusConverted = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
    this.statusControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    this.statusUniversalControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    this.journeyComplete = journeyLink.getDestination().getExitNode();
  }

  /*****************************************
  *
  *  constructor -- abnormal exit
  *
  *****************************************/

  public JourneyStatistic(EvolutionEventContext context, String subscriberID, JourneyState journeyState, Date exitDate)
  {
    this.journeyStatisticID = context.getUniqueKey();
    this.journeyInstanceID = journeyState.getJourneyInstanceID();
    this.journeyID = journeyState.getJourneyID();
    this.subscriberID = subscriberID;
    this.transitionDate = exitDate;
    this.linkID = null;
    this.fromNodeID = journeyState.getJourneyNodeID();
    this.toNodeID = journeyState.getJourneyNodeID();
    this.deliveryRequestID = null;
    this.markNotified = false;
    this.markConverted = false;
    this.statusNotified = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
    this.statusConverted = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
    this.statusControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    this.statusUniversalControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    this.journeyComplete = true;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private JourneyStatistic(String journeyStatisticID, String journeyInstanceID, String journeyID, String subscriberID, Date transitionDate, String linkID, String fromNodeID, String toNodeID, String deliveryRequestID, boolean markNotified, boolean markConverted, boolean statusNotified, boolean statusConverted, boolean statusControlGroup, boolean statusUniversalControlGroup, boolean journeyComplete)
  {
    this.journeyStatisticID = journeyStatisticID;
    this.journeyInstanceID = journeyInstanceID;
    this.journeyID = journeyID;
    this.subscriberID = subscriberID;
    this.transitionDate = transitionDate;
    this.linkID = linkID;
    this.fromNodeID = fromNodeID;
    this.toNodeID = toNodeID;
    this.deliveryRequestID = deliveryRequestID;
    this.markNotified = markNotified;
    this.markConverted = markConverted;
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
    this.journeyStatisticID = journeyStatistic.getJourneyStatisticID();
    this.journeyInstanceID = journeyStatistic.getJourneyInstanceID();
    this.journeyID = journeyStatistic.getJourneyID();
    this.subscriberID = journeyStatistic.getSubscriberID();
    this.transitionDate = journeyStatistic.getTransitionDate();
    this.linkID = journeyStatistic.getLinkID();
    this.fromNodeID = journeyStatistic.getFromNodeID();
    this.toNodeID = journeyStatistic.getToNodeID();
    this.deliveryRequestID = journeyStatistic.getDeliveryRequestID();
    this.markNotified = journeyStatistic.getMarkNotified();
    this.markConverted = journeyStatistic.getMarkConverted();
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
    struct.put("journeyStatisticID", journeyStatistic.getJourneyStatisticID());
    struct.put("journeyInstanceID", journeyStatistic.getJourneyInstanceID());
    struct.put("journeyID", journeyStatistic.getJourneyID());
    struct.put("subscriberID", journeyStatistic.getSubscriberID());
    struct.put("transitionDate", journeyStatistic.getTransitionDate());
    struct.put("linkID", journeyStatistic.getLinkID());
    struct.put("fromNodeID", journeyStatistic.getFromNodeID());
    struct.put("toNodeID", journeyStatistic.getToNodeID());
    struct.put("deliveryRequestID", journeyStatistic.getDeliveryRequestID());
    struct.put("markNotified", journeyStatistic.getMarkNotified());
    struct.put("markConverted", journeyStatistic.getMarkConverted());
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
    String journeyStatisticID = valueStruct.getString("journeyStatisticID");
    String journeyInstanceID = valueStruct.getString("journeyInstanceID");
    String journeyID = valueStruct.getString("journeyID");
    String subscriberID = valueStruct.getString("subscriberID");
    Date transitionDate = (Date) valueStruct.get("transitionDate");
    String linkID = valueStruct.getString("linkID");
    String fromNodeID = valueStruct.getString("fromNodeID");
    String toNodeID = valueStruct.getString("toNodeID");
    String deliveryRequestID = valueStruct.getString("deliveryRequestID");
    boolean markNotified = (schemaVersion >= 2) ? valueStruct.getBoolean("markNotified") : false;
    boolean markConverted = (schemaVersion >= 2) ? valueStruct.getBoolean("markConverted") : false;
    boolean statusNotified = valueStruct.getBoolean("statusNotified");
    boolean statusConverted = valueStruct.getBoolean("statusConverted");
    boolean statusControlGroup = valueStruct.getBoolean("statusControlGroup");
    boolean statusUniversalControlGroup = valueStruct.getBoolean("statusUniversalControlGroup");
    boolean journeyComplete = valueStruct.getBoolean("journeyComplete");
    
    //
    //  return
    //

    return new JourneyStatistic(journeyStatisticID, journeyInstanceID, journeyID, subscriberID, transitionDate, linkID, fromNodeID, toNodeID, deliveryRequestID, markNotified, markConverted, statusNotified, statusConverted, statusControlGroup, statusUniversalControlGroup, journeyComplete);
  }

  /*****************************************
  *
  *  compareTo
  *
  *****************************************/

  public int compareTo(Object obj)
  {
    int result = -1;
    if (obj instanceof JourneyStatistic)
      {
        JourneyStatistic entry = (JourneyStatistic) obj;
        result = transitionDate.compareTo(entry.getTransitionDate());
        if (result == 0) result = journeyStatisticID.compareTo(entry.getJourneyStatisticID());
      }
    return result;
  }
}
