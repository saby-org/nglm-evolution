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
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatusField;
import com.evolving.nglm.evolution.JourneyHistory.NodeHistory;
import com.evolving.nglm.evolution.JourneyHistory.RewardHistory;
import com.evolving.nglm.evolution.JourneyHistory.StatusHistory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
    schemaBuilder.field("sample", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("markNotified", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("markConverted", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("statusNotified", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("statusConverted", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("statusControlGroup", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("statusUniversalControlGroup", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("journeyComplete", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("journeyNodeHistory", SchemaBuilder.array(NodeHistory.schema()).schema());
    schemaBuilder.field("journeyStatusHistory", SchemaBuilder.array(StatusHistory.schema()).schema());
    schemaBuilder.field("journeyRewardHistory", SchemaBuilder.array(RewardHistory.schema()).schema());
    schemaBuilder.field("subscriberStratum", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).schema());
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
  private String sample;
  private boolean markNotified;
  private boolean markConverted;
  private boolean statusNotified;
  private boolean statusConverted;
  private boolean statusControlGroup;
  private boolean statusUniversalControlGroup;
  private boolean journeyComplete;
  private List<NodeHistory> journeyNodeHistory;
  private List<StatusHistory> journeyStatusHistory;
  private List<RewardHistory> journeyRewardHistory;
  private Map<String, String> subscriberStratum;

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
  public String getSample() { return sample; }
  public boolean getMarkNotified() { return markNotified; }
  public boolean getMarkConverted() { return markConverted; }
  public boolean getStatusNotified() { return statusNotified; }
  public boolean getStatusConverted() { return statusConverted; }
  public boolean getStatusControlGroup() { return statusControlGroup; }
  public boolean getStatusUniversalControlGroup() { return statusUniversalControlGroup; }
  public boolean getJourneyComplete() { return journeyComplete; }
  public Date getEventDate() { return transitionDate; }
  public List<NodeHistory> getJourneyNodeHistory() { return journeyNodeHistory; }
  public List<StatusHistory> getJourneyStatusHistory() { return journeyStatusHistory; }
  public List<RewardHistory> getJourneyRewardHistory() { return journeyRewardHistory; }
  public SubscriberJourneyStatus getSubscriberJourneyStatus() { return Journey.getSubscriberJourneyStatus(this); }
  public Map<String, String> getSubscriberStratum() { return subscriberStratum; }

  /*****************************************
  *
  *  constructor -- enter
  *
  *****************************************/

  public JourneyStatistic(EvolutionEventContext context, String subscriberID, JourneyHistory journeyHistory, JourneyState journeyState, Map<String, String> subscriberStratum)
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
    this.sample = null;
    this.markNotified = false;
    this.markConverted = false;
    this.statusNotified = false;
    this.statusConverted = false;
    this.statusControlGroup = false;
    this.statusUniversalControlGroup = false;
    this.journeyComplete = false;
    this.journeyNodeHistory = prepareJourneyNodeSummary(journeyHistory);
    this.journeyStatusHistory = prepareJourneyStatusSummary(journeyHistory);
    this.journeyRewardHistory = prepareJourneyRewardsSummary(journeyHistory);
    this.subscriberStratum = subscriberStratum;
  }

  /*****************************************
  *
  *  constructor -- transition
  *
  *****************************************/

  public JourneyStatistic(EvolutionEventContext context, String subscriberID, JourneyHistory journeyHistory, JourneyState journeyState, JourneyLink journeyLink, boolean markNotified, boolean markConverted, String sample, Map<String, String> subscriberStratum)
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
    this.sample = sample;
    this.markNotified = markNotified;
    this.markConverted = markConverted;
    this.statusNotified = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
    this.statusConverted = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
    this.statusControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    this.statusUniversalControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    this.journeyComplete = journeyLink.getDestination().getExitNode();
    this.journeyNodeHistory = prepareJourneyNodeSummary(journeyHistory);
    this.journeyStatusHistory = prepareJourneyStatusSummary(journeyHistory);
    this.journeyRewardHistory = prepareJourneyRewardsSummary(journeyHistory);
    this.subscriberStratum = subscriberStratum;
  }

  /*****************************************
  *
  *  constructor -- abnormal exit
  *
  *****************************************/

  public JourneyStatistic(EvolutionEventContext context, String subscriberID, JourneyHistory journeyHistory, JourneyState journeyState, Map<String, String> subscriberStratum, Date exitDate)
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
    this.statusNotified = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
    this.statusConverted = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
    this.statusControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    this.statusUniversalControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    this.journeyComplete = true;
    this.journeyNodeHistory = prepareJourneyNodeSummary(journeyHistory);
    this.journeyStatusHistory = prepareJourneyStatusSummary(journeyHistory);
    this.journeyRewardHistory = prepareJourneyRewardsSummary(journeyHistory);
    this.subscriberStratum = subscriberStratum;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private JourneyStatistic(String journeyStatisticID, String journeyInstanceID, String journeyID, String subscriberID, Date transitionDate, String linkID, String fromNodeID, String toNodeID, String deliveryRequestID, String sample, boolean markNotified, boolean markConverted, boolean statusNotified, boolean statusConverted, boolean statusControlGroup, boolean statusUniversalControlGroup, boolean journeyComplete, List<NodeHistory> journeyNodeHistory, List<StatusHistory> journeyStatusHistory, List<RewardHistory> journeyRewardHistory, Map<String, String> subscriberStratum)
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
    this.sample = sample;
    this.markNotified = markNotified;
    this.markConverted = markConverted;
    this.statusNotified = statusNotified;
    this.statusConverted = statusConverted;
    this.statusControlGroup = statusControlGroup;
    this.statusUniversalControlGroup = statusUniversalControlGroup;
    this.journeyComplete = journeyComplete;
    this.journeyNodeHistory = journeyNodeHistory;
    this.journeyStatusHistory = journeyStatusHistory;
    this.journeyRewardHistory = journeyRewardHistory;
    this.subscriberStratum = subscriberStratum;
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
    this.sample = journeyStatistic.getSample();
    this.markNotified = journeyStatistic.getMarkNotified();
    this.markConverted = journeyStatistic.getMarkConverted();
    this.statusNotified = journeyStatistic.getStatusNotified();
    this.statusConverted = journeyStatistic.getStatusConverted();
    this.statusControlGroup = journeyStatistic.getStatusControlGroup();
    this.statusUniversalControlGroup = journeyStatistic.getStatusUniversalControlGroup();
    this.journeyComplete = journeyStatistic.getJourneyComplete();
    
    this.journeyNodeHistory = new ArrayList<NodeHistory>();
    for(NodeHistory stat : journeyStatistic.getJourneyNodeHistory())
      {
        this.journeyNodeHistory.add(stat);
      }
    
    this.journeyStatusHistory = new ArrayList<StatusHistory>();
    for(StatusHistory status : journeyStatistic.getJourneyStatusHistory())
      {
        this.journeyStatusHistory.add(status);
      }
    
    this.journeyRewardHistory = new ArrayList<RewardHistory>();
    for(RewardHistory reward : journeyStatistic.getJourneyRewardHistory())
      {
        this.journeyRewardHistory.add(reward);
      }
    
    this.subscriberStratum = new HashMap<String, String>();
    for(String key: journeyStatistic.getSubscriberStratum().keySet())
      {
        this.subscriberStratum.put(key, journeyStatistic.getSubscriberStratum().get(key));
      }
  }
  
  public StatusHistory getPreviousJourneyStatus()
  {
    if(this.journeyStatusHistory == null) 
      {
        return null;
      }
    else if(this.journeyStatusHistory.size() <= 1)
      {
        return null;
      }
    else 
      {
        StatusHistory max = null;
        StatusHistory previous = null;
        for(StatusHistory status : this.journeyStatusHistory) 
          {
            if(max == null) 
              {
                max = status;
              } 
            else
              {
                if(status.getDate().compareTo(max.getDate()) > 0) 
                  {
                    previous = max;
                    max = status;
                  } 
                else if (previous == null)
                  {
                    previous = status;
                  }
                else if (status.getDate().compareTo(previous.getDate()) > 0)
                  {
                    previous = status;
                  }
              }
            
          }
        return previous;
      }
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
    struct.put("sample", journeyStatistic.getSample());
    struct.put("markNotified", journeyStatistic.getMarkNotified());
    struct.put("markConverted", journeyStatistic.getMarkConverted());
    struct.put("statusNotified", journeyStatistic.getStatusNotified());
    struct.put("statusConverted", journeyStatistic.getStatusConverted());
    struct.put("statusControlGroup", journeyStatistic.getStatusControlGroup());
    struct.put("statusUniversalControlGroup", journeyStatistic.getStatusUniversalControlGroup());
    struct.put("journeyComplete", journeyStatistic.getJourneyComplete());
    struct.put("journeyNodeHistory", packNodeHistory(journeyStatistic.getJourneyNodeHistory()));
    struct.put("journeyStatusHistory", packStatusHistory(journeyStatistic.getJourneyStatusHistory()));
    struct.put("journeyRewardHistory", packRewardHistory(journeyStatistic.getJourneyRewardHistory()));
    struct.put("subscriberStratum", journeyStatistic.getSubscriberStratum());
    return struct;
  }
  
  /*****************************************
  *
  *  packRewardHistory
  *
  *****************************************/

  private static List<Object> packRewardHistory(List<RewardHistory> rewardHistory)
  {
    List<Object> result = new ArrayList<Object>();
    for (RewardHistory reward : rewardHistory)
      {
        result.add(RewardHistory.pack(reward));
      }
    return result;
  }
  
  /*****************************************
  *
  *  packStatusHistory
  *
  *****************************************/

  private static List<Object> packStatusHistory(List<StatusHistory> statusHistory)
  {
    List<Object> result = new ArrayList<Object>();
    for (StatusHistory status : statusHistory)
      {
        result.add(StatusHistory.pack(status));
      }
    return result;
  }
  
  /*****************************************
  *
  *  packNodeHistory
  *
  *****************************************/

  private static List<Object> packNodeHistory(List<NodeHistory> nodeHistory)
  {
    List<Object> result = new ArrayList<Object>();
    for (NodeHistory node : nodeHistory)
      {
        result.add(NodeHistory.pack(node));
      }
    return result;
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
    String sample = valueStruct.getString("sample");
    boolean markNotified = (schemaVersion >= 2) ? valueStruct.getBoolean("markNotified") : false;
    boolean markConverted = (schemaVersion >= 2) ? valueStruct.getBoolean("markConverted") : false;
    boolean statusNotified = valueStruct.getBoolean("statusNotified");
    boolean statusConverted = valueStruct.getBoolean("statusConverted");
    boolean statusControlGroup = valueStruct.getBoolean("statusControlGroup");
    boolean statusUniversalControlGroup = valueStruct.getBoolean("statusUniversalControlGroup");
    boolean journeyComplete = valueStruct.getBoolean("journeyComplete");
    List<RewardHistory> journeyRewardHistory =  unpackRewardHistory(schema.field("journeyRewardHistory").schema(), valueStruct.get("journeyRewardHistory"));
    List<NodeHistory> journeyNodeHistory =  unpackNodeHistory(schema.field("journeyNodeHistory").schema(), valueStruct.get("journeyNodeHistory"));
    List<StatusHistory> journeyStatusHistory =  unpackStatusHistory(schema.field("journeyStatusHistory").schema(), valueStruct.get("journeyStatusHistory"));
    Map<String, String> subscriberStratum = (Map<String,String>) valueStruct.get("subscriberStratum");
    
    //
    //  return
    //

    return new JourneyStatistic(journeyStatisticID, journeyInstanceID, journeyID, subscriberID, transitionDate, linkID, fromNodeID, toNodeID, deliveryRequestID, sample, markNotified, markConverted, statusNotified, statusConverted, statusControlGroup, statusUniversalControlGroup, journeyComplete, journeyNodeHistory, journeyStatusHistory, journeyRewardHistory, subscriberStratum);
  }
  
  /*****************************************
  *
  *  unpackStatusHistory
  *
  *****************************************/

  private static List<StatusHistory> unpackStatusHistory(Schema schema, Object value)
  {
    //
    //  get schema for StatusHistory
    //

    Schema statusHistorySchema = schema.valueSchema();

    //
    //  unpack
    //

    List<StatusHistory> result = new ArrayList<StatusHistory>();
    List<Object> valueArray = (List<Object>) value;
    for (Object status : valueArray)
      {
        result.add(StatusHistory.unpack(new SchemaAndValue(statusHistorySchema, status)));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  unpackRewardHistory
  *
  *****************************************/

  private static List<RewardHistory> unpackRewardHistory(Schema schema, Object value)
  {
    //
    //  get schema for RewardHistory
    //

    Schema rewardHistorySchema = schema.valueSchema();

    //
    //  unpack
    //

    List<RewardHistory> result = new ArrayList<RewardHistory>();
    List<Object> valueArray = (List<Object>) value;
    for (Object reward : valueArray)
      {
        result.add(RewardHistory.unpack(new SchemaAndValue(rewardHistorySchema, reward)));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  unpackNodeHistory
  *
  *****************************************/

  private static List<NodeHistory> unpackNodeHistory(Schema schema, Object value)
  {
    //
    //  get schema for NodeHistory
    //

    Schema nodeHistorySchema = schema.valueSchema();

    //
    //  unpack
    //

    List<NodeHistory> result = new ArrayList<NodeHistory>();
    List<Object> valueArray = (List<Object>) value;
    for (Object node : valueArray)
      {
        result.add(NodeHistory.unpack(new SchemaAndValue(nodeHistorySchema, node)));
      }

    //
    //  return
    //

    return result;
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
  
  /*****************************************
  *
  *  prepareJourneyNodeSummary
  *
  *****************************************/
  
  private List<NodeHistory> prepareJourneyNodeSummary(JourneyHistory journeyHistory)
  {
    List<NodeHistory> result = new ArrayList<NodeHistory>();
    if(journeyHistory != null) 
      {
        if(journeyHistory.getJourneyID().equals(journeyID)) 
          {
            for(NodeHistory stat : journeyHistory.getNodeHistory()) 
              {
                result.add(stat);
              }
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  prepareJourneyStatusSummary
  *
  *****************************************/
  
  private List<StatusHistory> prepareJourneyStatusSummary(JourneyHistory journeyHistory)
  {
    List<StatusHistory> result = new ArrayList<StatusHistory>();
    if(journeyHistory != null) 
      {
        if(journeyHistory.getJourneyID().equals(journeyID))
          {
            for(StatusHistory status : journeyHistory.getStatusHistory()) 
              {
                result.add(status);
              }
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  prepareJourneyRewardsSummary
  *
  *****************************************/
  
  private List<RewardHistory> prepareJourneyRewardsSummary(JourneyHistory journeyHistory)
  {
    List<RewardHistory> result = new ArrayList<RewardHistory>();
    if(journeyHistory != null) 
      {
        if(journeyHistory.getJourneyID().equals(journeyID)) 
          {
            for(RewardHistory rewardHistory : journeyHistory.getRewardHistory()) 
              {
                result.add(rewardHistory);
              }
          }        
      }
    return result;
  }
}
