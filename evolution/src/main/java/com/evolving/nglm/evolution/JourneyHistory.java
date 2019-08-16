package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryRequest;
import com.evolving.nglm.evolution.EmptyFulfillmentManager.EmptyFulfillmentRequest;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatusField;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;

public class JourneyHistory 
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
    schemaBuilder.name("journey_history");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("nodeHistory", SchemaBuilder.array(NodeHistory.schema()).schema());
    schemaBuilder.field("statusHistory", SchemaBuilder.array(StatusHistory.schema()).schema());
    schemaBuilder.field("rewardHistory", SchemaBuilder.array(RewardHistory.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyHistory> serde = new ConnectSerde<JourneyHistory>(schema, false, JourneyHistory.class, JourneyHistory::pack, JourneyHistory::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyHistory> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/
  private String journeyID;
  private List<NodeHistory> nodeHistory;
  private List<StatusHistory> statusHistory;
  private List<RewardHistory> rewardHistory;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public String getJourneyID() { return journeyID; }
  public List<NodeHistory> getNodeHistory() { return nodeHistory; }
  public List<StatusHistory> getStatusHistory() { return statusHistory; }
  public List<RewardHistory> getRewardHistory() { return rewardHistory; }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyHistory journeyHistory = (JourneyHistory) value;
    Struct struct = new Struct(schema);
    struct.put("journeyID", journeyHistory.getJourneyID());
    struct.put("nodeHistory", packNodeHistory(journeyHistory.getNodeHistory()));
    struct.put("statusHistory", packStatusHistory(journeyHistory.getStatusHistory()));
    struct.put("rewardHistory", packRewardHistory(journeyHistory.getRewardHistory()));
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
  *  constructor -- external
  *
  *****************************************/

  public JourneyHistory(String journeyID)
  {
    this.journeyID = journeyID;
    this.nodeHistory = new ArrayList<NodeHistory>();
    this.statusHistory = new ArrayList<StatusHistory>();
    this.rewardHistory = new ArrayList<RewardHistory>();
  }
  
  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/
  
  public JourneyHistory(JourneyHistory journeyHistory)
  {
    this.journeyID = journeyHistory.getJourneyID();
    
    this.nodeHistory = new ArrayList<NodeHistory>();
    for(NodeHistory stat : journeyHistory.getNodeHistory())
      {
        this.nodeHistory.add(stat);
      }
    
    this.statusHistory = new ArrayList<StatusHistory>();
    for(StatusHistory status : journeyHistory.getStatusHistory())
      {
        this.statusHistory.add(status);
      }
    
    this.rewardHistory = new ArrayList<RewardHistory>();
    for(RewardHistory reward : journeyHistory.getRewardHistory())
      {
        this.rewardHistory.add(reward);
      }
  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public JourneyHistory(String journeyID, List<NodeHistory> nodeHistory, List<StatusHistory> statusHistory, List<RewardHistory> rewardHistory)
  {
    this.journeyID = journeyID;
    this.nodeHistory = nodeHistory;
    this.statusHistory = statusHistory;
    this.rewardHistory = rewardHistory;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static JourneyHistory unpack(SchemaAndValue schemaAndValue)
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
    String journeyID = valueStruct.getString("journeyID");
    List<RewardHistory> rewardHistory =  unpackRewardHistory(schema.field("rewardHistory").schema(), valueStruct.get("rewardHistory"));
    List<NodeHistory> nodeHistory =  unpackNodeHistory(schema.field("nodeHistory").schema(), valueStruct.get("nodeHistory"));
    List<StatusHistory> statusHistory = unpackStatusHistory(schema.field("statusHistory").schema(), valueStruct.get("statusHistory"));
    
    //
    //  return
    //

    return new JourneyHistory(journeyID, nodeHistory, statusHistory, rewardHistory);
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
  *  addRewardInformation
  *
  *****************************************/
  
  public void addRewardInformation(DeliveryRequest deliveryRequest) 
  {
    RewardHistory history = null;
    if(deliveryRequest instanceof CommodityDeliveryRequest)
      {
        CommodityDeliveryRequest request = (CommodityDeliveryRequest) deliveryRequest;
        history = new RewardHistory(request.getCommodityID(), request.getAmount(), request.getDeliveryDate());
      }
    else if(deliveryRequest instanceof EmptyFulfillmentRequest) 
      {
        EmptyFulfillmentRequest request = (EmptyFulfillmentRequest) deliveryRequest;
        history = new RewardHistory(request.getCommodityID(), request.getAmount(), request.getDeliveryDate());
      }
    else if(deliveryRequest instanceof INFulfillmentRequest) 
      {
        INFulfillmentRequest request = (INFulfillmentRequest) deliveryRequest;
        history = new RewardHistory(request.getProviderID(), request.getAmount(), request.getDeliveryDate());
      }
    else if(deliveryRequest instanceof PointFulfillmentRequest) 
      {
        PointFulfillmentRequest request = (PointFulfillmentRequest) deliveryRequest;
        history = new RewardHistory(request.getPointID(), request.getAmount(), request.getDeliveryDate());
      }
    else if(deliveryRequest instanceof PurchaseFulfillmentRequest) 
      {
        PurchaseFulfillmentRequest request = (PurchaseFulfillmentRequest) deliveryRequest;
        history = new RewardHistory(request.getOfferID(), request.getQuantity(), request.getDeliveryDate());
      }
    if(history != null) 
      {
        this.rewardHistory.add(history);
      }
  }
  
  /*****************************************
  *
  *  addNodeInformation
  *
  *****************************************/
  
  public void addNodeInformation(String fromNodeID, String toNodeID, String deliveryRequestID, String linkID) 
  {
    NodeHistory nodeHistory = new NodeHistory(fromNodeID, toNodeID, SystemTime.getActualCurrentTime(), deliveryRequestID, linkID);
    this.nodeHistory.add(nodeHistory);

  }

  /*****************************************
  *
  *  addStatusInformation
  *
  *****************************************/
  
  public void addStatusInformation(Date now, JourneyState journeyState, boolean journeyComplete) 
  { 
    boolean statusNotified = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
    boolean statusConverted = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
    boolean statusControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    boolean statusUniversalControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) ? journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) : Boolean.FALSE;
    
    StatusHistory statusHistory = new StatusHistory(now, statusNotified, statusConverted, statusControlGroup, statusUniversalControlGroup, journeyComplete);
    if(!this.statusHistory.contains(statusHistory)) 
      {
        this.statusHistory.add(statusHistory);
      }

  }

  /*****************************************
  *
  *  getLastNodeEntered
  *
  *****************************************/
  
  public NodeHistory getLastNodeEntered()
  {
    Comparator<NodeHistory> cmp = new Comparator<NodeHistory>() 
    {
      @Override
      public int compare(NodeHistory node1, NodeHistory node2) 
      {
        return node1.getTransitionDate().compareTo(node2.getTransitionDate());
      }
    };
    
    return this.nodeHistory!=null?Collections.max(this.nodeHistory, cmp):null;
  }
  
  /*****************************************
  *
  *  getLastestReward
  *
  *****************************************/
  
  public RewardHistory getLastestReward()
  {
    Comparator<RewardHistory> cmp = new Comparator<RewardHistory>() 
    {
      @Override
      public int compare(RewardHistory his1, RewardHistory his2) 
      {
        return his1.getRewardDate().compareTo(his2.getRewardDate());
      }
    };
    return this.rewardHistory!=null?Collections.max(this.rewardHistory, cmp):null;
  }
  
  /*****************************************
  *
  *  getLastestJourneyStatus
  *
  *****************************************/

  public StatusHistory getLastestJourneyStatus()
  {
    Comparator<StatusHistory> cmp = new Comparator<StatusHistory>() 
    {
      @Override
      public int compare(StatusHistory status1, StatusHistory status2) 
      {
        return status1.getDate().compareTo(status1.getDate());
      }
    };
    return this.statusHistory!=null?Collections.max(this.statusHistory, cmp):null;
  }

  public static class RewardHistory
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
      schemaBuilder.name("reward_history");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("rewardID", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("amount", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("rewardDate",  Timestamp.builder().optional().schema());
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<RewardHistory> serde = new ConnectSerde<RewardHistory>(schema, false, RewardHistory.class, RewardHistory::pack, RewardHistory::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<RewardHistory> serde() { return serde; }

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String rewardID;
    private int amount;
    private Date rewardDate;
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/
    
    public String getRewardID() { return rewardID; }
    public int getAmount() { return amount; }
    public Date getRewardDate() { return rewardDate; }
    
    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      RewardHistory rewardHistory = (RewardHistory) value;
      Struct struct = new Struct(schema);
      struct.put("rewardID", rewardHistory.getRewardID());
      struct.put("amount", rewardHistory.getAmount());
      struct.put("rewardDate", rewardHistory.getRewardDate());
      return struct;
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    public RewardHistory(String rewardID, int amount, Date rewardDate)
    {
      this.rewardID = rewardID;
      this.amount = amount;
      this.rewardDate = rewardDate;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static RewardHistory unpack(SchemaAndValue schemaAndValue)
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
      String rewardID = valueStruct.getString("rewardID");
      int amount = valueStruct.getInt32("amount");
      Date rewardDate = (Date) valueStruct.get("rewardDate");
      
      //
      //  return
      //

      return new RewardHistory(rewardID, amount, rewardDate);
    }
    
    /*****************************************
    *
    *  toString -- used for elasticsearch
    *
    *****************************************/
    
    @Override
    public String toString()
    {
      return rewardID + ";" + amount + ";" + rewardDate.getTime();
    }
    
  }
  
  public static class NodeHistory
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
      schemaBuilder.name("node_history");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("fromNode", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("toNode", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("transitionDate", Timestamp.builder().optional().schema());
      schemaBuilder.field("deliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("linkID", Schema.OPTIONAL_STRING_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<NodeHistory> serde = new ConnectSerde<NodeHistory>(schema, false, NodeHistory.class, NodeHistory::pack, NodeHistory::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<NodeHistory> serde() { return serde; }

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String fromNode;
    private String toNode;
    private Date transitionDate;
    private String deliveryRequestID;
    private String linkID;
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/
    
    public String getFromNodeID() { return fromNode; }
    public String getToNodeID() { return toNode; }
    public Date getTransitionDate() { return transitionDate; }
    public String getDeliveryRequestID() { return deliveryRequestID; }
    public String getLinkID() { return linkID; }
    
    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      NodeHistory nodeHistory = (NodeHistory) value;
      Struct struct = new Struct(schema);
      struct.put("fromNode", nodeHistory.getFromNodeID());
      struct.put("toNode", nodeHistory.getToNodeID());
      struct.put("transitionDate", nodeHistory.getTransitionDate());
      struct.put("deliveryRequestID", nodeHistory.getDeliveryRequestID());
      struct.put("linkID", nodeHistory.getLinkID());
      return struct;
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    public NodeHistory(String fromNode, String toNode, Date transitionDate, String deliveryRequestID, String linkID)
    {
      this.fromNode = fromNode;
      this.toNode = toNode;
      this.transitionDate = transitionDate;
      this.deliveryRequestID = deliveryRequestID;
      this.linkID = linkID;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static NodeHistory unpack(SchemaAndValue schemaAndValue)
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
      String fromNode = valueStruct.getString("fromNode");
      String toNode = valueStruct.getString("toNode");
      Date transitionDate = (Date) valueStruct.get("transitionDate");
      String deliveryRequestID = valueStruct.getString("deliveryRequestID");
      String linkID = valueStruct.getString("linkID");
      
      //
      //  return
      //

      return new NodeHistory(fromNode, toNode, transitionDate, deliveryRequestID, linkID);
    }
    
    /*****************************************
    *
    *  toString -- used for elasticsearch
    *
    *****************************************/
    
    @Override
    public String toString()
    {
      return fromNode + ";" + toNode + ";" + transitionDate.getTime();
    }
    
  }
  
  public static class StatusHistory
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
      schemaBuilder.name("status_history");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("status", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("date", Timestamp.builder().optional().schema());
      schemaBuilder.field("statusNotified", Schema.OPTIONAL_BOOLEAN_SCHEMA);
      schemaBuilder.field("statusConverted", Schema.OPTIONAL_BOOLEAN_SCHEMA);
      schemaBuilder.field("statusControlGroup", Schema.OPTIONAL_BOOLEAN_SCHEMA);
      schemaBuilder.field("statusUniversalControlGroup", Schema.OPTIONAL_BOOLEAN_SCHEMA);
      schemaBuilder.field("journeyComplete", Schema.OPTIONAL_BOOLEAN_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<StatusHistory> serde = new ConnectSerde<StatusHistory>(schema, false, StatusHistory.class, StatusHistory::pack, StatusHistory::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<StatusHistory> serde() { return serde; }

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String status;
    private Date date;
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
    
    public String getStatus() { return status; }
    public Date getDate() { return date; }
    public boolean getStatusNotified() { return statusNotified; }
    public boolean getStatusConverted() { return statusConverted; }
    public boolean getStatusControlGroup() { return statusControlGroup; }
    public boolean getStatusUniversalControlGroup() { return statusUniversalControlGroup; }
    public boolean getJourneyComplete() { return journeyComplete; }
    
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
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      StatusHistory statusHistory = (StatusHistory) value;
      Struct struct = new Struct(schema);
      struct.put("status", statusHistory.getStatus());
      struct.put("date", statusHistory.getDate());
      struct.put("statusNotified", statusHistory.getStatusNotified());
      struct.put("statusConverted", statusHistory.getStatusConverted());
      struct.put("statusControlGroup", statusHistory.getStatusControlGroup());
      struct.put("statusUniversalControlGroup", statusHistory.getStatusUniversalControlGroup());
      struct.put("journeyComplete", statusHistory.getJourneyComplete());
      return struct;
    }
    
    /*****************************************
    *
    *  constructor -- external
    *
    *****************************************/
    
    public StatusHistory(Date date, boolean statusNotified, boolean statusConverted, boolean statusControlGroup, boolean statusUniversalControlGroup, boolean journeyComplete)
    {
      this.date = date;
      this.statusNotified = statusNotified;
      this.statusConverted = statusConverted;
      this.statusControlGroup = statusControlGroup;
      this.statusUniversalControlGroup = statusUniversalControlGroup;
      this.journeyComplete = journeyComplete;
      this.status = getSubscriberJourneyStatus().getExternalRepresentation();
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    public StatusHistory(String status, Date date, boolean statusNotified, boolean statusConverted, boolean statusControlGroup, boolean statusUniversalControlGroup, boolean journeyComplete)
    {
      this.status = status;
      this.date = date;
      this.statusNotified = statusNotified;
      this.statusConverted = statusConverted;
      this.statusControlGroup = statusControlGroup;
      this.statusUniversalControlGroup = statusUniversalControlGroup;
      this.journeyComplete = journeyComplete;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static StatusHistory unpack(SchemaAndValue schemaAndValue)
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
      String status = valueStruct.getString("status");
      Date date = (Date) valueStruct.get("date");
      boolean statusNotified = valueStruct.getBoolean("statusNotified");
      boolean statusConverted = valueStruct.getBoolean("statusConverted");
      boolean statusControlGroup = valueStruct.getBoolean("statusControlGroup");
      boolean statusUniversalControlGroup = valueStruct.getBoolean("statusUniversalControlGroup");
      boolean journeyComplete = valueStruct.getBoolean("journeyComplete");
      
      //
      //  return
      //

      return new StatusHistory(status, date, statusNotified, statusConverted, statusControlGroup, statusUniversalControlGroup, journeyComplete);
    }
    
    /*****************************************
    *
    *  toString -- used for elasticsearch
    *
    *****************************************/
    
    @Override
    public String toString()
    {
      return status + ";" + date.getTime();
    }
    
    /*****************************************
    *
    *  equals
    *
    *****************************************/

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      StatusHistory other = (StatusHistory) obj;
      if (status == null)
        {
          if (other.status != null)
            return false;
        } else if (!status.equals(other.status))
        return false;
      return true;
    }
  }
}
