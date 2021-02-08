package com.evolving.nglm.evolution;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.evolving.nglm.evolution.retention.Cleanable;
import com.evolving.nglm.evolution.retention.RetentionService;
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
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatusField;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;

public class JourneyHistory implements Cleanable
{

  protected static final Logger log = LoggerFactory.getLogger(JourneyHistory.class);

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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("nodeHistory", SchemaBuilder.array(NodeHistory.schema()).schema());
    schemaBuilder.field("statusHistory", SchemaBuilder.array(StatusHistory.schema()).schema());
    schemaBuilder.field("rewardHistory", SchemaBuilder.array(RewardHistory.schema()).schema());
    schemaBuilder.field("lastConversionDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("conversionCount", Schema.INT32_SCHEMA);
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
  private List<RewardHistory> rewardHistory; // List of each rewards attributions (thus we need to sum them if we want the total for every different rewardID)
  private Date lastConversionDate;
  private int conversionCount;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public String getJourneyID() { return journeyID; }
  public List<NodeHistory> getNodeHistory() { return nodeHistory; }
  public List<StatusHistory> getStatusHistory() { return statusHistory; }
  public List<RewardHistory> getRewardHistory() { return rewardHistory; }
  public Date getLastConversionDate() { return lastConversionDate; }
  public int getConversionCount() { return conversionCount; }
  public void incrementConversions(Date date) { 
    this.conversionCount += 1; 
    this.lastConversionDate = date;
  } 
  public Boolean getControlGroupStatus()
  {
    Boolean result = null;
    for (StatusHistory statusHistory : getStatusHistory()) { if (statusHistory.getStatusControlGroup() != null) result = statusHistory.getStatusControlGroup(); }
    return result;
  }
  public Boolean getUniversalControlGroupStatus()
  {
    Boolean result = null;
    for (StatusHistory statusHistory : getStatusHistory()) { if (statusHistory.getStatusUniversalControlGroup() != null) result = statusHistory.getStatusUniversalControlGroup(); }
    return result;
  }
  public Boolean getTargetGroupStatus()
  {
    Boolean result = null;
    for (StatusHistory statusHistory : getStatusHistory()) { if (statusHistory.getStatusTargetGroup() != null) result = statusHistory.getStatusTargetGroup(); }
    return result;
  }

  @Override public Date getExpirationDate(RetentionService retentionService) {
    return getJourneyExitDate(retentionService.getJourneyService());
  }
  @Override public Duration getRetention(RetentionType type, RetentionService retentionService) {
    return retentionService.getJourneyRetention(type,getJourneyID());
  }
  
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
    struct.put("lastConversionDate", journeyHistory.getLastConversionDate());
    struct.put("conversionCount", journeyHistory.getConversionCount());
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
    this.lastConversionDate = null;
    this.conversionCount = 0;
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
    
    this.lastConversionDate = null;
    if(journeyHistory.getLastConversionDate() != null) {
      this.lastConversionDate = new Date(journeyHistory.getLastConversionDate().getTime());
    }
    this.conversionCount = journeyHistory.getConversionCount();
  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public JourneyHistory(String journeyID, List<NodeHistory> nodeHistory, List<StatusHistory> statusHistory, List<RewardHistory> rewardHistory, Date lastConversionDate, int conversionCount)
  {
    this.journeyID = journeyID;
    this.nodeHistory = nodeHistory;
    this.statusHistory = statusHistory;
    this.rewardHistory = rewardHistory;
    this.lastConversionDate = lastConversionDate;
    this.conversionCount = conversionCount;
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
    Date lastConversionDate = (schemaVersion >= 2) ? (Date) valueStruct.get("lastConversionDate") : null;
    int conversionCount = (schemaVersion >= 2) ? valueStruct.getInt32("conversionCount") : 0;
    
    //
    //  return
    //

    return new JourneyHistory(journeyID, nodeHistory, statusHistory, rewardHistory, lastConversionDate, conversionCount);
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

  /**
   * 
   * @return RewardHistory if a reward has been added in reward history, null otherwise
   */
  public RewardHistory addRewardInformation(DeliveryRequest deliveryRequest, DeliverableService deliverableService, Date now)
  {
    RewardHistory history = null;
    if(deliveryRequest instanceof CommodityDeliveryRequest) {
      CommodityDeliveryRequest request = (CommodityDeliveryRequest) deliveryRequest;
      
      // This part was based on addFieldsForGUIPresentation of CommodityDeliveryManager class, this could be factorized.
      Deliverable deliverable = deliverableService.getActiveDeliverable(request.getCommodityID(), now);
      String deliverableDisplay = deliverable != null ? deliverable.getGUIManagedObjectDisplay() : request.getCommodityID();
      
      history = new RewardHistory(deliverableDisplay, request.getAmount(), request.getDeliveryDate());
    } 
    else if(deliveryRequest instanceof RewardManagerRequest) {
      RewardManagerRequest request = (RewardManagerRequest) deliveryRequest;
      // THIS IS SHITTY a FUCKING hack for the RewardManager
      // This part was based on addFieldsForGUIPresentation of CommodityDeliveryManager class, this could be factorized.
      Deliverable deliverable = deliverableService.getActiveDeliverable(request.getDeliverableID(), now);
      String deliverableDisplay = deliverable != null ? deliverable.getGUIManagedObjectDisplay() : request.getDeliverableID();
      
      history = new RewardHistory(deliverableDisplay, (int)request.getAmount(), request.getDeliveryDate());
    } 
    // Special case for offers. 
    // PurchaseFulfillmentRequest are not managed by the CommodityManager (which is a proxy for a lot of requests)
    else if(deliveryRequest instanceof PurchaseFulfillmentRequest) {
      PurchaseFulfillmentRequest request = (PurchaseFulfillmentRequest) deliveryRequest;
      history = new RewardHistory(request.getOfferDisplay(), request.getQuantity(), request.getDeliveryDate());
    }
    
    if(history != null) {
      this.rewardHistory.add(history);
    }
    
    return history;
  }
  
  /*****************************************
  *
  *  addNodeInformation
  *
  *****************************************/
  
  public void addNodeInformation(String fromNodeID, String toNodeID, String deliveryRequestID, String linkID) 
  {
    NodeHistory nodeHistory = new NodeHistory(fromNodeID, toNodeID, SystemTime.getCurrentTime(), deliveryRequestID, linkID);
    this.nodeHistory.add(nodeHistory);

  }

  /*****************************************
  *
  *  addStatusInformation
  *
  *****************************************/
  
  /**
   * 
   * @return true if a new status has been published in status history
   */
  public boolean addStatusInformation(Date now, JourneyState journeyState, boolean journeyComplete) 
  { 
    boolean statusNotified = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
    boolean statusConverted = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
    Boolean statusTargetGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusTargetGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusTargetGroup.getJourneyParameterName()) : null;
    Boolean statusControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) : null;
    Boolean statusUniversalControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) : null;
    
    //
    // re-check
    //
    
    if (statusTargetGroup == Boolean.TRUE) statusControlGroup = statusUniversalControlGroup = !statusTargetGroup;
    if (statusControlGroup == Boolean.TRUE) statusTargetGroup = statusUniversalControlGroup = !statusControlGroup;
    if (statusUniversalControlGroup == Boolean.TRUE) statusTargetGroup = statusControlGroup = !statusUniversalControlGroup;
    
    //
    //  statusHistory
    //
    
    StatusHistory statusHistory = new StatusHistory(now, statusNotified, statusConverted, statusTargetGroup, statusControlGroup, statusUniversalControlGroup, journeyComplete);
    
    //
    //  add
    //
    
    if(!this.statusHistory.contains(statusHistory)) 
      {
        this.statusHistory.add(statusHistory);
        return true;
      }

    return false;
  }

  public boolean addStatusInformation(Date now, JourneyState journeyState, boolean journeyComplete, SubscriberJourneyStatus specialCase) 
  {   
    if(specialCase!=null) {
      StatusHistory statusHistory = new StatusHistory(now,specialCase);
      
      //
      //  add
      //
      
      if(!this.statusHistory.contains(statusHistory))  {
        this.statusHistory.add(statusHistory);
        return true;
      }
      return false; 
    } else {
      return addStatusInformation(now, journeyState, journeyComplete);
    }
  }
  
  /*****************************************
  *
  *  getLastNodeEntered
  *
  *****************************************/
  
  public NodeHistory getLastNodeEntered()
  {
    NodeHistory result = null;
    for (NodeHistory nodeHistory : this.nodeHistory)
      {
        if (result != null)
          {
            int compare = nodeHistory.getTransitionDate().compareTo(result.getTransitionDate());
            if (compare == 0) compare = 1; // in case of equality, let consider the one of the right is the good one
            if (compare > 0) result = nodeHistory;
          }
        else
          {
            result = nodeHistory;
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  getJourneyEntranceDate
  *
  *****************************************/
  
  public Date getJourneyEntranceDate()
  {
    Comparator<NodeHistory> cmp = new Comparator<NodeHistory>() 
    {
      @Override
      public int compare(NodeHistory node1, NodeHistory node2) 
      {
        return node1.getTransitionDate().compareTo(node2.getTransitionDate());
      }
    };
    
    return this.nodeHistory!=null?Collections.min(this.nodeHistory, cmp).getTransitionDate():null;
  }
  
  /*****************************************
  *
  *  getJourneyExitDate
  *
  *****************************************/

  /* TODO: Xavier, this one just can not work cause we don't update status while going to the end node
  public Date getJourneyExitDate()
  {
    if(this.statusHistory != null)
    {
      for(StatusHistory status : this.statusHistory)
      {
        if(status.getJourneyComplete())
        {
          return status.getDate();
        }
      }
    }
    return null;
  }
  */
  //TODO: this is probably a bad workarround but I can not modify status without checking all reporting and so
  public Date getJourneyExitDate(JourneyService journeyService)
  {
    NodeHistory latestNodeEntered = getLastNodeEntered();
    if(log.isTraceEnabled()) log.trace("JourneyHistory.getJourneyExitDate : "+getJourneyID()+" latestNodeEntered "+latestNodeEntered+getJourneyID());
    if(latestNodeEntered==null) return null;
    if(log.isTraceEnabled()) log.trace("JourneyHistory.getJourneyExitDate : "+getJourneyID()+" latestNodeEntered.getToNodeID() "+latestNodeEntered.getToNodeID());
    if(latestNodeEntered.getToNodeID()==null) return null;
    GUIManagedObject guiManagedObject = journeyService.getStoredJourney(getJourneyID());
    if(guiManagedObject==null){
      if(log.isDebugEnabled()) log.debug("JourneyHistory.getJourneyExitDate : "+getJourneyID()+" deleted campaign, returning current date");
      return SystemTime.getCurrentTime();
    }
    if(!(guiManagedObject instanceof Journey)){
      if(log.isDebugEnabled()) log.debug("JourneyHistory.getJourneyExitDate : "+getJourneyID()+" not valid campaign, returning null");
      return null;
    }
    if(latestNodeEntered.getToNodeID().equals(((Journey)guiManagedObject).getEndNodeID())){
      if(log.isTraceEnabled()) log.trace("JourneyHistory.getJourneyExitDate : "+latestNodeEntered.getToNodeID()+" is journey end node, returning "+latestNodeEntered.getTransitionDate());
      return latestNodeEntered.getTransitionDate();
    }
    if(log.isTraceEnabled()) log.trace("JourneyHistory.getJourneyExitDate : "+latestNodeEntered.getToNodeID()+" is not journey end node "+((Journey)guiManagedObject).getEndNodeID()+", returning null");
    return null;
  }
  
  /*****************************************
  *
  *  getLastestReward
  *
  *****************************************/
  
  public RewardHistory getLastestReward()
  {
    RewardHistory result = null;
    for (RewardHistory rewardHistory : this.rewardHistory)
      {
        if (result != null)
          {
            int compare = rewardHistory.getRewardDate().compareTo(result.getRewardDate());
            if (compare == 0) compare = 1; // in case of equality, let consider the one of the right is the good one
            if (compare > 0) result = rewardHistory;
          }
        else 
          {
            result = rewardHistory;
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  getLastestJourneyStatus
  *
  *****************************************/

  public StatusHistory getLastestJourneyStatus()
  {
    StatusHistory result = null;
    for (StatusHistory statusHistory : this.statusHistory)
      {
        if(result != null)
          {
            int compare = statusHistory.getDate().compareTo(result.getDate());
            if (compare == 0) compare = 1; // in case of equality, let consider the one of the right is the good one
            if (compare > 0) result = statusHistory;
          }
        else 
          {
            result = statusHistory;
          }
      }
    return result;
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
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
      schemaBuilder.field("rewardName", Schema.OPTIONAL_STRING_SCHEMA);
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
    private String rewardName;
    private int amount;
    private Date rewardDate;
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/
    public String getRewardName() { return rewardName; }
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
      struct.put("rewardName", rewardHistory.getRewardName());
      struct.put("amount", rewardHistory.getAmount());
      struct.put("rewardDate", rewardHistory.getRewardDate());
      return struct;
    }
    
    /*****************************************
    *
    *  constructor -- copy
    *
    *****************************************/
    public RewardHistory(RewardHistory rewardHistory)
    {
      this.rewardName = new String(rewardHistory.getRewardName());
      this.amount = rewardHistory.getAmount();
      this.rewardDate = rewardHistory.getRewardDate()!=null?new Date(rewardHistory.getRewardDate().getTime()):null;
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/
    public RewardHistory(String rewardName, int amount, Date rewardDate)
    {
      this.rewardName = rewardName;
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
      // In version <= 1, there were a attribute "rewardID".
      String rewardName = (schemaVersion >= 2) ? valueStruct.getString("rewardName") : valueStruct.getString("rewardID");
      int amount = valueStruct.getInt32("amount");
      Date rewardDate = (Date) valueStruct.get("rewardDate");
      
      //
      //  return
      //

      return new RewardHistory(rewardName, amount, rewardDate);
    }
    
    /*****************************************
    *
    *  toString -- used for elasticsearch
    *
    *****************************************/
    @Override
    public String toString()
    {
      return rewardName + ";" + amount + ";" + (rewardDate!=null?rewardDate.getTime():null);
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
  
  /*****************************************
  *
  *  class StatusHistory
  *
  *****************************************/

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
    private static int currntSchemaVersion = 2;
    static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("status_history");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(currntSchemaVersion));
      schemaBuilder.field("status", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("date", Timestamp.builder().optional().schema());
      schemaBuilder.field("statusNotified", Schema.OPTIONAL_BOOLEAN_SCHEMA);
      schemaBuilder.field("statusConverted", Schema.OPTIONAL_BOOLEAN_SCHEMA);
      schemaBuilder.field("statusTargetGroup", Schema.OPTIONAL_BOOLEAN_SCHEMA);
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
    private Boolean statusTargetGroup;
    private Boolean statusControlGroup;
    private Boolean statusUniversalControlGroup;
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
    public Boolean getStatusTargetGroup() { return statusTargetGroup; }
    public Boolean getStatusControlGroup() { return statusControlGroup; }
    public Boolean getStatusUniversalControlGroup() { return statusUniversalControlGroup; }
    public boolean getJourneyComplete() { return journeyComplete; }
    public SubscriberJourneyStatus getSubscriberJourneyStatus() { return Journey.getSubscriberJourneyStatus(this); }

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
      struct.put("statusTargetGroup", statusHistory.getStatusTargetGroup());
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
    
    public StatusHistory(Date date, boolean statusNotified, boolean statusConverted, Boolean statusTargetGroup, Boolean statusControlGroup, Boolean statusUniversalControlGroup, boolean journeyComplete)
    {
      this.date = date;
      this.statusNotified = statusNotified;
      this.statusConverted = statusConverted;
      this.statusTargetGroup = statusTargetGroup;
      this.statusControlGroup = statusControlGroup;
      this.statusUniversalControlGroup = statusUniversalControlGroup;
      this.journeyComplete = journeyComplete;
      this.status = getSubscriberJourneyStatus().getExternalRepresentation();
    }
    
    public StatusHistory(Date date, SubscriberJourneyStatus specialCase)
    {
      this.date = date;
      this.journeyComplete = true;
      this.status = specialCase.getExternalRepresentation();
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    public StatusHistory(String status, Date date, boolean statusNotified, boolean statusConverted, Boolean statusTargetGroup, Boolean statusControlGroup, Boolean statusUniversalControlGroup, boolean journeyComplete)
    {
      this.status = status;
      this.date = date;
      this.statusNotified = statusNotified;
      this.statusConverted = statusConverted;
      this.statusTargetGroup = statusTargetGroup;
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
      status = "controlGroup_entered".equals(status) ? SubscriberJourneyStatus.ControlGroup.getExternalRepresentation() : status;
      Date date = (Date) valueStruct.get("date");
      boolean statusNotified = valueStruct.getBoolean("statusNotified");
      boolean statusConverted = valueStruct.getBoolean("statusConverted");
      Boolean statusTargetGroup = schemaVersion >= 2 ? valueStruct.getBoolean("statusTargetGroup") : null;
      Boolean statusControlGroup = valueStruct.getBoolean("statusControlGroup");
      Boolean statusUniversalControlGroup = valueStruct.getBoolean("statusUniversalControlGroup");
      boolean journeyComplete = valueStruct.getBoolean("journeyComplete");
      
      //
      //  return
      //

      return new StatusHistory(status, date, statusNotified, statusConverted, statusTargetGroup, statusControlGroup, statusUniversalControlGroup, journeyComplete);
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

  @Override
  public String toString()
  {
    return "JourneyHistory [" + (journeyID != null ? "journeyID=" + journeyID + ", " : "") + (nodeHistory != null ? "nodeHistory=" + nodeHistory + ", " : "") + (statusHistory != null ? "statusHistory=" + statusHistory + ", " : "") + (rewardHistory != null ? "rewardHistory=" + rewardHistory : "") + "]";
  }
}
