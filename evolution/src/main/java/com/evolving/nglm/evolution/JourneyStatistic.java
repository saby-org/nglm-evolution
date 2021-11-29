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
import org.apache.kafka.connect.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JourneyStatistic extends SubscriberStreamOutput implements SubscriberStreamEvent, Comparable
{
  
  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(JourneyStatistic.class);
  
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  private static int currentSchemaVersion = 12; // 11->12: EVPRO-1318
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("journey_statistic");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),currentSchemaVersion));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("journeyStatisticID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyInstanceID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("transitionDate", Timestamp.SCHEMA);
    schemaBuilder.field("currentNodeID", Schema.STRING_SCHEMA);
    //schemaBuilder.field("deliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA); @rl THIS SHOULD BE REMOVED FROM ES INDEX LATER
    schemaBuilder.field("sample", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("lastConversionDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("conversionCount", Schema.INT32_SCHEMA);
    schemaBuilder.field("statusNotified", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("statusConverted", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("statusTargetGroup", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    schemaBuilder.field("statusControlGroup", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    schemaBuilder.field("statusUniversalControlGroup", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    schemaBuilder.field("journeyComplete", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("journeyNodeHistory", SchemaBuilder.array(NodeHistory.schema()).schema());
    schemaBuilder.field("journeyStatusHistory", SchemaBuilder.array(StatusHistory.schema()).schema());
    schemaBuilder.field("journeyRewardHistory", SchemaBuilder.array(RewardHistory.schema()).schema());
    schemaBuilder.field("subscriberStratum", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("specialExitStatus", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyExitDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("archive", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
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
  private String currentNodeID;
  private String sample;
  private Date lastConversionDate;
  private int conversionCount;
  private boolean statusNotified;
  private boolean statusConverted;
  private Boolean statusTargetGroup;
  private Boolean statusControlGroup;
  private Boolean statusUniversalControlGroup;
  private boolean journeyComplete;
  private List<NodeHistory> journeyNodeHistory;
  private List<StatusHistory> journeyStatusHistory;
  private List<RewardHistory> journeyRewardHistory;
  private Map<String, String> subscriberStratum; // Statistic stratum only
  private String specialExitStatus;
  private Date journeyExitDate;
  private boolean archive; // (not pushed in Elasticsearch) used to know if the JourneyStatistic can be pushed in a compacted & dedicated ES index (only for workflow for the moment)
  private int tenantID;
  
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
  public String getCurrentNodeID() { return currentNodeID; }
  public String getSample() { return sample; }
  public Date getLastConversionDate() { return lastConversionDate; }
  public int getConversionCount() { return conversionCount; }
  public boolean getStatusNotified() { return statusNotified; }
  public boolean getStatusConverted() { return statusConverted; }
  public Boolean getStatusTargetGroup() { return statusTargetGroup; }
  public Boolean getStatusControlGroup() { return statusControlGroup; }
  public Boolean getStatusUniversalControlGroup() { return statusUniversalControlGroup; }
  public boolean getJourneyComplete() { return journeyComplete; }
  public Date getEventDate() { return transitionDate; }
  public List<NodeHistory> getJourneyNodeHistory() { return journeyNodeHistory; }
  public List<StatusHistory> getJourneyStatusHistory() { return journeyStatusHistory; }
  public List<RewardHistory> getJourneyRewardHistory() { return journeyRewardHistory; }
  public SubscriberJourneyStatus getSubscriberJourneyStatus() { return Journey.getSubscriberJourneyStatus(this); }
  public Map<String, String> getSubscriberStratum() { return subscriberStratum; }
  public String getSpecialExitStatus() { return (specialExitStatus == null) ? "" : specialExitStatus; }
  public Date getJourneyExitDate() { return journeyExitDate; }
  public boolean isArchive() { return archive; }
  public int getTenantID() { return tenantID; }
  
  /*****************************************
  *
  * Constructor - This should be the only one public constructor. 
  * 
  * We are taking a picture of JourneyState. For two identical JourneyState, it should always return 
  * the same picture, no matter where we take it and how. Because, in the end, this will override the 
  * previous journeystatistic in Elasticsearch, therefore we should NEVER store information in 
  * journeystatistic that cannot be retrieve from journeyState / subscriberState.
  *
  *****************************************/
  /**
   * 
   * @param context
   * @param subscriberState
   * @param journeyState
   * @param journey                         /!\ Can be null - if journey has been removed in the meantime
   * @param statisticSubscriberStratum          We retrieve this info directly (even if it can be retrieve from SubscriberState) in order to avoid passing reader & service as an argument
   */
  public JourneyStatistic(EvolutionEventContext context, SubscriberState subscriberState, JourneyState journeyState, Journey journey, Map<String, String> statisticSubscriberStratum)
  {
    //
    // JourneyStatistic Key <JourneyInstanceID, JourneyID, SubscriberID>
    //
    this.journeyStatisticID = context.getUniqueKey();
    this.journeyInstanceID = journeyState.getJourneyInstanceID();
    this.journeyID = journeyState.getJourneyID();
    this.subscriberID = subscriberState.getSubscriberID();
    
    //
    // Subscriber info
    //
    this.tenantID = subscriberState.getSubscriberProfile().getTenantID();
    this.subscriberStratum = statisticSubscriberStratum;
    
    //
    // History
    //
    JourneyHistory journeyHistory = journeyState.getJourneyHistory();
    this.journeyNodeHistory = prepareJourneyNodeSummary(journeyHistory);
    this.journeyStatusHistory = prepareJourneyStatusSummary(journeyHistory);
    this.journeyRewardHistory = prepareJourneyRewardsSummary(journeyHistory);
    
    //
    // Current state info (duplicates with node history)
    //
    this.currentNodeID = journeyState.getJourneyNodeID();
    this.transitionDate = journeyState.getJourneyNodeEntryDate();
    
    //
    // Status
    //
    // StatusNotified (default: false)
    this.statusNotified = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusNotified.getJourneyParameterName()) : Boolean.FALSE;
    // StatusConverted (default: false)
    this.statusConverted = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusConverted.getJourneyParameterName()) : Boolean.FALSE;
    // Target (default: null)
    this.statusTargetGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusTargetGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusTargetGroup.getJourneyParameterName()) : null;
    // CG (default: null)
    this.statusControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusControlGroup.getJourneyParameterName()) : null;
    // UCG (default: null)
    this.statusUniversalControlGroup = journeyState.getJourneyParameters().containsKey(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) ? (Boolean) journeyState.getJourneyParameters().get(SubscriberJourneyStatusField.StatusUniversalControlGroup.getJourneyParameterName()) : null;

    // Check status constraints @rl: this is really difficult to read and would require comments...
    if (this.statusTargetGroup == Boolean.TRUE) this.statusControlGroup = this.statusUniversalControlGroup = !this.statusTargetGroup;
    if (this.statusControlGroup == Boolean.TRUE) this.statusTargetGroup = this.statusUniversalControlGroup = !this.statusControlGroup;
    if (this.statusUniversalControlGroup == Boolean.TRUE) this.statusTargetGroup = this.statusControlGroup = !this.statusUniversalControlGroup;
    
    // Exit status
    this.specialExitStatus = journeyState.isSpecialExit() ? journeyState.getSpecialExitReason().getExternalRepresentation() : null;
    
    //
    // Conversion 
    //
    this.lastConversionDate = journeyHistory.getLastConversionDate();
    this.conversionCount = journeyHistory.getConversionCount();
    
    //
    // Sample
    //
    this.sample = null;
    // abTesting (we remove it so its only counted once per journey)
    if(journeyState.getJourneyParameters().get("sample.a") != null) {
      sample = (String) journeyState.getJourneyParameters().get("sample.a");
      // journeyState.getJourneyParameters().remove("sample.a"); @rl this cannot work. If we remove it that would mean we store information in JourneyStatistic that would be overriden next transition
    }
    else if(journeyState.getJourneyParameters().get("sample.b") != null) {
      sample = (String) journeyState.getJourneyParameters().get("sample.b");
      // journeyState.getJourneyParameters().remove("sample.b"); @rl this cannot work. If we remove it that would mean we store information in JourneyStatistic that would be overriden next transition
    }
    
    //
    // End of journey info
    //
    this.journeyExitDate = journeyState.getJourneyExitDate();
    this.journeyComplete = journeyState.getJourneyExitDate() != null;
    this.archive = (journey != null) ? (journey.isWorkflow() && this.journeyComplete) : false; // default to false if journey cannot be found
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private JourneyStatistic(SchemaAndValue schemaAndValue, String journeyStatisticID, String journeyInstanceID, String journeyID, String subscriberID, Date transitionDate, String currentNodeID, String sample, Date lastConversionDate, int conversionCount, boolean statusNotified, boolean statusConverted, Boolean statusTargetGroup, Boolean statusControlGroup, Boolean statusUniversalControlGroup, boolean journeyComplete, List<NodeHistory> journeyNodeHistory, List<StatusHistory> journeyStatusHistory, List<RewardHistory> journeyRewardHistory, Map<String, String> subscriberStratum, String specialExitStatus, Date journeyExitDate, boolean archive, int tenantID)
  {
    super(schemaAndValue);
    this.journeyStatisticID = journeyStatisticID;
    this.journeyInstanceID = journeyInstanceID;
    this.journeyID = journeyID;
    this.subscriberID = subscriberID;
    this.transitionDate = transitionDate;
    this.currentNodeID = currentNodeID;
    this.sample = sample;
    this.lastConversionDate = lastConversionDate;
    this.conversionCount = conversionCount;
    this.statusNotified = statusNotified;
    this.statusConverted = statusConverted;
    this.statusTargetGroup = statusTargetGroup;
    this.statusControlGroup = statusControlGroup;
    this.statusUniversalControlGroup = statusUniversalControlGroup;
    this.journeyComplete = journeyComplete;
    this.journeyNodeHistory = journeyNodeHistory;
    this.journeyStatusHistory = journeyStatusHistory;
    this.journeyRewardHistory = journeyRewardHistory;
    this.subscriberStratum = subscriberStratum;
    this.specialExitStatus = specialExitStatus;
    this.journeyExitDate = journeyExitDate;
    this.archive = archive;
    this.tenantID = tenantID;
  }
  
  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public JourneyStatistic(JourneyStatistic journeyStatistic)
  {
    super(journeyStatistic);
    this.journeyStatisticID = journeyStatistic.getJourneyStatisticID();
    this.journeyInstanceID = journeyStatistic.getJourneyInstanceID();
    this.journeyID = journeyStatistic.getJourneyID();
    this.subscriberID = journeyStatistic.getSubscriberID();
    this.transitionDate = journeyStatistic.getTransitionDate();
    this.currentNodeID = journeyStatistic.getCurrentNodeID();
    this.sample = journeyStatistic.getSample();
    this.lastConversionDate = journeyStatistic.getLastConversionDate();
    this.conversionCount = journeyStatistic.getConversionCount();
    this.statusNotified = journeyStatistic.getStatusNotified();
    this.statusConverted = journeyStatistic.getStatusConverted();
    this.statusTargetGroup = journeyStatistic.getStatusTargetGroup();
    this.statusControlGroup = journeyStatistic.getStatusControlGroup();
    this.statusUniversalControlGroup = journeyStatistic.getStatusUniversalControlGroup();
    this.journeyComplete = journeyStatistic.getJourneyComplete();
    this.journeyExitDate = journeyStatistic.getJourneyExitDate();
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
    this.archive = journeyStatistic.isArchive();
    this.tenantID = journeyStatistic.getTenantID();
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
    packSubscriberStreamOutput(struct,journeyStatistic);
    struct.put("journeyStatisticID", journeyStatistic.getJourneyStatisticID());
    struct.put("journeyInstanceID", journeyStatistic.getJourneyInstanceID());
    struct.put("journeyID", journeyStatistic.getJourneyID());
    struct.put("subscriberID", journeyStatistic.getSubscriberID());
    struct.put("transitionDate", journeyStatistic.getTransitionDate());
    struct.put("currentNodeID", journeyStatistic.getCurrentNodeID());
    struct.put("sample", journeyStatistic.getSample());
    struct.put("lastConversionDate", journeyStatistic.getLastConversionDate());
    struct.put("conversionCount", journeyStatistic.getConversionCount());
    struct.put("statusNotified", journeyStatistic.getStatusNotified());
    struct.put("statusConverted", journeyStatistic.getStatusConverted());
    struct.put("statusTargetGroup", journeyStatistic.getStatusTargetGroup());
    struct.put("statusControlGroup", journeyStatistic.getStatusControlGroup());
    struct.put("statusUniversalControlGroup", journeyStatistic.getStatusUniversalControlGroup());
    struct.put("journeyComplete", journeyStatistic.getJourneyComplete());
    struct.put("journeyNodeHistory", packNodeHistory(journeyStatistic.getJourneyNodeHistory()));
    struct.put("journeyStatusHistory", packStatusHistory(journeyStatistic.getJourneyStatusHistory()));
    struct.put("journeyRewardHistory", packRewardHistory(journeyStatistic.getJourneyRewardHistory()));
    struct.put("subscriberStratum", journeyStatistic.getSubscriberStratum());
    struct.put("specialExitStatus", journeyStatistic.getSpecialExitStatus());
    struct.put("journeyExitDate", journeyStatistic.getJourneyExitDate());
    struct.put("archive", journeyStatistic.isArchive());
    struct.put("tenantID", (short) journeyStatistic.getTenantID());
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String journeyStatisticID = valueStruct.getString("journeyStatisticID");
    String journeyInstanceID = valueStruct.getString("journeyInstanceID");
    String journeyID = valueStruct.getString("journeyID");
    String subscriberID = valueStruct.getString("subscriberID");
    Date transitionDate = (Date) valueStruct.get("transitionDate");
    String currentNodeID = (schemaVersion >= 12) ? valueStruct.getString("currentNodeID") : valueStruct.getString("toNodeID");
    String sample = valueStruct.getString("sample");
    Date lastConversionDate = (schemaVersion >= 9) ? (Date) valueStruct.get("lastConversionDate") : null;
    int conversionCount = (schemaVersion >= 9) ? valueStruct.getInt32("conversionCount") : 0;
    boolean statusNotified = valueStruct.getBoolean("statusNotified");
    boolean statusConverted = valueStruct.getBoolean("statusConverted");
    Boolean statusControlGroup = valueStruct.getBoolean("statusControlGroup");
    Boolean statusUniversalControlGroup = valueStruct.getBoolean("statusUniversalControlGroup");
    Boolean statusTargetGroup = valueStruct.getBoolean("statusTargetGroup");
    if (schemaVersion < 3) statusTargetGroup = (statusControlGroup ? false : (statusUniversalControlGroup ? false : null));
    boolean journeyComplete = valueStruct.getBoolean("journeyComplete");
    List<RewardHistory> journeyRewardHistory =  unpackRewardHistory(schema.field("journeyRewardHistory").schema(), valueStruct.get("journeyRewardHistory"));
    List<NodeHistory> journeyNodeHistory =  unpackNodeHistory(schema.field("journeyNodeHistory").schema(), valueStruct.get("journeyNodeHistory"));
    List<StatusHistory> journeyStatusHistory =  unpackStatusHistory(schema.field("journeyStatusHistory").schema(), valueStruct.get("journeyStatusHistory"));
    Map<String, String> subscriberStratum = (Map<String,String>) valueStruct.get("subscriberStratum");
    String specialExitStatus = (schemaVersion >= 10) ? valueStruct.getString("specialExitStatus") : "";
    Date journeyExitDate = (schemaVersion >= 10) ? (Date) valueStruct.get("journeyExitDate") : null;
    boolean archive = (schemaVersion >= 12) ? valueStruct.getBoolean("archive") : false;
    int tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : 1; // by default tenant 1
    
    //
    //  return
    //

    return new JourneyStatistic(schemaAndValue, journeyStatisticID, journeyInstanceID, journeyID, subscriberID, transitionDate, currentNodeID, sample, lastConversionDate, conversionCount, statusNotified, statusConverted, statusTargetGroup, statusControlGroup, statusUniversalControlGroup, journeyComplete, journeyNodeHistory, journeyStatusHistory, journeyRewardHistory, subscriberStratum, specialExitStatus, journeyExitDate, archive, tenantID);
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
        if(journeyHistory.getJourneyID().equals(journeyID)) // @rl why check this ?????
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
  
  
  @Override
  public String toString()
  {
    final int maxLen = 10;
    return "JourneyStatistic [" + (journeyStatisticID != null ? "journeyStatisticID="
    + journeyStatisticID + ", " : "") 
        + (journeyInstanceID != null ? "journeyInstanceID=" + journeyInstanceID + ", " : "")
        + (journeyID != null ? "journeyID=" + journeyID + ", " : "")
        + (subscriberID != null ? "subscriberID=" + subscriberID + ", " : "") 
        + (transitionDate != null ? "transitionDate=" + transitionDate + ", " : "") 
        + (currentNodeID != null ? "currentNodeID=" + currentNodeID + ", " : "")
        + (sample != null ? "sample=" + sample + ", " : "") 
        + "statusNotified=" + statusNotified + ", "
        + "statusConverted=" + statusConverted + ", "
        + (lastConversionDate != null ? "lastConversionDate=" + lastConversionDate + ", " : "") 
        + "conversionCount=" + conversionCount + ", "
        + (statusTargetGroup != null ? "statusTargetGroup=" + statusTargetGroup + ", " : "") 
        + (statusControlGroup != null ? "statusControlGroup=" + statusControlGroup + ", " : "") 
        + (statusUniversalControlGroup != null ? "statusUniversalControlGroup=" + statusUniversalControlGroup + ", " : "") 
        + "journeyComplete=" + journeyComplete + ", " 
        + (journeyNodeHistory != null ? "journeyNodeHistory=" + toString(journeyNodeHistory, maxLen) + ", " : "")
        + (journeyStatusHistory != null ? "journeyStatusHistory=" + toString(journeyStatusHistory, maxLen) + ", " : "") 
        + (journeyRewardHistory != null ? "journeyRewardHistory=" + toString(journeyRewardHistory, maxLen) + ", " : "") 
        + (subscriberStratum != null ? "subscriberStratum=" + toString(subscriberStratum.entrySet(), maxLen) : "")
        + (journeyExitDate != null ? "journeyExitDate=" + journeyExitDate + ", " : "")
        + "archive=" + archive
        + "]";
  }

  private String toString(Collection<?> collection, int maxLen)
  {
    StringBuilder builder = new StringBuilder();
    builder.append("[");
    int i = 0;
    for (Iterator<?> iterator = collection.iterator(); iterator.hasNext() && i < maxLen; i++)
      {
        if (i > 0)
          builder.append(", ");
        builder.append(iterator.next());
      }
    builder.append("]");
    return builder.toString();
  }

}
