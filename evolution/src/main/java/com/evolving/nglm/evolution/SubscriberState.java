/*****************************************************************************
 *
 *  SubscriberState.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberTrace;

public class SubscriberState implements StateStore
{
  /*****************************************
   *
   *  static
   *
   *****************************************/

  public static void forceClassLoad() {}

  /*****************************************
   *
   *  schema
   *
   *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  private static Schema notificationHistorySchema = null;

  static
    {

      SchemaBuilder notificationHistorySchemaBuilder = SchemaBuilder.struct();
      notificationHistorySchemaBuilder.name("notification_history");
      notificationHistorySchemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      notificationHistorySchemaBuilder.field("channelID",Schema.STRING_SCHEMA);
      notificationHistorySchemaBuilder.field("metricHistory",MetricHistory.schema());
      notificationHistorySchema = notificationHistorySchemaBuilder.build();

      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("subscriber_state");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(12));
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("subscriberProfile", SubscriberProfile.getSubscriberProfileSerde().schema());
      schemaBuilder.field("journeyStates", SchemaBuilder.array(JourneyState.schema()).schema());
      schemaBuilder.field("journeyEndedStates", SchemaBuilder.array(JourneyEndedState.schema()).schema());
      schemaBuilder.field("scheduledEvaluations", SchemaBuilder.array(TimedEvaluation.schema()).optional());
      schemaBuilder.field("reScheduledDeliveryRequests", SchemaBuilder.array(ReScheduledDeliveryRequest.schema()).defaultValue(Collections.<ReScheduledDeliveryRequest>emptyList()).schema());
      schemaBuilder.field("workflowTriggering", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().schema());
      schemaBuilder.field("ucgRuleID", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("ucgEpoch", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("ucgRefreshDay", Timestamp.builder().optional().schema());
      schemaBuilder.field("lastEvaluationDate", Timestamp.builder().optional().schema());
      schemaBuilder.field("trackingID", Schema.OPTIONAL_BYTES_SCHEMA);
      schemaBuilder.field("notificationHistory",SchemaBuilder.array(notificationHistorySchema).optional());

      schema = schemaBuilder.build();
    };

  //
  //  serde
  //

  private static ConnectSerde<SubscriberState> serde = new ConnectSerde<SubscriberState>(schema, false, SubscriberState.class, SubscriberState::pack, SubscriberState::unpack);
  private static StateStoreSerde<SubscriberState> stateStoreSerde = new StateStoreSerde<SubscriberState>(serde);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberState> serde() { return serde; }
  public static StateStoreSerde<SubscriberState> stateStoreSerde() { return stateStoreSerde; }

  /****************************************
   *
   *  data
   *
   ****************************************/

  private String subscriberID;
  private SubscriberProfile subscriberProfile;
  private Set<JourneyState> journeyStates;
  private Set<JourneyEndedState> journeyEndedStates;
  private SortedSet<TimedEvaluation> scheduledEvaluations;
  private Set<ReScheduledDeliveryRequest> reScheduledDeliveryRequests;
  private List<String> workflowTriggering; // List of workflow to trigger on some event: <EventClassName>:<long eventDate>:<WorkflowID>:<sourceFeatureID>
  private String ucgRuleID;
  private Integer ucgEpoch;
  private Date ucgRefreshDay;
  private Date lastEvaluationDate;
  private List<UUID> trackingIDs;

  
  //
  // temporary lists for SubscriberStreamOutput extraction (see getEvolutionEngineOutputs)
  //
  
  private List<JourneyRequest> journeyRequests;
  private List<JourneyRequest> journeyResponses;
  private List<LoyaltyProgramRequest> loyaltyProgramRequests;
  private List<LoyaltyProgramRequest> loyaltyProgramResponses;
  private List<PointFulfillmentRequest> pointFulfillmentResponses;
  private List<DeliveryRequest> deliveryRequests;
  private List<JourneyStatistic> journeyStatisticWrappers;
  private List<JourneyMetric> journeyMetrics;
  private List<ProfileChangeEvent> profileChangeEvents;
  private List<ProfileSegmentChangeEvent> profileSegmentChangeEvents;
  private List<ProfileLoyaltyProgramChangeEvent> profileLoyaltyProgramChangeEvents;
  private SubscriberTrace subscriberTrace;
  private ExternalAPIOutput externalAPIOutput;
  private List<TokenChange> tokenChanges;
  private Map<String,MetricHistory> notificationHistory;
  private List<VoucherChange> voucherChanges;
  private List<ExecuteActionOtherSubscriber> executeActionOtherSubscribers;
  private List<VoucherAction> voucherActions;
  private List<EvolutionEngine.JourneyTriggerEventAction> journeyTriggerEventActions;
  private List<SubscriberProfileForceUpdate> subscriberProfileForceUpdates;
  //
  //  in memory only
  //

  private byte[] kafkaRepresentation = null;

  //TODO: remove once all got EVPRO-885 running (it is unpack once to migrate data, then not pack back)
  private Set<JourneyState> recentJourneyStates;
  @Deprecated public Set<JourneyState> getOldRecentJourneyStates() { return recentJourneyStates; }


  /****************************************
   *
   *  accessors - basic
   *
   ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public SubscriberProfile getSubscriberProfile() { return subscriberProfile; }
  public Set<JourneyState> getJourneyStates() { return journeyStates; }
  public Set<JourneyEndedState> getJourneyEndedStates() { return journeyEndedStates; }
  public SortedSet<TimedEvaluation> getScheduledEvaluations() { return scheduledEvaluations; }
  public Set<ReScheduledDeliveryRequest> getReScheduledDeliveryRequests() { return reScheduledDeliveryRequests; }
  public List<String> getWorkflowTriggering() { return workflowTriggering; }
  public String getUCGRuleID() { return ucgRuleID; }
  public Integer getUCGEpoch() { return ucgEpoch; }
  public Date getUCGRefreshDay() { return ucgRefreshDay; }
  public Date getLastEvaluationDate() { return lastEvaluationDate; }
  public List<JourneyRequest> getJourneyRequests() { return journeyRequests; }
  public List<JourneyRequest> getJourneyResponses() { return journeyResponses; }
  public List<LoyaltyProgramRequest> getLoyaltyProgramRequests() { return loyaltyProgramRequests; }
  public List<LoyaltyProgramRequest> getLoyaltyProgramResponses() { return loyaltyProgramResponses; }
  public List<PointFulfillmentRequest> getPointFulfillmentResponses() { return pointFulfillmentResponses; }
  public List<DeliveryRequest> getDeliveryRequests() { return deliveryRequests; }
  public List<JourneyStatistic> getJourneyStatisticWrappers() { return journeyStatisticWrappers; }
  public List<JourneyMetric> getJourneyMetrics() { return journeyMetrics; }
  public List<ProfileChangeEvent> getProfileChangeEvents() { return profileChangeEvents; }
  public List<ProfileSegmentChangeEvent> getProfileSegmentChangeEvents() { return profileSegmentChangeEvents; }
  public List<ProfileLoyaltyProgramChangeEvent> getProfileLoyaltyProgramChangeEvents() { return profileLoyaltyProgramChangeEvents; }
  public SubscriberTrace getSubscriberTrace() { return subscriberTrace; }
  public ExternalAPIOutput getExternalAPIOutput() { return externalAPIOutput; }
  public List<UUID> getTrackingIDs() { return trackingIDs; }
  public List<TokenChange> getTokenChanges() { return tokenChanges; }
  public Map<String,MetricHistory> getNotificationHistory() { return notificationHistory; }
  public List<VoucherChange> getVoucherChanges() { return voucherChanges; }
  public List<ExecuteActionOtherSubscriber> getExecuteActionOtherSubscribers() { return executeActionOtherSubscribers; }
  public List<VoucherAction> getVoucherActions() { return voucherActions; }
  public List<EvolutionEngine.JourneyTriggerEventAction> getJourneyTriggerEventActions() { return journeyTriggerEventActions; }
  public List<SubscriberProfileForceUpdate> getSubscriberProfileForceUpdates() { return subscriberProfileForceUpdates; }

  //
  //  kafkaRepresentation
  //

  @Override public void setKafkaRepresentation(byte[] kafkaRepresentation) { this.kafkaRepresentation = kafkaRepresentation; }
  @Override public byte[] getKafkaRepresentation() { return kafkaRepresentation; }

  /****************************************
   *
   *  setters
   *
   ****************************************/

  public void setLastEvaluationDate(Date lastEvaluationDate) { this.lastEvaluationDate = lastEvaluationDate; }
  public void setSubscriberTrace(SubscriberTrace subscriberTrace) { this.subscriberTrace = subscriberTrace; }
  public void setExternalAPIOutput(ExternalAPIOutput externalAPIOutput) { this.externalAPIOutput = externalAPIOutput; }
  public void setTokenChanges(List<TokenChange> tokenChanges) { this.tokenChanges = tokenChanges; }
  public void setTrackingID(UUID trackingID)
  {
    if(trackingID == null)
      {
        return;
      }
    byte[] trackingIDBytes = EvolutionUtilities.getBytesFromUUID(trackingID);
    byte[] savedTrackingIDBytes = EvolutionUtilities.getBytesFromUUIDs(this.trackingIDs);

    //here, need to check if the same source ID exists. If true then replace the UUID with the existing one
    int replacePos = -1;
    for(int i = 0; i < savedTrackingIDBytes.length; i += 16)
      {
        if(savedTrackingIDBytes[i] == trackingIDBytes[0])
          {
            replacePos = i;
            break;
          }
      }
    if( replacePos > -1)
      {
        this.trackingIDs.set(replacePos / 16, trackingID);
      }
    else
      {
        this.trackingIDs.add(trackingID);
      }
  }

  //
  //  setUCGState
  //

  public void setUCGState(UCGState ucgState, Date evaluationDate, int tenantID)
  {
    this.ucgRuleID = ucgState.getUCGRuleID();
    this.ucgEpoch = ucgState.getRefreshEpoch();
    this.ucgRefreshDay = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Deployment.getDeployment(tenantID).getTimeZone());
  }

  /*****************************************
   *
   *  constructor (simple)
   *
   *****************************************/

  public SubscriberState(String subscriberID, int tenantID)
  {
    try
      {
        this.subscriberID = subscriberID;
        this.subscriberProfile = (SubscriberProfile) SubscriberProfile.getSubscriberProfileConstructor().newInstance(subscriberID, tenantID);
        this.journeyStates = new HashSet<JourneyState>();
        this.journeyEndedStates = new HashSet<>();
        this.recentJourneyStates = new HashSet<JourneyState>();
        this.scheduledEvaluations = new TreeSet<TimedEvaluation>();
        this.reScheduledDeliveryRequests = new HashSet<ReScheduledDeliveryRequest>();
        this.workflowTriggering = new ArrayList<>();
        this.ucgRuleID = null;
        this.ucgEpoch = null;
        this.ucgRefreshDay = null;
        this.lastEvaluationDate = null;
        this.journeyRequests = new ArrayList<JourneyRequest>();
        this.journeyResponses = new ArrayList<JourneyRequest>();
        this.loyaltyProgramRequests = new ArrayList<LoyaltyProgramRequest>();
        this.loyaltyProgramResponses = new ArrayList<LoyaltyProgramRequest>();
        this.pointFulfillmentResponses = new ArrayList<PointFulfillmentRequest>();
        this.deliveryRequests = new ArrayList<DeliveryRequest>();
        this.journeyStatisticWrappers = new ArrayList<JourneyStatistic>();
        this.journeyMetrics = new ArrayList<JourneyMetric>();
        this.profileChangeEvents = new ArrayList<ProfileChangeEvent>();
        this.profileSegmentChangeEvents = new ArrayList<ProfileSegmentChangeEvent>();
        this.profileLoyaltyProgramChangeEvents = new ArrayList<ProfileLoyaltyProgramChangeEvent>();
        this.subscriberTrace = null;
        this.externalAPIOutput = null;
        this.kafkaRepresentation = null;
        this.trackingIDs = new ArrayList<UUID>();
        this.tokenChanges = new ArrayList<TokenChange>();
        this.notificationHistory = new HashMap<>();
        this.voucherChanges = new ArrayList<VoucherChange>();
        this.executeActionOtherSubscribers = new ArrayList<>();
        this.voucherActions = new ArrayList<>();
        this.journeyTriggerEventActions = new ArrayList<>();
        this.subscriberProfileForceUpdates = new ArrayList<>();
      }
    catch (InvocationTargetException e)
      {
        throw new RuntimeException(e.getCause());
      }
    catch (InstantiationException|IllegalAccessException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
   *
   *  constructor (unpack)
   *
   *****************************************/

  private SubscriberState(String subscriberID, SubscriberProfile subscriberProfile, Set<JourneyState> journeyStates, Set<JourneyEndedState> journeyEndedStates, Set<JourneyState> oldRecentJourneyStates, SortedSet<TimedEvaluation> scheduledEvaluations, Set<ReScheduledDeliveryRequest> reScheduledDeliveryRequests, List<String> workflowTriggering, String ucgRuleID, Integer ucgEpoch, Date ucgRefreshDay, Date lastEvaluationDate, List<UUID> trackingIDs, Map<String,MetricHistory> notificationHistory)
  {
    // stored
    this.subscriberID = subscriberID;
    this.subscriberProfile = subscriberProfile;
    this.journeyStates = journeyStates;
    this.journeyEndedStates = journeyEndedStates;
    this.scheduledEvaluations = scheduledEvaluations;
    this.reScheduledDeliveryRequests = reScheduledDeliveryRequests;
    this.workflowTriggering = workflowTriggering;
    this.ucgRuleID = ucgRuleID;
    this.ucgEpoch = ucgEpoch;
    this.ucgRefreshDay = ucgRefreshDay;
    this.lastEvaluationDate = lastEvaluationDate;
    this.trackingIDs = trackingIDs;
    this.notificationHistory = notificationHistory;
    // not stored
    this.journeyRequests = new ArrayList<>();
    this.journeyResponses = new ArrayList<>();
    this.loyaltyProgramRequests = new ArrayList<>();
    this.loyaltyProgramResponses = new ArrayList<>();
    this.pointFulfillmentResponses = new ArrayList<>();
    this.deliveryRequests = new ArrayList<>();
    this.journeyStatisticWrappers = new ArrayList<>();
    this.journeyMetrics = new ArrayList<>();
    this.profileChangeEvents = new ArrayList<>();
    this.profileSegmentChangeEvents = new ArrayList<>();
    this.profileLoyaltyProgramChangeEvents = new ArrayList<>();
    this.subscriberTrace = null;
    this.externalAPIOutput = null;
    this.kafkaRepresentation = null;
    this.tokenChanges = new ArrayList<>();
    this.voucherChanges = new ArrayList<>();
    this.executeActionOtherSubscribers = new ArrayList<>();
    this.voucherActions = new ArrayList<>();
    this.journeyTriggerEventActions = new ArrayList<>();
    this.subscriberProfileForceUpdates = new ArrayList<>();
    // for data migration purpose only, can be removed once all market run EVPRO-885
    this.recentJourneyStates = oldRecentJourneyStates;
  }

  /*****************************************
   *
   *  pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    SubscriberState subscriberState = (SubscriberState) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", subscriberState.getSubscriberID());
    struct.put("subscriberProfile", SubscriberProfile.getSubscriberProfileSerde().pack(subscriberState.getSubscriberProfile()));
    struct.put("journeyStates", packJourneyStates(subscriberState.getJourneyStates()));
    struct.put("journeyEndedStates", packJourneyEndedStates(subscriberState.getJourneyEndedStates()));
    struct.put("scheduledEvaluations", packScheduledEvaluations(subscriberState.getScheduledEvaluations()));
    struct.put("reScheduledDeliveryRequests", packReScheduledDeliveryRequests(subscriberState.getReScheduledDeliveryRequests()));
    struct.put("workflowTriggering", subscriberState.getWorkflowTriggering() != null ? subscriberState.getWorkflowTriggering() : new ArrayList<>());
    struct.put("ucgRuleID", subscriberState.getUCGRuleID());
    struct.put("ucgEpoch", subscriberState.getUCGEpoch());
    struct.put("ucgRefreshDay", subscriberState.getUCGRefreshDay());
    struct.put("lastEvaluationDate", subscriberState.getLastEvaluationDate());
    struct.put("trackingID", EvolutionUtilities.getBytesFromUUIDs(subscriberState.getTrackingIDs()));
    struct.put("notificationHistory", packNotificationHistory(subscriberState.getNotificationHistory()));
    return struct;
  }

  /*****************************************
   *
   *  packJourneyStates
   *
   *****************************************/

  private static List<Object> packJourneyStates(Set<JourneyState> journeyStates)
  {
    List<Object> result = new ArrayList<Object>();
    for (JourneyState journeyState : journeyStates)
      {
        result.add(JourneyState.pack(journeyState));
      }
    return result;
  }

  /*****************************************
   *
   *  packJourneyEndeds
   *
   *****************************************/

  private static List<Object> packJourneyEndedStates(Set<JourneyEndedState> journeyEndedStates)
  {
    List<Object> result = new ArrayList<Object>();
    for (JourneyEndedState journeyEndedState : journeyEndedStates)
      {
        result.add(JourneyEndedState.pack(journeyEndedState));
      }
    return result;
  }

  /*****************************************
   *
   *  packScheduledEvaluations
   *
   *****************************************/

  private static List<Object> packScheduledEvaluations(SortedSet<TimedEvaluation> scheduledEvaluations)
  {
    List<Object> result = new ArrayList<Object>();
    if(scheduledEvaluations != null)
      {
      for (TimedEvaluation scheduledEvaluation : scheduledEvaluations)
        {
          result.add(TimedEvaluation.pack(scheduledEvaluation));
        }
      }
    return result;
  }
  
  /*****************************************
  *
  *  packReScheduledDeliveryRequests
  *
  *****************************************/

  private static List<Object> packReScheduledDeliveryRequests(Set<ReScheduledDeliveryRequest> reScheduledDeliveryRequests)
  {
    List<Object> result = new ArrayList<Object>();
    for (ReScheduledDeliveryRequest reScheduledDeliveryRequest : reScheduledDeliveryRequests)
      {
        result.add(ReScheduledDeliveryRequest.pack(reScheduledDeliveryRequest));
      }
    return result;
  }

  /****************************************
   *
   *  packNotificationStatus
   *
   ****************************************/

  private static Object packNotificationHistory(Map<String,MetricHistory> notificationHistory)
  {
    List<Object> result = new ArrayList<>();//Map stored as List<Pair<String,MetricHistory>>
    notificationHistory = (notificationHistory != null) ? notificationHistory : Collections.emptyMap();
    for (Map.Entry entry : notificationHistory.entrySet())
      {
        Struct packedEntry = new Struct(notificationHistorySchema);
        packedEntry.put("channelID",entry.getKey());
        packedEntry.put("metricHistory",MetricHistory.pack(entry.getValue()));
        result.add(packedEntry);
      }
    return result;
  }


  /*****************************************
   *
   *  unpack
   *
   *****************************************/

  public static SubscriberState unpack(SchemaAndValue schemaAndValue)
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
    String subscriberID = valueStruct.getString("subscriberID");
    SubscriberProfile subscriberProfile = SubscriberProfile.getSubscriberProfileSerde().unpack(new SchemaAndValue(schema.field("subscriberProfile").schema(), valueStruct.get("subscriberProfile")));
    Set<JourneyState> journeyStates = unpackJourneyStates(schema.field("journeyStates").schema(), valueStruct.get("journeyStates"));
    SortedSet<TimedEvaluation> scheduledEvaluations = (schemaVersion >= 10) ? unpackScheduledEvaluations(schema.field("scheduledEvaluations").schema(), valueStruct.get("scheduledEvaluations")) : new TreeSet<>();
    Set<ReScheduledDeliveryRequest> reScheduledDeliveryRequest = (schemaVersion >= 7) ? unpackReScheduledDeliveryRequests(schema.field("reScheduledDeliveryRequests").schema(), valueStruct.get("reScheduledDeliveryRequests")) : new HashSet<ReScheduledDeliveryRequest>();
    List<String> workflowTriggering = schema.field("workflowTriggering") != null ? (ArrayList<String>) valueStruct.get("workflowTriggering") : new ArrayList<>();
    String ucgRuleID = valueStruct.getString("ucgRuleID");
    Integer ucgEpoch = valueStruct.getInt32("ucgEpoch");
    Date ucgRefreshDay = (Date) valueStruct.get("ucgRefreshDay");
    Date lastEvaluationDate = (Date) valueStruct.get("lastEvaluationDate");
    List<UUID> trackingIDs = schemaVersion >= 4 ? EvolutionUtilities.getUUIDsFromBytes(valueStruct.getBytes("trackingID")) : null;
    Map<String,MetricHistory> notificationHistory = schemaVersion >= 6 ? unpackNotificationHistory(new SchemaAndValue(schema.field("notificationHistory").schema(),valueStruct.get("notificationHistory"))) : new HashMap<>();

    Set<JourneyState> oldRecentJourneyStates = new HashSet<>();
    Set<JourneyEndedState> journeyEndedStates = new HashSet<>();
    if(schema.field("journeyEndedStates")!=null){
      // the new way
      journeyEndedStates = unpackJourneyEndedStates(schema.field("journeyEndedStates").schema(), valueStruct.get("journeyEndedStates"));
    }else{
      // or the unpack for the old way
      oldRecentJourneyStates = unpackJourneyStates(schema.field("recentJourneyStates").schema(), valueStruct.get("recentJourneyStates"));
      for(JourneyState oldJourneyState:oldRecentJourneyStates){
        // object is there only for journey metrics now, I'm assuming we still need it only if there is prior or during metrics data, but not yet post
        if( (!oldJourneyState.getJourneyMetricsPrior().isEmpty() || !oldJourneyState.getJourneyMetricsDuring().isEmpty()) && oldJourneyState.getJourneyMetricsPost().isEmpty() ){
          journeyEndedStates.add(oldJourneyState.getJourneyEndedState());
        }
      }
    }
    //
    //  return
    //

    return new SubscriberState(subscriberID, subscriberProfile, journeyStates, journeyEndedStates, oldRecentJourneyStates, scheduledEvaluations, reScheduledDeliveryRequest, workflowTriggering, ucgRuleID, ucgEpoch, ucgRefreshDay, lastEvaluationDate, trackingIDs, notificationHistory);
  }

  /*****************************************
   *
   *  unpackJourneyStates
   *
   *****************************************/

  private static Set<JourneyState> unpackJourneyStates(Schema schema, Object value)
  {
    //
    //  get schema for JourneyState
    //

    Schema journeyStateSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<JourneyState> result = new HashSet<JourneyState>();
    List<Object> valueArray = (List<Object>) value;
    for (Object state : valueArray)
      {
        JourneyState journeyState = JourneyState.unpack(new SchemaAndValue(journeyStateSchema, state));
        result.add(journeyState);
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
   *
   *  unpackJourneyEndedStates
   *
   *****************************************/

  private static Set<JourneyEndedState> unpackJourneyEndedStates(Schema schema, Object value)
  {
    //
    //  get schema for JourneyEndedState
    //

    Schema journeyEndedStateSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<JourneyEndedState> result = new HashSet<>();
    List<Object> valueArray = (List<Object>) value;
    for (Object state : valueArray)
      {
        JourneyEndedState journeyEndedState = JourneyEndedState.unpack(new SchemaAndValue(journeyEndedStateSchema, state));
        result.add(journeyEndedState);
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
   *
   *  unpackScheduledEvaluations
   *
   *****************************************/

  private static SortedSet<TimedEvaluation> unpackScheduledEvaluations(Schema schema, Object value)
  {
    
    if(value == null) {
      return new TreeSet<>();
    }
    
    //
    //  get schema for TimedEvaluation
    //

    Schema timedEvaluationSchema = schema.valueSchema();

    //
    //  unpack
    //

    SortedSet<TimedEvaluation> result = new TreeSet<TimedEvaluation>();
    List<Object> valueArray = (List<Object>) value;
    for (Object scheduledEvaluation : valueArray)
      {
        result.add(TimedEvaluation.unpack(new SchemaAndValue(timedEvaluationSchema, scheduledEvaluation)));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  unpackReScheduledDeliveryRequests
  *
  *****************************************/

  private static Set<ReScheduledDeliveryRequest> unpackReScheduledDeliveryRequests(Schema schema, Object value)
  {
    //
    //  get schema for TimedEvaluation
    //

    Schema reScheduledDeliveryRequestSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<ReScheduledDeliveryRequest> result = new HashSet<ReScheduledDeliveryRequest>();
    List<Object> valueArray = (List<Object>) value;
    for (Object reScheduledDeliveryRequest : valueArray)
      {
        result.add(ReScheduledDeliveryRequest.unpack(new SchemaAndValue(reScheduledDeliveryRequestSchema, reScheduledDeliveryRequest)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
   *
   *  unpackNotificationHistory
   *
   *****************************************/
  private static Map<String,MetricHistory> unpackNotificationHistory(SchemaAndValue schemaAndValue)
  {

    List<Struct> schemasAndValues = (List<Struct>) schemaAndValue.value();

    Map<String,MetricHistory> result = new HashMap<>();
    for (Struct value : schemasAndValues)
      {
        Schema schema = value.schema();
        String channelID = value.getString("channelID");
        MetricHistory metricHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("metricHistory").schema(),value.get("metricHistory")));
        result.put(channelID,metricHistory);
      }
    return result;
  }

  @Override
  public String toString()
  {
    return "SubscriberState [" + (subscriberID != null ? "subscriberID=" + subscriberID + ", " : "") + (subscriberProfile != null ? "subscriberProfile=" + subscriberProfile + ", " : "") + (journeyStates != null ? "journeyStates=" + journeyStates + ", " : "") + (recentJourneyStates != null ? "recentJourneyStates=" + recentJourneyStates + ", " : "") + (scheduledEvaluations != null ? "scheduledEvaluations=" + scheduledEvaluations + ", " : "")
        + (reScheduledDeliveryRequests != null ? "reScheduledDeliveryRequests=" + reScheduledDeliveryRequests + ", " : "") + (workflowTriggering != null ? "workflowTriggering=" + workflowTriggering + ", " : "") + (ucgRuleID != null ? "ucgRuleID=" + ucgRuleID + ", " : "") + (ucgEpoch != null ? "ucgEpoch=" + ucgEpoch + ", " : "") + (ucgRefreshDay != null ? "ucgRefreshDay=" + ucgRefreshDay + ", " : "") + (lastEvaluationDate != null ? "lastEvaluationDate=" + lastEvaluationDate + ", " : "") + (trackingIDs != null ? "trackingIDs=" + trackingIDs + ", " : "")
        + (journeyRequests != null ? "journeyRequests=" + journeyRequests + ", " : "") + (journeyResponses != null ? "journeyResponses=" + journeyResponses + ", " : "") + (loyaltyProgramRequests != null ? "loyaltyProgramRequests=" + loyaltyProgramRequests + ", " : "") + (loyaltyProgramResponses != null ? "loyaltyProgramResponses=" + loyaltyProgramResponses + ", " : "") + (pointFulfillmentResponses != null ? "pointFulfillmentResponses=" + pointFulfillmentResponses + ", " : "")
        + (deliveryRequests != null ? "deliveryRequests=" + deliveryRequests + ", " : "") + (journeyStatisticWrappers != null ? "journeyStatisticWrappers=" + journeyStatisticWrappers + ", " : "") + (journeyMetrics != null ? "journeyMetrics=" + journeyMetrics + ", " : "") + (profileChangeEvents != null ? "profileChangeEvents=" + profileChangeEvents + ", " : "") + (profileSegmentChangeEvents != null ? "profileSegmentChangeEvents=" + profileSegmentChangeEvents + ", " : "")
        + (profileLoyaltyProgramChangeEvents != null ? "profileLoyaltyProgramChangeEvents=" + profileLoyaltyProgramChangeEvents + ", " : "") + (subscriberTrace != null ? "subscriberTrace=" + subscriberTrace + ", " : "") + (externalAPIOutput != null ? "externalAPIOutput=" + externalAPIOutput + ", " : "") + (tokenChanges != null ? "tokenChanges=" + tokenChanges + ", " : "") + (notificationHistory != null ? "notificationHistory=" + notificationHistory + ", " : "")
        + (voucherChanges != null ? "voucherChanges=" + voucherChanges : "") + "]";
  }


}
