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
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(11));
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("subscriberProfile", SubscriberProfile.getSubscriberProfileSerde().schema());
      schemaBuilder.field("journeyStates", SchemaBuilder.array(JourneyState.schema()).schema());
      schemaBuilder.field("recentJourneyStates", SchemaBuilder.array(JourneyState.schema()).schema());
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
  private Set<JourneyState> recentJourneyStates;
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
  private DeliveryRequest deliveryResponse;
  private List<ExecuteActionOtherSubscriber> executeActionOtherSubscribers;
  private List<VoucherAction> voucherActions;
  private List<EvolutionEngine.JourneyTriggerEventAction> journeyTriggerEventActions;
  private List<SubscriberProfileForceUpdate> subscriberProfileForceUpdates;
  //
  //  in memory only
  //

  private byte[] kafkaRepresentation = null;

  /****************************************
   *
   *  accessors - basic
   *
   ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public SubscriberProfile getSubscriberProfile() { return subscriberProfile; }
  public Set<JourneyState> getJourneyStates() { return journeyStates; }
  public Set<JourneyState> getRecentJourneyStates() { return recentJourneyStates; }
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
  public DeliveryRequest getDeliveryResponse() { return deliveryResponse; }
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
  public void setDeliveryResponse(DeliveryRequest deliveryResponse) { this.deliveryResponse = deliveryResponse; }
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
    this.ucgRefreshDay = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getDeployment(tenantID).getBaseTimeZone());
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

  private SubscriberState(String subscriberID, SubscriberProfile subscriberProfile, Set<JourneyState> journeyStates, Set<JourneyState> recentJourneyStates, SortedSet<TimedEvaluation> scheduledEvaluations, Set<ReScheduledDeliveryRequest> reScheduledDeliveryRequests, List<String> workflowTriggering, String ucgRuleID, Integer ucgEpoch, Date ucgRefreshDay, Date lastEvaluationDate, List<UUID> trackingIDs, Map<String,MetricHistory> notificationHistory)
  {
    // stored
    this.subscriberID = subscriberID;
    this.subscriberProfile = subscriberProfile;
    this.journeyStates = journeyStates;
    this.recentJourneyStates = recentJourneyStates;
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
  }

  /*****************************************
   *
   *  constructor (copy)
   *
   *****************************************/

  public SubscriberState(SubscriberState subscriberState)
  {
    try
      {
        //
        //  simple fields
        //

        this.subscriberID = subscriberState.getSubscriberID();
        this.subscriberProfile = (SubscriberProfile) SubscriberProfile.getSubscriberProfileCopyConstructor().newInstance(subscriberState.getSubscriberProfile());
        this.recentJourneyStates = new HashSet<JourneyState>(subscriberState.getRecentJourneyStates());
        this.scheduledEvaluations = new TreeSet<TimedEvaluation>(subscriberState.getScheduledEvaluations());
        this.reScheduledDeliveryRequests = new HashSet<ReScheduledDeliveryRequest>(subscriberState.getReScheduledDeliveryRequests());
        this.workflowTriggering = new ArrayList<>(subscriberState.getWorkflowTriggering());
        this.ucgRuleID = subscriberState.getUCGRuleID();
        this.ucgEpoch = subscriberState.getUCGEpoch();
        this.ucgRefreshDay = subscriberState.getUCGRefreshDay();
        this.lastEvaluationDate = subscriberState.getLastEvaluationDate();
        this.kafkaRepresentation = null;
        this.trackingIDs = subscriberState.getTrackingIDs();
        
        //
        //  deep copy of journey states
        //

        this.journeyStates = new HashSet<JourneyState>();
        for (JourneyState journeyState : subscriberState.getJourneyStates())
          {
            this.journeyStates.add(new JourneyState(journeyState));
          }

        //
        // shallow copy of all temporary lists. Elements of those lists are considered immutable.
        // Observation: a cleaning of those temporary lists is made after the only call to this copy constructor. 
        //
        
        this.journeyRequests = new ArrayList<JourneyRequest>(subscriberState.getJourneyRequests());
        this.journeyResponses = new ArrayList<JourneyRequest>(subscriberState.getJourneyResponses());
        this.loyaltyProgramRequests = new ArrayList<LoyaltyProgramRequest>(subscriberState.getLoyaltyProgramRequests());
        this.loyaltyProgramResponses = new ArrayList<LoyaltyProgramRequest>(subscriberState.getLoyaltyProgramResponses());
        this.pointFulfillmentResponses= new ArrayList<PointFulfillmentRequest>(subscriberState.getPointFulfillmentResponses());
        this.deliveryRequests = new ArrayList<DeliveryRequest>(subscriberState.getDeliveryRequests());
        this.journeyStatisticWrappers = new ArrayList<JourneyStatistic>(subscriberState.getJourneyStatisticWrappers());
        this.journeyMetrics = new ArrayList<JourneyMetric>(subscriberState.getJourneyMetrics());
        this.profileChangeEvents= new ArrayList<ProfileChangeEvent>(subscriberState.getProfileChangeEvents());
        this.profileSegmentChangeEvents= new ArrayList<ProfileSegmentChangeEvent>(subscriberState.getProfileSegmentChangeEvents());
        this.profileLoyaltyProgramChangeEvents= new ArrayList<ProfileLoyaltyProgramChangeEvent>(subscriberState.getProfileLoyaltyProgramChangeEvents());
        this.subscriberTrace = subscriberState.getSubscriberTrace();
        this.externalAPIOutput = subscriberState.getExternalAPIOutput();
        this.tokenChanges = subscriberState.getTokenChanges();
        this.notificationHistory = subscriberState.getNotificationHistory();
        this.voucherChanges = subscriberState.getVoucherChanges();
        this.executeActionOtherSubscribers = subscriberState.getExecuteActionOtherSubscribers();
        this.voucherActions = subscriberState.getVoucherActions();
        this.journeyTriggerEventActions = subscriberState.getJourneyTriggerEventActions();
        this.subscriberProfileForceUpdates = subscriberState.getSubscriberProfileForceUpdates();
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
    struct.put("recentJourneyStates", packJourneyStates(subscriberState.getRecentJourneyStates()));
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
    Set<JourneyState> recentJourneyStates = unpackJourneyStates(schema.field("recentJourneyStates").schema(), valueStruct.get("recentJourneyStates"));
    SortedSet<TimedEvaluation> scheduledEvaluations = (schemaVersion >= 10) ? unpackScheduledEvaluations(schema.field("scheduledEvaluations").schema(), valueStruct.get("scheduledEvaluations")) : new TreeSet<>();
    Set<ReScheduledDeliveryRequest> reScheduledDeliveryRequest = (schemaVersion >= 7) ? unpackReScheduledDeliveryRequests(schema.field("reScheduledDeliveryRequests").schema(), valueStruct.get("reScheduledDeliveryRequests")) : new HashSet<ReScheduledDeliveryRequest>();
    List<String> workflowTriggering = schema.field("workflowTriggering") != null ? (ArrayList<String>) valueStruct.get("workflowTriggering") : new ArrayList<>();
    String ucgRuleID = valueStruct.getString("ucgRuleID");
    Integer ucgEpoch = valueStruct.getInt32("ucgEpoch");
    Date ucgRefreshDay = (Date) valueStruct.get("ucgRefreshDay");
    Date lastEvaluationDate = (Date) valueStruct.get("lastEvaluationDate");
    List<UUID> trackingIDs = schemaVersion >= 4 ? EvolutionUtilities.getUUIDsFromBytes(valueStruct.getBytes("trackingID")) : null;
    Map<String,MetricHistory> notificationHistory = schemaVersion >= 6 ? unpackNotificationHistory(new SchemaAndValue(schema.field("notificationHistory").schema(),valueStruct.get("notificationHistory"))) : new HashMap<>();

    //
    //  return
    //

    return new SubscriberState(subscriberID, subscriberProfile, journeyStates, recentJourneyStates, scheduledEvaluations, reScheduledDeliveryRequest, workflowTriggering, ucgRuleID, ucgEpoch, ucgRefreshDay, lastEvaluationDate, trackingIDs, notificationHistory);
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

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Struct valueStruct = (Struct) value;

    Map<String,MetricHistory> result = new HashMap<>();
    List<Object> valueMap = (List<Object>) value;
    for (Object notificationHistoryObject : valueMap)
      {
        String channelID = valueStruct.getString("channelID");
        MetricHistory metricHistory = MetricHistory.unpack(new SchemaAndValue(schema.field("metricHistory").schema(),valueStruct.get("metricHistory")));
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
