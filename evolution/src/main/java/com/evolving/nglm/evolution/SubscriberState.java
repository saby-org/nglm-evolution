/*****************************************************************************
 *
 *  SubscriberState.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.SubscriberTrace;
import com.evolving.nglm.core.Pair;

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
  private List<JourneyStatisticWrapper> journeyStatisticWrappers;
  private List<JourneyMetric> journeyMetrics;
  private List<ProfileChangeEvent> profileChangeEvents;
  private List<ProfileSegmentChangeEvent> profileSegmentChangeEvents;
  private List<ProfileLoyaltyProgramChangeEvent> profileLoyaltyProgramChangeEvents;
  private SubscriberTrace subscriberTrace;
  private ExternalAPIOutput externalAPIOutput;
  private List<TokenChange> tokenChanges;
  private List<Pair<String,MetricHistory>> notificationHistory;
  private List<VoucherChange> voucherChanges;
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
  public List<JourneyStatisticWrapper> getJourneyStatisticWrappers() { return journeyStatisticWrappers; }
  public List<JourneyMetric> getJourneyMetrics() { return journeyMetrics; }
  public List<ProfileChangeEvent> getProfileChangeEvents() { return profileChangeEvents; }
  public List<ProfileSegmentChangeEvent> getProfileSegmentChangeEvents() { return profileSegmentChangeEvents; }
  public List<ProfileLoyaltyProgramChangeEvent> getProfileLoyaltyProgramChangeEvents() { return profileLoyaltyProgramChangeEvents; }
  public SubscriberTrace getSubscriberTrace() { return subscriberTrace; }
  public ExternalAPIOutput getExternalAPIOutput() { return externalAPIOutput; }
  public List<UUID> getTrackingIDs() { return trackingIDs; }
  public List<TokenChange> getTokenChanges() { return tokenChanges; }
  public List<Pair<String,MetricHistory>> getNotificationHistory() { return notificationHistory; }
  public List<VoucherChange> getVoucherChanges() { return voucherChanges; }

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

  public void setUCGState(UCGState ucgState, Date evaluationDate)
  {
    this.ucgRuleID = ucgState.getUCGRuleID();
    this.ucgEpoch = ucgState.getRefreshEpoch();
    this.ucgRefreshDay = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
  }

  /*****************************************
   *
   *  constructor (simple)
   *
   *****************************************/

  public SubscriberState(String subscriberID)
  {
    try
      {
        this.subscriberID = subscriberID;
        this.subscriberProfile = (SubscriberProfile) SubscriberProfile.getSubscriberProfileConstructor().newInstance(subscriberID);
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
        this.journeyStatisticWrappers = new ArrayList<JourneyStatisticWrapper>();
        this.journeyMetrics = new ArrayList<JourneyMetric>();
        this.profileChangeEvents = new ArrayList<ProfileChangeEvent>();
        this.profileSegmentChangeEvents = new ArrayList<ProfileSegmentChangeEvent>();
        this.profileLoyaltyProgramChangeEvents = new ArrayList<ProfileLoyaltyProgramChangeEvent>();
        this.subscriberTrace = null;
        this.externalAPIOutput = null;
        this.kafkaRepresentation = null;
        this.trackingIDs = new ArrayList<UUID>();
        this.tokenChanges = new ArrayList<TokenChange>();
        this.notificationHistory = new ArrayList<Pair<String, MetricHistory>>();
        //put all commuication channels available. This is made in constructor to avoid verifying when delivery request is processed
        Deployment.getDeliveryTypeCommunicationChannelIDMap().forEach((deliveryType,communicationChannelId) -> notificationHistory.add(new Pair<String,MetricHistory>(communicationChannelId, new MetricHistory(MetricHistory.MINIMUM_DAY_BUCKETS,MetricHistory.MINIMUM_MONTH_BUCKETS))));
        this.voucherChanges = new ArrayList<VoucherChange>();
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

  private SubscriberState(String subscriberID, SubscriberProfile subscriberProfile, Set<JourneyState> journeyStates, Set<JourneyState> recentJourneyStates, SortedSet<TimedEvaluation> scheduledEvaluations, Set<ReScheduledDeliveryRequest> reScheduledDeliveryRequests, List<String> workflowTriggering, String ucgRuleID, Integer ucgEpoch, Date ucgRefreshDay, Date lastEvaluationDate, List<JourneyRequest> journeyRequests, List<JourneyRequest> journeyResponses, List<LoyaltyProgramRequest> loyaltyProgramRequests, List<LoyaltyProgramRequest> loyaltyProgramResponses, List<PointFulfillmentRequest> pointFulfillmentResponses, List<DeliveryRequest> deliveryRequests, List<ExecuteActionOtherSubscriber> executeActionsOtherSubscriber, List<JourneyStatisticWrapper> journeyStatisticWrappers, List<JourneyMetric> journeyMetrics, List<ProfileChangeEvent> profileChangeEvents, List<ProfileSegmentChangeEvent> profileSegmentChangeEvents, List<ProfileLoyaltyProgramChangeEvent> profileLoyaltyProgramChangeEvents, SubscriberTrace subscriberTrace, ExternalAPIOutput externalAPIOutput, List<UUID> trackingIDs, List<TokenChange> tokenChanges, List<Pair<String,MetricHistory>> notificationHistory, List<VoucherChange> voucherChanges)
  {
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
    this.journeyRequests = journeyRequests;
    this.journeyResponses = journeyResponses;
    this.loyaltyProgramRequests = loyaltyProgramRequests;
    this.loyaltyProgramResponses = loyaltyProgramResponses;
    this.pointFulfillmentResponses = pointFulfillmentResponses;
    this.deliveryRequests = deliveryRequests;
    this.journeyStatisticWrappers = journeyStatisticWrappers;
    this.journeyMetrics = journeyMetrics;
    this.profileChangeEvents = profileChangeEvents;
    this.profileSegmentChangeEvents = profileSegmentChangeEvents;
    this.profileLoyaltyProgramChangeEvents = profileLoyaltyProgramChangeEvents;
    this.subscriberTrace = subscriberTrace;
    this.externalAPIOutput = externalAPIOutput;
    this.kafkaRepresentation = null;
    this.trackingIDs = trackingIDs;
    this.tokenChanges = tokenChanges;
    this.notificationHistory = notificationHistory;
    this.voucherChanges = voucherChanges;
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
        this.journeyStatisticWrappers = new ArrayList<JourneyStatisticWrapper>(subscriberState.getJourneyStatisticWrappers());
        this.journeyMetrics = new ArrayList<JourneyMetric>(subscriberState.getJourneyMetrics());
        this.profileChangeEvents= new ArrayList<ProfileChangeEvent>(subscriberState.getProfileChangeEvents());
        this.profileSegmentChangeEvents= new ArrayList<ProfileSegmentChangeEvent>(subscriberState.getProfileSegmentChangeEvents());
        this.profileLoyaltyProgramChangeEvents= new ArrayList<ProfileLoyaltyProgramChangeEvent>(subscriberState.getProfileLoyaltyProgramChangeEvents());
        this.subscriberTrace = subscriberState.getSubscriberTrace();
        this.externalAPIOutput = subscriberState.getExternalAPIOutput();
        this.tokenChanges = subscriberState.getTokenChanges();
        this.notificationHistory = subscriberState.getNotificationHistory();
        this.voucherChanges = subscriberState.getVoucherChanges();
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

  private static Object packNotificationHistory(List<Pair<String,MetricHistory>> notificationHistory)
  {
    List<Object> result = new ArrayList<>();
    notificationHistory = (notificationHistory != null) ? notificationHistory : Collections.emptyList();
    for (Pair<String,MetricHistory> notificationStatus : notificationHistory)
      {
        Struct packedNotificationStatus = new Struct(notificationHistorySchema);
        packedNotificationStatus.put("channelID",notificationStatus.getFirstElement());
        packedNotificationStatus.put("metricHistory",MetricHistory.pack(notificationStatus.getSecondElement()));
        result.add(packedNotificationStatus);
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
    List<JourneyRequest> journeyRequests = new ArrayList<JourneyRequest>();
    List<JourneyRequest> journeyResponses = new ArrayList<JourneyRequest>();
    List<LoyaltyProgramRequest> loyaltyProgramRequests = new ArrayList<LoyaltyProgramRequest>();
    List<LoyaltyProgramRequest> loyaltyProgramResponses = new ArrayList<LoyaltyProgramRequest>();
    List<PointFulfillmentRequest> pointFulfillmentResponses = new ArrayList<PointFulfillmentRequest>();
    List<DeliveryRequest> deliveryRequests = new ArrayList<DeliveryRequest>();
    List<ExecuteActionOtherSubscriber> executeActionsOtherSubscriber = new ArrayList<ExecuteActionOtherSubscriber>();
    List<JourneyStatisticWrapper> journeyStatisticWrappers = new ArrayList<JourneyStatisticWrapper>();
    List<JourneyMetric> journeyMetrics = new ArrayList<JourneyMetric>();
    List<ProfileChangeEvent> profileChangeEvents = new ArrayList<ProfileChangeEvent>();
    List<ProfileSegmentChangeEvent> profileSegmentChangeEvents = new ArrayList<ProfileSegmentChangeEvent>();
    List<ProfileLoyaltyProgramChangeEvent> profileLoyaltyProgramChangeEvents = new ArrayList<ProfileLoyaltyProgramChangeEvent>();
    SubscriberTrace subscriberTrace = null;
    ExternalAPIOutput externalAPIOutput = null;
    List<UUID> trackingIDs = schemaVersion >= 4 ? EvolutionUtilities.getUUIDsFromBytes(valueStruct.getBytes("trackingID")) : null;
    List<TokenChange> tokenChanges = new ArrayList<TokenChange>();
    List<Pair<String,MetricHistory>> notificationHistory = schemaVersion >= 6 ? unpackNotificationHistory(valueStruct.get("notificationHistory")) : new ArrayList<Pair<String,MetricHistory>>();
    List<VoucherChange> voucherChanges = new ArrayList<VoucherChange>();

    //
    //  return
    //

    return new SubscriberState(subscriberID, subscriberProfile, journeyStates, recentJourneyStates, scheduledEvaluations, reScheduledDeliveryRequest, workflowTriggering, ucgRuleID, ucgEpoch, ucgRefreshDay, lastEvaluationDate, journeyRequests, journeyResponses, loyaltyProgramRequests, loyaltyProgramResponses,pointFulfillmentResponses, deliveryRequests, executeActionsOtherSubscriber, journeyStatisticWrappers, journeyMetrics, profileChangeEvents, profileSegmentChangeEvents, profileLoyaltyProgramChangeEvents, subscriberTrace, externalAPIOutput, trackingIDs, tokenChanges, notificationHistory, voucherChanges);
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

  private static List<Pair<String,MetricHistory>> unpackNotificationHistory(Object value)
  {
    if (value == null) return null;
    List<Pair<String,MetricHistory>> result = new ArrayList<>();
    if (value != null)
      {
        List<Object> valueMap = (List<Object>) value;
        for (Object notificationHistoryObject : valueMap)
          {
            Struct notificationHistoryStruct = (Struct)notificationHistoryObject;
            String channelID = notificationHistoryStruct.getString("channelID");
            MetricHistory metricHistory = MetricHistory.unpack(new SchemaAndValue(MetricHistory.schema(),notificationHistoryStruct.get("metricHistory")));
            result.add(new Pair<>(channelID,metricHistory));
          }
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
