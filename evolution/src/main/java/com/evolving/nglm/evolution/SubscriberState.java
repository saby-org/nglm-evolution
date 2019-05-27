/*****************************************************************************
*
*  SubscriberState.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.SubscriberTrace;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.lang.reflect.InvocationTargetException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class SubscriberState implements SubscriberStreamOutput
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
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("subscriber_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberProfile", SubscriberProfile.getSubscriberProfileSerde().schema());
    schemaBuilder.field("journeyStates", SchemaBuilder.array(JourneyState.schema()).schema());
    schemaBuilder.field("recentJourneyStates", SchemaBuilder.array(JourneyState.schema()).schema());
    schemaBuilder.field("scheduledEvaluations", SchemaBuilder.array(TimedEvaluation.schema()).schema());
    schemaBuilder.field("ucgRuleID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("ucgEpoch", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("ucgRefreshDay", Timestamp.builder().optional().schema());
    schemaBuilder.field("lastEvaluationDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("journeyRequests", SchemaBuilder.array(JourneyRequest.schema()).schema());
    schemaBuilder.field("deliveryRequests", SchemaBuilder.array(DeliveryRequest.commonSerde().schema()).schema());
    schemaBuilder.field("journeyStatistics", SchemaBuilder.array(JourneyStatistic.schema()).schema());
    schemaBuilder.field("propensityOutputs", SchemaBuilder.array(PropensityEventOutput.schema()).defaultValue(Collections.<PropensityEventOutput>emptyList()).schema());
    schemaBuilder.field("subscriberTraceMessage", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SubscriberState> serde = new ConnectSerde<SubscriberState>(schema, false, SubscriberState.class, SubscriberState::pack, SubscriberState::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberState> serde() { return serde; }

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
  private String ucgRuleID;
  private Integer ucgEpoch;
  private Date ucgRefreshDay;
  private Date lastEvaluationDate;
  private List<JourneyRequest> journeyRequests;
  private List<DeliveryRequest> deliveryRequests;
  private List<JourneyStatistic> journeyStatistics;
  private List<PropensityEventOutput> propensityOutputs;
  private SubscriberTrace subscriberTrace;

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
  public String getUCGRuleID() { return ucgRuleID; }
  public Integer getUCGEpoch() { return ucgEpoch; }
  public Date getUCGRefreshDay() { return ucgRefreshDay; }
  public Date getLastEvaluationDate() { return lastEvaluationDate; }
  public List<JourneyRequest> getJourneyRequests() { return journeyRequests; }
  public List<DeliveryRequest> getDeliveryRequests() { return deliveryRequests; }
  public List<JourneyStatistic> getJourneyStatistics() { return journeyStatistics; }
  public List<PropensityEventOutput> getPropensityOutputs() { return propensityOutputs; }
  public SubscriberTrace getSubscriberTrace() { return subscriberTrace; }

  /****************************************
  *
  *  setters
  *
  ****************************************/

  public void setLastEvaluationDate(Date lastEvaluationDate) { this.lastEvaluationDate = lastEvaluationDate; }
  public void setSubscriberTrace(SubscriberTrace subscriberTrace) { this.subscriberTrace = subscriberTrace; }
  
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
        this.ucgRuleID = null;
        this.ucgEpoch = null;
        this.ucgRefreshDay = null;
        this.lastEvaluationDate = null;
        this.journeyRequests = new ArrayList<JourneyRequest>();
        this.deliveryRequests = new ArrayList<DeliveryRequest>();
        this.journeyStatistics = new ArrayList<JourneyStatistic>();
        this.propensityOutputs = new ArrayList<PropensityEventOutput>();
        this.subscriberTrace = null;
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

  private SubscriberState(String subscriberID, SubscriberProfile subscriberProfile, Set<JourneyState> journeyStates, Set<JourneyState> recentJourneyStates, SortedSet<TimedEvaluation> scheduledEvaluations, String ucgRuleID, Integer ucgEpoch, Date ucgRefreshDay, Date lastEvaluationDate, List<JourneyRequest> journeyRequests, List<DeliveryRequest> deliveryRequests, List<JourneyStatistic> journeyStatistics, List<PropensityEventOutput> propensityOutputs, SubscriberTrace subscriberTrace)
  {
    this.subscriberID = subscriberID;
    this.subscriberProfile = subscriberProfile;
    this.journeyStates = journeyStates;
    this.recentJourneyStates = recentJourneyStates;
    this.scheduledEvaluations = scheduledEvaluations;
    this.ucgRuleID = ucgRuleID;
    this.ucgEpoch = ucgEpoch;
    this.ucgRefreshDay = ucgRefreshDay;
    this.lastEvaluationDate = lastEvaluationDate;
    this.journeyRequests = journeyRequests;
    this.deliveryRequests = deliveryRequests;
    this.journeyStatistics = journeyStatistics;
    this.propensityOutputs = propensityOutputs;
    this.subscriberTrace = subscriberTrace;
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
        this.ucgRuleID = subscriberState.getUCGRuleID();
        this.ucgEpoch = subscriberState.getUCGEpoch();
        this.ucgRefreshDay = subscriberState.getUCGRefreshDay();
        this.lastEvaluationDate = subscriberState.getLastEvaluationDate();
        this.journeyRequests = new ArrayList<JourneyRequest>(subscriberState.getJourneyRequests());
        this.deliveryRequests = new ArrayList<DeliveryRequest>(subscriberState.getDeliveryRequests());
        this.journeyStatistics = new ArrayList<JourneyStatistic>(subscriberState.getJourneyStatistics());
        this.propensityOutputs = new ArrayList<PropensityEventOutput>(subscriberState.getPropensityOutputs());
        this.subscriberTrace = subscriberState.getSubscriberTrace();

        //
        //  deep copy of journey states
        //

        this.journeyStates = new HashSet<JourneyState>();
        for (JourneyState journeyState : subscriberState.getJourneyStates())
          {
            this.journeyStates.add(new JourneyState(journeyState));
          }
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
    struct.put("ucgRuleID", subscriberState.getUCGRuleID());
    struct.put("ucgEpoch", subscriberState.getUCGEpoch());
    struct.put("ucgRefreshDay", subscriberState.getUCGRefreshDay());
    struct.put("lastEvaluationDate", subscriberState.getLastEvaluationDate());
    struct.put("journeyRequests", packJourneyRequests(subscriberState.getJourneyRequests()));
    struct.put("deliveryRequests", packDeliveryRequests(subscriberState.getDeliveryRequests()));
    struct.put("journeyStatistics", packJourneyStatistics(subscriberState.getJourneyStatistics()));
    struct.put("propensityOutputs", packPropensityOutputs(subscriberState.getPropensityOutputs()));
    struct.put("subscriberTraceMessage", subscriberState.getSubscriberTrace() != null ? subscriberState.getSubscriberTrace().getSubscriberTraceMessage() : null);
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
    for (TimedEvaluation scheduledEvaluation : scheduledEvaluations)
      {
        result.add(TimedEvaluation.pack(scheduledEvaluation));
      }
    return result;
  }

  /*****************************************
  *
  *  packJourneyRequests
  *
  *****************************************/

  private static List<Object> packJourneyRequests(List<JourneyRequest> journeyRequests)
  {
    List<Object> result = new ArrayList<Object>();
    for (JourneyRequest journeyRequest : journeyRequests)
      {
        result.add(JourneyRequest.pack(journeyRequest));
      }
    return result;
  }

  /*****************************************
  *
  *  packDeliveryRequests
  *
  *****************************************/

  private static List<Object> packDeliveryRequests(List<DeliveryRequest> deliveryRequests)
  {
    List<Object> result = new ArrayList<Object>();
    for (DeliveryRequest deliveryRequest : deliveryRequests)
      {
        result.add(DeliveryRequest.commonSerde().pack(deliveryRequest));
      }
    return result;
  }

  /*****************************************
  *
  *  packJourneyStatistics
  *
  *****************************************/

  private static List<Object> packJourneyStatistics(List<JourneyStatistic> journeyStatistics)
  {
    List<Object> result = new ArrayList<Object>();
    for (JourneyStatistic journeyStatistic : journeyStatistics)
      {
        result.add(JourneyStatistic.pack(journeyStatistic));
      }
    return result;
  }

  /*****************************************
  *
  *  packPropensityOutputs
  *
  *****************************************/

  private static List<Object> packPropensityOutputs(List<PropensityEventOutput> propensityOutputs)
  {
    List<Object> result = new ArrayList<Object>();
    for (PropensityEventOutput propensityOutput : propensityOutputs)
      {
        result.add(PropensityEventOutput.pack(propensityOutput));
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
    SortedSet<TimedEvaluation> scheduledEvaluations = unpackScheduledEvaluations(schema.field("scheduledEvaluations").schema(), valueStruct.get("scheduledEvaluations"));
    String ucgRuleID = valueStruct.getString("ucgRuleID");
    Integer ucgEpoch = valueStruct.getInt32("ucgEpoch");
    Date ucgRefreshDay = (Date) valueStruct.get("ucgRefreshDay");
    Date lastEvaluationDate = (Date) valueStruct.get("lastEvaluationDate");
    List<JourneyRequest> journeyRequests = unpackJourneyRequests(schema.field("journeyRequests").schema(), valueStruct.get("journeyRequests"));
    List<DeliveryRequest> deliveryRequests = unpackDeliveryRequests(schema.field("deliveryRequests").schema(), valueStruct.get("deliveryRequests"));
    List<JourneyStatistic> journeyStatistics = unpackJourneyStatistics(schema.field("journeyStatistics").schema(), valueStruct.get("journeyStatistics"));
    List<PropensityEventOutput> propensityOutputs = (schemaVersion >= 2) ? unpackPropensityOutputs(schema.field("propensityOutputs").schema(), valueStruct.get("propensityOutputs")) : Collections.<PropensityEventOutput>emptyList();
    SubscriberTrace subscriberTrace = valueStruct.getString("subscriberTraceMessage") != null ? new SubscriberTrace(valueStruct.getString("subscriberTraceMessage")) : null;

    //
    //  return
    //

    return new SubscriberState(subscriberID, subscriberProfile, journeyStates, recentJourneyStates, scheduledEvaluations, ucgRuleID, ucgEpoch, ucgRefreshDay, lastEvaluationDate, journeyRequests, deliveryRequests, journeyStatistics, propensityOutputs, subscriberTrace);
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
  *  unpackJourneyRequests
  *
  *****************************************/

  private static List<JourneyRequest> unpackJourneyRequests(Schema schema, Object value)
  {
    //
    //  get schema for JourneyRequest
    //

    Schema journeyRequestSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<JourneyRequest> result = new ArrayList<JourneyRequest>();
    List<Object> valueArray = (List<Object>) value;
    for (Object request : valueArray)
      {
        JourneyRequest journeyRequest = JourneyRequest.unpack(new SchemaAndValue(journeyRequestSchema, request));
        result.add(journeyRequest);
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  unpackDeliveryRequests
  *
  *****************************************/

  private static List<DeliveryRequest> unpackDeliveryRequests(Schema schema, Object value)
  {
    //
    //  get schema for DeliveryRequest
    //

    Schema deliveryRequestSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<DeliveryRequest> result = new ArrayList<DeliveryRequest>();
    List<Object> valueArray = (List<Object>) value;
    for (Object request : valueArray)
      {
        DeliveryRequest deliveryRequest = DeliveryRequest.commonSerde().unpack(new SchemaAndValue(deliveryRequestSchema, request));
        result.add(deliveryRequest);
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  unpackJourneyStatistics
  *
  *****************************************/

  private static List<JourneyStatistic> unpackJourneyStatistics(Schema schema, Object value)
  {
    //
    //  get schema for JourneyStatistic
    //

    Schema journeyStatisticSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<JourneyStatistic> result = new ArrayList<JourneyStatistic>();
    List<Object> valueArray = (List<Object>) value;
    for (Object statistic : valueArray)
      {
        JourneyStatistic journeyStatistic = JourneyStatistic.unpack(new SchemaAndValue(journeyStatisticSchema, statistic));
        result.add(journeyStatistic);
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  unpackPropensityOutputs
  *
  *****************************************/

  private static List<PropensityEventOutput> unpackPropensityOutputs(Schema schema, Object value)
  {
    //
    //  get schema for PropensityEventOutput
    //

    Schema propensityOutputsSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<PropensityEventOutput> result = new ArrayList<PropensityEventOutput>();
    List<Object> valueArray = (List<Object>) value;
    for (Object output : valueArray)
      {
        PropensityEventOutput propensityOutput = PropensityEventOutput.unpack(new SchemaAndValue(propensityOutputsSchema, output));
        result.add(propensityOutput);
      }

    //
    //  return
    //

    return result;
  }
}
