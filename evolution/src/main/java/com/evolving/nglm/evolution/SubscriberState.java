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

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberProfile", SubscriberProfile.getSubscriberProfileSerde().schema());
    schemaBuilder.field("journeyStates", SchemaBuilder.array(JourneyState.schema()).schema());
    schemaBuilder.field("evolutionSubscriberStatusUpdated", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("deliveryRequests", SchemaBuilder.array(DeliveryRequest.commonSerde().schema()).schema());
    schemaBuilder.field("journeyStatistics", SchemaBuilder.array(JourneyStatistic.schema()).schema());
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
  private boolean evolutionSubscriberStatusUpdated;
  private List<DeliveryRequest> deliveryRequests;
  private List<JourneyStatistic> journeyStatistics;
  private SubscriberTrace subscriberTrace;

  /****************************************
  *
  *  accessors - basic
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public SubscriberProfile getSubscriberProfile() { return subscriberProfile; }
  public Set<JourneyState> getJourneyStates() { return journeyStates; }
  public boolean getEvolutionSubscriberStatusUpdated() { return evolutionSubscriberStatusUpdated; }
  public List<DeliveryRequest> getDeliveryRequests() { return deliveryRequests; }
  public List<JourneyStatistic> getJourneyStatistics() { return journeyStatistics; }
  public SubscriberTrace getSubscriberTrace() { return subscriberTrace; }

  /****************************************
  *
  *  setters
  *
  ****************************************/

  public void setEvolutionSubscriberStatusUpdated(boolean evolutionSubscriberStatusUpdated) { this.evolutionSubscriberStatusUpdated = evolutionSubscriberStatusUpdated; }
  public void setSubscriberTrace(SubscriberTrace subscriberTrace) { this.subscriberTrace = subscriberTrace; }

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
        this.evolutionSubscriberStatusUpdated = true;
        this.deliveryRequests = new ArrayList<DeliveryRequest>();
        this.journeyStatistics = new ArrayList<JourneyStatistic>();
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

  private SubscriberState(String subscriberID, SubscriberProfile subscriberProfile, Set<JourneyState> journeyStates, boolean evolutionSubscriberStatusUpdated, List<DeliveryRequest> deliveryRequests, List<JourneyStatistic> journeyStatistics, SubscriberTrace subscriberTrace)
  {
    this.subscriberID = subscriberID;
    this.subscriberProfile = subscriberProfile;
    this.journeyStates = journeyStates;
    this.evolutionSubscriberStatusUpdated = evolutionSubscriberStatusUpdated;
    this.deliveryRequests = deliveryRequests;
    this.journeyStatistics = journeyStatistics;
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
        this.evolutionSubscriberStatusUpdated = subscriberState.getEvolutionSubscriberStatusUpdated();
        this.deliveryRequests = new ArrayList<DeliveryRequest>(subscriberState.getDeliveryRequests());
        this.journeyStatistics = new ArrayList<JourneyStatistic>(subscriberState.getJourneyStatistics());
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
    struct.put("evolutionSubscriberStatusUpdated", subscriberState.getEvolutionSubscriberStatusUpdated());
    struct.put("deliveryRequests", packDeliveryRequests(subscriberState.getDeliveryRequests()));
    struct.put("journeyStatistics", packJourneyStatistics(subscriberState.getJourneyStatistics()));
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
    boolean evolutionSubscriberStatusUpdated = valueStruct.getBoolean("evolutionSubscriberStatusUpdated");
    List<DeliveryRequest> deliveryRequests = unpackDeliveryRequests(schema.field("deliveryRequests").schema(), valueStruct.get("deliveryRequests"));
    List<JourneyStatistic> journeyStatistics = unpackJourneyStatistics(schema.field("journeyStatistics").schema(), valueStruct.get("journeyStatistics"));
    SubscriberTrace subscriberTrace = valueStruct.getString("subscriberTraceMessage") != null ? new SubscriberTrace(valueStruct.getString("subscriberTraceMessage")) : null;

    //
    //  return
    //

    return new SubscriberState(subscriberID, subscriberProfile, journeyStates, evolutionSubscriberStatusUpdated, deliveryRequests, journeyStatistics, subscriberTrace);
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
}
