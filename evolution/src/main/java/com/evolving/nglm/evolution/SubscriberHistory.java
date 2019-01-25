/*****************************************************************************
*
*  SubscriberHistory.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;

public class SubscriberHistory
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
    schemaBuilder.name("subscriber_history");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryRequests", SchemaBuilder.array(DeliveryRequest.commonSerde().schema()).schema());
    schemaBuilder.field("journeyStatistics", SchemaBuilder.array(JourneyStatistic.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SubscriberHistory> serde = new ConnectSerde<SubscriberHistory>(schema, false, SubscriberHistory.class, SubscriberHistory::pack, SubscriberHistory::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberHistory> serde() { return serde; }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String subscriberID;
  private List<DeliveryRequest> deliveryRequests;
  private List<JourneyStatistic> journeyStatistics;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberID() { return subscriberID; }
  public List<DeliveryRequest> getDeliveryRequests() { return deliveryRequests; }
  public List<JourneyStatistic> getJourneyStatistics() { return journeyStatistics; }

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  public SubscriberHistory(String subscriberID)
  {
    this.subscriberID = subscriberID;
    this.deliveryRequests = new ArrayList<DeliveryRequest>();
    this.journeyStatistics = new ArrayList<JourneyStatistic>();
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private SubscriberHistory(String subscriberID, List<DeliveryRequest> deliveryRequests, List<JourneyStatistic> journeyStatistics)
  {
    this.subscriberID = subscriberID;
    this.deliveryRequests = deliveryRequests;
    this.journeyStatistics = journeyStatistics;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public SubscriberHistory(SubscriberHistory subscriberHistory)
  {
    this.subscriberID = subscriberHistory.getSubscriberID();
    this.deliveryRequests = new ArrayList<DeliveryRequest>(subscriberHistory.getDeliveryRequests());
    this.journeyStatistics = new ArrayList<JourneyStatistic>(subscriberHistory.getJourneyStatistics());
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SubscriberHistory subscriberHistory = (SubscriberHistory) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", subscriberHistory.getSubscriberID());
    struct.put("deliveryRequests", packDeliveryRequests(subscriberHistory.getDeliveryRequests()));
    struct.put("journeyStatistics", packJourneyStatistics(subscriberHistory.getJourneyStatistics()));
    return struct;
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

  public static SubscriberHistory unpack(SchemaAndValue schemaAndValue)
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
    List<DeliveryRequest> deliveryRequests = unpackDeliveryRequests(schema.field("deliveryRequests").schema(), valueStruct.get("deliveryRequests"));
    List<JourneyStatistic> journeyStatistics = unpackJourneyStatistics(schema.field("journeyStatistics").schema(), valueStruct.get("journeyStatistics"));

    //  
    //  return
    //

    return new SubscriberHistory(subscriberID, deliveryRequests, journeyStatistics);
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
    for (Object request : valueArray)
      {
        JourneyStatistic journeyStatistic = JourneyStatistic.unpack(new SchemaAndValue(journeyStatisticSchema, request));
        result.add(journeyStatistic);
      }

    //
    //  return
    //

    return result;
  }
}
