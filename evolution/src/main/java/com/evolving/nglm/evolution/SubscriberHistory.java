/*****************************************************************************
*
*  SubscriberHistory.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SubscriberHistory implements StateStore
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryRequests", SchemaBuilder.array(DeliveryRequest.commonSerde().schema()).schema());
    schemaBuilder.field("journeyHistory", SchemaBuilder.array(JourneyHistory.schema()).schema());
    schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SubscriberHistory> serde = new ConnectSerde<SubscriberHistory>(schema, false, SubscriberHistory.class, SubscriberHistory::pack, SubscriberHistory::unpack);
  private static StateStoreSerde<SubscriberHistory> stateStoreSerde = new StateStoreSerde<SubscriberHistory>(serde);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberHistory> serde() { return serde; }
  public static StateStoreSerde<SubscriberHistory> stateStoreSerde() { return stateStoreSerde; }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String subscriberID;
  private int tenantID;
  private List<DeliveryRequest> deliveryRequests;
  private List<JourneyHistory> journeyHistory;

  //
  //  in memory only
  //

  private byte[] kafkaRepresentation = null;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberID() { return subscriberID; }
  public int getTenantID() { return tenantID; }
  public List<DeliveryRequest> getDeliveryRequests() { return deliveryRequests; }
  public List<JourneyHistory> getJourneyHistory() { return journeyHistory; }

  //
  //  kafkaRepresentation
  //

  @Override public void setKafkaRepresentation(byte[] kafkaRepresentation) { this.kafkaRepresentation = kafkaRepresentation; }
  @Override public byte[] getKafkaRepresentation() { return kafkaRepresentation; }

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  public SubscriberHistory(String subscriberID, int tenantID)
  {
    this.subscriberID = subscriberID;
    this.tenantID = tenantID;
    this.deliveryRequests = new ArrayList<DeliveryRequest>();
    this.journeyHistory = new ArrayList<JourneyHistory>();
    this.kafkaRepresentation = null;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private SubscriberHistory(String subscriberID, int tenantID, List<DeliveryRequest> deliveryRequests, List<JourneyHistory> journeyHistory)
  {
    this.subscriberID = subscriberID;
    this.tenantID = tenantID;
    this.deliveryRequests = deliveryRequests;
    this.journeyHistory = journeyHistory;
    this.kafkaRepresentation = null;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public SubscriberHistory(SubscriberHistory subscriberHistory)
  {
    this.subscriberID = subscriberHistory.getSubscriberID();
    this.tenantID = subscriberHistory.getTenantID();
    this.deliveryRequests = new ArrayList<DeliveryRequest>(subscriberHistory.getDeliveryRequests());
    this.kafkaRepresentation = null;
    
    //
    //  deep copy of journey statistics
    //
    
    this.journeyHistory = new ArrayList<JourneyHistory>();
    for(JourneyHistory journeyHistory : subscriberHistory.getJourneyHistory())
      {
        this.journeyHistory.add(new JourneyHistory(journeyHistory));
      }
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
    struct.put("tenantID", subscriberHistory.getTenantID());
    struct.put("deliveryRequests", packDeliveryRequests(subscriberHistory.getDeliveryRequests()));
    struct.put("journeyHistory", packJourneyHistory(subscriberHistory.getJourneyHistory()));
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
  *  packJourneyHistory
  *
  *****************************************/

  private static List<Object> packJourneyHistory(List<JourneyHistory> journeyHistory)
  {
    List<Object> result = new ArrayList<Object>();
    for (JourneyHistory history : journeyHistory)
      {
        result.add(JourneyHistory.pack(history));
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
    int tenantID = (schema.field("tenantID") != null) ? valueStruct.getInt16("tenantID") : 1; // by default tenant 1
    List<DeliveryRequest> deliveryRequests = unpackDeliveryRequests(schema.field("deliveryRequests").schema(), valueStruct.get("deliveryRequests"));
    List<JourneyHistory> journeyHistory = unpackJourneyHistory(schema.field("journeyHistory").schema(), valueStruct.get("journeyHistory"));

    //  
    //  return
    //

    return new SubscriberHistory(subscriberID, tenantID, deliveryRequests, journeyHistory);
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

  private static List<JourneyHistory> unpackJourneyHistory(Schema schema, Object value)
  {
    //
    //  get schema for JourneyHistory
    //

    Schema journeyHistorySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<JourneyHistory> result = new ArrayList<JourneyHistory>();
    List<Object> valueArray = (List<Object>) value;
    for (Object request : valueArray)
      {
        JourneyHistory journeyStatistic = JourneyHistory.unpack(new SchemaAndValue(journeyHistorySchema, request));
        result.add(journeyStatistic);
      }

    //
    //  return
    //

    return result;
  }
  
  @Override
  public String toString()
  {
    return "SubscriberHistory [" + (subscriberID != null ? "subscriberID=" + subscriberID + ", " : "") + "tenantID=" + tenantID + ", " + (deliveryRequests != null ? "deliveryRequests=" + deliveryRequests + ", " : "") + (journeyHistory != null ? "journeyHistory=" + journeyHistory + ", " : "") + "]";
  }
}
