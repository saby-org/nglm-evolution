/*****************************************************************************
*
*  ReScheduledDeliveryRequest.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;
import java.util.Objects;

public class ReScheduledDeliveryRequest implements Comparable
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
    schemaBuilder.name("re_scheduled_delivery_request");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("evaluationDate", Timestamp.SCHEMA);
    schemaBuilder.field("deliveryRequest", DeliveryRequest.commonSerde().schema());
    schema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String subscriberID;
  private Date evaluationDate;
  private DeliveryRequest deliveryRequest;
  
  /*****************************************
  *
  *  constructor (normal)
  *
  *****************************************/

  public ReScheduledDeliveryRequest(String subscriberID, Date evaluationDate, DeliveryRequest deliveryRequest)
  {
    this.subscriberID = subscriberID;
    this.evaluationDate = evaluationDate;
    this.deliveryRequest = deliveryRequest;
  }

  /*****************************************
  *
  *  construct (copy)
  *
  *****************************************/

  public ReScheduledDeliveryRequest(ReScheduledDeliveryRequest reScheduledDeliveryRequest)
  {
    this.subscriberID = reScheduledDeliveryRequest.getSubscriberID();
    this.evaluationDate = reScheduledDeliveryRequest.getEvaluationDate();
    this.deliveryRequest = reScheduledDeliveryRequest.getDeliveryRequest();

  }


  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberID() { return subscriberID; }
  public Date getEvaluationDate() { return evaluationDate; }
  public Date getEventDate() { return evaluationDate; }
  public DeliveryRequest getDeliveryRequest() { return deliveryRequest; }
  
  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<ReScheduledDeliveryRequest> serde()
  {
    return new ConnectSerde<ReScheduledDeliveryRequest>(schema, false, ReScheduledDeliveryRequest.class, ReScheduledDeliveryRequest::pack, ReScheduledDeliveryRequest::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ReScheduledDeliveryRequest reScheduledDeliveryRequest = (ReScheduledDeliveryRequest) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", reScheduledDeliveryRequest.getSubscriberID());
    struct.put("evaluationDate", reScheduledDeliveryRequest.getEvaluationDate());
    struct.put("deliveryRequest", DeliveryRequest.commonSerde().pack(reScheduledDeliveryRequest.getDeliveryRequest()));
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ReScheduledDeliveryRequest unpack(SchemaAndValue schemaAndValue)
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
    Date evaluationDate = (Date) valueStruct.get("evaluationDate");
    
    DeliveryRequest deliveryRequest = DeliveryRequest.commonSerde().unpack(new SchemaAndValue(schema.field("deliveryRequest").schema(), valueStruct.get("deliveryRequest")));

    //
    //  return
    //

    return new ReScheduledDeliveryRequest(subscriberID, evaluationDate, deliveryRequest);
  }

  /*****************************************
  *
  *  equals (for SortedSet)
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof ReScheduledDeliveryRequest)
      {
        ReScheduledDeliveryRequest entry = (ReScheduledDeliveryRequest) obj;
        result = true;
        result = result && Objects.equals(subscriberID, entry.getSubscriberID());
        result = result && Objects.equals(evaluationDate, entry.getEvaluationDate());
        result = result && Objects.equals(deliveryRequest, entry.getDeliveryRequest());
      }
    return result;
  }

  /*****************************************
  *
  *  compareTo (for SortedSet)
  *
  *****************************************/

  public int compareTo(Object obj)
  {
    int result = -1;
    if (obj instanceof ReScheduledDeliveryRequest)
      {
        ReScheduledDeliveryRequest entry = (ReScheduledDeliveryRequest) obj;
        result = evaluationDate.compareTo(entry.getEvaluationDate());
        if (result == 0) result = subscriberID.compareTo(entry.getSubscriberID());
      }
    return result;
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  public String toString()
  {
    return "ReScheduledDeliveryRequest[" + subscriberID + "," + evaluationDate + "," + deliveryRequest + "]";
  }
}
