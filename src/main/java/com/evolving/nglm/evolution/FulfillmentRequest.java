/*****************************************
*
*  FulfillmentRequest.java
*
*****************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public abstract class FulfillmentRequest implements DeliveryRequest
{
  /*****************************************
  *
  *  schema/serde
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema commonSchema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("fulfillment_request");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("fulfillmentRequestID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyInstanceID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("control", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("fulfillmentType", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryStatus", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("deliveryPartition", Schema.OPTIONAL_INT32_SCHEMA);
    commonSchema = schemaBuilder.build();
  };

  private static ConnectSerde<FulfillmentRequest> commonSerde;
  static
  {
    //
    //  get serdes from registered fulfillment classes
    //

    List<ConnectSerde<FulfillmentRequest>> fulfillmentRequestSerdes = new ArrayList<ConnectSerde<FulfillmentRequest>>();
    for (DeliveryManagerDeclaration fulfillmentManager : Deployment.getFulfillmentManagers().values())
      {
        fulfillmentRequestSerdes.add((ConnectSerde<FulfillmentRequest>) fulfillmentManager.getRequestSerde());
      }

    //
    //  return
    //

    commonSerde = new ConnectSerde<FulfillmentRequest>("fulfillmentrequest", false, fulfillmentRequestSerdes.toArray(new ConnectSerde[0]));
  };

  //
  //  accessor
  //

  public static Schema commonSchema() { return commonSchema; }
  public static ConnectSerde<FulfillmentRequest> commonSerde() { return commonSerde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String fulfillmentRequestID;
  private String subscriberID;
  private String journeyInstanceID;
  private String journeyID;
  private boolean control;
  private String fulfillmentType;
  private DeliveryStatus deliveryStatus;
  private Date deliveryDate;
  private Integer deliveryPartition;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDeliveryRequestID() { return fulfillmentRequestID; }
  public String getFulfillmentRequestID() { return fulfillmentRequestID; }
  public String getSubscriberID() { return subscriberID; }
  public String getJourneyInstanceID() { return journeyInstanceID; }
  public String getJourneyID() { return journeyID; }
  public boolean getControl() { return control; }
  public String getFulfillmentType() { return fulfillmentType; }
  public DeliveryStatus getDeliveryStatus() { return deliveryStatus; }
  public Date getDeliveryDate() { return deliveryDate; }
  public Integer getDeliveryPartition() { return deliveryPartition; }
  public Date getEventDate() { return deliveryDate; }

  //
  //  setters
  //

  public void setDeliveryStatus(DeliveryStatus deliveryStatus) { this.deliveryStatus = deliveryStatus; }
  public void setDeliveryDate(Date deliveryDate) { this.deliveryDate = deliveryDate; }
  public void setDeliveryPartition(Integer deliveryPartition) { this.deliveryPartition = deliveryPartition; }

  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  public abstract Schema subscriberStreamEventSchema();
  public abstract Object subscriberStreamEventPack(Object value);

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  protected FulfillmentRequest(EvolutionEventContext context, String fulfillmentType, JourneyState journeyState)
  {
    /*****************************************
    *
    *  simple fields
    *
    *****************************************/

    this.fulfillmentRequestID = context.getUniqueKey();
    this.subscriberID = context.getSubscriberState().getSubscriberID();
    this.journeyInstanceID = journeyState.getJourneyInstanceID();
    this.journeyID = journeyState.getJourneyID();
    this.control = context.getSubscriberState().getSubscriberProfile().getUniversalControlGroup(context.getSubscriberGroupEpochReader()) || context.getSubscriberState().getSubscriberProfile().getControlGroup(context.getSubscriberGroupEpochReader());
    this.fulfillmentType = fulfillmentType;
    this.deliveryStatus = DeliveryStatus.Pending;
    this.deliveryDate = null;
    this.deliveryPartition = null;
  }

  /*****************************************
  *
  *  packCommon
  *
  *****************************************/

  protected static void packCommon(Struct struct, FulfillmentRequest fulfillmentRequest)
  {
    struct.put("fulfillmentRequestID", fulfillmentRequest.getFulfillmentRequestID());
    struct.put("subscriberID", fulfillmentRequest.getSubscriberID());
    struct.put("journeyInstanceID", fulfillmentRequest.getJourneyInstanceID());
    struct.put("journeyID", fulfillmentRequest.getJourneyID());
    struct.put("control", fulfillmentRequest.getControl());
    struct.put("fulfillmentType", fulfillmentRequest.getFulfillmentType());
    struct.put("deliveryStatus", fulfillmentRequest.getDeliveryStatus().getExternalRepresentation());
    struct.put("deliveryDate", fulfillmentRequest.getDeliveryDate());
    struct.put("deliveryPartition", fulfillmentRequest.getDeliveryPartition());
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  protected FulfillmentRequest(SchemaAndValue schemaAndValue)
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
    String fulfillmentRequestID = valueStruct.getString("fulfillmentRequestID");
    String subscriberID = valueStruct.getString("subscriberID");
    String journeyInstanceID = valueStruct.getString("journeyInstanceID");
    String journeyID = valueStruct.getString("journeyID");
    boolean control = valueStruct.getBoolean("control");
    String fulfillmentType = valueStruct.getString("fulfillmentType");
    DeliveryStatus deliveryStatus = DeliveryStatus.fromExternalRepresentation(valueStruct.getString("deliveryStatus"));
    Date deliveryDate = (Date) valueStruct.get("deliveryDate");
    Integer deliveryPartition = valueStruct.getInt32("deliveryPartition");

    //
    //  return
    //

    this.fulfillmentRequestID = fulfillmentRequestID;
    this.subscriberID = subscriberID;
    this.journeyInstanceID = journeyInstanceID;
    this.journeyID = journeyID;
    this.control = control;
    this.fulfillmentType = fulfillmentType;
    this.deliveryStatus = deliveryStatus;
    this.deliveryDate = deliveryDate;
    this.deliveryPartition = deliveryPartition;
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  public String toString()
  {
    StringBuilder b = new StringBuilder();

    b.append("FulfillmentRequest:{");
    b.append(fulfillmentRequestID);
    b.append("," + subscriberID);
    b.append("," + journeyInstanceID);
    b.append("," + journeyID);
    b.append("," + control);
    b.append("," + fulfillmentType);
    b.append("," + deliveryStatus);
    b.append("," + deliveryDate);
    b.append("," + deliveryPartition);
    b.append("}");

    return b.toString();
  }
}
