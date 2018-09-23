/*****************************************
*
*  DeliveryRequest.java
*
*****************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.rii.utilities.JSONUtilities;
import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public abstract class DeliveryRequest implements SubscriberStreamEvent, SubscriberStreamOutput
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
    schemaBuilder.name("delivery_request");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("deliveryRequestID", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryRequestSource", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryPartition", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("correlator", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("control", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("deliveryType", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryStatus", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryDate", Timestamp.builder().optional().schema());
    commonSchema = schemaBuilder.build();
  };

  //
  //  commonSerde
  //

  private static ConnectSerde<DeliveryRequest> commonSerde = null;
  private static void initializeCommonSerde()
  {
    //
    //  get serdes from registered delivery classes
    //

    List<ConnectSerde<DeliveryRequest>> deliveryRequestSerdes = new ArrayList<ConnectSerde<DeliveryRequest>>();
    for (DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values())
      {
        deliveryRequestSerdes.add((ConnectSerde<DeliveryRequest>) deliveryManager.getRequestSerde());
      }

    //
    //  return
    //

    commonSerde = new ConnectSerde<DeliveryRequest>("deliveryrequest", false, deliveryRequestSerdes.toArray(new ConnectSerde[0]));
  };

  //
  //  accessor
  //

  public static Schema commonSchema() { return commonSchema; }
  public static ConnectSerde<DeliveryRequest> commonSerde() { if (commonSerde == null) initializeCommonSerde(); return commonSerde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String deliveryRequestID;
  private String deliveryRequestSource;
  private String subscriberID;
  private Integer deliveryPartition;
  private String correlator;
  private boolean control;
  private String deliveryType;
  private DeliveryStatus deliveryStatus;
  private Date deliveryDate;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDeliveryRequestID() { return deliveryRequestID; }
  public String getDeliveryRequestSource() { return deliveryRequestSource; }
  public String getSubscriberID() { return subscriberID; }
  public Integer getDeliveryPartition() { return deliveryPartition; }
  public String getCorrelator() { return correlator; }
  public boolean getControl() { return control; }
  public String getDeliveryType() { return deliveryType; }
  public DeliveryStatus getDeliveryStatus() { return deliveryStatus; }
  public Date getDeliveryDate() { return deliveryDate; }
  public Date getEventDate() { return deliveryDate; }

  //
  //  setters
  //

  public void setDeliveryPartition(int deliveryPartition) { this.deliveryPartition = deliveryPartition; }
  public void setDeliveryStatus(DeliveryStatus deliveryStatus) { this.deliveryStatus = deliveryStatus; }
  public void setDeliveryDate(Date deliveryDate) { this.deliveryDate = deliveryDate; }
  public void setCorrelator(String correlator) { this.correlator = correlator; }

  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  public abstract Schema subscriberStreamEventSchema();
  public abstract Object subscriberStreamEventPack(Object value);

  /*****************************************
  *
  *  constructor -- evolution engine
  *
  *****************************************/

  protected DeliveryRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource)
  {
    /*****************************************
    *
    *  simple fields
    *
    *****************************************/

    this.deliveryRequestID = context.getUniqueKey();
    this.deliveryRequestSource = deliveryRequestSource;
    this.subscriberID = context.getSubscriberState().getSubscriberID();
    this.deliveryPartition = null;
    this.correlator = null;
    this.control = context.getSubscriberState().getSubscriberProfile().getUniversalControlGroup(context.getSubscriberGroupEpochReader());
    this.deliveryType = deliveryType;
    this.deliveryStatus = DeliveryStatus.Pending;
    this.deliveryDate = null;
  }

  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  protected DeliveryRequest(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  simple fields
    *
    *****************************************/

    this.deliveryRequestID = JSONUtilities.decodeString(jsonRoot, "deliveryRequestID", true);
    this.deliveryRequestSource = "external";
    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    this.deliveryPartition = null;
    this.correlator = null;
    this.control = JSONUtilities.decodeBoolean(jsonRoot, "control", Boolean.FALSE);
    this.deliveryType = JSONUtilities.decodeString(jsonRoot, "deliveryType", true);
    this.deliveryStatus = DeliveryStatus.Pending;
    this.deliveryDate = null;
  }

  /*****************************************
  *
  *  packCommon
  *
  *****************************************/

  protected static void packCommon(Struct struct, DeliveryRequest deliveryRequest)
  {
    struct.put("deliveryRequestID", deliveryRequest.getDeliveryRequestID());
    struct.put("deliveryRequestSource", deliveryRequest.getDeliveryRequestSource());
    struct.put("subscriberID", deliveryRequest.getSubscriberID());
    struct.put("deliveryPartition", deliveryRequest.getDeliveryPartition()); 
    struct.put("correlator", deliveryRequest.getCorrelator()); 
    struct.put("control", deliveryRequest.getControl());
    struct.put("deliveryType", deliveryRequest.getDeliveryType());
    struct.put("deliveryStatus", deliveryRequest.getDeliveryStatus().getExternalRepresentation());
    struct.put("deliveryDate", deliveryRequest.getDeliveryDate());
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  protected DeliveryRequest(SchemaAndValue schemaAndValue)
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
    String deliveryRequestID = valueStruct.getString("deliveryRequestID");
    String deliveryRequestSource = valueStruct.getString("deliveryRequestSource");
    String subscriberID = valueStruct.getString("subscriberID");
    Integer deliveryPartition = valueStruct.getInt32("deliveryPartition");
    String correlator = valueStruct.getString("correlator");
    boolean control = valueStruct.getBoolean("control");
    String deliveryType = valueStruct.getString("deliveryType");
    DeliveryStatus deliveryStatus = DeliveryStatus.fromExternalRepresentation(valueStruct.getString("deliveryStatus"));
    Date deliveryDate = (Date) valueStruct.get("deliveryDate");

    //
    //  return
    //

    this.deliveryRequestID = deliveryRequestID;
    this.deliveryRequestSource = deliveryRequestSource;
    this.subscriberID = subscriberID;
    this.deliveryPartition = deliveryPartition;
    this.correlator = correlator;
    this.control = control;
    this.deliveryType = deliveryType;
    this.deliveryStatus = deliveryStatus;
    this.deliveryDate = deliveryDate;
  }

  /*****************************************
  *
  *  toStringFields
  *
  *****************************************/

  protected String toStringFields()
  {
    StringBuilder b = new StringBuilder();
    b.append(deliveryRequestID);
    b.append("," + deliveryRequestSource);
    b.append("," + subscriberID);
    b.append("," + deliveryPartition);
    b.append("," + correlator);
    b.append("," + control);
    b.append("," + deliveryType);
    b.append("," + deliveryStatus);
    b.append("," + deliveryDate);
    return b.toString();
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  public String toString()
  {
    StringBuilder b = new StringBuilder();
    b.append("DeliveryRequest:{");
    b.append(toStringFields());
    b.append("}");
    return b.toString();
  }
}
