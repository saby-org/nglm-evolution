/*****************************************************************************
*
*  MessageDelivery.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;

public class MessageDelivery implements EvolutionEngineEvent
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
    schemaBuilder.name("message_delivery");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("moduleID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("featureID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("returnCode", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("deliveryStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("returnCodeDetails", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("origin", Schema.OPTIONAL_STRING_SCHEMA);

    schemaBuilder.field("messageId", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<MessageDelivery> serde = new ConnectSerde<MessageDelivery>(schema, false, MessageDelivery.class, MessageDelivery::pack, MessageDelivery::unpack);

  //
  //  accessor
  //

  public String getEventName() { return "messageDelivery"; }
  public static Schema schema() { return schema; }
  public static ConnectSerde<MessageDelivery> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String subscriberID;
  private Date eventDate;
  private String moduleID;
  private String featureID;
  private int returnCode;
  private DeliveryStatus deliveryStatus;
  private String returnCodeDetails;
  private String origin;

  private String messageId;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public String getModuleId() { return moduleID; }
  public String getFeatureId() { return featureID; }
  public int getReturnCode() { return returnCode; }
  public DeliveryStatus getDeliveryStatus() { return deliveryStatus; }
  public String getReturnCodeDetails() { return returnCodeDetails; }
  public String getOrigin() { return origin; }

  public String getMessageId() { return messageId; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public MessageDelivery(SchemaAndValue schemaAndValue, String subscriberID, Date eventDate, String moduleID, String featureID, int returnCode, DeliveryStatus deliveryStatus, String returnCodeDetails, String origin, String messageId)
  {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.moduleID = moduleID;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.returnCode = returnCode;
    this.deliveryStatus = deliveryStatus;
    this.returnCodeDetails = returnCodeDetails;
    this.origin = origin;
  
    this.messageId = messageId;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public MessageDelivery(JSONObject jsonRoot)
  {
    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    this.subscriberID = JSONUtilities.decodeString(jsonRoot, "subscriberID", true);
    String date = JSONUtilities.decodeString(jsonRoot, "eventDate", true);
    if(date != null)
      {
        this.eventDate = GUIManagedObject.parseDateField(date);
      }
    else
      {
        this.eventDate = SystemTime.getCurrentTime();
      }
    this.moduleID = JSONUtilities.decodeString(jsonRoot, "moduleID", false);
    this.featureID = JSONUtilities.decodeString(jsonRoot, "featureID", false);
    this.returnCode = JSONUtilities.decodeInteger(jsonRoot, "returnCode", false);
    this.deliveryStatus = DeliveryStatus.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "deliveryStatus", false));
    this.returnCodeDetails = JSONUtilities.decodeString(jsonRoot, "returnCodeDetails", false);
    this.origin = JSONUtilities.decodeString(jsonRoot, "origin", false);

    this.messageId = JSONUtilities.decodeString(jsonRoot, "messageId", false);
  }

  /*****************************************
  *
  *  constructor -- DeliveryRequest
  *
  *****************************************/

  public MessageDelivery(DeliveryRequest deliveryRequest)
  {
    this.subscriberID = deliveryRequest.getSubscriberID();
    this.eventDate = deliveryRequest.getCreationDate();
    this.moduleID = deliveryRequest.getModuleID();
    this.featureID = deliveryRequest.getFeatureID();
    this.deliveryStatus = deliveryRequest.getDeliveryStatus();

    if (deliveryRequest instanceof SMSNotificationManagerRequest)
      {
        SMSNotificationManagerRequest snr = (SMSNotificationManagerRequest) deliveryRequest;
        this.returnCode = snr.getReturnCode();
        this.returnCodeDetails = snr.getReturnCodeDetails();
        this.origin = ""; // TODO
        this.messageId = snr.getEventID();
      } else if (deliveryRequest instanceof MailNotificationManagerRequest)
      {
        MailNotificationManagerRequest mnr = (MailNotificationManagerRequest) deliveryRequest;
        this.returnCode = mnr.getReturnCode();
        this.returnCodeDetails = mnr.getReturnCodeDetails();
        this.origin = mnr.getFromAddress();
        this.messageId = mnr.getEventID();
      }
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    MessageDelivery messageDelivery = (MessageDelivery) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", messageDelivery.getSubscriberID());
    struct.put("eventDate", messageDelivery.getEventDate());
    struct.put("moduleID", messageDelivery.getModuleId());
    struct.put("featureID", messageDelivery.getFeatureId());
    struct.put("returnCode", messageDelivery.getReturnCode());
    struct.put("deliveryStatus", messageDelivery.getDeliveryStatus().getExternalRepresentation());
    struct.put("returnCodeDetails", messageDelivery.getReturnCodeDetails());
    struct.put("origin", messageDelivery.getOrigin());

    struct.put("messageId", messageDelivery.getMessageId());
    return struct;
  }

  //
  //  subscriberStreamEventPack
  //

  public Object subscriberStreamEventPack(Object value) { return pack(value); }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static MessageDelivery unpack(SchemaAndValue schemaAndValue)
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
    Date eventDate = (Date) valueStruct.get("eventDate");
    String moduleID = valueStruct.getString("moduleID");
    String featureID = valueStruct.getString("featureID");
    int returnCode = valueStruct.getInt32("returnCode");
    DeliveryStatus deliveryStatus = DeliveryStatus.fromExternalRepresentation(valueStruct.getString("deliveryStatus"));
    String returnCodeDetails = valueStruct.getString("returnCodeDetails");
    String origin = valueStruct.getString("origin");

    String messageId = valueStruct.getString("messageId");

    //
    //  return
    //

    return new MessageDelivery(schemaAndValue, subscriberID, eventDate, moduleID, featureID, returnCode, deliveryStatus, returnCodeDetails, origin, messageId);
  }
}
