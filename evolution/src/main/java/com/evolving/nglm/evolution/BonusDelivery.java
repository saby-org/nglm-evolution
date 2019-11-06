/*****************************************************************************
*
*  BonusDelivery.java
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
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryRequest;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.EmptyFulfillmentManager.EmptyFulfillmentRequest;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;

public class BonusDelivery implements EvolutionEngineEvent
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
    schemaBuilder.name("bonus_delivery");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("moduleID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("featureID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("returnCode", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("deliveryStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("returnCodeDetails", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("origin", Schema.OPTIONAL_STRING_SCHEMA);

    schemaBuilder.field("providerId", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("deliverableId", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("deliverableQty", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("deliverableExpiration", Timestamp.builder().optional().schema());
    schemaBuilder.field("operation", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<BonusDelivery> serde = new ConnectSerde<BonusDelivery>(schema, false, BonusDelivery.class, BonusDelivery::pack, BonusDelivery::unpack);

  //
  //  accessor
  //

  public String getEventName() { return "bonusDelivery"; }
  public static Schema schema() { return schema; }
  public static ConnectSerde<BonusDelivery> serde() { return serde; }
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

  private String providerId;
  private String deliverableId;
  private int deliverableQty;
  private Date deliverableExpiration;
  private String operation;

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

  public String getProviderId() { return providerId; }
  public String getDeliverableId() { return deliverableId; }
  public int getDeliverableQty() { return deliverableQty; }
  public Date getDeliverableExpiration() { return deliverableExpiration; }
  public String getOperation() { return operation; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public BonusDelivery(SchemaAndValue schemaAndValue, String subscriberID, Date eventDate, String moduleID, String featureID, int returnCode, DeliveryStatus deliveryStatus, String returnCodeDetails, String origin, String providerId, String deliverableId, int deliverableQty, Date deliverableExpiration, String operation)
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
  
    this.providerId = providerId;
    this.deliverableId = deliverableId;
    this.deliverableQty = deliverableQty;
    this.deliverableExpiration = deliverableExpiration;
    this.operation = operation;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public BonusDelivery(JSONObject jsonRoot)
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

    this.providerId = JSONUtilities.decodeString(jsonRoot, "providerId", false);
    this.deliverableId = JSONUtilities.decodeString(jsonRoot, "deliverableId", false);
    this.deliverableQty = JSONUtilities.decodeInteger(jsonRoot, "deliverableQty", false);
    date = JSONUtilities.decodeString(jsonRoot, "deliverableExpiration", false);
    if(date != null)
      {
        this.deliverableExpiration = GUIManagedObject.parseDateField(date);
      }
    else
      {
        this.deliverableExpiration = SystemTime.getCurrentTime();
      }
    this.operation = JSONUtilities.decodeString(jsonRoot, "operation", false);
  }

  /*****************************************
  *
  *  constructor -- DeliveryRequest
  *
  *****************************************/

  public BonusDelivery(DeliveryRequest deliveryRequest)
  {
    this.subscriberID = deliveryRequest.getSubscriberID();
    this.eventDate = deliveryRequest.getCreationDate();
    this.moduleID = deliveryRequest.getModuleID();
    this.featureID = deliveryRequest.getFeatureID();
    this.deliverableExpiration = deliveryRequest.getTimeout();
    this.deliveryStatus = deliveryRequest.getDeliveryStatus();
    if (deliveryRequest instanceof CommodityDeliveryRequest)
      {
        CommodityDeliveryRequest cdr = (CommodityDeliveryRequest) deliveryRequest;
        this.deliverableId = cdr.getCommodityID();
        this.operation = cdr.getOperation().getExternalRepresentation();
        this.deliverableQty = cdr.getAmount();
        this.providerId = cdr.getProviderID();
        this.returnCode = cdr.getCommodityDeliveryStatus().getReturnCode();
        this.returnCodeDetails = cdr.getStatusMessage();
        this.origin = ""; // todo
      }
    else if (deliveryRequest instanceof EmptyFulfillmentRequest)
      {
        EmptyFulfillmentRequest efr = (EmptyFulfillmentRequest) deliveryRequest;
        this.deliverableId = efr.getCommodityID();
        this.operation = efr.getOperation().getExternalRepresentation();
        this.deliverableQty = efr.getAmount();
        this.providerId = efr.getProviderID();
        this.returnCode = (efr.getReturnCode() == null) ? 0 : efr.getReturnCode();
        this.returnCodeDetails = efr.getReturnCodeDetails();
        this.origin = ""; // TODO
      }
    else if (deliveryRequest instanceof INFulfillmentRequest)
      {
        INFulfillmentRequest ifr = (INFulfillmentRequest) deliveryRequest;
        this.deliverableId = ifr.getCommodityID();
        this.operation = ifr.getOperation().getExternalRepresentation();
        this.deliverableQty = ifr.getAmount();
        this.providerId = ifr.getProviderID();
        this.returnCode = (ifr.getReturnCode() == null) ? 0 : ifr.getReturnCode();
        this.returnCodeDetails = ifr.getReturnCodeDetails();
        this.origin = ""; // TODO
      }
    else if (deliveryRequest instanceof LoyaltyProgramRequest)
      {
        LoyaltyProgramRequest lpr = (LoyaltyProgramRequest) deliveryRequest;
        this.deliverableId = ""; // TODO
        this.operation = lpr.getOperation().getExternalRepresentation();
        this.deliverableQty = 0; // TODO
        this.providerId = ""; // TODO
        this.returnCode = 0; // TODO
        this.returnCodeDetails = ""; // TODO
        this.origin = ""; // TODO
      }
    else if (deliveryRequest instanceof PointFulfillmentRequest)
      {
        PointFulfillmentRequest pfr = (PointFulfillmentRequest) deliveryRequest;
        this.deliverableId = pfr.getPointID();
        this.operation = pfr.getOperation().getExternalRepresentation();
        this.deliverableQty = pfr.getAmount();
        this.providerId = ""; // TODO
        this.returnCode = 0; // TODO
        this.returnCodeDetails = ""; // TODO
        this.origin = ""; // TODO
      }
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    BonusDelivery bonusDelivery = (BonusDelivery) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", bonusDelivery.getSubscriberID());
    struct.put("eventDate", bonusDelivery.getEventDate());
    struct.put("moduleID", bonusDelivery.getModuleId());
    struct.put("featureID", bonusDelivery.getFeatureId());
    struct.put("returnCode", bonusDelivery.getReturnCode());
    struct.put("deliveryStatus", bonusDelivery.getDeliveryStatus().getExternalRepresentation());
    struct.put("returnCodeDetails", bonusDelivery.getReturnCodeDetails());
    struct.put("origin", bonusDelivery.getOrigin());
    
    struct.put("providerId", bonusDelivery.getProviderId());
    struct.put("deliverableId", bonusDelivery.getDeliverableId());
    struct.put("deliverableQty", bonusDelivery.getDeliverableQty());
    struct.put("deliverableExpiration", bonusDelivery.getDeliverableExpiration());
    struct.put("operation", bonusDelivery.getOperation());
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

  public static BonusDelivery unpack(SchemaAndValue schemaAndValue)
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

    String providerId = valueStruct.getString("providerId");
    String deliverableId = valueStruct.getString("deliverableId");
    int deliverableQty = valueStruct.getInt32("deliverableQty");
    Date deliverableExpiration = (Date) valueStruct.get("deliverableExpiration");
    String operation = valueStruct.getString("operation");

    //
    //  return
    //

    return new BonusDelivery(schemaAndValue, subscriberID, eventDate, moduleID, featureID, returnCode, deliveryStatus, returnCodeDetails, origin, providerId, deliverableId, deliverableQty, deliverableExpiration, operation);
  }
}
