/*****************************************************************************
*
*  OfferDelivery.java
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
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;

public class OfferDelivery implements EvolutionEngineEvent
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
    schemaBuilder.name("offer_delivery");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("moduleID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("featureID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("returnCode", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("deliveryStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("returnCodeDetails", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("origin", Schema.OPTIONAL_STRING_SCHEMA);

    schemaBuilder.field("offerId", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("offerQty", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("salesChannelId", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("offerPrice", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("meanOfPayment", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("offerStock", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("offerContent", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("voucherCode", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("voucherPartnerId", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<OfferDelivery> serde = new ConnectSerde<OfferDelivery>(schema, false, OfferDelivery.class, OfferDelivery::pack, OfferDelivery::unpack);

  //
  //  accessor
  //

  public String getEventName() { return "offerDelivery"; }
  public static Schema schema() { return schema; }
  public static ConnectSerde<OfferDelivery> serde() { return serde; }
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

  private String offerId;
  private int offerQty;
  private String salesChannelId;
  private int offerPrice;
  private String meanOfPayment;
  private int offerStock;
  private String offerContent;
  private String voucherCode;
  private String voucherPartnerId;

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

  public String getOfferId() { return offerId; }
  public int getOfferQty() { return offerQty; }
  public String getSalesChannelId() { return salesChannelId; }
  public int getOfferPrice() { return offerPrice; }
  public String getMeanOfPayment() { return meanOfPayment; }
  public int getOfferStock() { return offerStock; }
  public String getOfferContent() { return offerContent; }
  public String getVoucherCode() { return voucherCode; }
  public String getVoucherPartnerId() { return voucherPartnerId; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public OfferDelivery(SchemaAndValue schemaAndValue, String subscriberID, Date eventDate, String moduleID, String featureID, int returnCode, DeliveryStatus deliveryStatus, String returnCodeDetails, String origin, String offerId, int offerQty, String salesChannelId, int offerPrice, String meanOfPayment, int offerStock, String offerContent, String voucherCode, String voucherPartnerId)
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
  
    this.offerId = offerId;
    this.offerQty = offerQty;
    this.salesChannelId = salesChannelId;
    this.offerPrice = offerPrice;
    this.meanOfPayment = meanOfPayment;
    this.offerStock = offerStock;
    this.offerContent = offerContent;
    this.voucherCode = voucherCode;
    this.voucherPartnerId = voucherPartnerId;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public OfferDelivery(JSONObject jsonRoot)
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

    this.offerId = JSONUtilities.decodeString(jsonRoot, "offerId", false);;
    this.offerQty = JSONUtilities.decodeInteger(jsonRoot, "offerQty", false);;
    this.salesChannelId = JSONUtilities.decodeString(jsonRoot, "salesChannelId", false);
    this.offerPrice = JSONUtilities.decodeInteger(jsonRoot, "offerPrice", false);;
    this.meanOfPayment = JSONUtilities.decodeString(jsonRoot, "meanOfPayment", false);
    this.offerStock = JSONUtilities.decodeInteger(jsonRoot, "offerStock", false);
    this.offerContent = JSONUtilities.decodeString(jsonRoot, "offerContent", false);
    this.voucherCode = JSONUtilities.decodeString(jsonRoot, "voucherCode", false);
    this.voucherPartnerId = JSONUtilities.decodeString(jsonRoot, "voucherPartnerId", false);
  }

  /*****************************************
  *
  *  constructor -- DeliveryRequest
  *
  *****************************************/

  public OfferDelivery(DeliveryRequest deliveryRequest)
  {
    this.subscriberID = deliveryRequest.getSubscriberID();
    this.eventDate = deliveryRequest.getCreationDate();
    this.moduleID = deliveryRequest.getModuleID();
    this.featureID = deliveryRequest.getFeatureID();
    this.deliveryStatus = deliveryRequest.getDeliveryStatus();

    if (deliveryRequest instanceof PurchaseFulfillmentRequest)
      {
        PurchaseFulfillmentRequest pfr = (PurchaseFulfillmentRequest) deliveryRequest;
        this.returnCode = pfr.getReturnCode();
        this.offerId = pfr.getOfferID();
        this.offerQty = pfr.getQuantity();
        this.salesChannelId = pfr.getSalesChannelID();
        this.offerPrice = 1; // TODO
        this.meanOfPayment = "MOP"; // TODO Can be >1 which one to take ?
        this.offerStock = 1000; // TODO
        this.offerContent = ""; // TODO
        this.voucherCode = ""; // TODO
        this.voucherPartnerId = ""; // TODO
      }
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    OfferDelivery offerDelivery = (OfferDelivery) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", offerDelivery.getSubscriberID());
    struct.put("eventDate", offerDelivery.getEventDate());
    struct.put("moduleID", offerDelivery.getModuleId());
    struct.put("featureID", offerDelivery.getFeatureId());
    struct.put("returnCode", offerDelivery.getReturnCode());
    struct.put("deliveryStatus", offerDelivery.getDeliveryStatus().getExternalRepresentation());
    struct.put("returnCodeDetails", offerDelivery.getReturnCodeDetails());
    struct.put("origin", offerDelivery.getOrigin());

    struct.put("offerId", offerDelivery.getOfferId());
    struct.put("offerQty", offerDelivery.getOfferQty());
    struct.put("salesChannelId", offerDelivery.getSalesChannelId());
    struct.put("offerPrice", offerDelivery.getOfferPrice());
    struct.put("meanOfPayment", offerDelivery.getMeanOfPayment());
    struct.put("offerStock", offerDelivery.getOfferStock());
    struct.put("offerContent", offerDelivery.getOfferContent());
    struct.put("voucherCode", offerDelivery.getVoucherCode());
    struct.put("voucherPartnerId", offerDelivery.getVoucherPartnerId());
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

  public static OfferDelivery unpack(SchemaAndValue schemaAndValue)
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

    String offerId = valueStruct.getString("offerId");
    int offerQty = valueStruct.getInt32("offerQty");
    String salesChannelId = valueStruct.getString("salesChannelId");
    int offerPrice = valueStruct.getInt32("offerPrice");
    String meanOfPayment = valueStruct.getString("meanOfPayment");
    int offerStock = valueStruct.getInt32("offerStock");
    String offerContent = valueStruct.getString("offerContent");
    String voucherCode = valueStruct.getString("voucherCode");
    String voucherPartnerId = valueStruct.getString("voucherPartnerId");

    //
    //  return
    //

    return new OfferDelivery(schemaAndValue, subscriberID, eventDate, moduleID, featureID, returnCode, deliveryStatus, returnCodeDetails, origin, offerId, offerQty, salesChannelId, offerPrice, meanOfPayment, offerStock, offerContent, voucherCode, voucherPartnerId);
  }
}
