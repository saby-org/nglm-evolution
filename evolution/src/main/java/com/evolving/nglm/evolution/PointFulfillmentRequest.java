/*****************************************
*
*  PointFulfillmentRequest.java
*
*****************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.HashMap;

import org.apache.kafka.connect.data.Field;
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
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;

public class PointFulfillmentRequest extends DeliveryRequest
{
  
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  PointOperation
  //

  public enum PointOperation
  {
    Credit("credit"),
    Debit("debit"),
    Expire("expire"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private PointOperation(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static PointOperation fromExternalRepresentation(String externalRepresentation) { for (PointOperation enumeratedValue : PointOperation.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

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
    schemaBuilder.name("service_pointfulfillment_request");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("pointID", Schema.STRING_SCHEMA);
    schemaBuilder.field("operation", Schema.STRING_SCHEMA);
    schemaBuilder.field("amount", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("validityPeriodType", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("validityPeriodQuantity", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("resultValidityDate", Timestamp.builder().optional().schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //
      
  private static ConnectSerde<PointFulfillmentRequest> serde = new ConnectSerde<PointFulfillmentRequest>(schema, false, PointFulfillmentRequest.class, PointFulfillmentRequest::pack, PointFulfillmentRequest::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PointFulfillmentRequest> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }
      
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String pointID;
  private PointOperation operation;
  private int amount;
  private TimeUnit validityPeriodType;
  private Integer validityPeriodQuantity;
  private Date resultValidityDate;

  //
  //  accessors
  //

  public String getPointID() { return pointID; }
  public PointOperation getOperation() { return operation; }
  public int getAmount() { return amount; }
  public TimeUnit getValidityPeriodType() { return validityPeriodType; }
  public Integer getValidityPeriodQuantity() { return validityPeriodQuantity; }
  public Date getResultValidityDate() { return resultValidityDate; }

  //
  //  setters
  //  

  public void setResultValidityDate(Date resultValidityDate) { this.resultValidityDate = resultValidityDate; }
  public void setValidityPeriodType(TimeUnit validityPeriodType) { this.validityPeriodType = validityPeriodType; }
  public void setValidityPeriodQuantity(Integer validityPeriodQuantity) { this.validityPeriodQuantity = validityPeriodQuantity; } 

  //
  //  structure
  //

  @Override public Integer getActivityType() { return ActivityType.BDR.getExternalRepresentation(); }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public PointFulfillmentRequest(EvolutionEventContext context, String deliveryType, String deliveryRequestSource, String pointID, PointOperation operation, int amount, TimeUnit validityPeriodType, Integer validityPeriodQuantity)
  {
    super(context, deliveryType, deliveryRequestSource);
    this.pointID = pointID;
    this.operation = operation;
    this.amount = amount;
    this.validityPeriodType = validityPeriodType;
    this.validityPeriodQuantity = validityPeriodQuantity;
    this.resultValidityDate = null;
  }

  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  public PointFulfillmentRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
  {
    super(jsonRoot);
    this.pointID = JSONUtilities.decodeString(jsonRoot, "pointID", true);
    this.operation = PointOperation.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "operation", true));
    this.amount = JSONUtilities.decodeInteger(jsonRoot, "amount", true);
    this.validityPeriodType = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "validityPeriodType", false));
    this.validityPeriodQuantity = JSONUtilities.decodeInteger(jsonRoot, "validityPeriodQuantity", false);
    this.resultValidityDate = null;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private PointFulfillmentRequest(SchemaAndValue schemaAndValue, String pointID, PointOperation operation, int amount, TimeUnit validityPeriodType, Integer validityPeriodQuantity, Date resultValidityDate)
  {
    super(schemaAndValue);
    this.pointID = pointID;
    this.operation = operation;
    this.amount = amount;
    this.validityPeriodType = validityPeriodType;
    this.validityPeriodQuantity = validityPeriodQuantity;
    this.resultValidityDate = resultValidityDate;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private PointFulfillmentRequest(PointFulfillmentRequest pointFulfillmentRequest)
  {
    super(pointFulfillmentRequest);
    this.pointID = pointFulfillmentRequest.getPointID();
    this.operation = pointFulfillmentRequest.getOperation();
    this.amount = pointFulfillmentRequest.getAmount();
    this.validityPeriodType = pointFulfillmentRequest.getValidityPeriodType();
    this.validityPeriodQuantity = pointFulfillmentRequest.getValidityPeriodQuantity();
    this.resultValidityDate = pointFulfillmentRequest.getResultValidityDate();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public PointFulfillmentRequest copy()
  {
    return new PointFulfillmentRequest(this);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PointFulfillmentRequest pointFulfillmentRequest = (PointFulfillmentRequest) value;
    Struct struct = new Struct(schema);
    packCommon(struct, pointFulfillmentRequest);
    struct.put("pointID", pointFulfillmentRequest.getPointID());
    struct.put("operation", pointFulfillmentRequest.getOperation().getExternalRepresentation());
    struct.put("amount", pointFulfillmentRequest.getAmount());
    struct.put("validityPeriodType", pointFulfillmentRequest.getValidityPeriodType().getExternalRepresentation());
    struct.put("validityPeriodQuantity", pointFulfillmentRequest.getValidityPeriodQuantity());
    struct.put("resultValidityDate", pointFulfillmentRequest.getResultValidityDate());
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

  public static PointFulfillmentRequest unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String pointID = valueStruct.getString("pointID");
    PointOperation operation = PointOperation.fromExternalRepresentation(valueStruct.getString("operation"));
    int amount = valueStruct.getInt32("amount");
    TimeUnit validityPeriodType = TimeUnit.fromExternalRepresentation(valueStruct.getString("validityPeriodType"));
    Integer validityPeriodQuantity = valueStruct.getInt32("validityPeriodQuantity");
    Date resultValidityDate = (Date) valueStruct.get("resultValidityDate");

    //
    //  return
    //

    return new PointFulfillmentRequest(schemaAndValue, pointID, operation, amount, validityPeriodType, validityPeriodQuantity, resultValidityDate);
  }

  /****************************************
  *
  *  presentation utilities
  *
  ****************************************/
  
  @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
  {
    Module module = Module.fromExternalRepresentation(getModuleID());
    guiPresentationMap.put(CUSTOMERID, getSubscriberID());
    guiPresentationMap.put(DELIVERABLEID, getPointID());
    guiPresentationMap.put(DELIVERABLEQTY, getAmount());
    guiPresentationMap.put(OPERATION, getOperation().toString());
    guiPresentationMap.put(VALIDITYPERIODTYPE, getValidityPeriodType().getExternalRepresentation());
    guiPresentationMap.put(VALIDITYPERIODQUANTITY, getValidityPeriodQuantity());
    guiPresentationMap.put(MODULEID, getModuleID());
    guiPresentationMap.put(MODULENAME, module.toString());
    guiPresentationMap.put(FEATUREID, getFeatureID());
    guiPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService));
    guiPresentationMap.put(ORIGIN, "");
  }
  
  @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
  {
    Module module = Module.fromExternalRepresentation(getModuleID());
    thirdPartyPresentationMap.put(CUSTOMERID, getSubscriberID());
    thirdPartyPresentationMap.put(DELIVERABLEID, getPointID());
    thirdPartyPresentationMap.put(DELIVERABLEQTY, getAmount());
    thirdPartyPresentationMap.put(OPERATION, getOperation().toString());
    thirdPartyPresentationMap.put(VALIDITYPERIODTYPE, getValidityPeriodType().getExternalRepresentation());
    thirdPartyPresentationMap.put(VALIDITYPERIODQUANTITY, getValidityPeriodQuantity());
    thirdPartyPresentationMap.put(MODULEID, getModuleID());
    thirdPartyPresentationMap.put(MODULENAME, module.toString());
    thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
    thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService));
    thirdPartyPresentationMap.put(ORIGIN, "");
  }
  
  /****************************************
  *
  *  getEffectiveDeliveryTime
  *
  ****************************************/

  @Override
  public Date getEffectiveDeliveryTime(Date now)
  {
    return now;
  }   

  /*****************************************
  *  
  *  toString
  *
  *****************************************/

  public String toString()
  {
    StringBuilder b = new StringBuilder();
    b.append("PointFulfillmentRequest:{");
    b.append(super.toStringFields());
    b.append("," + getSubscriberID());
    b.append("," + pointID);
    b.append("," + operation);
    b.append("," + amount);
    b.append("," + validityPeriodType);
    b.append("," + validityPeriodQuantity);
    b.append("," + resultValidityDate);
    b.append("}");
    return b.toString();
  }
}
