/*****************************************
*
*  JourneyRequest.java
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
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramOperation;

public class LoyaltyProgramRequest extends DeliveryRequest implements BonusDelivery
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
    schemaBuilder.name("loyalty_program_request");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("operation", Schema.STRING_SCHEMA);
    schemaBuilder.field("loyaltyProgramRequestID", Schema.STRING_SCHEMA);
    schemaBuilder.field("loyaltyProgramID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<LoyaltyProgramRequest> serde = new ConnectSerde<LoyaltyProgramRequest>(schema, false, LoyaltyProgramRequest.class, LoyaltyProgramRequest::pack, LoyaltyProgramRequest::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<LoyaltyProgramRequest> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  *****************************************/

  private LoyaltyProgramOperation operation;
  private String loyaltyProgramRequestID;
  private String loyaltyProgramID;
  private Date eventDate;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public LoyaltyProgramOperation getOperation(){ return operation; }
  public String getLoyaltyProgramRequestID() { return loyaltyProgramRequestID; }
  public String getLoyaltyProgramID() { return loyaltyProgramID; }
  public Date getEventDate() { return eventDate; }
  @Override public ActivityType getActivityType() { return ActivityType.LoyaltyProgram; }
  
  //
  //  bonus delivery accessors
  //

  public int getBonusDeliveryReturnCode() { return 0; }
  public String getBonusDeliveryReturnCodeDetails() { return ""; }
  public String getBonusDeliveryOrigin() { return ""; }
  public String getBonusDeliveryProviderId() { return ""; }
  public String getBonusDeliveryDeliverableId() { return ""; }
  public String getBonusDeliveryDeliverableName() { return ""; }
  public int getBonusDeliveryDeliverableQty() { return 0; }
  public String getBonusDeliveryOperation() { return getOperation().getExternalRepresentation(); }

  /*****************************************
  *
  *  constructor -- journey
  *
  *****************************************/

  public LoyaltyProgramRequest(EvolutionEventContext context, String deliveryRequestSource, LoyaltyProgramOperation operation, String loyaltyProgramID)
  {
    super(context, "loyaltyProgramFulfillment", deliveryRequestSource);
    this.operation = operation;
    this.loyaltyProgramRequestID = context.getUniqueKey();
    this.loyaltyProgramID = loyaltyProgramID;
    this.eventDate = context.now();
  }
  
  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  public LoyaltyProgramRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
  {
    super(jsonRoot);
    this.operation = LoyaltyProgramOperation.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "operation", true));
    this.loyaltyProgramRequestID = JSONUtilities.decodeString(jsonRoot, "loyaltyProgramRequestID", true);
    this.loyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "loyaltyProgramID", true);
    this.eventDate = JSONUtilities.decodeDate(jsonRoot, "eventDate", true);
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramRequest(SchemaAndValue schemaAndValue, LoyaltyProgramOperation operation, String loyaltyProgramRequestID, String loyaltyProgramID, Date eventDate)
  {
    super(schemaAndValue);
    this.operation = operation;
    this.loyaltyProgramRequestID = loyaltyProgramRequestID;
    this.loyaltyProgramID = loyaltyProgramID;
    this.eventDate = eventDate;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private LoyaltyProgramRequest(LoyaltyProgramRequest loyaltyProgramRequest)
  {
    super(loyaltyProgramRequest);
    this.operation = loyaltyProgramRequest.getOperation();
    this.loyaltyProgramRequestID = loyaltyProgramRequest.getLoyaltyProgramRequestID();
    this.loyaltyProgramID = loyaltyProgramRequest.getLoyaltyProgramID();
    this.eventDate = loyaltyProgramRequest.getEventDate();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public LoyaltyProgramRequest copy()
  {
    return new LoyaltyProgramRequest(this);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramRequest loyaltyProgramRequest = (LoyaltyProgramRequest) value;
    Struct struct = new Struct(schema);
    packCommon(struct, loyaltyProgramRequest);
    struct.put("operation", loyaltyProgramRequest.getOperation().getExternalRepresentation());
    struct.put("loyaltyProgramRequestID", loyaltyProgramRequest.getLoyaltyProgramRequestID());
    struct.put("loyaltyProgramID", loyaltyProgramRequest.getLoyaltyProgramID());
    struct.put("eventDate", loyaltyProgramRequest.getEventDate());
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

  public static LoyaltyProgramRequest unpack(SchemaAndValue schemaAndValue)
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
    LoyaltyProgramOperation operation = LoyaltyProgramOperation.fromExternalRepresentation(valueStruct.getString("operation"));
    String loyaltyProgramRequestID = valueStruct.getString("loyaltyProgramRequestID");
    String loyaltyProgramID = valueStruct.getString("loyaltyProgramID");
    Date eventDate = (Date) valueStruct.get("eventDate");

    
    //
    //  return
    //

    return new LoyaltyProgramRequest(schemaAndValue, operation, loyaltyProgramRequestID, loyaltyProgramID, eventDate);
  }
  
  /****************************************
  *
  *  presentation utilities
  *
  ****************************************/
  
  @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
  {
    Module module = Module.fromExternalRepresentation(getModuleID());
    guiPresentationMap.put(CUSTOMERID, getSubscriberID());
    guiPresentationMap.put(DELIVERABLEID, getLoyaltyProgramID());
    guiPresentationMap.put(DELIVERABLEQTY, 1);
    guiPresentationMap.put(OPERATION, getOperation().getExternalRepresentation());
    guiPresentationMap.put(MODULEID, getModuleID());
    guiPresentationMap.put(MODULENAME, module.toString());
    guiPresentationMap.put(FEATUREID, getFeatureID());
    guiPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
    guiPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
    guiPresentationMap.put(ORIGIN, "");
  }
  
  @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
  {
    Module module = Module.fromExternalRepresentation(getModuleID());
    thirdPartyPresentationMap.put(DELIVERABLEID, getLoyaltyProgramID());
    thirdPartyPresentationMap.put(DELIVERABLEQTY, 1);
    thirdPartyPresentationMap.put(OPERATION, getOperation().getExternalRepresentation());
    thirdPartyPresentationMap.put(MODULEID, getModuleID());
    thirdPartyPresentationMap.put(MODULENAME, module.toString());
    thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
    thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
    thirdPartyPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(module, getFeatureID(), journeyService, offerService, loyaltyProgramService));
    thirdPartyPresentationMap.put(ORIGIN, "");
  }
}
