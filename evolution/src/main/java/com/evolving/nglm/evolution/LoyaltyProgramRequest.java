/*****************************************
*
*  JourneyRequest.java
*
*****************************************/

package com.evolving.nglm.evolution;

import java.util.HashMap;

import com.evolving.nglm.core.*;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramOperation;

public class LoyaltyProgramRequest extends BonusDelivery
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),9));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("operation", Schema.STRING_SCHEMA);
    schemaBuilder.field("loyaltyProgramRequestID", Schema.STRING_SCHEMA);
    schemaBuilder.field("loyaltyProgramID", Schema.STRING_SCHEMA);
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
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public LoyaltyProgramOperation getOperation(){ return operation; }
  public String getLoyaltyProgramRequestID() { return loyaltyProgramRequestID; }
  public String getLoyaltyProgramID() { return loyaltyProgramID; }
  @Override public ActivityType getActivityType() { return ActivityType.LoyaltyProgram; }
  
  /*****************************************
  *
  *  constructor -- journey
  *
  *****************************************/

  public LoyaltyProgramRequest(EvolutionEventContext context, String deliveryRequestSource, LoyaltyProgramOperation operation, String loyaltyProgramID, int tenantID)
  {
    super(context, "loyaltyProgramFulfillment", deliveryRequestSource, tenantID);
    this.operation = operation;
    this.loyaltyProgramRequestID = context.getUniqueKey();
    this.loyaltyProgramID = loyaltyProgramID;
  }
  
  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  public LoyaltyProgramRequest(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, JSONObject jsonRoot, int tenantID)
  {
    super(subscriberProfile,subscriberGroupEpochReader,jsonRoot, tenantID);
    this.operation = LoyaltyProgramOperation.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "operation", true));
    this.loyaltyProgramRequestID = JSONUtilities.decodeString(jsonRoot, "loyaltyProgramRequestID", true);
    this.loyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "loyaltyProgramID", true);
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramRequest(SchemaAndValue schemaAndValue, LoyaltyProgramOperation operation, String loyaltyProgramRequestID, String loyaltyProgramID)
  {
    super(schemaAndValue);
    this.operation = operation;
    this.loyaltyProgramRequestID = loyaltyProgramRequestID;
    this.loyaltyProgramID = loyaltyProgramID;
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion2(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    LoyaltyProgramOperation operation = LoyaltyProgramOperation.fromExternalRepresentation(valueStruct.getString("operation"));
    String loyaltyProgramRequestID = valueStruct.getString("loyaltyProgramRequestID");
    String loyaltyProgramID = valueStruct.getString("loyaltyProgramID");

    
    //
    //  return
    //

    return new LoyaltyProgramRequest(schemaAndValue, operation, loyaltyProgramRequestID, loyaltyProgramID);
  }
  
  /****************************************
  *
  *  presentation utilities
  *
  ****************************************/
  
  @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService, BadgeService badgeService, int tenantID)
  {
    guiPresentationMap.put(CUSTOMERID, getSubscriberID());
    guiPresentationMap.put(DELIVERABLEID, getLoyaltyProgramID());
    guiPresentationMap.put(DELIVERABLEQTY, 1);
    guiPresentationMap.put(OPERATION, getOperation().getExternalRepresentation());
    guiPresentationMap.put(MODULEID, getModuleID());
    guiPresentationMap.put(MODULENAME, getModule().toString());
    guiPresentationMap.put(FEATUREID, getFeatureID());
    guiPresentationMap.put(FEATURENAME, getFeatureName(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService, badgeService));
    guiPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService, badgeService));
    guiPresentationMap.put(ORIGIN, "");
  }
  
  @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, LoyaltyProgramService loyaltyProgramService, ProductService productService, VoucherService voucherService, DeliverableService deliverableService, PaymentMeanService paymentMeanService, ResellerService resellerService, BadgeService badgeService, int tenantID)
  {
    thirdPartyPresentationMap.put(DELIVERABLEID, getLoyaltyProgramID());
    thirdPartyPresentationMap.put(DELIVERABLEQTY, 1);
    thirdPartyPresentationMap.put(OPERATION, getOperation().getExternalRepresentation());
    thirdPartyPresentationMap.put(MODULEID, getModuleID());
    thirdPartyPresentationMap.put(MODULENAME, getModule().toString());
    thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
    thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService, badgeService));
    thirdPartyPresentationMap.put(FEATUREDISPLAY, getFeatureDisplay(getModule(), getFeatureID(), journeyService, offerService, loyaltyProgramService, badgeService));
    thirdPartyPresentationMap.put(ORIGIN, "");
  }
  @Override
  public void resetDeliveryRequestAfterReSchedule()
  {
    // 
    // LoyaltyProgramRequest never rescheduled, let return unchanged
    //  
    
  }
}
