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
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryOperation;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;

public class LoyaltyProgramRequest extends DeliveryRequest /*implements SubscriberStreamEvent, SubscriberStreamOutput, Action*/
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

  private String loyaltyProgramRequestID;
  private String loyaltyProgramID;
  private Date eventDate;

//  //
//  //  transient
//  //
//
//  private boolean eligible;
      
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getLoyaltyProgramRequestID() { return loyaltyProgramRequestID; }
  public String getLoyaltyProgramID() { return loyaltyProgramID; }
  public Date getEventDate() { return eventDate; }
//  public boolean getEligible() { return eligible; }
//  public ActionType getActionType() { return ActionType.JourneyRequest; }

//  //
//  //  setters
//  //
//
//  public void setEligible(boolean eligible) { this.eligible = eligible; }

  //
  //  structure
  //

  @Override public Integer getActivityType() { return ActivityType.BDR.getExternalRepresentation(); }
  
  /*****************************************
  *
  *  constructor -- journey
  *
  *****************************************/

  public LoyaltyProgramRequest(EvolutionEventContext context, String deliveryRequestSource, String loyaltyProgramID)
  {
    super(context, "loyaltyProgramFulfillment", deliveryRequestSource);
    this.loyaltyProgramRequestID = context.getUniqueKey();
    this.loyaltyProgramID = loyaltyProgramID;
    this.eventDate = context.now();
//    this.eligible = false;
  }
  
//  /*****************************************
//  *
//  *  constructor -- enterLoyaltyProgram
//  *
//  *****************************************/
//
//  public LoyaltyProgramRequest(String uniqueKey, String subscriberID, String deliveryRequestSource)
//  {
//    super(uniqueKey, subscriberID, "journeyFulfillment", deliveryRequestSource, universalControlGroup);
//    this.loyaltyProgramRequestID = uniqueKey;
//    this.loyaltyProgramID = deliveryRequestSource;
//    this.eventDate = SystemTime.getCurrentTime();
////    this.eligible = false;
//  }

  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  public LoyaltyProgramRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
  {
    super(jsonRoot);
    this.loyaltyProgramRequestID = JSONUtilities.decodeString(jsonRoot, "loyaltyProgramRequestID", true);
    this.loyaltyProgramID = JSONUtilities.decodeString(jsonRoot, "loyaltyProgramID", true);
    this.eventDate = JSONUtilities.decodeDate(jsonRoot, "eventDate", true);
//    this.eligible = false;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramRequest(SchemaAndValue schemaAndValue, String loyaltyProgramRequestID, String loyaltyProgramID, Date eventDate)
  {
    super(schemaAndValue);
    this.loyaltyProgramRequestID = loyaltyProgramRequestID;
    this.loyaltyProgramID = loyaltyProgramID;
    this.eventDate = eventDate;
//    this.eligible = false;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private LoyaltyProgramRequest(LoyaltyProgramRequest journeyRequest)
  {
    super(journeyRequest);
    this.loyaltyProgramRequestID = journeyRequest.getLoyaltyProgramRequestID();
    this.loyaltyProgramID = journeyRequest.getLoyaltyProgramID();
    this.eventDate = journeyRequest.getEventDate();
//    this.eligible = journeyRequest.getEligible();
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
    LoyaltyProgramRequest journeyRequest = (LoyaltyProgramRequest) value;
    Struct struct = new Struct(schema);
    packCommon(struct, journeyRequest);
    struct.put("loyaltyProgramRequestID", journeyRequest.getLoyaltyProgramRequestID());
    struct.put("loyaltyProgramID", journeyRequest.getLoyaltyProgramID());
    struct.put("eventDate", journeyRequest.getEventDate());
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
    String loyaltyProgramRequestID = valueStruct.getString("loyaltyProgramRequestID");
    String loyaltyProgramID = valueStruct.getString("loyaltyProgramID");
    Date eventDate = (Date) valueStruct.get("eventDate");

    
    //
    //  return
    //

    return new LoyaltyProgramRequest(schemaAndValue, loyaltyProgramRequestID, loyaltyProgramID, eventDate);
  }
  
  /****************************************
  *
  *  presentation utilities
  *
  ****************************************/
  
  @Override public void addFieldsForGUIPresentation(HashMap<String, Object> guiPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService)
  {
    guiPresentationMap.put(CUSTOMERID, getSubscriberID());
    guiPresentationMap.put(DELIVERABLEID, getLoyaltyProgramID());
    guiPresentationMap.put(DELIVERABLEQTY, 1);
    guiPresentationMap.put(OPERATION, CommodityDeliveryOperation.Credit.toString());
    guiPresentationMap.put(MODULEID, getModuleID());
    guiPresentationMap.put(MODULENAME, Module.fromExternalRepresentation(getModuleID()).toString());
    guiPresentationMap.put(FEATUREID, getFeatureID());
    guiPresentationMap.put(ORIGIN, "");
  }
  
  @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService)
  {
    thirdPartyPresentationMap.put(CUSTOMERID, getSubscriberID());
    thirdPartyPresentationMap.put(DELIVERABLEID, getLoyaltyProgramID());
    thirdPartyPresentationMap.put(DELIVERABLEQTY, 1);
    thirdPartyPresentationMap.put(OPERATION, CommodityDeliveryOperation.Credit.toString());
    thirdPartyPresentationMap.put(MODULEID, getModuleID());
    thirdPartyPresentationMap.put(MODULENAME, Module.fromExternalRepresentation(getModuleID()).toString());
    thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
    thirdPartyPresentationMap.put(ORIGIN, "");
  }


}
