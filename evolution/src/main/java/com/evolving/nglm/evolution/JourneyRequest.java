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
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;

public class JourneyRequest extends DeliveryRequest implements SubscriberStreamEvent, SubscriberStreamOutput, Action
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
    schemaBuilder.name("journey_request");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("journeyRequestID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyRequest> serde = new ConnectSerde<JourneyRequest>(schema, false, JourneyRequest.class, JourneyRequest::pack, JourneyRequest::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyRequest> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  *****************************************/

  private String journeyRequestID;
  private Date eventDate;
  private String journeyID;

  //
  //  transient
  //

  private boolean eligible;
      
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getJourneyRequestID() { return journeyRequestID; }
  public Date getEventDate() { return eventDate; }
  public String getJourneyID() { return journeyID; }
  public boolean getEligible() { return eligible; }
  public ActionType getActionType() { return ActionType.JourneyRequest; }

  //
  //  setters
  //

  public void setEligible(boolean eligible) { this.eligible = eligible; }

  //
  //  structure
  //

  @Override public Integer getActivityType() { return ActivityType.BDR.getExternalRepresentation(); }
  
  /*****************************************
  *
  *  constructor -- journey
  *
  *****************************************/

  public JourneyRequest(EvolutionEventContext context, String deliveryRequestSource, String journeyID)
  {
    super(context, "journeyFulfillment", deliveryRequestSource);
    this.journeyRequestID = context.getUniqueKey();
    this.eventDate = context.now();
    this.journeyID = journeyID;
    this.eligible = false;
  }
  
  /*****************************************
  *
  *  constructor -- enterCampaign
  *
  *****************************************/

  public JourneyRequest(String uniqueKey, String subscriberID, String deliveryRequestSource, boolean universalControlGroup)
  {
    super(uniqueKey, subscriberID, "journeyFulfillment", deliveryRequestSource, universalControlGroup);
    this.journeyRequestID = uniqueKey;
    this.eventDate = SystemTime.getCurrentTime();
    this.journeyID = deliveryRequestSource;
    this.eligible = false;
  }

  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  public JourneyRequest(JSONObject jsonRoot, DeliveryManagerDeclaration deliveryManager)
  {
    super(jsonRoot);
    this.journeyRequestID = JSONUtilities.decodeString(jsonRoot, "journeyRequestID", true);
    this.eventDate = JSONUtilities.decodeDate(jsonRoot, "eventDate", true);
    this.journeyID = JSONUtilities.decodeString(jsonRoot, "journeyID", true);
    this.eligible = false;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public JourneyRequest(SchemaAndValue schemaAndValue, String journeyRequestID, Date eventDate, String journeyID)
  {
    super(schemaAndValue);
    this.journeyRequestID = journeyRequestID;
    this.eventDate = eventDate;
    this.journeyID = journeyID;
    this.eligible = false;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  private JourneyRequest(JourneyRequest journeyRequest)
  {
    super(journeyRequest);
    this.journeyRequestID = journeyRequest.getJourneyRequestID();
    this.eventDate = journeyRequest.getEventDate();
    this.journeyID = journeyRequest.getJourneyID();
    this.eligible = journeyRequest.getEligible();
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public JourneyRequest copy()
  {
    return new JourneyRequest(this);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyRequest journeyRequest = (JourneyRequest) value;
    Struct struct = new Struct(schema);
    packCommon(struct, journeyRequest);
    struct.put("journeyRequestID", journeyRequest.getJourneyRequestID());
    struct.put("eventDate", journeyRequest.getEventDate());
    struct.put("journeyID", journeyRequest.getJourneyID());
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

  public static JourneyRequest unpack(SchemaAndValue schemaAndValue)
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
    String journeyRequestID = valueStruct.getString("journeyRequestID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    String journeyID = valueStruct.getString("journeyID");

    
    //
    //  return
    //

    return new JourneyRequest(schemaAndValue, journeyRequestID, eventDate, journeyID);
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
    guiPresentationMap.put(DELIVERABLEID, getJourneyID());
    guiPresentationMap.put(DELIVERABLEQTY, 1);
    guiPresentationMap.put(OPERATION, CommodityDeliveryOperation.Credit.toString());
    guiPresentationMap.put(MODULEID, getModuleID());
    guiPresentationMap.put(MODULENAME, module.toString());
    guiPresentationMap.put(FEATUREID, getFeatureID());
    guiPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService));
    guiPresentationMap.put(ORIGIN, "");
  }
  
  @Override public void addFieldsForThirdPartyPresentation(HashMap<String, Object> thirdPartyPresentationMap, SubscriberMessageTemplateService subscriberMessageTemplateService, SalesChannelService salesChannelService, JourneyService journeyService, OfferService offerService, ProductService productService, DeliverableService deliverableService, PaymentMeanService paymentMeanService)
  {
    Module module = Module.fromExternalRepresentation(getModuleID());
    thirdPartyPresentationMap.put(DELIVERABLEID, getJourneyID());
    thirdPartyPresentationMap.put(DELIVERABLEQTY, 1);
    thirdPartyPresentationMap.put(OPERATION, CommodityDeliveryOperation.Credit.toString());
    thirdPartyPresentationMap.put(MODULEID, getModuleID());
    thirdPartyPresentationMap.put(MODULENAME, module.toString());
    thirdPartyPresentationMap.put(FEATUREID, getFeatureID());
    thirdPartyPresentationMap.put(FEATURENAME, getFeatureName(module, getFeatureID(), journeyService, offerService));
    thirdPartyPresentationMap.put(ORIGIN, "");
  }
}
