package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ReferenceDataReader;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Date;
import java.util.Map;

public abstract class BonusDelivery extends DeliveryRequest
{

  CommodityDeliveryManager.CommodityDeliveryRequest commodityDeliveryRequest;

  public BonusDelivery(EvolutionEngine.EvolutionEventContext context, String deliveryType, String deliveryRequestSource, int tenantID){
    super(context, deliveryType, deliveryRequestSource, tenantID);
    init();
  }
  public BonusDelivery(SchemaAndValue schemaAndValue){
    super(schemaAndValue);
    init();
  }
  public BonusDelivery(BonusDelivery bonusDelivery){
    super(bonusDelivery);
    init();
  }
  public BonusDelivery(DeliveryRequest initialDeliveryRequest, JSONObject jsonRoot, int tenantID){
    super(initialDeliveryRequest, jsonRoot, tenantID);
    init();
  }
  public BonusDelivery(SubscriberProfile subscriberProfile, ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader, String uniqueKey, String subscriberID, String deliveryType, String deliveryRequestSource, boolean universalControlGroup, int tenantID) {
    super(subscriberProfile, subscriberGroupEpochReader, uniqueKey, subscriberID, deliveryType, deliveryRequestSource, universalControlGroup, tenantID);
    init();
  }
  public BonusDelivery(SubscriberProfile subscriberProfile, ReferenceDataReader<String, SubscriberGroupEpoch> subscriberGroupEpochReader, JSONObject jsonRoot, int tenantID) {
    super(subscriberProfile, subscriberGroupEpochReader, jsonRoot, tenantID);
    init();
  }
  public BonusDelivery(Map<String, Object> esFields) {
    super(esFields);
    init();
  }

  private void init(){
    if(this.getDiplomaticBriefcase()==null) return;
    if(this.getDiplomaticBriefcase().get(CommodityDeliveryManager.APPLICATION_BRIEFCASE)==null) return;
    try {
      JSONObject jsonCommodityDeliveryRequest = (JSONObject)(new JSONParser()).parse(this.getDiplomaticBriefcase().get(CommodityDeliveryManager.APPLICATION_BRIEFCASE));
      this.commodityDeliveryRequest = new CommodityDeliveryManager.CommodityDeliveryRequest(this,jsonCommodityDeliveryRequest,Deployment.getDeliveryManagers().get(CommodityDeliveryManager.COMMODITY_DELIVERY_TYPE),this.getTenantID());
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  //
  //  accessors map through commodityDeliveryRequest
  //

  public int getBonusDeliveryReturnCode(){ return this.commodityDeliveryRequest.getCommodityDeliveryStatus().getReturnCode(); }
  public String getBonusDeliveryReturnCodeDetails() { return this.commodityDeliveryRequest.getStatusMessage(); }
  public String getBonusDeliveryOrigin() { return this.commodityDeliveryRequest.getOrigin(); }
  public String getBonusDeliveryProviderId() { return this.commodityDeliveryRequest.getProviderID(); }
  public String getBonusDeliveryDeliverableId() { return this.commodityDeliveryRequest.getCommodityID(); }
  public String getBonusDeliveryDeliverableName() { return this.commodityDeliveryRequest.getCommodityName(); }
  public int getBonusDeliveryDeliverableQty() { return this.commodityDeliveryRequest.getAmount(); }
  public String getBonusDeliveryOperation() { return this.commodityDeliveryRequest.getOperation().getExternalRepresentation(); }
  public Date getBonusDeliveryDeliverableExpirationDate() { return this.commodityDeliveryRequest.getDeliverableExpirationDate(); }

}
