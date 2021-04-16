package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentCommon;
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
    // extract the embedded CommodityDeliveryRequest if there is
    if(this.getDiplomaticBriefcase()==null) return;
    if(this.getDiplomaticBriefcase().get(CommodityDeliveryManager.COMMODITY_DELIVERY_BRIEFCASE)==null) return;
    try {
      JSONObject jsonCommodityDeliveryRequest = (JSONObject)(new JSONParser()).parse(this.getDiplomaticBriefcase().get(CommodityDeliveryManager.COMMODITY_DELIVERY_BRIEFCASE));
      this.commodityDeliveryRequest = new CommodityDeliveryManager.CommodityDeliveryRequest(this,jsonCommodityDeliveryRequest,DeploymentCommon.getDeliveryManagers().get(CommodityDeliveryManager.COMMODITY_DELIVERY_TYPE),this.getTenantID());
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
    // then update the CommodityDeliveryRequest from sub request if needed (response update)
    if(this.getDeliveryStatus()==DeliveryManager.DeliveryStatus.Pending) return;
    updateResponse();
  }

  private void updateResponse(){
    this.commodityDeliveryRequest.setDeliveryDate(this.getDeliveryDate());
    switch(this.getDeliveryStatus()){
      case Delivered:
        this.commodityDeliveryRequest.setCommodityDeliveryStatus(CommodityDeliveryManager.CommodityDeliveryStatus.SUCCESS);
        this.commodityDeliveryRequest.setStatusMessage("Success");
        break;
      case CheckBalanceLowerThan:
        this.commodityDeliveryRequest.setCommodityDeliveryStatus(CommodityDeliveryManager.CommodityDeliveryStatus.CHECK_BALANCE_LT);
        this.commodityDeliveryRequest.setStatusMessage("Success");
        break;
      case CheckBalanceEqualsTo:
        this.commodityDeliveryRequest.setCommodityDeliveryStatus(CommodityDeliveryManager.CommodityDeliveryStatus.CHECK_BALANCE_ET);
        this.commodityDeliveryRequest.setStatusMessage("Success");
        break;
      case CheckBalanceGreaterThan:
        this.commodityDeliveryRequest.setCommodityDeliveryStatus(CommodityDeliveryManager.CommodityDeliveryStatus.CHECK_BALANCE_GT);
        this.commodityDeliveryRequest.setStatusMessage("Success");
        break;
      case BonusNotFound:
        this.commodityDeliveryRequest.setCommodityDeliveryStatus(CommodityDeliveryManager.CommodityDeliveryStatus.BONUS_NOT_FOUND);
        this.commodityDeliveryRequest.setStatusMessage("Commodity delivery request failed");
        break;
      case InsufficientBalance:
        this.commodityDeliveryRequest.setCommodityDeliveryStatus(CommodityDeliveryManager.CommodityDeliveryStatus.INSUFFICIENT_BALANCE);
        this.commodityDeliveryRequest.setStatusMessage("Commodity delivery request failed");
        break;
      case FailedRetry:
      case Indeterminate:
      case Failed:
      case FailedTimeout:
        this.commodityDeliveryRequest.setCommodityDeliveryStatus(CommodityDeliveryManager.CommodityDeliveryStatus.THIRD_PARTY_ERROR);
        this.commodityDeliveryRequest.setStatusMessage("Commodity delivery request failed");
        break;
      case Pending:
      case Unknown:
      default:
        this.commodityDeliveryRequest.setCommodityDeliveryStatus(CommodityDeliveryManager.CommodityDeliveryStatus.THIRD_PARTY_ERROR);
        this.commodityDeliveryRequest.setStatusMessage("Commodity delivery request failure");
        break;
    }

    // update the embedded CommodityDeliveryRequest
    this.getDiplomaticBriefcase().put(CommodityDeliveryManager.COMMODITY_DELIVERY_BRIEFCASE, this.commodityDeliveryRequest.getJSONRepresentation(getTenantID()).toJSONString());
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
