package com.evolving.nglm.evolution.purchase;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.INFulfillmentManager.Account;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentOperation;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;
import com.evolving.nglm.evolution.purchase.CommodityActionManager;
import com.evolving.nglm.evolution.purchase.IDRCallback;
import com.evolving.nglm.evolution.purchase.RequestPusher;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.DeliveryManagerDeclaration;
import com.evolving.nglm.evolution.Deployment;

public class CommodityActionManager
{

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  private enum RequestClass {
    IN(INFulfillmentRequest.class.getName());
    private String externalRepresentation;
    private RequestClass(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static RequestClass fromExternalRepresentation(String externalRepresentation) { for (RequestClass enumeratedValue : RequestClass.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return null; }
  }

  private static final Logger log = LoggerFactory.getLogger(CommodityActionManager.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Map<String, Map<String, String>> paymentMeans /*debit only*/ = new HashMap<String/*providerID*/, Map<String/*paymentMeanID*/, String/*requestClass+"-"+deliveryType*/>>();
  private Map<String, Map<String, String>> commodities /*credit only*/ = new HashMap<String/*providerID*/, Map<String/*commodityID*/, String/*requestClass+"-"+deliveryType*/>>();
  private RequestPusher pusher;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CommodityActionManager(IDRCallback callback){
    
    pusher = new RequestPusher(callback);    
    
    //
    // generate map ({providersID -> {paymentMeanID -> deliveryManager}})
    //

    for(DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values()){
      RequestClass requestClass = RequestClass.fromExternalRepresentation(deliveryManager.getRequestClassName());
      if(requestClass != null){

        log.debug("CommodityActionManager() : get information from deliveryManager "+deliveryManager);
        
        // get information
        JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
        String deliveryType = (String) deliveryManagerJSON.get("deliveryType");
        String providerID = (String) deliveryManagerJSON.get("providerID");
        JSONArray availableAccountsArray = (JSONArray) deliveryManagerJSON.get("availableAccounts");
        
        //
        Map<String, String> paymentMeanIDs = new HashMap<String/*paymentMeanID*/, String/*deliveryType*/>();
        Map<String, String> commodityIDs = new HashMap<String/*commodityID*/, String/*deliveryType*/>();
        for (int i=0; i<availableAccountsArray.size(); i++) {
          Account newAccount = new Account((JSONObject) availableAccountsArray.get(i));
          if(newAccount.getDebitable()){
            paymentMeanIDs.put(newAccount.getAccountID(), requestClass+"-"+deliveryType);
          }
          if(newAccount.getCreditable()){
            commodityIDs.put(newAccount.getAccountID(), requestClass+"-"+deliveryType);
          }
        }
        if(!paymentMeanIDs.isEmpty()){
          if(!paymentMeans.keySet().contains(providerID)){
            paymentMeans.put(providerID, paymentMeanIDs);
          }else{
            paymentMeans.get(providerID).putAll(paymentMeanIDs);
          }
        }
        if(!commodityIDs.isEmpty()){
          if(!commodities.keySet().contains(providerID)){
            commodities.put(providerID, commodityIDs);
          }else{
            commodities.get(providerID).putAll(commodityIDs);
          }
        }
        
        log.debug("CommodityActionManager() : get information from deliveryManager "+deliveryManager+" DONE");
        
      }else{
        
        log.debug("CommodityActionManager() : skip deliveryManager "+deliveryManager);
        
      }
    }
    
    //=========================================================================================
    //TODO :   | TODO : REMOVE THIS   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!
    //        \|/
    //=========================================================================================
    log.info("====================================================================================");
    log.info("        PAYMENT MEANS");
    log.info("====================================================================================");
    for(String providerIdentifier : paymentMeans.keySet()){
      log.info("PROVIDER : "+providerIdentifier);
      Map<String, String> providerPaymentMeans = paymentMeans.get(providerIdentifier);
      for(String paymentID : providerPaymentMeans.keySet()){
        log.info("      PaymentMean "+paymentID+"  ->  "+providerPaymentMeans.get(paymentID));
      }
    }
    log.info("====================================================================================");
    log.info("        COMMODITIES");
    log.info("====================================================================================");
    for(String providerIdentifier : commodities.keySet()){
      log.info("PROVIDER : "+providerIdentifier);
      Map<String, String> providerPaymentMeans = commodities.get(providerIdentifier);
      for(String commodityID : providerPaymentMeans.keySet()){
        log.info("      commodity "+commodityID+"  ->  "+providerPaymentMeans.get(commodityID));
      }
    }
    log.info("====================================================================================");
    //=========================================================================================
    //        /|\
    //TODO :   | TODO : REMOVE THIS   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!
    //=========================================================================================

  }
  
  /*****************************************
  *
  *  method makePayment
  *
  *****************************************/

  public boolean makePayment(JSONObject briefcase, String eventID, String moduleID, String featureID, String deliveryRequestID, String subscriberID, String providerID, String paymentMeanID, long amount, IDRCallback callback){
    return makeAction(paymentMeans, INFulfillmentOperation.Debit, briefcase, eventID, moduleID, featureID, deliveryRequestID, subscriberID, providerID, paymentMeanID, amount, callback);
  }
  
  /*****************************************
  *
  *  method makePayment
  *
  *****************************************/
  
  public boolean creditCommodity(JSONObject briefcase, String eventID, String moduleID, String featureID, String deliveryRequestID, String subscriberID, String providerID, String commodityID, long amount, IDRCallback callback){
    return makeAction(commodities, INFulfillmentOperation.Credit, briefcase, eventID, moduleID, featureID, deliveryRequestID, subscriberID, providerID, commodityID, amount, callback);
  }

  
  
  
  
  private boolean makeAction(Map<String, Map<String, String>> commoditiesSet, INFulfillmentOperation operation, JSONObject briefcase, String eventID, String moduleID, String featureID, String deliveryRequestID, String subscriberID, String providerID, String paymentMeanID, long amount, IDRCallback callback){
    log.info("CommodityActionManager.makeAction("+operation+", "+providerID+", "+paymentMeanID+", "+amount+") called");

    //
    // get paymentManager
    //
    String paymentDeliveryData = null;
    if(commoditiesSet.keySet().contains(providerID)){
      Map<String, String> providerPaymentMeans = commoditiesSet.get(providerID);
      if(providerPaymentMeans != null && providerPaymentMeans.keySet().contains(paymentMeanID)){
        paymentDeliveryData = providerPaymentMeans.get(paymentMeanID);
        String[] paymentDeliveryManagerInfos = paymentDeliveryData.split("-");
        if(paymentDeliveryManagerInfos.length == 2){
          RequestClass requestClass = RequestClass.valueOf(paymentDeliveryManagerInfos[0]);
          String deliveryType = paymentDeliveryManagerInfos[1];
          if(requestClass != null && !deliveryType.isEmpty()){
            switch (requestClass) {
            case IN:
              
              log.debug(Thread.currentThread().getId()+" - CommodityActionManager.makePayment("+operation+", "+providerID+", "+paymentMeanID+", "+amount+", callback) : generating IN request");
              HashMap<String,Object> requestData = new HashMap<String,Object>();
              requestData.put("dateFormat", "yyyy-MM-dd'T'HH:mm:ss:XX");
              requestData.put("deliveryRequestID", deliveryRequestID);
              requestData.put("deliveryType", deliveryType);
              requestData.put("subscriberID", subscriberID);
              requestData.put("providerID", providerID);
              requestData.put("paymentMeanID", paymentMeanID);
              requestData.put("operation", operation.getExternalRepresentation());
              requestData.put("amount", amount);
              requestData.put("eventID", eventID);
              requestData.put("moduleID", moduleID);
              requestData.put("featureID", featureID);
              Map<String,String> diplomaticBriefcase = new HashMap<String,String>();
              diplomaticBriefcase.put(IDRCallback.originalRequest, briefcase.toJSONString());
              requestData.put("diplomaticBriefcase", diplomaticBriefcase);
              //requestData.put("startValidityDate", now.toString()); //TODO SCH : what is this date for ?
              //requestData.put("endValidityDate", now.toString()); //TODO SCH : what is this date for ?
              log.debug(Thread.currentThread().getId()+" - CommodityActionManager.makePayment("+operation+", "+providerID+", "+paymentMeanID+", "+amount+", callback) : generating IN request DONE");

              log.info(Thread.currentThread().getId()+" - CommodityActionManager.makePayment("+operation+", "+providerID+", "+paymentMeanID+", "+amount+", callback) : sending IN request to RequestPusher");
              INFulfillmentRequest inRequest = new INFulfillmentRequest(JSONUtilities.encodeObject(requestData), Deployment.getDeliveryManagers().get(deliveryType));
              pusher.pushRequest(inRequest);
              log.info(Thread.currentThread().getId()+" - CommodityActionManager.makePayment("+operation+", "+providerID+", "+paymentMeanID+", "+amount+", callback) : sending IN request to RequestPusher DONE");

              break;
            }

            log.info("CommodityActionManager.makePayment("+operation+", "+providerID+", "+paymentMeanID+", "+amount+") DONE ");
            return true;
          }
        }
      }
    }
    
    log.warn("CommodityActionManager.makePayment("+operation+", "+providerID+", "+paymentMeanID+", "+amount+") FAILED (could not find paymentMean)");
    return false;

  }
  
}
