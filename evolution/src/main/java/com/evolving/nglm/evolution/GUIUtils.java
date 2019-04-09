package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.INFulfillmentManager.Account;
import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;

public class GUIUtils {

  /*****************************************
  *
  *  variables
  *
  *****************************************/

  private static final Logger log = LoggerFactory.getLogger(GUIUtils.class);

  private enum ProviderSelection {
    IN(INFulfillmentRequest.class.getName());

    private String externalRepresentation;
    private ProviderSelection(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static ProviderSelection fromExternalRepresentation(String externalRepresentation) {
      for (ProviderSelection enumeratedValue : ProviderSelection.values()) 
        {
          if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) 
            {
              return enumeratedValue;
            }
        }
      return null;
    }
  }
  
  private enum CommoditySelection {
    IN(INFulfillmentRequest.class.getName());

    private String externalRepresentation;
    private CommoditySelection(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CommoditySelection fromExternalRepresentation(String externalRepresentation) {
      for (CommoditySelection enumeratedValue : CommoditySelection.values()) 
        {
          if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) 
            {
              return enumeratedValue;
            }
        }
      return null;
    }
  }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private static Map<String,FulfillmentProvider> providersGUI = null;
  private static Map<String, Map<String, String>> commodities = null; /* HashMap < providerID , Map < commodityID , requestClass+"-"+deliveryType > > */
  private static Map<String, Map<String, String>> paymentMeans = null; /* HashMap < providerID , Map < commodityID , requestClass+"-"+deliveryType > > */

  
  
  /*****************************************
  *
  *  getProviders
  *
  *****************************************/
  
  public static Map<String,FulfillmentProvider> getProviders() {
    if (providersGUI == null) {
      providersGUI = new LinkedHashMap<String,FulfillmentProvider>();
      computeProviders();
    }
    return providersGUI;
  }
  
  private static void computeProviders() {
    for (DeliveryManagerDeclaration deliveryManager : Deployment.getDeliveryManagers().values()){
      ProviderSelection providerSelection = ProviderSelection.fromExternalRepresentation(deliveryManager.getRequestClassName());
      if (providerSelection != null){

        switch (providerSelection) {
        case IN:
          
          //
          // get data
          //
          
          JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
          String providerID = JSONUtilities.decodeString(deliveryManagerJSON, "providerID");
          String providerName = deliveryManager.getDeliveryType() + "-" + providerID;
          String providerType = providerSelection.getExternalRepresentation();
          String url = "";
          
          //
          // new provider
          //
          
          HashMap<String,Object> providerJSON = new HashMap<>();
          providerJSON.put("id", providerID);
          providerJSON.put("name", providerName);
          providerJSON.put("providerType", providerType);
          providerJSON.put("url", url);
          FulfillmentProvider provider = new FulfillmentProvider(JSONUtilities.encodeObject(providerJSON));
          
          //
          // add new provider
          //
          
          providersGUI.put(provider.getProviderID(), provider);
          break;

        default:
          break;
        }
      }
    }
    
    //=========================================================================================
    //TODO :   | TODO : REMOVE THIS   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!
    //        \|/
    //=========================================================================================
    log.info("====================================================================================");
    log.info("        PROVIDERS");
    log.info("====================================================================================");
    if(providersGUI != null){
      for(FulfillmentProvider provider : providersGUI.values()){
        log.info("    - " + provider.toString());
      }
    }else{
      log.info("    NO PROVIDER !!! !!! !!! ");
    }
    log.info("====================================================================================");
    //=========================================================================================
    //        /|\
    //TODO :   | TODO : REMOVE THIS   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!
    //=========================================================================================
    
  }
  
  /*****************************************
  *
  *  getCommodities
  *
  *****************************************/

  public static Map<String, Map<String, String>> getCommodities() {
    if (commodities == null) {
      commodities = new HashMap<String, Map<String, String>>();
      computeCommodities();
    }
    return commodities;
  }
  
  public static void computeCommodities() {
    for(DeliveryManagerDeclaration deliveryManager : getDeliveryManagers().values()){
      CommoditySelection requestClass = CommoditySelection.fromExternalRepresentation(deliveryManager.getRequestClassName());
      if((requestClass != null) && (requestClass.equals(CommoditySelection.IN))){
        log.debug("GUIUtils.computeCommodities() : get information from deliveryManager "+deliveryManager);
        // get information
        JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
        String deliveryType = (String) deliveryManagerJSON.get("deliveryType");
        String providerID = (String) deliveryManagerJSON.get("providerID");
        JSONArray availableAccountsArray = (JSONArray) deliveryManagerJSON.get("availableAccounts");

        Map<String, String> commodityIDs = new HashMap<String/*commodityID*/, String/*deliveryType*/>();
        for (int i=0; i<availableAccountsArray.size(); i++) {
          Account newAccount = new Account((JSONObject) availableAccountsArray.get(i));
          if(newAccount.getCreditable()){
            commodityIDs.put(newAccount.getAccountID(), requestClass+"-"+deliveryType);
          }
        }
        if(!commodityIDs.isEmpty()){
          if(!commodities.keySet().contains(providerID)){
            commodities.put(providerID, commodityIDs);
          }else{
            commodities.get(providerID).putAll(commodityIDs);
          }
        }

        log.debug("GUIUtils.computeCommodities() : get information from deliveryManager "+deliveryManager+" DONE");

      }else{

        log.debug("GUIUtils.computeCommodities() : skip deliveryManager "+deliveryManager);

      }

    }

    //=========================================================================================
    //TODO :   | TODO : REMOVE THIS   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!
    //        \|/
    //=========================================================================================
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
  *  getPaymentMeans
  *
  *****************************************/

  public static Map<String, Map<String, String>> getPaymentMeans() {
    if (paymentMeans == null) {
      paymentMeans = new HashMap<String, Map<String, String>>();
      computePaymentMeans();
    }
    return paymentMeans;
  }

  public static void computePaymentMeans() {
    for(DeliveryManagerDeclaration deliveryManager : getDeliveryManagers().values()){
      CommoditySelection requestClass = CommoditySelection.fromExternalRepresentation(deliveryManager.getRequestClassName());
      if((requestClass != null) && (requestClass.equals(CommoditySelection.IN))){
        log.debug("GUIUtils.computePaymentMeans() : get information from deliveryManager "+deliveryManager);
        // get information
        JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
        String deliveryType = (String) deliveryManagerJSON.get("deliveryType");
        String providerID = (String) deliveryManagerJSON.get("providerID");
        JSONArray availableAccountsArray = (JSONArray) deliveryManagerJSON.get("availableAccounts");

        Map<String, String> paymentMeanIDs = new HashMap<String/*paymentMeanID*/, String/*deliveryType*/>();
        for (int i=0; i<availableAccountsArray.size(); i++) {
          Account newAccount = new Account((JSONObject) availableAccountsArray.get(i));
          if(newAccount.getDebitable()){
            paymentMeanIDs.put(newAccount.getAccountID(), requestClass+"-"+deliveryType);
          }
        }
        if(!paymentMeanIDs.isEmpty()){
          if(!paymentMeans.keySet().contains(providerID)){
            paymentMeans.put(providerID, paymentMeanIDs);
          }else{
            paymentMeans.get(providerID).putAll(paymentMeanIDs);
          }
        }

        log.debug("GUIUtils.computePaymentMeans() : get information from deliveryManager "+deliveryManager+" DONE");

      }else{

        log.debug("GUIUtils.computePaymentMeans() : skip deliveryManager "+deliveryManager);

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
    //=========================================================================================
    //        /|\
    //TODO :   | TODO : REMOVE THIS   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!   !!!
    //=========================================================================================

  }

  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  /*===================================================================================================*/
  /*===================================================================================================*/
  /*                               REMOVE                                                              */
  /*===================================================================================================*/
  /*===================================================================================================*/
  
  
  public static void generateMaps(Map<String, Map<String, String>> paymentMeans, Map<String, Map<String, String>> commodities) {
    for(DeliveryManagerDeclaration deliveryManager : getDeliveryManagers().values()){
      RequestClass requestClass = RequestClass.fromExternalRepresentation(deliveryManager.getRequestClassName());
      if((requestClass != null) && (requestClass.equals(RequestClass.IN))){
        log.debug("CommodityActionManager() : get information from deliveryManager "+deliveryManager);
        // get information
        JSONObject deliveryManagerJSON = deliveryManager.getJSONRepresentation();
        String deliveryType = (String) deliveryManagerJSON.get("deliveryType");
        String providerID = (String) deliveryManagerJSON.get("providerID");
        JSONArray availableAccountsArray = (JSONArray) deliveryManagerJSON.get("availableAccounts");

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
  
  
  
  private static Map<String,DeliveryManagerDeclaration> getDeliveryManagers() {
    // Testing 
    //
    // to test with main(), comment next line and uncomment following line
    
    return Deployment.getDeliveryManagers();
//    return getDeliveryManagersTest();
  }

  private static JSONObject jsonRoot;
  private static Map<String,DeliveryManagerDeclaration> getDeliveryManagersTest() {
    Map<String,DeliveryManagerDeclaration> deliveryManagers = new LinkedHashMap<String,DeliveryManagerDeclaration>();
    JSONArray deliveryManagerValues = JSONUtilities.decodeJSONArray(jsonRoot, "deliveryManagers", true);
    for (int i=0; i<deliveryManagerValues.size(); i++) {
        JSONObject deliveryManagerJSON = (JSONObject) deliveryManagerValues.get(i);
        DeliveryManagerDeclaration deliveryManagerDeclaration;
        try {
          deliveryManagerDeclaration = new DeliveryManagerDeclaration(deliveryManagerJSON);
          deliveryManagers.put(deliveryManagerDeclaration.getDeliveryType(), deliveryManagerDeclaration);
        } catch (NoSuchMethodException | IllegalAccessException e) {
          e.printStackTrace();
        }
      }
    return deliveryManagers;
  }
  
  
  /**
   * To test
   */
  public static void main(String[] args) {
    String jsonRootStr = "{"
        + "\"deliveryManagers\" : [\n" + 
        "      {\n" + 
        "        \"deliveryType\"                   : \"sampleFulfillment\", \n" + 
        "        \"requestClass\"                   : \"com.evolving.nglm.carrierd.SampleFulfillmentManager$SampleFulfillmentRequest\", \n" + 
        "        \"requestTopic\"                   : \"${topic.fulfillment.samplefulfillment.request}\", \n" + 
        "        \"responseTopic\"                  : \"${topic.fulfillment.samplefulfillment.response}\",\n" + 
        "        \"internalTopic\"                  : \"${topic.fulfillment.samplefulfillment.internal}\",\n" + 
        "        \"routingTopic\"                   : \"${topic.fulfillment.samplefulfillment.routing}\",\n" + 
        "        \"retries\"                        : 2,\n" + 
        "        \"acknowledgementTimeoutSeconds\"  : 10,\n" + 
        "        \"correlatorUpdateTimeoutSeconds\" : 15,\n" + 
        "        \"deliveryRatePerMinute\"          : 60,\n" + 
        "        \"deliveryGuarantee\"              : \"atLeastOnce\",\n" + 
        "        \"availableAccounts\"              : [ \"intra\", \"extra\" ]\n" + 
        "      },\n" + 
        "      {\n" + 
        "        \"deliveryType\"              : \"inFulfillmentFakeA\",\n" + 
        "        \"requestClass\"              : \"com.evolving.nglm.evolution.INFulfillmentManager$INFulfillmentRequest\",\n" + 
        "        \"requestTopic\"              : \"${topic.fulfillment.infulfillment.request.A}\",\n" + 
        "        \"responseTopic\"             : \"${topic.fulfillment.infulfillment.response.A}\",\n" + 
        "        \"internalTopic\"             : \"${topic.fulfillment.infulfillment.internal.A}\",\n" + 
        "        \"routingTopic\"              : \"${topic.fulfillment.infulfillment.routing.A}\",\n" + 
        "        \"deliveryRatePerMinute\"     : 60,\n" + 
        "        \"deliveryGuarantee\"         : \"atLeastOnce\",\n" + 
        "        \"providerID\"                : \"2\",\n" + 
        "        \"availableAccounts\"         : [  {\"name\":\"intra_A\", \"accountID\" : \"1\", \"creditable\" : true, \"debitable\" : true},  {\"name\":\"extra_A\", \"accountID\" : \"2\", \"creditable\" : true, \"debitable\" : false},  {\"name\":\"median_A\", \"accountID\" : \"3\", \"creditable\" : true, \"debitable\" : true}],\n" + 
        "        \"inPluginClass\"             : \"com.evolving.nglm.carrierd.FakeINPlugin\",\n" + 
        "        \"inPluginConfiguration\"     : \n" + 
        "          {\n" + 
        "            \"inServers\"             : \"<_FAKE_IN_IN_SERVERS_A_>\"\n" + 
        "          },\n" + 
        "        \"dateFormat\"                : \"yyyy-MM-dd'T'HH:mm:ss:XX\"\n" + 
        "      },\n" + 
        "      {\n" + 
        "        \"deliveryType\"              : \"inFulfillmentFakeB\",\n" + 
        "        \"requestClass\"              : \"com.evolving.nglm.evolution.INFulfillmentManager$INFulfillmentRequest\",\n" + 
        "        \"requestTopic\"              : \"${topic.fulfillment.infulfillment.request.B}\",\n" + 
        "        \"responseTopic\"             : \"${topic.fulfillment.infulfillment.response.B}\",\n" + 
        "        \"internalTopic\"             : \"${topic.fulfillment.infulfillment.internal.B}\",\n" + 
        "        \"routingTopic\"              : \"${topic.fulfillment.infulfillment.routing.B}\",\n" + 
        "        \"deliveryRatePerMinute\"     : 60,\n" + 
        "        \"deliveryGuarantee\"         : \"atLeastOnce\",\n" + 
        "        \"providerID\"                : \"3\",\n" + 
        "        \"availableAccounts\"         : [  {\"name\":\"intra_B\", \"accountID\" : \"1\", \"creditable\" : true, \"debitable\" : true},  {\"name\":\"extra_B\", \"accountID\" : \"2\", \"creditable\" : true, \"debitable\" : false} ],\n" + 
        "        \"inPluginClass\"             : \"com.evolving.nglm.carrierd.FakeINPlugin\",\n" + 
        "        \"inPluginConfiguration\"     : \n" + 
        "          {\n" + 
        "            \"inServers\"             : \"<_FAKE_IN_IN_SERVERS_B_>\"\n" + 
        "          },\n" + 
        "        \"dateFormat\"                : \"yyyy-MM-dd'T'HH:mm:ss:XX\"\n" + 
        "      },\n" + 
        "      {\n" + 
        "        \"deliveryType\"              : \"purchaseFulfillment\",\n" + 
        "        \"requestClass\"              : \"com.evolving.nglm.evolution.purchase.PurchaseFulfillmentManager$PurchaseFulfillmentRequest\",\n" + 
        "        \"requestTopic\"              : \"${topic.fulfillment.purchasefulfillment.request}\",\n" + 
        "        \"responseTopic\"             : \"${topic.fulfillment.purchasefulfillment.response}\",\n" + 
        "        \"internalTopic\"             : \"${topic.fulfillment.purchasefulfillment.internal}\",\n" + 
        "        \"routingTopic\"              : \"${topic.fulfillment.purchasefulfillment.routing}\",\n" + 
        "        \"deliveryRatePerMinute\"     : 60,\n" + 
        "        \"deliveryGuarantee\"         : \"atLeastOnce\"\n" + 
        "      },\n" + 
        "      {\n" + 
        "        \"deliveryType\"              : \"notificationmanagersms\",\n" + 
        "        \"requestClass\"              : \"com.evolving.nglm.evolution.SMSNotificationManager$SMSNotificationManagerRequest\",\n" + 
        "        \"requestTopic\"              : \"${topic.notificationmanagersms.request}\",\n" + 
        "        \"responseTopic\"             : \"${topic.notificationmanagersms.response}\",\n" + 
        "        \"internalTopic\"             : \"${topic.notificationmanagersms.internal}\",\n" + 
        "        \"routingTopic\"              : \"${topic.notificationmanagersms.routing}\",\n" + 
        "        \"deliveryRatePerMinute\"     : 60,\n" + 
        "        \"deliveryGuarantee\"         : \"atLeastOnce\",\n" + 
        "        \"acknowledgementTimeoutSeconds\" : 10,\n" + 
        "        \"notificationPluginClass\"   : \"com.evolving.nglm.evolution.SMPPPlugin\",\n" + 
        "        \"notificationPluginConfiguration\" : \n" + 
        "          {\n" + 
        "            \"smscConnection\"        : \"<_SMSC_STUB_>\",\n" + 
        "            \"username\"              : \"smppclient-mt\",\n" + 
        "            \"password\"              : \"password\",\n" + 
        "            \"connectionType\"        : \"TRX\"\n" + 
        "          }\n" + 
        "       },\n" + 
        "       {\n" + 
        "        \"deliveryType\"              : \"notificationmanagermail\",\n" + 
        "        \"requestClass\"              : \"com.evolving.nglm.evolution.MailNotificationManager$MailNotificationManagerRequest\",\n" + 
        "        \"requestTopic\"              : \"${topic.notificationmanagermail.request}\",\n" + 
        "        \"responseTopic\"             : \"${topic.notificationmanagermail.response}\",\n" + 
        "        \"internalTopic\"             : \"${topic.notificationmanagermail.internal}\",\n" + 
        "        \"routingTopic\"              : \"${topic.notificationmanagermail.routing}\",\n" + 
        "        \"deliveryRatePerMinute\"     : 60,\n" + 
        "        \"deliveryGuarantee\"         : \"atLeastOnce\",\n" + 
        "        \"notificationPluginClass\"   : \"com.evolving.nglm.evolution.SMTPPlugin\",\n" + 
        "        \"notificationPluginConfiguration\"     : \n" + 
        "          {\n" + 
        "            \"username\"              : \"smppclient-mt\",\n" + 
        "            \"password\"              : \"password\"\n" + 
        "          }\n" + 
        "       }\n" + 
        "    ]}\n";
    try {
      org.apache.log4j.Logger.getRootLogger().getLoggerRepository().resetConfiguration(); // remove all appenders
      ConsoleAppender console = new ConsoleAppender();
      console.setLayout(new PatternLayout("[%d] %p %m (%c)%n")); 
      console.setThreshold(Level.ALL);
      console.activateOptions();
      org.apache.log4j.Logger.getRootLogger().addAppender(console);
            
      jsonRoot = (JSONObject) (new JSONParser()).parse(jsonRootStr);
      Map<String, Map<String, String>> paymentMeans /*debit only*/ = new HashMap<String/*providerID*/, Map<String/*paymentMeanID*/, String/*requestClass+"-"+deliveryType*/>>();
      Map<String, Map<String, String>> commodities /*credit only*/ = new HashMap<String/*providerID*/, Map<String/*commodityID*/, String/*requestClass+"-"+deliveryType*/>>();
      generateMaps(paymentMeans, commodities);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
  
}


