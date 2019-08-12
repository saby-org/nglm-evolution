/*****************************************************************************
*
*  ThirdPartyJSONGenerator.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;

public class ThirdPartyJSONGenerator 
{
  
  /*****************************************
  *
  *  generateOfferJSONForThirdParty
  *
  *****************************************/
  
  protected static JSONObject generateOfferJSONForThirdParty(Offer offer) 
  {
    HashMap<String, Object> offerMap = new HashMap<String, Object>();
    if ( null == offer ) return JSONUtilities.encodeObject(offerMap);
    offerMap.put("offerID", offer.getOfferID());
    offerMap.put("offerName", offer.getGUIManagedObjectName());
    offerMap.put("offerInitialPropensity", offer.getInitialPropensity());
    offerMap.put("offerUnitaryCost", offer.getUnitaryCost());
    List<JSONObject> products = offer.getOfferProducts().stream().map(product -> ThirdPartyJSONGenerator.generateProductJSONForThirdParty(product)).collect(Collectors.toList());
    offerMap.put("products", products);
    return JSONUtilities.encodeObject(offerMap);
  }
  
  /*****************************************
  *
  *  generateOfferJSONForThirdParty
  *
  *****************************************/
  
  public static JSONObject generateOfferJSONForThirdParty(Offer offer, OfferService offerService, OfferObjectiveService offerObjectiveService)
  {
    HashMap<String, Object> offerMap = new HashMap<String, Object>();
    if ( null == offer ) return JSONUtilities.encodeObject(offerMap);
    offerMap.put("offerID", offer.getOfferID());
    offerMap.put("offerName", offer.getGUIManagedObjectName());
    offerMap.put("offerState", offerService.isActiveOffer(offer, SystemTime.getCurrentTime()) ? "active" : "stored");
    offerMap.put("offerStartDate", getDateString(offer.getEffectiveStartDate()));
    offerMap.put("offerEndDate", getDateString(offer.getEffectiveEndDate()));
    offerMap.put("offerDescription", offer.getDescription());
    offerMap.put("offerOfferObjectiveNames", getOfferObjectivesJson(offer, offerObjectiveService));
    offerMap.put("offerInitialPropensity", offer.getInitialPropensity());
    offerMap.put("offerUnitaryCost", offer.getUnitaryCost());
    List<JSONObject> products = offer.getOfferProducts().stream().map(product -> ThirdPartyJSONGenerator.generateProductJSONForThirdParty(product)).collect(Collectors.toList());
    offerMap.put("products", products);
    return JSONUtilities.encodeObject(offerMap);
  }
  
  /*****************************************
  *
  *  getOfferObjectivesJson
  *
  *****************************************/
  
  private static JSONArray  getOfferObjectivesJson(Offer offer, OfferObjectiveService offerObjectiveService)
  {
    List<String> offerObjectiveNames = new ArrayList<String>();
    for (OfferObjectiveInstance instance : offer.getOfferObjectives())
      {
        offerObjectiveNames.add(offerObjectiveService.getStoredOfferObjective(instance.getOfferObjectiveID()).getGUIManagedObjectName());
      }
    return JSONUtilities.encodeArray(offerObjectiveNames);
  }

  /*****************************************
  *
  *  generateProductJSONForThirdParty
  *
  *****************************************/
  
  protected static JSONObject generateProductJSONForThirdParty(OfferProduct product) 
  {
    HashMap<String, Object> productMap = new HashMap<String, Object>();
    if ( null == product ) return JSONUtilities.encodeObject(productMap);
    productMap.put("productID", product.getProductID());
    productMap.put("quantity", product.getQuantity());
    return JSONUtilities.encodeObject(productMap);
  }
  
  /*****************************************
  *
  *  generateCurrencyJSONForThirdParty
  *
  *****************************************/
  
  protected static JSONObject generateCurrencyJSONForThirdParty(SupportedCurrency currency) 
  {
    HashMap<String, Object> currencyMap = new HashMap<String, Object>();
    if ( null == currency ) return JSONUtilities.encodeObject(currencyMap);
    currencyMap.put("display", currency.getDisplay());
    currencyMap.put("id", currency.getID());
    currencyMap.put("name", currency.getName());
    return JSONUtilities.encodeObject(currencyMap);
  }
  
  /*****************************************
  *
  *  generateTokenJSONForThirdParty
  *
  *****************************************/
  
  protected static JSONObject generateTokenJSONForThirdParty(Token token) 
  {
    HashMap<String, Object> tokenMap = new HashMap<String, Object>();
    if ( null == token ) return JSONUtilities.encodeObject(tokenMap);
    tokenMap.put("tokenStatus", token.getTokenStatus().getExternalRepresentation());
    tokenMap.put("creationDate", token.getCreationDate());
    tokenMap.put("boundDate", token.getBoundDate());
    tokenMap.put("redeemedDate", token.getRedeemedDate());
    tokenMap.put("tokenExpirationDate", token.getTokenExpirationDate());
    tokenMap.put("boundCount", token.getBoundCount());
    tokenMap.put("eventID", token.getEventID());
    tokenMap.put("subscriberID", token.getSubscriberID());
    tokenMap.put("tokenTypeID", token.getTokenTypeID());
    tokenMap.put("moduleID", token.getModuleID());
    tokenMap.put("featureID", token.getFeatureID());
    if (token instanceof DNBOToken)
      {
        DNBOToken dnboToken = (DNBOToken) token;
        tokenMap.put("presentationStrategyID", dnboToken.getPresentationStrategyID());
        tokenMap.put("isAutoBounded", dnboToken.isAutoBounded());
        tokenMap.put("isAutoRedeemed", dnboToken.isAutoRedeemed());
        tokenMap.put("presentedOffersIDs", dnboToken.getPresentedOffersIDs());
        tokenMap.put("acceptedOfferID", dnboToken.getAcceptedOfferID());
      }
    return JSONUtilities.encodeObject(tokenMap);
  }
  
  /*****************************************
  *
  *  getDateString
  *
  *****************************************/

 public static String getDateString(Date date)
 {
   String result = null;
   if (date == null) return result;
   try
   {
     SimpleDateFormat dateFormat = new SimpleDateFormat(Deployment.getAPIresponseDateFormat());
     dateFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
     result = dateFormat.format(date);
   }
   catch (Exception e)
   {
     e.printStackTrace();
   }
   return result;
 }
}
