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
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;

public class ThirdPartyJSONGenerator 
{
  
  /*****************************************
  *
  *  generateLoyaltyProgramJSONForThirdParty
  *
  *****************************************/
  
  protected static JSONObject generateLoyaltyProgramJSONForThirdParty(LoyaltyProgram loyaltyProgram) 
  {
    HashMap<String, Object> loyaltyProgramMap = new HashMap<String, Object>();
    if ( null == loyaltyProgram ) return JSONUtilities.encodeObject(loyaltyProgramMap);
    
    loyaltyProgramMap.put("loyaltyProgramID", loyaltyProgram.getLoyaltyProgramID());
    loyaltyProgramMap.put("loyaltyProgramName", loyaltyProgram.getLoyaltyProgramName());
    loyaltyProgramMap.put("loyaltyProgramDescription", loyaltyProgram.getLoyaltyProgramDescription());
    loyaltyProgramMap.put("loyaltyProgramType", loyaltyProgram.getLoyaltyProgramType().getExternalRepresentation());
    List<JSONObject> characteristics = loyaltyProgram.getCharacteristics().stream().map(characteristic -> ThirdPartyJSONGenerator.generateCharacteristicJSONForThirdParty(characteristic)).collect(Collectors.toList());
    loyaltyProgramMap.put("characteristics", characteristics);

    switch (loyaltyProgram.getLoyaltyProgramType()) {
    case POINTS:
      LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints) loyaltyProgram;
      loyaltyProgramMap.put("statusPointsID", loyaltyProgramPoints.getStatusPointsID());
      loyaltyProgramMap.put("rewardPointsID", loyaltyProgramPoints.getRewardPointsID());
      List<JSONObject> tiers = loyaltyProgramPoints.getTiers().stream().map(tier -> ThirdPartyJSONGenerator.generateTierJSONForThirdParty(tier)).collect(Collectors.toList());
      loyaltyProgramMap.put("tiers", tiers);
      break;

    case BADGES:
      // TODO
      break;
      
    default:
      break;
    }
    return JSONUtilities.encodeObject(loyaltyProgramMap);
  }
  
  /*****************************************
  *
  *  generateCharacteristicJSONForThirdParty
  *
  *****************************************/
  
  protected static JSONObject generateCharacteristicJSONForThirdParty(CatalogCharacteristicInstance characteristic) 
  {
    HashMap<String, Object> characteristicMap = new HashMap<String, Object>();
    if ( null == characteristic ) return JSONUtilities.encodeObject(characteristicMap);
    characteristicMap.put("characteristicID", characteristic.getCatalogCharacteristicID());
    characteristicMap.put("value", characteristic.getValue());
    return JSONUtilities.encodeObject(characteristicMap);
  }
  
  /*****************************************
  *
  *  generateCharacteristicJSONForThirdParty
  *
  *****************************************/
  
  protected static JSONObject generateTierJSONForThirdParty(Tier tier) 
  {
    HashMap<String, Object> tierMap = new HashMap<String, Object>();
    if ( null == tier ) return JSONUtilities.encodeObject(tierMap);
    tierMap.put("tierName", tier.getTierName());
    tierMap.put("statusEventName", tier.getStatusEventName());
    tierMap.put("statusPointLevel", tier.getStatusPointLevel());
    tierMap.put("numberOfStatusPointsPerUnit", tier.getNumberOfStatusPointsPerUnit());
    tierMap.put("rewardEventName", tier.getRewardEventName());
    tierMap.put("numberOfRewardPointsPerUnit", tier.getNumberOfRewardPointsPerUnit());
    return JSONUtilities.encodeObject(tierMap);
  }
  
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
  
  protected static JSONObject generateTokenJSONForThirdParty(Token token, JourneyService journeyService, OfferService offerService) 
  {
    Date now = new Date();
    HashMap<String, Object> tokenMap = new HashMap<String, Object>();
    if ( null == token ) return JSONUtilities.encodeObject(tokenMap);
    tokenMap.put("tokenStatus", token.getTokenStatus().getExternalRepresentation());
    tokenMap.put("creationDate", getDateString(token.getCreationDate()));
    tokenMap.put("boundDate", getDateString(token.getBoundDate()));
    tokenMap.put("redeemedDate", getDateString(token.getRedeemedDate()));
    tokenMap.put("tokenExpirationDate", getDateString(token.getTokenExpirationDate()));
    tokenMap.put("boundCount", token.getBoundCount());
    //tokenMap.put("eventID", token.getEventID());
    //tokenMap.put("subscriberID", token.getSubscriberID());
    tokenMap.put("tokenTypeID", token.getTokenTypeID());
    Module module = Module.fromExternalRepresentation(token.getModuleID());
    tokenMap.put("moduleName", module.toString());
    Integer featureID = token.getFeatureID();
    tokenMap.put("featureName", (featureID==null) ? "unknown feature" : ThirdPartyManager.getFeatureName(module, featureID.toString(), journeyService, offerService));
    tokenMap.put("tokenCode", token.getTokenCode());
    if (token instanceof DNBOToken)
      {
        DNBOToken dnboToken = (DNBOToken) token;
        //tokenMap.put("presentationStrategyID", dnboToken.getPresentationStrategyID());
        tokenMap.put("isAutoBounded", dnboToken.isAutoBounded());
        tokenMap.put("isAutoRedeemed", dnboToken.isAutoRedeemed());
        
        ArrayList<Object> presentedOffersList = new ArrayList<>();
        for (String offerID : dnboToken.getPresentedOfferIDs())
          {
            presentedOffersList.add(JSONUtilities.encodeObject(buildOfferElement(offerID, offerService, now)));
          }
        tokenMap.put("presentedOffers", JSONUtilities.encodeArray(presentedOffersList));
        
        String offerID = dnboToken.getAcceptedOfferID();
        tokenMap.put("acceptedOffer", JSONUtilities.encodeObject(buildOfferElement(offerID, offerService, now)));
      }
    return JSONUtilities.encodeObject(tokenMap);
  }
  
  private static HashMap<String, Object> buildOfferElement(String offerID, OfferService offerService, Date now) {
    HashMap<String, Object> offerMap = new HashMap<String, Object>();
    offerMap.put("id", offerID);
    if (offerID == null)
      {
        offerMap.put("name",null);
      }
    else
      {
        Offer offer = offerService.getActiveOffer(offerID, now);
        offerMap.put("name", (offer == null) ? "unknown offer" : offer.getDisplay());
      }
    return offerMap;
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
