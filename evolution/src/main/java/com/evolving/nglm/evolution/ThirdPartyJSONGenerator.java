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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;
import com.evolving.nglm.evolution.OfferCharacteristics.OfferCharacteristicsLanguageProperty;

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

//    case BADGES:
//      // TODO
//      break;
      
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
  
  protected static JSONObject generateOfferJSONForThirdParty(Offer offer, ProductService productService) 
  {
    HashMap<String, Object> offerMap = new HashMap<String, Object>();
    if ( null == offer ) return JSONUtilities.encodeObject(offerMap);
    offerMap.put("offerID", offer.getOfferID());
    offerMap.put("offerName", offer.getGUIManagedObjectName());
    offerMap.put("offerInitialPropensity", offer.getInitialPropensity());
    offerMap.put("offerUnitaryCost", offer.getUnitaryCost());
    List<JSONObject> products = offer.getOfferProducts().stream().map(product -> ThirdPartyJSONGenerator.generateProductJSONForThirdParty(product, productService)).collect(Collectors.toList());
    offerMap.put("products", products);
    return JSONUtilities.encodeObject(offerMap);
  }
  
  /*****************************************
  *
  *  generateOfferJSONForThirdParty
  *
  *****************************************/
  
  public static JSONObject generateOfferJSONForThirdParty(Offer offer, OfferService offerService, OfferObjectiveService offerObjectiveService, ProductService productService, SalesChannelService salesChannelService)
  {
    HashMap<String, Object> offerMap = new HashMap<String, Object>();
    if ( null == offer ) return JSONUtilities.encodeObject(offerMap);
    offerMap.put("offerID", offer.getOfferID());
    offerMap.put("offerName", offer.getGUIManagedObjectName());
    offerMap.put("offerState", offerService.isActiveOffer(offer, SystemTime.getCurrentTime()) ? "active" : "stored");
    offerMap.put("offerStartDate", getDateString(offer.getEffectiveStartDate()));
    offerMap.put("offerEndDate", getDateString(offer.getEffectiveEndDate()));
    offerMap.put("offerDescription", offer.getDescription());
    offerMap.put("offerExternalID", offer.getJSONRepresentation().get("externalID")!=null?offer.getJSONRepresentation().get("externalID"):"");
    offerMap.put("offerAvailableStock", offer.getJSONRepresentation().get("presentationStock")!=null?offer.getJSONRepresentation().get("presentationStock"):"");
    offerMap.put("offerAvailableStockAlertThreshold", offer.getJSONRepresentation().get("presentationStockAlertThreshold")!=null?offer.getJSONRepresentation().get("presentationStockAlertThreshold"):"");
    offerMap.put("offerImageURL", offer.getJSONRepresentation().get("imageURL")!=null?offer.getJSONRepresentation().get("imageURL"):"");
    offerMap.put("offerObjectives", getOfferObjectivesJson(offer, offerObjectiveService));
    offerMap.put("offerCharacteristics", offer.getOfferCharacteristics().toJSONObject()!=null?offer.getOfferCharacteristics().toJSONObject():"");
    offerMap.put("offerSalesChannels", getOfferSalesChannelsJson(offer, salesChannelService));
    offerMap.put("offerInitialPropensity", offer.getInitialPropensity());
    offerMap.put("offerUnitaryCost", offer.getUnitaryCost());
    List<JSONObject> products = offer.getOfferProducts().stream().map(product -> ThirdPartyJSONGenerator.generateProductJSONForThirdParty(product, productService)).collect(Collectors.toList());
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
    List<JSONObject> offerObjectives = new ArrayList<JSONObject>();
    if(offer.getOfferObjectives() != null)
      {
        for (OfferObjectiveInstance instance : offer.getOfferObjectives())
          {
            offerObjectives.add(offerObjectiveService.getStoredOfferObjective(instance.getOfferObjectiveID()).getJSONRepresentation());
          }
      }
    return JSONUtilities.encodeArray(offerObjectives);
  }
  
  /*****************************************
  *
  *  getOfferObjectivesJson
  *
  *****************************************/
  
  private static JSONArray  getOfferSalesChannelsJson(Offer offer, SalesChannelService salesChannelService)
  {
    List<JSONObject> offerSalesChannels = new ArrayList<JSONObject>();
    if(offer.getOfferSalesChannelsAndPrices() != null)
      {
        for(OfferSalesChannelsAndPrice channel : offer.getOfferSalesChannelsAndPrices())
          {
            if(channel.getSalesChannelIDs() != null) 
              {
                for(String salesChannelID : channel.getSalesChannelIDs()) 
                  {
                    SalesChannel salesChannel = salesChannelService.getActiveSalesChannel(salesChannelID, SystemTime.getCurrentTime());
                    if(salesChannel != null)
                      {
                        JSONObject channelObject = new JSONObject();
                        channelObject.put("salesChannelID", salesChannel.getSalesChannelID());
                        channelObject.put("salesChannelName", salesChannel.getSalesChannelName());

                        offerSalesChannels.add(channelObject);

                      }
                  }
              }
          }
      }
    return JSONUtilities.encodeArray(offerSalesChannels);
  }

  /*****************************************
  *
  *  generateProductJSONForThirdParty
  *
  *****************************************/
  
  protected static JSONObject generateProductJSONForThirdParty(OfferProduct offerProduct, ProductService productService) 
  {
    HashMap<String, Object> productMap = new HashMap<String, Object>();
    if ( null == offerProduct ) return JSONUtilities.encodeObject(productMap);
    productMap.put("productID", offerProduct.getProductID());
    Product product = (Product) productService.getStoredGUIManagedObject(offerProduct.getProductID());
    if(product != null)
      {
        productMap.put("productName", product.getJSONRepresentation().get("display"));
      }
    productMap.put("quantity", offerProduct.getQuantity());
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
  
  protected static JSONObject generateTokenJSONForThirdParty(Token token, JourneyService journeyService, OfferService offerService, ScoringStrategyService scoringStrategyService) 
  {
    Date now = SystemTime.getCurrentTime();
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
        
        ArrayList<Object> scoringStrategiesList = new ArrayList<>();
        for (String scoringStrategyID : dnboToken.getScoringStrategyIDs())
          {
            scoringStrategiesList.add(JSONUtilities.encodeObject(buildStrategyElement(scoringStrategyID, scoringStrategyService, now)));
          }
        tokenMap.put("scoringStrategies", JSONUtilities.encodeArray(scoringStrategiesList));        
        
        ArrayList<Object> presentedOffersList = new ArrayList<>();
        for (String offerID : dnboToken.getPresentedOfferIDs())
          {
            presentedOffersList.add(JSONUtilities.encodeObject(buildOfferElement(offerID, offerService, now)));
          }
        tokenMap.put("presentedOffers", JSONUtilities.encodeArray(presentedOffersList));
        tokenMap.put("presentedOffersSalesChannel", dnboToken.getPresentedOffersSalesChannel());
        
        String offerID = dnboToken.getAcceptedOfferID();
        tokenMap.put("acceptedOffer", JSONUtilities.encodeObject(buildOfferElement(offerID, offerService, now)));
      }
    return JSONUtilities.encodeObject(tokenMap);
  }
  
  private static HashMap<String, Object> buildStrategyElement(String scoringStrategyID, ScoringStrategyService scoringStrategyService, Date now) {
    HashMap<String, Object> scoringStrategyMap = new HashMap<String, Object>();
    scoringStrategyMap.put("id", scoringStrategyID);
    if (scoringStrategyID == null)
      {
        scoringStrategyMap.put("name",null);
      }
    else
      {
        ScoringStrategy scoringStrategy = scoringStrategyService.getActiveScoringStrategy(scoringStrategyID, now);
        scoringStrategyMap.put("name", (scoringStrategy == null) ? "unknown scoring strategy" : scoringStrategy.getGUIManagedObjectName());
      }
    return scoringStrategyMap;
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
