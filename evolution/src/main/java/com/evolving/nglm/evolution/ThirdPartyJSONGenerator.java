/*****************************************************************************
*
*  ThirdPartyJSONGenerator.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;
import com.evolving.nglm.evolution.offeroptimizer.ProposedOfferDetails;

import kafka.log.Log;

public class ThirdPartyJSONGenerator 
{
  private static final String UNKNOWN_OFFER = "unknown offer";
  private static final Logger log = LoggerFactory.getLogger(ThirdPartyJSONGenerator.class);

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
  
  public static JSONObject generateOfferJSONForThirdParty(Offer offer, OfferService offerService, OfferObjectiveService offerObjectiveService, ProductService productService, VoucherService voucherService, SalesChannelService salesChannelService)
  {
    HashMap<String, Object> offerMap = new HashMap<String, Object>();
    if ( null == offer ) return JSONUtilities.encodeObject(offerMap);
    offerMap.put("offerID", offer.getOfferID());
    offerMap.put("offerDisplay", offer.getDisplay());
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
    List<JSONObject> products = offer.getOfferProducts()==null?null:offer.getOfferProducts().stream().map(product -> ThirdPartyJSONGenerator.generateProductJSONForThirdParty(product, productService)).collect(Collectors.toList());
    offerMap.put("products", products);
    List<JSONObject> vouchers = offer.getOfferVouchers()==null?null:offer.getOfferVouchers().stream().map(voucher -> ThirdPartyJSONGenerator.generateVoucherJSONForThirdParty(voucher, voucherService)).collect(Collectors.toList());
    offerMap.put("vouchers", vouchers);
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
    if (offer != null && offer.getOfferObjectives() != null)
      {
        for (OfferObjectiveInstance instance : offer.getOfferObjectives())
          {
            GUIManagedObject offerObjective = offerObjectiveService.getStoredOfferObjective(instance.getOfferObjectiveID());
            if (offerObjective != null)
              {
                offerObjectives.add(offerObjective.getJSONRepresentation());
              }
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
    OfferPrice offerPrice = new OfferPrice();
    
    if(offer.getOfferSalesChannelsAndPrices() != null)
      {
        for(OfferSalesChannelsAndPrice channel : offer.getOfferSalesChannelsAndPrices())
          {
            if (channel.getPrice() != null)
              {
                offerPrice = channel.getPrice();
              }
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
                        if (offerPrice != null)
                          {
                            channelObject.put("paymentMeanID", offerPrice.getPaymentMeanID());
                            channelObject.put("amount", offerPrice.getAmount());
                          }
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
   *  generateVoucherJSONForThirdParty
   *
   *****************************************/

  protected static JSONObject generateVoucherJSONForThirdParty(OfferVoucher offerVoucher, VoucherService voucherService)
  {
    HashMap<String, Object> voucherMap = new HashMap<String, Object>();
    if ( null == offerVoucher ) return JSONUtilities.encodeObject(voucherMap);
    voucherMap.put("voucherID", offerVoucher.getVoucherID());
    Voucher voucher = (Voucher) voucherService.getStoredGUIManagedObject(offerVoucher.getVoucherID());
    if(voucher != null)
    {
      voucherMap.put("voucherName", voucher.getJSONRepresentation().get("display"));
    }
    voucherMap.put("quantity", offerVoucher.getQuantity());
    return JSONUtilities.encodeObject(voucherMap);
  }
  
  /*****************************************
  *
  *  getResellerJson
  *
  *****************************************/
  
  protected static JSONObject generateResellerJSONForThirdParty(Reseller reseller, ResellerService resellerService)
  {
    HashMap<String, Object> resellerDetailsMap = new HashMap<String, Object>();
    if (null == reseller)
      return JSONUtilities.encodeObject(resellerDetailsMap);
    resellerDetailsMap.put("resellerDisplay", reseller.getGUIManagedObjectDisplay());
    resellerDetailsMap.put("resellerName", reseller.getGUIManagedObjectName());
    resellerDetailsMap.put("resellerDescription", reseller.getJSONRepresentation().get("description"));
    resellerDetailsMap.put("resellerWebsite", reseller.getWebsite());
    resellerDetailsMap.put("resellerActive",
        resellerService.isActiveReseller(reseller, SystemTime.getCurrentTime()) ? "active" : "inactive");
    resellerDetailsMap.put("resellerCreatedDate", getDateString(reseller.getCreatedDate()));
    resellerDetailsMap.put("resellerUpdatedDate", getDateString(reseller.getUpdatedDate()));
    resellerDetailsMap.put("resellerUserIDs", reseller.getUserIDs());
    resellerDetailsMap.put("resellerEmail", reseller.getEmail());
    resellerDetailsMap.put("resellerPhone", reseller.getPhone());
    resellerDetailsMap.put("resellerParentID", reseller.getParentResellerID());
    return JSONUtilities.encodeObject(resellerDetailsMap);
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
   * @param offerObjectiveService 
  *
  *****************************************/
  protected static JSONObject generateTokenJSONForThirdParty(Token token, JourneyService journeyService, OfferService offerService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, OfferObjectiveService offerObjectiveService, LoyaltyProgramService loyaltyProgramService, int tenantID)
  {
    return generateTokenJSONForThirdParty(token, journeyService, offerService, scoringStrategyService, presentationStrategyService, offerObjectiveService, loyaltyProgramService, null, null, null, null, tenantID);
  }
  protected static JSONObject generateTokenJSONForThirdParty(Token token, JourneyService journeyService, OfferService offerService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, OfferObjectiveService offerObjectiveService, LoyaltyProgramService loyaltyProgramService, TokenTypeService tokenTypeService, int tenantID)
  {
    return generateTokenJSONForThirdParty(token, journeyService, offerService, scoringStrategyService, presentationStrategyService, offerObjectiveService, loyaltyProgramService, tokenTypeService, null, null, null, tenantID);
  }
  
  protected static JSONObject generateTokenJSONForThirdParty(Token token, JourneyService journeyService, OfferService offerService, ScoringStrategyService scoringStrategyService, PresentationStrategyService presentationStrategyService, OfferObjectiveService offerObjectiveService, LoyaltyProgramService loyaltyProgramService, TokenTypeService tokenTypeService, CallingChannel callingChannel, Collection<ProposedOfferDetails> presentedOffers, PaymentMeanService paymentMeanService, int tenantID) 
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
    String tokenTypeID = token.getTokenTypeID();
    tokenMap.put("tokenTypeID", tokenTypeID);
    TokenType tokenType = tokenTypeService.getActiveTokenType(tokenTypeID, now);
    tokenMap.put("tokenTypeDisplay", tokenType != null ? tokenType.getGUIManagedObjectDisplay() : "unknownTokenType");
    Module module = Module.fromExternalRepresentation(token.getModuleID());
    tokenMap.put("moduleName", module.toString());
    String featureID = token.getFeatureID();
    tokenMap.put("featureName", (featureID==null) ? "unknown feature" : DeliveryRequest.getFeatureDisplay(module, featureID, journeyService, offerService, loyaltyProgramService));
    tokenMap.put("tokenCode", token.getTokenCode());
    if (token instanceof DNBOToken)
      {
        DNBOToken dnboToken = (DNBOToken) token;
        //tokenMap.put("presentationStrategyID", dnboToken.getPresentationStrategyID());
        tokenMap.put("isAutoBound", dnboToken.isAutoBound());
        tokenMap.put("isAutoRedeemed", dnboToken.isAutoRedeemed());
        
        ArrayList<Object> scoringStrategiesList = new ArrayList<>();
        for (String scoringStrategyID : dnboToken.getScoringStrategyIDs())
          {
            scoringStrategiesList.add(JSONUtilities.encodeObject(buildScoringStrategyElement(scoringStrategyID, scoringStrategyService, now, tenantID)));
          }
        tokenMap.put("scoringStrategies", JSONUtilities.encodeArray(scoringStrategiesList));        
        
        String presentationStrategyID = dnboToken.getPresentationStrategyID();
        tokenMap.put("presentationStrategy", JSONUtilities.encodeObject(buildPresentationStrategyElement(presentationStrategyID, presentationStrategyService, now)));
        
        ArrayList<Object> presentedOffersList = new ArrayList<>();
        for (String offerID : dnboToken.getPresentedOfferIDs())
          {
            presentedOffersList.add(JSONUtilities.encodeObject(buildOfferElement(offerID, offerService, offerObjectiveService, now, callingChannel, presentedOffers, dnboToken, paymentMeanService, tenantID)));
          }
        tokenMap.put("presentedOffers", JSONUtilities.encodeArray(presentedOffersList));
        tokenMap.put("presentedOffersSalesChannel", dnboToken.getPresentedOffersSalesChannel());
        
        String offerID = dnboToken.getAcceptedOfferID();
        if (offerID == null)
          {
            tokenMap.put("acceptedOffer", null);
          }
        else
          {
            tokenMap.put("acceptedOffer", JSONUtilities.encodeObject(buildOfferElement(offerID, offerService, offerObjectiveService, now, callingChannel, presentedOffers, dnboToken, paymentMeanService, tenantID)));
          }
      }
    return JSONUtilities.encodeObject(tokenMap);
  }
  
  private static HashMap<String, Object> buildPresentationStrategyElement(String presentationStrategyID, PresentationStrategyService presentationStrategyService, Date now) {
    HashMap<String, Object> presentationStrategyMap = new HashMap<String, Object>();
    presentationStrategyMap.put("id", presentationStrategyID);
    if (presentationStrategyID == null)
      {
        presentationStrategyMap.put("name",null);
      }
    else
      {
        PresentationStrategy scoringStrategy = presentationStrategyService.getActivePresentationStrategy(presentationStrategyID, now);
        presentationStrategyMap.put("name", (scoringStrategy == null) ? "unknown presentation strategy" : scoringStrategy.getGUIManagedObjectDisplay());
      }
    return presentationStrategyMap;
  }
  
  private static HashMap<String, Object> buildScoringStrategyElement(String scoringStrategyID, ScoringStrategyService scoringStrategyService, Date now, int tenantID) {
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
  
  private static HashMap<String, Object> buildOfferElement(String offerID, OfferService offerService, OfferObjectiveService offerObjectiveService, Date now, CallingChannel callingChannel, Collection<ProposedOfferDetails> presentedOffers, DNBOToken dnboToken, PaymentMeanService paymentMeanService, int tenantID) {
    HashMap<String, Object> offerMap = new HashMap<String, Object>();
    offerMap.put("id", offerID);
    if (offerID == null)
      {
        offerMap.put("name",UNKNOWN_OFFER);
      }
    else
      {
        Offer offer = offerService.getActiveOffer(offerID, now);
        if (offer == null)
          {
            offerMap.put("name", UNKNOWN_OFFER); 
          }
        else
          {
            offerMap.put("name", offer.getDisplay());
            JSONObject offerJSON = offer.getJSONRepresentation();
            if (callingChannel != null && offerJSON != null)
              {
                // Add more elements to offerMap, based on what's in the channel
                JSONObject callingChannelJSON = callingChannel.getJSONRepresentation();
                if (callingChannelJSON != null)
                  {
                    JSONArray offerProperties = JSONUtilities.decodeJSONArray(callingChannelJSON, "offerProperties", false);
                    if (offerProperties != null)
                      {
                        for (int i=0; i<offerProperties.size(); i++)
                          {
                            JSONObject offerPropertyJSON = (JSONObject) offerProperties.get(i);
                            if (offerPropertyJSON != null)
                              {
                                boolean presentOffers = JSONUtilities.decodeBoolean(offerPropertyJSON, "presentOffers", Boolean.TRUE);
                                if (presentOffers)
                                  {
                                    String offerPropertyName = JSONUtilities.decodeString(offerPropertyJSON, "offerPropertyName", false);
                                    if (offerPropertyName != null) 
                                      {
                                        if ("offerScore".equals(offerPropertyName) && presentedOffers != null)
                                          {
                                            for (ProposedOfferDetails presentedOffer : presentedOffers)
                                              {
                                                if (offerID.equals(presentedOffer.getOfferId()))
                                                  {
                                                    offerMap.put("offerScore", presentedOffer.getOfferScore());
                                                    break;
                                                  }
                                              }
                                          }
                                        else if ("offerRank".equals(offerPropertyName) && presentedOffers != null)
                                          {
                                            int rank=1;
                                            for (ProposedOfferDetails presentedOffer : presentedOffers)
                                              {
                                                if (offerID.equals(presentedOffer.getOfferId()))
                                                  {
                                                    offerMap.put("offerRank", rank);
                                                    break;
                                                  }
                                                rank++;
                                              }
                                          }
                                        else if ("price".equals(offerPropertyName))
                                          {
                                            String salesChannel = dnboToken.getPresentedOffersSalesChannel();
                                            for (OfferSalesChannelsAndPrice sc : offer.getOfferSalesChannelsAndPrices())
                                              {
                                                if (sc.getSalesChannelIDs() != null && sc.getSalesChannelIDs().contains(salesChannel))
                                                  {
                                                    Map<String, Object> salesChannelJSON = new LinkedHashMap<>(); // to preserve order when displaying
                                                    String paymentMean = "";
                                                    String currency = "";
                                                    long amount = 0;
                                                    OfferPrice offerPrice = sc.getPrice(); // Can be null for free offer
                                                    if (offerPrice != null)
                                                      {
                                                        amount = offerPrice.getAmount();
                                                        String paymentMeanID = offerPrice.getPaymentMeanID();
                                                        String currencyID = offerPrice.getSupportedCurrencyID();
                                                        GUIManagedObject paymentMeanObject = paymentMeanService.getStoredPaymentMean(paymentMeanID);
                                                        if (paymentMeanObject != null && (paymentMeanObject instanceof PaymentMean))
                                                          {
                                                            paymentMean = ((PaymentMean) paymentMeanObject).getDisplay();
                                                          }
                                                        if (currencyID != null)
                                                          {
                                                            for (SupportedCurrency supportedCurrency : Deployment.getDeployment(tenantID).getSupportedCurrencies().values())
                                                              {
                                                                JSONObject supportedCurrencyJSON = supportedCurrency.getJSONRepresentation();
                                                                if (supportedCurrencyJSON != null && currencyID.equals(supportedCurrencyJSON.get("id")))
                                                                  {
                                                                    currency = "" + supportedCurrencyJSON.get("display");
                                                                    break;
                                                                  }
                                                              }
                                                          }
                                                      }
                                                    salesChannelJSON.put("paymentMean", paymentMean);
                                                    salesChannelJSON.put("amount", amount);
                                                    salesChannelJSON.put("currency", currency);
                                                    offerMap.put("price", salesChannelJSON);
                                                    break;
                                                  }
                                              }
                                          }
                                        else
                                          {
                                            Object offerProperty = offerJSON.get(offerPropertyName);
                                            if (offerProperty != null)
                                              {
                                                log.debug("Adding property " + offerPropertyName + " : " + offerProperty);
                                                offerMap.put(offerPropertyName, offerProperty);
                                              }
                                          }
                                      }
                                  }
                              }
                          }
                      }
                    // build an array of what's in the offer to speed up processing
                    Map<String,Object>offerPropertiesMap = new HashMap<>();

                    JSONObject offerCharacteristics = JSONUtilities.decodeJSONObject(offerJSON, "offerCharacteristics", false);
                    if (offerCharacteristics != null)
                      {
                        JSONArray languageProperties = JSONUtilities.decodeJSONArray(offerCharacteristics, "languageProperties", false);
                        if (languageProperties != null)
                          {
                            // take 1st language. What if we have more than 1 ?
                            JSONObject languagePropertiesJSON = (JSONObject) languageProperties.get(0);
                            if (languagePropertiesJSON != null)
                              {
                                JSONArray properties = JSONUtilities.decodeJSONArray(languagePropertiesJSON, "properties", false);
                                if (properties != null)
                                  {
                                    for (int i=0; i<properties.size(); i++)
                                      {
                                        JSONObject propertiesJSON = (JSONObject) properties.get(i);
                                        if (propertiesJSON != null)
                                          {
                                            String catalogCharacteristicName = JSONUtilities.decodeString(propertiesJSON, "catalogCharacteristicName", false);
                                            if (catalogCharacteristicName != null)
                                              {
                                                Object value = propertiesJSON.get("value");
                                                offerPropertiesMap.put(catalogCharacteristicName, value);
                                              }
                                          }
                                      }
                                  }
                                if (!offerPropertiesMap.isEmpty())
                                  {
                                    JSONArray catalogCharacteristics = JSONUtilities.decodeJSONArray(callingChannelJSON, "catalogCharacteristics", false);
                                    if (catalogCharacteristics != null)
                                      {
                                        for (int i=0; i<catalogCharacteristics.size(); i++)
                                          {
                                            JSONObject catalogCharacteristicsJSON = (JSONObject) catalogCharacteristics.get(i);
                                            if (catalogCharacteristicsJSON != null)
                                              {
                                                boolean presentOffers = JSONUtilities.decodeBoolean(catalogCharacteristicsJSON, "presentOffers", Boolean.TRUE);
                                                if (presentOffers)
                                                  {
                                                    String catalogCharacteristicName = JSONUtilities.decodeString(catalogCharacteristicsJSON, "catalogCharacteristicName", false);
                                                    if (catalogCharacteristicName != null) 
                                                      {
                                                        Object offerProperty = offerPropertiesMap.get(catalogCharacteristicName);
                                                        offerMap.put(catalogCharacteristicName, offerProperty);
                                                      }
                                                  }
                                              }
                                          }
                                      }
                                  }
                              }
                          }
                      }
                  }
              }
          }
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
