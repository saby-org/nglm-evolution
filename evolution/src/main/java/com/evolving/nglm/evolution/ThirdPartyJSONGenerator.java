/*****************************************************************************
*
*  ThirdPartyJSONGenerator.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;

import com.evolving.nglm.evolution.OfferCallingChannel.OfferCallingChannelProperty;
import com.rii.utilities.JSONUtilities;

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
    offerMap.put("offerInitialPropensity", offer.getInitialPropensity());
    offerMap.put("offerUnitaryCost", offer.getUnitaryCost());
    List<JSONObject> offerCallingChannels = offer.getOfferCallingChannels().stream().map(offerCallingChannel -> ThirdPartyJSONGenerator.generateOfferCallingChannelJSONForThirdParty(offerCallingChannel)).collect(Collectors.toList());
    offerMap.put("offerCallingChannels", JSONUtilities.encodeArray(offerCallingChannels));
    offerMap.put("offerType", generateOfferTypeJSONForThirdParty(offer.getOfferType()));
    List<JSONObject> products = offer.getOfferProducts().stream().map(product -> ThirdPartyJSONGenerator.generateProductJSONForThirdParty(product)).collect(Collectors.toList());
    offerMap.put("products", products);
    return JSONUtilities.encodeObject(offerMap);
  }
  
  /*****************************************
  *
  *  generateOfferCallingChannelJSONForThirdParty
  *
  *****************************************/
  
  protected static JSONObject generateOfferCallingChannelJSONForThirdParty(OfferCallingChannel offerCallingChannel) 
  {
    HashMap<String, Object> offerCallingChannelMap = new HashMap<String, Object>();
    if ( null == offerCallingChannel ) return JSONUtilities.encodeObject(offerCallingChannelMap);
    offerCallingChannelMap.put("callingChannelID", offerCallingChannel.getCallingChannelID());
    List<JSONObject> offerCallingChannelProperties = offerCallingChannel.getOfferCallingChannelProperties().stream().map(offerCallingChannelProperty -> ThirdPartyJSONGenerator.generateOfferCallingChannelPropertyJSONForThirdParty(offerCallingChannelProperty)).collect(Collectors.toList());
    offerCallingChannelMap.put("offerCallingChannelProperties", JSONUtilities.encodeArray(offerCallingChannelProperties));
    return JSONUtilities.encodeObject(offerCallingChannelMap);
  }
  
  /*****************************************
  *
  *  generateOfferCallingChannelPropertyJSONForThirdParty
  *
  *****************************************/
  
  protected static JSONObject generateOfferCallingChannelPropertyJSONForThirdParty(OfferCallingChannelProperty offerCallingChannelProperty) 
  {
    HashMap<String, Object> offerCallingChannelPropertyMap = new HashMap<String, Object>();
    if ( null == offerCallingChannelProperty ) return JSONUtilities.encodeObject(offerCallingChannelPropertyMap);
    offerCallingChannelPropertyMap.put("property", generateCallingChannelPropertyJSONForThirdParty(offerCallingChannelProperty.getProperty()));
    offerCallingChannelPropertyMap.put("propertyValue", offerCallingChannelProperty.getPropertyValue());
    offerCallingChannelPropertyMap.put("textValue", offerCallingChannelProperty.getTextValues());
    return JSONUtilities.encodeObject(offerCallingChannelPropertyMap);
  }
  
  /*****************************************
  *
  *  generateCallingChannelPropertyJSONForThirdParty
  *
  *****************************************/
  
  protected static JSONObject generateCallingChannelPropertyJSONForThirdParty(CallingChannelProperty callingChannelProperty) 
  {
    HashMap<String, Object> callingChannelPropertyMap = new HashMap<String, Object>();
    if ( null == callingChannelProperty ) return JSONUtilities.encodeObject(callingChannelPropertyMap);
    callingChannelPropertyMap.put("display", callingChannelProperty.getDisplay());
    callingChannelPropertyMap.put("id", callingChannelProperty.getID());
    callingChannelPropertyMap.put("name", callingChannelProperty.getName());
    return JSONUtilities.encodeObject(callingChannelPropertyMap);
  }
  
  /*****************************************
  *
  *  generateOfferTypeJSONForThirdParty
  *
  *****************************************/
  
  protected static JSONObject generateOfferTypeJSONForThirdParty(OfferType offerType) 
  {
    HashMap<String, Object> offerTypeMap = new HashMap<String, Object>();
    if ( null == offerType ) return JSONUtilities.encodeObject(offerTypeMap);
    offerTypeMap.put("display", offerType.getDisplay());
    offerTypeMap.put("id", offerType.getID());
    offerTypeMap.put("name", offerType.getName());
    return JSONUtilities.encodeObject(offerTypeMap);
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

}
