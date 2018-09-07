/*****************************************************************************
*
*  Offer.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionContext;
import com.evolving.nglm.evolution.OfferPresentationChannel.OfferPresentationChannelProperty;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

public class Offer extends GUIManagedObject
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
    schemaBuilder.name("offer");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("defaultPropensity", Schema.INT32_SCHEMA);
    schemaBuilder.field("marginFactor", Schema.INT32_SCHEMA);
    schemaBuilder.field("cost", Schema.INT32_SCHEMA);
    schemaBuilder.field("price", Schema.INT32_SCHEMA);
    schemaBuilder.field("currency", Schema.STRING_SCHEMA);
    schemaBuilder.field("salesChannels", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("profileCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("offerType", Schema.STRING_SCHEMA);
    schemaBuilder.field("productType", Schema.STRING_SCHEMA);
    schemaBuilder.field("rewardType", Schema.STRING_SCHEMA);
    schemaBuilder.field("offerPresentationChannels", SchemaBuilder.array(OfferPresentationChannel.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Offer> serde = new ConnectSerde<Offer>(schema, false, Offer.class, Offer::pack, Offer::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Offer> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private int defaultPropensity;
  private int marginFactor;
  private int cost;
  private int price;
  private SupportedCurrency currency;
  private Set<SalesChannel> salesChannels; 
  private List<EvaluationCriterion> profileCriteria;
  private OfferType offerType;
  private ProductType productType;
  private RewardType rewardType;
  private Set<OfferPresentationChannel> offerPresentationChannels;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getOfferID() { return getGUIManagedObjectID(); }
  public int getDefaultPropensity() { return defaultPropensity; }
  public int getMarginFactor() { return marginFactor; }
  public int getCost() { return cost; }
  public int getPrice() { return price; }
  public SupportedCurrency getCurrency() { return currency; }
  public Set<SalesChannel> getSalesChannels() { return salesChannels;  }
  public List<EvaluationCriterion> getProfileCriteria() { return profileCriteria; }
  public OfferType getOfferType() { return offerType; }
  public ProductType getProductType() { return productType; }
  public RewardType getRewardType() { return rewardType; }
  public Set<OfferPresentationChannel> getOfferPresentationChannels() { return offerPresentationChannels; }

  /*****************************************
  *
  *  evaluateProfileCriteria
  *
  *****************************************/

  public boolean evaluateProfileCriteria(SubscriberEvaluationRequest evaluationRequest)
  {
    return EvaluationCriterion.evaluateCriteria(evaluationRequest, profileCriteria);
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Offer(SchemaAndValue schemaAndValue, int defaultPropensity, int marginFactor, int cost, int price, SupportedCurrency currency, Set<SalesChannel> salesChannels, List<EvaluationCriterion> profileCriteria, OfferType offerType, ProductType productType, RewardType rewardType, Set<OfferPresentationChannel> offerPresentationChannels)
  {
    super(schemaAndValue);
    this.defaultPropensity = defaultPropensity;
    this.marginFactor = marginFactor;
    this.cost = cost;
    this.price = price;
    this.currency = currency;
    this.salesChannels = salesChannels;
    this.profileCriteria = profileCriteria;
    this.offerType = offerType;
    this.productType = productType;
    this.rewardType = rewardType;
    this.offerPresentationChannels = offerPresentationChannels;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Offer offer = (Offer) value;
    Struct struct = new Struct(schema);
    packCommon(struct, offer);
    struct.put("defaultPropensity", offer.getDefaultPropensity());
    struct.put("marginFactor", offer.getMarginFactor());
    struct.put("cost", offer.getCost());
    struct.put("price", offer.getPrice());
    struct.put("currency", offer.getCurrency().getID());
    struct.put("salesChannels", packSalesChannels(offer.getSalesChannels()));
    struct.put("profileCriteria", packProfileCriteria(offer.getProfileCriteria()));
    struct.put("offerType", offer.getOfferType().getID());
    struct.put("productType", offer.getProductType().getID());
    struct.put("rewardType", offer.getRewardType().getID());
    struct.put("offerPresentationChannels", packOfferPresentationChannels(offer.getOfferPresentationChannels()));
    return struct;
  }

  /****************************************
  *
  *  packSalesChannels
  *
  ****************************************/

  private static List<Object> packSalesChannels(Set<SalesChannel> salesChannels)
  {
    List<Object> result = new ArrayList<Object>();
    for (SalesChannel salesChannel : salesChannels)
      {
        result.add(salesChannel.getID());
      }
    return result;
  }

  /****************************************
  *
  *  packProfileCriteria
  *
  ****************************************/

  private static List<Object> packProfileCriteria(List<EvaluationCriterion> profileCriteria)
  {
    List<Object> result = new ArrayList<Object>();
    for (EvaluationCriterion criterion : profileCriteria)
      {
        result.add(EvaluationCriterion.pack(criterion));
      }
    return result;
  }

  /****************************************
  *
  *  packOfferPresentationChannels
  *
  ****************************************/

  private static List<Object> packOfferPresentationChannels(Set<OfferPresentationChannel> offerPresentationChannels)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferPresentationChannel offerPresentationChannel : offerPresentationChannels)
      {
        result.add(OfferPresentationChannel.pack(offerPresentationChannel));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Offer unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    int defaultPropensity = valueStruct.getInt32("defaultPropensity");
    int marginFactor = valueStruct.getInt32("marginFactor");
    int cost = valueStruct.getInt32("cost");
    int price = valueStruct.getInt32("price");
    SupportedCurrency currency = Deployment.getSupportedCurrencies().get(valueStruct.getString("currency"));
    Set<SalesChannel> salesChannels = unpackSalesChannels((List<String>) valueStruct.get("salesChannels"));
    List<EvaluationCriterion> profileCriteria = unpackProfileCriteria(schema.field("profileCriteria").schema(), valueStruct.get("profileCriteria"));
    OfferType offerType = Deployment.getOfferTypes().get(valueStruct.getString("offerType"));
    ProductType productType = Deployment.getProductTypes().get(valueStruct.getString("productType"));
    RewardType rewardType = Deployment.getRewardTypes().get(valueStruct.getString("rewardType"));
    Set<OfferPresentationChannel> offerPresentationChannels = unpackOfferPresentationChannels(schema.field("offerPresentationChannels").schema(), valueStruct.get("offerPresentationChannels"));
    
    //
    //  validate
    //

    if (currency == null) throw new SerializationException("unknown currency: " + valueStruct.getString("currency"));
    if (offerType == null) throw new SerializationException("unknown offerType: " + valueStruct.getString("offerType"));
    if (productType == null) throw new SerializationException("unknown productType: " + valueStruct.getString("productType"));
    if (rewardType == null) throw new SerializationException("unknown rewardType: " + valueStruct.getString("rewardType"));
    
    //
    //  return
    //

    return new Offer(schemaAndValue, defaultPropensity, marginFactor, cost, price, currency, salesChannels, profileCriteria, offerType, productType, rewardType, offerPresentationChannels);
  }
  
  /*****************************************
  *
  *  unpackSalesChannels
  *
  *****************************************/

  private static Set<SalesChannel> unpackSalesChannels(List<String> salesChannelIDs)
  {
    Set<SalesChannel> salesChannels = new HashSet<SalesChannel>();
    for (String salesChannelID : salesChannelIDs)
      {
        SalesChannel salesChannel = Deployment.getSalesChannels().get(salesChannelID);
        if (salesChannel == null) throw new SerializationException("unknown salesChannel: " + salesChannelID);
        salesChannels.add(salesChannel);
      }
    return salesChannels;
  }

  /*****************************************
  *
  *  unpackProfileCriteria
  *
  *****************************************/

  private static List<EvaluationCriterion> unpackProfileCriteria(Schema schema, Object value)
  {
    //
    //  get schema for EvaluationCriterion
    //

    Schema evaluationCriterionSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    List<Object> valueArray = (List<Object>) value;
    for (Object criterion : valueArray)
      {
        result.add(EvaluationCriterion.unpack(new SchemaAndValue(evaluationCriterionSchema, criterion)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  unpackOfferPresentationChannels
  *
  *****************************************/

  private static Set<OfferPresentationChannel> unpackOfferPresentationChannels(Schema schema, Object value)
  {
    //
    //  get schema for OfferPresentationChannel
    //

    Schema offerPresentationChannelSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<OfferPresentationChannel> result = new HashSet<OfferPresentationChannel>();
    List<Object> valueArray = (List<Object>) value;
    for (Object offerPresentationChannel : valueArray)
      {
        result.add(OfferPresentationChannel.unpack(new SchemaAndValue(offerPresentationChannelSchema, offerPresentationChannel)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Offer(JSONObject jsonRoot, long epoch, GUIManagedObject existingOfferUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingOfferUnchecked != null) ? existingOfferUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingOffer
    *
    *****************************************/

    Offer existingOffer = (existingOfferUnchecked != null && existingOfferUnchecked instanceof Offer) ? (Offer) existingOfferUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    this.defaultPropensity = JSONUtilities.decodeInteger(jsonRoot, "defaultPropensity", true);
    this.marginFactor = JSONUtilities.decodeInteger(jsonRoot, "marginFactor", true);
    this.cost = JSONUtilities.decodeInteger(jsonRoot, "cost", true);
    this.price = JSONUtilities.decodeInteger(jsonRoot, "price", true);
    this.currency = Deployment.getSupportedCurrencies().get(JSONUtilities.decodeString(jsonRoot, "currencyID", true));
    this.salesChannels = decodeSalesChannels(JSONUtilities.decodeJSONArray(jsonRoot, "salesChannelIDs", true));
    this.profileCriteria = decodeProfileCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", true));
    this.offerType = Deployment.getOfferTypes().get(JSONUtilities.decodeString(jsonRoot, "offerTypeID", true));
    this.productType = Deployment.getProductTypes().get(JSONUtilities.decodeString(jsonRoot, "productTypeID", true));
    this.rewardType = Deployment.getRewardTypes().get(JSONUtilities.decodeString(jsonRoot, "rewardTypeID", true));
    this.offerPresentationChannels = decodeOfferPresentationChannels(JSONUtilities.decodeJSONArray(jsonRoot, "presentationChannels", false));

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (this.currency == null) throw new GUIManagerException("unsupported currency", JSONUtilities.decodeString(jsonRoot, "currencyID", true));
    if (this.offerType == null) throw new GUIManagerException("unsupported offerType", JSONUtilities.decodeString(jsonRoot, "offerTypeID", true));
    if (this.productType == null) throw new GUIManagerException("unsupported productType", JSONUtilities.decodeString(jsonRoot, "productTypeID", true));
    if (this.rewardType == null) throw new GUIManagerException("unsupported rewardType", JSONUtilities.decodeString(jsonRoot, "rewardTypeID", true));

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingOffer))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodeSalesChannels
  *
  *****************************************/

  private Set<SalesChannel> decodeSalesChannels(JSONArray jsonArray) throws GUIManagerException
  {
    Set<SalesChannel> salesChannels = new HashSet<SalesChannel>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        String salesChannelID = (String) jsonArray.get(i);
        SalesChannel salesChannel = Deployment.getSalesChannels().get(salesChannelID);
        if (salesChannel == null) throw new GUIManagerException("unknown salesChannel", salesChannelID);
        salesChannels.add(salesChannel);
      }
    return salesChannels;
  }

  /*****************************************
  *
  *  decodeProfileCriteria
  *
  *****************************************/

  private List<EvaluationCriterion> decodeProfileCriteria(JSONArray jsonArray) throws GUIManagerException
  {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.Profile));
      }
    return result;
  }

  /*****************************************
  *
  *  decodeOfferPresentationChannels
  *
  *****************************************/

  private Set<OfferPresentationChannel> decodeOfferPresentationChannels(JSONArray jsonArray) throws GUIManagerException
  {
    Set<OfferPresentationChannel> result = new HashSet<OfferPresentationChannel>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new OfferPresentationChannel((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Offer existingOffer)
  {
    if (existingOffer != null && existingOffer.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingOffer.getGUIManagedObjectID());
        epochChanged = epochChanged || ! (defaultPropensity == existingOffer.getDefaultPropensity());
        epochChanged = epochChanged || ! (marginFactor == existingOffer.getMarginFactor());
        epochChanged = epochChanged || ! (cost == existingOffer.getCost());
        epochChanged = epochChanged || ! (price == existingOffer.getPrice());
        epochChanged = epochChanged || ! Objects.equals(currency, existingOffer.getCurrency());
        epochChanged = epochChanged || ! Objects.equals(salesChannels, existingOffer.getSalesChannels());
        epochChanged = epochChanged || ! Objects.equals(profileCriteria, existingOffer.getProfileCriteria());
        epochChanged = epochChanged || ! Objects.equals(offerType, existingOffer.getOfferType());
        epochChanged = epochChanged || ! Objects.equals(productType, existingOffer.getProductType());
        epochChanged = epochChanged || ! Objects.equals(rewardType, existingOffer.getRewardType());
        epochChanged = epochChanged || ! Objects.equals(offerPresentationChannels, existingOffer.getOfferPresentationChannels());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

  /*****************************************
  *
  *  validatePresentationChannels
  *
  *****************************************/

  public void validatePresentationChannels(PresentationChannelService presentationChannelService, Date date) throws GUIManagerException
  {
    for (OfferPresentationChannel offerPresentationChannel : offerPresentationChannels)
      {
        /*****************************************
        *
        *  retrieve presentationChannel
        *
        *****************************************/

        PresentationChannel presentationChannel = presentationChannelService.getActivePresentationChannel(offerPresentationChannel.getPresentationChannelID(), date);

        /*****************************************
        *
        *  validate the presentationChannel exists and is active
        *
        *****************************************/

        if (presentationChannel == null) throw new GUIManagerException("unknown presentation channel", offerPresentationChannel.getPresentationChannelID());

        /*****************************************
        *
        *  validate the properties
        *
        *****************************************/

        //
        //  set of properties defined for this offer
        //

        Set<PresentationChannelProperty> offerProperties = new HashSet<PresentationChannelProperty>();
        for (OfferPresentationChannelProperty offerPresentationChannelProperty : offerPresentationChannel.getOfferPresentationChannelProperties())
          {
            offerProperties.add(offerPresentationChannelProperty.getProperty());
          }

        //
        //  validate mandatory properties
        //

        if (! offerProperties.containsAll(presentationChannel.getMandatoryPresentationChannelProperties())) throw new GUIManagerException("missing required presentation channel properties", presentationChannel.getGUIManagedObjectID());

        //
        //  validate optional properties
        //

        offerProperties.removeAll(presentationChannel.getMandatoryPresentationChannelProperties());
        offerProperties.removeAll(presentationChannel.getOptionalPresentationChannelProperties());
        if (offerProperties.size() > 0) throw new GUIManagerException("unknown presentation channel properties", presentationChannel.getGUIManagedObjectID());
      }
  }
}
