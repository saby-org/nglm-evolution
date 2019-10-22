/*****************************************************************************
*
*  Offer.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.StockMonitor.StockableItem;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.StockMonitor.StockableItem;

public class Offer extends GUIManagedObject implements StockableItem
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
    schemaBuilder.field("initialPropensity", Schema.FLOAT64_SCHEMA);
    schemaBuilder.field("stock", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("unitaryCost", Schema.INT32_SCHEMA);
    schemaBuilder.field("profileCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("offerOfferObjectives", SchemaBuilder.array(OfferObjectiveInstance.schema()).schema());
    schemaBuilder.field("offerSalesChannelsAndPrices", SchemaBuilder.array(OfferSalesChannelsAndPrice.schema()).schema());
    schemaBuilder.field("offerProducts", SchemaBuilder.array(OfferProduct.schema()).schema());
    schemaBuilder.field("offerTranslations", SchemaBuilder.array(OfferTranslation.schema()).schema());
    schemaBuilder.field("offerCharacteristics", OfferCharacteristics.schema());
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

  private double initialPropensity;
  private Integer stock;
  private int unitaryCost;
  private List<EvaluationCriterion> profileCriteria;
  private Set<OfferObjectiveInstance> offerOfferObjectives; 
  private Set<OfferSalesChannelsAndPrice> offerSalesChannelsAndPrices;
  private Set<OfferProduct> offerProducts;
  private Set<OfferTranslation> offerTranslations;
  private OfferCharacteristics offerCharacteristics;
  private String description;

  //
  //  derived
  //

  private String stockableItemID;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getOfferID() { return getGUIManagedObjectID(); }
  public String getDisplay() { return getGUIManagedObjectDisplay(); }
  public double getInitialPropensity() { return initialPropensity; }
  public Integer getStock() { return stock; } 
  public int getUnitaryCost() { return unitaryCost; }
  public List<EvaluationCriterion> getProfileCriteria() { return profileCriteria; }
  public Set<OfferObjectiveInstance> getOfferObjectives() { return offerOfferObjectives;  }
  public Set<OfferSalesChannelsAndPrice> getOfferSalesChannelsAndPrices() { return offerSalesChannelsAndPrices;  }
  public Set<OfferProduct> getOfferProducts() { return offerProducts; }
  public Set<OfferTranslation> getOfferTranslations() { return offerTranslations; }
  public OfferCharacteristics getOfferCharacteristics() { return offerCharacteristics; }
  public String getStockableItemID() { return stockableItemID; }
  public String getDescription() { return JSONUtilities.decodeString(getJSONRepresentation(), "description"); }

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

  public Offer(SchemaAndValue schemaAndValue, double initialPropensity, Integer stock, int unitaryCost, List<EvaluationCriterion> profileCriteria, Set<OfferObjectiveInstance> offerObjectives, Set<OfferSalesChannelsAndPrice> offerSalesChannelsAndPrices, Set<OfferProduct> offerProducts, OfferCharacteristics offerCharacteristics, Set<OfferTranslation> offerTranslations)
  {
    super(schemaAndValue);
    this.initialPropensity = initialPropensity;
    this.stock = stock;
    this.unitaryCost = unitaryCost;
    this.profileCriteria = profileCriteria;
    this.offerOfferObjectives = offerObjectives;
    this.offerSalesChannelsAndPrices = offerSalesChannelsAndPrices;
    this.offerProducts = offerProducts;
    this.offerTranslations = offerTranslations;
    this.stockableItemID = "offer-" + getOfferID();
    this.offerCharacteristics = offerCharacteristics;
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
    struct.put("initialPropensity", offer.getInitialPropensity());
    struct.put("stock", offer.getStock());
    struct.put("unitaryCost", offer.getUnitaryCost());
    struct.put("profileCriteria", packProfileCriteria(offer.getProfileCriteria()));
    struct.put("offerOfferObjectives", packOfferObjectives(offer.getOfferObjectives()));
    struct.put("offerSalesChannelsAndPrices", packOfferSalesChannelsAndPrices(offer.getOfferSalesChannelsAndPrices()));
    struct.put("offerProducts", packOfferProducts(offer.getOfferProducts()));
    struct.put("offerTranslations", packOfferTranslations(offer.getOfferTranslations()));
    struct.put("offerCharacteristics", OfferCharacteristics.pack(offer.getOfferCharacteristics()));
    return struct;
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
  *  packOfferObjectives
  *
  ****************************************/

  private static List<Object> packOfferObjectives(Set<OfferObjectiveInstance> offerObjectives)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferObjectiveInstance offerObjective : offerObjectives)
      {
        result.add(OfferObjectiveInstance.pack(offerObjective));
      }
    return result;
  }
  
  /****************************************
  *
  *  packOfferSalesChannelsAndPrices
  *
  ****************************************/

  private static List<Object> packOfferSalesChannelsAndPrices(Set<OfferSalesChannelsAndPrice> offerSalesChannelsAndPrices)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferSalesChannelsAndPrice offerSalesChannelsAndPrice : offerSalesChannelsAndPrices)
      {
        result.add(OfferSalesChannelsAndPrice.pack(offerSalesChannelsAndPrice));
      }
    return result;
  }

  /****************************************
  *
  *  packOfferProducts
  *
  ****************************************/

  private static List<Object> packOfferProducts(Set<OfferProduct> offerProducts)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferProduct offerProduct : offerProducts)
      {
        result.add(OfferProduct.pack(offerProduct));
      }
    return result;
  }

  /****************************************
  *
  *  packOfferTranslations
  *
  ****************************************/

  private static List<Object> packOfferTranslations(Set<OfferTranslation> offerTranslations)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferTranslation offerTranslation : offerTranslations)
      {
        result.add(OfferTranslation.pack(offerTranslation));
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    double initialPropensity = valueStruct.getFloat64("initialPropensity");
    Integer stock = valueStruct.getInt32("stock");
    int unitaryCost = valueStruct.getInt32("unitaryCost");
    List<EvaluationCriterion> profileCriteria = unpackProfileCriteria(schema.field("profileCriteria").schema(), valueStruct.get("profileCriteria"));
    Set<OfferObjectiveInstance> offerObjectives = unpackOfferObjectives(schema.field("offerOfferObjectives").schema(), valueStruct.get("offerOfferObjectives"));
    Set<OfferSalesChannelsAndPrice> offerSalesChannelsAndPrices = unpackOfferSalesChannelsAndPrices(schema.field("offerSalesChannelsAndPrices").schema(), valueStruct.get("offerSalesChannelsAndPrices"));
    Set<OfferProduct> offerProducts = unpackOfferProducts(schema.field("offerProducts").schema(), valueStruct.get("offerProducts"));
    Set<OfferTranslation> offerTranslations = unpackOfferTranslations(schema.field("offerTranslations").schema(), valueStruct.get("offerTranslations"));
    OfferCharacteristics offerCharacteristics = OfferCharacteristics.unpack(new SchemaAndValue(schema.field("offerCharacteristics").schema(), valueStruct.get("offerCharacteristics")));
    
    //
    //  return
    //

    return new Offer(schemaAndValue, initialPropensity, stock, unitaryCost, profileCriteria, offerObjectives, offerSalesChannelsAndPrices, offerProducts, offerCharacteristics, offerTranslations);
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
  *  unpackOfferObjectives
  *
  *****************************************/

  private static Set<OfferObjectiveInstance> unpackOfferObjectives(Schema schema, Object value)
  {
    //
    //  get schema for OfferObjective
    //

    Schema offerObjectiveSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<OfferObjectiveInstance> result = new HashSet<OfferObjectiveInstance>();
    List<Object> valueArray = (List<Object>) value;
    for (Object offerObjective : valueArray)
      {
        result.add(OfferObjectiveInstance.unpack(new SchemaAndValue(offerObjectiveSchema, offerObjective)));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  unpackOfferSalesChannelsAndPrices
  *
  *****************************************/

  private static Set<OfferSalesChannelsAndPrice> unpackOfferSalesChannelsAndPrices(Schema schema, Object value)
  {
    //
    //  get schema for OfferSalesChannelsAndPrice
    //

    Schema offerSalesChannelsAndPricesSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<OfferSalesChannelsAndPrice> result = new HashSet<OfferSalesChannelsAndPrice>();
    List<Object> valueArray = (List<Object>) value;
    for (Object offerSalesChannelsAndPrices : valueArray)
      {
        result.add(OfferSalesChannelsAndPrice.unpack(new SchemaAndValue(offerSalesChannelsAndPricesSchema, offerSalesChannelsAndPrices)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  unpackOfferProducts
  *
  *****************************************/

  private static Set<OfferProduct> unpackOfferProducts(Schema schema, Object value)
  {
    //
    //  get schema for OfferProduct
    //

    Schema offerProductSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<OfferProduct> result = new HashSet<OfferProduct>();
    List<Object> valueArray = (List<Object>) value;
    for (Object offerProduct : valueArray)
      {
        result.add(OfferProduct.unpack(new SchemaAndValue(offerProductSchema, offerProduct)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  unpackOfferTranslations
  *
  *****************************************/

  private static Set<OfferTranslation> unpackOfferTranslations(Schema schema, Object value)
  {
    //
    //  get schema for OfferTranslation
    //

    Schema offerTranslationSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<OfferTranslation> result = new HashSet<OfferTranslation>();
    List<Object> valueArray = (List<Object>) value;
    for (Object offerTranslation : valueArray)
      {
        result.add(OfferTranslation.unpack(new SchemaAndValue(offerTranslationSchema, offerTranslation)));
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

  public Offer(JSONObject jsonRoot, long epoch, GUIManagedObject existingOfferUnchecked, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
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
    
    this.initialPropensity = JSONUtilities.decodeDouble(jsonRoot, "initialPropensity", true);
    this.stock = JSONUtilities.decodeInteger(jsonRoot, "presentationStock", false);
    this.unitaryCost = JSONUtilities.decodeInteger(jsonRoot, "unitaryCost", true);
    this.profileCriteria = decodeProfileCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", true));
    this.offerOfferObjectives = decodeOfferObjectives(JSONUtilities.decodeJSONArray(jsonRoot, "offerObjectives", true), catalogCharacteristicService);
    this.offerSalesChannelsAndPrices = decodeOfferSalesChannelsAndPrices(JSONUtilities.decodeJSONArray(jsonRoot, "salesChannelsAndPrices", true));
    this.offerProducts = decodeOfferProducts(JSONUtilities.decodeJSONArray(jsonRoot, "products", false));
    this.offerTranslations = decodeOfferTranslations(JSONUtilities.decodeJSONArray(jsonRoot, "offerTranslations", false));
    this.stockableItemID = "offer-" + getOfferID();
    this.offerCharacteristics = new OfferCharacteristics(JSONUtilities.decodeJSONObject(jsonRoot, "offerCharacteristics", false), catalogCharacteristicService);

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
  *  decodeOfferObjectives
  *
  *****************************************/

  private Set<OfferObjectiveInstance> decodeOfferObjectives(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    Set<OfferObjectiveInstance> result = new HashSet<OfferObjectiveInstance>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new OfferObjectiveInstance((JSONObject) jsonArray.get(i), catalogCharacteristicService));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  decodeOfferSalesChannelsAndPrices
  *
  *****************************************/

  private Set<OfferSalesChannelsAndPrice> decodeOfferSalesChannelsAndPrices(JSONArray jsonArray) throws GUIManagerException
  {
    Set<OfferSalesChannelsAndPrice> result = new HashSet<OfferSalesChannelsAndPrice>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new OfferSalesChannelsAndPrice((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  decodeOfferProducts
  *
  *****************************************/

  private Set<OfferProduct> decodeOfferProducts(JSONArray jsonArray) throws GUIManagerException
  {
    Set<OfferProduct> result = new HashSet<OfferProduct>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new OfferProduct((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  decodeOfferTranslations
  *
  *****************************************/

  private Set<OfferTranslation> decodeOfferTranslations(JSONArray jsonArray) throws GUIManagerException
  {
    Set<OfferTranslation> result = new HashSet<OfferTranslation>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new OfferTranslation((JSONObject) jsonArray.get(i)));
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
        epochChanged = epochChanged || ! (initialPropensity == existingOffer.getInitialPropensity());
        epochChanged = epochChanged || ! Objects.equals(stock, existingOffer.getStock());
        epochChanged = epochChanged || ! (unitaryCost == existingOffer.getUnitaryCost());
        epochChanged = epochChanged || ! Objects.equals(profileCriteria, existingOffer.getProfileCriteria());
        epochChanged = epochChanged || ! Objects.equals(offerOfferObjectives, existingOffer.getOfferObjectives());
        epochChanged = epochChanged || ! Objects.equals(offerSalesChannelsAndPrices, existingOffer.getOfferSalesChannelsAndPrices());
        epochChanged = epochChanged || ! Objects.equals(offerProducts, existingOffer.getOfferProducts());
        epochChanged = epochChanged || ! Objects.equals(offerTranslations, existingOffer.getOfferTranslations());
        epochChanged = epochChanged || ! Objects.equals(offerCharacteristics, existingOffer.getOfferCharacteristics());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(CallingChannelService callingChannelService, SalesChannelService salesChannelService, ProductService productService, Date date) throws GUIManagerException
  {
    // TODO validate offerCharacteristics
    
    /****************************************
    *
    *  ensure active sales channel
    *
    ****************************************/

    Set<OfferSalesChannelsAndPrice> validOfferSalesChannelsAndPrices = new HashSet<OfferSalesChannelsAndPrice>();
    for (OfferSalesChannelsAndPrice offerSalesChannelsAndPrice : offerSalesChannelsAndPrices)
      {
        for (String salesChannelID : offerSalesChannelsAndPrice.getSalesChannelIDs())
          {
            //
            //  retrieve salesChannel
            //

            SalesChannel salesChannel = salesChannelService.getActiveSalesChannel(salesChannelID, date);

            //
            //  validate the salesChannel exists and is active
            //

            if (salesChannel == null)
              {
                log.info("offer {} uses unknown sales channel: {}", getOfferID(), salesChannelID);
                continue;
              }

            //
            //  valid salesChannelAndPrice
            //

            validOfferSalesChannelsAndPrices.add(offerSalesChannelsAndPrice);
          }
      }

    /*****************************************
    *
    *  ensure at least one valid sales channel
    *
    *****************************************/

    if (validOfferSalesChannelsAndPrices.size() == 0)
      {
        throw new GUIManagerException("no valid sales channels", getOfferID());
      }
    
    /****************************************
    *
    *  ensure valid/active products
    *
    ****************************************/
    
    for (OfferProduct offerProduct : offerProducts)
      {
        //
        //  retrieve product
        //

        GUIManagedObject product = productService.getStoredProduct(offerProduct.getProductID());

        //
        //  validate the product exists
        //

        if (product == null) throw new GUIManagerException("unknown product", offerProduct.getProductID());

        //
        //  validate the product start/end dates include the entire offer active period
        //

        if (! productService.isActiveProductThroughInterval(product, this.getEffectiveStartDate(), this.getEffectiveEndDate())) throw new GUIManagerException("invalid product (start/end dates)", offerProduct.getProductID());
      }
  }
  
  /*****************************************
  *
  *  toString
  *
  *****************************************/
  @Override
  public String toString()
    {
      return "Offer [initialPropensity=" + initialPropensity + ", "
          + (getGUIManagedObjectID() != null ? "getGUIManagedObjectID()=" + getGUIManagedObjectID() : "") + "]";
    }
}
