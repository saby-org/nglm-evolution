/*****************************************************************************
*
*  Offer.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionContext;
import com.evolving.nglm.evolution.OfferCallingChannel.OfferCallingChannelProperty;
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
    schemaBuilder.field("initialPropensity", Schema.INT32_SCHEMA);
    schemaBuilder.field("unitaryCost", Schema.INT32_SCHEMA);
    schemaBuilder.field("profileCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("offerType", Schema.STRING_SCHEMA);
    schemaBuilder.field("offerCatalogObjectives", SchemaBuilder.array(OfferCatalogObjective.schema()).schema());
    schemaBuilder.field("offerProducts", SchemaBuilder.array(OfferProduct.schema()).schema());
    schemaBuilder.field("offerCallingChannels", SchemaBuilder.array(OfferCallingChannel.schema()).schema());
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

  private int initialPropensity;
  private int unitaryCost;
  private List<EvaluationCriterion> profileCriteria;
  private OfferType offerType;
  private Set<OfferCatalogObjective> offerCatalogObjectives; 
  private Set<OfferProduct> offerProducts;
  private Set<OfferCallingChannel> offerCallingChannels;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getOfferID() { return getGUIManagedObjectID(); }
  public int getInitialPropensity() { return initialPropensity; }
  public int getUnitaryCost() { return unitaryCost; }
  public List<EvaluationCriterion> getProfileCriteria() { return profileCriteria; }
  public OfferType getOfferType() { return offerType; }
  public Set<OfferCatalogObjective> getOfferCatalogObjectives() { return offerCatalogObjectives;  }
  public Set<OfferProduct> getOfferProducts() { return offerProducts; }
  public Set<OfferCallingChannel> getOfferCallingChannels() { return offerCallingChannels; }

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

  public Offer(SchemaAndValue schemaAndValue, int initialPropensity, int unitaryCost, List<EvaluationCriterion> profileCriteria, OfferType offerType, Set<OfferCatalogObjective> offerCatalogObjectives, Set<OfferProduct> offerProducts, Set<OfferCallingChannel> offerCallingChannels)
  {
    super(schemaAndValue);
    this.initialPropensity = initialPropensity;
    this.unitaryCost = unitaryCost;
    this.profileCriteria = profileCriteria;
    this.offerType = offerType;
    this.offerCatalogObjectives = offerCatalogObjectives;
    this.offerProducts = offerProducts;
    this.offerCallingChannels = offerCallingChannels;
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
    struct.put("unitaryCost", offer.getUnitaryCost());
    struct.put("profileCriteria", packProfileCriteria(offer.getProfileCriteria()));
    struct.put("offerType", offer.getOfferType().getID());
    struct.put("offerCatalogObjectives", packOfferCatalogObjectives(offer.getOfferCatalogObjectives()));
    struct.put("offerProducts", packOfferProducts(offer.getOfferProducts()));
    struct.put("offerCallingChannels", packOfferCallingChannels(offer.getOfferCallingChannels()));
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
  *  packOfferCatalogObjectives
  *
  ****************************************/

  private static List<Object> packOfferCatalogObjectives(Set<OfferCatalogObjective> offerCatalogObjectives)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferCatalogObjective offerCatalogObjective : offerCatalogObjectives)
      {
        result.add(OfferCatalogObjective.pack(offerCatalogObjective));
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
  *  packOfferCallingChannels
  *
  ****************************************/

  private static List<Object> packOfferCallingChannels(Set<OfferCallingChannel> offerCallingChannels)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferCallingChannel offerCallingChannel : offerCallingChannels)
      {
        result.add(OfferCallingChannel.pack(offerCallingChannel));
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
    int initialPropensity = valueStruct.getInt32("initialPropensity");
    int unitaryCost = valueStruct.getInt32("unitaryCost");
    List<EvaluationCriterion> profileCriteria = unpackProfileCriteria(schema.field("profileCriteria").schema(), valueStruct.get("profileCriteria"));
    OfferType offerType = Deployment.getOfferTypes().get(valueStruct.getString("offerType"));
    Set<OfferCatalogObjective> offerCatalogObjectives = unpackOfferCatalogObjectives(schema.field("offerCatalogObjectives").schema(), valueStruct.get("offerCatalogObjectives"));
    Set<OfferProduct> offerProducts = unpackOfferProducts(schema.field("offerProducts").schema(), valueStruct.get("offerProducts"));
    Set<OfferCallingChannel> offerCallingChannels = unpackOfferCallingChannels(schema.field("offerCallingChannels").schema(), valueStruct.get("offerCallingChannels"));
    
    //
    //  validate
    //

    if (offerType == null) throw new SerializationException("unknown offerType: " + valueStruct.getString("offerType"));
    
    //
    //  return
    //

    return new Offer(schemaAndValue, initialPropensity, unitaryCost, profileCriteria, offerType, offerCatalogObjectives, offerProducts, offerCallingChannels);
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
  *  unpackOfferCatalogObjectives
  *
  *****************************************/

  private static Set<OfferCatalogObjective> unpackOfferCatalogObjectives(Schema schema, Object value)
  {
    //
    //  get schema for OfferCatalogObjective
    //

    Schema offerCatalogObjectiveSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<OfferCatalogObjective> result = new HashSet<OfferCatalogObjective>();
    List<Object> valueArray = (List<Object>) value;
    for (Object offerCatalogObjective : valueArray)
      {
        result.add(OfferCatalogObjective.unpack(new SchemaAndValue(offerCatalogObjectiveSchema, offerCatalogObjective)));
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
  *  unpackOfferCallingChannels
  *
  *****************************************/

  private static Set<OfferCallingChannel> unpackOfferCallingChannels(Schema schema, Object value)
  {
    //
    //  get schema for OfferCallingChannel
    //

    Schema offerCallingChannelSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<OfferCallingChannel> result = new HashSet<OfferCallingChannel>();
    List<Object> valueArray = (List<Object>) value;
    for (Object offerCallingChannel : valueArray)
      {
        result.add(OfferCallingChannel.unpack(new SchemaAndValue(offerCallingChannelSchema, offerCallingChannel)));
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
    
    this.initialPropensity = JSONUtilities.decodeInteger(jsonRoot, "initialPropensity", true);
    this.unitaryCost = JSONUtilities.decodeInteger(jsonRoot, "unitaryCost", true);
    this.profileCriteria = decodeProfileCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", true));
    this.offerType = Deployment.getOfferTypes().get(JSONUtilities.decodeString(jsonRoot, "offerTypeID", true));
    this.offerCatalogObjectives = decodeOfferCatalogObjectives(JSONUtilities.decodeJSONArray(jsonRoot, "catalogObjectives", true), catalogCharacteristicService);
    this.offerProducts = decodeOfferProducts(JSONUtilities.decodeJSONArray(jsonRoot, "products", false));
    this.offerCallingChannels = decodeOfferCallingChannels(JSONUtilities.decodeJSONArray(jsonRoot, "callingChannels", false));

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (this.offerType == null) throw new GUIManagerException("unsupported offerType", JSONUtilities.decodeString(jsonRoot, "offerTypeID", true));

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
  *  decodeOfferCatalogObjectives
  *
  *****************************************/

  private Set<OfferCatalogObjective> decodeOfferCatalogObjectives(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    Set<OfferCatalogObjective> result = new HashSet<OfferCatalogObjective>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new OfferCatalogObjective((JSONObject) jsonArray.get(i)));
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
  *  decodeOfferCallingChannels
  *
  *****************************************/

  private Set<OfferCallingChannel> decodeOfferCallingChannels(JSONArray jsonArray) throws GUIManagerException
  {
    Set<OfferCallingChannel> result = new HashSet<OfferCallingChannel>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new OfferCallingChannel((JSONObject) jsonArray.get(i)));
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
        epochChanged = epochChanged || ! (unitaryCost == existingOffer.getUnitaryCost());
        epochChanged = epochChanged || ! Objects.equals(profileCriteria, existingOffer.getProfileCriteria());
        epochChanged = epochChanged || ! Objects.equals(offerType, existingOffer.getOfferType());
        epochChanged = epochChanged || ! Objects.equals(offerCatalogObjectives, existingOffer.getOfferCatalogObjectives());
        epochChanged = epochChanged || ! Objects.equals(offerProducts, existingOffer.getOfferProducts());
        epochChanged = epochChanged || ! Objects.equals(offerCallingChannels, existingOffer.getOfferCallingChannels());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

  /*****************************************
  *
  *  validateCallingChannels
  *
  *****************************************/

  public void validateCallingChannels(CallingChannelService callingChannelService, Date date) throws GUIManagerException
  {
    for (OfferCallingChannel offerCallingChannel : offerCallingChannels)
      {
        /*****************************************
        *
        *  retrieve callingChannel
        *
        *****************************************/

        CallingChannel callingChannel = callingChannelService.getActiveCallingChannel(offerCallingChannel.getCallingChannelID(), date);

        /*****************************************
        *
        *  validate the callingChannel exists and is active
        *
        *****************************************/

        if (callingChannel == null) throw new GUIManagerException("unknown calling channel", offerCallingChannel.getCallingChannelID());

        /*****************************************
        *
        *  validate the properties
        *
        *****************************************/

        //
        //  set of properties defined for this offer
        //

        Set<CallingChannelProperty> offerProperties = new HashSet<CallingChannelProperty>();
        for (OfferCallingChannelProperty offerCallingChannelProperty : offerCallingChannel.getOfferCallingChannelProperties())
          {
            offerProperties.add(offerCallingChannelProperty.getProperty());
          }

        //
        //  validate mandatory properties
        //

        if (! offerProperties.containsAll(callingChannel.getMandatoryCallingChannelProperties())) throw new GUIManagerException("missing required calling channel properties", callingChannel.getGUIManagedObjectID());

        //
        //  validate optional properties
        //

        offerProperties.removeAll(callingChannel.getMandatoryCallingChannelProperties());
        offerProperties.removeAll(callingChannel.getOptionalCallingChannelProperties());
        if (offerProperties.size() > 0) throw new GUIManagerException("unknown calling channel properties", callingChannel.getGUIManagedObjectID());
      }
  }
  
  /*****************************************
  *
  *  validateProducts
  *
  *****************************************/

  public void validateProducts(ProductService productService, Date date) throws GUIManagerException
  {
    for (OfferProduct offerProduct : offerProducts)
      {
        /*****************************************
        *
        *  retrieve product
        *
        *****************************************/

        Product product = productService.getActiveProduct(offerProduct.getProductID(), date);

        /*****************************************
        *
        *  validate the product exists and is active
        *
        *****************************************/

        if (product == null) throw new GUIManagerException("unknown product", offerProduct.getProductID());
      }
  }
}
