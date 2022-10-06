/*****************************************************************************
*
*  Offer.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionOperator;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.Expression.ConstantExpression;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.OfferCharacteristics.OfferCharacteristicsLanguageProperty;
import com.evolving.nglm.evolution.OfferCharacteristics.OfferCharacteristicsProperty;
import com.evolving.nglm.evolution.StockMonitor.StockableItem;

@GUIDependencyDef(objectType = "offer", serviceClass = OfferService.class, dependencies = { "badge", "offer", "product" , "voucher", "saleschannel" , "callingchannel", "offerobjective", "target", "catalogcharacteristic", "loyaltyProgramPoints", "loyaltyprogramchallenge", "loyaltyprogrammission", "point" })
public class Offer extends GUIManagedObject implements StockableItem
{  
  //
  //  logger
  //
  private static final Logger log = LoggerFactory.getLogger(Offer.class);

  public static final int UNSET = -123;
  
  //
  //  initial propensity
  //
  public static double getValidPropensity(Double p) {
    if(p == null) {
      // Default value is 0.50
      return 0.50d;
    } else if(p >= 0.0d && p <= 1.0d) {
      return p;
    } else {
      log.error("Trying to set invalid initial propensity (" + p + "). Will be set to the default value (0.50).");
      return 0.50d;
    }
  }
  
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),5));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("initialPropensity", Schema.FLOAT64_SCHEMA);
    schemaBuilder.field("stock", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("unitaryCost", Schema.INT32_SCHEMA);
    schemaBuilder.field("profileCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schemaBuilder.field("offerOfferObjectives", SchemaBuilder.array(OfferObjectiveInstance.schema()).schema());
    schemaBuilder.field("offerSalesChannelsAndPrices", SchemaBuilder.array(OfferSalesChannelsAndPrice.schema()).schema());
    schemaBuilder.field("offerProducts", SchemaBuilder.array(OfferProduct.schema()).optional().schema());
    schemaBuilder.field("offerVouchers", SchemaBuilder.array(OfferVoucher.schema()).optional().schema());
    schemaBuilder.field("offerTranslations", SchemaBuilder.array(OfferTranslation.schema()).schema());
    schemaBuilder.field("offerCharacteristics", OfferCharacteristics.schema());
    schemaBuilder.field("simpleOffer", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    schemaBuilder.field("maximumAcceptances", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("maximumAcceptancesPeriodDays", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("maximumAcceptancesPeriodMonths", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("stockRecurrence", Schema.OPTIONAL_BOOLEAN_SCHEMA);    //v5
    schemaBuilder.field("stockRecurrenceBatch", Schema.OPTIONAL_INT32_SCHEMA); //v5
    schemaBuilder.field("stockScheduler", JourneyScheduler.serde().optionalSchema()); //v5
    schemaBuilder.field("lastStockRecurrenceDate", Timestamp.builder().optional().schema()); //v5
    schemaBuilder.field("stockAlertThreshold", Schema.OPTIONAL_INT32_SCHEMA);  //v5
    schemaBuilder.field("stockAlert", Schema.BOOLEAN_SCHEMA);  //v5
    schemaBuilder.field("notificationEmails", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(new ArrayList<String>()).schema());
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
  private Set<OfferVoucher> offerVouchers;
  private Set<OfferTranslation> offerTranslations;
  private OfferCharacteristics offerCharacteristics;
  private String description;
  private boolean simpleOffer;
  private Integer maximumAcceptances;
  private Integer maximumAcceptancesPeriodDays;
  private Integer maximumAcceptancesPeriodMonths;
  private String notEligibilityReason;
  private String limitsReachedReason;
  private boolean stockRecurrence;
  private Integer stockRecurrenceBatch;
  private Integer stockAlertThreshold;
  private boolean stockAlert;
  private List<String> notificationEmails;
  private JourneyScheduler stockScheduler;
  private Date lastStockRecurrenceDate;
  
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
  public Set<OfferVoucher> getOfferVouchers() { return offerVouchers; }
  public Set<OfferTranslation> getOfferTranslations() { return offerTranslations; }
  public OfferCharacteristics getOfferCharacteristics() { return offerCharacteristics; }
  public String getStockableItemID() { return stockableItemID; }
  public String getDescription() { return JSONUtilities.decodeString(getJSONRepresentation(), "description"); }
  public boolean getSimpleOffer() { return simpleOffer; }
  public Integer getMaximumAcceptances() { return maximumAcceptances; }
  public Integer getMaximumAcceptancesPeriodDays() { return maximumAcceptancesPeriodDays; }
  public Integer getMaximumAcceptancesPeriodMonths() { return maximumAcceptancesPeriodMonths; }
  public String getNotEligibilityReason() { return notEligibilityReason; }
  public String getLimitsReachedReason() {	return limitsReachedReason; }
  public boolean getStockRecurrence() { return stockRecurrence; }
  public Integer getStockRecurrenceBatch() { return stockRecurrenceBatch; }
  public Integer getStockAlertThreshold() { return stockAlertThreshold; }
  public boolean getStockAlert() { return stockAlert; }
  public List<String> getNotificationEmails() { return notificationEmails; }
  public JourneyScheduler getStockScheduler() { return stockScheduler; }
  public Date getLastStockRecurrenceDate() { return (lastStockRecurrenceDate != null) ? lastStockRecurrenceDate : getEffectiveStartDate(); }
  
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
  *  evaluateProfileCriteriaWithReason
  *
  *****************************************/

  public boolean evaluateProfileCriteriaWithReason(SubscriberEvaluationRequest evaluationRequest)
  {
	    //
	    //  clear evaluationVariables
	    //

	    evaluationRequest.getEvaluationVariables().clear();

	    setNotEligibilityReason(null);
	    //
	    //  evaluate
	    //

	    for (EvaluationCriterion criterion : profileCriteria)
	      {
	        if(!criterion.evaluate(evaluationRequest)) {
	        	setNotEligibilityReason(criterion.getCriterionField().getName());
	        	break;
	        }
	        
	      }

	    return true;
  }
  
  /*****************************************
  *
  *  evaluateLimitsReachedWithReason
  *
  *****************************************/
  public boolean evaluateLimitsReachedWithReason(Map<String,List<Date>> oldFullPurchaseHistory, Map<String, List<Pair<String, Date>>> newFullPurchaseHistory, int tenantID, boolean isLimitsReached)
  {

	Date now = SystemTime.getCurrentTime();    
	setLimitsReachedReason(null);
	Date earliestDateToKeepForCriteria = EvolutionEngine.computeEarliestDateForAdvanceCriteria(now, tenantID);
    Date earliestDateToKeep = EvolutionEngine.computeEarliestDateToKeep(now, this, tenantID);
    Date earliestDateToKeepInHistory = earliestDateToKeep.after(earliestDateToKeepForCriteria) ? earliestDateToKeepForCriteria : earliestDateToKeep; // this is advance criteria - we must have data for 4months EVPRO-1066
    List<Pair<String, Date>> cleanPurchaseHistory = new ArrayList<Pair<String, Date>>();
    
  //TODO: before EVPRO-1066 all the purchase were kept like Map<String,List<Date>, now it is Map<String, List<Pair<String, Date>>> <saleschnl, Date>
    // so it is important to migrate data, but once all customer run over this version, this should be removed
    // ------ START DATA MIGRATION COULD BE REMOVED
    List<Date> oldPurchaseHistory = oldFullPurchaseHistory.get(getOfferID());
    
    //
    //  oldPurchaseHistory migration TO BE removed
    //
    if (oldPurchaseHistory != null)
    {
      String salesChannelIDMigration = "migrating-ActualWasntAvlbl";
      // only keep earliestDateToKeepInHistory purchase dates (discard dates that are too old)
      for (Date purchaseDate : oldPurchaseHistory)
        {
      	if (purchaseDate.after(earliestDateToKeepInHistory))
          {
            cleanPurchaseHistory.add(new Pair<String, Date>(salesChannelIDMigration, purchaseDate));
          }
        }
        oldFullPurchaseHistory.put(getOfferID(), new ArrayList<Date>()); // old will be blank - will be removed future
    }
    // ------ END DATA MIGRATION COULD BE REMOVED
    
    //
    //  newPurchaseHistory
    //
    List<Pair<String, Date>> newPurchaseHistory = newFullPurchaseHistory.get(getOfferID());
    if (newPurchaseHistory != null)
      {
  	  for (Pair<String, Date> purchaseDatePair : newPurchaseHistory)
        {
          Date purchaseDate = purchaseDatePair.getSecondElement();
          if (purchaseDate.after(earliestDateToKeepInHistory))
            {
              cleanPurchaseHistory.add(new Pair<String, Date>(purchaseDatePair.getFirstElement(), purchaseDatePair.getSecondElement()));
            }
        }
      }
    //
    //  filter on earliestDateToKeep (this is for offer purchase limitation - not adv criteria)
    //
    
    long previousPurchseCount = cleanPurchaseHistory.stream().filter(history -> history.getSecondElement().after(earliestDateToKeep)).count();
    int totalPurchased = (int) (previousPurchseCount) + 1; //put +1 to check if possible to purchase now a new offer
    
    if (EvolutionEngine.isPurchaseLimitReached(this, totalPurchased))
    {
  	  if (log.isTraceEnabled()) log.trace("maximumAcceptances : " + getMaximumAcceptances() + " of offer " + getOfferID() + " exceeded for subscriber as totalPurchased = " + totalPurchased + " (" + cleanPurchaseHistory.size() + ") earliestDateToKeep : " + earliestDateToKeep);
        setLimitsReachedReason("purchases limit reached");
        if(!isLimitsReached) {
        	return false;
        }
      }
    
    return true;

  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Offer(SchemaAndValue schemaAndValue, double initialPropensity, Integer stock, int unitaryCost, List<EvaluationCriterion> profileCriteria, Set<OfferObjectiveInstance> offerObjectives, Set<OfferSalesChannelsAndPrice> offerSalesChannelsAndPrices, Set<OfferProduct> offerProducts, Set<OfferVoucher> offerVouchers, OfferCharacteristics offerCharacteristics, Set<OfferTranslation> offerTranslations, boolean simpleOffer, Integer maximumAcceptances, Integer maximumAcceptancesPeriodDays, Integer maximumAcceptancesPeriodMonths, boolean stockRecurrence, Integer stockRecurrenceBatch, Integer stockAlertThreshold, boolean stockAlert, List<String> notificationEmails, JourneyScheduler stockScheduler, Date lastStockRecurrenceDate)
  {
    super(schemaAndValue);
    this.initialPropensity = getValidPropensity(initialPropensity);
    this.stock = stock;
    this.unitaryCost = unitaryCost;
    this.profileCriteria = profileCriteria;
    this.offerOfferObjectives = offerObjectives;
    this.offerSalesChannelsAndPrices = offerSalesChannelsAndPrices;
    this.offerProducts = offerProducts;
    this.offerVouchers = offerVouchers;
    this.offerTranslations = offerTranslations;
    this.stockableItemID = "offer-" + getOfferID();
    this.offerCharacteristics = offerCharacteristics;
    this.simpleOffer = simpleOffer;
    this.maximumAcceptances = maximumAcceptances;
    this.maximumAcceptancesPeriodDays = maximumAcceptancesPeriodDays;
    this.maximumAcceptancesPeriodMonths = maximumAcceptancesPeriodMonths;
    this.stockRecurrence = stockRecurrence;
    this.stockRecurrenceBatch = stockRecurrenceBatch;
    this.stockAlertThreshold = stockAlertThreshold;
    this.stockAlert = stockAlert;
    this.notificationEmails = notificationEmails;
    this.stockScheduler = stockScheduler;
    this.lastStockRecurrenceDate = lastStockRecurrenceDate;
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
    if(offer.getOfferProducts()!=null) struct.put("offerProducts", packOfferProducts(offer.getOfferProducts()));
    if(offer.getOfferVouchers()!=null) struct.put("offerVouchers", packOfferVouchers(offer.getOfferVouchers()));
    struct.put("offerTranslations", packOfferTranslations(offer.getOfferTranslations()));
    struct.put("offerCharacteristics", OfferCharacteristics.pack(offer.getOfferCharacteristics()));
    struct.put("simpleOffer", offer.getSimpleOffer());
    struct.put("maximumAcceptances", offer.getMaximumAcceptances());
    struct.put("maximumAcceptancesPeriodDays", offer.getMaximumAcceptancesPeriodDays());
    struct.put("maximumAcceptancesPeriodMonths", offer.getMaximumAcceptancesPeriodMonths());
    struct.put("stockRecurrence", offer.getStockRecurrence());
    struct.put("stockRecurrenceBatch", offer.getStockRecurrenceBatch());
    struct.put("stockAlertThreshold", offer.getStockAlertThreshold());
    struct.put("stockAlert", offer.getStockAlert());
    struct.put("notificationEmails", offer.getNotificationEmails());
    struct.put("stockScheduler", JourneyScheduler.serde().packOptional(offer.getStockScheduler()));
    struct.put("lastStockRecurrenceDate", offer.getLastStockRecurrenceDate());
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
   *  packOfferVouchers
   *
   ****************************************/

  private static List<Object> packOfferVouchers(Set<OfferVoucher> offerVouchers)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferVoucher offerVoucher : offerVouchers)
    {
      result.add(OfferVoucher.pack(offerVoucher));
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
    Set<OfferVoucher> offerVouchers = schemaVersion>1?unpackOfferVouchers(schema.field("offerVouchers").schema(), valueStruct.get("offerVouchers")):null;
    Set<OfferTranslation> offerTranslations = unpackOfferTranslations(schema.field("offerTranslations").schema(), valueStruct.get("offerTranslations"));
    OfferCharacteristics offerCharacteristics = OfferCharacteristics.unpack(new SchemaAndValue(schema.field("offerCharacteristics").schema(), valueStruct.get("offerCharacteristics")));
    boolean simpleOffer = (schemaVersion >= 3) ? valueStruct.getBoolean("simpleOffer") : false;
    Integer maximumAcceptances = (schemaVersion >= 3) ? valueStruct.getInt32("maximumAcceptances") : Integer.MAX_VALUE;
    Integer maximumAcceptancesPeriodDays = (schemaVersion >= 3) ? valueStruct.getInt32("maximumAcceptancesPeriodDays") : 1;
    Integer maximumAcceptancesPeriodMonths = (schema.field("maximumAcceptancesPeriodMonths")!= null) ? valueStruct.getInt32("maximumAcceptancesPeriodMonths") : 1;
    boolean stockRecurrence = (schemaVersion >= 5) ? valueStruct.getBoolean("stockRecurrence") : false;
    Integer stockRecurrenceBatch = (schemaVersion >= 5) ? valueStruct.getInt32("stockRecurrenceBatch") : 0;
    Integer stockAlertThreshold = (schemaVersion >= 5) ? valueStruct.getInt32("stockAlertThreshold") : 0;
    boolean stockAlert = (schemaVersion >= 5) ? valueStruct.getBoolean("stockAlert") : false;
    List<String> notificationEmails = schema.field("notificationEmails") != null ? valueStruct.getArray("notificationEmails") : new ArrayList<String>();
    JourneyScheduler stockScheduler = (schema.field("stockScheduler")!= null) ? JourneyScheduler.serde().unpackOptional(new SchemaAndValue(schema.field("stockScheduler").schema(),valueStruct.get("stockScheduler"))) : null;
    Date lastStockRecurrenceDate = schema.field("lastStockRecurrenceDate") != null ? (Date) valueStruct.get("lastStockRecurrenceDate") : null;
    
    //
    //  return
    //

    return new Offer(schemaAndValue, initialPropensity, stock, unitaryCost, profileCriteria, offerObjectives, offerSalesChannelsAndPrices, offerProducts, offerVouchers, offerCharacteristics, offerTranslations, simpleOffer, maximumAcceptances, maximumAcceptancesPeriodDays, maximumAcceptancesPeriodMonths, stockRecurrence, stockRecurrenceBatch, stockAlertThreshold, stockAlert, notificationEmails, stockScheduler, lastStockRecurrenceDate);
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

    if ( value == null ) return null;

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
   *  unpackOfferVouchers
   *
   *****************************************/

  private static Set<OfferVoucher> unpackOfferVouchers(Schema schema, Object value)
  {

    if ( value == null ) return null;

    //
    //  get schema for OfferVoucher
    //

    Schema offerVoucherSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<OfferVoucher> result = new HashSet<OfferVoucher>();
    List<Object> valueArray = (List<Object>) value;
    for (Object offerVoucher : valueArray)
    {
      result.add(OfferVoucher.unpack(new SchemaAndValue(offerVoucherSchema, offerVoucher)));
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

  public Offer(JSONObject jsonRoot, long epoch, GUIManagedObject existingOfferUnchecked, CatalogCharacteristicService catalogCharacteristicService, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingOfferUnchecked != null) ? existingOfferUnchecked.getEpoch() : epoch, tenantID);

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
    
    this.initialPropensity = getValidPropensity(JSONUtilities.decodeDouble(jsonRoot, "initialPropensity", false));
    this.stock = JSONUtilities.decodeInteger(jsonRoot, "presentationStock", false);
    this.unitaryCost = JSONUtilities.decodeInteger(jsonRoot, "unitaryCost", true);
    this.profileCriteria = decodeProfileCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", true), tenantID);
    this.offerOfferObjectives = decodeOfferObjectives(JSONUtilities.decodeJSONArray(jsonRoot, "offerObjectives", true), catalogCharacteristicService);
    this.offerSalesChannelsAndPrices = decodeOfferSalesChannelsAndPrices(JSONUtilities.decodeJSONArray(jsonRoot, "salesChannelsAndPrices", true));
    this.offerProducts = decodeOfferProducts(JSONUtilities.decodeJSONArray(jsonRoot, "products", false));
    this.offerVouchers = decodeOfferVouchers(JSONUtilities.decodeJSONArray(jsonRoot, "vouchers", false));
    this.offerTranslations = decodeOfferTranslations(JSONUtilities.decodeJSONArray(jsonRoot, "offerTranslations", false));
    this.stockableItemID = "offer-" + getOfferID();
    this.offerCharacteristics = new OfferCharacteristics(JSONUtilities.decodeJSONObject(jsonRoot, "offerCharacteristics", false), catalogCharacteristicService);
    this.simpleOffer = JSONUtilities.decodeBoolean(jsonRoot, "simpleOffer", Boolean.FALSE);
    this.maximumAcceptances = JSONUtilities.decodeInteger(jsonRoot, "maximumAcceptances", Integer.MAX_VALUE);
    this.maximumAcceptancesPeriodDays = UNSET;
    this.maximumAcceptancesPeriodMonths = UNSET;
    Integer maximumAcceptancesPeriod = JSONUtilities.decodeInteger(jsonRoot, "maximumAcceptancesPeriod", UNSET);
    if (maximumAcceptancesPeriod != UNSET) { // new version
      String maximumAcceptancesUnitStr = JSONUtilities.decodeString(jsonRoot, "maximumAcceptancesUnit", TimeUnit.Day.getExternalRepresentation());
      TimeUnit maximumAcceptancesUnit = TimeUnit.fromExternalRepresentation(maximumAcceptancesUnitStr);
      if (maximumAcceptancesUnit.equals(TimeUnit.Day)) {
        this.maximumAcceptancesPeriodDays = maximumAcceptancesPeriod;
      } else if (maximumAcceptancesUnit.equals(TimeUnit.Month)) {
        this.maximumAcceptancesPeriodMonths = maximumAcceptancesPeriod;
      } else {
        throw new GUIManagerException("Unsupported unit for maximumAcceptancesUnit", maximumAcceptancesUnitStr);
      }
    } else { // old version
      this.maximumAcceptancesPeriodDays = JSONUtilities.decodeInteger(jsonRoot, "maximumAcceptancesPeriodDays", 1);
    }
    this.stockRecurrence = JSONUtilities.decodeBoolean(jsonRoot, "stockRecurrence", Boolean.FALSE);
    this.stockRecurrenceBatch = JSONUtilities.decodeInteger(jsonRoot, "stockRecurrenceBatch", 0);
    if (stockRecurrence) this.stockScheduler = new JourneyScheduler(JSONUtilities.decodeJSONObject(jsonRoot, "stockScheduler", stockRecurrence));
    this.stockAlertThreshold = JSONUtilities.decodeInteger(jsonRoot, "presentationStockAlertThreshold", 0);
    this.stockAlert = JSONUtilities.decodeBoolean(jsonRoot, "stockAlert", Boolean.FALSE);
    this.notificationEmails = new ArrayList<String>();
    JSONArray notificationArray = JSONUtilities.decodeJSONArray(jsonRoot, "notificationEmails", new JSONArray());
    for (int i=0; i<notificationArray.size(); i++)
      {
        this.notificationEmails.add((String) notificationArray.get(i));
      }
    //this.lastStockRecurrenceDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "lastStockRecurrenceDate", Boolean.FALSE));

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

  private List<EvaluationCriterion> decodeProfileCriteria(JSONArray jsonArray, int tenantID) throws GUIManagerException
  {
    List<EvaluationCriterion> result = new ArrayList<EvaluationCriterion>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new EvaluationCriterion((JSONObject) jsonArray.get(i), CriterionContext.DynamicProfile(tenantID), tenantID));
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
    if(jsonArray==null) return null;
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
   *  decodeOfferVouchers
   *
   *****************************************/

  private Set<OfferVoucher> decodeOfferVouchers(JSONArray jsonArray) throws GUIManagerException
  {
    if(jsonArray==null) return null;
    Set<OfferVoucher> result = new HashSet<OfferVoucher>();
    if (jsonArray != null)
    {
      for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new OfferVoucher((JSONObject) jsonArray.get(i)));
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
        epochChanged = epochChanged || ! Objects.equals(offerVouchers, existingOffer.getOfferVouchers());
        epochChanged = epochChanged || ! Objects.equals(offerTranslations, existingOffer.getOfferTranslations());
        epochChanged = epochChanged || ! Objects.equals(offerCharacteristics, existingOffer.getOfferCharacteristics());
        epochChanged = epochChanged || ! Objects.equals(simpleOffer, existingOffer.getSimpleOffer());
        epochChanged = epochChanged || ! Objects.equals(maximumAcceptances, existingOffer.getMaximumAcceptances());
        epochChanged = epochChanged || ! Objects.equals(maximumAcceptancesPeriodDays, existingOffer.getMaximumAcceptancesPeriodDays());
        epochChanged = epochChanged || ! Objects.equals(stockRecurrence, existingOffer.getStockRecurrence());
        epochChanged = epochChanged || ! Objects.equals(stockRecurrenceBatch, existingOffer.getStockRecurrenceBatch());
        epochChanged = epochChanged || ! Objects.equals(lastStockRecurrenceDate, existingOffer.getLastStockRecurrenceDate());
        epochChanged = epochChanged || ! Objects.equals(stockAlertThreshold, existingOffer.getStockAlertThreshold());
        epochChanged = epochChanged || ! Objects.equals(stockAlert, existingOffer.getStockAlert());
        epochChanged = epochChanged || ! Objects.equals(notificationEmails, existingOffer.getNotificationEmails());
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

  public void validate(CallingChannelService callingChannelService, SalesChannelService salesChannelService, ProductService productService, VoucherService voucherService, Date date) throws GUIManagerException
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

    if ( offerProducts != null && !offerProducts.isEmpty() )
      {
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

    /****************************************
     *
     *  ensure valid/active vouchers
     *
     ****************************************/

    if ( offerVouchers != null && !offerVouchers.isEmpty() )
      {
        for (OfferVoucher offerVoucher : offerVouchers)
        {
          //
          //  retrieve voucher
          //

          GUIManagedObject voucher = voucherService.getStoredVoucher(offerVoucher.getVoucherID());

          //
          //  validate the voucher exists
          //

          if (voucher == null) throw new GUIManagerException("unknown voucher", offerVoucher.getVoucherID());

          //
          //  validate the voucher start/end dates include the entire offer active period
          //

          if (! voucherService.isActiveThroughInterval(voucher, this.getEffectiveStartDate(), this.getEffectiveEndDate())) throw new GUIManagerException("invalid voucher (start/end dates)", offerVoucher.getVoucherID());
        }
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
      return "Offer [initialPropensity=" + initialPropensity + ", " + (getGUIManagedObjectID() != null ? "getGUIManagedObjectID()=" + getGUIManagedObjectID() : "") + "]";
    }
  
  /*******************************
   * 
   * getGUIDependencies
   * 
   *******************************/
  
  @Override public Map<String, List<String>> getGUIDependencies(List<GUIService> guiServiceList, int tenantID)
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    List<String> productIDs = new ArrayList<String>();
    if (getOfferProducts() != null && !getOfferProducts().isEmpty()){
      productIDs = getOfferProducts().stream().map(product -> product.getProductID()).collect(Collectors.toList());
    }
    List<String> voucherIDs = new ArrayList<String>();
    if (getOfferVouchers() != null && !getOfferVouchers().isEmpty()){
      voucherIDs = getOfferVouchers().stream().map(voucher -> voucher.getVoucherID()).collect(Collectors.toList());
    }
    List<String> saleschannelIDs = new ArrayList<String>();
    List<String> offerObjectiveIDs = getOfferObjectives().stream().map(offerObjective -> offerObjective.getOfferObjectiveID()).collect(Collectors.toList());
    List<String> targetIDs = new ArrayList<String>();
    List<String> offerCharacteristicsLanguagePropertyIDs = new ArrayList<String>();
    List<String> offerIDs = new ArrayList<String>();
    List<String> loyaltyProgramPointsIDs = new ArrayList<String>();
    List<String> loyaltyprogramchallengeIDs = new ArrayList<String>();
    List<String> loyaltyprogrammissionIDs = new ArrayList<String>();
    List<String> callingchannelIDs = new ArrayList<String>();
    List<String> pointIDs = new ArrayList<String>();
    List<String> badgeIDs = new ArrayList<String>();

    for (OfferSalesChannelsAndPrice offerSalesChannelsAndPrice : getOfferSalesChannelsAndPrices())
      {
        saleschannelIDs.addAll(offerSalesChannelsAndPrice.getSalesChannelIDs());
      }

    List<EvaluationCriterion> profileCriterias = getProfileCriteria();
    offerIDs.addAll(getSubcriteriaFieldArgumentValues(profileCriterias, "offer.id"));
    offerIDs = offerIDs.stream().filter(ofrID -> !ofrID.equals(getGUIManagedObjectID())).collect(Collectors.toList()); // cycle
    
    voucherIDs.addAll(getSubcriteriaFieldArgumentValues(profileCriterias, "voucher.id"));
    badgeIDs.addAll(getSubcriteriaFieldArgumentValues(profileCriterias, "badge.id"));
    
    for (EvaluationCriterion profileCrt : profileCriterias)
      {
        String loyaltyProgramPointsID = getGUIManagedObjectIDFromDynamicCriterion(profileCrt, "loyaltyprogrampoints", guiServiceList);
        String loyaltyprogramchallengeID = getGUIManagedObjectIDFromDynamicCriterion(profileCrt, "loyaltyprogramchallenge", guiServiceList);
        String loyaltyprogrammissionID = getGUIManagedObjectIDFromDynamicCriterion(profileCrt, "loyaltyprogrammission", guiServiceList);
        
        if (loyaltyProgramPointsID != null) loyaltyProgramPointsIDs.add(loyaltyProgramPointsID);
        if (loyaltyprogramchallengeID != null) loyaltyprogramchallengeIDs.add(loyaltyprogramchallengeID);
        if (loyaltyprogrammissionID != null) loyaltyprogrammissionIDs.add(loyaltyprogrammissionID);
        
        if (profileCrt != null && profileCrt.getCriterionField() != null && profileCrt.getCriterionField().getESField() != null && profileCrt.getCriterionField().getESField().equals("internal.targets"))
          {
            if (profileCrt.getCriterionOperator() == CriterionOperator.ContainsOperator || profileCrt.getCriterionOperator() == CriterionOperator.DoesNotContainOperator)
              targetIDs.add(profileCrt.getArgumentExpression().replace("'", ""));
            else if (profileCrt.getCriterionOperator() == CriterionOperator.NonEmptyIntersectionOperator || profileCrt.getCriterionOperator() == CriterionOperator.EmptyIntersectionOperator)
              targetIDs.addAll(Arrays.asList(profileCrt.getArgumentExpression().replace("[", "").replace("]", "").replace("'", "").split(",")));

          }
      }
    
    if (getOfferCharacteristics() != null && getOfferCharacteristics().getOfferCharacteristicProperties() != null && !getOfferCharacteristics().getOfferCharacteristicProperties().isEmpty())
      {
        CallingChannelService callingChannelService = (CallingChannelService) guiServiceList.stream().filter(srvc -> srvc.getClass() == CallingChannelService.class).findFirst().orElse(null);
        List<GUIManagedObject> callingChannels = new ArrayList<GUIManagedObject>();
        if (callingChannelService != null) callingChannels = callingChannelService.getStoredCallingChannels(tenantID).stream().filter(callingChannelUnchecked -> callingChannelUnchecked.getAccepted()).collect(Collectors.toList());
        for (OfferCharacteristicsLanguageProperty offerCharacteristicsLanguageProperty : getOfferCharacteristics().getOfferCharacteristicProperties())
          {
            if (offerCharacteristicsLanguageProperty.getProperties() != null && !offerCharacteristicsLanguageProperty.getProperties().isEmpty())
              {
                for (OfferCharacteristicsProperty characteristicsProperty : offerCharacteristicsLanguageProperty.getProperties())
                  {
                    String catalogCharacteristicID = characteristicsProperty.getCatalogCharacteristicID();
                    offerCharacteristicsLanguagePropertyIDs.add(catalogCharacteristicID);
                    CallingChannel callingChannel = (CallingChannel) callingChannels.stream().filter(callingChannelObj -> ((CallingChannel) callingChannelObj).getCatalogCharacteristics().contains(catalogCharacteristicID)).findFirst().orElse(null);
                    if (callingChannel != null) callingchannelIDs.add(callingChannel.getGUIManagedObjectID());
                  }
              }
          }
      }

    if (getOfferSalesChannelsAndPrices() != null && !getOfferSalesChannelsAndPrices().isEmpty())
      {
        for (OfferSalesChannelsAndPrice offerSalesChannelsAndPrice : getOfferSalesChannelsAndPrices())
          {
            if (offerSalesChannelsAndPrice.getPrice() != null && "provider_Point".equals(offerSalesChannelsAndPrice.getPrice().getProviderID()))
              {
                String pointID = offerSalesChannelsAndPrice.getPrice().getPaymentMeanID() != null && offerSalesChannelsAndPrice.getPrice().getPaymentMeanID().startsWith(CommodityDeliveryManager.POINT_PREFIX) ? offerSalesChannelsAndPrice.getPrice().getPaymentMeanID().replace(CommodityDeliveryManager.POINT_PREFIX, "") : null ;
                if (pointID != null) pointIDs.add(pointID);
              }
          }
      }
    
    result.put("badge", badgeIDs);
    result.put("offer", offerIDs);
    result.put("target", targetIDs);
    result.put("product", productIDs);
    result.put("voucher", voucherIDs);
    result.put("saleschannel", saleschannelIDs);
    result.put("callingchannel", callingchannelIDs);
    result.put("offerobjective", offerObjectiveIDs);
    result.put("catalogcharacteristic", offerCharacteristicsLanguagePropertyIDs);
    result.put("loyaltyprogrampoints", loyaltyProgramPointsIDs);
    result.put("loyaltyprogramchallenge", loyaltyprogramchallengeIDs);
    result.put("loyaltyprogrammission", loyaltyprogrammissionIDs);
    result.put("point", pointIDs);

    return result;
  }
  
  /*****************************************
  *
  *  hasThisOfferCharacteristics
  *
  *****************************************/
  
  public boolean hasThisOfferCharacteristics(OfferCharacteristics offerCharacteristics)
  {
    boolean result = false;
    if (getOfferCharacteristics() != null && getOfferCharacteristics().getOfferCharacteristicProperties() != null && !getOfferCharacteristics().getOfferCharacteristicProperties().isEmpty())
      {
        for (OfferCharacteristicsLanguageProperty characteristicsLanguageProperty : offerCharacteristics.getOfferCharacteristicProperties())
          {
            OfferCharacteristicsLanguageProperty languageProperty = getOfferCharacteristics().getOfferCharacteristicProperties().stream().filter(offerLanChar -> offerLanChar.getLanguageID().equals(characteristicsLanguageProperty.getLanguageID())).findFirst().orElse(null);
            if (languageProperty != null)
              {
                for (OfferCharacteristicsProperty offerCharacteristicsProperty : characteristicsLanguageProperty.getProperties())
                  {
                    result = languageProperty.getProperties().stream().anyMatch(property -> property.equalsNonRobustly(offerCharacteristicsProperty));
                    if (!result) break;
                  }
              }
            else
              {
                result = false;
              }
            if (!result)break;
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  hasThisOfferSalesChannel
  *
  *****************************************/
  
  public boolean hasThisOfferSalesChannel(String salesChannelID)
  {
    boolean result = false;
    if (getOfferSalesChannelsAndPrices()!=null && !getOfferSalesChannelsAndPrices().isEmpty())
      {
        for (OfferSalesChannelsAndPrice offerSalesChannelsAndPrice : getOfferSalesChannelsAndPrices())
          {
            if(offerSalesChannelsAndPrice.getSalesChannelIDs() != null) 
            {
              for(String scID : offerSalesChannelsAndPrice.getSalesChannelIDs()) 
                {
                  if(scID.equals(salesChannelID))
                    {
                      result = true;
                      break;
                    }
                }
            }
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  hasThisOfferCharacteristics
  *
  *****************************************/
  
  public boolean hasThisOfferObjectiveAndCharacteristics(final OfferObjectiveInstance offerObjectiveInstance)
  {
    boolean result = false;
    if (getOfferObjectives() != null && !getOfferObjectives().isEmpty())
      {
        OfferObjectiveInstance offerObjective = getOfferObjectives().stream().filter(offerObj -> offerObj.getOfferObjectiveID().equals(offerObjectiveInstance.getOfferObjectiveID())).findFirst().orElse(null);
        if (offerObjective != null)
          {
            for (CatalogCharacteristicInstance catalogCharacteristicInstance : offerObjectiveInstance.getCatalogCharacteristics())
              {
                result = offerObjective.getCatalogCharacteristics().stream().anyMatch(catalogChara -> catalogChara.equalsNonRobustly(catalogCharacteristicInstance));
                if (!result)break;
              }
          }
      }
    return result;
  }
  
 
  public List<String> getSubcriteriaFieldArgumentValues(List<EvaluationCriterion> allCriterions, String fieldID)
  {
    List<String> result = new ArrayList<String>();
    if (allCriterions != null && !allCriterions.isEmpty())
      {
        for (EvaluationCriterion criterion : allCriterions)
          {
            if (criterion.getSubcriteriaExpressions() != null && !criterion.getSubcriteriaExpressions().isEmpty())
              {
                for (String field : criterion.getSubcriteriaExpressions().keySet())
                  {
                    if (field.equals(fieldID))
                      {
                        Expression expression = criterion.getSubcriteriaExpressions().get(field);
                        if (expression != null && expression instanceof ConstantExpression)
                          {
                            ConstantExpression consExpression = (ConstantExpression) expression;
                            String fieldIDArgVal  = (String) consExpression.evaluateConstant();
                            result.add(fieldIDArgVal);
                          }
                      }
                  }
              }
          }
      }
    return result;
  }
  
  private String getGUIManagedObjectIDFromDynamicCriterion(EvaluationCriterion criteria, String objectType, List<GUIService> guiServiceList)
  {
    String result = null;
    try
    {
      switch (objectType.toLowerCase())
      {
        case "loyaltyprogrampoints":
          Pattern fieldNamePattern = Pattern.compile("^loyaltyprogram\\.([^.]+)\\.(.+)$");
          Matcher fieldNameMatcher = fieldNamePattern.matcher(criteria.getCriterionField().getID());
          if (fieldNameMatcher.find())
            {
              String loyaltyProgramID = fieldNameMatcher.group(1);
              LoyaltyProgramService loyaltyProgramService = (LoyaltyProgramService) guiServiceList.stream().filter(srvc -> srvc.getClass() == LoyaltyProgramService.class).findFirst().orElse(null);
              if (loyaltyProgramService != null)
                {
                  GUIManagedObject uncheckedLoyalty = loyaltyProgramService.getStoredLoyaltyProgram(loyaltyProgramID);
                  if (uncheckedLoyalty !=  null && uncheckedLoyalty.getAccepted() && ((LoyaltyProgram) uncheckedLoyalty).getLoyaltyProgramType() == LoyaltyProgramType.POINTS) result = uncheckedLoyalty.getGUIManagedObjectID();
                }
            }
          break;
          
        case "loyaltyprogramchallenge":
          fieldNamePattern = Pattern.compile("^loyaltyprogram\\.([^.]+)\\.(.+)$");
          fieldNameMatcher = fieldNamePattern.matcher(criteria.getCriterionField().getID());
          if (fieldNameMatcher.find())
            {
              String loyaltyProgramID = fieldNameMatcher.group(1);
              LoyaltyProgramService loyaltyProgramService = (LoyaltyProgramService) guiServiceList.stream().filter(srvc -> srvc.getClass() == LoyaltyProgramService.class).findFirst().orElse(null);
              if (loyaltyProgramService != null)
                {
                  GUIManagedObject uncheckedLoyalty = loyaltyProgramService.getStoredLoyaltyProgram(loyaltyProgramID);
                  if (uncheckedLoyalty !=  null && uncheckedLoyalty.getAccepted() && ((LoyaltyProgram) uncheckedLoyalty).getLoyaltyProgramType() == LoyaltyProgramType.CHALLENGE) result = uncheckedLoyalty.getGUIManagedObjectID();
                }
            }
          break;
          
        case "loyaltyprogrammission":
          fieldNamePattern = Pattern.compile("^loyaltyprogram\\.([^.]+)\\.(.+)$");
          fieldNameMatcher = fieldNamePattern.matcher(criteria.getCriterionField().getID());
          if (fieldNameMatcher.find())
            {
              String loyaltyProgramID = fieldNameMatcher.group(1);
              LoyaltyProgramService loyaltyProgramService = (LoyaltyProgramService) guiServiceList.stream().filter(srvc -> srvc.getClass() == LoyaltyProgramService.class).findFirst().orElse(null);
              if (loyaltyProgramService != null)
                {
                  GUIManagedObject uncheckedLoyalty = loyaltyProgramService.getStoredLoyaltyProgram(loyaltyProgramID);
                  if (uncheckedLoyalty !=  null && uncheckedLoyalty.getAccepted() && ((LoyaltyProgram) uncheckedLoyalty).getLoyaltyProgramType() == LoyaltyProgramType.MISSION) result = uncheckedLoyalty.getGUIManagedObjectID();
                }
            }
          break;

        default:
          break;
      }
    }
  catch (PatternSyntaxException e)
    {
      if (log.isTraceEnabled()) log.trace("PatternSyntaxException Description: {}, Index: ", e.getDescription(), e.getIndex());
    }
    
    return result;
  }
  
  public void setNotEligibilityReason(String notEligibilityReason) {
		this.notEligibilityReason = notEligibilityReason;
	}
  
  public void setLimitsReachedReason(String limitsReachedReason) {
		this.limitsReachedReason = limitsReachedReason;
	}
  
  public void setLastStockRecurrenceDate(Date lastStockRecurrenceDate) {
        this.lastStockRecurrenceDate = lastStockRecurrenceDate;
    }
  
}
