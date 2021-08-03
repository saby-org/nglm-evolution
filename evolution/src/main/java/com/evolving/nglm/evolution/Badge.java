package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

@GUIDependencyDef(objectType = "badge", serviceClass = BadgeService.class, dependencies = { })
public class Badge extends GUIManagedObject
{
  
  //
  //  logger
  //
  
  private static final Logger log = LoggerFactory.getLogger(Badge.class);
  
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  BadgeType
  //

  public enum BadgeType
  {
    PERMANENT("0"),
    STATUS("1"),
    COLLECTIBLE("2"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private BadgeType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static BadgeType fromExternalRepresentation(String externalRepresentation) { for (BadgeType enumeratedValue : BadgeType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
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
    schemaBuilder.name("badge");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),4));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("badgeObjectives", SchemaBuilder.array(OfferObjectiveInstance.schema()).schema());
    schemaBuilder.field("badgeTranslations", SchemaBuilder.array(OfferTranslation.schema()).schema());
    schemaBuilder.field("description", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("badgeTypeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("pendingImageURL", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("awardedImageURL", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("awardedWorkflowID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("removeWorkflowID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("profileCriteria", SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schema = schemaBuilder.build();
  };
  
  //
  //  serde
  //

  private static ConnectSerde<Badge> serde = new ConnectSerde<Badge>(schema, false, Badge.class, Badge::pack, Badge::unpack);
  
  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Badge> serde() { return serde; }
  
  /****************************************
  *
  *  data
  *
  ****************************************/

  private Set<OfferObjectiveInstance> badgeObjectives; 
  private Set<OfferTranslation> badgeTranslations;
  private String description;
  private String pendingImageURL;
  private String awardedImageURL;
  private String awardedWorkflowID;
  private String removeWorkflowID;
  private List<EvaluationCriterion> profileCriteria;
  private String badgeTypeID;

  /****************************************
  *
  *  accessors
  *
  ****************************************/
  
  public Set<OfferObjectiveInstance> getBadgeObjectives()
  {
    return badgeObjectives;
  }
  public Set<OfferTranslation> getBadgeTranslations()
  {
    return badgeTranslations;
  }
  public String getDescription()
  {
    return description;
  }
  public String getPendingImageURL()
  {
    return pendingImageURL;
  }
  public String getAwardedImageURL()
  {
    return awardedImageURL;
  }
  public String getAwardedWorkflowID()
  {
    return awardedWorkflowID;
  }
  public String getRemoveWorkflowID()
  {
    return removeWorkflowID;
  }
  public List<EvaluationCriterion> getProfileCriteria()
  {
    return profileCriteria;
  }
  public BadgeType getBadgeType()
  {
    return BadgeType.fromExternalRepresentation(badgeTypeID);
  }
  
  /*****************************************
  *
  *  evaluateProfileCriteria
  *
  *****************************************/

  public boolean evaluateProfileCriteria(SubscriberEvaluationRequest evaluationRequest)
  {
    return EvaluationCriterion.evaluateCriteria(evaluationRequest, profileCriteria);
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
  *  packBadgeObjectives
  *
  ****************************************/

  private static List<Object> packBadgeObjectives(Set<OfferObjectiveInstance> offerObjectives)
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
  *  packBadgeTranslations
  *
  ****************************************/

  private static List<Object> packBadgeTranslations(Set<OfferTranslation> offerTranslations)
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
  *  unpackBadgeObjectives
  *
  *****************************************/

  private static Set<OfferObjectiveInstance> unpackBadgeObjectives(Schema schema, Object value)
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
  *  unpackBadgeTranslations
  *
  *****************************************/

  private static Set<OfferTranslation> unpackBadgeTranslations(Schema schema, Object value)
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
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Badge badge = (Badge) value;
    Struct struct = new Struct(schema);
    packCommon(struct, badge);
    struct.put("badgeObjectives", packBadgeObjectives(badge.getBadgeObjectives()));
    struct.put("badgeTranslations", packBadgeTranslations(badge.getBadgeTranslations()));
    struct.put("description", badge.getDescription());
    struct.put("badgeTypeID", badge.getBadgeType().getExternalRepresentation());
    struct.put("pendingImageURL", badge.getPendingImageURL());
    struct.put("awardedImageURL", badge.getAwardedImageURL());
    struct.put("awardedWorkflowID", badge.getAwardedWorkflowID());
    struct.put("removeWorkflowID", badge.getRemoveWorkflowID());
    struct.put("profileCriteria", packProfileCriteria(badge.getProfileCriteria()));
    return struct;
  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Badge(SchemaAndValue schemaAndValue, Set<OfferObjectiveInstance> badgeObjectives, Set<OfferTranslation> badgeTranslations, String description, String badgeTypeID, String pendingImageURL, String awardedImageURL, String awardedWorkflowID, String removeWorkflowID, List<EvaluationCriterion> profileCriteria)
  {
    super(schemaAndValue);
    this.badgeObjectives = badgeObjectives;
    this.badgeTranslations = badgeTranslations;
    this.description = description;
    this.badgeTypeID = badgeTypeID;
    this.pendingImageURL = pendingImageURL;
    this.awardedImageURL = awardedImageURL;
    this.awardedWorkflowID = awardedWorkflowID;
    this.removeWorkflowID = removeWorkflowID;
    this.profileCriteria = profileCriteria;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Badge unpack(SchemaAndValue schemaAndValue)
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
    Set<OfferObjectiveInstance> badgeObjectives = unpackBadgeObjectives(schema.field("badgeObjectives").schema(), valueStruct.get("badgeObjectives"));
    Set<OfferTranslation> badgeTranslations = unpackBadgeTranslations(schema.field("badgeTranslations").schema(), valueStruct.get("badgeTranslations"));
    String description = valueStruct.getString("description");
    String badgeTypeID = valueStruct.getString("badgeTypeID");
    String pendingImageURL = valueStruct.getString("pendingImageURL");
    String awardedImageURL = valueStruct.getString("awardedImageURL");
    String awardedWorkflowID = valueStruct.getString("awardedWorkflowID");
    String removeWorkflowID = valueStruct.getString("removeWorkflowID");
    List<EvaluationCriterion> profileCriteria = unpackProfileCriteria(schema.field("profileCriteria").schema(), valueStruct.get("profileCriteria"));
    
    //
    //  return
    //

    return new Badge(schemaAndValue, badgeObjectives, badgeTranslations, description, badgeTypeID, pendingImageURL, awardedImageURL, awardedWorkflowID, removeWorkflowID, profileCriteria);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/
  
  public Badge(JSONObject jsonRoot, long epoch, GUIManagedObject existingBadgeUnchecked, CatalogCharacteristicService catalogCharacteristicService, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingBadgeUnchecked != null) ? existingBadgeUnchecked.getEpoch() : epoch, tenantID);

    /*****************************************
    *
    *  existingOffer
    *
    *****************************************/

    Badge existingBadge = (existingBadgeUnchecked != null && existingBadgeUnchecked instanceof Badge) ? (Badge) existingBadgeUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    this.badgeObjectives = decodeBadgeObjectives(JSONUtilities.decodeJSONArray(jsonRoot, "badgeObjectives", true), catalogCharacteristicService);
    this.description = JSONUtilities.decodeString(jsonRoot, "description", false);
    this.badgeTypeID = JSONUtilities.decodeString(jsonRoot, "badgeTypeID", BadgeType.PERMANENT.getExternalRepresentation()); // PERMANENT is default
    this.pendingImageURL = JSONUtilities.decodeString(jsonRoot, "pendingImageURL", false);
    this.awardedImageURL = JSONUtilities.decodeString(jsonRoot, "awardedImageURL", false);
    this.awardedWorkflowID = JSONUtilities.decodeString(jsonRoot, "awardedWorkflowID", false);
    this.removeWorkflowID = JSONUtilities.decodeString(jsonRoot, "removeWorkflowID", false);
    this.badgeTranslations = decodeBadgeTranslations(JSONUtilities.decodeJSONArray(jsonRoot, "translations", false));
    this.profileCriteria = decodeProfileCriteria(JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", true), tenantID);
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
  *  decodeBadgeObjectives
  *
  *****************************************/

  private Set<OfferObjectiveInstance> decodeBadgeObjectives(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
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
  *  decodeBadgeTranslations
  *
  *****************************************/

  private Set<OfferTranslation> decodeBadgeTranslations(JSONArray jsonArray) throws GUIManagerException
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

  private boolean epochChanged(Badge existingBadge)
  {
    if (existingBadge != null && existingBadge.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingBadge.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(badgeObjectives, existingBadge.getBadgeObjectives());
        epochChanged = epochChanged || ! Objects.equals(badgeTranslations, existingBadge.getBadgeTranslations());
        epochChanged = epochChanged || ! Objects.equals(description, existingBadge.getDescription());
        epochChanged = epochChanged || ! Objects.equals(badgeTypeID, existingBadge.getBadgeType().getExternalRepresentation());
        epochChanged = epochChanged || ! Objects.equals(pendingImageURL, existingBadge.getPendingImageURL());
        epochChanged = epochChanged || ! Objects.equals(awardedImageURL, existingBadge.getAwardedImageURL());
        epochChanged = epochChanged || ! Objects.equals(awardedWorkflowID, existingBadge.getAwardedWorkflowID());
        epochChanged = epochChanged || ! Objects.equals(removeWorkflowID, existingBadge.getRemoveWorkflowID());
        epochChanged = epochChanged || ! Objects.equals(profileCriteria, existingBadge.getProfileCriteria());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  /*****************************************
  *
  *  validation
  *
  *****************************************/
  
  public boolean validate() throws GUIManagerException
  {
    return true;
  }
  
  /*******************************
   * 
   * getGUIDependencies
   * 
   *******************************/
  
  @Override public Map<String, List<String>> getGUIDependencies(int tenantID)
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    return result;
  }

}
