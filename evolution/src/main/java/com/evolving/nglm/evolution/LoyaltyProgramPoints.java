/*****************************************************************************
 *
 *  LoyaltyProgram.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class LoyaltyProgramPoints extends LoyaltyProgram
{
  
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  LoyaltyProgramType
  //

  public enum LoyaltyProgramPointsEventInfos
  {
    ENTERING("entering"),
    OLD_TIER("oldTier"),
    NEW_TIER("newTier"),
    LEAVING("leaving"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private LoyaltyProgramPointsEventInfos(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static LoyaltyProgramPointsEventInfos fromExternalRepresentation(String externalRepresentation) { for (LoyaltyProgramPointsEventInfos enumeratedValue : LoyaltyProgramPointsEventInfos.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
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
    schemaBuilder.name("loyalty_program_points");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(LoyaltyProgram.commonSchema().version(),1));
    for (Field field : LoyaltyProgram.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("rewardPointID", Schema.STRING_SCHEMA);
    schemaBuilder.field("statusPointID", Schema.STRING_SCHEMA);
    schemaBuilder.field("tiers", SchemaBuilder.array(Tier.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<LoyaltyProgramPoints> serde = new ConnectSerde<LoyaltyProgramPoints>(schema, false, LoyaltyProgramPoints.class, LoyaltyProgramPoints::pack, LoyaltyProgramPoints::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<LoyaltyProgramPoints> serde() { return serde; }
  
  // 
  //  constants
  // 
  
  public final static String CRITERION_FIELD_NAME_OLD_PREFIX = "pointLoyaltyProgramChange.old.";
  public final static String CRITERION_FIELD_NAME_NEW_PREFIX = "pointLoyaltyProgramChange.new.";
  public final static String CRITERION_FIELD_NAME_IS_UPDATED_PREFIX = "pointLoyaltyProgramChange.isupdated.";


  /*****************************************
   *
   *  data
   *
   *****************************************/

  private String rewardPointID = null;
  private String statusPointID = null;
  private List<Tier> tiers = null;

  /*****************************************
   *
   *  accessors
   *
   *****************************************/

  public String getRewardPointsID() { return rewardPointID; }
  public String getStatusPointsID() { return statusPointID; }
  public List<Tier> getTiers() { return tiers; }

  /*****************************************
   *
   *  constructor -- unpack
   *
   *****************************************/

  public LoyaltyProgramPoints(SchemaAndValue schemaAndValue, String rewardPointsID, String statusPointsID, List<Tier> tiers)
  {
    super(schemaAndValue);
    this.rewardPointID = rewardPointsID;
    this.statusPointID = statusPointsID;
    this.tiers = tiers;
  }

  /*****************************************
   *
   *  pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints) value;
    Struct struct = new Struct(schema);
    LoyaltyProgram.packCommon(struct, loyaltyProgramPoints);
    struct.put("rewardPointID", loyaltyProgramPoints.getRewardPointsID());
    struct.put("statusPointID", loyaltyProgramPoints.getStatusPointsID());
    struct.put("tiers", packLoyaltyProgramTiers(loyaltyProgramPoints.getTiers()));
    return struct;
  }
  
  /****************************************
  *
  *  packLoyaltyProgramTiers
  *
  ****************************************/

  private static List<Object> packLoyaltyProgramTiers(List<Tier> tiers)
  {
    List<Object> result = new ArrayList<Object>();
    for (Tier tier : tiers)
      {
        result.add(Tier.pack(tier));
      }
    return result;
  }

  /*****************************************
   *
   *  unpack
   *
   *****************************************/

  public static LoyaltyProgramPoints unpack(SchemaAndValue schemaAndValue)
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
    String rewardPointsID = valueStruct.getString("rewardPointID");
    String statusPointsID = valueStruct.getString("statusPointID");
    List<Tier> tiers = unpackLoyaltyProgramTiers(schema.field("tiers").schema(), valueStruct.get("tiers"));

    //
    //  return
    //

    return new LoyaltyProgramPoints(schemaAndValue, rewardPointsID, statusPointsID, tiers);
  }
  
  /*****************************************
  *
  *  unpackLoyaltyProgramTiers
  *
  *****************************************/

  private static List<Tier> unpackLoyaltyProgramTiers(Schema schema, Object value)
  {
    //
    //  get schema for LoyaltyProgramTiers
    //

    Schema propertySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<Tier> result = new ArrayList<Tier>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(Tier.unpack(new SchemaAndValue(propertySchema, property)));
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

  public LoyaltyProgramPoints(JSONObject jsonRoot, long epoch, GUIManagedObject existingLoyaltyProgramUnchecked, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    /*****************************************
     *
     *  super
     *
     *****************************************/

    super(jsonRoot, epoch, existingLoyaltyProgramUnchecked, catalogCharacteristicService);

    /*****************************************
     *
     *  existingLoyaltyProgramPoints
     *
     *****************************************/

    LoyaltyProgramPoints existingLoyaltyProgramPoints = (existingLoyaltyProgramUnchecked != null && existingLoyaltyProgramUnchecked instanceof LoyaltyProgramPoints) ? (LoyaltyProgramPoints) existingLoyaltyProgramUnchecked : null;

    /*****************************************
     *
     *  attributes
     *
     *****************************************/
    this.rewardPointID = JSONUtilities.decodeString(jsonRoot, "rewardPointID", true);
    this.statusPointID = JSONUtilities.decodeString(jsonRoot, "statusPointID", true);
    this.tiers = decodeLoyaltyProgramTiers(JSONUtilities.decodeJSONArray(jsonRoot, "tiers", true));
    
    /*****************************************
     *
     *  epoch
     *
     *****************************************/

    if (epochChanged(existingLoyaltyProgramPoints))
      {
        this.setEpoch(epoch);
      }
  }
  
  /*****************************************
  *
  *  decodeLoyaltyProgramTiers
  *
  *****************************************/

  private List<Tier> decodeLoyaltyProgramTiers(JSONArray jsonArray) throws GUIManagerException
  {
    List<Tier> result = new ArrayList<Tier>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new Tier((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }

//  /*****************************************
//   *
//   *  schedule
//   *
//   *****************************************/
//
//  public Date schedule(String touchPointID, Date now)
//  {
//    return now;
//  }

  /*****************************************
   *
   *  epochChanged
   *
   *****************************************/

  private boolean epochChanged(LoyaltyProgramPoints existingLoyaltyProgramPoints)
  {
    if (existingLoyaltyProgramPoints != null && existingLoyaltyProgramPoints.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingLoyaltyProgramPoints.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectName(), existingLoyaltyProgramPoints.getGUIManagedObjectName());
        epochChanged = epochChanged || ! Objects.equals(getLoyaltyProgramType(), existingLoyaltyProgramPoints.getLoyaltyProgramType());
        epochChanged = epochChanged || ! Objects.equals(getRewardPointsID(), existingLoyaltyProgramPoints.getRewardPointsID());
        epochChanged = epochChanged || ! Objects.equals(getStatusPointsID(), existingLoyaltyProgramPoints.getStatusPointsID());
        epochChanged = epochChanged || ! Objects.equals(getTiers(), existingLoyaltyProgramPoints.getTiers());
        epochChanged = epochChanged || ! Objects.equals(getCharacteristics(), existingLoyaltyProgramPoints.getCharacteristics());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  /****************************************
  *
  *  validate
  *
  ****************************************/
  
  @Override public boolean validate() throws GUIManagerException 
  {
    // TODO : any validation needed ?
    return true;
  }

  public static class Tier
  {
    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(Tier.class);

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
      schemaBuilder.name("tier");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("tierName", Schema.STRING_SCHEMA);
      schemaBuilder.field("statusPointLevel", Schema.INT32_SCHEMA);
      schemaBuilder.field("statusEventName", Schema.STRING_SCHEMA);
      schemaBuilder.field("numberOfStatusPointsPerUnit", Schema.INT32_SCHEMA);
      schemaBuilder.field("rewardEventName", Schema.STRING_SCHEMA);
      schemaBuilder.field("numberOfRewardPointsPerUnit", Schema.INT32_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<Tier> serde = new ConnectSerde<Tier>(schema, false, Tier.class, Tier::pack, Tier::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<Tier> serde() { return serde; }

    /*****************************************
     *
     *  data
     *
     *****************************************/

    private String tierName = null;
    private int statusPointLevel = 0;
    private String statusEventName = null;
    private int numberOfStatusPointsPerUnit = 0;
    private String rewardEventName = null;
    private int numberOfRewardPointsPerUnit = 0;


    /*****************************************
     *
     *  accessors
     *
     *****************************************/

    public String getTierName() { return tierName; }
    public int getStatusPointLevel() { return statusPointLevel; }
    public String getStatusEventName() { return statusEventName; }
    public int getNumberOfStatusPointsPerUnit() { return numberOfStatusPointsPerUnit; }
    public String getRewardEventName() { return rewardEventName; }
    public int getNumberOfRewardPointsPerUnit() { return numberOfRewardPointsPerUnit; }


    /*****************************************
     *
     *  constructor -- unpack
     *
     *****************************************/

    public Tier(String tierName, int statusPointLevel, String statusEventName, int numberOfStatusPointsPerUnit, String rewardEventName, int numberOfRewardPointsPerUnit)
    {
      this.tierName = tierName;
      this.statusPointLevel = statusPointLevel;
      this.statusEventName = statusEventName;
      this.numberOfStatusPointsPerUnit = numberOfStatusPointsPerUnit;
      this.rewardEventName = rewardEventName;
      this.numberOfRewardPointsPerUnit = numberOfRewardPointsPerUnit;
    }

    /*****************************************
     *
     *  pack
     *
     *****************************************/

    public static Object pack(Object value)
    {
      Tier tier = (Tier) value;
      Struct struct = new Struct(schema);
      struct.put("tierName", tier.getTierName());
      struct.put("statusPointLevel", tier.getStatusPointLevel());
      struct.put("statusEventName", tier.getStatusEventName());
      struct.put("numberOfStatusPointsPerUnit", tier.getNumberOfStatusPointsPerUnit());
      struct.put("rewardEventName", tier.getRewardEventName());
      struct.put("numberOfRewardPointsPerUnit", tier.getNumberOfRewardPointsPerUnit());
      return struct;
    }

    /*****************************************
     *
     *  unpack
     *
     *****************************************/

    public static Tier unpack(SchemaAndValue schemaAndValue)
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
      String tierName = valueStruct.getString("tierName");
      int statusPointLevel = valueStruct.getInt32("statusPointLevel");
      String statusEventName = valueStruct.getString("statusEventName");
      int numberOfStatusPointsPerUnit = valueStruct.getInt32("numberOfStatusPointsPerUnit");
      String rewardEventName = valueStruct.getString("rewardEventName");
      int numberOfRewardPointsPerUnit = valueStruct.getInt32("numberOfRewardPointsPerUnit");

      //
      //  return
      //

      return new Tier(tierName, statusPointLevel, statusEventName, numberOfStatusPointsPerUnit, rewardEventName, numberOfRewardPointsPerUnit);
    }

    /*****************************************
     *
     *  constructor -- JSON
     *
     *****************************************/

    public Tier(JSONObject jsonRoot) throws GUIManagerException
    {

      /*****************************************
       *
       *  attributes
       *
       *****************************************/
      this.tierName = JSONUtilities.decodeString(jsonRoot, "tierName", true);
      this.statusPointLevel = JSONUtilities.decodeInteger(jsonRoot, "statusPointLevel", true);
      this.statusEventName = JSONUtilities.decodeString(jsonRoot, "statusEventName", true);
      this.numberOfStatusPointsPerUnit = JSONUtilities.decodeInteger(jsonRoot, "numberOfStatusPointsPerUnit", true);
      this.rewardEventName = JSONUtilities.decodeString(jsonRoot, "rewardEventName", true);
      this.numberOfRewardPointsPerUnit = JSONUtilities.decodeInteger(jsonRoot, "numberOfRewardPointsPerUnit", true);
      
    }
  }
}
