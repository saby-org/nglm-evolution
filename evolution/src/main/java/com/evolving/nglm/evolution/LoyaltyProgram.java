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

public class LoyaltyProgram extends GUIManagedObject
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
    schemaBuilder.name("loyalty_program");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("programType", Schema.STRING_SCHEMA);
    schemaBuilder.field("rewardPointID", Schema.STRING_SCHEMA);
    schemaBuilder.field("statusPointID", Schema.STRING_SCHEMA);
    schemaBuilder.field("tiers", SchemaBuilder.array(Tiers.schema()).schema());
    schemaBuilder.field("characteristics", SchemaBuilder.array(CatalogCharacteristicInstance.schema()).optional().schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<LoyaltyProgram> serde = new ConnectSerde<LoyaltyProgram>(schema, false, LoyaltyProgram.class, LoyaltyProgram::pack, LoyaltyProgram::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<LoyaltyProgram> serde() { return serde; }

  /*****************************************
   *
   *  data
   *
   *****************************************/

  private String programType = null;
  private String rewardPointID = null;
  private String statusPointID = null;
  private Set<Tiers> tiers = null;
  private Set<CatalogCharacteristicInstance> characteristics = null;

  /*****************************************
   *
   *  accessors
   *
   *****************************************/

  public String getProgramType() { return programType; }
  public String getRewardPointsID() { return rewardPointID; }
  public String getStatusPointsID() { return statusPointID; }
  public Set<Tiers> getTiers() { return tiers; }
  public Set<CatalogCharacteristicInstance> getCharacteristics() { return characteristics; }

  /*****************************************
   *
   *  constructor -- unpack
   *
   *****************************************/

  public LoyaltyProgram(SchemaAndValue schemaAndValue, String programType, String rewardPointsID, String statusPointsID, Set<Tiers> tiers, Set<CatalogCharacteristicInstance> characteristics)
  {
    super(schemaAndValue);
    this.programType = programType;
    this.rewardPointID = rewardPointsID;
    this.statusPointID = statusPointsID;
    this.tiers = tiers;
    this.characteristics = characteristics;
  }

  /*****************************************
   *
   *  pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgram loyaltyProgram = (LoyaltyProgram) value;
    Struct struct = new Struct(schema);
    packCommon(struct, loyaltyProgram);
    struct.put("programType", loyaltyProgram.getProgramType());
    struct.put("rewardPointID", loyaltyProgram.getRewardPointsID());
    struct.put("statusPointID", loyaltyProgram.getStatusPointsID());
    struct.put("tiers", packLoyaltyProgramTiers(loyaltyProgram.getTiers()));
    struct.put("characteristics", packLoyaltyProgramCharacteristics(loyaltyProgram.getCharacteristics()));
    return struct;
  }
  
  /****************************************
  *
  *  packCatalogCharacteristics
  *
  ****************************************/

  private static List<Object> packLoyaltyProgramCharacteristics(Set<CatalogCharacteristicInstance> catalogCharacteristics)
  {
    List<Object> result = new ArrayList<Object>();
    for (CatalogCharacteristicInstance catalogCharacteristic : catalogCharacteristics)
      {
        result.add(CatalogCharacteristicInstance.pack(catalogCharacteristic));
      }
    return result;
  }
  
  /****************************************
  *
  *  packLoyaltyProgramTiers
  *
  ****************************************/

  private static List<Object> packLoyaltyProgramTiers(Set<Tiers> tiers)
  {
    List<Object> result = new ArrayList<Object>();
    for (Tiers tier : tiers)
      {
        result.add(Tiers.pack(tier));
      }
    return result;
  }

  /*****************************************
   *
   *  unpack
   *
   *****************************************/

  public static LoyaltyProgram unpack(SchemaAndValue schemaAndValue)
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
    
    String programType = valueStruct.getString("programType");
    String rewardPointsID = valueStruct.getString("rewardPointID");
    String statusPointsID = valueStruct.getString("statusPointID");
    Set<Tiers> tiers = unpackLoyaltyProgramTiers(schema.field("tiers").schema(), valueStruct.get("tiers"));
    Set<CatalogCharacteristicInstance> characteristics = unpackLoyaltyProgramCharacteristics(schema.field("characteristics").schema(), valueStruct.get("characteristics"));
    //
    //  return
    //

    return new LoyaltyProgram(schemaAndValue, programType, rewardPointsID, statusPointsID, tiers, characteristics);
  }
  
  /*****************************************
  *
  *  unpackLoyaltyProgramCharacteristics
  *
  *****************************************/

  private static Set<CatalogCharacteristicInstance> unpackLoyaltyProgramCharacteristics(Schema schema, Object value)
  {
    //
    //  get schema for LoyaltyProgramCharacteristics
    //

    Schema propertySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<CatalogCharacteristicInstance> result = new HashSet<CatalogCharacteristicInstance>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(CatalogCharacteristicInstance.unpack(new SchemaAndValue(propertySchema, property)));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  unpackLoyaltyProgramTiers
  *
  *****************************************/

  private static Set<Tiers> unpackLoyaltyProgramTiers(Schema schema, Object value)
  {
    //
    //  get schema for LoyaltyProgramTiers
    //

    Schema propertySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<Tiers> result = new HashSet<Tiers>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(Tiers.unpack(new SchemaAndValue(propertySchema, property)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
   *
   *  constructor
   *
   *****************************************/

  public LoyaltyProgram(JSONObject jsonRoot, long epoch, GUIManagedObject existingLoyaltyProgramUnchecked, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    /*****************************************
     *
     *  super
     *
     *****************************************/

    super(jsonRoot, (existingLoyaltyProgramUnchecked != null) ? existingLoyaltyProgramUnchecked.getEpoch() : epoch);

    /*****************************************
     *
     *  existingLoyaltyProgram
     *
     *****************************************/

    LoyaltyProgram existingContactPolicy = (existingLoyaltyProgramUnchecked != null && existingLoyaltyProgramUnchecked instanceof LoyaltyProgram) ? (LoyaltyProgram) existingLoyaltyProgramUnchecked : null;

    /*****************************************
     *
     *  attributes
     *
     *****************************************/
    this.programType = JSONUtilities.decodeString(jsonRoot, "programType", true);
    this.rewardPointID = JSONUtilities.decodeString(jsonRoot, "rewardPointID", true);
    this.statusPointID = JSONUtilities.decodeString(jsonRoot, "statusPointID", true);
    this.tiers = decodeLoyaltyProgramTiers(JSONUtilities.decodeJSONArray(jsonRoot, "tiers", true));
    this.characteristics = decodeLoyaltyProgramCharacteristics(JSONUtilities.decodeJSONArray(jsonRoot, "characteristics", false), catalogCharacteristicService);
    
    
    /*****************************************
     *
     *  epoch
     *
     *****************************************/

    if (epochChanged(existingContactPolicy))
      {
        this.setEpoch(epoch);
      }
  }
  
  /*****************************************
  *
  *  decodeCatalogCharacteristics
  *
  *****************************************/

  private Set<CatalogCharacteristicInstance> decodeLoyaltyProgramCharacteristics(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    Set<CatalogCharacteristicInstance> result = new HashSet<CatalogCharacteristicInstance>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new CatalogCharacteristicInstance((JSONObject) jsonArray.get(i), catalogCharacteristicService));
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  decodeLoyaltyProgramTiers
  *
  *****************************************/

  private Set<Tiers> decodeLoyaltyProgramTiers(JSONArray jsonArray) throws GUIManagerException
  {
    Set<Tiers> result = new HashSet<Tiers>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new Tiers((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }

  /*****************************************
   *
   *  schedule
   *
   *****************************************/

  public Date schedule(String touchPointID, Date now)
  {
    return now;
  }

  /*****************************************
   *
   *  epochChanged
   *
   *****************************************/

  private boolean epochChanged(LoyaltyProgram existingLoyaltyProgram)
  {
    if (existingLoyaltyProgram != null && existingLoyaltyProgram.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingLoyaltyProgram.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectName(), existingLoyaltyProgram.getGUIManagedObjectName());
        epochChanged = epochChanged || ! Objects.equals(getProgramType(), existingLoyaltyProgram.getProgramType());
        epochChanged = epochChanged || ! Objects.equals(getRewardPointsID(), existingLoyaltyProgram.getRewardPointsID());
        epochChanged = epochChanged || ! Objects.equals(getStatusPointsID(), existingLoyaltyProgram.getStatusPointsID());
        epochChanged = epochChanged || ! Objects.equals(getTiers(), existingLoyaltyProgram.getTiers());
        epochChanged = epochChanged || ! Objects.equals(getCharacteristics(), existingLoyaltyProgram.getCharacteristics());
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
  
  public void validate() throws GUIManagerException 
  {
    //
    //  ensure file exists if specified
    //

    if (this.programType != null)
      {
        ProgramType programType = Deployment.getProgramTypes().get(this.programType);
        if (programType == null)
          { 
            throw new GUIManagerException("unknown program type ", this.programType);
          }
      }
  }

  public static class Tiers
  {
    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(Tiers.class);

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
      schemaBuilder.name("tiers");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("tierName", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("statusPointLevel", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("statusEventName", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("numberOfStatusPointsPerUnit", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("rewardEventName", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("numberOfRewardPointsPerUnit", Schema.OPTIONAL_STRING_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<Tiers> serde = new ConnectSerde<Tiers>(schema, false, Tiers.class, Tiers::pack, Tiers::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<Tiers> serde() { return serde; }

    /*****************************************
     *
     *  data
     *
     *****************************************/

    private String tierName = null;
    private String statusPointLevel = null;
    private String statusEventName = null;
    private String numberOfStatusPointsPerUnit = null;
    private String rewardEventName = null;
    private String numberOfRewardPointsPerUnit = null;


    /*****************************************
     *
     *  accessors
     *
     *****************************************/

    public String getTierName() { return tierName; }
    public String getStatusPointLevel() { return statusPointLevel; }
    public String getStatusEventName() { return statusEventName; }
    public String getNumberOfStatusPointsPerUnit() { return numberOfStatusPointsPerUnit; }
    public String getRewardEventName() { return rewardEventName; }
    public String getNumberOfRewardPointsPerUnit() { return numberOfRewardPointsPerUnit; }


    /*****************************************
     *
     *  constructor -- unpack
     *
     *****************************************/

    public Tiers(String tierName, String statusPointLevel, String statusEventName, String numberOfStatusPointsPerUnit, String rewardEventName, String numberOfRewardPointsPerUnit)
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
      Tiers tier = (Tiers) value;
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

    public static Tiers unpack(SchemaAndValue schemaAndValue)
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
      String statusPointLevel = valueStruct.getString("statusPointLevel");
      String statusEventName = valueStruct.getString("statusEventName");
      String numberOfStatusPointsPerUnit = valueStruct.getString("numberOfStatusPointsPerUnit");
      String rewardEventName = valueStruct.getString("rewardEventName");
      String numberOfRewardPointsPerUnit = valueStruct.getString("numberOfRewardPointsPerUnit");

      //
      //  return
      //

      return new Tiers(tierName, statusPointLevel, statusEventName, numberOfStatusPointsPerUnit, rewardEventName, numberOfRewardPointsPerUnit);
    }

    /*****************************************
     *
     *  constructor -- JSON
     *
     *****************************************/

    public Tiers(JSONObject jsonRoot) throws GUIManagerException
    {

      /*****************************************
       *
       *  attributes
       *
       *****************************************/
      this.tierName = JSONUtilities.decodeString(jsonRoot, "tierName", true);
      this.statusPointLevel = JSONUtilities.decodeString(jsonRoot, "statusPointLevel", true);
      this.statusEventName = JSONUtilities.decodeString(jsonRoot, "statusEventName", true);
      this.numberOfStatusPointsPerUnit = JSONUtilities.decodeString(jsonRoot, "numberOfStatusPointsPerUnit", true);
      this.rewardEventName = JSONUtilities.decodeString(jsonRoot, "rewardEventName", true);
      this.numberOfRewardPointsPerUnit = JSONUtilities.decodeString(jsonRoot, "numberOfRewardPointsPerUnit", true);
      
    }
  }
}
