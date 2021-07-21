/*****************************************************************************
 *
 *  LoyaltyProgramPoints.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@GUIDependencyDef(objectType = "loyaltyProgramPoints", serviceClass = LoyaltyProgramService.class, dependencies = {"point" , "catalogcharacteristic", "workflow"})
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
    TIER_UPDATE_TYPE("tierUpdateType"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private LoyaltyProgramPointsEventInfos(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static LoyaltyProgramPointsEventInfos fromExternalRepresentation(String externalRepresentation) { for (LoyaltyProgramPointsEventInfos enumeratedValue : LoyaltyProgramPointsEventInfos.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  LoyaltyProgramTierChange
  //

  public enum LoyaltyProgramTierChange
  {
    Optin("opt-in"),
    Optout("opt-out"),
    Upgrade("upgrade"),
    Downgrade("downgrade"),
    NoChange("nochange"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private LoyaltyProgramTierChange(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static LoyaltyProgramTierChange fromExternalRepresentation(String externalRepresentation) { for (LoyaltyProgramTierChange enumeratedValue : LoyaltyProgramTierChange.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(LoyaltyProgram.commonSchema().version(),2));
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
   * getTier
   *
   *****************************************/
  
  public Tier getTier(String tierName)
  {
    if (tierName == null) return null; // optimization
    for (Tier tier : tiers)
      {
        if (tierName.equals(tier.getTierName()))
          {
            return tier;
          }
      }
    return null;
  }

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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion2(schema.version()) : null;

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

  public LoyaltyProgramPoints(JSONObject jsonRoot, long epoch, GUIManagedObject existingLoyaltyProgramUnchecked, CatalogCharacteristicService catalogCharacteristicService, int tenantID) throws GUIManagerException
  {
    /*****************************************
     *
     *  super
     *
     *****************************************/

    super(jsonRoot, GUIManagedObjectType.LoyaltyProgramPoints, epoch, existingLoyaltyProgramUnchecked, catalogCharacteristicService, tenantID);

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
    return LoyaltyProgramType.POINTS == getLoyaltyProgramType();
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
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
      schemaBuilder.field("tierName", Schema.STRING_SCHEMA);
      schemaBuilder.field("statusPointLevel", Schema.INT32_SCHEMA);
      schemaBuilder.field("statusEventName", Schema.STRING_SCHEMA);
      schemaBuilder.field("numberOfStatusPointsPerUnit", Schema.INT32_SCHEMA);
      schemaBuilder.field("rewardEventName", Schema.STRING_SCHEMA);
      schemaBuilder.field("numberOfRewardPointsPerUnit", Schema.INT32_SCHEMA);
      schemaBuilder.field("workflowChange", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("workflowReward", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("workflowStatus", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("workflowDaily", Schema.OPTIONAL_STRING_SCHEMA);
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
    private String workflowChange = null;
    private String workflowReward = null;
    private String workflowStatus = null;
    private String workflowDaily = null;


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
    public String getWorkflowChange()    {      return workflowChange;    }
    public String getWorkflowReward()    {      return workflowReward;    }
    public String getWorkflowStatus()    {      return workflowStatus;    }
    public String getWorkflowDaily()    {      return workflowDaily;    }


    /*****************************************
     *
     *  constructor -- unpack
     *
     *****************************************/

    public Tier(String tierName, int statusPointLevel, String statusEventName, int numberOfStatusPointsPerUnit, String rewardEventName, int numberOfRewardPointsPerUnit, String workflowChange, String workflowReward, String workflowStatus, String workflowDaily)
    {
      this.tierName = tierName;
      this.statusPointLevel = statusPointLevel;
      this.statusEventName = statusEventName;
      this.numberOfStatusPointsPerUnit = numberOfStatusPointsPerUnit;
      this.rewardEventName = rewardEventName;
      this.numberOfRewardPointsPerUnit = numberOfRewardPointsPerUnit;
      this.workflowChange = workflowChange;
      this.workflowReward = workflowReward;
      this.workflowStatus = workflowStatus;
      this.workflowDaily = workflowDaily;
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
      struct.put("workflowChange", tier.getWorkflowChange());
      struct.put("workflowReward", tier.getWorkflowReward());
      struct.put("workflowStatus", tier.getWorkflowStatus());
      struct.put("workflowDaily", tier.getWorkflowDaily());
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
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

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
      
      String workflowChange = schema.field("workflowChange") != null ? valueStruct.getString("workflowChange") : null;
      String workflowReward = schema.field("workflowReward") != null ? valueStruct.getString("workflowReward") : null;
      String workflowStatus = schema.field("workflowStatus") != null ? valueStruct.getString("workflowStatus") : null;
      String workflowDaily = schema.field("workflowDaily") != null ? valueStruct.getString("workflowDaily") : null;

      //
      //  return
      //

      return new Tier(tierName, statusPointLevel, statusEventName, numberOfStatusPointsPerUnit, rewardEventName, numberOfRewardPointsPerUnit, workflowChange, workflowReward, workflowStatus, workflowDaily);
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
      this.workflowChange = JSONUtilities.decodeString(jsonRoot, "workflowChange", false);
      this.workflowReward = JSONUtilities.decodeString(jsonRoot, "workflowReward", false);
      this.workflowStatus = JSONUtilities.decodeString(jsonRoot, "workflowStatus", false);
      this.workflowDaily = JSONUtilities.decodeString(jsonRoot, "workflowDaily", false);
    }

    /*****************************************
     *
     *  changeFrom
     *
     *****************************************/
    public static LoyaltyProgramTierChange changeFromTierToTier(Tier from, Tier to)
    {
      if(to == null) { return LoyaltyProgramTierChange.Optout; }
      if(from == null) { return LoyaltyProgramTierChange.Optin; }
      
      if(to.statusPointLevel - from.statusPointLevel > 0)
        {
          return LoyaltyProgramTierChange.Upgrade;
        }
      else if (to.statusPointLevel - from.statusPointLevel < 0)
        {
          return LoyaltyProgramTierChange.Downgrade;
        }
      else 
        {
          return LoyaltyProgramTierChange.NoChange;
        }
    }
    
    @Override
    public String toString()
    {
      return "Tier [" + (tierName != null ? "tierName=" + tierName + ", " : "") + "statusPointLevel=" + statusPointLevel + ", " + (statusEventName != null ? "statusEventName=" + statusEventName + ", " : "") + "numberOfStatusPointsPerUnit=" + numberOfStatusPointsPerUnit + ", " + (rewardEventName != null ? "rewardEventName=" + rewardEventName + ", " : "") + "numberOfRewardPointsPerUnit=" + numberOfRewardPointsPerUnit + ", " + (workflowChange != null ? "workflowChange=" + workflowChange + ", " : "")
          + (workflowReward != null ? "workflowReward=" + workflowReward + ", " : "") + (workflowStatus != null ? "workflowStatus=" + workflowStatus + ", " : "") + (workflowDaily != null ? "workflowDaily=" + workflowDaily : "") + "]";
    }
  }

	@Override
	public Map<String, List<String>> getGUIDependencies(int tenantID) {
		Map<String, List<String>> result = new HashMap<String, List<String>>();
		List<String> pointIDs = new ArrayList<String>();
		List<String> charIDs = new ArrayList<String>();
		List<String> wrkflowIDs = new ArrayList<String>();
		
		if (this.getRewardPointsID() != null)
			pointIDs.add(this.getRewardPointsID().replace(CommodityDeliveryManager.POINT_PREFIX, ""));
		if (this.getStatusPointsID() != null)
			pointIDs.add(this.getStatusPointsID().replace(CommodityDeliveryManager.POINT_PREFIX, ""));
		if (this.getCharacteristics() != null)
		{	for(CatalogCharacteristicInstance character:this.getCharacteristics())
				charIDs.add(character.getCatalogCharacteristicID());
		}
		if (getTiers() != null)
	      {
	        for (Tier tier : getTiers())
	          {
	            if (tier.getWorkflowChange() != null && !tier.getWorkflowChange().isEmpty()) wrkflowIDs.add(tier.getWorkflowChange());
	            if (tier.getWorkflowDaily() != null && !tier.getWorkflowDaily().isEmpty()) wrkflowIDs.add(tier.getWorkflowDaily());
	            if (tier.getWorkflowReward() != null && !tier.getWorkflowReward().isEmpty()) wrkflowIDs.add(tier.getWorkflowReward());
	            if (tier.getWorkflowStatus() != null && !tier.getWorkflowStatus().isEmpty()) wrkflowIDs.add(tier.getWorkflowStatus());
	          }
	      }
		
		result.put("workflow", wrkflowIDs);
		result.put("point", pointIDs);
		result.put("catalogcharacteristic", charIDs);
		return result;

	}
}
