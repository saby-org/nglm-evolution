/*****************************************************************************
 *
 *  LoyaltyProgramChallenge.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.LoyaltyProgramTierChange;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;

@GUIDependencyDef(objectType = "loyaltyProgramChallenge", serviceClass = LoyaltyProgramService.class, dependencies = { "catalogcharacteristic", "point" })
public class LoyaltyProgramChallenge extends LoyaltyProgram
{

  //
  // LoyaltyProgramLevelChange
  //

  public enum LoyaltyProgramLevelChange
  {
    Optin("opt-in"), Optout("opt-out"), Upgrade("upgrade"), Downgrade("downgrade"), NoChange("nochange"), Unknown("(unknown)");
    private String externalRepresentation;
    private LoyaltyProgramLevelChange(String externalRepresentation)
    {
      this.externalRepresentation = externalRepresentation;
    }

    public String getExternalRepresentation()
    {
      return externalRepresentation;
    }

    public static LoyaltyProgramLevelChange fromExternalRepresentation(String externalRepresentation)
    {
      for (LoyaltyProgramLevelChange enumeratedValue : LoyaltyProgramLevelChange.values())
        {
          if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation))
            return enumeratedValue;
        }
      return Unknown;
    }
  }

  /*****************************************
   *
   * schema
   *
   *****************************************/

  //
  // schema
  //

  private static Schema schema = null;
  static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("loyalty_program_challenge");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(LoyaltyProgram.commonSchema().version(), 1));
      for (Field field : LoyaltyProgram.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("createLeaderBoard", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("recurrence", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("recurrenceId", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("occurrenceNumber", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("scheduler", JourneyScheduler.serde().optionalSchema());
      schemaBuilder.field("lastCreatedOccurrenceNumber", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("lastOccurrenceCreateDate", Timestamp.builder().optional().schema());
      schemaBuilder.field("levels", SchemaBuilder.array(ChallengeLevel.schema()).schema());
      schemaBuilder.field("scoreID", Schema.STRING_SCHEMA);
      schema = schemaBuilder.build();
    };

  //
  // serde
  //

  private static ConnectSerde<LoyaltyProgramChallenge> serde = new ConnectSerde<LoyaltyProgramChallenge>(schema, false, LoyaltyProgramChallenge.class, LoyaltyProgramChallenge::pack, LoyaltyProgramChallenge::unpack);

  //
  // accessor
  //

  public static Schema schema()
  {
    return schema;
  }

  public static ConnectSerde<LoyaltyProgramChallenge> serde()
  {
    return serde;
  }

  /*****************************************
   *
   * data
   *
   *****************************************/

  private boolean createLeaderBoard;
  private boolean recurrence;
  private String recurrenceId;
  private Integer occurrenceNumber;
  private JourneyScheduler journeyScheduler;
  private Integer lastCreatedOccurrenceNumber;
  private Date lastOccurrenceCreateDate;
  private List<ChallengeLevel> levels = null;
  private String scoreID;

  /*****************************************
   *
   * accessors
   *
   *****************************************/

  public boolean getCreateLeaderBoard()
  {
    return createLeaderBoard;
  }
  
  public boolean getRecurrence()
  {
    return recurrence;
  }

  public String getRecurrenceId()
  {
    return recurrenceId;
  }

  public Integer getOccurrenceNumber()
  {
    return occurrenceNumber;
  }

  public JourneyScheduler getJourneyScheduler()
  {
    return journeyScheduler;
  }

  public Integer getLastCreatedOccurrenceNumber()
  {
    return lastCreatedOccurrenceNumber;
  }
  
  public Date getLastOccurrenceCreateDate()
  {
    return lastOccurrenceCreateDate;
  }

  public List<ChallengeLevel> getLevels()
  {
    return levels;
  }
  
  public String getScoreID()
  {
    return scoreID;
  }
  
  /*****************************************
  *
  * getLevel
  *
  *****************************************/
 
 public ChallengeLevel getLevel(String levelName)
 {
   ChallengeLevel challengeLevel = null;
   if (levelName == null) return challengeLevel;
   for (ChallengeLevel level : levels)
     {
       if (levelName.equals(level.getLevelName()))
         {
           challengeLevel = level;
           break;
         }
     }
   return challengeLevel;
 }

  /*****************************************
   *
   * constructor -- JSON
   *
   *****************************************/

  public LoyaltyProgramChallenge(JSONObject jsonRoot, long epoch, GUIManagedObject existingLoyaltyProgramUnchecked, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    /*****************************************
     *
     * super
     *
     *****************************************/

    super(jsonRoot, epoch, existingLoyaltyProgramUnchecked, catalogCharacteristicService);

    /*****************************************
     *
     * existingLoyaltyProgramChallenge
     *
     *****************************************/

    LoyaltyProgramChallenge existingLoyaltyProgramChallenge = (existingLoyaltyProgramUnchecked != null && existingLoyaltyProgramUnchecked instanceof LoyaltyProgramChallenge) ? (LoyaltyProgramChallenge) existingLoyaltyProgramUnchecked : null;

    /*****************************************
     *
     * attributes
     *
     *****************************************/

    this.createLeaderBoard = JSONUtilities.decodeBoolean(jsonRoot, "createLeaderBoard", Boolean.FALSE);
    this.recurrence = JSONUtilities.decodeBoolean(jsonRoot, "recurrence", Boolean.FALSE);
    this.recurrenceId = JSONUtilities.decodeString(jsonRoot, "recurrenceId", recurrence);
    this.occurrenceNumber = JSONUtilities.decodeInteger(jsonRoot, "occurrenceNumber", recurrence);
    if (recurrence) this.journeyScheduler = new JourneyScheduler(JSONUtilities.decodeJSONObject(jsonRoot, "scheduler", recurrence));
    this.lastCreatedOccurrenceNumber = JSONUtilities.decodeInteger(jsonRoot, "lastCreatedOccurrenceNumber", false);
    this.lastOccurrenceCreateDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "lastOccurrenceCreateDate", false));
    this.levels = decodeLoyaltyProgramLevels(JSONUtilities.decodeJSONArray(jsonRoot, "levels", true));
    this.scoreID = JSONUtilities.decodeString(jsonRoot, "scoreID", true);

    /*****************************************
     *
     * epoch
     *
     *****************************************/

    if (epochChanged(existingLoyaltyProgramChallenge))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
   *
   * constructor -- unpack
   *
   *****************************************/

  public LoyaltyProgramChallenge(SchemaAndValue schemaAndValue, boolean createLeaderBoard, boolean recurrence, String recurrenceId, Integer occurrenceNumber, JourneyScheduler scheduler, Integer lastCreatedOccurrenceNumber, Date lastOccurrenceCreateDate, List<ChallengeLevel> levels, String scoreID)
  {
    super(schemaAndValue);
    this.createLeaderBoard = createLeaderBoard;
    this.recurrence = recurrence;
    this.recurrenceId = recurrenceId;
    this.occurrenceNumber = occurrenceNumber;
    this.journeyScheduler = scheduler;
    this.lastCreatedOccurrenceNumber = lastCreatedOccurrenceNumber;
    this.lastOccurrenceCreateDate = lastOccurrenceCreateDate;
    this.levels = levels;
    this.scoreID = scoreID;
  }

  /*****************************************
   *
   * pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramChallenge loyaltyProgramChallenge = (LoyaltyProgramChallenge) value;
    Struct struct = new Struct(schema);
    LoyaltyProgram.packCommon(struct, loyaltyProgramChallenge);
    struct.put("createLeaderBoard", loyaltyProgramChallenge.getCreateLeaderBoard());
    struct.put("recurrence", loyaltyProgramChallenge.getRecurrence());
    struct.put("recurrenceId", loyaltyProgramChallenge.getRecurrenceId());
    struct.put("occurrenceNumber", loyaltyProgramChallenge.getOccurrenceNumber());
    struct.put("scheduler", JourneyScheduler.serde().packOptional(loyaltyProgramChallenge.getJourneyScheduler()));
    struct.put("lastCreatedOccurrenceNumber", loyaltyProgramChallenge.getLastCreatedOccurrenceNumber());
    struct.put("lastOccurrenceCreateDate", loyaltyProgramChallenge.getLastOccurrenceCreateDate());
    struct.put("levels", packLoyaltyProgramLevels(loyaltyProgramChallenge.getLevels()));
    struct.put("scoreID", loyaltyProgramChallenge.getScoreID());
    return struct;
  }

  /****************************************
   *
   * packLoyaltyProgramLevels
   *
   ****************************************/

  private static List<Object> packLoyaltyProgramLevels(List<ChallengeLevel> levels)
  {
    List<Object> result = new ArrayList<Object>();
    for (ChallengeLevel level : levels)
      {
        result.add(ChallengeLevel.pack(level));
      }
    return result;
  }

  /*****************************************
   *
   * unpack
   *
   *****************************************/

  public static LoyaltyProgramChallenge unpack(SchemaAndValue schemaAndValue)
  {
    //
    // data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion2(schema.version()) : null;

    //
    // unpack
    //

    Struct valueStruct = (Struct) value;
    boolean createLeaderBoard = valueStruct.getBoolean("createLeaderBoard");
    boolean recurrence = valueStruct.getBoolean("recurrence");
    String recurrenceId = valueStruct.getString("recurrenceId");
    Integer occurrenceNumber = valueStruct.getInt32("occurrenceNumber");
    JourneyScheduler scheduler = JourneyScheduler.serde().unpackOptional(new SchemaAndValue(schema.field("scheduler").schema(), valueStruct.get("scheduler")));
    Integer lastCreatedOccurrenceNumber = valueStruct.getInt32("lastCreatedOccurrenceNumber");
    Date lastOccurrenceCreateDate = (Date) valueStruct.get("lastOccurrenceCreateDate");
    List<ChallengeLevel> levels = unpackLoyaltyProgramTiers(schema.field("levels").schema(), valueStruct.get("levels"));
    String scoreID = valueStruct.getString("scoreID");

    //
    // return
    //

    return new LoyaltyProgramChallenge(schemaAndValue, createLeaderBoard, recurrence, recurrenceId, occurrenceNumber, scheduler, lastCreatedOccurrenceNumber, lastOccurrenceCreateDate, levels, scoreID);
  }

  /*****************************************
   *
   * unpackLoyaltyProgramLevels
   *
   *****************************************/

  private static List<ChallengeLevel> unpackLoyaltyProgramTiers(Schema schema, Object value)
  {
    //
    // get schema for LoyaltyProgramLevels
    //

    Schema propertySchema = schema.valueSchema();

    //
    // unpack
    //

    List<ChallengeLevel> result = new ArrayList<ChallengeLevel>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(ChallengeLevel.unpack(new SchemaAndValue(propertySchema, property)));
      }

    //
    // return
    //

    return result;
  }

  /*****************************************
   *
   * decodeLoyaltyProgramLevels
   *
   *****************************************/

  private List<ChallengeLevel> decodeLoyaltyProgramLevels(JSONArray jsonArray) throws GUIManagerException
  {
    List<ChallengeLevel> result = new ArrayList<ChallengeLevel>();
    if (jsonArray != null)
      {
        for (int i = 0; i < jsonArray.size(); i++)
          {
            result.add(new ChallengeLevel((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }

  /*****************************************
   *
   * epochChanged
   *
   *****************************************/

  private boolean epochChanged(LoyaltyProgramChallenge existingLoyaltyProgramChallenge)
  {
    if (existingLoyaltyProgramChallenge != null && existingLoyaltyProgramChallenge.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || !Objects.equals(getGUIManagedObjectID(), existingLoyaltyProgramChallenge.getGUIManagedObjectID());
        epochChanged = epochChanged || !Objects.equals(getGUIManagedObjectName(), existingLoyaltyProgramChallenge.getGUIManagedObjectName());
        epochChanged = epochChanged || !Objects.equals(getLoyaltyProgramType(), existingLoyaltyProgramChallenge.getLoyaltyProgramType());
        epochChanged = epochChanged || !Objects.equals(getLevels(), existingLoyaltyProgramChallenge.getLevels());
        epochChanged = epochChanged || !Objects.equals(getCharacteristics(), existingLoyaltyProgramChallenge.getCharacteristics());
        epochChanged = epochChanged || !Objects.equals(getCreateLeaderBoard(), existingLoyaltyProgramChallenge.getCreateLeaderBoard());
        epochChanged = epochChanged || !Objects.equals(getRecurrence(), existingLoyaltyProgramChallenge.getRecurrence());
        epochChanged = epochChanged || !Objects.equals(getRecurrenceId(), existingLoyaltyProgramChallenge.getRecurrenceId());
        epochChanged = epochChanged || !Objects.equals(getOccurrenceNumber(), existingLoyaltyProgramChallenge.getOccurrenceNumber());
        epochChanged = epochChanged || !Objects.equals(getJourneyScheduler(), existingLoyaltyProgramChallenge.getJourneyScheduler());
        epochChanged = epochChanged || !Objects.equals(getScoreID(), existingLoyaltyProgramChallenge.getScoreID());
        return epochChanged;
      } else
      {
        return true;
      }
  }

  /*****************************************
   *
   * validate
   *
   *****************************************/

  @Override
  public boolean validate() throws GUIManagerException
  {
    return LoyaltyProgramType.CHALLENGE == getLoyaltyProgramType();
  }
  
  public static class ChallengeLevel
  {
    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(ChallengeLevel.class);

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
      schemaBuilder.name("challenge_level");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("levelName", Schema.STRING_SCHEMA);
      schemaBuilder.field("scoreLevel", Schema.INT32_SCHEMA);
      schemaBuilder.field("scoreEventName", Schema.STRING_SCHEMA);
      schemaBuilder.field("numberOfscorePerEvent", Schema.INT32_SCHEMA);
      schemaBuilder.field("workflowChange", Schema.OPTIONAL_STRING_SCHEMA); //level up workflow
      schemaBuilder.field("workflowScore", Schema.OPTIONAL_STRING_SCHEMA); //score workflow
      schemaBuilder.field("workflowDaily", Schema.OPTIONAL_STRING_SCHEMA); //daily workflow
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<ChallengeLevel> serde = new ConnectSerde<ChallengeLevel>(schema, false, ChallengeLevel.class, ChallengeLevel::pack, ChallengeLevel::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<ChallengeLevel> serde() { return serde; }

    /*****************************************
     *
     *  data
     *
     *****************************************/

    private String levelName;
    private int scoreLevel = 0;
    private String scoreEventName;
    private int numberOfscorePerEvent = 0;
    private String workflowChange = null;
    private String workflowScore = null;
    private String workflowDaily = null;


    /*****************************************
     *
     *  accessors
     *
     *****************************************/

    public String getLevelName() { return levelName; }
    public int getScoreLevel() { return scoreLevel; }
    public String getScoreEventName() { return scoreEventName; }
    public int getNumberOfscorePerEvent() { return numberOfscorePerEvent; }
    public String getWorkflowChange()    {      return workflowChange;    }
    public String getWorkflowScore()    {      return workflowScore;    }
    public String getWorkflowDaily()    {      return workflowDaily;    }


    /*****************************************
     *
     *  constructor -- unpack
     *
     *****************************************/

    public ChallengeLevel(String levelName, int scoreLevel, String scoreEventName, int numberOfscorePerEvent, String workflowChange, String workflowScore, String workflowDaily)
    {
      this.levelName = levelName;
      this.scoreLevel = scoreLevel;
      this.scoreEventName = scoreEventName;
      this.numberOfscorePerEvent = numberOfscorePerEvent;
      this.workflowChange = workflowChange;
      this.workflowScore = workflowScore;
      this.workflowDaily = workflowDaily;
    }

    /*****************************************
     *
     *  pack
     *
     *****************************************/

    public static Object pack(Object value)
    {
      ChallengeLevel tier = (ChallengeLevel) value;
      Struct struct = new Struct(schema);
      struct.put("levelName", tier.getLevelName());
      struct.put("scoreLevel", tier.getScoreLevel());
      struct.put("scoreEventName", tier.getScoreEventName());
      struct.put("numberOfscorePerEvent", tier.getNumberOfscorePerEvent());
      struct.put("workflowChange", tier.getWorkflowChange());
      struct.put("workflowScore", tier.getWorkflowScore());
      struct.put("workflowDaily", tier.getWorkflowDaily());
      return struct;
    }

    /*****************************************
     *
     *  unpack
     *
     *****************************************/

    public static ChallengeLevel unpack(SchemaAndValue schemaAndValue)
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
      String levelName = valueStruct.getString("levelName");
      int scoreLevel = valueStruct.getInt32("scoreLevel");
      String scoreEventName = valueStruct.getString("scoreEventName");
      int numberOfscorePerEvent = valueStruct.getInt32("numberOfscorePerEvent");
      String workflowChange = valueStruct.getString("workflowChange");
      String workflowScore = valueStruct.getString("workflowScore");
      String workflowDaily = valueStruct.getString("workflowDaily");

      //
      //  return
      //

      return new ChallengeLevel(levelName, scoreLevel, scoreEventName, numberOfscorePerEvent, workflowChange, workflowScore, workflowDaily);
    }

    /*****************************************
     *
     *  constructor -- JSON
     *
     *****************************************/

    public ChallengeLevel(JSONObject jsonRoot) throws GUIManagerException
    {

      /*****************************************
       *
       *  attributes
       *
       *****************************************/
      this.levelName = JSONUtilities.decodeString(jsonRoot, "levelName", true);
      this.scoreLevel = JSONUtilities.decodeInteger(jsonRoot, "scoreLevel", true);
      this.scoreEventName = JSONUtilities.decodeString(jsonRoot, "scoreEventName", true);
      this.numberOfscorePerEvent = JSONUtilities.decodeInteger(jsonRoot, "numberOfscorePerEvent", true);
      this.workflowChange = JSONUtilities.decodeString(jsonRoot, "workflowChange", false);
      this.workflowScore = JSONUtilities.decodeString(jsonRoot, "workflowScore", false);
      this.workflowDaily = JSONUtilities.decodeString(jsonRoot, "workflowDaily", false);
    }
    
    /*****************************************
     *
     * changeFrom
     *
     *****************************************/
    
    public static LoyaltyProgramLevelChange changeFromLevelToLevel(ChallengeLevel from, ChallengeLevel to)
    {
      if (to == null)
        {
          return LoyaltyProgramLevelChange.Optout;
        }
      if (from == null)
        {
          return LoyaltyProgramLevelChange.Optin;
        }

      if (to.scoreLevel - from.scoreLevel > 0)
        {
          return LoyaltyProgramLevelChange.Upgrade;
        } 
      else if (to.scoreLevel - from.scoreLevel < 0)
        {
          return LoyaltyProgramLevelChange.Downgrade;
        } 
      else
        {
          return LoyaltyProgramLevelChange.NoChange;
        }
   }
   
  }
  
  /*******************************
   * 
   * getGUIDependencies
   * 
   *******************************/

  @Override
  public Map<String, List<String>> getGUIDependencies()
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    List<String> catalogcharacteristicIDs = new ArrayList<String>();
    List<String> scoreID = new ArrayList<String>();
    
    if (getCharacteristics() != null)
      {
        for (CatalogCharacteristicInstance catalogCharacteristicInstance : getCharacteristics())
          {
            catalogcharacteristicIDs.add(catalogCharacteristicInstance.getCatalogCharacteristicID());
          }
      }
    if (getScoreID() != null) scoreID.add(getScoreID().replace(CommodityDeliveryManager.POINT_PREFIX, ""));
    
    result.put("point", scoreID);
    result.put("catalogcharacteristic", catalogcharacteristicIDs);
    return result;
  }
}
