/*****************************************************************************
 *
 *  LoyaltyProgramChallenge.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

@GUIDependencyDef(objectType = "loyaltyProgramChallenge", serviceClass = LoyaltyProgramService.class, dependencies = { "catalogcharacteristic" })
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
      schemaBuilder.field("challengeLevels", SchemaBuilder.array(Level.schema()).schema());
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
  private List<Level> levels = null;

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

  public List<Level> getLevels()
  {
    return levels;
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
    if (recurrence)
      this.journeyScheduler = new JourneyScheduler(JSONUtilities.decodeJSONObject(jsonRoot, "scheduler", recurrence));
    this.lastCreatedOccurrenceNumber = JSONUtilities.decodeInteger(jsonRoot, "lastCreatedOccurrenceNumber", recurrence);
    this.levels = decodeLoyaltyProgramLevels(JSONUtilities.decodeJSONArray(jsonRoot, "levels", true));

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

  public LoyaltyProgramChallenge(SchemaAndValue schemaAndValue, boolean createLeaderBoard, boolean recurrence, String recurrenceId, Integer occurrenceNumber, JourneyScheduler scheduler, Integer lastCreatedOccurrenceNumber, List<Level> levels)
  {
    super(schemaAndValue);
    this.createLeaderBoard = createLeaderBoard;
    this.recurrence = recurrence;
    this.recurrenceId = recurrenceId;
    this.occurrenceNumber = occurrenceNumber;
    this.journeyScheduler = scheduler;
    this.lastCreatedOccurrenceNumber = lastCreatedOccurrenceNumber;
    this.levels = levels;
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
    struct.put("challengeLevels", packLoyaltyProgramLevels(loyaltyProgramChallenge.getLevels()));
    return struct;
  }

  /****************************************
   *
   * packLoyaltyProgramLevels
   *
   ****************************************/

  private static List<Object> packLoyaltyProgramLevels(List<Level> levels)
  {
    List<Object> result = new ArrayList<Object>();
    for (Level level : levels)
      {
        result.add(Level.pack(level));
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
    List<Level> levels = unpackLoyaltyProgramLevels(schema.field("challengeLevels").schema(), valueStruct.get("challengeLevels"));

    //
    // return
    //

    return new LoyaltyProgramChallenge(schemaAndValue, createLeaderBoard, recurrence, recurrenceId, occurrenceNumber, scheduler, lastCreatedOccurrenceNumber, levels);
  }

  /*****************************************
   *
   * unpackLoyaltyProgramLevels
   *
   *****************************************/

  private static List<Level> unpackLoyaltyProgramLevels(Schema schema, Object value)
  {
    //
    // get schema for LoyaltyProgramLevels
    //

    Schema propertySchema = schema.valueSchema();

    //
    // unpack
    //

    List<Level> result = new ArrayList<Level>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(Level.unpack(new SchemaAndValue(propertySchema, property)));
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

  private List<Level> decodeLoyaltyProgramLevels(JSONArray jsonArray) throws GUIManagerException
  {
    List<Level> result = new ArrayList<Level>();
    if (jsonArray != null)
      {
        for (int i = 0; i < jsonArray.size(); i++)
          {
            result.add(new Level((JSONObject) jsonArray.get(i)));
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
    return true;
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
    if (getCharacteristics() != null)
      {
        for (CatalogCharacteristicInstance catalogCharacteristicInstance : getCharacteristics())
          {
            catalogcharacteristicIDs.add(catalogCharacteristicInstance.getCatalogCharacteristicID());
          }
      }
    result.put("catalogcharacteristic", catalogcharacteristicIDs);
    return result;
  }
  
  /*******************************
   * 
   * Level
   * 
   *******************************/
  
  public static class Level
  {
    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(Level.class);

    /*****************************************
     *
     *  schema
     *
     *****************************************/
    
    private static Schema schema = null;
    static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("challenge_level");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("levelName", Schema.STRING_SCHEMA);
      schemaBuilder.field("scoreLevel", Schema.INT32_SCHEMA);
      schemaBuilder.field("scoreEvent", Schema.STRING_SCHEMA);
      schemaBuilder.field("numberOfScorePerEvent", Schema.INT32_SCHEMA);
      schemaBuilder.field("levelUpAction", Schema.OPTIONAL_STRING_SCHEMA);
      schema = schemaBuilder.build();
    };
    
    //
    //  serde
    //

    private static ConnectSerde<Level> serde = new ConnectSerde<Level>(schema, false, Level.class, Level::pack, Level::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<Level> serde() { return serde; }

    /*****************************************
     *
     * data
     *
     *****************************************/

    private String levelName;
    private int scoreLevel = 0;
    private String scoreEvent;
    private int numberOfScorePerEvent = 0;
    private String levelUpAction;

    /*****************************************
     *
     * accessors
     *
     *****************************************/
    
    public String getLevelName() { return levelName; }
    public Integer getScoreLevel() { return scoreLevel; }
    public String getScoreEvent() { return scoreEvent; }
    public Integer getNumberOfScorePerEvent() { return numberOfScorePerEvent; }
    public String getLevelUpAction() { return levelUpAction; }
    
    /*****************************************
     *
     * constructor -- JSON
     *
     *****************************************/

    public Level(JSONObject jsonRoot) throws GUIManagerException
    {

      /*****************************************
       *
       * attributes
       *
       *****************************************/
      this.levelName = JSONUtilities.decodeString(jsonRoot, "levelName", true);
      this.scoreLevel = JSONUtilities.decodeInteger(jsonRoot, "scoreLevel", true);
      this.scoreEvent = JSONUtilities.decodeString(jsonRoot, "scoreEvent", true);
      this.numberOfScorePerEvent = JSONUtilities.decodeInteger(jsonRoot, "numberOfScorePerEvent", true);
      this.levelUpAction = JSONUtilities.decodeString(jsonRoot, "levelUpAction", false);
    }

    /*****************************************
     *
     * constructor -- unpack
     *
     *****************************************/

    public Level(String levelName, int scoreLevel, String scoreEvent, int numberOfScorePerEvent, String levelUpAction)
    {
      this.levelName = levelName;
      this.scoreLevel = scoreLevel;
      this.scoreEvent = scoreEvent;
      this.numberOfScorePerEvent = numberOfScorePerEvent;
      this.levelUpAction = levelUpAction;
    }

    /*****************************************
     *
     * pack
     *
     *****************************************/

    public static Object pack(Object value)
    {
      Level level = (Level) value;
      Struct struct = new Struct(schema);
      struct.put("levelName", level.getLevelName());
      struct.put("scoreLevel", level.getScoreLevel());
      struct.put("scoreEvent", level.getScoreEvent());
      struct.put("numberOfScorePerEvent", level.getNumberOfScorePerEvent());
      struct.put("levelUpAction", level.getLevelUpAction());
      return struct;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static Level unpack(SchemaAndValue schemaAndValue)
    {
      //
      // data
      //

      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

      //
      // unpack
      //

      Struct valueStruct = (Struct) value;
      String levelName = valueStruct.getString("levelName");
      int scoreLevel = valueStruct.getInt32("scoreLevel");
      String scoreEvent = valueStruct.getString("scoreEvent");
      int numberOfScorePerEvent = valueStruct.getInt32("numberOfScorePerEvent");
      String levelUpAction = valueStruct.getString("levelUpAction");

      //
      // return
      //

      return new Level(levelName, scoreLevel, scoreEvent, numberOfScorePerEvent, levelUpAction);
    }
    
  }

}
