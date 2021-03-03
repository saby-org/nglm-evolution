/*****************************************************************************
*
*  LoyaltyProgramChallengeState.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramOperation;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.LoyaltyProgramChallenge.ChallengeLevel;
import com.evolving.nglm.evolution.LoyaltyProgramChallenge.LoyaltyProgramLevelChange;
import com.evolving.nglm.evolution.LoyaltyProgramChallengeHistory.LevelHistory;


public class LoyaltyProgramChallengeState extends LoyaltyProgramState
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
    schemaBuilder.name("loyalty_program_challenge_subscriber_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(), 1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("levelName", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("previousLevelName", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("levelEnrollmentDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("scoreLevel", SchemaBuilder.int32().defaultValue(0).schema());
    schemaBuilder.field("previousPeriodScore", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("previousPeriodLevel", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("previousPeriodStartDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("loyaltyProgramChallengeHistory", LoyaltyProgramChallengeHistory.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<LoyaltyProgramChallengeState> serde = new ConnectSerde<LoyaltyProgramChallengeState>(schema, false, LoyaltyProgramChallengeState.class, LoyaltyProgramChallengeState::pack, LoyaltyProgramChallengeState::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<LoyaltyProgramChallengeState> serde() { return serde; }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String levelName;
  private String previousLevelName;
  private Date levelEnrollmentDate;
  private int scoreLevel;
  private Integer previousPeriodScore;
  private String previousPeriodLevel;
  private Date previousPeriodStartDate;
  private LoyaltyProgramChallengeHistory loyaltyProgramChallengeHistory;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getLevelName() { return levelName; }
  public String getPreviousLevelName() { return previousLevelName; }
  public Date getLevelEnrollmentDate() { return levelEnrollmentDate; }
  public int getScoreLevel() { return scoreLevel; }
  public LoyaltyProgramChallengeHistory getLoyaltyProgramChallengeHistory() { return loyaltyProgramChallengeHistory; }
  public Integer getPreviousPeriodScore() { return previousPeriodScore; }
  public String getPreviousPeriodLevel() { return previousPeriodLevel; }
  public Date getPreviousPeriodStartDate() { return previousPeriodStartDate; }

  //
  //  setters
  //

  public void setScoreLevel(int scoreLevel) { this.scoreLevel = scoreLevel; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public LoyaltyProgramChallengeState(LoyaltyProgramType loyaltyProgramType, long loyaltyProgramEpoch, String loyaltyProgramName, String loyaltyProgramID, Date loyaltyProgramEnrollmentDate, Date loyaltyProgramExitDate, String levelName, String previousLevelName, Date levelEnrollmentDate, LoyaltyProgramChallengeHistory loyaltyProgramChallengeHistory)
  {
    super(loyaltyProgramType, loyaltyProgramEpoch, loyaltyProgramName, loyaltyProgramID, loyaltyProgramEnrollmentDate, loyaltyProgramExitDate);
    this.levelName = levelName;
    this.previousLevelName = previousLevelName;
    this.levelEnrollmentDate = levelEnrollmentDate;
    this.scoreLevel = 0;
    this.loyaltyProgramChallengeHistory = loyaltyProgramChallengeHistory;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramChallengeState(SchemaAndValue schemaAndValue, String levelName, String previousLevelName, Date levelEnrollmentDate, int scoreLevel, LoyaltyProgramChallengeHistory loyaltyProgramChallengeHistory)
  {
    super(schemaAndValue);
    this.levelName = levelName;
    this.previousLevelName = previousLevelName;
    this.levelEnrollmentDate = levelEnrollmentDate;
    this.scoreLevel = scoreLevel;
    this.loyaltyProgramChallengeHistory = loyaltyProgramChallengeHistory;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramChallengeState loyaltyProgramPointsState = (LoyaltyProgramChallengeState) value;
    Struct struct = new Struct(schema);
    packCommon(struct, loyaltyProgramPointsState);
    struct.put("levelName", loyaltyProgramPointsState.getLevelName());
    struct.put("previousLevelName", loyaltyProgramPointsState.getPreviousLevelName());
    struct.put("levelEnrollmentDate", loyaltyProgramPointsState.getLevelEnrollmentDate());
    struct.put("scoreLevel", loyaltyProgramPointsState.getScoreLevel());
    struct.put("loyaltyProgramChallengeHistory", LoyaltyProgramChallengeHistory.serde().pack(loyaltyProgramPointsState.getLoyaltyProgramChallengeHistory()));
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static LoyaltyProgramChallengeState unpack(SchemaAndValue schemaAndValue)
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
    String levelName = valueStruct.getString("levelName");
    String previousLevelName = valueStruct.getString("previousLevelName");
    Date levelEnrollmentDate = (Date) valueStruct.get("levelEnrollmentDate");
    int scoreLevel = valueStruct.getInt32("scoreLevel");
    LoyaltyProgramChallengeHistory loyaltyProgramChallengeHistory = LoyaltyProgramChallengeHistory.serde().unpack(new SchemaAndValue(schema.field("loyaltyProgramChallengeHistory").schema(), valueStruct.get("loyaltyProgramChallengeHistory")));
    
    //  
    //  return
    //

    return new LoyaltyProgramChallengeState(schemaAndValue, levelName, previousLevelName, levelEnrollmentDate, scoreLevel, loyaltyProgramChallengeHistory);
  }
  
  /*****************************************
  *
  *  update
  *
  *****************************************/

  public LoyaltyProgramLevelChange update(long loyaltyProgramEpoch, LoyaltyProgramOperation operation, String loyaltyProgramName, String toLevel, Date enrollmentDate, String deliveryRequestID, LoyaltyProgramService loyaltyProgramService, Integer previousScore)
  {
    Date now = SystemTime.getCurrentTime();
    LevelHistory lastLevelEntered = null;
    
    if (loyaltyProgramChallengeHistory != null)
      {
        lastLevelEntered = loyaltyProgramChallengeHistory.getLastLevelEntered();
      }
    String fromLevel = (lastLevelEntered == null ? null : lastLevelEntered.getToLevel());

    //
    // get the level informations
    //
    
    LoyaltyProgramLevelChange loyaltyProgramLevelChange = LoyaltyProgramLevelChange.Unknown;
    Integer occouranceNumber = null;
    if (loyaltyProgramChallengeHistory != null)
      {
        String loyaltyProgramID = loyaltyProgramChallengeHistory.getLoyaltyProgramID();
        LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramID, now);
        if (loyaltyProgram instanceof LoyaltyProgramChallenge)
          {
            LoyaltyProgramChallenge loyaltyProgramChallenge = (LoyaltyProgramChallenge) loyaltyProgram;
            ChallengeLevel level = loyaltyProgramChallenge.getLevel(toLevel);
            ChallengeLevel previousLevel = loyaltyProgramChallenge.getLevel(fromLevel);
            loyaltyProgramLevelChange = ChallengeLevel.changeFromLevelToLevel(level, previousLevel);
            occouranceNumber = loyaltyProgramChallenge.getOccurrenceNumber();
          }
      }
    switch (operation)
    {
      case Optin:

        //
        // update current state
        //

        this.loyaltyProgramEpoch = loyaltyProgramEpoch;
        this.loyaltyProgramName = loyaltyProgramName;
        this.loyaltyProgramEnrollmentDate = enrollmentDate;
        if (this.loyaltyProgramExitDate != null) { this.loyaltyProgramExitDate = null; }

        this.previousLevelName = fromLevel;
        this.levelName = toLevel;
        this.levelEnrollmentDate = enrollmentDate;

        //
        // update history
        //

        if (occouranceNumber != null && occouranceNumber != 1)
          {
            //
            //  first or no occurrences
            //
            
            LoyaltyProgramChallenge loyaltyProgramChallenge = (LoyaltyProgramChallenge) loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramID, now);
            previousPeriodStartDate = loyaltyProgramChallenge.getLastOccurrenceCreateDate();
            
            //
            //  thisPeroidLevels
            //
            
            List<LevelHistory> thisPeroidLevels = loyaltyProgramChallengeHistory.getAllLevelHistoryForThisPeriod(occouranceNumber);
            List<LevelHistory> lastPeroidLevels = loyaltyProgramChallengeHistory.getAllLevelHistoryForThisPeriod(occouranceNumber-1);
            if (thisPeroidLevels == null || thisPeroidLevels.isEmpty())
              {
                //
                // period change(entry to new period)
                //
                
                this.previousPeriodLevel = fromLevel;
                this.previousPeriodScore = previousScore;
                
                if (lastPeroidLevels != null && !lastPeroidLevels.isEmpty())
                  {
                    List<LevelHistory> firstLevels = lastPeroidLevels.stream().filter(level -> level.getFromLevel().equals(loyaltyProgramChallenge.getFirstLevel().getLevelName())).collect(Collectors.toList());
                    if (firstLevels != null && !firstLevels.isEmpty())
                      {
                        this.previousPeriodStartDate = firstLevels.get(0).getTransitionDate();
                      }
                  }
                else
                  {
                    this.previousPeriodStartDate = loyaltyProgramChallenge.getEffectiveStartDate();
                  }
              }
          }
        
        loyaltyProgramChallengeHistory.addLevelHistory(fromLevel, toLevel, occouranceNumber, enrollmentDate, deliveryRequestID, loyaltyProgramLevelChange);
        break;

      case Optout:

        //
        // update current state
        //

        this.loyaltyProgramEpoch = loyaltyProgramEpoch;
        this.loyaltyProgramName = loyaltyProgramName;
        if (this.loyaltyProgramEnrollmentDate == null) { this.loyaltyProgramEnrollmentDate = enrollmentDate; }
        this.loyaltyProgramExitDate = enrollmentDate;

        this.previousLevelName = fromLevel;
        this.levelName = null;
        this.levelEnrollmentDate = enrollmentDate;

        //
        // update history
        //

        if (occouranceNumber != null && occouranceNumber != 1)
          {
            //
            //  first or no occurrences
            //
            
            LoyaltyProgramChallenge loyaltyProgramChallenge = (LoyaltyProgramChallenge) loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramID, now);
            previousPeriodStartDate = loyaltyProgramChallenge.getLastOccurrenceCreateDate();
            
            //
            //  thisPeroidLevels
            //
            
            List<LevelHistory> thisPeroidLevels = loyaltyProgramChallengeHistory.getAllLevelHistoryForThisPeriod(occouranceNumber);
            List<LevelHistory> lastPeroidLevels = loyaltyProgramChallengeHistory.getAllLevelHistoryForThisPeriod(occouranceNumber-1);
            if (thisPeroidLevels == null || thisPeroidLevels.isEmpty())
              {
                //
                // period change(entry to new period)
                //
                
                this.previousPeriodLevel = fromLevel;
                this.previousPeriodScore = previousScore;
                
                if (lastPeroidLevels != null && !lastPeroidLevels.isEmpty())
                  {
                    List<LevelHistory> firstLevels = lastPeroidLevels.stream().filter(level -> level.getFromLevel().equals(loyaltyProgramChallenge.getFirstLevel().getLevelName())).collect(Collectors.toList());
                    if (firstLevels != null && !firstLevels.isEmpty())
                      {
                        this.previousPeriodStartDate = firstLevels.get(0).getTransitionDate();
                      }
                  }
                else
                  {
                    this.previousPeriodStartDate = loyaltyProgramChallenge.getEffectiveStartDate();
                  }
              }
          }

        loyaltyProgramChallengeHistory.addLevelHistory(fromLevel, toLevel, occouranceNumber, enrollmentDate, deliveryRequestID, loyaltyProgramLevelChange);
        break;

      default:
        break;
    }
    return loyaltyProgramLevelChange;
  }
}
