/*****************************************************************************
*
*  LoyaltyProgramChallengeState.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    schemaBuilder.field("currentScore", SchemaBuilder.int32().defaultValue(0).schema());
    schemaBuilder.field("previousPeriodScore", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("previousPeriodLevel", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("previousPeriodStartDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("lastScoreChangeDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("loyaltyProgramChallengeHistory", LoyaltyProgramChallengeHistory.schema());
    schema = schemaBuilder.build();
  };
  
  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(LoyaltyProgramChallengeState.class);

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
  private int currentScore;
  private Integer previousPeriodScore;
  private String previousPeriodLevel;
  private Date previousPeriodStartDate;
  private Date lastScoreChangeDate;
  private LoyaltyProgramChallengeHistory loyaltyProgramChallengeHistory;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getLevelName() { return levelName; }
  public String getPreviousLevelName() { return previousLevelName; }
  public Date getLevelEnrollmentDate() { return levelEnrollmentDate; }
  public int getCurrentScore() { return currentScore; }
  public LoyaltyProgramChallengeHistory getLoyaltyProgramChallengeHistory() { return loyaltyProgramChallengeHistory; }
  public Integer getPreviousPeriodScore() { return previousPeriodScore; }
  public String getPreviousPeriodLevel() { return previousPeriodLevel; }
  public Date getPreviousPeriodStartDate() { return previousPeriodStartDate; }
  public Date getLastScoreChangeDate() { return lastScoreChangeDate; }

  //
  //  setters
  //

  public void setCurrentScore(int currentScore) { this.currentScore = currentScore; }
  public void setLastScoreChangeDate(Date lastScoreChangeDate) { this.lastScoreChangeDate = lastScoreChangeDate; }

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
    this.currentScore = 0;
    this.loyaltyProgramChallengeHistory = loyaltyProgramChallengeHistory;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramChallengeState(SchemaAndValue schemaAndValue, String levelName, String previousLevelName, Date levelEnrollmentDate, int currentScore, Integer previousPeriodScore, String previousPeriodLevel, Date previousPeriodStartDate, Date lastScoreChangeDate, LoyaltyProgramChallengeHistory loyaltyProgramChallengeHistory)
  {
    super(schemaAndValue);
    this.levelName = levelName;
    this.previousLevelName = previousLevelName;
    this.levelEnrollmentDate = levelEnrollmentDate;
    this.currentScore = currentScore;
    this.previousPeriodScore = previousPeriodScore;
    this.previousPeriodLevel = previousPeriodLevel;
    this.previousPeriodStartDate = previousPeriodStartDate;
    this.lastScoreChangeDate = lastScoreChangeDate;
    this.loyaltyProgramChallengeHistory = loyaltyProgramChallengeHistory;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramChallengeState loyaltyProgramChallengeState = (LoyaltyProgramChallengeState) value;
    Struct struct = new Struct(schema);
    packCommon(struct, loyaltyProgramChallengeState);
    struct.put("levelName", loyaltyProgramChallengeState.getLevelName());
    struct.put("previousLevelName", loyaltyProgramChallengeState.getPreviousLevelName());
    struct.put("levelEnrollmentDate", loyaltyProgramChallengeState.getLevelEnrollmentDate());
    struct.put("currentScore", loyaltyProgramChallengeState.getCurrentScore());
    struct.put("previousPeriodScore", loyaltyProgramChallengeState.getPreviousPeriodScore());
    struct.put("previousPeriodLevel", loyaltyProgramChallengeState.getPreviousPeriodLevel());
    struct.put("previousPeriodStartDate", loyaltyProgramChallengeState.getPreviousPeriodStartDate());
    struct.put("lastScoreChangeDate", loyaltyProgramChallengeState.getLastScoreChangeDate());
    struct.put("loyaltyProgramChallengeHistory", LoyaltyProgramChallengeHistory.serde().pack(loyaltyProgramChallengeState.getLoyaltyProgramChallengeHistory()));
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
    int currentScore = valueStruct.getInt32("currentScore");
    Integer previousPeriodScore = valueStruct.getInt32("previousPeriodScore");
    String previousPeriodLevel = valueStruct.getString("previousPeriodLevel");
    Date previousPeriodStartDate = (Date) valueStruct.get("previousPeriodStartDate");
    Date lastScoreChangeDate = (Date) valueStruct.get("lastScoreChangeDate");
    LoyaltyProgramChallengeHistory loyaltyProgramChallengeHistory = LoyaltyProgramChallengeHistory.serde().unpack(new SchemaAndValue(schema.field("loyaltyProgramChallengeHistory").schema(), valueStruct.get("loyaltyProgramChallengeHistory")));
    
    //  
    //  return
    //

    return new LoyaltyProgramChallengeState(schemaAndValue, levelName, previousLevelName, levelEnrollmentDate, currentScore, previousPeriodScore, previousPeriodLevel, previousPeriodStartDate, lastScoreChangeDate, loyaltyProgramChallengeHistory);
  }
  
  /*****************************************
  *
  *  update
  *
  *****************************************/
  
  public LoyaltyProgramLevelChange update(long loyaltyProgramEpoch, LoyaltyProgramOperation operation, String loyaltyProgramName, String toLevel, Date enrollmentDate, String deliveryRequestID, LoyaltyProgramService loyaltyProgramService)
  {
    return update(loyaltyProgramEpoch, operation, loyaltyProgramName, toLevel, enrollmentDate, deliveryRequestID, loyaltyProgramService, false, null);
  }

  public LoyaltyProgramLevelChange update(long loyaltyProgramEpoch, LoyaltyProgramOperation operation, String loyaltyProgramName, String toLevel, Date enrollmentDate, String deliveryRequestID, LoyaltyProgramService loyaltyProgramService, boolean isPeriodChange, Integer previousScore)
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
    
    LoyaltyProgramChallenge loyaltyProgramChallenge = (LoyaltyProgramChallenge) loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramID, now);
    
    switch (operation)
    {
      case Optin:
        boolean isOptInAfterOptOut = false;
        boolean resetOnReoptin = false;

        //
        // update current state
        //

        this.loyaltyProgramEpoch = loyaltyProgramEpoch;
        this.loyaltyProgramName = loyaltyProgramName;
        if (this.loyaltyProgramEnrollmentDate == null) { this.loyaltyProgramEnrollmentDate = enrollmentDate; }
        if (this.loyaltyProgramExitDate != null) { this.loyaltyProgramExitDate = null; isOptInAfterOptOut = true; }

        this.previousLevelName = fromLevel;
        this.levelName = toLevel;
        this.levelEnrollmentDate = enrollmentDate;
        if (loyaltyProgramChallenge.getRecurrence() && previousPeriodStartDate == null) previousPeriodStartDate = loyaltyProgramChallenge.getPreviousPeriodStartDate(); // can happen if subscriber enters after 1st occur and isPeriodChange is false        
        //
        // isOptInAfterOptOut
        //

        if (isOptInAfterOptOut && resetOnReoptin)
          {
            //
            //  loyaltyProgramEnrollmentDate
            //

            this.loyaltyProgramEnrollmentDate = enrollmentDate;

            //
            //  loyaltyProgramChallengeHistory
            //

            loyaltyProgramChallengeHistory.clearLevelHistory();

            //
            //   previousPeriod
            //

            this.previousPeriodLevel = null;
            this.previousPeriodScore = null;
            this.previousPeriodStartDate = null;
          }


        //
        // update history
        //

        if (isPeriodChange)
          {
            //
            // period change(entry to new period)
            //
            
            this.previousPeriodLevel = fromLevel;
            this.previousPeriodScore = previousScore;
            this.previousPeriodStartDate = loyaltyProgramChallenge.getPreviousPeriodStartDate();
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
        this.currentScore = 0;

        //
        // update history
        //

        if (isPeriodChange)
          {
            //
            // period change(entry to new period)
            //
            
            this.previousPeriodLevel = fromLevel;
            this.previousPeriodScore = previousScore;
            this.previousPeriodStartDate = loyaltyProgramChallenge.getPreviousPeriodStartDate();
          }

        loyaltyProgramChallengeHistory.addLevelHistory(fromLevel, toLevel, occouranceNumber, enrollmentDate, deliveryRequestID, loyaltyProgramLevelChange);
        break;

      default:
        break;
    }
    return loyaltyProgramLevelChange;
  }
}
