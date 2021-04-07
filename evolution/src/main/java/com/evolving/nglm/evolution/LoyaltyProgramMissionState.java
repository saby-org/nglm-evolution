/*****************************************************************************
*
*  LoyaltyProgramMissionState.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

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
import com.evolving.nglm.evolution.LoyaltyProgramMission.LoyaltyProgramStepChange;
import com.evolving.nglm.evolution.LoyaltyProgramMission.MissionStep;
import com.evolving.nglm.evolution.LoyaltyProgramMissionHistory.StepHistory;

public class LoyaltyProgramMissionState extends LoyaltyProgramState
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
    schemaBuilder.name("loyalty_program_mission_subscriber_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(), 1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("stepName", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("previousStepName", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("stepEnrollmentDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("previousPeriodStep", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("previousPeriodStartDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("loyaltyProgramMissionHistory", LoyaltyProgramMissionHistory.schema());
    schema = schemaBuilder.build();
  };
  
  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(LoyaltyProgramMissionState.class);

  //
  //  serde
  //

  private static ConnectSerde<LoyaltyProgramMissionState> serde = new ConnectSerde<LoyaltyProgramMissionState>(schema, false, LoyaltyProgramMissionState.class, LoyaltyProgramMissionState::pack, LoyaltyProgramMissionState::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<LoyaltyProgramMissionState> serde() { return serde; }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String stepName;
  private String previousStepName;
  private Date stepEnrollmentDate;
  private String previousPeriodStep;
  private Date previousPeriodStartDate;
  private LoyaltyProgramMissionHistory loyaltyProgramMissionHistory;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getStepName() { return stepName; }
  public String getPreviousStepName() { return previousStepName; }
  public Date getStepEnrollmentDate() { return stepEnrollmentDate; }
  public LoyaltyProgramMissionHistory getLoyaltyProgramMissionHistory() { return loyaltyProgramMissionHistory; }
  public String getPreviousPeriodStep() { return previousPeriodStep; }
  public Date getPreviousPeriodStartDate() { return previousPeriodStartDate; }

  //
  //  setters
  //

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public LoyaltyProgramMissionState(LoyaltyProgramType loyaltyProgramType, long loyaltyProgramEpoch, String loyaltyProgramName, String loyaltyProgramID, Date loyaltyProgramEnrollmentDate, Date loyaltyProgramExitDate, String stepName, String previousStepName, Date stepEnrollmentDate, LoyaltyProgramMissionHistory loyaltyProgramMissionHistory)
  {
    super(loyaltyProgramType, loyaltyProgramEpoch, loyaltyProgramName, loyaltyProgramID, loyaltyProgramEnrollmentDate, loyaltyProgramExitDate);
    this.stepName = stepName;
    this.previousStepName = previousStepName;
    this.stepEnrollmentDate = stepEnrollmentDate;
    this.loyaltyProgramMissionHistory = loyaltyProgramMissionHistory;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramMissionState(SchemaAndValue schemaAndValue, String stepName, String previousStepName, Date stepEnrollmentDate, String previousPeriodStep, Date previousPeriodStartDate, LoyaltyProgramMissionHistory loyaltyProgramMissionHistory)
  {
    super(schemaAndValue);
    this.stepName = stepName;
    this.previousStepName = previousStepName;
    this.stepEnrollmentDate = stepEnrollmentDate;
    this.previousPeriodStep = previousPeriodStep;
    this.previousPeriodStartDate = previousPeriodStartDate;
    this.loyaltyProgramMissionHistory = loyaltyProgramMissionHistory;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramMissionState loyaltyProgramMissionState = (LoyaltyProgramMissionState) value;
    Struct struct = new Struct(schema);
    packCommon(struct, loyaltyProgramMissionState);
    struct.put("stepName", loyaltyProgramMissionState.getStepName());
    struct.put("previousStepName", loyaltyProgramMissionState.getPreviousStepName());
    struct.put("stepEnrollmentDate", loyaltyProgramMissionState.getStepEnrollmentDate());
    struct.put("previousPeriodStep", loyaltyProgramMissionState.getPreviousPeriodStep());
    struct.put("previousPeriodStartDate", loyaltyProgramMissionState.getPreviousPeriodStartDate());
    struct.put("loyaltyProgramMissionHistory", LoyaltyProgramMissionHistory.serde().pack(loyaltyProgramMissionState.getLoyaltyProgramMissionHistory()));
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static LoyaltyProgramMissionState unpack(SchemaAndValue schemaAndValue)
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
    String stepName = valueStruct.getString("stepName");
    String previousStepName = valueStruct.getString("previousStepName");
    Date stepEnrollmentDate = (Date) valueStruct.get("stepEnrollmentDate");
    String previousPeriodStep = valueStruct.getString("previousPeriodStep");
    Date previousPeriodStartDate = (Date) valueStruct.get("previousPeriodStartDate");
    LoyaltyProgramMissionHistory loyaltyProgramMissionHistory = LoyaltyProgramMissionHistory.serde().unpack(new SchemaAndValue(schema.field("loyaltyProgramMissionHistory").schema(), valueStruct.get("loyaltyProgramMissionHistory")));
    
    //  
    //  return
    //

    return new LoyaltyProgramMissionState(schemaAndValue, stepName, previousStepName, stepEnrollmentDate, previousPeriodStep, previousPeriodStartDate, loyaltyProgramMissionHistory);
  }
  
  /*****************************************
  *
  *  update
  *
  *****************************************/

  public LoyaltyProgramStepChange update(long loyaltyProgramEpoch, LoyaltyProgramOperation operation, String loyaltyProgramName, String toStep, Date enrollmentDate, String deliveryRequestID, LoyaltyProgramService loyaltyProgramService)
  {
    Date now = SystemTime.getCurrentTime();
    StepHistory lastStepEntered = null;
    
    if (loyaltyProgramMissionHistory != null)
      {
        lastStepEntered = loyaltyProgramMissionHistory.getLastStepEntered();
      }
    String fromStep = (lastStepEntered == null ? null : lastStepEntered.getToStep());

    //
    // get the step informations
    //
    
    LoyaltyProgramStepChange loyaltyProgramStepChange = LoyaltyProgramStepChange.Unknown;
    Integer occouranceNumber = null;
    if (loyaltyProgramMissionHistory != null)
      {
        String loyaltyProgramID = loyaltyProgramMissionHistory.getLoyaltyProgramID();
        LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramID, now);
        if (loyaltyProgram instanceof LoyaltyProgramMission)
          {
            LoyaltyProgramMission loyaltyProgramMission = (LoyaltyProgramMission) loyaltyProgram;
            MissionStep step = loyaltyProgramMission.getStep(toStep);
            MissionStep previousStep = loyaltyProgramMission.getStep(fromStep);
            loyaltyProgramStepChange = MissionStep.changeFromStepToStep(step, previousStep);
            occouranceNumber = loyaltyProgramMission.getOccurrenceNumber();
          }
      }
    
    LoyaltyProgramMission loyaltyProgramMission = (LoyaltyProgramMission) loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramID, now);
    
    switch (operation)
    {
      case Optin:

        //
        // update current state
        //

        this.loyaltyProgramEpoch = loyaltyProgramEpoch;
        this.loyaltyProgramName = loyaltyProgramName;
        if (this.loyaltyProgramEnrollmentDate == null) { this.loyaltyProgramEnrollmentDate = enrollmentDate; }
        if (this.loyaltyProgramExitDate != null) { this.loyaltyProgramExitDate = null; }

        this.previousStepName = fromStep;
        this.stepName = toStep;
        this.stepEnrollmentDate = enrollmentDate;

        //
        // update history
        //

        if (loyaltyProgramMission.getRecurrence() && occouranceNumber != null && occouranceNumber != 1)
          {
            //
            //  first or no occurrences
            //
            
            
            previousPeriodStartDate = loyaltyProgramMission.getLastOccurrenceCreateDate();
            
            //
            //  thisPeroidSteps
            //
            
            List<StepHistory> thisPeroidSteps = loyaltyProgramMissionHistory.getAllStepHistoryForThisPeriod(occouranceNumber);
            if (thisPeroidSteps == null || thisPeroidSteps.isEmpty())
              {
                //
                // period change(entry to new period)
                //
                
                this.previousPeriodStep = fromStep;
                this.previousPeriodStartDate = loyaltyProgramMission.getPreviousPeriodStartDate();
              }
          }
        loyaltyProgramMissionHistory.addStepHistory(fromStep, toStep, occouranceNumber, enrollmentDate, deliveryRequestID, loyaltyProgramStepChange);
        break;

      case Optout:

        //
        // update current state
        //

        this.loyaltyProgramEpoch = loyaltyProgramEpoch;
        this.loyaltyProgramName = loyaltyProgramName;
        if (this.loyaltyProgramEnrollmentDate == null) { this.loyaltyProgramEnrollmentDate = enrollmentDate; }
        this.loyaltyProgramExitDate = enrollmentDate;

        this.previousStepName = fromStep;
        this.stepName = null;
        this.stepEnrollmentDate = enrollmentDate;

        //
        // update history
        //

        if (loyaltyProgramMission.getRecurrence() && occouranceNumber != null && occouranceNumber != 1)
          {
            //
            //  first or no occurrences
            //
            
            previousPeriodStartDate = loyaltyProgramMission.getLastOccurrenceCreateDate();
            
            //
            //  thisPeroidSteps
            //
            
            List<StepHistory> thisPeroidSteps = loyaltyProgramMissionHistory.getAllStepHistoryForThisPeriod(occouranceNumber);
            if (thisPeroidSteps == null || thisPeroidSteps.isEmpty())
              {
                //
                // period change(entry to new period)
                //
                
                this.previousPeriodStep = fromStep;
                this.previousPeriodStartDate = loyaltyProgramMission.getPreviousPeriodStartDate();
              }
          }

        loyaltyProgramMissionHistory.addStepHistory(fromStep, toStep, occouranceNumber, enrollmentDate, deliveryRequestID, loyaltyProgramStepChange);
        break;

      default:
        break;
    }
    return loyaltyProgramStepChange;
  }
}
