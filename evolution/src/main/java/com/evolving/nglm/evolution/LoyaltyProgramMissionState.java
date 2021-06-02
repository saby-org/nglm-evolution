/*****************************************************************************
*
*  LoyaltyProgramMissionState.java
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
    schemaBuilder.field("currentProgression", Schema.OPTIONAL_FLOAT64_SCHEMA);
    schemaBuilder.field("loyaltyProgramMissionHistory", LoyaltyProgramMissionHistory.schema());
    schemaBuilder.field("isMissionCompleted", Schema.BOOLEAN_SCHEMA);
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
  private Double currentProgression;
  private LoyaltyProgramMissionHistory loyaltyProgramMissionHistory;
  private boolean isMissionCompleted;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getStepName() { return stepName; }
  public String getPreviousStepName() { return previousStepName; }
  public Date getStepEnrollmentDate() { return stepEnrollmentDate; }
  public Double getCurrentProgression() { return currentProgression; }
  public LoyaltyProgramMissionHistory getLoyaltyProgramMissionHistory() { return loyaltyProgramMissionHistory; }
  public boolean isMissionCompleted() { return isMissionCompleted; }
  public void markAsCompleted()
  {
    this.isMissionCompleted = true;
    this.currentProgression = Double.valueOf(100.0);
  }

  //
  //  setters
  //

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public LoyaltyProgramMissionState(LoyaltyProgramType loyaltyProgramType, long loyaltyProgramEpoch, String loyaltyProgramName, String loyaltyProgramID, Date loyaltyProgramEnrollmentDate, Date loyaltyProgramExitDate, String stepName, String previousStepName, Date stepEnrollmentDate, Double currentProgression, LoyaltyProgramMissionHistory loyaltyProgramMissionHistory, boolean isMissionCompleted)
  {
    super(loyaltyProgramType, loyaltyProgramEpoch, loyaltyProgramName, loyaltyProgramID, loyaltyProgramEnrollmentDate, loyaltyProgramExitDate);
    this.stepName = stepName;
    this.previousStepName = previousStepName;
    this.stepEnrollmentDate = stepEnrollmentDate;
    this.currentProgression = currentProgression;
    this.loyaltyProgramMissionHistory = loyaltyProgramMissionHistory;
    this.isMissionCompleted = isMissionCompleted;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramMissionState(SchemaAndValue schemaAndValue, String stepName, String previousStepName, Date stepEnrollmentDate, Double currentProgression, LoyaltyProgramMissionHistory loyaltyProgramMissionHistory, boolean isMissionCompleted)
  {
    super(schemaAndValue);
    this.stepName = stepName;
    this.previousStepName = previousStepName;
    this.stepEnrollmentDate = stepEnrollmentDate;
    this.currentProgression = currentProgression;
    this.loyaltyProgramMissionHistory = loyaltyProgramMissionHistory;
    this.isMissionCompleted = isMissionCompleted;
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
    struct.put("currentProgression", loyaltyProgramMissionState.getCurrentProgression());
    struct.put("loyaltyProgramMissionHistory", LoyaltyProgramMissionHistory.serde().pack(loyaltyProgramMissionState.getLoyaltyProgramMissionHistory()));
    struct.put("isMissionCompleted", loyaltyProgramMissionState.isMissionCompleted());
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
    Double currentProgression = valueStruct.getFloat64("currentProgression");
    LoyaltyProgramMissionHistory loyaltyProgramMissionHistory = LoyaltyProgramMissionHistory.serde().unpack(new SchemaAndValue(schema.field("loyaltyProgramMissionHistory").schema(), valueStruct.get("loyaltyProgramMissionHistory")));
    boolean isMissionCompleted = valueStruct.getBoolean("isMissionCompleted");
    
    //  
    //  return
    //

    return new LoyaltyProgramMissionState(schemaAndValue, stepName, previousStepName, stepEnrollmentDate, currentProgression, loyaltyProgramMissionHistory, isMissionCompleted);
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
          }
      }
    
    switch (operation)
    {
      case Optin:
        boolean isOptInAfterOptOut = false;

        //
        // update current state
        //

        this.loyaltyProgramEpoch = loyaltyProgramEpoch;
        this.loyaltyProgramName = loyaltyProgramName;
        if (this.loyaltyProgramEnrollmentDate == null) { this.loyaltyProgramEnrollmentDate = enrollmentDate; }
        if (this.loyaltyProgramExitDate != null) 
          { 
            this.loyaltyProgramExitDate = null; 
            isOptInAfterOptOut = true; 
          }
        this.previousStepName = fromStep;
        this.stepName = toStep;
        this.stepEnrollmentDate = enrollmentDate;
        this.currentProgression = calculateCurrentProgression(toStep, loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramID, now));
        
        //
        // isOptInAfterOptOut
        //
        
        if (isOptInAfterOptOut)
          {
            //
            //  loyaltyProgramEnrollmentDate
            //
            
            this.loyaltyProgramEnrollmentDate = enrollmentDate;
            
            //
            //  loyaltyProgramMissionHistory
            //
            
            loyaltyProgramMissionHistory.clearStepHistory();
          }

        //
        // update history
        //

        loyaltyProgramMissionHistory.addStepHistory(fromStep, toStep, enrollmentDate, deliveryRequestID, loyaltyProgramStepChange);
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
        this.currentProgression = Double.valueOf(0.0);
        this.isMissionCompleted = false;

        //
        // update history
        //

        loyaltyProgramMissionHistory.addStepHistory(fromStep, toStep, enrollmentDate, deliveryRequestID, loyaltyProgramStepChange);
        break;

      default:
        break;
    }
    return loyaltyProgramStepChange;
  }
  
  
  private Double calculateCurrentProgression(String toStep, LoyaltyProgram activeLoyaltyProgram)
  {
    Double result = null;
    if (isMissionCompleted)
      {
        result = Double.valueOf(100.0);
      }
    else
      {
        LoyaltyProgramMission mission = (LoyaltyProgramMission) activeLoyaltyProgram;
        MissionStep step = mission.getStep(toStep);
        Integer currentStepID = step.getStepID();
        currentStepID = currentStepID > 0 ? currentStepID - 1 : 0;
        Integer totalSteps = mission.getTotalNumberOfSteps(true);
        result = (Double.valueOf(currentStepID) / Double.valueOf(totalSteps)) * 100;
      }
    return result;
  }
}
