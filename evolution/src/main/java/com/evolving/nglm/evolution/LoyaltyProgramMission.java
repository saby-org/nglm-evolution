/*****************************************************************************
 *
 *  LoyaltyProgramMission.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collections;
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
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

@GUIDependencyDef(objectType = "loyaltyprogrammission", serviceClass = LoyaltyProgramService.class, dependencies = { "catalogcharacteristic", "workflow"})
public class LoyaltyProgramMission extends LoyaltyProgram
{
  
  //
  //  LoyaltyProgramMissionEventInfos
  //

  public enum MissionSchedule
  {
    FIXDURATION("FIXDURATION"),
    FIXENDDATE("FIXENDDATE"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private MissionSchedule(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static MissionSchedule fromExternalRepresentation(String externalRepresentation) { for (MissionSchedule enumeratedValue : MissionSchedule.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
  //
  //  LoyaltyProgramMissionEventInfos
  //

  public enum LoyaltyProgramMissionEventInfos
  {
    ENTERING("entering"),
    OLD_STEP("oldStep"),
    NEW_STEP("newStep"),
    LEAVING("leaving"),
    STEP_UPDATE_TYPE("stepUpdateType"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private LoyaltyProgramMissionEventInfos(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static LoyaltyProgramMissionEventInfos fromExternalRepresentation(String externalRepresentation) { for (LoyaltyProgramMissionEventInfos enumeratedValue : LoyaltyProgramMissionEventInfos.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  // LoyaltyProgramStepChange
  //

  public enum LoyaltyProgramStepChange
  {
    Optin("opt-in"), Optout("opt-out"), Upgrade("upgrade"), Downgrade("downgrade"), NoChange("nochange"), Unknown("(unknown)");
    private String externalRepresentation;
    private LoyaltyProgramStepChange(String externalRepresentation)
    {
      this.externalRepresentation = externalRepresentation;
    }
    public String getExternalRepresentation()
    {
      return externalRepresentation;
    }
    public static LoyaltyProgramStepChange fromExternalRepresentation(String externalRepresentation)
    {
      for (LoyaltyProgramStepChange enumeratedValue : LoyaltyProgramStepChange.values())
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
      schemaBuilder.name("loyalty_program_mission");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(LoyaltyProgram.commonSchema().version(), 1));
      for (Field field : LoyaltyProgram.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("proportionalProgression", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("scheduleType", Schema.STRING_SCHEMA);
      schemaBuilder.field("entryStartDate", Timestamp.builder().schema());
      schemaBuilder.field("entryEndDate", Timestamp.builder().schema());
      schemaBuilder.field("duration", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("createContest", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("steps", SchemaBuilder.array(MissionStep.schema()).schema());
      schema = schemaBuilder.build();
    };

  //
  // serde
  //

  private static ConnectSerde<LoyaltyProgramMission> serde = new ConnectSerde<LoyaltyProgramMission>(schema, false, LoyaltyProgramMission.class, LoyaltyProgramMission::pack, LoyaltyProgramMission::unpack);

  //
  // accessor
  //

  public static Schema schema()
  {
    return schema;
  }

  public static ConnectSerde<LoyaltyProgramMission> serde()
  {
    return serde;
  }
  
  //
  // constants
  //

  public final static String CRITERION_FIELD_NAME_OLD_PREFIX = "missionLoyaltyProgramChange.old.";
  public final static String CRITERION_FIELD_NAME_NEW_PREFIX = "missionLoyaltyProgramChange.new.";
  public final static String CRITERION_FIELD_NAME_IS_UPDATED_PREFIX = "missionLoyaltyProgramChange.isupdated.";

  /*****************************************
   *
   * data
   *
   *****************************************/

  private boolean proportionalProgression;
  private String scheduleType;
  private Date entryStartDate;
  private Date entryEndDate;
  private Integer duration;
  private boolean createContest;
  private List<MissionStep> steps = null;

  /*****************************************
   *
   * accessors
   *
   *****************************************/

  public boolean getProportionalProgression()
  {
    return proportionalProgression;
  }
  public String getScheduleType()
  {
    return scheduleType;
  }

  public Date getEntryStartDate()
  {
    return entryStartDate;
  }

  public Date getEntryEndDate()
  {
    return entryEndDate;
  }

  public Integer getDuration()
  {
    return duration;
  }
  public boolean getCreateContest()
  {
    return createContest;
  }
  
  public List<MissionStep> getSteps()
  {
    return steps;
  }
  
  public MissionStep getFirstStep()
  {
    MissionStep result = null;
    if (steps != null && !steps.isEmpty())
      {
        Collections.sort(steps);
        result = steps.get(0);
      }
    return result;
  }
  
  public MissionStep getLastStep()
  {
    MissionStep result = null;
    if (steps != null && !steps.isEmpty())
      {
        Collections.sort(steps, Collections.reverseOrder());
        result = steps.get(0);
      }
    return result;
  }
  
  /*****************************************
  *
  * getStep
  *
  *****************************************/
 
 public MissionStep getStep(String stepName)
 {
   MissionStep missionStep = null;
   if (stepName == null) return missionStep;
   for (MissionStep step : steps)
     {
       if (stepName.equals(step.getStepName()))
         {
           missionStep = step;
           break;
         }
     }
   return missionStep;
 }

  /*****************************************
   *
   * constructor -- JSON
   *
   *****************************************/

  public LoyaltyProgramMission(JSONObject jsonRoot, long epoch, GUIManagedObject existingLoyaltyProgramUnchecked, CatalogCharacteristicService catalogCharacteristicService, int tenantID) throws GUIManagerException
  {
    /*****************************************
     *
     * super
     *
     *****************************************/

    super(jsonRoot, GUIManagedObjectType.LoyaltyProgramMission, epoch, existingLoyaltyProgramUnchecked, catalogCharacteristicService, tenantID);

    /*****************************************
     *
     * existingLoyaltyProgramMission
     *
     *****************************************/

    LoyaltyProgramMission existingLoyaltyProgramMission = (existingLoyaltyProgramUnchecked != null && existingLoyaltyProgramUnchecked instanceof LoyaltyProgramMission) ? (LoyaltyProgramMission) existingLoyaltyProgramUnchecked : null;

    /*****************************************
     *
     * attributes
     *
     *****************************************/

    this.proportionalProgression = JSONUtilities.decodeBoolean(jsonRoot, "proportionalProgression", Boolean.TRUE);
    if (!proportionalProgression) throw new GUIManagerException("currently only proportionalProgression is supported", "proportionalProgression");
    this.scheduleType = JSONUtilities.decodeString(jsonRoot, "scheduleType", true);
    this.entryStartDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "entryStartDate", true));
    this.entryEndDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "entryEndDate", true));
    this.duration = JSONUtilities.decodeInteger(jsonRoot, "duration", MissionSchedule.FIXDURATION == MissionSchedule.fromExternalRepresentation(scheduleType));
    this.createContest = JSONUtilities.decodeBoolean(jsonRoot, "createContest", Boolean.FALSE);
    this.steps = decodeLoyaltyProgramSteps(JSONUtilities.decodeJSONArray(jsonRoot, "steps", true), proportionalProgression);

    /*****************************************
     *
     * epoch
     *
     *****************************************/

    if (epochChanged(existingLoyaltyProgramMission))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
   *
   * constructor -- unpack
   *
   *****************************************/

  public LoyaltyProgramMission(SchemaAndValue schemaAndValue, boolean proportionalProgression, String scheduleType, Date entryStartDate, Date entryEndDate, Integer duration, boolean createContest, List<MissionStep> steps)
  {
    super(schemaAndValue);
    this.proportionalProgression = proportionalProgression;
    this.scheduleType = scheduleType;
    this.entryStartDate = entryStartDate;
    this.entryEndDate = entryEndDate;
    this.duration = duration;
    this.createContest = createContest;
    this.steps = steps;
  }

  /*****************************************
   *
   * pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramMission loyaltyProgramMission = (LoyaltyProgramMission) value;
    Struct struct = new Struct(schema);
    LoyaltyProgram.packCommon(struct, loyaltyProgramMission);
    struct.put("proportionalProgression", loyaltyProgramMission.getProportionalProgression());
    struct.put("scheduleType", loyaltyProgramMission.getScheduleType());
    struct.put("entryStartDate", loyaltyProgramMission.getEntryStartDate());
    struct.put("entryEndDate", loyaltyProgramMission.getEntryEndDate());
    struct.put("duration", loyaltyProgramMission.getDuration());
    struct.put("createContest", loyaltyProgramMission.getCreateContest());
    struct.put("steps", packLoyaltyProgramSteps(loyaltyProgramMission.getSteps()));
    return struct;
  }

  /****************************************
   *
   * packLoyaltyProgramSteps
   *
   ****************************************/

  private static List<Object> packLoyaltyProgramSteps(List<MissionStep> steps)
  {
    List<Object> result = new ArrayList<Object>();
    for (MissionStep step : steps)
      {
        result.add(MissionStep.pack(step));
      }
    return result;
  }

  /*****************************************
   *
   * unpack
   *
   *****************************************/

  public static LoyaltyProgramMission unpack(SchemaAndValue schemaAndValue)
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
    boolean proportionalProgression = valueStruct.getBoolean("proportionalProgression");
    String scheduleType = valueStruct.getString("scheduleType");
    Date entryStartDate = (Date) valueStruct.get("entryStartDate");
    Date entryEndDate = (Date) valueStruct.get("entryEndDate");
    Integer duration = valueStruct.getInt32("duration");
    boolean createContest = valueStruct.getBoolean("createContest");
    List<MissionStep> steps = unpackLoyaltyProgramTiers(schema.field("steps").schema(), valueStruct.get("steps"));

    //
    // return
    //

    return new LoyaltyProgramMission(schemaAndValue, proportionalProgression, scheduleType, entryStartDate, entryEndDate, duration, createContest, steps);
  }

  /*****************************************
   *
   * unpackLoyaltyProgramSteps
   *
   *****************************************/

  private static List<MissionStep> unpackLoyaltyProgramTiers(Schema schema, Object value)
  {
    //
    // get schema for LoyaltyProgramSteps
    //

    Schema propertySchema = schema.valueSchema();

    //
    // unpack
    //

    List<MissionStep> result = new ArrayList<MissionStep>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(MissionStep.unpack(new SchemaAndValue(propertySchema, property)));
      }

    //
    // return
    //

    return result;
  }

  /*****************************************
   *
   * decodeLoyaltyProgramSteps
   *
   *****************************************/

  private List<MissionStep> decodeLoyaltyProgramSteps(JSONArray jsonArray, boolean proportionalProgression) throws GUIManagerException
  {
    List<MissionStep> result = new ArrayList<MissionStep>();
    if (jsonArray != null)
      {
        for (int i = 0; i < jsonArray.size(); i++)
          {
            result.add(new MissionStep((JSONObject) jsonArray.get(i), proportionalProgression));
          }
      }
    return result;
  }

  /*****************************************
   *
   * epochChanged
   *
   *****************************************/

  private boolean epochChanged(LoyaltyProgramMission existingLoyaltyProgramMission)
  {
    if (existingLoyaltyProgramMission != null && existingLoyaltyProgramMission.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || !Objects.equals(getGUIManagedObjectID(), existingLoyaltyProgramMission.getGUIManagedObjectID());
        epochChanged = epochChanged || !Objects.equals(getGUIManagedObjectName(), existingLoyaltyProgramMission.getGUIManagedObjectName());
        epochChanged = epochChanged || !Objects.equals(getLoyaltyProgramType(), existingLoyaltyProgramMission.getLoyaltyProgramType());
        epochChanged = epochChanged || !Objects.equals(getSteps(), existingLoyaltyProgramMission.getSteps());
        epochChanged = epochChanged || !Objects.equals(getCharacteristics(), existingLoyaltyProgramMission.getCharacteristics());
        epochChanged = epochChanged || !Objects.equals(getCreateContest(), existingLoyaltyProgramMission.getCreateContest());
        epochChanged = epochChanged || !Objects.equals(getProportionalProgression(), existingLoyaltyProgramMission.getProportionalProgression());
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
    return LoyaltyProgramType.MISSION == getLoyaltyProgramType();
  }
  
  public static class MissionStep implements Comparable<MissionStep>
  {
    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(MissionStep.class);

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
      schemaBuilder.name("mission_step");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("stepID", Schema.INT32_SCHEMA);
      schemaBuilder.field("stepName", Schema.STRING_SCHEMA);
      schemaBuilder.field("completionEventName", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("progression", Schema.OPTIONAL_FLOAT64_SCHEMA);
      schemaBuilder.field("workflowStepUP", Schema.OPTIONAL_STRING_SCHEMA); //workflowStepUP
      schemaBuilder.field("workflowDaily", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("workflowCompletion", Schema.OPTIONAL_STRING_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<MissionStep> serde = new ConnectSerde<MissionStep>(schema, false, MissionStep.class, MissionStep::pack, MissionStep::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<MissionStep> serde() { return serde; }

    /*****************************************
     *
     *  data
     *
     *****************************************/

    private int stepID;
    private String stepName;
    private String completionEventName = null;
    private Double progression;
    private String workflowStepUP = null;
    private String workflowDaily = null;
    private String workflowCompletion = null;


    /*****************************************
     *
     *  accessors
     *
     *****************************************/
    
    public int getStepID()
    {
      return stepID;
    }
    public String getStepName()
    {
      return stepName;
    }
    public String getCompletionEventName()
    {
      return completionEventName;
    }
    public Double getProgression()
    {
      return progression;
    }
    public String getWorkflowStepUP()
    {
      return workflowStepUP;
    }
    public String getWorkflowDaily()
    {
      return workflowDaily;
    }
    public String getWorkflowCompletion()
    {
      return workflowCompletion;
    }

    /*****************************************
     *
     *  constructor -- unpack
     *
     *****************************************/

    public MissionStep(int stepID, String stepName, String completionEventName, Double progression, String workflowStepUP, String workflowDaily, String workflowCompletion)
    {
      this.stepID = stepID;
      this.stepName = stepName;
      this.completionEventName = completionEventName;
      this.progression = progression;
      this.workflowStepUP = workflowStepUP;
      this.workflowDaily = workflowDaily;
      this.workflowCompletion = workflowCompletion;
    }

    /*****************************************
     *
     *  pack
     *
     *****************************************/

    public static Object pack(Object value)
    {
      MissionStep steps = (MissionStep) value;
      Struct struct = new Struct(schema);
      struct.put("stepID", steps.getStepID());
      struct.put("stepName", steps.getStepName());
      struct.put("completionEventName", steps.getCompletionEventName());
      struct.put("progression", steps.getProgression());
      struct.put("workflowStepUP", steps.getWorkflowStepUP());
      struct.put("workflowDaily", steps.getWorkflowDaily());
      struct.put("workflowCompletion", steps.getWorkflowCompletion());
      return struct;
    }

    /*****************************************
     *
     *  unpack
     *
     *****************************************/

    public static MissionStep unpack(SchemaAndValue schemaAndValue)
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
      int stepID = valueStruct.getInt32("stepID");
      String stepName = valueStruct.getString("stepName");
      String completionEventName = valueStruct.getString("completionEventName");
      Double progression = valueStruct.getFloat64("progression");
      String workflowStepUP = valueStruct.getString("workflowStepUP");
      String workflowDaily = valueStruct.getString("workflowDaily");
      String workflowCompletion = valueStruct.getString("workflowCompletion");

      //
      //  return
      //

      return new MissionStep(stepID, stepName, completionEventName, progression, workflowStepUP, workflowDaily, workflowCompletion);
    }

    /*****************************************
     *
     *  constructor -- JSON
     *
     *****************************************/

    public MissionStep(JSONObject jsonRoot, boolean proportionalProgression) throws GUIManagerException
    {

      /*****************************************
       *
       *  attributes
       *
       *****************************************/
      this.stepID = JSONUtilities.decodeInteger(jsonRoot, "stepID", true);
      this.stepName = JSONUtilities.decodeString(jsonRoot, "stepName", true);
      this.completionEventName = stepID != 0 ? JSONUtilities.decodeString(jsonRoot, "completionEventName", stepID != 0) : null;
      this.progression = JSONUtilities.decodeDouble(jsonRoot, "progression", !proportionalProgression);
      this.workflowStepUP = JSONUtilities.decodeString(jsonRoot, "workflowStepUP", false);
      this.workflowDaily = JSONUtilities.decodeString(jsonRoot, "workflowDaily", false);
      this.workflowCompletion = JSONUtilities.decodeString(jsonRoot, "workflowCompletion", false);
    }
    
    /*****************************************
     *
     * changeFrom
     *
     *****************************************/
    
    public static LoyaltyProgramStepChange changeFromStepToStep(MissionStep from, MissionStep to)
    {
      if (to == null)
        {
          return LoyaltyProgramStepChange.Optout;
        }
      if (from == null)
        {
          return LoyaltyProgramStepChange.Optin;
        }

      if (to.stepID - from.stepID > 0)
        {
          return LoyaltyProgramStepChange.Upgrade;
        } 
      else if (to.stepID - from.stepID < 0)
        {
          return LoyaltyProgramStepChange.Downgrade;
        } 
      else
        {
          return LoyaltyProgramStepChange.NoChange;
        }
   }
    @Override public int compareTo(MissionStep missionStep)
    {
      return this.stepID - missionStep.getStepID();
    }
    
    /*******************************************
     * 
     *  evaluateStepChangeCriteria
     * 
     *******************************************/
    
    public boolean evaluateStepChangeCriteria(SubscriberStreamEvent evolutionEvent)
    {
      boolean result = false;
      result = getCompletionEventName() == null || getCompletionEventName().isEmpty();
      if (!result)
        {
          EvolutionEngineEventDeclaration completionEvent = Deployment.getEvolutionEngineEvents().get(getCompletionEventName());
          result = completionEvent.getEventClassName().equals(evolutionEvent.getClass().getName());
        }
      return result;
    }
  }
  
  /*******************************
   * 
   * getTotalNumberOfSteps
   * 
   *******************************/
  
  public Integer getTotalNumberOfSteps(boolean skipFirstStep)
  {
    Integer result = getSteps().size();
    if (skipFirstStep) result = result - 1;
    return result;
  }
  
  /*******************************
   * 
   * getNextStep
   * 
   *******************************/

  public MissionStep getNextStep(int stepID)
  {
    MissionStep result = null;
    for (MissionStep step : getSteps())
      {
        if (step.getStepID() == (stepID + 1))
          {
            result = step;
            break;
          }
      }
    return result;
  }
  
  /*******************************
   * 
   * getNextPreviousStep
   * 
   *******************************/

  public MissionStep getNextPreviousStep(int stepID)
  {
    MissionStep result = null;
    for (MissionStep step : getSteps())
      {
        if (step.getStepID() == (stepID - 1))
          {
            result = step;
            break;
          }
      }
    return result;
  }
  
  /*******************************
   * 
   * getGUIDependencies
   * 
   *******************************/

  @Override
  public Map<String, List<String>> getGUIDependencies(int tenantID)
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    List<String> catalogcharacteristicIDs = new ArrayList<String>();
    List<String> wrkflowIDs = new ArrayList<String>();
    
    if (getCharacteristics() != null)
      {
        for (CatalogCharacteristicInstance catalogCharacteristicInstance : getCharacteristics())
          {
            catalogcharacteristicIDs.add(catalogCharacteristicInstance.getCatalogCharacteristicID());
          }
      }
    
    if (getSteps() != null)
      {
        for (MissionStep step : getSteps())
          {
            if (step.getWorkflowCompletion() != null && !step.getWorkflowCompletion().isEmpty()) wrkflowIDs.add(step.getWorkflowCompletion());
            if (step.getWorkflowDaily() != null && !step.getWorkflowDaily().isEmpty()) wrkflowIDs.add(step.getWorkflowDaily());
            if (step.getWorkflowStepUP() != null && !step.getWorkflowStepUP().isEmpty()) wrkflowIDs.add(step.getWorkflowStepUP());
          }
      }
    
    result.put("workflow", wrkflowIDs);
    result.put("catalogcharacteristic", catalogcharacteristicIDs);
    return result;
  }
}
