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
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

@GUIDependencyDef(objectType = "loyaltyProgramMission", serviceClass = LoyaltyProgramService.class, dependencies = { "catalogcharacteristic"})
public class LoyaltyProgramMission extends LoyaltyProgram
{
  
  //
  //  LoyaltyProgramType
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
      schemaBuilder.field("createContest", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("recurrence", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("recurrenceId", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("occurrenceNumber", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("scheduler", JourneyScheduler.serde().optionalSchema());
      schemaBuilder.field("lastCreatedOccurrenceNumber", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("lastOccurrenceCreateDate", Timestamp.builder().optional().schema());
      schemaBuilder.field("previousPeriodStartDate", Timestamp.builder().optional().schema());
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
  private boolean createContest;
  private boolean recurrence;
  private String recurrenceId;
  private Integer occurrenceNumber;
  private JourneyScheduler journeyScheduler;
  private Integer lastCreatedOccurrenceNumber;
  private Date lastOccurrenceCreateDate;
  private Date previousPeriodStartDate;
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
  public boolean getCreateContest()
  {
    return createContest;
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
    return lastOccurrenceCreateDate == null ? getEffectiveStartDate() : lastOccurrenceCreateDate;
  }
  
  public Date getPreviousPeriodStartDate()
  {
    return previousPeriodStartDate;
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

    super(jsonRoot, epoch, existingLoyaltyProgramUnchecked, catalogCharacteristicService, tenantID);

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
    this.createContest = JSONUtilities.decodeBoolean(jsonRoot, "createContest", Boolean.FALSE);
    this.recurrence = JSONUtilities.decodeBoolean(jsonRoot, "recurrence", Boolean.FALSE);
    this.recurrenceId = JSONUtilities.decodeString(jsonRoot, "recurrenceId", recurrence);
    this.occurrenceNumber = JSONUtilities.decodeInteger(jsonRoot, "occurrenceNumber", recurrence);
    if (recurrence) this.journeyScheduler = new JourneyScheduler(JSONUtilities.decodeJSONObject(jsonRoot, "scheduler", recurrence));
    this.lastCreatedOccurrenceNumber = JSONUtilities.decodeInteger(jsonRoot, "lastCreatedOccurrenceNumber", false);
    this.lastOccurrenceCreateDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "lastOccurrenceCreateDate", false));
    this.previousPeriodStartDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "previousPeriodStartDate", false));
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

  public LoyaltyProgramMission(SchemaAndValue schemaAndValue, boolean proportionalProgression, boolean createContest, boolean recurrence, String recurrenceId, Integer occurrenceNumber, JourneyScheduler scheduler, Integer lastCreatedOccurrenceNumber, Date lastOccurrenceCreateDate, Date previousPeriodStartDate, List<MissionStep> steps)
  {
    super(schemaAndValue);
    this.proportionalProgression = proportionalProgression;
    this.createContest = createContest;
    this.recurrence = recurrence;
    this.recurrenceId = recurrenceId;
    this.occurrenceNumber = occurrenceNumber;
    this.journeyScheduler = scheduler;
    this.lastCreatedOccurrenceNumber = lastCreatedOccurrenceNumber;
    this.lastOccurrenceCreateDate = lastOccurrenceCreateDate;
    this.previousPeriodStartDate = previousPeriodStartDate;
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
    struct.put("createContest", loyaltyProgramMission.getCreateContest());
    struct.put("recurrence", loyaltyProgramMission.getRecurrence());
    struct.put("recurrenceId", loyaltyProgramMission.getRecurrenceId());
    struct.put("occurrenceNumber", loyaltyProgramMission.getOccurrenceNumber());
    struct.put("scheduler", JourneyScheduler.serde().packOptional(loyaltyProgramMission.getJourneyScheduler()));
    struct.put("lastCreatedOccurrenceNumber", loyaltyProgramMission.getLastCreatedOccurrenceNumber());
    struct.put("lastOccurrenceCreateDate", loyaltyProgramMission.getLastOccurrenceCreateDate());
    struct.put("previousPeriodStartDate", loyaltyProgramMission.getPreviousPeriodStartDate());
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
    boolean createContest = valueStruct.getBoolean("createContest");
    boolean recurrence = valueStruct.getBoolean("recurrence");
    String recurrenceId = valueStruct.getString("recurrenceId");
    Integer occurrenceNumber = valueStruct.getInt32("occurrenceNumber");
    JourneyScheduler scheduler = JourneyScheduler.serde().unpackOptional(new SchemaAndValue(schema.field("scheduler").schema(), valueStruct.get("scheduler")));
    Integer lastCreatedOccurrenceNumber = valueStruct.getInt32("lastCreatedOccurrenceNumber");
    Date lastOccurrenceCreateDate = (Date) valueStruct.get("lastOccurrenceCreateDate");
    Date previousPeriodStartDate = (Date) valueStruct.get("previousPeriodStartDate");
    List<MissionStep> steps = unpackLoyaltyProgramTiers(schema.field("steps").schema(), valueStruct.get("steps"));

    //
    // return
    //

    return new LoyaltyProgramMission(schemaAndValue, proportionalProgression, createContest, recurrence, recurrenceId, occurrenceNumber, scheduler, lastCreatedOccurrenceNumber, lastOccurrenceCreateDate, previousPeriodStartDate, steps);
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
        epochChanged = epochChanged || !Objects.equals(getRecurrence(), existingLoyaltyProgramMission.getRecurrence());
        epochChanged = epochChanged || !Objects.equals(getRecurrenceId(), existingLoyaltyProgramMission.getRecurrenceId());
        epochChanged = epochChanged || !Objects.equals(getOccurrenceNumber(), existingLoyaltyProgramMission.getOccurrenceNumber());
        epochChanged = epochChanged || !Objects.equals(getJourneyScheduler(), existingLoyaltyProgramMission.getJourneyScheduler());
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
      schemaBuilder.field("completionEventName", Schema.STRING_SCHEMA);
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
      this.completionEventName = JSONUtilities.decodeString(jsonRoot, "completionEventName", stepID != 0);
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
    
    @Override
    public int compareTo(MissionStep missionStep)
    {
      return this.stepID - missionStep.getStepID();
    }
   
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
}
