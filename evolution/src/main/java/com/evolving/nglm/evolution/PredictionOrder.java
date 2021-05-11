/*****************************************************************************
*
*  PredictionOrder.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.apache.kafka.connect.data.*;
import org.json.simple.JSONObject;

import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

public class PredictionOrder extends GUIManagedObject
{
  /*****************************************
  *
  * PredictionAlgorithm
  *
  *****************************************/
  public enum PredictionAlgorithm
  {
    ChurnPred("churn_pred"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private PredictionAlgorithm(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static PredictionAlgorithm fromExternalRepresentation(String externalRepresentation) { for (PredictionAlgorithm enumeratedValue : PredictionAlgorithm.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
  /*****************************************
  *
  * Schema
  *
  *****************************************/
  private static Schema schema = null;
  private static int currentSchemaVersion = 1;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("prediction_order");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(), currentSchemaVersion));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("description",            Schema.STRING_SCHEMA);
    schemaBuilder.field("algorithm",              Schema.STRING_SCHEMA); // PredictionAlgorithm (unique string ID)
    schemaBuilder.field("annotation",             Schema.STRING_SCHEMA); // Field to be predicted 
    schemaBuilder.field("frequency",              JourneyScheduler.serde().optionalSchema()); // re-use of JourneyScheduler strcuture for GUI
    schemaBuilder.field("targetCriteria",         SchemaBuilder.array(EvaluationCriterion.schema()).schema());
    schema = schemaBuilder.build();
  };
  
  private static ConnectSerde<PredictionOrder> serde = new ConnectSerde<PredictionOrder>(schema, false, PredictionOrder.class, PredictionOrder::pack, PredictionOrder::unpack);
  
  public static Schema schema() { return schema; }
  public static ConnectSerde<PredictionOrder> serde() { return serde; }
  
  /*****************************************
  *
  * Pack
  *
  *****************************************/
  public static Object pack(Object value)
  {
    PredictionOrder t = (PredictionOrder) value;
    
    Struct struct = new Struct(schema);
    packCommon(struct, t);
    struct.put("description", t.description);
    struct.put("algorithm", t.algorithm.getExternalRepresentation());
    struct.put("annotation", t.annotation);
    struct.put("frequency", JourneyScheduler.serde().packOptional(t.frequency));
    struct.put("targetCriteria", t.targetCriteria.stream().map(EvaluationCriterion::pack).collect(Collectors.toList()));
    return struct;
  }
  
  /*****************************************
  *
  * Unpack
  *
  *****************************************/
  public static PredictionOrder unpack(SchemaAndValue schemaAndValue)
  {
    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    // unpack
    //
    PredictionOrder result = new PredictionOrder(schemaAndValue);
    Struct valueStruct = (Struct) value;
    result.description = valueStruct.getString("description");
    result.algorithm = PredictionAlgorithm.fromExternalRepresentation(valueStruct.getString("algorithm"));
    result.annotation = valueStruct.getString("annotation");
    result.frequency = JourneyScheduler.serde().unpackOptional(new SchemaAndValue(schema.field("frequency").schema(),valueStruct.get("frequency")));
    result.targetCriteria = ((List<Object>) valueStruct.get("targetCriteria")).stream()
        .map(v -> EvaluationCriterion.unpack(new SchemaAndValue(schema.field("targetCriteria").schema().valueSchema(), v))).collect(Collectors.toList());
    
    return result;
  }
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private String description; 
  private PredictionAlgorithm algorithm;
  private String annotation;
  private JourneyScheduler frequency;
  private List<EvaluationCriterion> targetCriteria;
  

  /*****************************************
  *
  * Accessors
  *
  *****************************************/
  public String getDescription(){ return description; }
  public PredictionAlgorithm getAlgorithm() { return algorithm; }
  public String getAnnotation() { return annotation; }
  public JourneyScheduler getFrequency() { return frequency; }
  public List<EvaluationCriterion> getTargetCriteria() { return targetCriteria; }

  /*****************************************
  *
  * retrieveCronFrequency
  *
  *****************************************/
  /**
   * CRON frequency - this will not manage "every duration" field (does not fit CRON standard).
   * The "every duration" setting will need to be managed every time the job is called 
   */
  public String retrieveCronFrequency() throws GUIManagerException {
    String scheduling = this.frequency.getRunEveryUnit().toLowerCase();
    if ("day".equalsIgnoreCase(scheduling)) {
      return "0 0 * * *";
    }
    else if ("week".equalsIgnoreCase(scheduling)) {
      String weekArg = "";
      for(String day: this.frequency.getRunEveryWeekDay()) {          // GUI         return days from 1 (Sunday) to 7 (Saturday)
        String cronDay = Integer.toString(Integer.parseInt(day) - 1); // CRON format expect days from 0 (Sunday) to 6 (Saturday)
        if(!weekArg.equals("")) {
          weekArg += ","; 
        }
        weekArg += cronDay;
      }
      
      if(weekArg.equals("")) {
        log.error("invalid week day configuration {}", this.frequency.getRunEveryWeekDay());
        throw new GUIManagerException("invalid week day configuration {}", this.frequency.getRunEveryWeekDay().toString());
      }
      
      return "0 0 * * "+weekArg;
    } 
    else if ("month".equalsIgnoreCase(scheduling)) {
      String monthArg = "";
      for(String day: this.frequency.getRunEveryMonthDay()) {
        if(!monthArg.equals("")) {
          monthArg += ","; 
        }
        monthArg += day;
      }
      
      if(monthArg.equals("")) {
        log.error("invalid month day configuration {}", this.frequency.getRunEveryMonthDay());
        throw new GUIManagerException("invalid month day configuration {}", this.frequency.getRunEveryMonthDay().toString());
      }
      
      return "0 0 "+monthArg+" * *";
      
    }
    else {
      log.error("invalid scheduling {}", scheduling);
      throw new GUIManagerException("invalid scheduling {}", scheduling);
    }
  }
  
  /*****************************************
  *
  * getDuration
  *
  *****************************************/
  /**
   * return duration between start and end dates, e.g.
   * - number of days (if day frequency) 
   * - number of weeks (if week frequency)
   * - number of months (if month frequency)
   */
  public int getDuration(Date start, Date end) {
    int tenantID = this.getTenantID();
    String timeZone = Deployment.getDeployment(tenantID).getTimeZone();
    String scheduling = this.frequency.getRunEveryUnit().toLowerCase();
    if ("day".equalsIgnoreCase(scheduling)) {
      Date startDay = RLMDateUtils.truncate(start, Calendar.DATE, timeZone);
      Date endDay = RLMDateUtils.truncate(end, Calendar.DATE, timeZone);
      log.info("DEBUG: duration between "+ start.toGMTString() + " and "+ end.toGMTString() +" = " + RLMDateUtils.daysBetween(startDay, endDay, timeZone) + " days.");
      return RLMDateUtils.daysBetween(startDay, endDay, timeZone);
    }
    else if ("week".equalsIgnoreCase(scheduling)) {
      Date startWeekDay = RLMDateUtils.truncate(start, Calendar.DAY_OF_WEEK, timeZone); // /!\ WARNING, it depends on Deployment.getFirstDayOfTheWeek !
      Date endWeekDay = RLMDateUtils.truncate(end, Calendar.DAY_OF_WEEK, timeZone); // /!\ WARNING, it depends on Deployment.getFirstDayOfTheWeek !
      log.info("DEBUG: duration between "+ start.toGMTString() + " and "+ end.toGMTString() +" = " + RLMDateUtils.daysBetween(startWeekDay, endWeekDay, timeZone) + "/7 weeks.");
      return RLMDateUtils.daysBetween(startWeekDay, endWeekDay, timeZone) / 7;
    } 
    else if ("month".equalsIgnoreCase(scheduling)) {
      Date startMonth = RLMDateUtils.truncate(start, Calendar.MONTH, timeZone);
      Date endMonth = RLMDateUtils.truncate(end, Calendar.MONTH, timeZone);
      log.info("DEBUG: duration between "+ start.toGMTString() + " and "+ end.toGMTString() +" = " + RLMDateUtils.monthsBetween(startMonth, endMonth, timeZone) + " months.");
      return RLMDateUtils.monthsBetween(startMonth, endMonth, timeZone);
    }
    else {
      log.error("invalid scheduling {}", scheduling);
      throw new ServerRuntimeException("invalid scheduling "+ scheduling); // should not happen, have been validate before - runtime exception (nothing to manage)
    }
  }

  /*****************************************
  *
  * doSkip
  *
  *****************************************/
  public boolean isValidRun(Date start, Date now) {
    int duration = getDuration(start, now);
    return (duration % this.frequency.getRunEveryDuration()) == 0;
  }
  
  /*****************************************
  *
  * Constructor -- empty for unpack (private)
  *
  *****************************************/
  private PredictionOrder(SchemaAndValue schemaAndValue)
  {
    super(schemaAndValue);
  }

  /*****************************************
  *
  * Constructor -- from JSON for GUIManager (public)
  *
  *****************************************/
  public PredictionOrder(JSONObject jsonRoot, long epoch, GUIManagedObject existingPredictionOrderUnchecked, int tenantID) throws GUIManagerException, JSONUtilitiesException
  {
    super(jsonRoot, epoch, tenantID);
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    this.description = JSONUtilities.decodeString(jsonRoot, "description", "");
    this.algorithm = PredictionAlgorithm.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "algorithm", true));
    this.annotation = JSONUtilities.decodeString(jsonRoot, "annotation", "");
    
    // Special for frequency - We re-use JourneyScheduler, but because we don't need "numberOfOccurrences"
    // We fill it here, therefore, it's not required on GUI side (and moreover, it will be override by a special value (-1) no matter what)
    JSONObject jsonFrequency = JSONUtilities.decodeJSONObject(jsonRoot, "frequency", true);
    jsonFrequency.put("numberOfOccurrences", -1);
    this.frequency = new JourneyScheduler(jsonFrequency);
    
    this.targetCriteria = ((List<JSONObject>) JSONUtilities.decodeJSONArray(jsonRoot, "targetCriteria", true)).stream()
      .map(jsonObject -> {  try { return new EvaluationCriterion(jsonObject, CriterionContext.FullDynamicProfile(tenantID), tenantID); } catch (GUIManagerException e) { throw new JSONUtilitiesException(e.getMessage(), e.getCause()); } })
      .collect(Collectors.toList()); // We need to transform GUIManagerException into JSONUtilitiesException in lambda because GUIManagerException is not a RuntimeException
    // @rl: Not sure why FullDynamicProfile !
    
    /*****************************************
    *
    *  validate
    *
    *****************************************/
    this.validate();
  }
  
  /*****************************************
  *
  * validate
  *
  *****************************************/
  public void validate() throws GUIManagerException
  {
    if(algorithm.equals(PredictionAlgorithm.Unknown)) {
      throw new GUIManagerException("Unknown prediction alogirthm.", null);
    }
    String cronString = this.retrieveCronFrequency(); // test if throw error
  }
}
