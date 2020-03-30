/*****************************************************************************
 *
 *  Report.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.JourneyStatus;

public class Report extends GUIManagedObject 
{
  public static final String AVAILABLE_SCHEDULING = "availableScheduling";
  public static final String EFFECTIVE_SCHEDULING = "effectiveScheduling";
  public static final String REPORT_CLASS = "class";
  public static final String DEFAULT_REPORT_PERIOD_QUANTITY = "defaultReportPeriodQuantity";
  public static final String DEFAULT_REPORT_PERIOD_UNIT = "defaultReportPeriodUnit";

  /*****************************************
   *
   *  enum
   *
   *****************************************/

  public enum SchedulingInterval {
    MONTHLY("monthly", Deployment.getMonthlyReportCronEntryString()),
    WEEKLY("weekly", Deployment.getWeeklyReportCronEntryString()),
    DAILY("daily", Deployment.getDailyReportCronEntryString()),
    HOURLY("hourly", Deployment.getHourlyReportCronEntryString()),
    UNKNOWN("(unknown)", "");
    private String externalRepresentation;
    private String cron;
    private SchedulingInterval(String externalRepresentation, String cron) { this.externalRepresentation = externalRepresentation; this.cron = cron; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getCron() { return cron; }
    public static SchedulingInterval fromExternalRepresentation(String externalRepresentation) { for (SchedulingInterval enumeratedValue : SchedulingInterval.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
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
    schemaBuilder.name("report");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field(REPORT_CLASS, Schema.STRING_SCHEMA);
    schemaBuilder.field(EFFECTIVE_SCHEDULING, SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field(AVAILABLE_SCHEDULING, SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field(DEFAULT_REPORT_PERIOD_QUANTITY, Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field(DEFAULT_REPORT_PERIOD_UNIT, Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("jobIDs", SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Report> serde = new ConnectSerde<Report>(schema, false, Report.class, Report::pack, Report::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Report> serde() { return serde; }

  /****************************************
   *
   *  data
   *
   ****************************************/

  private String reportClass;
  // TODO : this really ought to be Set's instead of List's, for comparison purposes.
  private List<SchedulingInterval> effectiveScheduling = null;
  private List<SchedulingInterval> availableScheduling = null;
  private String defaultReportPeriodUnit;
  private Integer defaultReportPeriodQuantity;
  private List<Long> jobIDs;

  /****************************************
   *
   *  accessors
   *
   ****************************************/

  //
  //  public
  //

  public String getReportID() { return getGUIManagedObjectID(); }
  public String getName() { return getGUIManagedObjectName(); }
  public String getReportClass() { return reportClass; }
  public List<SchedulingInterval> getEffectiveScheduling() { return effectiveScheduling; }
  public List<SchedulingInterval> getAvailableScheduling() { return availableScheduling; }
  public String getDefaultReportPeriodUnit() { return defaultReportPeriodUnit; }
  public Integer getDefaultReportPeriodQuantity() { return defaultReportPeriodQuantity; }
  public List<Long> getJobIDs() { return jobIDs; }

  public void addJobID(long jobID) { this.jobIDs.add(jobID); }
  
  /*****************************************
   *
   *  constructor -- unpack
   *
   *****************************************/

  public Report(SchemaAndValue schemaAndValue, String reportClass, List<SchedulingInterval> effectiveScheduling, List<SchedulingInterval> availableScheduling, String defaultReportPeriodUnit, Integer defaultReportPeriodQuantity, List<Long> jobIDs)
  {
    super(schemaAndValue);
    this.reportClass = reportClass;
    this.effectiveScheduling = effectiveScheduling;
    this.availableScheduling = availableScheduling;
    this.defaultReportPeriodUnit = defaultReportPeriodUnit;
    this.defaultReportPeriodQuantity = defaultReportPeriodQuantity;
    this.jobIDs = jobIDs;
  }

  /*****************************************
   *
   *  constructor -- JSON
   *
   *****************************************/

  public Report(JSONObject jsonRoot, long epoch, GUIManagedObject existingReportUnchecked) throws GUIManagerException
  {
    /*****************************************
     *
     *  super
     *
     *****************************************/

    super(jsonRoot, (existingReportUnchecked != null) ? existingReportUnchecked.getEpoch() : epoch);

    /*****************************************
     *
     *  existingReport
     *
     *****************************************/

    Report existingReport = (existingReportUnchecked != null && existingReportUnchecked instanceof Report) ? (Report) existingReportUnchecked : null;

    /*****************************************
     *
     *  attributes
     *
     *****************************************/

    this.reportClass = JSONUtilities.decodeString(jsonRoot, REPORT_CLASS, false);
    if(jsonRoot.containsKey(DEFAULT_REPORT_PERIOD_QUANTITY)) {
      this.defaultReportPeriodQuantity = JSONUtilities.decodeInteger(jsonRoot, DEFAULT_REPORT_PERIOD_QUANTITY, false);
    }else {
      this.defaultReportPeriodQuantity = 0;
    }
    if(jsonRoot.containsKey(DEFAULT_REPORT_PERIOD_UNIT)) {
      this.defaultReportPeriodUnit = JSONUtilities.decodeString(jsonRoot, DEFAULT_REPORT_PERIOD_UNIT, false);
    }
    
    this.effectiveScheduling = new ArrayList<>();
    JSONArray effectiveSchedulingJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, EFFECTIVE_SCHEDULING, false);
    if (effectiveSchedulingJSONArray != null) {
      for (int i=0; i<effectiveSchedulingJSONArray.size(); i++) {
        String schedulingIntervalStr = (String) effectiveSchedulingJSONArray.get(i);
        this.effectiveScheduling.add(SchedulingInterval.fromExternalRepresentation(schedulingIntervalStr));
      }
    }

    this.availableScheduling = new ArrayList<>();
    JSONArray availableSchedulingJSON = JSONUtilities.decodeJSONArray(jsonRoot, AVAILABLE_SCHEDULING, false);
    if (availableSchedulingJSON != null) {
      for (int i=0; i<availableSchedulingJSON.size(); i++) {
        String schedulingIntervalStr = (String) availableSchedulingJSON.get(i);
        this.availableScheduling.add(SchedulingInterval.fromExternalRepresentation(schedulingIntervalStr));
      }
    }
    this.jobIDs = new ArrayList<>();

    /*****************************************
     *
     *  epoch
     *
     *****************************************/

    if (epochChanged(existingReport))
    {
      this.setEpoch(epoch);
    }
  }

  /*****************************************
   *
   *  pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    Report report = (Report) value;
    Struct struct = new Struct(schema);
    packCommon(struct, report);
    struct.put(REPORT_CLASS, report.getReportClass());
    struct.put(EFFECTIVE_SCHEDULING, packScheduling(report.getEffectiveScheduling()));
    struct.put(AVAILABLE_SCHEDULING, packScheduling(report.getAvailableScheduling()));
    struct.put(DEFAULT_REPORT_PERIOD_UNIT, report.getDefaultReportPeriodUnit());
    struct.put(DEFAULT_REPORT_PERIOD_QUANTITY, report.getDefaultReportPeriodQuantity());
    struct.put("jobIDs", packJobIDs(report.getJobIDs()));
    return struct;
  }

  /*****************************************
   *
   *  unpack
   *
   *****************************************/

  public static Report unpack(SchemaAndValue schemaAndValue)
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
    String reportClass = valueStruct.getString(REPORT_CLASS);
    String defaultReportPeriodUnit = valueStruct.getString(DEFAULT_REPORT_PERIOD_UNIT);
    Integer defaultReportPeriodQuantity = valueStruct.getInt32(DEFAULT_REPORT_PERIOD_QUANTITY);

    List<String> effectiveSchedulingStr = (List<String>) valueStruct.get(EFFECTIVE_SCHEDULING);
    List<SchedulingInterval> effectiveScheduling = new ArrayList<>();
    for (String str : effectiveSchedulingStr) {
      effectiveScheduling.add(SchedulingInterval.fromExternalRepresentation(str));
    }

    List<String> availableSchedulingStr = (List<String>) valueStruct.get(AVAILABLE_SCHEDULING);
    List<SchedulingInterval> availableScheduling = new ArrayList<>();
    for (String str : availableSchedulingStr) {
      availableScheduling.add(SchedulingInterval.fromExternalRepresentation(str));
    }
    
    List<Long> jobIDs = (schemaVersion >= 2) ? (List<Long>) valueStruct.get("jobIDs") : new ArrayList<>();

    //
    //  return
    //

    return new Report(schemaAndValue, reportClass, effectiveScheduling, availableScheduling, defaultReportPeriodUnit, defaultReportPeriodQuantity, jobIDs);
  }

  /****************************************
   *
   *  packScheduling
   *
   ****************************************/

  private static List<Object> packScheduling(List<SchedulingInterval> set)
  {
    List<Object> result = new ArrayList<>();
    for (SchedulingInterval schedulingInterval : set)
    {
      result.add(schedulingInterval.getExternalRepresentation());
    }
    return result;
  }

  /****************************************
   *
   *  packJobIDs
   *
   ****************************************/

  private static List<Object> packJobIDs(List<Long> set)
  {
    List<Object> result = new ArrayList<>();
    for (Long jobID : set)
    {
      result.add(jobID);
    }
    return result;
  }

  /*****************************************
   *
   *  epochChanged
   *
   *****************************************/

  private boolean epochChanged(Report existingReport)
  {
    if (existingReport != null && existingReport.getAccepted())
    {
      boolean epochChanged = false;
      epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingReport.getGUIManagedObjectID());
      // Note : for this to work, both List<> need to contains the same scheduling elements, in the same order. This may not be true.
      // For example, ["monthly","daily"] and ["daily","monthly"] will be considered different.
      epochChanged = epochChanged || ! Objects.equals(reportClass, existingReport.getReportClass());
      epochChanged = epochChanged || ! Objects.equals(effectiveScheduling, existingReport.getEffectiveScheduling());
      epochChanged = epochChanged || ! Objects.equals(availableScheduling, existingReport.getAvailableScheduling());
      epochChanged = epochChanged || ! Objects.equals(defaultReportPeriodQuantity, existingReport.getDefaultReportPeriodQuantity());
      epochChanged = epochChanged || ! Objects.equals(defaultReportPeriodUnit, existingReport.getDefaultReportPeriodUnit());
      epochChanged = epochChanged || ! Objects.equals(jobIDs, existingReport.getJobIDs());
      return epochChanged;
    }
    else
    {
      return true;
    }
  }

  /*****************************************
   *
   *  toString
   *
   *****************************************/

  @Override
  public String toString() {
    return "Report ["
        + (getReportID() != null ? "getReportID()=" + getReportID() + ", " : "")
        + (getName() != null ? "getName()=" + getName() + ", " : "")
        + (getReportClass() != null ? "getReportClass()=" + getReportClass() + ", " : "")
        + (getJobIDs() != null ? "getJobIDs()=" + getJobIDs() + ", " : "")
        + (getDefaultReportPeriodUnit() != null ? "getDefaultReportPeriodUnit()=" + getDefaultReportPeriodUnit() + ", " : "")
        + (getDefaultReportPeriodQuantity() != 0 ? "getDefaultReportPeriodQuantity()=" + getDefaultReportPeriodQuantity() + ", " : "")
        + (getReportClass() != null ? "getReportClass()=" + getReportClass() + ", " : "")
        + (effectiveScheduling != null ? "effectiveScheduling=" + effectiveScheduling + ", " : "")
        + (availableScheduling != null ? "availableScheduling=" + availableScheduling + ", " : "")
        + (getGUIManagedObjectID() != null ? "getGUIManagedObjectID()=" + getGUIManagedObjectID() + ", " : "")
        + (getJSONRepresentation() != null ? "getJSONRepresentation()=" + getJSONRepresentation() + ", " : "")
        + "getEpoch()=" + getEpoch() + ", "
        + (getEffectiveStartDate() != null ? "getEffectiveStartDate()="+ getEffectiveStartDate() + ", " : "")
        + (getEffectiveEndDate() != null ? "getEffectiveEndDate()=" + getEffectiveEndDate() + ", " : "")
        + "getActive()=" + getActive() + ", getAccepted()=" + getAccepted() + "]";
  }

}
