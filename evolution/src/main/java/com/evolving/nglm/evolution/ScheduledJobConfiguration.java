package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.DeploymentJSONReader;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.evolution.datacubes.mapping.ModuleInformation.ModuleFeature;

public class ScheduledJobConfiguration
{
  /*****************************************
  *
  * Type
  *
  *****************************************/
  public enum Type
  {
    ODRDailyPreview("ODR-daily-preview"),
    ODRDailyDefinitive("ODR-daily-definitive"),
    ODRHourlyPreview("ODR-hourly-preview"),
    ODRHourlyDefinitive("ODR-hourly-definitive"),
    BDRDailyPreview("BDR-daily-preview"),
    BDRDailyDefinitive("BDR-daily-definitive"),
    BDRHourlyPreview("BDR-hourly-preview"),
    BDRHourlyDefinitive("BDR-hourly-definitive"),
    TDRDailyPreview("BDR-daily-preview"),
    TDRDailyDefinitive("BDR-daily-definitive"),
    TDRHourlyPreview("BDR-hourly-preview"),
    TDRHourlyDefinitive("BDR-hourly-definitive"),
    MDRDailyPreview("MDR-daily-preview"),
    MDRDailyDefinitive("MDR-daily-definitive"),
    MDRHourlyPreview("MDR-hourly-preview"),
    MDRHourlyDefinitive("MDR-hourly-definitive"),
    VDRDailyPreview("VDR-daily-preview"),
    VDRDailyDefinitive("VDR-daily-definitive"),
    VDRHourlyPreview("VDR-hourly-preview"),
    VDRHourlyDefinitive("VDR-hourly-definitive"),
    LoyaltyProgramsPreview("LoyaltyPrograms-preview"),
    LoyaltyProgramsDefinitive("LoyaltyPrograms-definitive"),
    SubscriberProfilePreview("SubscriberProfile-preview"),
    SubscriberProfileDefinitive("SubscriberProfile-definitive"),
    Journeys("Journeys"),
    SubscriberProfileSnapshot("SubscriberProfileSnapshot"),
    JourneystatisticCleanUp("JourneystatisticCleanUp"),
    ExpiredVoucherCleanUp("ExpiredVoucherCleanUp"),
    None("none");
    private String externalRepresentation;
    private Type(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static Type fromExternalRepresentation(String externalRepresentation) { for (Type enumeratedValue : Type.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return None; }
  }

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private String jobID;
  private Type type;
  private boolean enabled;
  private boolean scheduledAtRestart;
  private String cronEntry;
  private int tenantID;
  private String timeZone;

  /*****************************************
  *
  * Constructors
  *
  *****************************************/
  public ScheduledJobConfiguration(String jobID, Type type, boolean enabled, boolean scheduledAtRestart, String cronEntry, int tenantID, String timeZone) 
  {
    this.jobID = jobID;
    this.type = type;
    this.enabled = enabled;
    this.scheduledAtRestart = scheduledAtRestart;
    this.cronEntry = cronEntry;
    this.tenantID = tenantID;
    this.timeZone = timeZone;
  }
  
  public ScheduledJobConfiguration(String jobID, DeploymentJSONReader jsonReader, int tenantID, String timeZone) throws JSONUtilitiesException 
  {
    this(jobID,
        Type.fromExternalRepresentation(jsonReader.decodeString("type")),
        jsonReader.decodeBoolean("enabled"),
        jsonReader.decodeBoolean("scheduledAtRestart"),
        jsonReader.decodeString("cronEntry"),
        tenantID,
        timeZone);
  }

  /*****************************************
  *
  * Getters
  *
  *****************************************/
  public String getJobID() { return this.jobID; }
  public Type getType() { return this.type; }
  public boolean isEnabled() { return this.enabled; }
  public boolean isScheduledAtRestart() { return this.scheduledAtRestart; }
  public String getCronEntry() { return this.cronEntry; }
  public int getTenantID() { return this.tenantID; }
  public String getTimeZone() { return this.timeZone; }
}