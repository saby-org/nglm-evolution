package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

public class ScheduledJobConfiguration
{
  private boolean enabled;
  private boolean scheduledAtRestart;
  private String cronEntry;
  
  public ScheduledJobConfiguration(boolean enabled, boolean scheduledAtRestart, String cronEntry) {
    this.enabled = enabled;
    this.scheduledAtRestart = scheduledAtRestart;
    this.cronEntry = cronEntry;
  }
  
  public ScheduledJobConfiguration(JSONObject jsonRoot) throws JSONUtilitiesException {
    this(JSONUtilities.decodeBoolean(jsonRoot, "enabled", true),
        JSONUtilities.decodeBoolean(jsonRoot, "scheduledAtRestart", true),
        JSONUtilities.decodeString(jsonRoot, "cronEntry", true));
  }
  
  public boolean isEnabled() { return this.enabled; }
  public boolean isScheduledAtRestart() { return this.scheduledAtRestart; }
  public String getCronEntry() { return this.cronEntry; }
}