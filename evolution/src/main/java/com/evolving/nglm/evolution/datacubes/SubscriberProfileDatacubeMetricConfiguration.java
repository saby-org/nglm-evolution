package com.evolving.nglm.evolution.datacubes;

import java.util.Collections;
import java.util.Map;

public class SubscriberProfileDatacubeMetricConfiguration
{
  /*****************************************
  *
  *  data
  *
  *****************************************/
  private boolean enabled;
  private int periodROI; 
  private String timeUnitROI; 
  private Map<String,SubscriberProfileDatacubeMetric> metrics;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  public boolean isEnabled() { return enabled; }
  public int getPeriodROI() { return periodROI; }
  public String getTimeUnitROI() { return timeUnitROI; }
  public Map<String,SubscriberProfileDatacubeMetric> getMetrics() { return metrics; }

  /*****************************************
  *
  *  constructors
  *
  *****************************************/
  // Disabled subscriberProfileDatacubeMetrics
  public SubscriberProfileDatacubeMetricConfiguration()
  {
    this.enabled = false;
    this.periodROI = 0;
    this.timeUnitROI = "";
    this.metrics = Collections.emptyMap();
  }
  
  // Enabled subscriberProfileDatacubeMetrics
  public SubscriberProfileDatacubeMetricConfiguration(int periodROI, String timeUnitROI, Map<String,SubscriberProfileDatacubeMetric> metrics)
  {
    this.enabled = true;
    this.periodROI = periodROI;
    this.timeUnitROI = timeUnitROI;
    this.metrics = metrics;
  }
}
