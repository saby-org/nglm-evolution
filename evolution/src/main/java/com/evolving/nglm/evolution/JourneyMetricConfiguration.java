package com.evolving.nglm.evolution;

import java.util.Collections;
import java.util.Map;

public class JourneyMetricConfiguration
{
  /*****************************************
  *
  *  data
  *
  *****************************************/
  private boolean enabled;
  private int priorPeriodDays; // Must be >= 1. Otherwise equals 0 when disabled.
  private int postPeriodDays; // Must be >= 1. Otherwise equals 0 when disabled.
  private Map<String,JourneyMetricDeclaration> metrics;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  public boolean isEnabled() { return enabled; }
  public int getPriorPeriodDays() { return priorPeriodDays; }
  public int getPostPeriodDays() { return postPeriodDays; }
  public Map<String,JourneyMetricDeclaration> getMetrics() { return metrics; }

  /*****************************************
  *
  *  constructors
  *
  *****************************************/
  // Disabled JourneyMetrics
  public JourneyMetricConfiguration()
  {
    this.enabled = false;
    this.priorPeriodDays = 0;
    this.postPeriodDays = 0;
    this.metrics = Collections.emptyMap();
  }
  
  // Enabled JourneyMetrics
  public JourneyMetricConfiguration(int priorPeriodDays, int postPeriodDays, Map<String,JourneyMetricDeclaration> metrics)
  {
    this.enabled = true;
    this.priorPeriodDays = priorPeriodDays;
    this.postPeriodDays = postPeriodDays;
    this.metrics = metrics;
  }
}
