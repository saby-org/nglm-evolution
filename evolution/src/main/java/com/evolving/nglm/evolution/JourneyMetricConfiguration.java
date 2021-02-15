package com.evolving.nglm.evolution;

import java.util.Map;

public class JourneyMetricConfiguration
{
  /*****************************************
  *
  *  data
  *
  *****************************************/
  private int priorPeriodDays;
  private int postPeriodDays;
  private Map<String,JourneyMetricDeclaration> metrics;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  public int getPriorPeriodDays() { return priorPeriodDays; }
  public int getPostPeriodDays() { return postPeriodDays; }
  public Map<String,JourneyMetricDeclaration> getMetrics() { return metrics; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  public JourneyMetricConfiguration(int priorPeriodDays, int postPeriodDays, Map<String,JourneyMetricDeclaration> metrics)
  {
    this.priorPeriodDays = priorPeriodDays;
    this.postPeriodDays = postPeriodDays;
    this.metrics = metrics;
  }
}
