package com.evolving.nglm.evolution.statistics;

public class InstantStat extends Stat<InstantStat> implements InstantStatMBean {

  //volatile should not be needed for int, it would be for long (https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html#jls-17.7)
  private int value = Integer.MIN_VALUE;

  InstantStat(String metricName, String processName) { super(metricName,processName); }

  @Override protected String getMetricTypeInfo() { return "current"; }
  @Override public Integer getValue() { return value==Integer.MIN_VALUE ? null : value; }
  @Override protected InstantStat getNew(String metricName, String processName) { return new InstantStat(metricName,processName); }

  public void set(int value) {this.value=value;}

}
