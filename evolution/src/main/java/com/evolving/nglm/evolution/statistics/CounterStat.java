package com.evolving.nglm.evolution.statistics;

import java.util.concurrent.atomic.AtomicLong;

public class CounterStat extends Stat<CounterStat> implements CounterStatMBean {

  private AtomicLong counter = new AtomicLong();

  CounterStat(String metricName, String processName) { super(metricName,processName); }

  @Override protected String getMetricTypeInfo() { return "count"; }
  @Override public long getTotal() { return counter.get(); }
  @Override protected CounterStat getNew(String metricName, String processName) { return new CounterStat(metricName,processName); }

  public void increment() {counter.incrementAndGet();}

}
