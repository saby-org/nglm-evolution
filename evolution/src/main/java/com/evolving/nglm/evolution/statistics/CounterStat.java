package com.evolving.nglm.evolution.statistics;

import java.util.concurrent.atomic.LongAdder;

public class CounterStat extends Stat<CounterStat> implements CounterStatMBean {

  private LongAdder counter = new LongAdder();

  CounterStat(String metricName, String processName) { super(metricName,processName); }

  @Override protected String getMetricTypeInfo() { return "count"; }
  @Override public long getTotal() { return counter.sum(); }
  @Override protected CounterStat getNew(String metricName, String processName) { return new CounterStat(metricName,processName); }

  public void increment() {counter.increment();}

}
