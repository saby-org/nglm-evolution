package com.evolving.nglm.evolution.statistics;

import java.util.concurrent.atomic.AtomicLong;

public class EvolutionCounterStatistics extends EvolutionStatistics<EvolutionCounterStatistics> implements EvolutionCounterStatisticsMBean {

  private AtomicLong counter = new AtomicLong();

  EvolutionCounterStatistics(String metricName, String processName) { super(metricName,processName); }

  @Override protected String getMetricTypeInfo() { return "count"; }
  @Override public long getTotal() { return counter.get(); }
  @Override protected EvolutionCounterStatistics getNew(String metricName, String processName) { return new EvolutionCounterStatistics(metricName,processName); }

  private void increment() {counter.incrementAndGet();}

  public void increment(String name){
    getPerNameInstance(getMetricName(),name).increment();
    counter.incrementAndGet();
  }

}
