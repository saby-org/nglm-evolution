package com.evolving.nglm.evolution.statistics;

import java.util.concurrent.atomic.AtomicLong;

public class EvolutionDurationStatistics extends EvolutionStatistics<EvolutionDurationStatistics> implements EvolutionDurationStatisticsMBean {

  // stored in micro sec
  private AtomicLong duration = new AtomicLong();

  EvolutionDurationStatistics(String metricName, String processName) { super(metricName,processName); }

  @Override protected String getMetricTypeInfo() { return "seconds"; }
  @Override protected EvolutionDurationStatistics getNew(String metricName, String processName) { return new EvolutionDurationStatistics(metricName,processName); }
  @Override public long getTotal() { return Math.round(duration.get() / 1_000_000.0d); } // return in sec

  // an helper, that just the time to save outside and then returned once event processed to compute a duration
  public long startTime() { return System.nanoTime(); }

  private void add(long durationNanoSec){ duration.addAndGet(Math.round(durationNanoSec/1_000.0d)); }//stored in micro sec

  public void add(String name,long startTimeNanoSec) {
    long endTime = System.nanoTime();
    // increment duration
    getPerNameInstance(getMetricName(),name).add(endTime - startTimeNanoSec);
    // increment a counter
    Stats.getEvolutionCounterStatistics(getMetricName(),getProcessName()).increment(name);
  }

}
