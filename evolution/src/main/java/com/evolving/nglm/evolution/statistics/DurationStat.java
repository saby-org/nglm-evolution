package com.evolving.nglm.evolution.statistics;

import java.util.concurrent.atomic.LongAdder;

public class DurationStat extends Stat<DurationStat> implements DurationStatMBean {

  // stored in micro sec
  private LongAdder duration = new LongAdder();
  // got a counter stat as well
  private StatBuilder<CounterStat> counterStatBuilder;

  DurationStat(String metricName, String processName, StatBuilder<CounterStat> counterStatBuilder) {
    super(metricName,processName);
    this.counterStatBuilder = counterStatBuilder;
  }

  @Override protected String getMetricTypeInfo() { return "seconds"; }
  @Override protected DurationStat getNew(String metricName, String processName) { return new DurationStat(metricName,processName,counterStatBuilder); }
  @Override public long getTotal() { return Math.round(duration.sum() / 1_000_000.0d); } // return in sec

  // an helper, that just the time to save outside and then returned once event processed to compute a duration
  public static long startTime() { return System.nanoTime(); }

  public void add(long startTimeNanoSec){
    long endTime = System.nanoTime();
    duration.add(Math.round((endTime - startTimeNanoSec)/1_000.0d));//stored in micro sec
    counterStatBuilder.getStats().increment();
  }


}
