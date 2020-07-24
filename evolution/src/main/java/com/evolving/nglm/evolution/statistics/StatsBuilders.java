package com.evolving.nglm.evolution.statistics;

import java.util.HashMap;
import java.util.Map;


public class StatsBuilders {

  // keep different labels name used at one place
  public enum LABEL{
    name,
    status,
    commoditytype,
	operation,
    module
  }
  // keep different status used at one place
  public enum STATUS{
    ok,
    ko,
    unknown
  }

  // the CounterStat instances
  private static final Map<String,CounterStat> evolutionCounterStatistics = new HashMap<>();
  // get the stats instance
  public static StatBuilder<CounterStat> getEvolutionCounterStatisticsBuilder(String metricName, String processName){
    String key=metricName+"-"+processName;
    CounterStat toRet = evolutionCounterStatistics.get(key);
    if(toRet==null){
      synchronized (evolutionCounterStatistics){
        toRet = evolutionCounterStatistics.get(key);
        if(toRet==null){
          toRet = new CounterStat(metricName,processName);
          evolutionCounterStatistics.put(key,toRet);
        }
      }
    }
    return new StatBuilder<>(toRet);
  }

  // the DurationStat instances
  private static final Map<String,DurationStat> evolutionDurationStatistics = new HashMap<>();
  // get the instance
  public static StatBuilder<DurationStat> getEvolutionDurationStatisticsBuilder(String metricName, String processName){
    // we need a counter instance to put in duration one
    StatBuilder<CounterStat> toLink = getEvolutionCounterStatisticsBuilder(metricName,processName);
    String key=metricName+"-"+processName;
    DurationStat statToRet = evolutionDurationStatistics.get(key);
    if(statToRet==null){
      synchronized (evolutionDurationStatistics){
        statToRet = evolutionDurationStatistics.get(key);
        if(statToRet==null){
          statToRet = new DurationStat(metricName,processName,toLink);
          evolutionDurationStatistics.put(key,statToRet);
        }
      }
    }
    StatBuilder<DurationStat> toRet = new StatBuilder<>(statToRet);
    toRet.addLinkedBuilder(toLink);
    return toRet;
  }

}
