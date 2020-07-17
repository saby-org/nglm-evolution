package com.evolving.nglm.evolution.statistics;

import java.util.HashMap;
import java.util.Map;


public class Stats {

  // the EvolutionCounterStatistics per processName instances
  private static final Map<String,EvolutionCounterStatistics> evolutionCounterStatistics = new HashMap<>();
  // get the instance for the process name
  public static EvolutionCounterStatistics getEvolutionCounterStatistics(String metricName, String processName){
    String key=metricName+"-"+processName;
    EvolutionCounterStatistics toRet = evolutionCounterStatistics.get(key);
    if(toRet==null){
      synchronized (evolutionCounterStatistics){
        toRet = evolutionCounterStatistics.get(key);
        if(toRet==null){
          toRet = new EvolutionCounterStatistics(metricName,processName);
          evolutionCounterStatistics.put(key,toRet);
        }
      }
    }
    return toRet;
  }

  // the EvolutionDurationStatistics per processName instances
  private static final Map<String,EvolutionDurationStatistics> evolutionDurationStatistics = new HashMap<>();
  // get the instance for the process name
  public static EvolutionDurationStatistics getEvolutionDurationStatistics(String metricName, String processName){
    String key=metricName+"-"+processName;
    EvolutionDurationStatistics toRet = evolutionDurationStatistics.get(key);
    if(toRet==null){
      synchronized (evolutionDurationStatistics){
        toRet = evolutionDurationStatistics.get(key);
        if(toRet==null){
          toRet = new EvolutionDurationStatistics(metricName,processName);
          evolutionDurationStatistics.put(key,toRet);
        }
      }
    }
    return toRet;
  }

}
