package com.evolving.nglm.evolution.statistics;

import com.evolving.nglm.core.ServerRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.*;

// the mother class of all our different statistics ones
public abstract class Stat<T extends Stat> {

  private static final Logger log = LoggerFactory.getLogger(Stat.class);

  private String metricName;
  private String processName;
  String getMetricName(){return metricName;}
  String getProcessName(){return processName;}

  // return the entire jmx name to register
  String getFullJmxName(Map<String,String> labels){
    StringBuilder sb = new StringBuilder("com.evolving.nglm.evolution:type="+metricName+"_"+getMetricTypeInfo()+",process="+processName);
    labels.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry->sb.append(","+entry.getKey()+"="+entry.getValue()));
    return sb.toString();
  }

  // we forced at least those 2 labels always
  Stat(String metricName, String processName) {
    this.metricName=metricName;
    this.processName=processName;
  }

  // we need to register the different "metrics type" (count/duration/...) for same "type" (name chosen by user) with a distinct JMX type to be registered (a bit hacky)
  protected abstract String getMetricTypeInfo();
  // we need subclass to provide a new instance
  protected abstract T getNew(String metricName, String processName);

  // all stats per object
  private final Map<String,T> evolutionStats = new HashMap<>();
  // lazy instantiation if not yet known of the one instance per name
  T getInstance(Stat<T> stat, Map<String,String> labels){
    // instance stored by jmx name
    String fullName = stat.getFullJmxName(labels);
    // stored is full jmx name
    T toRet = evolutionStats.get(fullName);
    if(toRet == null){
      synchronized (evolutionStats){
        toRet = evolutionStats.get(fullName);
        if(toRet == null){
          toRet = getNew(stat.getMetricName(),stat.getProcessName());
          evolutionStats.put(fullName,toRet);
          try {
            log.info("registering mbean for stats : "+fullName);
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName jmxName = new ObjectName(fullName);
            mbs.registerMBean(toRet, jmxName);
          } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException  e) {
            throw new ServerRuntimeException("Exception on mbean registration " + e.getMessage(), e);
          }
        }
      }
    }
    return toRet;
  }

}
