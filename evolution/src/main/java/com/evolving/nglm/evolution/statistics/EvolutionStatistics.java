package com.evolving.nglm.evolution.statistics;

import com.evolving.nglm.core.ServerRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

// the mother class of all our different statistics ones
public abstract class EvolutionStatistics<T extends EvolutionStatistics> {

  private static final Logger log = LoggerFactory.getLogger(EvolutionStatistics.class);

  private String metricName;
  private String processName;
  String getMetricName(){return metricName;}
  String getProcessName(){return processName;}

  // all stats per name
  private final Map<String,T> evolutionStats = new HashMap<>();

  EvolutionStatistics(String metricName,String processName) {
    this.metricName=metricName;
    this.processName=processName;
  }

  // we need to register the different "metrics type" (count/duration/...) for same "type" (name chosen by user) with a distinct JMX type to be registered (a bit hacky)
  protected abstract String getMetricTypeInfo();
  // we need subclass to provide a new instance
  protected abstract T getNew(String metricName, String processName);
  // lazy instantiation if not yet known of the one instance per name
  T getPerNameInstance(String metricName, String name){
    T toRet = evolutionStats.get(name);
    if(toRet == null){
      synchronized (evolutionStats){
        toRet = evolutionStats.get(name);
        if(toRet == null){
          toRet = getNew(metricName,processName);
          evolutionStats.put(name,toRet);
          try {
            String toRegister = "com.evolving.nglm.evolution:type="+metricName+"_"+toRet.getMetricTypeInfo()+",name="+name+",process="+this.processName;
            log.info("registering mbean for stats : "+toRegister);

            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName jmxName = new ObjectName(toRegister);
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
