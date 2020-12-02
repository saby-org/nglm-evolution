/****************************************************************************
*
*  NGLMRuntime.java
*
****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.evolution.DeliveryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import java.lang.management.ManagementFactory;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.MBeanRegistrationException;
import javax.management.NotCompliantMBeanException;
import javax.management.RuntimeErrorException;
import javax.management.RuntimeMBeanException;
import javax.management.RuntimeOperationsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import static com.evolving.nglm.core.NGLMRuntime.NGLMStatus.Running;

public class NGLMRuntime
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(NGLMRuntime.class);

  // we hard kill JVM after 3 minutes if normal shutdown still did not finished (if some thread are not released, can hang forever)
  private static final long JVM_HARD_KILLING_AFTER_X_MS = 180000;

  /****************************************
  *
  *  time constants
  *
  ****************************************/

  public static final Date BEGINNING_OF_TIME = (new GregorianCalendar(2000,0,1)).getTime();
  public static final Date END_OF_TIME = (new GregorianCalendar(2099,0,1)).getTime();
  
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum NGLMStatus { Created, Running, Stopped, FatalError }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private static List<NGLMShutdownHook> shutdownHooks = Collections.<NGLMShutdownHook>synchronizedList(new ArrayList<NGLMShutdownHook>());
  private static NGLMStatus nglmStatus = NGLMStatus.Created;
  private static Object nglmStatusLock = new Object();
  private static Set<Object> systemTimeDependencies = new HashSet<Object>();
  private KafkaConsumer<byte[], byte[]> simulatedTimeConsumer = null;
  
  /****************************************
  *
  *  initialize
  *
  ****************************************/
  
  public static void initialize(boolean initializeSimulatedTime)
  {
    //
    //  basic initialization
    //

    Thread.setDefaultUncaughtExceptionHandler(NGLMRuntime::uncaughtException);
    Runtime.getRuntime().addShutdownHook(new NGLMRuntimeShutdownHook());
    setStatus(Running);

    //
    //  simulated time
    //

    if (initializeSimulatedTime)
      {
        Runnable simulatedTime = new Runnable() { @Override public void run() { runSimulatedTime(); } };
        Thread simulatedTimeThread = new Thread(simulatedTime, "SimulatedTimeService");
        simulatedTimeThread.start();
      }
  }

  //
  //  initialize
  //

  public static void initialize()
  {
    initialize(false);
  }

  /*****************************************
  *
  *  registerMonitoringObject
  *
  *****************************************/

  @Deprecated // use com.evolving.nglm.evolution.statistics.Stats static methods in code directly to get stats instance (NOTE METRICS NAME WILL MOST LIKELY CHANGE, DASHBOARD CONF TO CHANGE !)
  public static void registerMonitoringObject(NGLMMonitoringObject monitoringObject) 
  {
    registerMonitoringObject(monitoringObject, false);
  }

  @Deprecated // use com.evolving.nglm.evolution.statistics.Stats static methods in code directly to get stats instance (NOTE METRICS NAME WILL MOST LIKELY CHANGE, DASHBOARD CONF TO CHANGE !)
  public static void registerMonitoringObject(NGLMMonitoringObject monitoringObject, boolean replaceObject) 
  {
    try
      {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
        ObjectName name = new ObjectName(monitoringObject.getObjectNameForManagement());
        if (replaceObject && mbs.isRegistered(name))
          {
            mbs.unregisterMBean(name);
          }
        mbs.registerMBean(monitoringObject, name);
      }
    catch (MalformedObjectNameException | InstanceAlreadyExistsException |InstanceNotFoundException | MBeanRegistrationException | NotCompliantMBeanException  e)
      {
        throw new ServerRuntimeException("Exception on mbean registration " + e.getMessage(), e);
      }
  }

  /*****************************************
  *
  *  unregisterMonitoringObject
  *
  *****************************************/

  public static void unregisterMonitoringObject(NGLMMonitoringObject monitoringObject) 
  {
    try
      {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
        ObjectName name = new ObjectName(monitoringObject.getObjectNameForManagement());
        if (mbs.isRegistered(name))
          {
            mbs.unregisterMBean(name);
          }
      }
    catch (RuntimeOperationsException | InstanceNotFoundException |  MBeanRegistrationException | RuntimeErrorException | RuntimeMBeanException | MalformedObjectNameException e)
      {
        throw new ServerRuntimeException("Exception on mbean un-registration " + e.getMessage(), e);
      }
  }

  /*****************************************
  *
  *  registerSystemTimeDependency
  *
  *****************************************/

  public static void registerSystemTimeDependency(Object systemTimeDependency)
  {
    synchronized (systemTimeDependencies)
      {
        systemTimeDependencies.add(systemTimeDependency);
      }
  }
  

  /*****************************************
  *
  *  unregisterSystemTimeDependency
  *
  *****************************************/

  public static void unregisterSystemTimeDependency(Object systemTimeDependency)
  {
    synchronized (systemTimeDependencies)
      {
        systemTimeDependencies.remove(systemTimeDependency);
      }
  }
  
  /*****************************************
  *
  *  runSimulatedTime
  *
  *****************************************/

  private static void runSimulatedTime()
  {
    //
    //  consumer
    //

    Properties simulatedTimeConsumerProperties = new Properties();
    simulatedTimeConsumerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
    simulatedTimeConsumerProperties.put("group.id", "simulatedtime-" + Integer.toHexString(ThreadLocalRandom.current().nextInt(10000000)));
    simulatedTimeConsumerProperties.put("auto.offset.reset", "earliest");
    simulatedTimeConsumerProperties.put("enable.auto.commit", "false");
    simulatedTimeConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    simulatedTimeConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaConsumer<byte[], byte[]> simulatedTimeConsumer = new KafkaConsumer<>(simulatedTimeConsumerProperties);

    //
    //  subscribe
    //

    simulatedTimeConsumer.subscribe(Arrays.asList(Deployment.getSimulatedTimeTopic()));

    //
    //  process
    //

    while (true)
      {
        //
        //  poll
        //

        ConsumerRecords<byte[], byte[]> records;
        try
          {
            records = simulatedTimeConsumer.poll(5000);
          }
        catch (WakeupException e)
          {
            records = ConsumerRecords.<byte[], byte[]>empty();
          }

        //
        //  parse
        //

        Date simulatedTime = null;
        for (ConsumerRecord<byte[], byte[]> simulatedTimeRecord : records)
          {
            TopicPartition topicPartition = new TopicPartition(simulatedTimeRecord.topic(), simulatedTimeRecord.partition());
            DateValue dateValue = DateValue.serde().deserializer().deserialize(topicPartition.topic(), simulatedTimeRecord.value());
            simulatedTime = (simulatedTime == null || dateValue.getValue().before(simulatedTime)) ? dateValue.getValue() : simulatedTime;
          }

        //
        //  process
        //

        if (simulatedTime != null)
          {
            SystemTime.setEffectiveSystemStartTime(simulatedTime);
            synchronized (systemTimeDependencies)
              {
                for (Object systemTimeDependency : systemTimeDependencies)
                  {
                    synchronized (systemTimeDependency)
                      {
                        systemTimeDependency.notifyAll();
                      }
                  }
              }
          }
      }
  }

  /*****************************************
  *
  *  addShutdownHook
  *
  *****************************************/

  public static void addShutdownHook(NGLMShutdownHook shutdownHook)
  {
    shutdownHooks.add(shutdownHook);
  }

  /*****************************************
  *
  *  shutdown
  *
  *****************************************/

  public static void shutdown(){shutdown(0);}
  public static void failureShutdown(){shutdown(-1);}
  private static void shutdown(int status)
  {
    log.error("NGLM shutdown");
    Runnable exit = new Runnable() { @Override public void run() { System.exit(status); } };
    Thread exitThread = new Thread(exit, "NGLMExit");
    exitThread.start();
  }

  /*****************************************
  *
  *  halt
  *
  *****************************************/
  
  public static void halt(String message)
  {
    log.error("NGLM halt - {}", message);
    Runtime.getRuntime().halt(-1);
  }

  /*****************************************
  *
  *  status
  *
  *****************************************/

  private static NGLMStatus setStatus(NGLMStatus newStatus)
  {
    synchronized (nglmStatusLock)
      {
        NGLMStatus currentStatus = nglmStatus;
        nglmStatus = newStatus;
        return currentStatus;
      }
  }

  /****************************************
  *
  *  uncaughtException
  *
  ****************************************/
  
  private static void uncaughtException(Thread t, Throwable e)
  {
    //
    //  log 
    //
    
    log.error("fatal error {} in thread {}", e.getMessage(), t.getName());
    StringWriter stackTraceWriter = new StringWriter();
    e.printStackTrace(new PrintWriter(stackTraceWriter, true));
    log.error(stackTraceWriter.toString());

    //
    //  shutdown
    //
    
    NGLMStatus startingStatus = setStatus(NGLMStatus.FatalError);
    switch (startingStatus)
      {
        case Running:
          log.error("NGLMRuntime forcing shutdown");
          Runnable exit = new Runnable() { @Override public void run() { System.exit(-1); } };
          Thread exitThread = new Thread(exit, "NGLMExit");
          exitThread.start();
          break;
      }
  }

  /*****************************************
  *
  *  class NGLMRuntimeShutdownHook
  *
  *****************************************/

  public static class NGLMRuntimeShutdownHook extends Thread
  {
    @Override public void run()
    {

      // "quick and dirty" JVM hard killer after 3 minutes (because normal shutdown can hang forever if some Threads does not finish)
      Thread hardKillerThread = new Thread(
        ()->{
          try {
            Thread.sleep(JVM_HARD_KILLING_AFTER_X_MS);
            halt("HARD KILLING JVM "+JVM_HARD_KILLING_AFTER_X_MS+"ms AFTER NORMAL SHUTDOWN TRIGGERED");
          } catch (InterruptedException e) {
            log.info("hard killer thread sleep interrupted",e);
          }
        }
      ,"HardKiller");
      hardKillerThread.setDaemon(true);// important to avoid this one Thread running to not block the normal shutdown
      hardKillerThread.start();

      NGLMStatus startingStatus = setStatus(NGLMStatus.Stopped);
      switch (startingStatus)
        {
          case Running:
            for (NGLMShutdownHook shutdownHook : shutdownHooks)
              {
                shutdownHook.shutdown(true);
              }
            break;
            
          case FatalError:
            for (NGLMShutdownHook shutdownHook : shutdownHooks)
              {
                shutdownHook.shutdown(false);
              }
            break;
        }
      log.info("NGLMRuntime shutdown");
    }
  }

  /******************************************
  *
  *  interface NGLMShutdownHook
  *
  *****************************************/

  public interface NGLMShutdownHook
  {
    public void shutdown(boolean normalShutdown);
  }
}
