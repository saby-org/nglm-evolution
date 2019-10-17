/****************************************************************************
*
*  TimerService.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.CronFormat;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.utilities.UtilitiesException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;

public class TimerService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(EvolutionEngine.class);

  //
  //  limites
  //

  private int entriesInMemoryLimit = 100000;
  private int reloadThreshold = 20000;

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private volatile boolean stopRequested = false;
  private EvolutionEngine evolutionEngine = null;
  private ReadOnlyKeyValueStore<StringKey, SubscriberState> subscriberStateStore = null;
  private boolean forceLoadSchedule = true;
  private Date earliestOutOfMemoryDate = NGLMRuntime.END_OF_TIME;
  private SortedSet<TimedEvaluation> schedule = new TreeSet<TimedEvaluation>();
  private KafkaProducer<byte[], byte[]> kafkaProducer;
  private ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
  private ConnectSerde<TimedEvaluation> timedEvaluationSerde = TimedEvaluation.serde();
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public TimerService(EvolutionEngine evolutionEngine, String bootstrapServers)
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    this.evolutionEngine = evolutionEngine;

    /*****************************************
    *
    *  set up producer
    *
    *****************************************/

    Properties producerProperties = new Properties();
    producerProperties.put("bootstrap.servers", bootstrapServers);
    producerProperties.put("acks", "all");
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);
  }

  /*****************************************
  *
  *  start
  *
  *****************************************/

  public void start(ReadOnlyKeyValueStore<StringKey, SubscriberState> subscriberStateStore)
  {
    /*****************************************
    *
    *  waiting for initialization
    *
    *****************************************/

    evolutionEngine.waitForStreams();

    /*****************************************
    *
    *  subscriberStateStore
    *
    *****************************************/

    this.subscriberStateStore = subscriberStateStore;

    /*****************************************
    *
    *  reader thread
    *
    *****************************************/

    if (! stopRequested)
      {
        Runnable scheduleLoader = new Runnable() { @Override public void run() { runScheduleLoader(); } };
        Thread scheduleLoaderThread = new Thread(scheduleLoader, "TimerService-ScheduleLoader");
        scheduleLoaderThread.start();
      }

    /*****************************************
    *
    *  scheduler thread
    *
    *****************************************/

    if (! stopRequested)
      {
        Runnable scheduler = new Runnable() { @Override public void run() { runScheduler(); } };
        Thread schedulerThread = new Thread(scheduler, "TimerService-Scheduler");
        schedulerThread.start();
      }

    /*****************************************
    *
    *  periodic evaluation thread
    *
    *****************************************/

    if (! stopRequested)
      {
        Runnable periodicEvaluator = new Runnable() { @Override public void run() { runPeriodicEvaluator(); } };
        Thread periodicEvaluatorThread = new Thread(periodicEvaluator, "TimerService-PeriodicEvaluator");
        periodicEvaluatorThread.start();
      }
  }

  /*****************************************
  *
  *  schedule
  *
  *****************************************/

  public void schedule(TimedEvaluation timedEvaluation)
  {
    synchronized (this)
      {
        //
        //  add to the schedule
        //

        if (timedEvaluation.getEvaluationDate().before(earliestOutOfMemoryDate))
          {
            schedule.add(timedEvaluation);
          }

        //
        //  enforce limit
        //

        while (schedule.size() > entriesInMemoryLimit)
          {
            TimedEvaluation entryToBeRemovedFromMemory = schedule.last();
            schedule.remove(entryToBeRemovedFromMemory);
            if (entryToBeRemovedFromMemory.getEvaluationDate().before(earliestOutOfMemoryDate))
              {
                earliestOutOfMemoryDate = entryToBeRemovedFromMemory.getEvaluationDate();
              }
          }

        //
        //  notify
        //

        this.notifyAll();
      }
  }

  /*****************************************
  *
  *  deschedule
  *
  *****************************************/

  public void deschedule(TimedEvaluation timedEvaluation)
  {
    synchronized (this)
      {
        //
        //  remove from the schedule
        //

        schedule.remove(timedEvaluation);

        //
        //  notify
        //

        this.notifyAll();
      }
  }

  /*****************************************
  *
  *  stop
  *
  *****************************************/

  public void stop()
  {
    synchronized (this)
      {
        stopRequested = true;
        this.notifyAll();
      }
  }

  /*****************************************
  *
  *  forceLoadSchedule
  *
  *****************************************/

  public void forceLoadSchedule()
  {
    synchronized (this)
      {
        forceLoadSchedule = true;
        this.notifyAll();
      }
  }

  /*****************************************
  *
  *  runScheduler
  *
  *****************************************/

  private void runScheduler()
  {
    /*****************************************
    *
    *  runScheduler
    *
    *****************************************/

    NGLMRuntime.registerSystemTimeDependency(this);
    while (! stopRequested)
      {
        /*****************************************
        *
        *  process schedule
        *
        *****************************************/

        TimedEvaluation scheduledEvaluation = null;
        synchronized (this)
          {
            /*****************************************
            *
            *  process schedule
            *
            *****************************************/

            Date now = SystemTime.getCurrentTime();
            TimedEvaluation entry = (TimedEvaluation) ((! schedule.isEmpty()) ? schedule.first() : null);
            if (entry != null && ! entry.getEvaluationDate().after(now))
              {
                scheduledEvaluation = entry;
                schedule.remove(entry);
                this.notifyAll();
              }
            else if (entry != null)
              {
                long timeout = entry.getEvaluationDate().getTime() - now.getTime();
                if (timeout > 0) 
                  {
                    try
                      { 
                        this.wait(timeout);
                      }
                    catch (InterruptedException e)
                      {
                      }
                  }
              }
            else
              {
                try
                  {
                    this.wait(0);
                  }
                catch (InterruptedException e)
                  {
                  }
              }
          }

        /*****************************************
        *
        *  send (if necessary)
        *
        *****************************************/

        if (scheduledEvaluation != null)
          {
            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getTimedEvaluationTopic(), stringKeySerde.serializer().serialize(Deployment.getTimedEvaluationTopic(), new StringKey(scheduledEvaluation.getSubscriberID())), timedEvaluationSerde.serializer().serialize(Deployment.getTimedEvaluationTopic(), scheduledEvaluation)));
          }
      }

    /*****************************************
    *
    *  stop
    *
    *****************************************/

    if (kafkaProducer != null) kafkaProducer.close();
  }

  /*****************************************
  *
  *  runScheduleLoader
  *
  *****************************************/

  private void runScheduleLoader()
  {
    NGLMRuntime.registerSystemTimeDependency(this);
    while (! stopRequested)
      {
        /*****************************************
        *
        *  wait until reload schedule is needed
        *
        *****************************************/

        synchronized (this)
          {
            while (! forceLoadSchedule && (schedule.size() > reloadThreshold || earliestOutOfMemoryDate.equals(NGLMRuntime.END_OF_TIME)))
              {
                try
                  {
                    this.wait();
                  }
                catch (InterruptedException e)
                  {
                  }
              }
          }
          
        /*****************************************
        *
        *  waitForStreams
        *
        *****************************************/

        evolutionEngine.waitForStreams();

        /*****************************************
        *
        *  log start
        *
        *****************************************/

        synchronized (this)
          {
            log.info("loadSchedule (start): forceLoadSchedule {}, earliestOutOfMemoryDate {}, schedule size {}, earliest date {}, latest date {}", forceLoadSchedule, earliestOutOfMemoryDate, schedule.size(), (schedule.size() > 0) ? schedule.first().getEvaluationDate() : null, (schedule.size() > 0) ? schedule.last().getEvaluationDate() : null);
          }

        /*****************************************
        *
        *  prepare for loadSchedule
        *
        *****************************************/

        synchronized (this)
          {
            forceLoadSchedule = false;
            earliestOutOfMemoryDate = NGLMRuntime.END_OF_TIME;
          }

        /*****************************************
        *
        *  loadSchedule
        *
        *****************************************/

        boolean loadScheduleCompleted = false;
        try
          {
            KeyValueIterator<StringKey,SubscriberState> subscriberStateStoreIterator = subscriberStateStore.all();
            while (! stopRequested && subscriberStateStoreIterator.hasNext())
              {
                SubscriberState subscriberState = subscriberStateStoreIterator.next().value;
                for (TimedEvaluation timedEvaluation : subscriberState.getScheduledEvaluations())
                  {
                    schedule(timedEvaluation);
                  }
              }
            subscriberStateStoreIterator.close();
            loadScheduleCompleted = true;
          }
        catch (InvalidStateStoreException e)
          {
            log.error("load schedule exception {}", e.getMessage());
            StringWriter stackTraceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
            log.info(stackTraceWriter.toString());
          }

        /*****************************************
        *
        *  log end
        *
        *****************************************/

        synchronized (this)
          {
            log.info("loadSchedule ({}): schedule size {}, earliest date {}, latest date {}", (loadScheduleCompleted ? "end" : "aborted"), schedule.size(), (schedule.size() > 0) ? schedule.first().getEvaluationDate() : null, (schedule.size() > 0) ? schedule.last().getEvaluationDate() : null);
          }
      }
  }

  /*****************************************
  *
  *  runPeriodicEvaluator
  *
  *****************************************/

  private void runPeriodicEvaluator()
  {
    /*****************************************
    *
    *  abort if regression mode
    *
    *****************************************/

    if (Deployment.getRegressionMode()) return;

    /*****************************************
    *
    *  abort if periodic evaluation is not configured
    *
    *****************************************/

    if (Deployment.getPeriodicEvaluationCronEntry() == null) return;

    /*****************************************
    *
    *  cron
    *
    *****************************************/

    CronFormat periodicEvaluation = null;
    try
      {
        periodicEvaluation = new CronFormat(Deployment.getPeriodicEvaluationCronEntry(), TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
      }
    catch (UtilitiesException e)
      {
        log.error("bad perodicEvaluationCronEntry {}", e.getMessage());
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.info(stackTraceWriter.toString());
        return;
      }

    /*****************************************
    *
    *  main loop
    *
    *****************************************/

    NGLMRuntime.registerSystemTimeDependency(this);
    Date now = SystemTime.getCurrentTime();
    Date nextPeriodicEvaluation = periodicEvaluation.previous(now);
    while (! stopRequested)
      {
        /*****************************************
        *
        *  wait until reload schedule is needed
        *
        *****************************************/

        synchronized (this)
          {
            while (! stopRequested && now.before(nextPeriodicEvaluation))
              {
                try
                  {
                    this.wait(nextPeriodicEvaluation.getTime() - now.getTime());
                  }
                catch (InterruptedException e)
                  {
                  }
                now = SystemTime.getCurrentTime();
              }
          }

        //
        //  abort if stopping
        //

        if (stopRequested)
          {
            continue;
          }
          
        /*****************************************
        *
        *  waitForStreams
        *
        *****************************************/

        evolutionEngine.waitForStreams();

        /*****************************************
        *
        *  log start
        *
        *****************************************/

        log.info("periodicEvaluation {} (start)", nextPeriodicEvaluation);

        /*****************************************
        *
        *  periodic evaluation
        *
        *****************************************/

        boolean periodicEvaluationComplete = false;
        try
          {
            KeyValueIterator<StringKey,SubscriberState> subscriberStateStoreIterator = subscriberStateStore.all();
            while (! stopRequested && subscriberStateStoreIterator.hasNext())
              {
                SubscriberState subscriberState = subscriberStateStoreIterator.next().value;
                if (subscriberState.getLastEvaluationDate().before(nextPeriodicEvaluation))
                  {
                    TimedEvaluation scheduledEvaluation = new TimedEvaluation(subscriberState.getSubscriberID(), nextPeriodicEvaluation, true);
                    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getTimedEvaluationTopic(), stringKeySerde.serializer().serialize(Deployment.getTimedEvaluationTopic(), new StringKey(scheduledEvaluation.getSubscriberID())), timedEvaluationSerde.serializer().serialize(Deployment.getTimedEvaluationTopic(), scheduledEvaluation)));
                  }
              }
            subscriberStateStoreIterator.close();
            periodicEvaluationComplete = true;
          }
        catch (InvalidStateStoreException e)
          {
            log.error("perodicEvaluation exception {}", e.getMessage());
            StringWriter stackTraceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
            log.info(stackTraceWriter.toString());
          }

        //
        //  complete?
        //

        if (! periodicEvaluationComplete)
          {
            log.info("periodicEvaluation {} (retrying)", nextPeriodicEvaluation);
            continue;
          }

        /*****************************************
        *
        *  log end
        *
        *****************************************/

        log.info("periodicEvaluation {} (end)", nextPeriodicEvaluation);

        /*****************************************
        *
        *  nextPeriodicEvaluation
        *
        *****************************************/

        nextPeriodicEvaluation = periodicEvaluation.next(RLMDateUtils.addSeconds(nextPeriodicEvaluation,1));
        log.info("periodicEvaluation {} (next)", nextPeriodicEvaluation);
      }
  }
}
