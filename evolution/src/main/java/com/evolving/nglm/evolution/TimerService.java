/****************************************************************************
*
*  EvolutionEngine.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;

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
  private KafkaStreams streams = null;
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

  public TimerService(EvolutionEngine evolutionEngine, String bootstrapServers, KafkaStreams streams, ReadOnlyKeyValueStore<StringKey, SubscriberState> subscriberStateStore)
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    this.evolutionEngine = evolutionEngine;
    this.streams = streams;
    this.subscriberStateStore = subscriberStateStore;

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

  public void start()
  {
    /*****************************************
    *
    *  waiting for initialization
    *
    *****************************************/

    evolutionEngine.waitForStreams();

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

        /*****************************************
        *
        *  log end
        *
        *****************************************/

        synchronized (this)
          {
            log.info("loadSchedule (end): schedule size {}, earliest date {}, latest date {}", schedule.size(), (schedule.size() > 0) ? schedule.first().getEvaluationDate() : null, (schedule.size() > 0) ? schedule.last().getEvaluationDate() : null);
          }
      }
  }
}
