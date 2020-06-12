/****************************************************************************
*
*  TimerService.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.CronFormat;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.UniqueKeyServer;
import com.evolving.nglm.core.utilities.UtilitiesException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.GUIService.GUIManagedObjectListener;
import com.evolving.nglm.evolution.Journey.JourneyStatus;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

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

  private static final Logger log = LoggerFactory.getLogger(TimerService.class);

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
  private ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader = null;
  private TargetService targetService = null;
  private JourneyService journeyService = null;
  private CatalogCharacteristicService catalogCharacteristicService = null;
  private SubscriberMessageTemplateService subscriberMessageTemplateService = null;
  private DynamicEventDeclarationsService dynamicEventDeclarationsService = null;
  private JourneyObjectiveService journeyObjectiveService = null;
  protected static UniqueKeyServer epochServer = new UniqueKeyServer();
  private ExclusionInclusionTargetService exclusionInclusionTargetService = null;
  private boolean forceLoadSchedule = true;
  private Date earliestOutOfMemoryDate = NGLMRuntime.END_OF_TIME;
  private SortedSet<TimedEvaluation> schedule = new TreeSet<TimedEvaluation>();
  private SynchronousQueue<EvaluateTargets> evaluateTargetsToProcess = new SynchronousQueue<EvaluateTargets>();
  private KafkaConsumer<byte[], byte[]> evaluateTargetsConsumer;
  private KafkaProducer<byte[], byte[]> kafkaProducer;
  private ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
  private ConnectSerde<TimedEvaluation> timedEvaluationSerde = TimedEvaluation.serde();
  Thread scheduleLoaderThread = null;
  Thread schedulerThread = null;
  Thread periodicEvaluatorThread = null;
  Thread evaluateTargetsReaderThread = null;
  Thread evaluateTargetsWorkerThread = null;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public TimerService(EvolutionEngine evolutionEngine, String bootstrapServers, String apiKey)
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    this.evolutionEngine = evolutionEngine;

    /*****************************************
    *
    *  set up consumer
    *
    *****************************************/

    Properties consumerProperties = new Properties();
    consumerProperties.put("bootstrap.servers", bootstrapServers);
    consumerProperties.put("group.id", "timerService-evaluateTargets-" + apiKey);
    consumerProperties.put("auto.offset.reset", "earliest");
    consumerProperties.put("enable.auto.commit", "false");
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    evaluateTargetsConsumer = new KafkaConsumer<>(consumerProperties);

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

  public void start(ReadOnlyKeyValueStore<StringKey, SubscriberState> subscriberStateStore, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, TargetService targetService, JourneyService journeyService, CatalogCharacteristicService catalogCharacteristicService, SubscriberMessageTemplateService subscriberMessageTemplateService, DynamicEventDeclarationsService dynamicEventDeclarationsService, JourneyObjectiveService journeyObjectiveService, ExclusionInclusionTargetService exclusionInclusionTargetService)
  {
    /*****************************************
    *
    *  waiting for initialization
    *
    *****************************************/

    evolutionEngine.waitForStreams();

    /*****************************************
    *
    *  fields
    *
    *****************************************/

    this.subscriberStateStore = subscriberStateStore;
    this.subscriberGroupEpochReader = subscriberGroupEpochReader;
    this.targetService = targetService;
    this.journeyService = journeyService;
    this.exclusionInclusionTargetService = exclusionInclusionTargetService;
    this.catalogCharacteristicService = catalogCharacteristicService;
    this.subscriberMessageTemplateService = subscriberMessageTemplateService;
    this.dynamicEventDeclarationsService = dynamicEventDeclarationsService;
    this.journeyObjectiveService = journeyObjectiveService;

    /*****************************************
    *
    *  journeyService listener
    *
    *****************************************/

    //
    //  listener
    //

    GUIManagedObjectListener journeyListener = new GUIManagedObjectListener()
    {
      @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { processJourneyActivated(); }
      @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { }
    };

    //
    //  register
    //

    journeyService.registerListener(journeyListener);


    /*****************************************
    *
    *  subscribe
    *
    *****************************************/

    evaluateTargetsConsumer.subscribe(Arrays.asList(Deployment.getEvaluateTargetsTopic()));
    
    /*****************************************
    *
    *  scheduler reader thread
    *
    *****************************************/

    if (! stopRequested)
      {
        Runnable scheduleLoader = new Runnable() { @Override public void run() { runScheduleLoader(); } };
        scheduleLoaderThread = new Thread(scheduleLoader, "TimerService-ScheduleLoader");
        scheduleLoaderThread.start();
      }

    /*****************************************
    *
    *  scheduler worker thread
    *
    *****************************************/

    if (! stopRequested)
      {
        Runnable scheduler = new Runnable() { @Override public void run() { runScheduler(); } };
        schedulerThread = new Thread(scheduler, "TimerService-Scheduler");
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
        periodicEvaluatorThread = new Thread(periodicEvaluator, "TimerService-PeriodicEvaluator");
        periodicEvaluatorThread.start();
      }
    
    /*****************************************
    *
    *  evaluate targets reader thread
    *
    *****************************************/

    if (! stopRequested)
      {
        Runnable evaluateTargetsReader = new Runnable() { @Override public void run() { runEvaluateTargetsReader(); } };
        evaluateTargetsReaderThread = new Thread(evaluateTargetsReader, "TimerService-EvaluateTargetsReader");
        evaluateTargetsReaderThread.start();
      }
    
    /*****************************************
    *
    *  evaluate targets worker thread
    *
    *****************************************/

    if (! stopRequested)
      {
        Runnable evaluateTargetsWorker = new Runnable() { @Override public void run() { runEvaluateTargetsWorker(); } };
        evaluateTargetsWorkerThread = new Thread(evaluateTargetsWorker, "TimerService-EvaluateTargetsWorker");
        evaluateTargetsWorkerThread.start();
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
        scheduleLoaderThread.interrupt();
        schedulerThread.interrupt();
        periodicEvaluatorThread.interrupt();
        evaluateTargetsReaderThread.interrupt();
        evaluateTargetsWorkerThread.interrupt();
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
        *  createRecurrentJourneys
        *
        *****************************************/

        createRecurrentJourneys();

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

  /*****************************************
  *
  *  createRecurrentJourneys
  *
  *****************************************/
  
  private void createRecurrentJourneys()
  {
    log.info("creating recurrent campaigns");
    Date now = SystemTime.getCurrentTime();
    now = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());
    Collection<Journey> recurrentJourneys = journeyService.getActiveRecurrentJourneys(now);
    for (Journey recurrentJourney : recurrentJourneys)
      {
        List<Date> journeyCreationDates = new ArrayList<Date>();
        JourneyScheduler journeyScheduler = recurrentJourney.getJourneyScheduler();
        Journey latestJourney = journeyService.getLatestRecurrentJourney(recurrentJourney.getGUIManagedObjectID());
        
        //
        //  limit reached
        //
        
        if (journeyScheduler.getNumberOfOccurrences() <= latestJourney.getOccurrenceNumber()) continue;
        log.info("RAJ K creating recurrent campaign limit ok");
        
        //
        //  scheduling
        //
        
        String scheduling = journeyScheduler.getRunEveryUnit().toLowerCase();
        Integer scheduligInterval = journeyScheduler.getRunEveryDuration();
        
        log.info("RAJ K creating recurrent campaign for {} and scheduling {} scheduligInterval {}", recurrentJourney.getJourneyID(), scheduling, scheduligInterval);
        switch (scheduling)
          {
            case "week":
              Date firstDateOfThisWk = getFirstDate(now, Calendar.DAY_OF_WEEK);
              Date lastDateOfThisWk = getLastDate(now, Calendar.DAY_OF_WEEK);
              
              //
              //  nextExpectedDate
              //
              
              Date nextExpectedDate = RLMDateUtils.addWeeks(recurrentJourney.getEffectiveStartDate(), scheduligInterval, Deployment.getBaseTimeZone());
              while (nextExpectedDate.before(firstDateOfThisWk))
                {
                  nextExpectedDate = RLMDateUtils.addWeeks(nextExpectedDate, scheduligInterval, Deployment.getBaseTimeZone());
                }
              
              //
              //  not in this week
              //
              log.info("RAJ K nextExpectedDate {} ", nextExpectedDate);
              if (RLMDateUtils.truncate(nextExpectedDate, Calendar.DATE, Deployment.getBaseTimeZone()).after(lastDateOfThisWk)) continue;
              log.info("RAJ K nextExpectedDate is ok");
              //
              //  this is the week
              //
              
              List<Date> expectedCreationDates = getExpectedCreationDates(firstDateOfThisWk, lastDateOfThisWk, scheduling, journeyScheduler.getRunEveryWeekDay());
              
              //
              //  journeyCreationDates
              //
              
              Collection<Journey> recurrentSubJourneys = journeyService.getAllRecurrentJourneysByID(recurrentJourney.getJourneyID());
              for (Date expectedDate : expectedCreationDates)
                {
                  boolean exists = false;
                  for (Journey subJourney : recurrentSubJourneys)
                    {
                      exists = RLMDateUtils.truncatedCompareTo(expectedDate, subJourney.getCreatedDate(), Calendar.DATE, Deployment.getBaseTimeZone()) == 0;
                      if (exists) break;
                    }
                  if(!exists) journeyCreationDates.add(expectedDate);
                }
              break;
              
            case "month":
              Date firstDateOfThisMonth = getFirstDate(now, Calendar.DAY_OF_MONTH);
              Date lastDateOfThisMonth = getLastDate(now, Calendar.DAY_OF_MONTH);
              
              //
              //  nextExpectedDate
              //
              
              Date nextExpDate = RLMDateUtils.addMonths(recurrentJourney.getEffectiveStartDate(), scheduligInterval, Deployment.getBaseTimeZone());
              while (nextExpDate.before(firstDateOfThisMonth))
                {
                  nextExpDate = RLMDateUtils.addMonths(nextExpDate, scheduligInterval, Deployment.getBaseTimeZone());
                }
              
              //
              //  not in this month
              //
              log.info("RAJ K nextExpDate {} ", nextExpDate);
              if (RLMDateUtils.truncate(nextExpDate, Calendar.DATE, Deployment.getBaseTimeZone()).after(lastDateOfThisMonth)) continue;
              log.info("RAJ K nextExpDate is ok");
              //
              //  this is the month
              //
              
              List<Date> expCreationDates = getExpectedCreationDates(firstDateOfThisMonth, lastDateOfThisMonth, scheduling, journeyScheduler.getRunEveryMonthDay());
              
              //
              //  journeyCreationDates
              //
              
              Collection<Journey> rcrntSubJourneys = journeyService.getAllRecurrentJourneysByID(recurrentJourney.getJourneyID());
              for (Date expectedDate : expCreationDates)
                {
                  boolean exists = false;
                  for (Journey subJourney : rcrntSubJourneys)
                    {
                      exists = RLMDateUtils.truncatedCompareTo(expectedDate, subJourney.getCreatedDate(), Calendar.DATE, Deployment.getBaseTimeZone()) == 0;
                      if (exists) break;
                    }
                  if(!exists) journeyCreationDates.add(expectedDate);
                }
              break;

            default:
              break;
        }
        
        //
        //  createJourneys
        //
        
        createJourneys(recurrentJourney, journeyCreationDates, latestJourney.getOccurrenceNumber());
      }
    log.info("created recurrent campaigns");
  }

  //
  //  createJourneys
  //
  
  private void createJourneys(Journey recurrentJourney, List<Date> journeyCreationDates, Integer existingOccurrenceNumber)
  {
    log.info("RAJ K createingJourneys of {}, for {} existingOccurrenceNumber {}", recurrentJourney.getJourneyID(), journeyCreationDates, existingOccurrenceNumber);
    Integer occurrenceNumber = existingOccurrenceNumber;
    int daysBetween = RLMDateUtils.daysBetween(recurrentJourney.getEffectiveStartDate(), recurrentJourney.getEffectiveEndDate(), Deployment.getBaseTimeZone());
    for (Date startDate : journeyCreationDates)
      {
        JSONObject journeyJSON = (JSONObject) journeyService.getJSONRepresentation(recurrentJourney).clone();
        
        //
        //  remove
        //
        
        journeyJSON.remove("recurrence");
        journeyJSON.remove("scheduler");
        
        //
        //  add
        //
        
        String journeyID = journeyService.generateJourneyID();
        journeyJSON.put("id", journeyID);
        journeyJSON.put("name", recurrentJourney.getGUIManagedObjectName() + "_" + journeyID);
        journeyJSON.put("occurrenceNumber", ++occurrenceNumber);
        journeyJSON.put("active", true);
        journeyJSON.put("effectiveStartDate", recurrentJourney.formatDateField(startDate));
        journeyJSON.put("effectiveEndDate", recurrentJourney.formatDateField(RLMDateUtils.addDays(startDate, daysBetween, Deployment.getBaseTimeZone())));
        try
          {
            Journey subJourney = new Journey(journeyJSON, recurrentJourney.getGUIManagedObjectType(), epochServer.getKey(), null, journeyService, catalogCharacteristicService, subscriberMessageTemplateService, dynamicEventDeclarationsService, JourneyStatus.StartedApproved);
            subJourney.setCreatedDate(SystemTime.getCurrentTime());
            subJourney.setUpdatedDate(SystemTime.getCurrentTime());
            journeyService.putJourney(subJourney, journeyObjectiveService, catalogCharacteristicService, targetService, false, "0");
            log.info("RAJ K createJourneys of {} new journey {} ", recurrentJourney.getJourneyID(), subJourney.getGUIManagedObjectName());
          } 
        catch (GUIManagerException e)
          {
            --occurrenceNumber;
            e.printStackTrace();
            log.error("unable to create journey " + e.getMessage());
          }
      }
    
  }

  //
  //  getExpectedCreationDates
  //
  
  private List<Date> getExpectedCreationDates(Date firstDateOfThisWk, Date lastDateOfThisWk, String scheduling, List<String> runEveryDay)
  {
    List<Date> result = new ArrayList<Date>();
    while (firstDateOfThisWk.before(lastDateOfThisWk) || firstDateOfThisWk.compareTo(lastDateOfThisWk) == 0)
      {
        int day = -1;
        switch (scheduling)
          {
            case "week":
              day = RLMDateUtils.getField(firstDateOfThisWk, Calendar.DAY_OF_WEEK, Deployment.getBaseTimeZone());
              break;
              
            case "month":
              day = RLMDateUtils.getField(firstDateOfThisWk, Calendar.DAY_OF_MONTH, Deployment.getBaseTimeZone());
              break;

            default:
              break;
        }
        String dayOf = String.valueOf(day);
        if (runEveryDay.contains(dayOf))
          {
            result.add(new Date(firstDateOfThisWk.getTime()));
          }
        firstDateOfThisWk = RLMDateUtils.addDays(firstDateOfThisWk, 1, Deployment.getBaseTimeZone());
      }
    log.info("RAJ K getExpectedCreationDates {}", result);
    return result;
  }

  //
  //  getFirstDate
  //
  
  private Date getFirstDate(Date now, int dayOf)
  {
    Date firstDateOfNext = RLMDateUtils.ceiling(now, dayOf, Deployment.getBaseTimeZone());
    if (Calendar.DAY_OF_WEEK == dayOf)
      {
        return RLMDateUtils.addDays(firstDateOfNext, -7, Deployment.getBaseTimeZone());
      }
    else
      {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
        c.setTime(now);
        int dayOfMonth = RLMDateUtils.getField(now, Calendar.DAY_OF_MONTH, Deployment.getBaseTimeZone());
        Date firstDate = RLMDateUtils.addDays(now, -dayOfMonth+1, Deployment.getBaseTimeZone());
        return firstDate;
      }
  }
  
  //
  //  getLastDate
  //
  
  private Date getLastDate(Date now, int dayOf)
  {
    Date firstDateOfNext = RLMDateUtils.ceiling(now, dayOf, Deployment.getBaseTimeZone());
    if (Calendar.DAY_OF_WEEK == dayOf)
      {
        Date firstDateOfthisWk = RLMDateUtils.addDays(firstDateOfNext, -7, Deployment.getBaseTimeZone());
        return RLMDateUtils.addDays(firstDateOfthisWk, 6, Deployment.getBaseTimeZone());
      }
    else
      {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
        c.setTime(now);
        int toalNoOfDays = c.getActualMaximum(Calendar.DAY_OF_MONTH);
        int dayOfMonth = RLMDateUtils.getField(now, Calendar.DAY_OF_MONTH, Deployment.getBaseTimeZone());
        Date firstDate = RLMDateUtils.addDays(now, -dayOfMonth+1, Deployment.getBaseTimeZone());
        Date lastDate = RLMDateUtils.addDays(firstDate, toalNoOfDays-1, Deployment.getBaseTimeZone());
        return lastDate;
      }
  }

  /*****************************************
  *
  *  runEvaluateTargetsReader
  *
  *****************************************/

  private void runEvaluateTargetsReader()
  {
    EvaluateTargets currentEvaluateTargetsJob = null;
    NGLMRuntime.registerSystemTimeDependency(this);
    while (! stopRequested)
      {
        /*****************************************
        *
        *  poll
        *
        *****************************************/

        ConsumerRecords<byte[], byte[]> evaluateTargetsSourceRecords;
        try
          {
            evaluateTargetsSourceRecords = evaluateTargetsConsumer.poll(5000);
          }
        catch (WakeupException e)
          {
            evaluateTargetsSourceRecords = ConsumerRecords.<byte[], byte[]>empty();
          }

        /*****************************************
        *
        *  processing?
        *
        *****************************************/

        if (stopRequested) continue;

        /*****************************************
        *
        *  complete outstanding job (if necessary)
        *
        *****************************************/

        if (evaluateTargetsSourceRecords.count() == 0 && currentEvaluateTargetsJob != null && currentEvaluateTargetsJob.getCompleted())
          {
            evaluateTargetsConsumer.commitSync();
            currentEvaluateTargetsJob = null;
          }

        /*****************************************
        *
        *  process incoming records request
        *
        *****************************************/

        if (evaluateTargetsSourceRecords.count() > 0)
          {
            Set<String> campaignIDs = new HashSet<String>();
            Set<String> targetIDs = new HashSet<String>();

            //
            //  outstanding job
            //

            if (currentEvaluateTargetsJob != null)
              {
                currentEvaluateTargetsJob.markAborted();
                campaignIDs.addAll(currentEvaluateTargetsJob.getCampaignIDs());
                targetIDs.addAll(currentEvaluateTargetsJob.getTargetIDs());
                currentEvaluateTargetsJob = null;
              }

            //
            //  requested targetIDs
            //

            for (ConsumerRecord<byte[], byte[]> evaluateTargetsSourceRecord : evaluateTargetsSourceRecords)
              {
                //
                //  parse
                //

                EvaluateTargets evaluateTargets = null;
                try
                  {
                    evaluateTargets = EvaluateTargets.serde().deserializer().deserialize(Deployment.getEvaluateTargetsTopic(), evaluateTargetsSourceRecord.value());
                  }
                catch (SerializationException e)
                  {
                    log.info("error reading evaluateTargets: {}", e.getMessage());
                  }
                if (evaluateTargets != null) log.info("read evaluateTargets {}", evaluateTargets);

                //
                //  process
                //

                if (evaluateTargets != null)
                  {
                    campaignIDs.addAll(evaluateTargets.getCampaignIDs());
                    targetIDs.addAll(evaluateTargets.getTargetIDs());
                  }
              }

            //
            //  wait for active campaigns
            //

            Date now = SystemTime.getCurrentTime();
            Date timeout = RLMDateUtils.addSeconds(now, 30);
            Set<String> activeCampaignIDs = new HashSet<>();
            while (activeCampaignIDs.size() < campaignIDs.size() && now.before(timeout))
              {
                //
                //  all campaigns active?
                //

                activeCampaignIDs = new HashSet<>();
                for (String campaignID : campaignIDs)
                  {
                    Journey campaign = journeyService.getActiveJourney(campaignID, now);
                    if (campaign != null)
                      {
                        activeCampaignIDs.add(campaignID);
                      }
                  }

                //
                //  wait (if necessary)
                //

                if (activeCampaignIDs.size() < campaignIDs.size())
                  {
                    synchronized (this)
                      {
                        try
                          {
                            this.wait(timeout.getTime() - now.getTime());
                          }
                        catch (InterruptedException e)
                          {
                            // ignore
                          }
                      }
                  }

                //
                //  now
                //

                now = SystemTime.getCurrentTime();
              }

            //
            //  log 
            //

            if (activeCampaignIDs.size() == campaignIDs.size())
              log.info("schedule evaluateTargets {}", targetIDs);
            else
              log.error("schedule evaluateTargets (not all campaigns active) {} / {}", campaignIDs, activeCampaignIDs);

            //
            //  submit job
            //

            if (targetIDs.size() > 0)
              {
                EvaluateTargets evaluateTargets = new EvaluateTargets(campaignIDs, targetIDs);
                currentEvaluateTargetsJob = evaluateTargets;
                while (! stopRequested)
                  {
                    try { evaluateTargetsToProcess.put(evaluateTargets); break; } catch (InterruptedException e) { }
                  }
              }
          }
      }
  }
  
  /*****************************************
  *
  *  runEvaluateTargetsWorker
  *
  *****************************************/

  private void runEvaluateTargetsWorker()
  {
    NGLMRuntime.registerSystemTimeDependency(this);
    Date now = SystemTime.getCurrentTime();
    EvaluateTargets evaluateTargetsJob = null;
    int nbOfExtraLoopToDo=0;
    KeyValueIterator<StringKey,SubscriberState> subscriberStateStoreIterator = null;
    while (! stopRequested)
      {
        /*****************************************
        *
        *  poll (if no outstanding work)
        *
        *****************************************/

        if (evaluateTargetsJob == null)
          {
            while (! stopRequested)
              {
                try { evaluateTargetsJob = evaluateTargetsToProcess.poll(5L, TimeUnit.SECONDS); break; } catch (InterruptedException e) { }
              }
          }
        else
          {
            log.info("evaluateTargets job still to continue "+evaluateTargetsJob);
          }

        /*****************************************
        *
        *  processing?
        *
        *****************************************/

        if (stopRequested) continue;

        /*****************************************
        *
        *  process job
        *
        *****************************************/

        if (evaluateTargetsJob != null)
          {

            log.info("evaluateTargets job to do for "+evaluateTargetsJob);

            //
            //  waitForStreams
            //
            
            evolutionEngine.waitForStreams();
            
            //
            //  now
            //

            now = SystemTime.getCurrentTime();

            //
            //  job
            //

            try
              {
                //
                //  submit events
                //

                if (subscriberStateStoreIterator == null)
                  {

                    subscriberStateStoreIterator = subscriberStateStore.all();

                    // no on going job, "randomness" activated (NOT RECOMMENDED REGARDING PERF)
                    if(Deployment.getEnableEvaluateTargetRandomness() && nbOfExtraLoopToDo==0){
                      // will have to do 2 loops over the statestore at least, if no new jobs coming in meanwhile
                      nbOfExtraLoopToDo=1;
                      // probably not optimized at all, but sounds like the only way to "randomize" which subscriber to start with (investigate maye the subscriberStateStore.range() call for better...)
                      // the idea is to avoid that it is always the first subscriber in the statestore evaluate first and so always having the "limited" campaigns
                      // NOTE however that I'm not sure at all if subscriberStateStore.all() always give a fixed ordering (only personal tests seems indicate it does)
                      int totalSubs = (int)subscriberStateStore.approximateNumEntries();
                      int skipXFirst = new Random().nextInt(totalSubs);
                      log.info("evaluateTargets will skip the first "+skipXFirst+" subscribers from the around "+totalSubs+" this statestore instance contains for \"randomization\"");
                      int reallySkiped=0;
                      Long startTime = System.currentTimeMillis();
                      for( int i=0 ; i<skipXFirst && subscriberStateStoreIterator.hasNext() ; i++) { reallySkiped++ ; subscriberStateStoreIterator.next(); }
                      Long durationInMs = System.currentTimeMillis() - startTime;
                      log.info("evaluateTargets skipped the first "+reallySkiped+" subscribers in "+durationInMs+" ms");
                      // in case we skipped all....
                      if(!subscriberStateStoreIterator.hasNext()){
                        log.warn("evaluateTargets lost 1 entire loop for nothing... "+reallySkiped+" subscribers skipped in "+durationInMs+" ms");
                        subscriberStateStoreIterator.close();
                        subscriberStateStoreIterator=subscriberStateStore.all();
                      }
                    }

                  }

                Long startTime = System.currentTimeMillis();
                int nbSubsEvaluated=0;
                while (! stopRequested && ! evaluateTargetsJob.getAborted() && subscriberStateStoreIterator.hasNext())
                  {

                    //
                    //  subscriber state
                    //

                    SubscriberState subscriberState = subscriberStateStoreIterator.next().value;
                    nbSubsEvaluated++;

                    if(log.isTraceEnabled()) log.trace("evaluateTargets starting for subscriber "+subscriberState.getSubscriberID());

                    //
                    //  subscriber targets
                    //

                    Set<String> subscriberTargets = subscriberState.getSubscriberProfile().getTargets(subscriberGroupEpochReader);

                    //
                    //  (re-) evaluate rules based target lists
                    //

                    for (Target target : targetService.getActiveTargets(now))
                      {
                        if (evaluateTargetsJob.getTargetIDs().contains(target.getTargetID()))
                          {
                            switch (target.getTargetingType())
                              {
                                case Eligibility:
                                  SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, now);
                                  boolean addTarget = EvaluationCriterion.evaluateCriteria(evaluationRequest, target.getTargetingCriteria());
                                  if (addTarget)
                                    {
                                      subscriberTargets.add(target.getTargetID());
                                    }
                                  break;
                              }
                          }
                      }

                    //
                    //  match?
                    //

                    boolean match = false;
                    for (String targetID : evaluateTargetsJob.getTargetIDs())
                      {
                        if (subscriberTargets.contains(targetID))
                          {
                            match = true;
                            break;
                          }
                      }
                    
                    SubscriberEvaluationRequest inclusionExclusionEvaluationRequest = new SubscriberEvaluationRequest(subscriberState.getSubscriberProfile(), subscriberGroupEpochReader, now);
                    boolean inclusionList = subscriberState.getSubscriberProfile().getInInclusionList(inclusionExclusionEvaluationRequest, exclusionInclusionTargetService, subscriberGroupEpochReader, now);
                    if(inclusionList)
                      {
                        match = true;
                      }

                    //
                    // generate event (if necessary)
                    //
                    
                    if (match)
                      {
                        if(log.isTraceEnabled()) log.trace("evaluateTargets match for subscriber "+subscriberState.getSubscriberID()+", generating an event");
                        TimedEvaluation scheduledEvaluation = new TimedEvaluation(subscriberState.getSubscriberID(), now);
                        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(Deployment.getTimedEvaluationTopic(), stringKeySerde.serializer().serialize(Deployment.getTimedEvaluationTopic(), new StringKey(scheduledEvaluation.getSubscriberID())), timedEvaluationSerde.serializer().serialize(Deployment.getTimedEvaluationTopic(), scheduledEvaluation)));
                      }
                    else
                      {
                        if(log.isTraceEnabled()) log.trace("evaluateTargets does not match for subscriber "+subscriberState.getSubscriberID());
                      }

                    if(! subscriberStateStoreIterator.hasNext() && ! evaluateTargetsJob.getAborted() && nbOfExtraLoopToDo>0)
                      {
                        // we need to to a fool loop again, we did not start from the start
                        Long durationInMs = System.currentTimeMillis() - startTime;
                        log.info("evaluateTargets first loop finish in "+durationInMs+" ms for "+nbSubsEvaluated+" subscribers evaluated, but still need to do "+nbOfExtraLoopToDo+" loop");
                        subscriberStateStoreIterator.close();
                        subscriberStateStoreIterator=subscriberStateStore.all();
                        nbOfExtraLoopToDo--;
                      }

                  }
                Long durationInMs = System.currentTimeMillis() - startTime;
                if(nbOfExtraLoopToDo>0) nbOfExtraLoopToDo--;

                //
                //  mark completed if finished successfully
                //

                if (! stopRequested && ! evaluateTargetsJob.getAborted() && ! subscriberStateStoreIterator.hasNext() && nbOfExtraLoopToDo==0)
                  {
                    log.info("evaluateTargets completed for "+evaluateTargetsJob+" in "+durationInMs+" ms for "+nbSubsEvaluated+" subscribers evaluated");
                    evaluateTargetsJob.markCompleted();
                  }

                // if we got new job, not closing state store iterator to continue where we were
                if (evaluateTargetsJob.getAborted())
                  {
                    log.info("evaluateTargets aborted by new request for "+evaluateTargetsJob+" in "+durationInMs+" ms for "+nbSubsEvaluated+" subscribers evaluated");
                    // if new job arrived, but not a full loop again to do, then we need one more
                    if(nbOfExtraLoopToDo==0) nbOfExtraLoopToDo=1;
                  }
                // else we close it
                else
                  {
                    log.info("evaluateTargets finished for "+evaluateTargetsJob+" in "+durationInMs+" ms for "+nbSubsEvaluated+" subscribers evaluated");
                    subscriberStateStoreIterator.close();
                    subscriberStateStoreIterator=null;
                  }

                //
                //  finish job
                //

                evaluateTargetsJob = null;
              }
            catch (InvalidStateStoreException e)
              {
                log.error("evaluateTargets exception {}", e.getMessage());
                StringWriter stackTraceWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                log.info(stackTraceWriter.toString());
              }
          }
      }
  }

  /*****************************************
  *
  *  processJourneyActivated
  *
  *****************************************/

  private void processJourneyActivated()
  {
    synchronized (this)
      {
        this.notifyAll();
      }
  }
}
