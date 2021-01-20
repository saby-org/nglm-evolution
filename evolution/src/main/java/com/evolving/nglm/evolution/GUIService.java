/****************************************************************************
*
*  GUIService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.ElasticSearchMapping;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.json.simple.JSONObject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(GUIService.class);  
  
  //
  //  elasticsearch ElasticsearchClientAPI
  //
  private ElasticsearchClientAPI elasticsearch;
  
  //
  //  statistics
  //

  private GUIServiceStatistics serviceStatistics = null;

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private volatile boolean stopRequested = false;
  private ConcurrentHashMap<String,GUIManagedObject> storedGUIManagedObjects = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String,GUIManagedObject> availableGUIManagedObjects = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String,GUIManagedObject> activeGUIManagedObjects = new ConcurrentHashMap<>();
  // store objects that should have been "active", but are not because an update "suspend" them, either from direct normal GUI call "suspend" or an invalid update, but they were active at some point to end up there
  private ConcurrentHashMap<String,GUIManagedObject> interruptedGUIManagedObjects = new ConcurrentHashMap<>();
  private Date lastUpdate = SystemTime.getCurrentTime();
  private TreeSet<ScheduleEntry> schedule = new TreeSet<ScheduleEntry>();
  private String guiManagedObjectTopic;
  private String guiAuditTopic = Deployment.getGUIAuditTopic();
  private KafkaProducer<byte[], byte[]> kafkaProducer;
  private Properties guiManagedObjectsConsumerProperties;
  private KafkaConsumer<byte[], byte[]> guiManagedObjectsConsumer;
  private boolean masterService;
  Thread schedulerThread = null;
  Thread listenerThread = null;
  Thread guiManagedObjectReaderThread = null;
  private List<GUIManagedObjectListener> guiManagedObjectListeners = new ArrayList<GUIManagedObjectListener>();
  private boolean notifyOnSignificantChange;
  private BlockingQueue<GUIManagedObject> listenerQueue = new LinkedBlockingQueue<GUIManagedObject>();
  private int lastGeneratedObjectID = 0;
  private String putAPIString;
  private String removeAPIString;
  
  //
  // services usable only by the GUIManager (with a special start)
  //
  
  private JourneyService journeyService;
  private TargetService targetService;
  private JourneyObjectiveService journeyObjectiveService;
  private ContactPolicyService contactPolicyService;


  //
  //  serdes
  //
  
  private ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
  private ConnectSerde<GUIManagedObject> guiManagedObjectSerde = GUIManagedObject.commonSerde();
  private ConnectSerde<GUIManagedObject> incompleteObjectSerde = GUIManagedObject.incompleteObjectSerde();
  private ConnectSerde<GUIObjectAudit> guiObjectAuditSerde = GUIObjectAudit.serde();

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public synchronized Date getLastUpdate() { return lastUpdate; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  // to remove once cleaned up
  @Deprecated // groupID not needed
  protected GUIService(String bootstrapServers, String serviceName, String groupID, String guiManagedObjectTopic, boolean masterService, GUIManagedObjectListener guiManagedObjectListener, String putAPIString, String removeAPIString, boolean notifyOnSignificantChange){
    this(bootstrapServers,serviceName,guiManagedObjectTopic,masterService,guiManagedObjectListener,putAPIString,removeAPIString,notifyOnSignificantChange);
  }
  protected GUIService(String bootstrapServers, String serviceName, String guiManagedObjectTopic, boolean masterService, GUIManagedObjectListener guiManagedObjectListener, String putAPIString, String removeAPIString, boolean notifyOnSignificantChange)
  {
    //
    //  configuration
    //

    this.guiManagedObjectTopic = guiManagedObjectTopic;
    this.masterService = masterService;
    this.putAPIString = putAPIString;
    this.removeAPIString = removeAPIString;
    this.notifyOnSignificantChange = notifyOnSignificantChange;

    //
    //  listener
    //

    if (guiManagedObjectListener != null)
      {
        guiManagedObjectListeners.add(guiManagedObjectListener);
      }

    //
    //  statistics
    //

    if (masterService)
      {
        try
          {
            this.serviceStatistics = new GUIServiceStatistics(serviceName);
          }
        catch (ServerException e)
          {
            throw new ServerRuntimeException("Could not initialize statistics");
          }
      }

    //
    //  set up producer
    //

    Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);
    
    //
    // set up consumer
    //

    Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    guiManagedObjectsConsumerProperties = consumerProperties;
    guiManagedObjectsConsumer = new KafkaConsumer<>(guiManagedObjectsConsumerProperties);
    
    //
    //  read initial guiManagedObjects
    //

    readGUIManagedObjects(true);

    //
    //  close consumer (if master)
    //

    if (masterService)
      {
        guiManagedObjectsConsumer.close();
      }

    //
    //  initialize listenerQueue
    //

    listenerQueue.clear();
  }

  /*****************************************
  *
  *  start
  *
  *****************************************/

  public void start(ElasticsearchClientAPI elasticSearch, JourneyService journeyService, JourneyObjectiveService journeyObjectiveService, TargetService targetService, ContactPolicyService contactPolicyService)
  {
    this.elasticsearch = elasticSearch;
    this.journeyService = journeyService;
    this.journeyObjectiveService = journeyObjectiveService;
    this.targetService = targetService;
    this.contactPolicyService = contactPolicyService;
    start();
  }
  
  public void start()
  {
    //
    //  scheduler
    //

    Runnable scheduler = new Runnable() { @Override public void run() { runScheduler(); } };
    schedulerThread = new Thread(scheduler, "GUIManagedObjectScheduler "+this.getClass().getSimpleName());
    schedulerThread.start();

    //
    //  listener
    //

    Runnable listener = new Runnable() { @Override public void run() { runListener(); } };
    listenerThread = new Thread(listener, "GUIManagedObjectListener "+this.getClass().getSimpleName());
    listenerThread.start();

    //
    //  read guiManagedObject updates
    //

    if (! masterService)
      {
        Runnable guiManagedObjectReader = new Runnable() { @Override public void run() { readGUIManagedObjects(false); } };
        guiManagedObjectReaderThread = new Thread(guiManagedObjectReader, "GUIManagedObjectReader "+this.getClass().getSimpleName());
        guiManagedObjectReaderThread.start();
      }
    
  }
  
  /*****************************************
  *
  *  stop
  *
  *****************************************/

  public void stop()
  {
    /*****************************************
    *
    *  stopRequested
    *
    *****************************************/

    synchronized (this)
      {
        //
        //  mark stopRequested
        //

        stopRequested = true;

        //
        //  wake sleeping polls/threads (if necessary)
        //

        if (guiManagedObjectsConsumer != null) guiManagedObjectsConsumer.wakeup();
        if (schedulerThread != null) schedulerThread.interrupt();
        if (listenerThread != null) listenerThread.interrupt();
      }

    /*****************************************
    *
    *  wait for threads to finish
    *
    *****************************************/

    try
      {
        if (schedulerThread != null) schedulerThread.join();
        if (listenerThread != null) listenerThread.join();
        if (guiManagedObjectReaderThread != null) guiManagedObjectReaderThread.join();
      }
    catch (InterruptedException e)
      {
        // nothing
      }

    /*****************************************
    *
    *  close resources
    *
    *****************************************/

    if (guiManagedObjectsConsumer != null) guiManagedObjectsConsumer.close();
    if (kafkaProducer != null) kafkaProducer.close();
    
    NGLMRuntime.unregisterSystemTimeDependency(this); // remove this, otherwise references to the service exists, even after we stop it

  }

  /*****************************************
  *
  *  registerListener
  *
  *****************************************/

  public void registerListener(GUIManagedObjectListener guiManagedObjectListener)
  {
    synchronized (this)
      {
        guiManagedObjectListeners.add(guiManagedObjectListener);
      }
  }

  /*****************************************
  *
  *  generateGUIManagedObjectID
  *
  *****************************************/

  protected String generateGUIManagedObjectID()
  {
    synchronized (this)
      {
        lastGeneratedObjectID += 1;
        return Long.toString(lastGeneratedObjectID);
      }
  }

  /*****************************************
  *
  *  getLastGeneratedObjectID
  *
  *****************************************/

  int getLastGeneratedObjectID()
  {
    synchronized (this)
      {
        return lastGeneratedObjectID;
      }
  }

  /*****************************************
  *
  *  getStoredGUIManagedObject
  *
  *****************************************/

  protected GUIManagedObject getStoredGUIManagedObject(String guiManagedObjectID, boolean includeArchived)
  {
    if(guiManagedObjectID==null) return null;
    GUIManagedObject result = storedGUIManagedObjects.get(guiManagedObjectID);
    result = (result != null && (includeArchived || ! result.getDeleted())) ? result : null;
      return result;
  }

  //
  //  (w/o includeArchived)
  //

  protected GUIManagedObject getStoredGUIManagedObject(String guiManagedObjectID) { return getStoredGUIManagedObject(guiManagedObjectID, false); }

  /*****************************************
  *
  *  getStoredGUIManagedObjects
  *
  ****************************************/

  protected Collection<GUIManagedObject> getStoredGUIManagedObjects(boolean includeArchived)
  {
    List<GUIManagedObject> result = new ArrayList<GUIManagedObject>();
    for (GUIManagedObject guiManagedObject : storedGUIManagedObjects.values())
      {
        if (includeArchived || ! guiManagedObject.getDeleted())
          {
            result.add(guiManagedObject);
          }
      }
    return result;
  }

  //
  //  (w/o includeArchived)
  //

  protected Collection<GUIManagedObject> getStoredGUIManagedObjects() { return getStoredGUIManagedObjects(false); }

  /*****************************************
  *
  *  isActiveThroughInterval
  *
  *****************************************/

  protected boolean isActiveThroughInterval(GUIManagedObject guiManagedObject, Date startDate, Date endDate)
  {
    boolean active = (guiManagedObject != null) && guiManagedObject.getAccepted() && guiManagedObject.getActive() && ! guiManagedObject.getDeleted();
    boolean activeThroughInterval = active && (guiManagedObject.getEffectiveStartDate().compareTo(startDate) <= 0) && (guiManagedObject.getEffectiveEndDate().compareTo(endDate) >= 0);
    return activeThroughInterval;
  }

  /*****************************************
  *
  *  isActiveGUIManagedObject
  *
  *****************************************/

  protected boolean isActiveGUIManagedObject(GUIManagedObject guiManagedObject, Date date) {
    if(guiManagedObject==null) return false;
    if(!guiManagedObject.getAccepted()) return false;
    if(activeGUIManagedObjects.get(guiManagedObject.getGUIManagedObjectID())==null) return false;
    if(guiManagedObject.getEffectiveStartDate().after(date)) return false;
    if(guiManagedObject.getEffectiveEndDate().before(date)) return false;
    return true;
  }

  protected boolean isInterruptedGUIManagedObject(GUIManagedObject guiManagedObject, Date date) {
    if(guiManagedObject==null) return false;
    if(interruptedGUIManagedObjects.get(guiManagedObject.getGUIManagedObjectID())==null) return false;
    if(guiManagedObject.getEffectiveStartDate()!=null && guiManagedObject.getEffectiveStartDate().after(date)) return false;
    if(guiManagedObject.getEffectiveEndDate()!=null && guiManagedObject.getEffectiveEndDate().before(date)) return false;
    return true;
  }

  /*****************************************
  *
  *  getActiveGUIManagedObject
  *
  *****************************************/

  protected GUIManagedObject getActiveGUIManagedObject(String guiManagedObjectID, Date date)
  {
    if(guiManagedObjectID==null) return null;
    GUIManagedObject guiManagedObject = activeGUIManagedObjects.get(guiManagedObjectID);
    if (isActiveGUIManagedObject(guiManagedObject, date))
      return guiManagedObject;
    else
      return null;
  }

  protected GUIManagedObject getInterruptedGUIManagedObject(String guiManagedObjectID, Date date)
  {
    if(guiManagedObjectID==null) return null;
    GUIManagedObject guiManagedObject = interruptedGUIManagedObjects.get(guiManagedObjectID);
    if (isInterruptedGUIManagedObject(guiManagedObject, date))
      return guiManagedObject;
    else
      return null;
  }

  /*****************************************
  *
  *  getActiveGUIManagedObjects
  *
  ****************************************/

  protected Collection<? extends GUIManagedObject> getActiveGUIManagedObjects(Date date)
  {
    Collection<GUIManagedObject> result = new HashSet<GUIManagedObject>();
	for (GUIManagedObject guiManagedObject : activeGUIManagedObjects.values())
	  {
		if (guiManagedObject.getEffectiveStartDate().compareTo(date) <= 0 && date.compareTo(guiManagedObject.getEffectiveEndDate()) < 0)
		  {
			result.add(guiManagedObject);
		  }
	  }
    return result;
  }

  /*****************************************
  *
  *  putGUIManagedObject
  *
  *****************************************/

  public void putGUIManagedObject(GUIManagedObject guiManagedObject, Date date, boolean newObject, String userID)
  {
    //
    //  created/updated date
    //

    GUIManagedObject existingStoredGUIManagedObject = storedGUIManagedObjects.get(guiManagedObject.getGUIManagedObjectID());
    guiManagedObject.setCreatedDate((existingStoredGUIManagedObject != null && existingStoredGUIManagedObject.getCreatedDate() != null) ? existingStoredGUIManagedObject.getCreatedDate() : date);
    guiManagedObject.setUpdatedDate(date);

    //
    //  mark (not) deleted
    //

    guiManagedObject.markDeleted(false);

    //
    //  submit to kafka
    //
    try {
      kafkaProducer.send(new ProducerRecord<byte[], byte[]>(guiManagedObjectTopic, stringKeySerde.serializer().serialize(guiManagedObjectTopic, new StringKey(guiManagedObject.getGUIManagedObjectID())), guiManagedObjectSerde.optionalSerializer().serialize(guiManagedObjectTopic, guiManagedObject))).get();
    } catch (InterruptedException|ExecutionException e) {
      log.error("putGUIManagedObject error saving to kafka "+guiManagedObject.getClass().getSimpleName()+" "+guiManagedObject.getGUIManagedObjectID(),e);
      if(e.getCause() instanceof RecordTooLargeException){
        throw new RuntimeException("too big to be saved",e);
	  }
	  throw new RuntimeException(e);
    }

    //
    //  audit
    //

    if (userID != null)
      {
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(guiAuditTopic, guiObjectAuditSerde.serializer().serialize(guiAuditTopic, new GUIObjectAudit(userID, putAPIString, newObject, guiManagedObject.getGUIManagedObjectID(), guiManagedObject, date))));
      }

    //
    //  process
    //

    processGUIManagedObject(guiManagedObject.getGUIManagedObjectID(), guiManagedObject, date);
    updateElasticSearch(guiManagedObject);
  }

  /*****************************************
  *
  *  removeGUIManagedObject
  *
  *****************************************/

  protected void removeGUIManagedObject(String guiManagedObjectID, Date date, String userID)
  {

    if(guiManagedObjectID==null) throw new RuntimeException("null guiManagedObjectID");

    //
    //  created/updated date
    //

    GUIManagedObject existingStoredGUIManagedObject = storedGUIManagedObjects.get(guiManagedObjectID);
    existingStoredGUIManagedObject.setCreatedDate((existingStoredGUIManagedObject != null && existingStoredGUIManagedObject.getCreatedDate() != null) ? existingStoredGUIManagedObject.getCreatedDate() : date);
    existingStoredGUIManagedObject.setUpdatedDate(date);

    //
    //  mark deleted
    //

    if (existingStoredGUIManagedObject != null) existingStoredGUIManagedObject.markDeleted(true);

    //
    //  submit to kafka
    //

    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(guiManagedObjectTopic, stringKeySerde.serializer().serialize(guiManagedObjectTopic, new StringKey(guiManagedObjectID)), guiManagedObjectSerde.optionalSerializer().serialize(guiManagedObjectTopic, existingStoredGUIManagedObject)));

    //
    //  audit
    //

    if (userID != null)
      {
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(guiAuditTopic, guiObjectAuditSerde.serializer().serialize(guiAuditTopic, new GUIObjectAudit(userID, removeAPIString, false, guiManagedObjectID, null, date))));
      }

    //
    //  process
    //

    processGUIManagedObject(guiManagedObjectID, existingStoredGUIManagedObject, date);
    updateElasticSearch(existingStoredGUIManagedObject);
  }

  /****************************************
  *
  *  processGUIManagedObject
  *
  ****************************************/

  protected void processGUIManagedObject(String guiManagedObjectID, GUIManagedObject guiManagedObject, Date date)
  {
    if(guiManagedObjectID==null) throw new RuntimeException("null guiManagedObjectID");
    synchronized (this)
      {
        //
        //  accepted?
        //

        boolean accepted = (guiManagedObject != null) && guiManagedObject.getAccepted();

        //
        //  created/updated dates
        //

        GUIManagedObject existingStoredGUIManagedObject = storedGUIManagedObjects.get(guiManagedObjectID);
        guiManagedObject.setCreatedDate((existingStoredGUIManagedObject != null && existingStoredGUIManagedObject.getCreatedDate() != null) ? existingStoredGUIManagedObject.getCreatedDate() : date);
        guiManagedObject.setUpdatedDate(date);

        //
        //  classify
        //

        boolean inActivePeriod = !guiManagedObject.getDeleted() && (guiManagedObject.getEffectiveStartDate()==null || guiManagedObject.getEffectiveStartDate().compareTo(date) <= 0) && (guiManagedObject.getEffectiveEndDate()==null || date.compareTo(guiManagedObject.getEffectiveEndDate()) < 0);
        boolean active = accepted && !guiManagedObject.getDeleted() && guiManagedObject.getActive() && (guiManagedObject.getEffectiveStartDate().compareTo(date) <= 0) && (date.compareTo(guiManagedObject.getEffectiveEndDate()) < 0);
        boolean future = accepted && !guiManagedObject.getDeleted() && guiManagedObject.getActive() && (guiManagedObject.getEffectiveStartDate().compareTo(date) > 0);
        boolean deleted = (guiManagedObject == null) || guiManagedObject.getDeleted();

        //
        //  store
        //

        if (guiManagedObject != null)
          {
            storedGUIManagedObjects.put(guiManagedObject.getGUIManagedObjectID(), guiManagedObject);
            if (serviceStatistics != null)
              {
                if (! deleted)
                  serviceStatistics.updatePutCount(guiManagedObject.getGUIManagedObjectID());
                else
                  serviceStatistics.updateRemoveCount(guiManagedObjectID);
              }
          }
        else
          {
            storedGUIManagedObjects.remove(guiManagedObjectID);
            if (serviceStatistics != null)
              {
                serviceStatistics.updateRemoveCount(guiManagedObjectID);
              }
          }

        //
        //  existingActiveGUIManagedObject
        //

        GUIManagedObject existingActiveGUIManagedObject = activeGUIManagedObjects.get(guiManagedObjectID);

        //
        //  clear
        //

        if (!inActivePeriod || active || future)
          {
            interruptedGUIManagedObjects.remove(guiManagedObjectID);
          }

        if (!active)
          {
            availableGUIManagedObjects.remove(guiManagedObjectID);
            activeGUIManagedObjects.remove(guiManagedObjectID);
            if (existingActiveGUIManagedObject != null){
              if(inActivePeriod) interruptedGUIManagedObjects.put(guiManagedObjectID,existingActiveGUIManagedObject);
              notifyListener(existingActiveGUIManagedObject);
            }
          }

        //
        //  add to availableGUIManagedObjects
        //

        if (active || future)
          {
            availableGUIManagedObjects.put(guiManagedObjectID, (GUIManagedObject) guiManagedObject);
            if (guiManagedObject.getEffectiveEndDate().compareTo(NGLMRuntime.END_OF_TIME) < 0)
              {
                ScheduleEntry scheduleEntry = new ScheduleEntry(guiManagedObject.getEffectiveEndDate(), guiManagedObject.getGUIManagedObjectID());
                schedule.add(scheduleEntry);
                this.notifyAll();
              }
          }

        //
        //  add to activeGUIManagedObjects
        //

        if (active)
          {
            activeGUIManagedObjects.put(guiManagedObjectID, (GUIManagedObject) guiManagedObject);
            if (existingActiveGUIManagedObject == null || existingActiveGUIManagedObject.getEpoch() != guiManagedObject.getEpoch() || !notifyOnSignificantChange) notifyListener(guiManagedObject);
          }

        //
        //  scheduler
        //

        if (future)
          {
            ScheduleEntry scheduleEntry = new ScheduleEntry(guiManagedObject.getEffectiveStartDate(), guiManagedObject.getGUIManagedObjectID());
            schedule.add(scheduleEntry);
            this.notifyAll();
          }

        //
        //  record guiManagedObjectID for autogenerate (if necessary)
        //

        Pattern p = Pattern.compile("[0-9]+$");
        Matcher m = p.matcher(guiManagedObjectID);
        Integer objectID = m.find() ? Integer.parseInt(m.group(0)) : null;
        lastGeneratedObjectID = (objectID != null && objectID.intValue() > lastGeneratedObjectID) ? objectID.intValue() : lastGeneratedObjectID;

        //
        //  statistics
        //

        if (serviceStatistics != null)
          {
            serviceStatistics.setActiveCount(activeGUIManagedObjects.size());
            serviceStatistics.setObjectCount(availableGUIManagedObjects.size());
          }

        //
        //  lastUpdate
        //

        lastUpdate = date.after(lastUpdate) ? date : lastUpdate;
      }
  }
  
  /****************************************
  *
  *  readGUIManagedObjects
  *
  ****************************************/

  private void readGUIManagedObjects(boolean readInitialTopicRecords)
  {

    assignAllTopicPartitions();

    //
    //  on the initial read, skip the poll if there are no records
    //

    if (readInitialTopicRecords)
      {
        boolean foundRecord = false;
        Map<TopicPartition,Long> endOffsets = getEndOffsets();
        for (TopicPartition partition : endOffsets.keySet())
          {
            if (endOffsets.get(partition) > 0)
              {
                foundRecord = true;
                break;
              }
          }
        if (!foundRecord)
          {
            log.info("No records found.  Skipping initial read for {}", guiManagedObjectTopic);
            return;
          }
      }
    
    //
    //  initialize consumedOffsets
    //
        
    Date readStartDate = SystemTime.getCurrentTime();
    boolean consumedAllAvailable = false;
    Map<TopicPartition,Long> consumedOffsets = new HashMap<TopicPartition,Long>();
    for (TopicPartition topicPartition : guiManagedObjectsConsumer.assignment())
      {
        consumedOffsets.put(topicPartition, guiManagedObjectsConsumer.position(topicPartition) - 1L);
      }
    
    //
    //  read
    //
        
    do
      {
        //
        // poll
        //

        ConsumerRecords<byte[], byte[]> guiManagedObjectRecords=ConsumerRecords.<byte[], byte[]>empty();
        try
          {
            guiManagedObjectRecords = guiManagedObjectsConsumer.poll(5000);
          }
        catch (WakeupException e)
          {
            log.info("wakeup while reading topic "+guiManagedObjectTopic);
          }

        //
        //  processing?
        //

        if (stopRequested) continue;
        
        //
        //  process
        //

        Date now = SystemTime.getCurrentTime();
        for (ConsumerRecord<byte[], byte[]> guiManagedObjectRecord : guiManagedObjectRecords)
          {
            //
            //  parse
            //

            String guiManagedObjectID =  stringKeySerde.deserializer().deserialize(guiManagedObjectRecord.topic(), guiManagedObjectRecord.key()).getKey();
            GUIManagedObject guiManagedObject;
            try
              {
                guiManagedObject = guiManagedObjectSerde.optionalDeserializer().deserialize(guiManagedObjectRecord.topic(), guiManagedObjectRecord.value());
              }
            catch (SerializationException e)
              {
                log.info("error reading guiManagedObject on " + guiManagedObjectTopic + " : {}", e.getMessage());
                guiManagedObject = incompleteObjectSerde.optionalDeserializer().deserialize(guiManagedObjectRecord.topic(), guiManagedObjectRecord.value());
              }

            if (guiManagedObject != null)
              log.debug("read {} {}", guiManagedObject.getClass().getSimpleName(), guiManagedObjectID);
            else
              log.info("clearing {}", guiManagedObjectID);

            //
            //  process
            //

            processGUIManagedObject(guiManagedObjectID, guiManagedObject, readInitialTopicRecords ? readStartDate : now);
            
            //
            //  offsets
            //

            consumedOffsets.put(new TopicPartition(guiManagedObjectRecord.topic(), guiManagedObjectRecord.partition()), guiManagedObjectRecord.offset());
          }

        //
        //  consumed all available?
        //

        Set<TopicPartition> assignedPartitions = guiManagedObjectsConsumer.assignment();
        Map<TopicPartition,Long> availableOffsets = getEndOffsets();
        consumedAllAvailable = true;
        for (TopicPartition partition : availableOffsets.keySet())
          {
            Long availableOffsetForPartition = availableOffsets.get(partition);
            Long consumedOffsetForPartition = consumedOffsets.get(partition);
            if (consumedOffsetForPartition == null)
              {
                consumedOffsetForPartition = guiManagedObjectsConsumer.position(partition) - 1L;
                consumedOffsets.put(partition, consumedOffsetForPartition);
              }
            if (consumedOffsetForPartition < availableOffsetForPartition-1)
              {
                consumedAllAvailable = false;
                break;
              }
          }
      }
    while (!stopRequested && (! consumedAllAvailable || ! readInitialTopicRecords));
  }

  private void assignAllTopicPartitions(){
    if(guiManagedObjectsConsumer!=null){
      Set<TopicPartition> partitions = new HashSet<>();
      List<PartitionInfo> partitionInfos=null;
      while(partitionInfos==null){
        try{
          partitionInfos=guiManagedObjectsConsumer.partitionsFor(guiManagedObjectTopic, Duration.ofSeconds(5));
        }catch (TimeoutException e){
          // a kafka broker might just be down, consumer.partitionsFor() can ends up timeout trying on this one
          reconnectConsumer();
          log.warn("timeout while getting topic partitions", e.getMessage());
        }catch (WakeupException e){
        }
      }
      for (PartitionInfo partitionInfo : partitionInfos) {
        partitions.add(new TopicPartition(guiManagedObjectTopic, partitionInfo.partition()));
      }
      guiManagedObjectsConsumer.assign(partitions);
    }else{
      log.error("NULL kafka consumer while assigning topic partitions "+guiManagedObjectTopic);
    }
  }

  private Map<TopicPartition, Long> getEndOffsets(){
    if(guiManagedObjectsConsumer==null){
      log.error("NULL kafka consumer reading end offets");
      return Collections.emptyMap();
    }
    while(true){
      try{
        return guiManagedObjectsConsumer.endOffsets(guiManagedObjectsConsumer.assignment());
      }catch (TimeoutException e){
        // a kafka broker might just went down (kafkaConsumer.assign(), not kafkaConsumer.consume(), so need to catch it)
        reconnectConsumer();
        log.warn("timeout while getting end offsets", e.getMessage());
      }catch (WakeupException e){
      }
    }
  }

  private void reconnectConsumer(){
    if(guiManagedObjectsConsumer!=null) guiManagedObjectsConsumer.close();
    guiManagedObjectsConsumer=new KafkaConsumer<byte[], byte[]>(guiManagedObjectsConsumerProperties);
    assignAllTopicPartitions();
  }

  /****************************************
  *
  *  runScheduler
  *
  ****************************************/

  private void runScheduler()
  {
    NGLMRuntime.registerSystemTimeDependency(this);
    while (!stopRequested)
      {
        synchronized (this)
          {
            //
            //  wait for next evaluation date
            //

            Date now = SystemTime.getCurrentTime();
            Date nextEvaluationDate = (schedule.size() > 0) ? schedule.first().getEvaluationDate() : NGLMRuntime.END_OF_TIME;
            long waitTime = nextEvaluationDate.getTime() - now.getTime();
            while (!stopRequested && waitTime > 0)
              {
                try
                  {
                    this.wait(waitTime);
                  }
                catch (InterruptedException e)
                  {
                    // ignore
                  }
                now = SystemTime.getCurrentTime();
                nextEvaluationDate = (schedule.size() > 0) ? schedule.first().getEvaluationDate() : NGLMRuntime.END_OF_TIME;
                waitTime = nextEvaluationDate.getTime() - now.getTime();
              }

            //
            //  processing?
            //

            if (stopRequested) continue;
            
            //
            //  process
            //

            ScheduleEntry entry = schedule.pollFirst();
            GUIManagedObject guiManagedObject = availableGUIManagedObjects.get(entry.getGUIManagedObjectID());
            if (guiManagedObject != null)
              {

                //
                //  existingActiveGUIManagedObject
                //

                GUIManagedObject existingActiveGUIManagedObject = activeGUIManagedObjects.get(guiManagedObject.getGUIManagedObjectID());

                //
                //  active window
                //

                if (guiManagedObject.getEffectiveStartDate().compareTo(now) <= 0 && now.compareTo(guiManagedObject.getEffectiveEndDate()) < 0)
                  {
                    activeGUIManagedObjects.put(guiManagedObject.getGUIManagedObjectID(), guiManagedObject);
                    interruptedGUIManagedObjects.remove(guiManagedObject.getGUIManagedObjectID());
                    notifyListener(guiManagedObject);
                  }

                //
                //  after active window
                //

                if (now.compareTo(guiManagedObject.getEffectiveEndDate()) >= 0)
                  {
                    availableGUIManagedObjects.remove(guiManagedObject.getGUIManagedObjectID());
                    activeGUIManagedObjects.remove(guiManagedObject.getGUIManagedObjectID());
                    interruptedGUIManagedObjects.remove(guiManagedObject.getGUIManagedObjectID());
                    if (existingActiveGUIManagedObject != null) notifyListener(guiManagedObject);
                  }

                //
                //  lastUpdate
                //

                lastUpdate = now.after(lastUpdate) ? now : lastUpdate;
              }
          }
      }
  }

  /*****************************************
  *
  *  generateResponseJSON
  *
  *****************************************/

  public JSONObject generateResponseJSON(GUIManagedObject guiManagedObject, boolean fullDetails, Date date)
  {
    JSONObject responseJSON = new JSONObject();
    if (guiManagedObject != null)
      {
        responseJSON.putAll(fullDetails ? getJSONRepresentation(guiManagedObject) : getSummaryJSONRepresentation(guiManagedObject));
        responseJSON.put("accepted", guiManagedObject.getAccepted());
        responseJSON.put("active", guiManagedObject.getActive());
        responseJSON.put("valid", guiManagedObject.getAccepted());
        responseJSON.put("processing", isActiveGUIManagedObject(guiManagedObject, date));
        responseJSON.put("readOnly", guiManagedObject.getReadOnly());
      }
    return responseJSON;
  }

  /*****************************************
  *
  *  getJSONRepresentation
  *
  *****************************************/

  protected JSONObject getJSONRepresentation(GUIManagedObject guiManagedObject)
  {
    JSONObject result = new JSONObject();
    result.putAll(guiManagedObject.getJSONRepresentation());
    return result;
  }
  
  /*****************************************
  *
  *  getSummaryJSONRepresentation
  *
  *****************************************/

  protected JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject)
  {
    JSONObject result = new JSONObject();
    result.put("id", guiManagedObject.getJSONRepresentation().get("id"));
    result.put("name", guiManagedObject.getJSONRepresentation().get("name"));
    result.put("description", guiManagedObject.getJSONRepresentation().get("description"));
    result.put("display", guiManagedObject.getJSONRepresentation().get("display"));
    result.put("icon", guiManagedObject.getJSONRepresentation().get("icon"));
    result.put("effectiveStartDate", guiManagedObject.getJSONRepresentation().get("effectiveStartDate"));
    result.put("effectiveEndDate", guiManagedObject.getJSONRepresentation().get("effectiveEndDate"));
    result.put("userID", guiManagedObject.getJSONRepresentation().get("userID"));
    result.put("userName", guiManagedObject.getJSONRepresentation().get("userName"));
    result.put("groupID", guiManagedObject.getJSONRepresentation().get("groupID"));
    result.put("createdDate", guiManagedObject.getJSONRepresentation().get("createdDate"));
    result.put("updatedDate", guiManagedObject.getJSONRepresentation().get("updatedDate"));
    result.put("info", guiManagedObject.getJSONRepresentation().get("info"));
    result.put("deleted", guiManagedObject.getJSONRepresentation().get("deleted") != null ? guiManagedObject.getJSONRepresentation().get("deleted") : false);
    return result;
  }
  
  /****************************************************************************
  *
  *  ScheduleEntry
  *
  ****************************************************************************/
  
  private static class ScheduleEntry implements Comparable<ScheduleEntry>
  {
    //
    //  data
    //

    private Date evaluationDate;
    private String guiManagedObjectID;

    //
    //  accessors
    //

    Date getEvaluationDate() { return evaluationDate; }
    String getGUIManagedObjectID() { return guiManagedObjectID; }

    //
    //  constructor
    //

    ScheduleEntry(Date evaluationDate, String guiManagedObjectID)
    {
      this.evaluationDate = evaluationDate;
      this.guiManagedObjectID = guiManagedObjectID;
    }

    //
    //  compareTo
    //
    
    public int compareTo(ScheduleEntry other)
    {
      if (this.evaluationDate.before(other.evaluationDate)) return -1;
      else if (this.evaluationDate.after(other.evaluationDate)) return 1;
      else return this.guiManagedObjectID.compareTo(other.guiManagedObjectID);
    }
  }
  
  /*****************************************
  *
  *  class GuiManagedObjectsConsumerRebalanceListener
  *
  *****************************************/
  
  private class GuiManagedObjectsConsumerRebalanceListener implements ConsumerRebalanceListener
  {
    //
    //  data
    //
    
    private String serviceName;
    private String groupID;
    
    //
    //  constructor
    //
    
    public GuiManagedObjectsConsumerRebalanceListener(String serviceName, String groupId)
    {
      this.serviceName = serviceName;
      this.groupID = groupId;
    }

    @Override public void onPartitionsRevoked(Collection<TopicPartition> lastAssignedPartitions) {}

    @Override public void onPartitionsAssigned(Collection<TopicPartition> partitionsToBeAssigned) 
    { 
      if (partitionsToBeAssigned.size() == 0)
        {
          log.error("{} has multiple instance with same key {}", serviceName, groupID);
        }
    }
  }
  
  /*****************************************
  *
  *  interface GUIManagedObjectListener
  *
  *****************************************/

  protected interface GUIManagedObjectListener
  {
    public void guiManagedObjectActivated(GUIManagedObject guiManagedObject);
    public void guiManagedObjectDeactivated(String objectID);
  }

  /*****************************************
  *
  *  notifyListener
  *
  *****************************************/

  private void notifyListener(GUIManagedObject guiManagedObject)
  {
    updateElasticSearch(guiManagedObject);
    listenerQueue.add(guiManagedObject);
  }

  /*****************************************
  *
  *  runListener
  *
  *****************************************/
  
  private void runListener()
  {
    while (!stopRequested)
      {
        try
          {
            //
            //  get next 
            //

            GUIManagedObject guiManagedObject = listenerQueue.take();

            //
            //  listeners
            //

            List<GUIManagedObjectListener> guiManagedObjectListeners = new ArrayList<GUIManagedObjectListener>();
            synchronized (this)
              {
                guiManagedObjectListeners.addAll(this.guiManagedObjectListeners);
              }

            //
            //  notify
            //

            Date now = SystemTime.getCurrentTime();
            for (GUIManagedObjectListener guiManagedObjectListener : guiManagedObjectListeners)
              {
                if (isActiveGUIManagedObject(guiManagedObject, now))
                  guiManagedObjectListener.guiManagedObjectActivated(guiManagedObject);
                else
                  guiManagedObjectListener.guiManagedObjectDeactivated(guiManagedObject.getGUIManagedObjectID());
              }
          }
        catch (InterruptedException e)
          {
            //
            // ignore
            //
          }
        catch (Throwable e)
          {
            StringWriter stackTraceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
            log.warn("Exception processing listener: {}", stackTraceWriter.toString());
          }
      }
  }


  public static void setCommonConsumerProperties(Properties consumerProperties)
  {
    consumerProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Deployment.getMaxPollIntervalMs());

  }
  
  public void updateElasticSearch(GUIManagedObject guiManagedObject)
  {
    if(guiManagedObject instanceof ElasticSearchMapping && elasticsearch != null /* to ensure it has been started with the good parameters */) 
      {
        if (guiManagedObject.getDeleted())
          {
            DeleteRequest deleteRequest = new DeleteRequest(((ElasticSearchMapping)guiManagedObject).getESIndexName(), ((ElasticSearchMapping)guiManagedObject).getESDocumentID());
          deleteRequest.id(((ElasticSearchMapping)guiManagedObject).getESDocumentID());
          try {
            elasticsearch.delete(deleteRequest,RequestOptions.DEFAULT);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      else
        {
          UpdateRequest request = new UpdateRequest(((ElasticSearchMapping)guiManagedObject).getESIndexName(), ((ElasticSearchMapping)guiManagedObject).getESDocumentID());
          request.doc(((ElasticSearchMapping)guiManagedObject).getESDocumentMap(journeyService, targetService, journeyObjectiveService, contactPolicyService));
          request.docAsUpsert(true);
          request.retryOnConflict(4);
          try {
            elasticsearch.update(request,RequestOptions.DEFAULT);
          } catch (IOException e) {
            e.printStackTrace();
          }          
        }
      }    
  }
}
