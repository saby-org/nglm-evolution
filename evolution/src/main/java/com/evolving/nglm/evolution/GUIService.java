/****************************************************************************
*
*  GUIService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
  //  statistics
  //

  private GUIServiceStatistics serviceStatistics = null;

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private volatile boolean stopRequested = false;
  private Map<String,GUIManagedObject> storedGUIManagedObjects = new HashMap<String,GUIManagedObject>();
  private Map<String,GUIManagedObject> availableGUIManagedObjects = new HashMap<String,GUIManagedObject>();
  private Map<String,GUIManagedObject> activeGUIManagedObjects = new HashMap<String,GUIManagedObject>();
  private Date lastUpdate = SystemTime.getCurrentTime();
  private TreeSet<ScheduleEntry> schedule = new TreeSet<ScheduleEntry>();
  private String guiManagedObjectTopic;
  private String guiAuditTopic = Deployment.getGUIAuditTopic();
  private KafkaProducer<byte[], byte[]> kafkaProducer;
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

  protected GUIService(String bootstrapServers, String serviceName, String groupID, String guiManagedObjectTopic, boolean masterService, GUIManagedObjectListener guiManagedObjectListener, String putAPIString, String removeAPIString, boolean notifyOnSignificantChange)
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
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    guiManagedObjectsConsumer = new KafkaConsumer<>(consumerProperties);
    
    //
    //  subscribe to topic
    //

    guiManagedObjectsConsumer.subscribe(Arrays.asList(guiManagedObjectTopic), new GuiManagedObjectsConsumerRebalanceListener(serviceName, groupID));
    
    //
    //  read initial guiManagedObjects
    //

    readGUIManagedObjects(guiManagedObjectsConsumer, true);

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

  public void start()
  {
    //
    //  scheduler
    //

    Runnable scheduler = new Runnable() { @Override public void run() { runScheduler(); } };
    schedulerThread = new Thread(scheduler, "GUIManagedObjectScheduler");
    schedulerThread.start();

    //
    //  listener
    //

    Runnable listener = new Runnable() { @Override public void run() { runListener(); } };
    listenerThread = new Thread(listener, "GUIManagedObjectListener");
    listenerThread.start();

    //
    //  read guiManagedObject updates
    //

    if (! masterService)
      {
        Runnable guiManagedObjectReader = new Runnable() { @Override public void run() { readGUIManagedObjects(guiManagedObjectsConsumer, false); } };
        guiManagedObjectReaderThread = new Thread(guiManagedObjectReader, "GUIManagedObjectReader");
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
        return String.format(Deployment.getGenerateNumericIDs() ? "%d" : "%03d", lastGeneratedObjectID);
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
    synchronized (this)
      {
        GUIManagedObject result = storedGUIManagedObjects.get(guiManagedObjectID);
        result = (result != null && (includeArchived || ! result.getDeleted())) ? result : null;
        return result;
      }
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
    synchronized (this)
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

  protected boolean isActiveGUIManagedObject(GUIManagedObject guiManagedObjectUnchecked, Date date)
  {
    if (guiManagedObjectUnchecked instanceof GUIManagedObject)
      {
        GUIManagedObject guiManagedObject = (GUIManagedObject) guiManagedObjectUnchecked;
        synchronized (this)
          {
            return guiManagedObject.getAccepted() && activeGUIManagedObjects.containsKey(guiManagedObject.getGUIManagedObjectID()) && guiManagedObject.getEffectiveStartDate().compareTo(date) <= 0 && date.compareTo(guiManagedObject.getEffectiveEndDate()) < 0;
          }
      }
    else
      {
        return false;
      }
  }

  /*****************************************
  *
  *  getActiveGUIManagedObject
  *
  *****************************************/

  protected GUIManagedObject getActiveGUIManagedObject(String guiManagedObjectID, Date date)
  {
    synchronized (this)
      {
        GUIManagedObject guiManagedObject = activeGUIManagedObjects.get(guiManagedObjectID);
        if (isActiveGUIManagedObject(guiManagedObject, date))
          return guiManagedObject;
        else
          return null;
      }
  }

  /*****************************************
  *
  *  getActiveGUIManagedObjects
  *
  ****************************************/

  protected Collection<? extends GUIManagedObject> getActiveGUIManagedObjects(Date date)
  {
    Collection<GUIManagedObject> result = new HashSet<GUIManagedObject>();
    synchronized (this)
      {
        for (GUIManagedObject guiManagedObject : activeGUIManagedObjects.values())
          {
            if (guiManagedObject.getEffectiveStartDate().compareTo(date) <= 0 && date.compareTo(guiManagedObject.getEffectiveEndDate()) < 0)
              {
                result.add(guiManagedObject);
              }
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

    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(guiManagedObjectTopic, stringKeySerde.serializer().serialize(guiManagedObjectTopic, new StringKey(guiManagedObject.getGUIManagedObjectID())), guiManagedObjectSerde.optionalSerializer().serialize(guiManagedObjectTopic, guiManagedObject)));

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
  }

  /*****************************************
  *
  *  removeGUIManagedObject
  *
  *****************************************/

  protected void removeGUIManagedObject(String guiManagedObjectID, Date date, String userID)
  {
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
  }

  /****************************************
  *
  *  processGUIManagedObject
  *
  ****************************************/

  protected void processGUIManagedObject(String guiManagedObjectID, GUIManagedObject guiManagedObject, Date date)
  {
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

        boolean active = accepted && !guiManagedObject.getDeleted() && guiManagedObject.getActive() && (guiManagedObject.getEffectiveStartDate().compareTo(date) <= 0) && (date.compareTo(guiManagedObject.getEffectiveEndDate()) < 0);
        boolean future = accepted && !guiManagedObject.getDeleted() && guiManagedObject.getActive() && (guiManagedObject.getEffectiveStartDate().compareTo(date) > 0);
        boolean deleted = (guiManagedObject == null) || guiManagedObject.getDeleted();

        //
        //  copy
        //

        storedGUIManagedObjects = new HashMap<String,GUIManagedObject>(storedGUIManagedObjects);
        availableGUIManagedObjects = new HashMap<String,GUIManagedObject>(availableGUIManagedObjects);
        activeGUIManagedObjects = new HashMap<String,GUIManagedObject>(activeGUIManagedObjects);

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

        if (!active)
          {
            availableGUIManagedObjects.remove(guiManagedObjectID);
            activeGUIManagedObjects.remove(guiManagedObjectID);
            if (existingActiveGUIManagedObject != null) notifyListener(new IncompleteObject(guiManagedObjectID));
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

  private void readGUIManagedObjects(KafkaConsumer<byte[], byte[]> consumer, boolean readInitialTopicRecords)
  {
    //
    //  on the initial read, skip the poll if there are no records
    //

    if (readInitialTopicRecords)
      {
        boolean foundRecord = false;
        Set<TopicPartition> partitions = new HashSet<TopicPartition>();
        for (org.apache.kafka.common.PartitionInfo partitionInfo : consumer.partitionsFor(guiManagedObjectTopic))
          {
            partitions.add(new TopicPartition(guiManagedObjectTopic, partitionInfo.partition()));
          }
        Map<TopicPartition,Long> endOffsets = consumer.endOffsets(partitions);
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
    for (TopicPartition topicPartition : consumer.assignment())
      {
        consumedOffsets.put(topicPartition, consumer.position(topicPartition) - 1L);
      }
    
    //
    //  read
    //
        
    do
      {
        //
        // poll
        //

        ConsumerRecords<byte[], byte[]> guiManagedObjectRecords;
        try
          {
            guiManagedObjectRecords = consumer.poll(5000);
          }
        catch (WakeupException e)
          {
            guiManagedObjectRecords = ConsumerRecords.<byte[], byte[]>empty();
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
                log.info("error reading guiManagedObject: {}", e.getMessage());
                guiManagedObject = incompleteObjectSerde.optionalDeserializer().deserialize(guiManagedObjectRecord.topic(), guiManagedObjectRecord.value());
              }

            if (guiManagedObject != null)
              log.info("read {} {}", guiManagedObject.getClass().getSimpleName(), guiManagedObjectID);
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

        Set<TopicPartition> assignedPartitions = consumer.assignment();
        Map<TopicPartition,Long> availableOffsets = consumer.endOffsets(assignedPartitions);
        consumedAllAvailable = true;
        for (TopicPartition partition : availableOffsets.keySet())
          {
            Long availableOffsetForPartition = availableOffsets.get(partition);
            Long consumedOffsetForPartition = consumedOffsets.get(partition);
            if (consumedOffsetForPartition == null)
              {
                consumedOffsetForPartition = consumer.position(partition) - 1L;
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
                //  copy
                //

                availableGUIManagedObjects = new HashMap<String,GUIManagedObject>(availableGUIManagedObjects);
                activeGUIManagedObjects = new HashMap<String,GUIManagedObject>(activeGUIManagedObjects);

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
                    notifyListener(guiManagedObject);
                  }

                //
                //  after active window
                //

                if (now.compareTo(guiManagedObject.getEffectiveEndDate()) >= 0)
                  {
                    availableGUIManagedObjects.remove(guiManagedObject.getGUIManagedObjectID());
                    activeGUIManagedObjects.remove(guiManagedObject.getGUIManagedObjectID());
                    if (existingActiveGUIManagedObject != null) notifyListener(new IncompleteObject(guiManagedObject.getGUIManagedObjectID()));
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
}
