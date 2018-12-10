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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import java.util.Collections;
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

  protected  GUIServiceStatistics serviceStatistics = null;

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

    try
      {
        this.serviceStatistics = new GUIServiceStatistics(serviceName);
      }
    catch (ServerException e)
      {
        throw new ServerRuntimeException("Could not initialize statistics");
      }

    //
    //  set up producer
    //

    Properties producerProperties = new Properties();
    producerProperties.put("bootstrap.servers", bootstrapServers);
    producerProperties.put("acks", "all");
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);
    
    //
    // set up consumer
    //

    Properties consumerProperties = new Properties();
    consumerProperties.put("bootstrap.servers", bootstrapServers);
    consumerProperties.put("group.id", groupID);
    consumerProperties.put("auto.offset.reset", "earliest");
    consumerProperties.put("enable.auto.commit", "false");
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    guiManagedObjectsConsumer = new KafkaConsumer<>(consumerProperties);

    //
    //  subscribe to topic
    //

    guiManagedObjectsConsumer.subscribe(Arrays.asList(guiManagedObjectTopic));

    //
    //  read initial guiManagedObjects
    //

    readGUIManagedObjects(guiManagedObjectsConsumer, true);

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

  public synchronized void stop()
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

    //
    //  wait for threads to finish
    //

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

    //
    //  close
    //

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
  *  getStoredGUIManagedObject
  *
  *****************************************/

  protected GUIManagedObject getStoredGUIManagedObject(String guiManagedObjectID)
  {
    synchronized (this)
      {
        return storedGUIManagedObjects.get(guiManagedObjectID);
      }
  }

  /*****************************************
  *
  *  getStoredGUIManagedObjects
  *
  ****************************************/

  protected Collection<GUIManagedObject> getStoredGUIManagedObjects()
  {
    synchronized (this)
      {
        return storedGUIManagedObjects.values();
      }
  }

  /*****************************************
  *
  *  isActiveThroughInterval
  *
  *****************************************/

  protected boolean isActiveThroughInterval(GUIManagedObject guiManagedObject, Date startDate, Date endDate)
  {
    boolean active = (guiManagedObject != null) && guiManagedObject.getAccepted() && guiManagedObject.getValid() && guiManagedObject.getActive();
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
            return activeGUIManagedObjects.containsKey(guiManagedObject.getGUIManagedObjectID()) && guiManagedObject.getEffectiveStartDate().compareTo(date) <= 0 && date.compareTo(guiManagedObject.getEffectiveEndDate()) < 0;
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
    //  submit to kafka
    //

    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(guiManagedObjectTopic, stringKeySerde.serializer().serialize(guiManagedObjectTopic, new StringKey(guiManagedObjectID)), guiManagedObjectSerde.optionalSerializer().serialize(guiManagedObjectTopic, null)));

    //
    //  audit
    //

    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(guiAuditTopic, guiObjectAuditSerde.serializer().serialize(guiAuditTopic, new GUIObjectAudit(userID, removeAPIString, false, guiManagedObjectID, null, date))));

    //
    //  process
    //

    processGUIManagedObject(guiManagedObjectID, null, date);
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
        //  classify
        //

        boolean active = accepted && guiManagedObject.getValid() && guiManagedObject.getActive() && (guiManagedObject.getEffectiveStartDate().compareTo(date) <= 0) && (date.compareTo(guiManagedObject.getEffectiveEndDate()) < 0);
        boolean future = accepted && guiManagedObject.getValid() && guiManagedObject.getActive() && (guiManagedObject.getEffectiveStartDate().compareTo(date) > 0);

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
            serviceStatistics.updatePutCount(guiManagedObject.getGUIManagedObjectID());
          }
        else
          {
            storedGUIManagedObjects.remove(guiManagedObjectID);
            serviceStatistics.updateRemoveCount(guiManagedObjectID);
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

        try
          {
            int objectID = Integer.parseInt(guiManagedObjectID);
            lastGeneratedObjectID = (objectID > lastGeneratedObjectID) ? objectID : lastGeneratedObjectID;
          }
        catch (NumberFormatException e)
          {
            //
            //  guiManagedObjectID is NOT a number, ignore
            //
          }

        //
        //  statistics
        //

        serviceStatistics.setActiveCount(activeGUIManagedObjects.size());
        serviceStatistics.setObjectCount(availableGUIManagedObjects.size());

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
    Date readStartDate = SystemTime.getCurrentTime();
    boolean consumedAllAvailable = false;
    Map<TopicPartition,Long> consumedOffsets = new HashMap<TopicPartition,Long>();
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
              log.info("read guiManagedObject {}", guiManagedObjectID);
            else
              log.info("clearing guiManagedObject {}", guiManagedObjectID);

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
            Long consumedOffsetForPartition = (consumedOffsets.get(partition) != null) ? consumedOffsets.get(partition) : -1L;
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

  JSONObject generateResponseJSON(GUIManagedObject guiManagedObject, boolean fullDetails, Date date)
  {
    JSONObject responseJSON = new JSONObject();
    if (guiManagedObject != null)
      {
        responseJSON.putAll(fullDetails ? guiManagedObject.getJSONRepresentation() : getSummaryJSONRepresentation(guiManagedObject));
        responseJSON.put("accepted", guiManagedObject.getAccepted());
        responseJSON.put("processing", isActiveGUIManagedObject(guiManagedObject, date));
        responseJSON.put("readOnly", guiManagedObject.getReadOnly());
      }
    return responseJSON;
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
    result.put("valid", guiManagedObject.getJSONRepresentation().get("valid"));
    result.put("active", guiManagedObject.getJSONRepresentation().get("active"));
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
