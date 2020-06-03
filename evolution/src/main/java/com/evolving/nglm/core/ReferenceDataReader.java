/****************************************************************************
*
*  ReferenceDataReader.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import io.confluent.connect.avro.AvroConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ReferenceDataReader<K, V extends ReferenceDataValue<K>>
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ReferenceDataReader.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  singleton
  //

  protected static Object readersLock = new Object();
  protected static Map<String, Integer> readerReferences = new HashMap<String, Integer>();
  protected static Map<String, ReferenceDataReader> readers = new HashMap<String, ReferenceDataReader>();

  //
  //  data
  //

  private volatile boolean stopRequested = false;
  private String readerName;
  private String referenceDataTopic;
  private Properties consumerProperties;
  private KafkaConsumer<byte[], byte[]> consumer = null;
  private Thread readerThread = null;
  private Converter converter;
  private UnpackValue<V> unpackValue;
  protected Map<K,Set<ReferenceDataRecord>> referenceData = new HashMap<K,Set<ReferenceDataRecord>>();

  /*****************************************
  *
  *  startReader
  *
  *****************************************/

  //to remove once cleaned up
  public static <K, V extends ReferenceDataValue<K>> ReferenceDataReader<K,V> startReader(String readerName, String readerKey, String bootstrapServers, String referenceDataTopic, UnpackValue<V> unpackValue){
  	return startReader(readerName,bootstrapServers,referenceDataTopic,unpackValue);
  }
  public static <K, V extends ReferenceDataValue<K>> ReferenceDataReader<K,V> startReader(String readerName, String bootstrapServers, String referenceDataTopic, UnpackValue<V> unpackValue)
  {
    synchronized (readersLock)
      {
        if (! readerReferences.containsKey(readerName))
          {
            if (readers.get(readerName) != null) throw new ServerRuntimeException("invariant - readers/readReferences start 1");
            readerReferences.put(readerName, new Integer(1));
            readers.put(readerName, new ReferenceDataReader<K,V>(readerName, bootstrapServers, referenceDataTopic, unpackValue));
            readers.get(readerName).start();
          }
        else
          {
            if (readers.get(readerName) == null) throw new ServerRuntimeException("invariant - readers/readReferences start 2");
            readerReferences.put(readerName, new Integer(readerReferences.get(readerName).intValue() + 1));
          }
        return (ReferenceDataReader<K,V>) readers.get(readerName);
      }
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  // to remove once cleaned up
  protected ReferenceDataReader(String readerName, String readerKey, String bootstrapServers, String referenceDataTopic, UnpackValue<V> unpackValue){
  	this(readerName,bootstrapServers,referenceDataTopic,unpackValue);
  }
  protected ReferenceDataReader(String readerName, String bootstrapServers, String referenceDataTopic, UnpackValue<V> unpackValue)
  {
    //
    //  converter
    //

    if (System.getProperty("nglm.converter") != null && System.getProperty("nglm.converter").equals("Avro"))
      {
        Map<String, String> avroConverterConfigs = new HashMap<String,String>();
        avroConverterConfigs.put("schema.registry.url", System.getProperty("nglm.schemaRegistryURL"));
        this.converter = new AvroConverter();
        this.converter.configure(avroConverterConfigs, false);
      }
    else
      {
        this.converter = new JsonConverter();
        this.converter.configure(Collections.<String, Object>emptyMap(), false);
      }
    
    //
    //  arguments
    //

    this.readerName = readerName;
    this.referenceDataTopic = referenceDataTopic;
    this.unpackValue = unpackValue;

    //
    //  set up consumer
    //

    Properties consumerProperties = new Properties();
    consumerProperties.put("bootstrap.servers", bootstrapServers);
    consumerProperties.put("auto.offset.reset", "earliest");
    consumerProperties.put("enable.auto.commit", "false");
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    this.consumerProperties = consumerProperties;
    this.consumer = new KafkaConsumer<>(this.consumerProperties);

  }

  /*****************************************
  *
  *  start
  *
  *****************************************/

  protected void start()
  {
    //
    //  read initial data
    //

    readReferenceData(true);

    //
    //  read updates
    //

    Runnable referenceDataReader = new Runnable() { @Override public void run() { readReferenceData(false); } };
    readerThread = new Thread(referenceDataReader, "ReferenceDataReader-" + readerName);
    readerThread.start();
  }
  
  /*****************************************
  *
  *  close
  *
  *****************************************/

  public void close()
  {
    /****************************************
    *
    *  respect reference count
    *
    ****************************************/

    boolean performClose = false;
    synchronized (readersLock)
      {
        if (! readerReferences.containsKey(readerName)) throw new ServerRuntimeException("invariant - readers/readReferences close 1");
        if (! readers.containsKey(readerName)) throw new ServerRuntimeException("invariant - readers/readReferences close 2");
        readerReferences.put(readerName, new Integer(readerReferences.get(readerName).intValue() - 1));
        if (readerReferences.get(readerName).intValue() == 0)
          {
            readerReferences.remove(readerName);
            readers.remove(readerName);
            performClose = true;
          }
      }

    /****************************************
    *
    *  perform close (if necessary)
    *
    ****************************************/

    if (performClose)
      {
        //
        //  mark stopRequested
        //

        stopRequested = true;

        //
        //  wake sleeping poll (if necessary)
        //

        if (consumer != null) consumer.wakeup();

        //
        //  wait (forever) for reader thread to finish
        //

        try
          {
            if (readerThread != null) readerThread.join();
          }
        catch (InterruptedException e)
          {
            // nothing
          }

        //
        //  close
        //

        if (consumer != null) consumer.close();
      }
  }

  /*****************************************
  *
  *  readReferenceData
  *
  *****************************************/

  private void readReferenceData(boolean readInitialTopicRecords)
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
            log.info("No records found.  Skipping initial read for {}", referenceDataTopic);
            return;
          }
      }

    //
    //  initialize consumedOffsets
    //
        
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
        /*****************************************
        *
        *  poll
        *
        *****************************************/

        ConsumerRecords<byte[], byte[]> records= ConsumerRecords.<byte[], byte[]>empty();
        try
          {
            records = consumer.poll(5000);
          }
        catch (WakeupException e)
          {
            log.info("wakeup while reading topic "+referenceDataTopic);
          }

        /*****************************************
        *
        *  process
        *
        *****************************************/

        Date now = SystemTime.getCurrentTime();
        for (ConsumerRecord<byte[], byte[]> record : records)
          {
            /*****************************************
            *
            *  parse
            *
            *****************************************/

            ReferenceDataRecord referenceDataRecord = new ReferenceDataRecord(record);

            /*****************************************
            *
            *  update referenceData
            *
            *****************************************/

            synchronized (this)
              {
                /*****************************************
                *
                *  recordsForKey
                *
                *****************************************/

                Set<ReferenceDataRecord> recordsForKey = (referenceData.get(referenceDataRecord.getKey()) != null) ? referenceData.get(referenceDataRecord.getKey()) : new HashSet<ReferenceDataRecord>();

                /*****************************************
                *
                *  remove old and/or deleted records
                *
                *****************************************/

                //
                //  identify records to remove
                //

                Set<ReferenceDataRecord> recordsToRemove = new HashSet<ReferenceDataRecord>();
                for (ReferenceDataRecord recordForKey : recordsForKey)
                  {
                    //
                    //  deleted record
                    //

                    if (referenceDataRecord.getDeleted() && referenceDataRecord.getEffectiveStartDate().equals(recordForKey.getEffectiveStartDate()) && referenceDataRecord.getEffectiveEndDate().equals(recordForKey.getEffectiveEndDate()))
                      {
                        recordsToRemove.add(recordForKey);
                      }

                    //
                    //  expired record
                    //

                    if (recordForKey.getEffectiveEndDate().compareTo(now) <= 0)
                      {
                        recordsToRemove.add(recordForKey);
                      }
                   
                  }

                //
                //  remove records (if necessary)
                //

                for (ReferenceDataRecord recordToRemove : recordsToRemove)
                  {
                    recordsForKey.remove(recordToRemove);
                  }

                /*****************************************
                *
                *  add new record (if necessary)
                *
                *****************************************/

                if (! referenceDataRecord.getDeleted() && referenceDataRecord.getEffectiveEndDate().compareTo(now) > 0)
                  {
                    recordsForKey.remove(referenceDataRecord);
                    recordsForKey.add(referenceDataRecord);
                  }

                /*****************************************
                *
                *  update referenceData
                *
                *****************************************/

                updateKey(referenceDataRecord.getKey(), recordsForKey);
              }
            
            //
            //  offsets
            //
            
            consumedOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
          }

        //
        //  consumed all available?
        //

        Set<TopicPartition> assignedPartitions = consumer.assignment();
        Map<TopicPartition,Long> availableOffsets = getEndOffsets();
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

  // so far, those 3 methods COPY/PAST from GUIService class, as lot of this code
  private void assignAllTopicPartitions(){
    if(this.consumer!=null){
      Set<TopicPartition> partitions = new HashSet<>();
      List<PartitionInfo> partitionInfos=null;
      while(partitionInfos==null){
        try{
          partitionInfos=this.consumer.partitionsFor(this.referenceDataTopic, Duration.ofSeconds(5));
        }catch (TimeoutException e){
          // a kafka broker might just be down, consumer.partitionsFor() can ends up timeout trying on this one
          reconnectConsumer();
          log.warn("timeout while getting topic partitions", e.getMessage());
        }catch (WakeupException e){
        }
      }
      for (PartitionInfo partitionInfo : partitionInfos) {
        partitions.add(new TopicPartition(this.referenceDataTopic, partitionInfo.partition()));
      }
      this.consumer.assign(partitions);
    }else{
      log.error("NULL kafka consumer while assigning topic partitions "+this.referenceDataTopic);
    }
  }
  private Map<TopicPartition, Long> getEndOffsets(){
    if(this.consumer==null){
      log.error("NULL kafka consumer reading end offets");
      return Collections.emptyMap();
    }
    while(true){
      try{
        return this.consumer.endOffsets(this.consumer.assignment());
      }catch (TimeoutException e){
        // a kafka broker might just be down, consumer.endOffsets() can ends up timeout trying on this one
        reconnectConsumer();
        log.warn("timeout while getting end offsets", e.getMessage());
      }catch (WakeupException e){
      }
    }
  }
  private void reconnectConsumer(){
    if(this.consumer!=null) this.consumer.close();
    this.consumer=new KafkaConsumer<byte[], byte[]>(this.consumerProperties);
    assignAllTopicPartitions();
  }

  /*****************************************
  *
  *  putKey
  *
  *****************************************/

  protected void updateKey(K key, Set<ReferenceDataRecord> recordsForKey)
  {
    if (recordsForKey.size() > 0)
      {
        referenceData.put(key, recordsForKey);
      }
    else
      {
        referenceData.remove(key);
      }
  }

  /*****************************************
  *
  *  getAll
  *
  *****************************************/

  public Map<K,V> getAll()
  {
    synchronized (this)
      {
        Map<K,V> result = new HashMap<K,V>();
        for (K key : referenceData.keySet())
          {
            result.put(key, get(key));
          }
        return result;
      }
  }
  
  /*****************************************
  *
  *  get
  *
  *****************************************/

  public V get(K key)
  {
    synchronized (this)
      {
        /*****************************************
        *
        *  recordsForKey
        *
        *****************************************/
        
        Set<ReferenceDataRecord> recordsForKey = (referenceData.get(key) != null) ? referenceData.get(key) : new HashSet<ReferenceDataRecord>();

        /*****************************************
        *
        *  remove old records
        *
        *****************************************/

        //
        //  identify records to remove
        //

        Date now = SystemTime.getCurrentTime();
        Set<ReferenceDataRecord> recordsToRemove = new HashSet<ReferenceDataRecord>();
        for (ReferenceDataRecord recordForKey : recordsForKey)
          {
            //
            //  expired record
            //

            if (recordForKey.getEffectiveEndDate().compareTo(now) <= 0)
              {
                recordsToRemove.add(recordForKey);
              }
          }

        //
        //  remove records (if necessary)
        //

        for (ReferenceDataRecord recordToRemove : recordsToRemove)
          {
            recordsForKey.remove(recordToRemove);
          }
        
        //
        //  update referenceData
        //

        updateKey(key, recordsForKey);

        /*****************************************
        *
        *  find candidate result
        *
        *****************************************/

        ReferenceDataRecord result = null;
        for (ReferenceDataRecord recordForKey : recordsForKey)
          {
            if ((recordForKey.getEffectiveStartDate().compareTo(now) <= 0) && (now.compareTo(recordForKey.getEffectiveEndDate()) < 0))
              {
                if ((result == null) || (result.getKafkaTimestamp() < recordForKey.getKafkaTimestamp()))
                  {
                    result = recordForKey;
                  }
              }
          }

        /*****************************************
        *
        *  return
        *
        *****************************************/

        return (result != null) ? result.getValue() : null;
      }
  }

  /*****************************************
  *
  *  containsKey
  *
  *****************************************/

  public boolean containsKey(K key)
  {
    return get(key) != null;
  }

  /*****************************************
  *
  *  class ReferenceDataRecord
  *
  *****************************************/

  protected class ReferenceDataRecord
  {
    //
    //  data
    //

    private K key;
    private V value;
    private long kafkaTimestamp;
    private boolean deleted;
    private Date effectiveStartDate;
    private Date effectiveEndDate;

    //
    //  accessors
    //

    private K getKey() { return key; }
    private V getValue() { return value; }
    private long getKafkaTimestamp() { return kafkaTimestamp; }
    private boolean getDeleted() { return deleted; }
    private Date getEffectiveStartDate() { return effectiveStartDate; }
    private Date getEffectiveEndDate() { return effectiveEndDate; }

    //
    //  constructor
    //

    private ReferenceDataRecord(ConsumerRecord<byte[], byte[]> record)
    {
      V value = unpackValue.unpack(converter.toConnectData(record.topic(), record.value()));
      this.key = value.getKey();
      this.value = value;
      this.kafkaTimestamp = record.timestamp();
      this.deleted = value.getDeleted();
      this.effectiveStartDate = value.getEffectiveStartDate();
      this.effectiveEndDate = value.getEffectiveEndDate();
    }

    /*****************************************
    *
    *  equals/hashCode
    *
    *****************************************/

    //
    //  equals
    //

    public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj.getClass() == ReferenceDataRecord.class)      
        {
          ReferenceDataRecord referenceDataRecord = (ReferenceDataRecord) obj;
          result = true;
          result = result && Objects.equals(effectiveStartDate, referenceDataRecord.getEffectiveStartDate());
          result = result && Objects.equals(effectiveEndDate, referenceDataRecord.getEffectiveEndDate());
        }
      return result;
    }

    //
    //  hashCode
    //

    public int hashCode()
    {
      return Objects.hash(value.getEffectiveStartDate(), value.getEffectiveEndDate());
    }

    //
    //  toString
    //

    public String toString()
    {
      StringBuilder builder = new StringBuilder();
      builder.append("{ value : " + value + ", ");
      builder.append("hashCode : " + this.hashCode() + ", ");
      builder.append("effectiveStartDate : " + effectiveStartDate.getTime() + ", ");
      builder.append("effectiveEndDate : " + effectiveEndDate.getTime() + " ");
      builder.append("}");
      return builder.toString();
    }
  }
  
  /*****************************************
  *
  *  interface UnpackValue
  *
  *****************************************/

  public interface UnpackValue<V>
  {
    public V unpack(SchemaAndValue schemaAndValue);
  }
}
