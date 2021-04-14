/****************************************************************************
*
*  ReferenceDataReader.java
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import io.confluent.connect.avro.AvroConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ReferenceDataReader<K, V extends ReferenceDataValue<K>>
{

  private static final Logger log = LoggerFactory.getLogger(ReferenceDataReader.class);

  private volatile boolean stopRequested = false;
  private String readerName;
  private String referenceDataTopic;
  private Properties consumerProperties;
  private KafkaConsumer<byte[], byte[]> consumer = null;
  private Map<TopicPartition,Long> consumedOffsets;
  private Thread readerThread = null;
  private Converter converter;
  private UnpackValue<V> unpackValue;
  protected ConcurrentMap<K,V> referenceData = new ConcurrentHashMap<>();

  // singletons
  protected static final Map<String, ReferenceDataReader> readers = new HashMap<>();
  public static <K, V extends ReferenceDataValue<K>> ReferenceDataReader<K,V> startReader(String readerName, String bootstrapServers, String referenceDataTopic, UnpackValue<V> unpackValue) {
    ReferenceDataReader toRet = readers.get(readerName);
    if(toRet==null){
      synchronized (readers){
        toRet = readers.get(readerName);
        if(toRet==null){
          toRet=new ReferenceDataReader<>(readerName, bootstrapServers, referenceDataTopic, unpackValue);
          toRet.start();
          readers.put(readerName, toRet);
        }
      }
    }
    return toRet;
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
    consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,com.evolving.nglm.evolution.Deployment.getGuiConfigurationInitialConsumerMaxPollRecords());// speed up initial read
    consumerProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,com.evolving.nglm.evolution.Deployment.getGuiConfigurationInitialConsumerMaxFetchBytes());// speed up initial read
    consumerProperties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,com.evolving.nglm.evolution.Deployment.getGuiConfigurationInitialConsumerMaxFetchBytes());// speed up initial read
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

    // once initial read over can lower a lot memory, we expect one record at a time update
    consumerProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,1024);
    consumerProperties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,1024);
    reconnectConsumer();

    //
    //  read updates
    //

    Runnable referenceDataReader = new Runnable() { @Override public void run() { readReferenceData(false); } };
    readerThread = new Thread(referenceDataReader, "ReferenceDataReader-" + readerName);
    readerThread.start();
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


    while (!stopRequested)
      {

        ConsumerRecords<byte[], byte[]> records= ConsumerRecords.<byte[], byte[]>empty();
        try
          {
            records = consumer.poll(5000);
          }
        catch (WakeupException e)
          {
            if (!stopRequested) log.info("wakeup while reading topic "+referenceDataTopic);
          }

        if (stopRequested) continue;

        Map<Bytes, byte[]> toLoad = new HashMap<>();//key is Bytes and not byte[] directly, primitive byte array would not behave as expected regarding Map contract (hashcode and equals)
        int sizeConsumed=0;// for debug logging
        for(ConsumerRecord<byte[],byte[]> record:records){
          toLoad.put(new Bytes(record.key()),record.value());
          sizeConsumed+=record.serializedKeySize()+record.serializedValueSize();
          consumedOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
        }

        if(log.isDebugEnabled()) log.debug("will process "+toLoad.size()+" records, after reading "+sizeConsumed+" bytes for "+records.count()+" total records");

        Iterator<Map.Entry<Bytes,byte[]>> groupedRecordIterator = toLoad.entrySet().iterator();
        while(groupedRecordIterator.hasNext()){

          Map.Entry<Bytes,byte[]> record = groupedRecordIterator.next();
          groupedRecordIterator.remove();

          ReferenceDataRecord referenceDataRecord = new ReferenceDataRecord(referenceDataTopic,record.getValue());
          if(referenceDataRecord.getDeleted()){
            remove(referenceDataRecord.getKey());
          }else{
            put(referenceDataRecord.getKey(),referenceDataRecord.getValue());
          }

        }


        if(readInitialTopicRecords){
          //
          //  consumed all available?
          //

          Map<TopicPartition,Long> availableOffsets = getEndOffsets();
          boolean consumedAllAvailable = true;
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
          if(consumedAllAvailable) return;
        }

      }

  }

  // so far, those 3 methods COPY/PAST from GUIService class, as lot of this code
  private void assignAllTopicPartitions(){
    boolean freshAsignment = consumedOffsets == null;
    if(this.consumer!=null){
      Set<TopicPartition> partitions = new HashSet<>();
      List<PartitionInfo> partitionInfos=null;
      while(partitionInfos==null){
        try{
          partitionInfos=consumer.partitionsFor(referenceDataTopic, Duration.ofSeconds(5));
        }catch (TimeoutException e){
          // a kafka broker might just be down, consumer.partitionsFor() can ends up timeout trying on this one
          reconnectConsumer();
          log.warn("timeout while getting topic partitions", e.getMessage());
        }catch (WakeupException e){
        }
      }
      if(freshAsignment) consumedOffsets = new HashMap<>();
      for (PartitionInfo partitionInfo : partitionInfos) {
        TopicPartition topicPartition = new TopicPartition(referenceDataTopic, partitionInfo.partition());
        partitions.add(topicPartition);
      }
      consumer.assign(partitions);
      for(Map.Entry<TopicPartition,Long> position:consumedOffsets.entrySet()){
        if(freshAsignment) consumedOffsets.put(position.getKey(), consumer.position(position.getKey()) - 1L);
        consumer.seek(position.getKey(),position.getValue());
      }
    }else{
      log.error("NULL kafka consumer while assigning topic partitions "+this.referenceDataTopic);
    }
  }
  private Map<TopicPartition, Long> getEndOffsets(){
    if(consumer==null){
      log.error("NULL kafka consumer reading end offets");
      return Collections.emptyMap();
    }
    while(true){
      try{
        return consumer.endOffsets(consumer.assignment());
      }catch (TimeoutException e){
        // a kafka broker might just be down, consumer.endOffsets() can ends up timeout trying on this one
        reconnectConsumer();
        log.warn("timeout while getting end offsets", e.getMessage());
      }catch (WakeupException e){
      }
    }
  }
  private void reconnectConsumer(){
    if(consumer!=null) consumer.close();
    consumer=new KafkaConsumer<byte[], byte[]>(this.consumerProperties);
    assignAllTopicPartitions();
  }

  protected void put(K key, V value){
    log.info("ReferenceDataReader "+readerName+" put "+key+":"+value);
    referenceData.put(key,value);
  }

  protected void remove(K key){
    log.info("ReferenceDataReader "+readerName+" remove "+key);
    referenceData.remove(key);
  }

  public V get(K key) {
    V toRet = referenceData.get(key);
    if(log.isTraceEnabled()) log.trace("ReferenceDataReader "+readerName+" get "+key+":"+toRet);
    return toRet;
  }

  public Map<K,V> getAll() {
    return new HashMap<>(referenceData);
  }

  /*****************************************
  *
  *  class ReferenceDataRecord
  *
  *****************************************/

  protected class ReferenceDataRecord
  {

    private K key;
    private V value;
    private boolean deleted;

    private K getKey() { return key; }
    private V getValue() { return value; }
    private boolean getDeleted() { return deleted; }

    private ReferenceDataRecord(String recordTopic, byte[] recordValue)
    {
      V value = unpackValue.unpack(converter.toConnectData(recordTopic, recordValue));
      this.key = value.getKey();
      this.value = value;
      this.deleted = value.getDeleted();
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
