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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

  protected static volatile Map<String, ReferenceDataReader> readers = new HashMap<>();

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
  protected ConcurrentMap<K,V> referenceData = new ConcurrentHashMap<>();

  /*****************************************
  *
  *  startReader
  *
  *****************************************/

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

        for (ConsumerRecord<byte[], byte[]> record : records)
          {
            /*****************************************
            *
            *  parse
            *
            *****************************************/

            ReferenceDataRecord referenceDataRecord = new ReferenceDataRecord(record);

            //
            //  remove or add
            //

            if(referenceDataRecord.getDeleted()){
              remove(referenceDataRecord.getKey());
            }else{
              put(referenceDataRecord.getKey(),referenceDataRecord.getValue());
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

    private ReferenceDataRecord(ConsumerRecord<byte[], byte[]> record)
    {
      V value = unpackValue.unpack(converter.toConnectData(record.topic(), record.value()));
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
