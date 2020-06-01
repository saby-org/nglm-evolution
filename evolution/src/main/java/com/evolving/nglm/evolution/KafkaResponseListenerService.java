package com.evolving.nglm.evolution;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class KafkaResponseListenerService<K,V> {

  private static final Logger log = LoggerFactory.getLogger(KafkaResponseListenerService.class);


  // to stop the service
  private volatile boolean isRunning;
  // the Thread doing the job
  private Thread queueCheckerThread;
  // the consumer of topic listen
  KafkaConsumer<byte[],byte[]> kafkaConsumer;
  String topic;
  Serde<K> keySerde;
  Serde<V> valueSerde;

  private Set<BlockingResponse> waitingForResponse =  new HashSet<>();

  KafkaResponseListenerService(String broker, String topic, Serde<K> keySerde, Serde<V> valueSerde){
    synchronized (this){
      Properties topicConsumerProperties = new Properties();
      topicConsumerProperties.put("bootstrap.servers", broker);
      topicConsumerProperties.put("auto.offset.reset", "latest");
      topicConsumerProperties.put("enable.auto.commit", "false");
      topicConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      topicConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      kafkaConsumer = new KafkaConsumer<>(topicConsumerProperties);
      this.topic=topic;
      this.keySerde = keySerde;
      this.valueSerde = valueSerde;
    }
    queueCheckerThread=new Thread(this::runResponseCheck);

  }

  public Future<V> addWithOnKeyFilter(Predicate<K> keyPredicate){
    return this.addWithOnKeyValueFilter((k,v)->keyPredicate.test(k));
  }

  public Future<V> addWithOnValueFilter(Predicate<V> valuePredicate){
    return this.addWithOnKeyValueFilter((k,v)->valuePredicate.test(v));
  }

  public Future<V> addWithOnKeyValueFilter(BiPredicate<K,V> keyValuePredicate){
    BlockingResponse blockingResponse = new BlockingResponse(keyValuePredicate);
    synchronized (waitingForResponse){
      waitingForResponse.add(blockingResponse);
    }
    return blockingResponse;
  }

  public void start() {
    synchronized (this){
      if(isRunning) return;
      this.isRunning=true;
      queueCheckerThread.start();
    }
  }

  public void stop(){
    synchronized (this){
      this.isRunning=false;
      kafkaConsumer.close();
      queueCheckerThread.interrupt();
    }
  }

  private void runResponseCheck(){

    synchronized (this){
      // consume all partitions of the topic
      Set<TopicPartition> partitions = new HashSet<>();
      List<PartitionInfo> partitionInfos=null;
      while(partitionInfos==null){
        try{
          partitionInfos=kafkaConsumer.partitionsFor(this.topic, Duration.ofSeconds(5));
        }catch (org.apache.kafka.common.errors.TimeoutException e){
          // a kafka broker might just be down, consumer.partitionsFor() can ends up timeout trying on this one
          log.warn("timeout while getting topic partitions", e.getMessage());
        }catch (WakeupException e){
        }
      }
      for(PartitionInfo partitionInfo:partitionInfos){
        partitions.add(new TopicPartition(topic, partitionInfo.partition()));
      }
      kafkaConsumer.assign(partitions);
    }

    while(isRunning){

      // skip kafka topic read if no job waiting
      synchronized (waitingForResponse){
        if(waitingForResponse.isEmpty()){
          kafkaConsumer.seekToEnd(kafkaConsumer.assignment());
          if(log.isDebugEnabled()) log.debug("KafkaResponseListenerService.runResponseCheck : no waiting jobs, not polling records from "+topic);
          try {
            Thread.sleep(50);// a release CPU
          } catch (InterruptedException e) {}
          continue;
        }
      }

      ConsumerRecords<byte[],byte[]> consumerRecords = kafkaConsumer.poll(5000);

      for(ConsumerRecord<byte[],byte[]> record:consumerRecords){

        K key=keySerde.deserializer().deserialize(topic,record.key());
        V value=valueSerde.deserializer().deserialize(topic,record.value());

        if(log.isDebugEnabled()) log.debug("KafkaResponseListenerService.runResponseCheck : "+topic+" ("+key+")("+value+")");

        synchronized (waitingForResponse){
          Iterator<BlockingResponse> iterator = waitingForResponse.iterator();
          while (iterator.hasNext()){
            BlockingResponse toCheck = iterator.next();
            // cleaning already processed ones
            if(toCheck.isCancelled()||toCheck.isDone()){
              iterator.remove();
              continue;
            }
            // check if wait this response
            if(toCheck.getKeyValuePredicate().test(key,value)){
              synchronized (toCheck){
                toCheck.setResponse(value);
                toCheck.notifyAll();
              }
              iterator.remove();
            }
          }
        }
      }

      if(log.isDebugEnabled()) log.debug("KafkaResponseListenerService.runResponseCheck : waiting queue size "+waitingForResponse.size());
      if(waitingForResponse.size()>1000) log.warn("KafkaResponseListenerService.runResponseCheck : check me, seems more than 1000 waiting requests");

    }
    log.info("KafkaResponseListenerService.runResponseCheck : stopped");
  }

  private class BlockingResponse implements Future<V> {

    private V response;
    private BiPredicate<K,V> keyValuePredicate;
    private boolean canceled=false;
    private boolean done=false;

    private BlockingResponse(BiPredicate keyValuePredicate){
      this.keyValuePredicate = keyValuePredicate;
    }

    private BiPredicate<K,V> getKeyValuePredicate() { return keyValuePredicate; }
    private void setResponse(V response){ this.response=response; }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      synchronized (this){
        canceled=true;
        this.notifyAll();
      }
      return true;
    }

    @Override
    public boolean isCancelled() {
      return canceled;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public V get(){
      V result = null;
      try{
        result = get(0, TimeUnit.MILLISECONDS);
      }catch (InterruptedException|ExecutionException|TimeoutException ex){
      }
      return result;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      if(response!=null){
        synchronized (this){
          done=true;
          return response;
        }
      }
      synchronized (this){
        try{
          this.wait(unit.toMillis(timeout));
        }catch (InterruptedException ex){}
      }
      synchronized (this){
        if(response==null){
          done=true;
          if(canceled) throw new InterruptedException();
          throw new TimeoutException();
        }
        done=true;
        if(canceled) throw new InterruptedException();
        return response;
      }
    }
  }
}