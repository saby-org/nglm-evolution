package com.evolving.nglm.evolution.test;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.evolving.nglm.core.NGLMRuntime;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.DeliveryRequest;
import com.evolving.nglm.evolution.RewardManagerRequest;

public class RewardManagerSimulator
{

  public static void main(String[] args)
  {

    String brokerSerers = "fduclos.evolving.com:9092";

    // Response to EE

    //
    // update list (kafka) request producers
    //

    Properties kafkaProducerProperties = new Properties();
    kafkaProducerProperties.put("bootstrap.servers", brokerSerers);
    kafkaProducerProperties.put("acks", "all");
    kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    KafkaProducer rewardProducer = new KafkaProducer<byte[], byte[]>(kafkaProducerProperties);

    String requestTopic = "rewardmanager-request";
    String responseTopic = "rewardmanager-response";
    String prefix = "CommodityDeliveryResponseConsumer_001";
    Thread consumerThread = new Thread(new Runnable()
    {
      private volatile boolean stopping = false;

      @Override
      public void run()
      {
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", brokerSerers);
        consumerProperties.put("group.id", prefix + "_" + "requestReader");
        consumerProperties.put("auto.offset.reset", "earliest");
        consumerProperties.put("enable.auto.commit", "false");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties);
        consumer.subscribe(Arrays.asList(requestTopic));
        System.out.println("CommodityDeliveryManager.addCommodityDeliveryResponseConsumer(...) : added kafka consumer for application ");

        while (!stopping)
          {

            // poll

            long lastPollTime = System.currentTimeMillis();// just to log if exception happened later, can be because of this, but most
                                                           // likely because of rebalance because of new consumer created
            ConsumerRecords<byte[], byte[]> fileRecords = consumer.poll(5000);

            // process records

            try
              {
                for (ConsumerRecord<byte[], byte[]> fileRecord : fileRecords)
                  {
                    // parse
                    RewardManagerRequest request = RewardManagerRequest.serde().deserializer().deserialize(requestTopic, fileRecord.value());
                    request.setDeliveryDate(new Date());
                    request.setDeliveryStatus(DeliveryStatus.Delivered);
                    request.setReturnCodeDetails("Response=<?xml version=\\\"1.0\\\" encoding=\\\"utf-8\\\"?>\\n<methodResponse>\\n<params>\\n<param>\\n<value>\\n<struct>\\n<member>\\n<name>originTransactionID</name>\\n<value><string>6592385600354642</string></value>\\n</member>\\n<member>\\n<name>responseCode</name>\\n<value><i4>104</i4></value>\\n</member>\\n</struct>\\n</value>\\n</param>\\n</params>\\n</methodResponse>\\nSuccessStatus=False\\nStatus=FAILED\\nResponseError=<<NULL>>\\nResponseErrorCode=<<NULL>>\\nExtra=NULL\\n");
                    request.setReturnStatus(0);
                    System.out.println("CommodityDeliveryManager.getCommodityAndPaymentMeanFromDM() : added kafka producer for provider");
                    if (rewardProducer != null)
                      {
                        rewardProducer.send(new ProducerRecord<byte[], byte[]>(responseTopic, StringKey.serde().serializer().serialize(responseTopic, new StringKey(request.getDeliveryRequestID())), ((ConnectSerde<RewardManagerRequest>) request.serde()).serializer().serialize(responseTopic, request)));
                      }

                    //
                    // commit offsets
                    //

                    consumer.commitSync();
                  }
              }
            catch (CommitFailedException ex)
              {
                long lastPoll_ms = System.currentTimeMillis() - lastPollTime;
                System.out.println(Thread.currentThread().getId() + " CommodityDeliveryManager : CommitFailedException catched, can be normal rebalancing or poll time interval too long, last was " + lastPoll_ms + "ms ago");
              }

          }
        consumer.close();
        System.out.println(" CommodityDeliveryManager.addCommodityDeliveryResponseConsumer : STOPPING reading response from " + requestTopic);
      }
    }, "consumer_" + prefix);
    consumerThread.start();
  }

}
