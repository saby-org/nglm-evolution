package com.evolving.nglm.evolution.purchase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.evolution.DeliveryManagerDeclaration;
import com.evolving.nglm.evolution.DeliveryRequest;

public class RequestPusher
{

  /*****************************************
  *
  *  logger
  *
  *****************************************/
  
  private static final Logger log = LoggerFactory.getLogger(RequestPusher.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private int threadNumber = 5;   //TODO : make this configurable 
  private final String callbackID = "callbackID";
  private Map<String, KafkaProducer> producers = new HashMap<String/*delivery type*/, KafkaProducer>();
  private Map<String, Thread> consumerThreads = new HashMap<String/*responseTopic+"_"+deliveryType+"_"+index*/, Thread>(); //TODO SCH : this map should be initialized and filled in the constructor !!!
  
  private IDRCallback callback;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public RequestPusher(IDRCallback callback){
    this.callback = callback;
  }
  
  /*****************************************
  *
  *  pushRequest
  *  
  *****************************************/
  
  public void pushRequest(DeliveryRequest request){
    
    // ---------------------------------
    //
    // Flag request with 
    //
    // ---------------------------------
    if(callback.getIdentifier() != null && !callback.getIdentifier().isEmpty()){
      if(request.getDiplomaticBriefcase() == null){
        request.setDiplomaticBriefcase(new HashMap<String, String>());
      }
      request.getDiplomaticBriefcase().put(callbackID, callback.getIdentifier());
    }else{
      log.warn("RequestPusher.pushRequest(DeliveryRequest "+request+", IDRCallback callback) : IDRCallback.getIdentifier() is null or empty => will not be able to handle reponse");
    }
    
    // ---------------------------------
    //
    // Get all informations we need
    //
    // ---------------------------------
    
    log.debug("RequestPusher.pushRequest(DeliveryRequest "+request+", IDRCallback callback) : getting needed information");
    DeliveryManagerDeclaration deliveryManagerDeclaration = Deployment.getDeliveryManagers().get(request.getDeliveryType());
    String requestTopic = deliveryManagerDeclaration.getRequestTopic();
    String responseTopic = deliveryManagerDeclaration.getResponseTopic();
    log.debug("RequestPusher.pushRequest(...) : getting needed information DONE");
    
    // ---------------------------------
    //
    // Send request
    //
    // ---------------------------------

    // get kafka producer
    
    log.debug("RequestPusher.pushRequest(...) : getting KafkaProducer to topic "+requestTopic);
    KafkaProducer kafkaProducer = producers.get(request.getDeliveryType());
    if(kafkaProducer == null){
      Properties kafkaProducerProperties = new Properties();
      kafkaProducerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
      kafkaProducerProperties.put("acks", "all");
      kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      kafkaProducer = new KafkaProducer<byte[], byte[]>(kafkaProducerProperties);
      producers.put(request.getDeliveryType(), kafkaProducer);
    }
    log.debug("RequestPusher.pushRequest(...) : getting KafkaProducer to topic "+requestTopic+" DONE");

    // send the request
    
    log.info("RequestPusher.pushRequest(...) : sending request (DeliveryType = "+request.getDeliveryType()+")");
    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(requestTopic, StringKey.serde().serializer().serialize(requestTopic, new StringKey(request.getDeliveryRequestID())), ((ConnectSerde<DeliveryRequest>)deliveryManagerDeclaration.getRequestSerde()).serializer().serialize(requestTopic, request))); 
    log.debug("RequestPusher.pushRequest(...) : sending request DONE");

    // ---------------------------------
    //
    // Get response
    //
    // ---------------------------------


    // check consumer exists
    
    boolean consumerCreated = false;
    String prefix = responseTopic+"_"+request.getDeliveryType()+"_";
    Pattern p = Pattern.compile(prefix+".*");
    log.debug("RequestPusher.pushRequest(...) : searching for consumer "+prefix);
    for (String key : consumerThreads.keySet()) {
      if (p.matcher(key).matches()) {
        log.debug("RequestPusher.pushRequest(...) : consumer "+prefix+" FOUND !!!!!!!!!!!!!!!!!!!!!!!!");
        consumerCreated = true;
        break;
      }else{
        log.debug("RequestPusher.pushRequest(...) : consumer "+key+" does not match "+prefix+".*");
      }
    }

    if(!consumerCreated){
      
      if(callback.getIdentifier() != null && !callback.getIdentifier().isEmpty()){
        
        for(int index = 0; index < threadNumber; index++){
          Thread consumerThread = new Thread(new Runnable(){
            @Override
            public void run()
            {
              // create consumer
              
              log.info(Thread.currentThread().getId()+" - RequestPusher.pushRequest(...) : creating KafkaConsumer of topic "+responseTopic);
              Properties consumerProperties = new Properties();
              consumerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
              consumerProperties.put("group.id", prefix+"requestReader");
              consumerProperties.put("auto.offset.reset", "earliest");
              consumerProperties.put("enable.auto.commit", "false");
              consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
              consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
              KafkaConsumer consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties);
              consumer.subscribe(Arrays.asList(responseTopic));
              log.debug(Thread.currentThread().getId()+" - RequestPusher.pushRequest(...) : creating KafkaConsumer of topic "+responseTopic+" DONE");

              while(true){

                // poll
                
                log.debug(Thread.currentThread().getId()+" - RequestPusher.pushRequest(...) : getting records from topic "+responseTopic);
                ConsumerRecords<byte[], byte[]> fileRecords = consumer.poll(5000);
                log.debug(Thread.currentThread().getId()+" - RequestPusher.pushRequest(...) : getting records from topic "+responseTopic+" DONE ("+fileRecords.count()+" records)");

                //  process records
                
                log.debug(Thread.currentThread().getId()+" - RequestPusher.pushRequest(...) : handling "+fileRecords.count()+" records ...");
                for (ConsumerRecord<byte[], byte[]> fileRecord : fileRecords)
                  {
                    //  parse
                    DeliveryRequest response = deliveryManagerDeclaration.getRequestSerde().deserializer().deserialize(responseTopic, fileRecord.value());
                    if(callback.getIdentifier() != null && response.getDiplomaticBriefcase() != null && callback.getIdentifier().equals(response.getDiplomaticBriefcase().get(callbackID))){
                      log.info(Thread.currentThread().getId()+" - RequestPusher.pushRequest(...) : found one record that needs to be handled (DeliveryType = "+response.getDeliveryType()+", DeliveryStatus = "+response.getDeliveryStatus()+")");
                      callback.onDRResponse(response);
                    }
                  }
                log.debug(Thread.currentThread().getId()+" - RequestPusher.pushRequest(...) : handling "+fileRecords.count()+" records DONE");
                
              }
            }
          }, "requestPusher_consumer_"+responseTopic+"_"+index);
          consumerThread.start();
          consumerThreads.put(prefix+index, consumerThread);
          
        }

      }else{
        
        log.info(Thread.currentThread().getId()+" - RequestPusher.pushRequest(...) : KafkaConsumer not created (IDRCallback.getIdentifier() is null or empty)");

      }
      
    }else{
      
      log.debug(Thread.currentThread().getId()+" - RequestPusher.pushRequest(...) : KafkaConsumer of topic "+responseTopic+" already exists");
    
    }
    
  }
  
}
