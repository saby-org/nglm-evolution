package com.evolving.nglm.evolution.backup.kafka;

import com.evolving.nglm.core.Deployment;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class BackupTopic {
  private static final Logger log = LoggerFactory.getLogger(BackupTopic.class);

	private static final KafkaConsumer<byte[],byte[]> kafkaConsumer;
	static {
		Properties consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Deployment.getBrokerServers());
		consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		kafkaConsumer = new KafkaConsumer<>(consumerProperties);
	}

	private static final KafkaProducer<byte[], byte[]> kafkaProducer;
	static{
		Properties producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Deployment.getBrokerServers());
		producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
		producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,Deployment.getBackupManagerRequestTimeoutMS());
		producerConfig.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,Integer.MAX_VALUE);
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		kafkaProducer = new KafkaProducer<>(producerConfig);
	}


	public static void backupTopic(String topicName, String filefullpath) throws IOException {
    Collection<TopicPartition> topicPartitions = getTopicPartition(topicName, kafkaConsumer);
    if (topicPartitions.isEmpty()) {
      log.info("No partitions for topic " + topicName + " nothing to do");
      return;
    }
		File file = new File(filefullpath);
		if (file.exists()){
			log.info("Backup file " + filefullpath + " already exists, erase");
			Files.delete(file.toPath());
		}
  	Files.createFile(file.toPath());

		OutputStream out = new GZIPOutputStream(new FileOutputStream(file));
		Encoder encoder = EncoderFactory.get().binaryEncoder(out,null);// avro encoder

		// save the current end position of the topic, we will stop there once reached
		Map<TopicPartition,Long> endAtStart = getEndOffsets(topicPartitions,kafkaConsumer);

		long nbRecords=0;
		kafkaConsumer.assign(topicPartitions);
		kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());
		while(!doesConsumerReachedPosition(kafkaConsumer,endAtStart)){
			for(ConsumerRecord<byte[],byte[]> record:kafkaConsumer.poll(Duration.ofSeconds(5)).records(topicName)){
				encoder.writeBytes(record.key());
				encoder.writeBytes(record.value());
				nbRecords++;
			}
		}
		out.close();
    log.info("=== Backup done, "+nbRecords+" records saved in "+file);
	}

	public static void restoreBackupFileToTopic(String topicName, String filefullpath) throws IOException{

		File file = new File(filefullpath);
		if(!file.exists()){
			log.info(file+" file does not exist");
			return;
		}

		InputStream in = new GZIPInputStream(new FileInputStream(file));
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in,null);

		long nbRecords=0;
		ByteBuffer buffer = null;//100KB buffer init, org.apache.avro.io.BinaryDecoder.readBytes(ByteBuffer)
		while(!decoder.isEnd()){
			//key
			buffer = decoder.readBytes(buffer);
			byte[] keyByte = new byte[buffer.remaining()];
			buffer.get(keyByte);
			//value
			buffer = decoder.readBytes(buffer);
			byte[] valueByte = new byte[buffer.remaining()];
			buffer.get(valueByte);

			kafkaProducer.send(new ProducerRecord<>(topicName,keyByte,valueByte));

			nbRecords++;
		}

		kafkaProducer.flush();
		in.close();
		log.info("=== Backup restore done, "+nbRecords+" records load from "+file+" to topic "+topicName);
	}

	public static List<PartitionInfo> getPartitionsInfo(String topic, KafkaConsumer<?,?> kafkaConsumer){
	  try {
		  return kafkaConsumer.partitionsFor(topic,Duration.ofSeconds(Integer.MAX_VALUE));
	  } catch (KafkaException  ex) {
	    log.info("Cannot get partitions for " + topic + " (wrong topic name ?)");
	    return new ArrayList<>();
	  }
	}

	public static Map<TopicPartition,Long> getEndOffsets(Collection<TopicPartition> topicPartitions, KafkaConsumer<?,?> kafkaConsumer){
		return kafkaConsumer.endOffsets(topicPartitions,Duration.ofSeconds(Integer.MAX_VALUE));
	}

	public static Collection<TopicPartition> getTopicPartition(String topic, KafkaConsumer<?,?> kafkaConsumer){
		Set<TopicPartition> result = new HashSet<>();
		for(PartitionInfo partitionInfo:getPartitionsInfo(topic,kafkaConsumer)) result.add(new TopicPartition(partitionInfo.topic(),partitionInfo.partition()));
		return result;
	}

	public static boolean doesConsumerReachedPosition(KafkaConsumer<?,?> kafkaConsumer, Map<TopicPartition,Long> position){
		for(Map.Entry<TopicPartition,Long> entry:position.entrySet()){
			if(kafkaConsumer.position(entry.getKey())<entry.getValue()) return false;
		}
		return true;
	}
	
	public static void main(String[] args) {
	  if (args.length < 2) {
	    System.err.println("This commandes requires 2 arguments : topicname backup filename");
	    return;
	  }
	  String topic = args[0];
	  String filename = args[1];
	  try
      {
        restoreBackupFileToTopic(topic, filename);
      }
    catch (IOException e)
      {
        System.err.println("An error occured while restoring data : " + e.getLocalizedMessage());
      }
	}

}
