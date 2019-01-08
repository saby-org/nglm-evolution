package com.evolving.nglm.evolution.reports;

import static com.evolving.nglm.evolution.reports.ReportUtils.d;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElementDeserializer;

/**
 * Handles phase 3 of the Report generation, reading a Kafka topic and writing a csv file.
 *
 */
public class ReportCsvWriter {

	private static final Logger log = LoggerFactory.getLogger(ReportCsvWriter.class);
	private static final int nbLoopForTrace = 10;
	private Runtime rt = Runtime.getRuntime();

	private ReportCsvFactory reportFactory;
	private String topicIn;
	private String kafkaNodeList;

	/**
	 * Creates a ReportCsvWriter instance.
	 * <p>
	 * The number of messages fetched per poll() call from a topic is set to {@link ReportUtils#DEFAULT_MAX_POLL_RECORDS_CONFIG} .
	 * This value can be redefined by setting the environment variable {@link ReportUtils#ENV_MAX_POLL_RECORDS}
	 * 
	 * @param factory        The {@link ReportCsvFactory}.
	 * @param kafkaNodeList  The Kafka cluster node list.
	 * @param topicIn        The Kafka topic to read from.
	 */
	public ReportCsvWriter(ReportCsvFactory factory, String kafkaNodeList, String topicIn) {
		this.reportFactory = factory;
		this.kafkaNodeList = kafkaNodeList;
		this.topicIn = topicIn;
	} 

	private Consumer<String, ReportElement> createConsumer(String topic) {
		final Properties props = new Properties();
		
		String groupId = getGroupId() + System.currentTimeMillis();
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaNodeList);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ReportElementDeserializer.class.getName());
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, getMaxPollRecords());

		log.info("Creating consumer on topic "+topic+" with group "+groupId);
		final Consumer<String, ReportElement> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(topic));
		log.debug("We are now subscribed to ");
		consumer.subscription().forEach(t -> log.debug(" "+t));
		return consumer;
	}

	private String getGroupId() {
		String groupId = System.getenv().get(ReportUtils.ENV_GROUP_ID);
		if (groupId == null)
			groupId = ReportUtils.DEFAULT_CONSUMER_GROUPID;
		log.trace("Using "+groupId+" as groupId prefix for Kafka consumer");
		return groupId;
	}

	private int getMaxPollRecords() {
		int maxPollRecords = ReportUtils.DEFAULT_MAX_POLL_RECORDS_CONFIG;
		String maxPollRecordsStr = System.getenv().get(ReportUtils.ENV_MAX_POLL_RECORDS);
		if (maxPollRecordsStr != null)
			maxPollRecords = Integer.parseInt(maxPollRecordsStr);
		log.trace("Using "+maxPollRecords+" for MAX_POLL_RECORDS_CONFIG");
		return maxPollRecords;
	}

	/**
	 * Produces the csv file.
	 * <p>
	 * All exceptions related to file system access are trapped and reported to the logs.
	 * In this case, the report might be missing or incomplete.
	 * @param csvfile The csv file name to produce.
	 * @return a boolean value indicating if all went well. A value of false means an error has occurred and
	 * the report might be missing or incomplete.
	 */
	public final boolean produceReport(String csvfile) {
		if (csvfile == null) {
			log.info("csvfile is null !");
			return false;
		}
		if (new File(csvfile).exists()) {
			log.info(csvfile+" already exists, do nothing");
			return false;
		}
		FileWriter fw;
		try {
			fw = new FileWriter(csvfile);
		} catch (IOException ex) {
			log.info("Error when creating "+csvfile+" : "+ex.getLocalizedMessage());
			return false;
		}
		try {
			BufferedWriter writer = new BufferedWriter(fw);
			final Consumer<String, ReportElement> consumer = createConsumer(topicIn);

			//final Duration delay = Duration.ofSeconds(1);
			final long delay = 10*1000; // 10 seconds
			final int giveUp = 3;
			int noRecordsCount = 0;

			log.debug("Waiting 10 seconds...");
			consumer.poll(10*1000); // necessary for consumer to reset to beginning of partitions
			consumer.seekToBeginning(consumer.assignment());	    
			//consumer.poll(Duration.ofSeconds(10));
			showOffsets(consumer);
			int nbRecords = 1;
			boolean breakMainLoop = false;
			// List<String> alreadySeen = new ArrayList<>(30_000_000);
			for (int nbLoop=0; !breakMainLoop; nbLoop++) {
				log.debug("Doing poll...");
				final ConsumerRecords<String, ReportElement> consumerRecords = consumer.poll(delay);
				if (consumerRecords.count() == 0) {
					noRecordsCount++;
					if (noRecordsCount > giveUp) break;
					else continue;
				}
				if (nbLoop % nbLoopForTrace == 0)
					log.debug(""
							+ new Date()
							+ " got "
							+ d(consumerRecords.count()) + " records, total " + d(nbRecords)
							+ " free mem = " + d(rt.freeMemory()) + "/" + d(rt.totalMemory())
							);

				for (ConsumerRecord<String, ReportElement> record : consumerRecords) {
					String key = record.key();
					// if (alreadySeen.contains(key)) {
					// //System.err.println("Skip record (already seen) "+key);
					// System.err.print(".");
					// continue;
					// }
					// //System.err.println("Adding "+key);
					//					alreadySeen.add(key);
					nbRecords++;
					ReportElement re = record.value();
					reportFactory.dumpElementToCsv(key, re, writer);					
				}
				writer.flush();
				while (true) {
					try {
						consumer.commitSync();
						break;
					} catch (Exception e) {
						log.info(("Got "+e.getLocalizedMessage()+" we took too long to process batch and got kicked out of the group..."));
						break;
					}
				}
			}
			writer.close();
			consumer.close();
		} catch (IOException ex) {
			log.info("Error when writing to "+csvfile+" : "+ex.getLocalizedMessage());
			return false;
		}
		log.info("Finished producing "+csvfile);
		return true;
	}

	private void showOffsets(final Consumer<String, ReportElement> consumer) {
		log.trace("Reading at offsets :");
		consumer.assignment().forEach(p -> log.trace(" "+p));
	}

}

