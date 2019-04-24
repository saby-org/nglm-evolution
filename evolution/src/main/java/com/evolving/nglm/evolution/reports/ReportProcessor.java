/*****************************************************************************
 *
 *  ReportProcessor.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.reports.ReportUtils.JsonTimestampExtractor;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElementSerde;

/**
 * A class that implements phase 2 of the Report Generation. It reads a Kafka topic and produces another Kafka topic. 
 * <p>
 * The temporary directory used by the Kafka Streams topology for state stores can be redefined by setting the environment variable
 * defined by {@link ReportUtils#ENV_TEMP_DIR}.
 * See {@link org.apache.kafka.streams.StreamsConfig#STATE_DIR_CONFIG} for default value, typically "/tmp/kafka-streams".
 * <p>
 * Currently {@link #process()} never returns.
 */
public class ReportProcessor {
	private static final Logger log = LoggerFactory.getLogger(ReportProcessor.class);

	private ReportProcessorFactory factory;
	private String topicIn;
	private String topicOut;
	private String kafkaNodeList;
	private String zkHostList;
	private int instanceNb;
	private String appId;

	/**
	 * Creates a {@link ReportProcessor} instance.
	 * <p>
	 * Multiple processors can be started in parallel, where each one will read a subset of partitions of topicIn, and produce
	 * the corresponding partitions of topicOut. In this case, each processor need to specify the same Application instance Id, and
	 * different Instance numbers.
	 * 
	 * @param factory       Class that implements the {@link ReportProcessorFactory}.
	 * @param topicIn       Topic to read from.
	 * @param topicOut      Topic to write to. Created if necessary.
	 * @param kafkaNodeList The Kafka cluster node list.
     * @param zkHostList    The Zookeeper cluster.
	 * @param appId         Application instance Id, provided to {@link StreamsConfig#APPLICATION_ID_CONFIG}
	 * @param instanceNb    Instance number.
	 * 
	 * @see org.apache.kafka.streams.StreamsConfig#APPLICATION_ID_CONFIG
	 * @see ReportProcessorFactory
	 */
	public ReportProcessor(
			ReportProcessorFactory factory,
			String topicIn,
			String topicOut,
			String kafkaNodeList, 
			String zkHostList, 
			String appId, 
			int instanceNb) {
		this.factory = factory;
		this.topicIn = topicIn;
		this.topicOut = topicOut;
		this.kafkaNodeList = kafkaNodeList;
		this.zkHostList = zkHostList;
		this.appId = appId;
		this.instanceNb = instanceNb;
		log.info("Reading from Kafka topic "+topicIn+" and producing "+topicOut+" appId = "+appId+" instanceNb = "+instanceNb);
	}

	/**
	 * This method does the following :
	 * <ol>
	 * <li>
	 * Calls {@link ReportProcessorFactory#createTopology(StreamsBuilder, String, String, Serde, String)} to create the Streams topology.
	 * <li>
	 * Calls {@link org.apache.kafka.streams.KafkaStreams#start()}
	 * <li>
	 * Calls {@link ReportProcessorFactory#afterStart(KafkaStreams)}, typically waiting for all markers to be received.
	 * </ol>
	 * Currently this method never returns.
	 */
	public void process() {
		ReportUtils.createTopic(topicOut, zkHostList); // In case it does not exist
		Properties streamsConfig = new Properties();
		streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
		streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaNodeList);
		streamsConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
		streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, getTempDir());
		streamsConfig.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
		int portNumber = 8080+instanceNb;
		streamsConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "http://localhost:"+portNumber); // Needed for allMetadata() to work
		//streamsConfig.put(StreamsConfig.CLIENT_ID_CONFIG, "client-id-1");
		final Serde<ReportElement> reportElementSerde = new ReportElementSerde();
		streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ReportElementSerde.class);
		streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		factory.createTopology(builder, topicIn, topicOut, reportElementSerde, kafkaNodeList);
		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
		
		// React to rebalancing, when same process is started in parallel
		streams.setStateListener((n,o) -> {
			log.trace("###### REBALANCE ###### from "+o.name()+" to "+n.name());
			try {
				Collection<StreamsMetadata> allMtd = streams.allMetadata();
				if (allMtd.isEmpty()) {
					factory.setNumberOfPartitions(-1); // default : Number of expected markers = number of partitions of input topic
					log.trace("We got assigned default value of partitions of input topic");
				} else {
					allMtd.forEach(sm -> {
						sm.topicPartitions().forEach(tp -> {
							log.trace("-------------> partition "+tp.partition()+" (port "+sm.port()+") " 
									+ ((sm.port() == portNumber)?" <-- ME":""));
						});
					});
					if (n.equals(State.RUNNING) && o.equals(State.REBALANCING)) {
						AtomicBoolean mustContinue = new AtomicBoolean(true); // To add some control in the lambda
						allMtd.forEach(sm -> {
							if (mustContinue.get() && (sm.port() == portNumber)) {
								int nbPartitions = sm.topicPartitions().size();
								factory.setNumberOfPartitions(nbPartitions);
								log.trace("-------------> We got assigned "+nbPartitions
										+" partitions of input topic : ");
								sm.topicPartitions().forEach(tp -> log.trace(tp.partition()+" "));
								mustContinue.set(false);
							}
						});
					}
				}
			} catch (IllegalStateException ex) {} // For some transitions (like SHUTDOWN) there is no metadata
		});
		streams.start();
		// displayAllMetrics(streams);
		
		factory.afterStart(streams);
		
		log.info("Finished producing topic "+topicOut);
		// Here we should not close the streams
		// because it causes a rebalance, and other instances get our partitions
		// (even though they are already fully processed)
		// TODO
		// We need to find a way to exit without this rebalance to occur (possible ?)
		
		//while (System.currentTimeMillis() > 1) // Forever !
		//	try { TimeUnit.SECONDS.sleep(5); } catch (InterruptedException e) {}

		streams.close();
	}

	private String getTempDir() {
		String tempDir = System.getenv().get(ReportUtils.ENV_TEMP_DIR);
		if (tempDir == null) {
			tempDir = Deployment.getReportManagerStreamsTempDir();
		}
		log.debug("Using "+tempDir+" as temporary directory for streams");
		return tempDir;
	}

	private void displayAllMetrics(final KafkaStreams streams) {
		log.trace("####### Metrics #######");
		Map<MetricName, ? extends Metric> metrics = streams.metrics();
		List<Metric> metricsList = new ArrayList<>(metrics.values());
		metricsList.sort((a,b) -> a.metricName().name().compareTo(b.metricName().name()));
		for (Metric metric : metricsList) {
			log.trace("##### Metric "+metric.metricName().name()+" ( tags "+metric.metricName().tags()+" ) = "+metric.metricValue());
		}
		log.trace("####### Metrics #######");		
	}
	

	
}

