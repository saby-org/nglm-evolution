/*****************************************************************************
 *
 *  ReportProcessorFactory.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;

/**
 * A factory that must be implemented to produce phase 2 of the Report Generation. 
 *
 */
public interface ReportProcessorFactory {
	/**
	 * Method called to create the Streams topology.
	 * @param builder            {@link org.apache.kafka.streams.StreamsBuilder} used to build the topology.
	 * @param topicIn            Topic to be read from.
	 * @param topicOut           Topic to be produced.
	 * @param reportElementSerde {@link org.apache.kafka.common.serialization.Serde} for report elements.
	 * @param kafkaNode          The Kafka node list
	 */
	void createTopology(
			StreamsBuilder builder,
			final String topicIn,
			final String topicOut,
			final Serde<ReportElement> reportElementSerde,
			final String kafkaNode
		);

	/**
	 * Method called after the Streams topology has been started.
	 * @param streams    {@link KafkaStreams} streams that has been started.
	 */
	void afterStart(final KafkaStreams streams);
	
	/**
	 * Called after a Kafka rebalance has occurred.
	 * @param nbPartitions The number of partition this process will handle from now on.
	 */
	void setNumberOfPartitions(int nbPartitions);
}
