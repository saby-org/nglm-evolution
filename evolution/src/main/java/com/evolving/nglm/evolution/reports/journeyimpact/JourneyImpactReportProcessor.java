package com.evolving.nglm.evolution.reports.journeyimpact;

import com.evolving.nglm.evolution.reports.journeycustomerstates.JourneyCustomerStatesReportObjects;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.reports.ReportProcessor;
import com.evolving.nglm.evolution.reports.ReportProcessorFactory;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.examples.pageview.JsonPOJOSerializer;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.evolving.nglm.evolution.reports.ReportUtils.d;

/**
 * This class implements phase 2 for the BDR Report.
 * It implements {@link ReportProcessorFactory} and is passed as parameter when instancing {@link ReportProcessor}.
 *
 */
public class JourneyImpactReportProcessor implements ReportProcessorFactory {

    private static final Logger log = LoggerFactory.getLogger(JourneyImpactReportProcessor.class);

    final static String STORENAME = "evol-store";
    private String kafkaNode;
    private String topicOut;

    final AtomicInteger nbMetric = new AtomicInteger(0);
    final AtomicInteger nbStat = new AtomicInteger(0);
    final AtomicInteger nbOut = new AtomicInteger(0);
    final AtomicInteger nbMarkers = new AtomicInteger(0);
    final AtomicInteger nbExpectedMarkers = new AtomicInteger(ReportUtils.getNbPartitions());
    private Runtime rt = Runtime.getRuntime();
    private KTable<String, ReportElement> journeyStatJoined;

    /**
     * Creates the topology of Kafka Streams that will build the report.
     * We create a KStream, then group it by key ("customer_id"). Values are {@link ReportElement}
     * We then call {@link KGroupedStream#aggregate(org.apache.kafka.streams.kstream.Initializer,
     *                                       org.apache.kafka.streams.kstream.Aggregator, Materialized)}
     * to aggregate all messages with the same key :
     * <ul>
     * <li>
     * If the ReportElement contains customer info, we copy that to the aggregate.
     * <li>
     * If it is a Marker, we increment the number of markers received so far.
     * </ul>
     * Then we materialise this KTable as an internal topic, which is dumped to another topic
     * when {@link #afterStart(KafkaStreams)} is called.
     */
    @Override
    public void createTopology(
            final StreamsBuilder builder,
            final String topicIn,
            final String topicOut,
            final Serde<ReportElement> reportElementSerde,
            final String kafkaNode) {

              this.topicOut = topicOut;
              this.kafkaNode = kafkaNode;
              KStream<String, ReportElement> reStream =
                  builder
                  .stream(topicIn, Consumed.with(Serdes.String(), reportElementSerde))
                  .peek((k,re) -> log.trace("peek : k="+k+" re="+re.toString())) // for debug
                  ;
              log.trace("reStream = "+reStream);
              KGroupedStream<String, ReportElement> groupedBy =
                  reStream.groupByKey(Serialized.with(Serdes.String(), reportElementSerde));
              log.trace("groupedBy = "+groupedBy);

              journeyStatJoined = groupedBy
                  .aggregate(
                      () -> {
                        log.trace("Initializer");
                        return new ReportElement();
                      },
                      (k, v, agg) -> {
                        log.trace("Got k="+k+" v="+v+" agg="+agg);
                        ReportElement re = new ReportElement(agg);
                        final int indexJourneyStat = 0;
                        final int indexJourneyMetric = 1;
                        if (v.type == indexJourneyStat) {
                          Map<String, Object> journeyStatV = v.fields.get(indexJourneyStat);
                          if (re.fields.get(indexJourneyStat) != null) {
                            log.debug("Unexpected : got new data for existing customer ! Ignore it "+k+", "+v+", "+agg);
                          } else {
                            re.fields.put(indexJourneyStat, journeyStatV);
                            if (re.fields.get(indexJourneyMetric) != null) {
                              // log.trace("------------> complete record 1 ! "+v+" "+re);
                              re.isComplete = true;
                            }
                          }
                          nbStat.incrementAndGet();
                        } else if (v.type == indexJourneyMetric) {
                          
                          Map<String, Object> journeyMetricV = v.fields.get(indexJourneyMetric);
                          if (re.fields.get(indexJourneyMetric) != null) {
                            log.debug("Unexpected : got new data for existing customer ! Ignore it "+k+", "+v+", "+agg);
                          } else {
                            re.fields.put(indexJourneyMetric, journeyMetricV);
                            if (re.fields.get(indexJourneyStat) != null) {
                              // log.trace("------------> complete record 1 ! "+v+" "+re);
                              re.isComplete = true;
                            }
                          }
                          nbMetric.incrementAndGet();
                          
                        } else if (v.type == ReportElement.MARKER) {
                          re.type = ReportElement.MARKER;
                          nbMarkers.incrementAndGet();
                          log.trace("############### merge marker "+nbMarkers.get()+" "+v+" "+re);
                        } else {
                          log.error("Internal error : unexpected type "+v);
                        }
                        log.trace("--> Produced "+re);
                        return re;
                      }, Materialized.as(STORENAME) 
                      );

    }

    /**
     * Called after the topology has been created and started.
     * It waits until all markers are received.
     * Then it writes the content of the materialised view of the KTable to the output topic.
     * <p>
     * Some various traces are displayed on {@link System#out} to show progress.
     * 
     */
    @Override
    public void afterStart(final KafkaStreams streams) {
        int beforeMetric = 0;
        int beforeStat = 0;
        final int delay = 5;
        do {
            try { TimeUnit.SECONDS.sleep(delay); } catch (InterruptedException e) {}
            int nowMetric = nbMetric.get();
            int speedMetric = (nowMetric-beforeMetric)/delay;
            beforeMetric = nowMetric;
            int nowStat = nbStat.get();
            int speedSubscriber = (nowStat-beforeStat)/delay;
            beforeStat = nowStat;
            log.trace(SystemTime.getCurrentTime()
                    + " nbMetric = " + d(nbMetric)
                    + " (" + d(speedMetric)+"/s)"
                    + " nbStat = " + d(nbStat)
                    + " (" + d(speedSubscriber)+"/s)"
                    // + " assigned-partitions = "+assignedPartitions.metricValue() // =assigned-partitions
                    + " nbMarkers = " + d(nbMarkers)
                    + " free mem = "+d(rt.freeMemory())+"/"+d(rt.totalMemory())
            );
        } while (nbMarkers.get() != nbExpectedMarkers.get());

        // Now dump the KTable to topicOut
        final String queryableStoreName = journeyStatJoined.queryableStoreName(); // returns null if KTable is not queryable
        log.trace("queryableStoreName = "+queryableStoreName); // "${applicationId}-${STORENAME}-changelog"
        ReadOnlyKeyValueStore<String, ReportElement> view = streams.store(queryableStoreName, QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, ReportElement> kvi = view.all();

        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<ReportElement> reportElementSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", ReportElement.class);
        reportElementSerializer.configure(serdeProps, false);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaNode);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, JourneyCustomerStatesReportObjects.CLIENTID_PREFIX + System.currentTimeMillis());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, reportElementSerializer.getClass().getName());
        final Producer<String, ReportElement> producer = new KafkaProducer<>(props);

        final AtomicInteger count = new AtomicInteger(0);
        final AtomicInteger nbReallySent = new AtomicInteger(0);
        final AtomicLong before = new AtomicLong(SystemTime.getCurrentTime().getTime());
        final AtomicLong lastTS = new AtomicLong(0);
        
        final int traceInterval = 100_000;
        while (kvi.hasNext()) {
            KeyValue<String,ReportElement> kv = kvi.next();
            String key = kv.key;
            ReportElement re = kv.value;
            if (re.type == ReportElement.MARKER)
                continue;
            if (re.type != ReportElement.GENERIC) {
                log.info("Something was wrong, type is not GENERIC : "+re);
                continue;
            }
            if (!re.isComplete)
                continue;
            ProducerRecord<String, ReportElement> record = new ProducerRecord<>(topicOut, key, re);
            producer.send(record
                    , (mdata, e) -> {
                        nbReallySent.incrementAndGet();
                        lastTS.set(mdata.timestamp());
                    });
            if (count.getAndIncrement() % traceInterval == 0) {
                long now = SystemTime.getCurrentTime().getTime();
                long diff = now - before.get();
                double speed = (traceInterval*1000.0)/(double)diff;
                before.set(now);
                log.trace(SystemTime.getCurrentTime()
                        + " Sending msg "+d(count.get()-1)
                        + " to topic " + topicOut
                        + " nbReallySent : "+d(nbReallySent)
                        //+ " lastTS : "+d(lastTS.get())
                        + " speed = "+d((int)speed)+" messages/sec"
                        + " ( "+key+" , "+record.value()+" )"
                        );
            }       
        }
        kvi.close();
        log.info("####### count = " + d(count)
                + " nbReallySent : "+d(nbReallySent)
            );
        
        producer.close();
        reportElementSerializer.close();
        while (nbReallySent.get() < count.get()) {
            log.trace(SystemTime.getCurrentTime()
                    + " Sent "+d(count)+" messages, nbReallySent : "+d(nbReallySent)
                );
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.trace(SystemTime.getCurrentTime()
                + " Sent "+d(count)+" messages, nbReallySent : "+d(nbReallySent)
            );
    }

    /**
     * This is where we are notified that a rebalance has occured within the Kafka cluster (including the initial rebalance).
     * We might have got assigned more partitions than before, or less.
     * We use it to store the number of markers we should be expecting in {@link #afterStart(KafkaStreams)}.
     */
    @Override
    public void setNumberOfPartitions(int nbPartitions) {
        if (nbPartitions < 0)
            nbExpectedMarkers.set(ReportUtils.getNbPartitions());
        else
            nbExpectedMarkers.set(nbPartitions);
    }
    
    public static void main(String[] args) {
        log.info("received " + args.length + " args");
        for(String arg : args){
          log.info("JourneyCustomerStatisticsReportProcessor: arg " + arg);
        }

        if (args.length < 6) {
            log.warn("Usage : JourneyCustomerStatisticsReportProcessor topicIn topicOut KafkaNode ZKhostList appIdSuffix instance");
            return;
        }
        String topicIn     = args[0];
        String topicOut    = args[1];
        String kafkaNode   = args[2];
        String zkHostList  = args[3];
        String appIdSuffix = args[4];
        String instNbStr   = args[5];
        
        int instanceNb = Integer.parseInt(instNbStr);
        String appId = JourneyCustomerStatesReportObjects.APPLICATION_ID_PREFIX+appIdSuffix;

        ReportProcessorFactory reportFactory = new JourneyImpactReportProcessor();
        ReportProcessor reportProcessor = new ReportProcessor(
                reportFactory,
                topicIn,
                topicOut,
                kafkaNode,
                zkHostList,
                appId,
                instanceNb
            );
        reportProcessor.process();
        log.info("Finished JourneyCustomerStatisticsReportProcessor");
    }

}

