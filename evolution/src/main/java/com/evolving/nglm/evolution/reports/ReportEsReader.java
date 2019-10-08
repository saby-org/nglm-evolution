/*****************************************************************************
 *
 *  ReportEsReader.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import static com.evolving.nglm.evolution.reports.ReportUtils.d;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.examples.pageview.JsonPOJOSerializer;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;

/**
 * A class that implements phase 1 of the Report Generation. It reads a list of Elastic Search indexes
 * and produces Kafka messages in a topic.
 * <p>
 * Upon calling {@link #start()}, this class does the following :
 * <ol>
 * <li>
 * It reads all documents in the Elastic Search indexes, in order. For each document :
 * <li>
 * Writes a message in the output topic representing the document, using the key defined by {@link ReportEsReader#elasticKey}.
 * <li>
 * After all documents are processed, writes "markers" in the topic, one marker per topic partition.
 * Markers are of the {@link ReportUtils.ReportElement#MARKER} type.
 * </ol> 
 * <p>
 * The port used to connect to Elastic Search is {@link ReportUtils#DEFAULT_ELASTIC_SEARCH_PORT}. This default value can be redefined
 * in the environment variable {@link ReportUtils#ENV_ELASTIC_SEARCH_PORT}.
 * <p>
 * The scroll size used when doing search is Elastic Search is set by default to {@link ReportUtils#DEFAULT_ELASTIC_SEARCH_SCROLL_SIZE}.
 * This default value can be redefined in the environment variable {@link ReportUtils#ENV_ELASTIC_SEARCH_SCROLL_SIZE}.
 * <p>
 * The Kafka producer is created with a clientId of {@link ReportUtils#DEFAULT_PRODUCER_CLIENTID}. This default value can be redefined
 * in the environment variable {@link ReportUtils#ENV_CLIENT_ID}.
 * <p>
 * This method will create the {@link #topicName} if necessary, this is why it needs {@link #kzHostList}.
 * <p>
 * This class produces traces, currently on {@link System#out} PrintStream.
 * @see ReportUtils.ReportElement
 */
public class ReportEsReader {
	private static final Logger log = LoggerFactory.getLogger(ReportEsReader.class);

	private String topicName;
	private String kafkaNodeList;
	private String kzHostList;
	private String esNode;
	private LinkedHashMap<String, QueryBuilder> esIndex;
	private String elasticKey;

    /**
     * Create a {@code ReportEsReader} instance.
     * <p>
     * After creating the instance, one needs to call {@link #start()} to begin processing.
     * <p>
     * The Elastic Search indexes are provided as an array of String.
     * <p>
     * Note : currently, only keys of type Integer or String (in Elastic Search) can be used as key ({@link #elasticKey} parameter).
     *
     * @param elasticKey    the ElasticSearch field to be used as a key in all Kafka messages.
     * @param topicName     the topic to write to (will be created if necessary).
     * @param kafkaNodeList the kafka cluster node list that will hold the topic.
     * @param kzHostList    the zookeeper cluster (required to create the topic).
     * @param esNode        the Elastic Search node to read from.
     * @param esIndex       a hashmap with each index to read and the associated query
     */
	public ReportEsReader(String elasticKey, String topicName, String kafkaNodeList,
			String kzHostList, String esNode, LinkedHashMap<String, QueryBuilder> esIndex) {
		this.elasticKey = elasticKey;
		this.topicName = topicName;
		this.kafkaNodeList = kafkaNodeList;
		this.kzHostList = kzHostList;
		this.esNode = esNode;
		this.esIndex = esIndex;
	}

	public enum PERIOD{
	  DAYS("DAYS"),
	  WEEKS("WEEKS"),
	  MONTHS("MONTHS"),
	  UNKNOWN("UNKNOWN");
	  private String externalRepresentation;
	  private PERIOD(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
	  public String getExternalRepresentation() { return externalRepresentation; }
	  public static PERIOD fromExternalRepresentation(String externalRepresentation) { for (PERIOD enumeratedValue : PERIOD.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
	}

    /**
     * Starts processing with basic matchAll query. This is a synchronous call : when it returns, all messages have been sent to the Kafka topic.
     * <p>
     * This creates the Kafka topic, if necessary. 
     */
    public void start() {

      String indexes = "";
      for (String s : esIndex.keySet())
        indexes += s+" ";
      log.info("Reading data from ES in "+indexes+"indexes and writing to "+topicName+" topic.");

      ReportUtils.createTopic(topicName, kzHostList); // In case it does not exist

      Map<String, Object> serdeProps = new HashMap<>();
      final Serializer<ReportElement> reportElementSerializer = new JsonPOJOSerializer<>();
      serdeProps.put("JsonPOJOClass", ReportElement.class); // Topic consists of stream of ReportElement
      reportElementSerializer.configure(serdeProps, false);

      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaNodeList);
      props.put(ProducerConfig.CLIENT_ID_CONFIG, getProducerId() + System.currentTimeMillis());
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, reportElementSerializer.getClass().getName());
      final Producer<String, ReportElement> producerReportElement = new KafkaProducer<>(props);

//      log.info("Query="+searchRequest.source().query());

      final AtomicInteger count = new AtomicInteger(0);
      final AtomicInteger nbReallySent = new AtomicInteger(0);
      final AtomicLong before = new AtomicLong(new Date().getTime());
      final AtomicLong lastTS = new AtomicLong(0);
      final int traceInterval = 100_000;
      int i=0;
      for (Entry<String, QueryBuilder> index : esIndex.entrySet()) {
        
        SearchRequest searchRequest = new SearchRequest()
            .source(new SearchSourceBuilder()
                .query(index.getValue()));
        
        // Read all docs from ES, on esIndex[i]
        // Write to topic, one message per document
        try {
          
          // ESROUTER can have two access points
          // need to cut the string to get at least one
          String node = null;
          int port = 0;
          if(esNode.contains(",")) {
            String[] split = esNode.split(",");
            if(split[0] != null) {
              Scanner s = new Scanner(split[0]);
              s.useDelimiter(":");
              node = s.next();
              port = s.nextInt();
              s.close();
            }
          }else {
            Scanner s = new Scanner(esNode);
            s.useDelimiter(":");
            node = s.next();
            port = s.nextInt();
            s.close();
          }
          
          elasticsearchReaderClient = new RestHighLevelClient(
              RestClient.builder(
                  new HttpHost(node , port, "http")));
          // Search for everyone

          Scroll scroll = new Scroll(TimeValue.timeValueSeconds(10L));
          String indexToRead = index.getKey();
          log.trace("Reading ES index "+indexToRead);

          searchRequest.indices(indexToRead);
          searchRequest.source().size(getScrollSize());
          searchRequest.scroll(scroll);
          SearchResponse searchResponse;
          searchResponse = elasticsearchReaderClient.search(searchRequest, RequestOptions.DEFAULT);

          String scrollId = searchResponse.getScrollId();
          SearchHit[] searchHits = searchResponse.getHits().getHits();
          log.trace("searchHits = "+searchHits);
          if (searchHits != null) {
            //log.info("searchResponse = " + searchResponse.toString());
            log.info("getFailedShards = "+searchResponse.getFailedShards());
            log.info("getSkippedShards = "+searchResponse.getSkippedShards());
            log.info("getTotalShards = "+searchResponse.getTotalShards());
            log.info("getTook = "+searchResponse.getTook());
            log.info("searchHits.length = "+searchHits.length
                +" totalHits = "+searchResponse.getHits().getTotalHits());
          }
          while (searchHits != null && searchHits.length > 0) {
            log.debug("got "+searchHits.length+" hits");
            for (SearchHit searchHit : searchHits) {
              Map<String,Object> sourceMap = searchHit.getSourceAsMap();
              // write record to kafka topic
              String key;
              Object res = sourceMap.get(elasticKey);
              // Need to be extended to support other types of attributes, currently only int and String
              if (res instanceof Integer)
                key = Integer.toString((Integer)res);
              else
                key = (String) res;
              ReportElement re = new ReportElement(i, sourceMap);
              log.trace("Sending record k="+key+", v="+re);
              ProducerRecord<String, ReportElement> record = new ProducerRecord<>(topicName, key, re);
              producerReportElement.send(record
                  , (mdata, e) -> {
                    nbReallySent.incrementAndGet();
                    lastTS.set(mdata.timestamp());
                  });
              if (count.getAndIncrement() % traceInterval == 0) {
                long now = new Date().getTime();
                long diff = now - before.get();
                double speed = (traceInterval*1000.0)/(double)diff;
                before.set(now);
                log.debug(new Date()
                    + " Sending msg "+d(count.get()-1)
                    + " to topic " + topicName
                    + " nbReallySent : "+d(nbReallySent.get())
                    //+ " lastTS : "+d(lastTS.get())
                    + " speed = "+d((int)speed)+" messages/sec"
                    + " ( "+key+" , "+record.value()+" )"
                    );
              }
            }
            //log.trace("processing next scroll");
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
            scrollRequest.scroll(scroll);
            searchResponse = elasticsearchReaderClient.searchScroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
          }
          if (i == esIndex.size()-1) {
            // send 1 marker per partition
            for (int partition=0; partition<ReportUtils.getNbPartitions(); partition++) {
              ReportElement re = new ReportElement();
              re.type = ReportElement.MARKER;
              ProducerRecord<String, ReportElement> record = new ProducerRecord<>(topicName, partition, "-1", re); // negative key
              log.debug("Sending Marker message "+re);
              count.getAndIncrement();
              producerReportElement.send(record
                  , (mdata, e) -> {
                    System.out.println("Marker was sent to partition "+mdata.partition());
                    nbReallySent.incrementAndGet();
                    lastTS.set(mdata.timestamp());
                  });
            }
          } else {
            log.debug("Finished with index "+i); // Markers are sent at the end
          }
          ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
          clearScrollRequest.addScrollId(scrollId);
          elasticsearchReaderClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
          e.printStackTrace();
        }
        i++;
      }

      reportElementSerializer.close();
      producerReportElement.close();

      while (nbReallySent.get() < count.get()) {
        log.trace(new Date()
            + " Sent "+d(count.get())+" messages, nbReallySent : "+d(nbReallySent.get())
            );
        try { TimeUnit.SECONDS.sleep(10); } catch (InterruptedException e) {}
      }

      log.trace(new Date()
          + " Sent "+d(count.get())+" messages, nbReallySent : "+d(nbReallySent.get())
          );
    }

    private int getScrollSize() {
		int scrollSize = ReportUtils.DEFAULT_ELASTIC_SEARCH_SCROLL_SIZE;
		String scrollSizeStr = System.getenv().get(ReportUtils.ENV_ELASTIC_SEARCH_SCROLL_SIZE);
		if (scrollSizeStr != null)
			scrollSize = Integer.parseInt(scrollSizeStr);
		log.trace("Using "+scrollSize+" as scroll size in Elastic Search");
		return scrollSize;
	}

	private int getElasticsearchPort() {
		int elasticsearchPort = ReportUtils.DEFAULT_ELASTIC_SEARCH_PORT;
		String elasticsearchPortStr = System.getenv().get(ReportUtils.ENV_ELASTIC_SEARCH_PORT);
		if (elasticsearchPortStr != null)
			elasticsearchPort = Integer.parseInt(elasticsearchPortStr);
		log.trace("Using "+elasticsearchPort+" port to connect to Elastic Search");
		return elasticsearchPort;
	}

	private String getProducerId() {
		String producerId = System.getenv().get(ReportUtils.ENV_CLIENT_ID);
		if (producerId == null)
			producerId = ReportUtils.DEFAULT_PRODUCER_CLIENTID;
		log.trace("Using "+producerId+" as clientId for Kafka producer");
		return producerId;
	}

	private static RestHighLevelClient elasticsearchReaderClient;

}
