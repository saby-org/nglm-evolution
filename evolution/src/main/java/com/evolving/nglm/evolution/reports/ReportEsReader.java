/*****************************************************************************
 *
 *  ReportEsReader.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.evolving.nglm.evolution.reports.ReportUtils.d;

/**
 * A class that implements phase 1 of the Report Generation. It reads a list of
 * Elastic Search indexes and produces Kafka messages in a topic.
 * <p>
 * Upon calling {@link #start()}, this class does the following :
 * <ol>
 * <li>It reads all documents in the Elastic Search indexes, in order. For each
 * document :
 * <li>Writes a message in the output topic representing the document, using the
 * key defined by {@link ReportEsReader#elasticKey}.
 * <li>After all documents are processed, writes "markers" in the topic, one
 * marker per topic partition. Markers are of the
 * {@link ReportUtils.ReportElement#MARKER} type.
 * </ol>
 * <p>
 * The port used to connect to Elastic Search is
 * {@link ReportUtils#DEFAULT_ELASTIC_SEARCH_PORT}. This default value can be
 * redefined in the environment variable
 * {@link ReportUtils#ENV_ELASTIC_SEARCH_PORT}.
 * <p>
 * The scroll size used when doing search is Elastic Search is set by default to
 * {@link ReportUtils#DEFAULT_ELASTIC_SEARCH_SCROLL_SIZE}. This default value
 * can be redefined in the environment variable
 * {@link ReportUtils#ENV_ELASTIC_SEARCH_SCROLL_SIZE}.
 * <p>
 * The Kafka producer is created with a clientId of
 * {@link ReportUtils#DEFAULT_PRODUCER_CLIENTID}. This default value can be
 * redefined in the environment variable {@link ReportUtils#ENV_CLIENT_ID}.
 * <p>
 * This method will create the {@link #topicName} if necessary, this is why it
 * needs {@link #kzHostList}.
 * <p>
 * This class produces traces, currently on {@link System#out} PrintStream.
 * 
 * @see ReportUtils.ReportElement
 */
public class ReportEsReader
{
  private static final Logger log = LoggerFactory.getLogger(ReportEsReader.class);

  private static ElasticsearchClientAPI elasticsearchReaderClient;
  
  private String topicName;
  private String kafkaNodeList;
  private String kzHostList;
  private String esNode;
  private LinkedHashMap<String, QueryBuilder> esIndex;
  private String elasticKey;
  private boolean onlyOneIndex;
  private boolean onlyKeepAlternateIDs; // when we read subscriberprofile, only keep info about alternateIDs
  private boolean onlyKeepAlternateIDsExtended; // when we read subscriberprofile, only keep info about alternateIDs and a little more (for 2 specific reports)
  private int returnSize; //used when a desired size of subscriber base is returned

  /**
   * Create a {@code ReportEsReader} instance.
   * <p>
   * After creating the instance, one needs to call {@link #start()} to begin
   * processing.
   * <p>
   * The Elastic Search indexes are provided as an array of String.
   * <p>
   * Note : currently, only keys of type Integer or String (in Elastic Search) can
   * be used as key ({@link #elasticKey} parameter).
   *
   * @param elasticKey
   *          the ElasticSearch field to be used as a key in all Kafka messages.
   * @param topicName
   *          the topic to write to (will be created if necessary).
   * @param kafkaNodeList
   *          the kafka cluster node list that will hold the topic.
   * @param kzHostList
   *          the zookeeper cluster (required to create the topic).
   * @param esNode
   *          the Elastic Search node to read from.
   * @param esIndex
   *          a hashmap with each index to read and the associated query
   */
  public ReportEsReader(String elasticKey, String topicName, String kafkaNodeList, String kzHostList, String esNode, LinkedHashMap<String, QueryBuilder> esIndex, boolean onlyKeepAlternateIDs, boolean onlyKeepAlternateIDsExtended)
  {
    this.elasticKey = elasticKey;
    this.topicName = topicName;
    this.kafkaNodeList = kafkaNodeList;
    this.kzHostList = kzHostList;
    this.esNode = esNode;
    this.onlyKeepAlternateIDs = onlyKeepAlternateIDs;
    this.onlyKeepAlternateIDsExtended = onlyKeepAlternateIDsExtended;
    // convert index names to lower case, because this is what ElasticSearch expects
    this.esIndex = new LinkedHashMap<>();
    for (Entry<String, QueryBuilder> elem : esIndex.entrySet())
      {
        this.esIndex.put(elem.getKey().toLowerCase(), elem.getValue());
      }
    log.info("Starting ES read with indexes : " + this.esIndex);
    this.onlyOneIndex = (this.esIndex.size() == 1);
  }

  public ReportEsReader(String elasticKey, String topicName, String kafkaNodeList, String kzHostList, String esNode, LinkedHashMap<String, QueryBuilder> esIndex)
  {
    this(elasticKey, topicName, kafkaNodeList, kzHostList, esNode, esIndex, false, false);
  }

  public ReportEsReader(String elasticKey, String topicName, String kafkaNodeList, String kzHostList, String esNode, LinkedHashMap<String, QueryBuilder> esIndex, boolean onlyKeepAlternateIDs)
  {
    this(elasticKey, topicName, kafkaNodeList, kzHostList, esNode, esIndex, onlyKeepAlternateIDs, false);
  }

  public ReportEsReader(String elasticKey, String topicName, String kafkaNodeList, String kzHostList, String esNode, LinkedHashMap<String, QueryBuilder> esIndex,boolean onlyKeepAlternateIDs,int returnSize)
  {
    this(elasticKey, topicName, kafkaNodeList, kzHostList, esNode, esIndex, onlyKeepAlternateIDs);
    this.returnSize = returnSize;
  }

  public enum PERIOD
  {
    DAYS("DAYS"), WEEKS("WEEKS"), MONTHS("MONTHS"), UNKNOWN("UNKNOWN");

    private String externalRepresentation;

    private PERIOD(String externalRepresentation)
    {
      this.externalRepresentation = externalRepresentation;
    }

    public String getExternalRepresentation()
    {
      return externalRepresentation;
    }

    public static PERIOD fromExternalRepresentation(String externalRepresentation)
    {
      for (PERIOD enumeratedValue : PERIOD.values())
        {
          if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation))
            return enumeratedValue;
        }
      return UNKNOWN;
    }
  }

  /**
   * Starts processing with basic matchAll query. This is a synchronous call :
   * when it returns, all messages have been sent to the Kafka topic.
   * <p>
   * This creates the Kafka topic, if necessary.
   */
  public void start()
  {

    String indexes = "";
    int scrollSize = getScrollSize();
    for (String s : esIndex.keySet())
      indexes += s + " ";
    log.info("Reading data from ES in \"" + indexes + "\" indexes and writing to \"" + topicName + "\" topic.");

    //if returnSize is zero all record will be returned
    if (returnSize == 0)
    {
      returnSize = Integer.MAX_VALUE;
    }

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

    final AtomicInteger count = new AtomicInteger(0);
    final AtomicInteger nbReallySent = new AtomicInteger(0);
    final AtomicLong before = new AtomicLong(SystemTime.getCurrentTime().getTime());
    final AtomicLong lastTS = new AtomicLong(0);
    final int traceInterval = 100_000;

    // ESROUTER can have two access points
    // need to cut the string to get at least one
    String node = null;
    int port = 0;
    int connectTimeout = Deployment.getElasticsearchConnectionSettings().get("ReportManager").getConnectTimeout();
    int queryTimeout = Deployment.getElasticsearchConnectionSettings().get("ReportManager").getQueryTimeout();
    String username = null;
    String password = null;
    
    if (esNode.contains(","))
      {
        String[] split = esNode.split(",");
        if (split[0] != null)
          {
            Scanner s = new Scanner(split[0]);
            s.useDelimiter(":");
            node = s.next();
            port = s.nextInt();
            username = s.next();
            password = s.next();
            s.close();
          }
      } else
      {
        Scanner s = new Scanner(esNode);
        s.useDelimiter(":");
        node = s.next();
        port = s.nextInt();
        username = s.next();
        password = s.next();
        s.close();
      }

    elasticsearchReaderClient = new ElasticsearchClientAPI(node, port, connectTimeout, queryTimeout, username, password);

    int i = 0;
    for (Entry<String, QueryBuilder> index : esIndex.entrySet())
      {

        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder().query(index.getValue()));

        // Read all docs from ES, on esIndex[i]
        // Write to topic, one message per document
        try
          {
            Scroll scroll = new Scroll(TimeValue.timeValueSeconds(10L));
            String[] indicesToRead = getIndices(index.getKey());
            
            //
            //  indicesToRead is blank?
            //
            
            if (indicesToRead == null || indicesToRead.length == 0)
              {
                i++;
                continue;
              }
            
            searchRequest.indices(indicesToRead);
            searchRequest.source().size(getScrollSize());
            searchRequest.scroll(scroll);
            SearchResponse searchResponse;
            searchResponse = elasticsearchReaderClient.search(searchRequest, RequestOptions.DEFAULT);

            String scrollId = searchResponse.getScrollId(); // always null
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            log.trace("searchHits = " + Arrays.toString(searchHits));
            if (searchHits != null)
              {
                // log.info("searchResponse = " + searchResponse.toString());
                log.trace("getFailedShards = " + searchResponse.getFailedShards());
                log.trace("getSkippedShards = " + searchResponse.getSkippedShards());
                log.trace("getTotalShards = " + searchResponse.getTotalShards());
                log.trace("getTook = " + searchResponse.getTook());
                log.info("for " + Arrays.toString(indicesToRead) + " searchHits.length = " + searchHits.length + " totalHits = " + searchResponse.getHits().getTotalHits());
              }
            boolean alreadyTraced1 = false;
            boolean alreadyTraced2 = false;
            //label for break. When return size is beccoming zero out from for and while
            returnSizeCompleted:
            while (searchHits != null && searchHits.length > 0 )
              {
                log.debug("got " + searchHits.length + " hits");
                for (SearchHit searchHit : searchHits)
                  {
                    Map<String, Object> sourceMap = searchHit.getSourceAsMap();
                    // write record to kafka topic
                    String key;
                    Object res = sourceMap.get(elasticKey);
                    if (res == null)
                      {
                        if (!alreadyTraced1)
                          {
                            log.warn("unexpected, null key while reading " + Arrays.stream(indicesToRead).map(s -> "\""+s+"\"").collect(Collectors.toList()) + " sourceMap=" + sourceMap);
                            alreadyTraced1 = true;
                          }
                      }
                    else
                      {
                        // Need to be extended to support other types of attributes, currently only int and String
                        key = (res instanceof Integer) ? Integer.toString((Integer) res) : (String) res;
                        
                        Map<String, Object> miniSourceMap = sourceMap;
                        if (onlyKeepAlternateIDs && (i == (esIndex.size()-1))) // subscriber index is always last
                          {
                            // size optimize : only keep what is needed for the join later
                            if (!alreadyTraced2)
                              {
                                log.info("Keeping only alternate IDs");
                                alreadyTraced2 = true;
                              }
                            miniSourceMap = new HashMap<>();
                            miniSourceMap.put(elasticKey, sourceMap.get(elasticKey));
                            for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                              {
                                String name = alternateID.getName();
                                log.trace("Only keep alternateID " + name);
                                if (sourceMap.get(name) == null)
                                  {
                                    log.trace("Unexpected : no value for alternateID " + name);
                                  }
                                else
                                  {
                                    miniSourceMap.put(name, sourceMap.get(name));
                                  }
                              }
                            if (onlyKeepAlternateIDsExtended)
                              {
                                miniSourceMap.put("pointBalances", sourceMap.get("pointBalances")); // keep this (for Customer Point Details report)
                                miniSourceMap.put("loyaltyPrograms", sourceMap.get("loyaltyPrograms")); // keep this (for Loyalty Program Customer States report)
                              }
                            sourceMap = null; // to help GC do its job
                          }
                        
                        ReportElement re = new ReportElement(i, miniSourceMap);
                        if (onlyOneIndex) // record is complete, we don't need to join it
                          {
                            re.isComplete = true;
                          }
                        log.trace("Sending record k=" + key + ", v=" + re);
                        ProducerRecord<String, ReportElement> record = new ProducerRecord<>(topicName, key, re);
                        producerReportElement.send(record, (mdata, e) -> {
                          nbReallySent.incrementAndGet();
                          lastTS.set(mdata.timestamp());
                        });
                        if (count.getAndIncrement() % traceInterval == 0)
                          {
                            long now = SystemTime.getCurrentTime().getTime();
                            long diff = now - before.get();
                            double speed = (traceInterval * 1000.0) / (double) diff;
                            before.set(now);
                            log.debug(now + " Sending msg " + d(count.get() - 1) + " to topic " + topicName + " nbReallySent : " + d(nbReallySent.get())
                            // + " lastTS : "+d(lastTS.get())
                            + " speed = " + d((int) speed) + " messages/sec" + " ( " + key + " , " + record.value() + " )");
                          }
                      }
                    returnSize --;
                    if (returnSize == 0)
                    {
                      break returnSizeCompleted;
                    }
                  }
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = elasticsearchReaderClient.searchScroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
              }
            log.debug("Finished with index " + i);
            if (scrollId != null)
              {
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(scrollId);
                elasticsearchReaderClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
              }
          } catch (IOException e)
          {
            log.info("Exception while reading ElasticSearch " + e.getLocalizedMessage());
          }
        i++;
      }
    
    try
    {
      elasticsearchReaderClient.close();
    }
    catch (IOException e)
    {
      log.info("Exception while closing ElasticSearch client " + e.getLocalizedMessage());
    }


    // send 1 marker per partition
    for (int partition = 0; partition < ReportUtils.getNbPartitions(); partition++)
      {
        ReportElement re = new ReportElement();
        re.type = ReportElement.MARKER;
        ProducerRecord<String, ReportElement> record = new ProducerRecord<>(topicName, partition, "-1", re); // negative key
        log.debug("Sending Marker message " + re);
        count.getAndIncrement();
        producerReportElement.send(record, (mdata, e) -> {
          log.debug("Marker was sent to partition " + mdata.partition());
          nbReallySent.incrementAndGet();
          lastTS.set(mdata.timestamp());
        });
      }

    reportElementSerializer.close();
    producerReportElement.close();

    while (nbReallySent.get() < count.get())
      {
        log.trace(SystemTime.getCurrentTime() + " Sent " + d(count.get()) + " messages, nbReallySent : " + d(nbReallySent.get()));
        try
          {
            TimeUnit.SECONDS.sleep(10);
          } catch (InterruptedException e)
          {
          }
      }

    log.trace(SystemTime.getCurrentTime() + " Sent " + d(count.get()) + " messages, nbReallySent : " + d(nbReallySent.get()));
  }

  private String[] getIndices(String key)
  {
    StringBuilder existingIndexes = new StringBuilder();
    boolean firstEntry = true;
    
    //
    // blank key 
    //
    
    if (key == null || key.isEmpty()) return null;
    
    for (String index : key.split(","))
      {
        
        //
        //  wildcard
        //
        
        if(index.endsWith("*")) 
          {
            if (!firstEntry) existingIndexes.append(",");
            existingIndexes.append(index); 
            firstEntry = false;
            continue;
          }
        
        //
        //  indices-exists
        //
        
        GetIndexRequest request = new GetIndexRequest(index);
        request.local(false); 
        request.humanReadable(true); 
        request.includeDefaults(false); 
        try
          {
            boolean exists = elasticsearchReaderClient.indices().exists(request, RequestOptions.DEFAULT);
            if (exists) 
              {
                if (!firstEntry) existingIndexes.append(",");
                existingIndexes.append(index);
                firstEntry = false;
              }
            else
              {
                log.debug("{} index does not exists - record will not be in report", index);
              }
          } 
        catch (IOException e)
          {
            e.printStackTrace();
          }
      }
    log.debug("index to be read {}", existingIndexes.toString());
    if (firstEntry) // nothing got added
      {
        return null;
      }
    else
      {
        return existingIndexes.toString().split(",");
      }
  }

  private int getScrollSize()
  {
    int scrollSize = com.evolving.nglm.core.Deployment.getElasticsearchScrollSize();
    if (scrollSize == 0)
      {
        scrollSize = ReportUtils.DEFAULT_ELASTIC_SEARCH_SCROLL_SIZE;
      }
    log.trace("Using " + scrollSize + " as scroll size in Elastic Search");
    return scrollSize;
  }

  private int getElasticsearchPort()
  {
    int elasticsearchPort = ReportUtils.DEFAULT_ELASTIC_SEARCH_PORT;
    String elasticsearchPortStr = System.getenv().get(ReportUtils.ENV_ELASTIC_SEARCH_PORT);
    if (elasticsearchPortStr != null)
      elasticsearchPort = Integer.parseInt(elasticsearchPortStr);
    log.trace("Using " + elasticsearchPort + " port to connect to Elastic Search");
    return elasticsearchPort;
  }

  private String getProducerId()
  {
    String producerId = System.getenv().get(ReportUtils.ENV_CLIENT_ID);
    if (producerId == null)
      producerId = ReportUtils.DEFAULT_PRODUCER_CLIENTID;
    log.trace("Using " + producerId + " as clientId for Kafka producer");
    return producerId;
  }

}
