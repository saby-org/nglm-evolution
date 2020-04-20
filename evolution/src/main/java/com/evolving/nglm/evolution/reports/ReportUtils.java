/*****************************************************************************
 *
 *  ReportUtils.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.text.NumberFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.examples.pageview.JsonPOJODeserializer;
import org.apache.kafka.streams.examples.pageview.JsonPOJOSerializer;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.EvolutionSetup;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.Deployment;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.Set;

/**
 * This class defines constants and utility functions than can be used in custom reports.
 *
 */
public class ReportUtils {

	private static final Logger log = LoggerFactory.getLogger(ReportUtils.class);

	/**
	 * Environment variable that can be set to redefine the CLIENT_ID string passed to the Kafka Producer for phase 1. 
	 */
	public static final String ENV_CLIENT_ID = "EVOLUTION_REPORTS_CLIENT_ID";
	
	/**
	 * Environment variable that can be set to redefine the port used to connect to ElasticSearch in phase 1.
	 */
	final public static String ENV_ELASTIC_SEARCH_PORT = "EVOLUTION_REPORTS_ELASTIC_SEARCH_PORT";

	/**
	 * Environment variable that can be set to redefine the size of scroll when doing search in phase 1.
	 */
	final public static String ENV_ELASTIC_SEARCH_SCROLL_SIZE = "EVOLUTION_REPORTS_ELASTIC_SEARCH_SCROLL_SIZE";

	/**
	 * Environment variable that can be set to redefine the temp directory used by Streams topology in phase 2.
	 */
	final static String ENV_TEMP_DIR = "EVOLUTION_REPORTS_TEMPDIR";

	/**
	 * Environment variable that can be set to redefine the number of messages fetched per poll() call from a topic in phase 3.
	 */
	final public static String ENV_MAX_POLL_RECORDS = "EVOLUTION_REPORTS_MAX_POLL_RECORDS";
	
	/**
	 * Environment variable that can be set to redefine the Group Id prefix used by the Kafka consumer in phase 3.
	 */
	final public static String ENV_GROUP_ID = "EVOLUTION_REPORTS_GROUP_ID";

	/**
	 * Environment variable that can be set to redefine the number of partitions in created topics in all phases.
	 */
	final public static String ENV_NB_PARTITIONS = "EVOLUTION_REPORTS_NB_PARTITIONS";
	
	/**
	 * Default port used to connect to ElasticSearch in phase 1.
	 */
	public static final int DEFAULT_ELASTIC_SEARCH_PORT = 9200;
	
	/**
	 * Default size of scroll when doing search in phase 1.
	 */
	public static final int DEFAULT_ELASTIC_SEARCH_SCROLL_SIZE = 10_000;

	/**
	 * Default number of messages fetched per poll() call from a topic in phase 3.
	 */
	public static final int DEFAULT_MAX_POLL_RECORDS_CONFIG = 1000;
	
	/**
	 * Default CLIENT_ID string passed to the kafka Producer. 
	 */
	public static final String DEFAULT_PRODUCER_CLIENTID = "ESReader";

	/**
	 * Default GROUP_ID prefix string passed to the kafka Consumer. 
	 */
	public static final String DEFAULT_CONSUMER_GROUPID = "evolution-reports-";
	
	   /**
     * Zip extension for reports. 
     */
    public static final String ZIP_EXTENSION = ".zip";

	static public class JsonTimestampExtractor implements TimestampExtractor {
		@Override
		public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
			//log.trace("extract from "+record);

			if (record.value() instanceof ReportElement) {
				return ((ReportElement) record.value()).timestamp;
			}

			throw new IllegalArgumentException("JsonTimestampExtractor cannot recognize the record value " + record.value());
		}
	}
	
	static public class SuperJSonClass {
		// Just for generics
		// TODO : is this still required ?
	}

	static public class ReportElement extends SuperJSonClass {
		public static final int MARKER = -1; // Special type
		public static final int GENERIC = -2; // Special type
		static final String TIMESTAMP = "timestamp";
		public Map<Integer,Map<String, Object>> fields;
		@Override
		public String toString() {
			return "RE [t=" + type
					+ ", fields=" + fields
					+ ", isC=" + isComplete
					+ "]";
		}
		public int type; // -1 for marker, otherwise key of fields that is set
		public boolean isComplete;
		public long timestamp;
		public ReportElement() {
			super();
			this.fields = new HashMap<>();
			this.isComplete = false;
			this.type = GENERIC;
		}
		public ReportElement(int index, Map<String, Object> map) {
			this();
			this.type = index;
			this.fields.put(index, map);
			if (map.containsKey(TIMESTAMP)) {
				try {
					this.timestamp = (long)map.get(TIMESTAMP);
				} catch (ClassCastException e) { // Leave it unset
					log.error("Timestamp was not a long ! "+map.get(TIMESTAMP));
				}
			} else {
				this.timestamp = System.currentTimeMillis();
			}
		}
		
		public ReportElement(ReportElement re) {
			this();
			for (Integer key : re.fields.keySet()) {
				this.fields.put(key, re.fields.get(key));						
			}
			this.type = re.type;
			this.isComplete = re.isComplete;
			this.timestamp = re.timestamp;
		}
	}

	static public class ReportElementSerializer implements Serializer<ReportElement> {
		private static Serializer<ReportElement> reportElementSerializer;
		static {
			Map<String, Object> serdeProps = new HashMap<>();
			reportElementSerializer = new JsonPOJOSerializer<>();
			serdeProps.put("JsonPOJOClass", ReportElement.class);
			reportElementSerializer.configure(serdeProps, false);
		}
		public ReportElementSerializer() {
			super();
		}

		@Override
		public void close() {
			reportElementSerializer.close();
		}

		@Override
		public void configure(Map<String, ?> arg0, boolean arg1) {
		}

		@Override
		public byte[] serialize(String arg0, ReportElement arg1) {
			return reportElementSerializer.serialize(arg0, arg1);
		}		
	}
	static public class ReportElementDeserializer implements Deserializer<ReportElement> {
		private static Deserializer<ReportElement> reportElementDeserializer;
		static {
			Map<String, Object> serdeProps = new HashMap<>();
			reportElementDeserializer = new JsonPOJODeserializer<>();
			serdeProps.put("JsonPOJOClass", ReportElement.class);
			reportElementDeserializer.configure(serdeProps, false);
		}
		public ReportElementDeserializer() {
			super();
		}

		@Override
		public void close() {
			reportElementDeserializer.close();
		}

		@Override
		public void configure(Map<String, ?> arg0, boolean arg1) {
		}

		@Override
		public ReportElement deserialize(String arg0, byte[] arg1) {
			return reportElementDeserializer.deserialize(arg0, arg1);
		}
	}
	static public class ReportElementSerde implements Serde<ReportElement> {
		static Serializer<ReportElement> reportElementSerializer;
		static Deserializer<ReportElement> reportElementDeserializer;
		static Serde<ReportElement> reportElementSerde;
		static {
			reportElementSerializer = new ReportElementSerializer();
			reportElementDeserializer = new ReportElementDeserializer();
			reportElementSerde = Serdes.serdeFrom(reportElementSerializer, reportElementDeserializer);
		}
		public ReportElementSerde() {
			super();
		}

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}

		@Override
		public void close() {
		}

		@Override
		public Serializer<ReportElement> serializer() {
			return reportElementSerializer;
		}

		@Override
		public Deserializer<ReportElement> deserializer() {
			return reportElementDeserializer;
		}
		
	}
	
	public static int getNbPartitions() {
		int nbPartitions = ReportManager.nbPartitions;
		String nbPartitionsStr = System.getenv().get(ENV_NB_PARTITIONS);
		if (nbPartitionsStr != null) nbPartitions = Integer.parseInt(nbPartitionsStr);
		log.debug("Using "+nbPartitions);
		return nbPartitions;
	}
	
	static private void createTopic(String topicName, Properties prop, String kzHostList) {
			Properties adminClientConfig = new Properties();
	    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("broker.servers"));

	    AdminClient adminClient = AdminClient.create(adminClientConfig);
      int partitions = getNbPartitions();
      short replicationFactor = ReportManager.replicationFactor;
      
      Pattern optionPattern = Pattern.compile("\\s*(\\S+)=(\\S+)");
      String additionalParameters = Deployment.getReportManagerTopicsCreationProperties();
      Map<String, String> configs = new HashMap<>();
      Matcher optionMatcher = optionPattern.matcher(additionalParameters);
      while (optionMatcher.find())
        {
          configs.put(optionMatcher.group(1), optionMatcher.group(2));
        }

      log.info("Creating topic "+topicName+" with "+partitions+" partitions and " + additionalParameters);
      
			try
			{
			  NewTopic topicToCreate = new NewTopic(topicName, partitions, replicationFactor).configs(configs);
			  EvolutionSetup.createSingleTopic(adminClient, topicToCreate);
      }
    catch (InterruptedException | ExecutionException e)
      {
        log.error("Problem when creating topic " + topicName + " : " + e.getLocalizedMessage());
      }
	}
	
	public static void createTopic(String topicName, String kzHostList) {
	    createTopic(topicName, new Properties(), kzHostList);
	}
	
	public static void createTopicCompacted(String topicName, String kzHostList) {
	    Properties topicConfiguration = new Properties();
	    topicConfiguration.put("cleanup.policy", "compact");
	    createTopic(topicName, topicConfiguration, kzHostList);
	}
	
  public static void cleanupTopics(String topic1) 
  {
    deleteTopic(topic1);
  }
	
	public static void cleanupTopics(String topic1, String topic2, String appIdPrefix1, String appIdPrefix2, String appIdSuffix) 
	{
	  deleteTopic(topic1);
	  deleteTopic(topic2);
	  String streamsStoreTopic = appIdPrefix1 + appIdPrefix2 + "-" + appIdSuffix + "-changelog";
	  deleteTopic(streamsStoreTopic);
	}
	
	public static void deleteTopic(String topicName) {
    Properties adminClientConfig = new Properties();
    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("broker.servers"));
    AdminClient adminClient = AdminClient.create(adminClientConfig);
    Map<String, String> configs = new HashMap<>();
    log.info("Deleting topic "+topicName);
    try
    {
      DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(topicName));
      for (KafkaFuture<Void> future : result.values().values())
        future.get();
    }
  catch (InterruptedException | ExecutionException e)
    {
      log.error("Problem when deleting topic " + topicName + " : " + e.getLocalizedMessage());
    }
}

	/**
	 * "pretty-print" method for logging purposes.
	 * @param p The value to format. 
	 * @return Formated string of the input parameter.
	 */
	public static String d(long p) {
		return NumberFormat.getInstance().format(p);
	}
	/**
	 * "pretty-print" method for logging purposes.
	 * @param p The value to format. 
	 * @return Formated string of the input parameter.
	 */
	public static String d(double p) {
		return NumberFormat.getInstance().format(p);
	}
	/**
	 * "pretty-print" method for logging purposes.
	 * @param p The value to format. 
	 * @return Formated string of the input parameter.
	 */
	public static String d(int p) {
		return NumberFormat.getInstance().format(p);
	}
	/**
	 * "pretty-print" method for logging purposes.
	 * @param ai The value to format. 
	 * @return Formated string of the input parameter.
	 */
	public static String d(AtomicInteger ai) {
		return NumberFormat.getInstance().format(ai.get());
	}

	public static String getSeparator() {
		return Deployment.getReportManagerCsvSeparator();
	}

  //
  // format regular field
  //
  public static void format(StringBuilder sb, String s)
  {
    // Surround s with double quotes, and escape double quotes inside it
    sb.append("\"").append(s.replaceAll("\"", Matcher.quoteReplacement("\\\""))).append("\"");
  }

  //
  // format empty field
  //
  public static void format(StringBuilder sb)
  {
    sb.append("\"\"");
  }

  //
  //
  //
  public static String formatResult(Map<String, Object> result)
  {
    boolean first = true;
    StringBuilder line = new StringBuilder();
    for (String field : result.keySet())
      {
        if (first)
          {
            first = false;
          }
        else
          {
            line.append(getSeparator());
          }
        if (result.get(field) != null)
          {
            format(line, result.get(field).toString());
          }
        else
          {
            format(line);
          }
      }
    return line.toString();
  }

  //
  //
  //
  public static String formatResult(List<String> headerFieldsOrder, Map<String, Object> info, Map<String, Object> subscriberFields)
  {
    boolean first = true;
    StringBuilder line = new StringBuilder();
    for (String field : headerFieldsOrder)
      {
        if (first)
          {
            first = false;
          }
        else 
          {
            line.append(getSeparator());
          }
        if (info.get(field) != null)
          {
            format(line, info.get(field).toString());
          }
        else if (subscriberFields.get(field) != null)
          {
            format(line, subscriberFields.get(field).toString());
          }
        else 
          {
            format(line);
          }
      }
    return line.toString();
  }

  //
  //
  //
  public static String formatResult(List<String> headerFieldsOrder, Map<String, Object> info)
  {
    boolean first = true;
    StringBuilder line = new StringBuilder();
    for (String field : headerFieldsOrder)
      {
        if (first)
          {
            first = false;
          }
        else 
          {
            line.append(getSeparator());
          }
        if (info.get(field) != null)
          {
            format(line, info.get(field).toString());
          }
        else 
          {
            format(line);
          }
      }
    return line.toString();
  }
  
  //
  // formatJSON for Lists
  //
  public static String formatJSON(List<Map<String, Object>> json)
  {
    String res = JSONUtilities.encodeArray(json).toString();
    return res;
  }

  //
  // formatJSON for Maps
  //
  public static String formatJSON(Map<String, Object> json)
  {
    String res = JSONUtilities.encodeObject(json).toString();
    return res;
  }
  
}
