/*****************************************************************************
 *
 *  ReportUtils.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

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

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.EvolutionSetup;
import com.evolving.nglm.core.JSONUtilities;

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

  public static final String APPLICATION_ID_PREFIX = "Reports_";

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
	 * Default size of scroll when doing search
	 */
	public static final int DEFAULT_ELASTIC_SEARCH_SCROLL_SIZE = 1_000;

  /**
   * Default scroll keep alive (seconds) when doing search
   */
	public static final int DEFAULT_ELASTIC_SEARCH_SCROLL_KEEP_ALIVE = 15;

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
			if (adminClient != null) adminClient.close();
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
    if (adminClient != null) adminClient.close();
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
  private static void format(StringBuilder sb, String s)
  {
    String fieldSurrounder = Deployment.getReportManagerFieldSurrounder();
    // Surround s with fieldSurrounder, and escape fieldSurrounder inside it
    if (fieldSurrounder.isEmpty())
      sb.append(s);
    else
      {
        boolean hasNewline = s.contains("\n");
        if (hasNewline)
          {
            s = s.replace("\n", " ");
          }
        sb.append(fieldSurrounder).append(s.replace(fieldSurrounder, "\\" + fieldSurrounder)).append(fieldSurrounder);
      }
  }

  //
  // format empty field
  //
  private static void format(StringBuilder sb)
  {
    String fieldSurrounder = Deployment.getReportManagerFieldSurrounder();
    if (!fieldSurrounder.isEmpty()) sb.append(fieldSurrounder).append(fieldSurrounder);
  }

  //
  //
  //
  public static String formatResult(Map<String, Object> result)
  {
    StringBuilder line = new StringBuilder();
    for (String field : result.keySet())
      {
        line.append(getSeparator());
        if (result.get(field) != null)
          {
            format(line, result.get(field).toString());
          }
        else
          {
            format(line);
          }
      }
    return line.append("\n").toString().substring(1);
  }

  //
  //
  //
  public static String formatResult(List<String> headerFieldsOrder, Map<String, Object> info, Map<String, Object> subscriberFields)
  {
    StringBuilder line = new StringBuilder();
    for (String field : headerFieldsOrder)
      {
        line.append(getSeparator());
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
    return line.append("\n").toString().substring(1);
  }

  //
  //
  //
  public static String formatResult(List<String> headerFieldsOrder, Map<String, Object> info)
  {
    StringBuilder line = new StringBuilder();
    for (String field : headerFieldsOrder)
      {
        line.append(getSeparator());
        if (info.get(field) != null)
          {
            format(line, info.get(field).toString());
          }
        else 
          {
            format(line);
          }
      }
    return line.append("\n").toString().substring(1);
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

  public static String unzip(String zipFilePath) {
    try {
      Path destDirPath = Files.createTempDirectory("zipdir");
      String destDir = destDirPath.toString();
      FileInputStream fis;
      byte[] buffer = new byte[100 * 1024];
      File newFile = null;
  
      fis = new FileInputStream(zipFilePath);
      ZipInputStream zis = new ZipInputStream(fis);
      ZipEntry ze = zis.getNextEntry();
      while(ze != null){
        String fileName = ze.getName();
        newFile = new File(destDir + File.separator + fileName);
  
        FileOutputStream fos = new FileOutputStream(newFile);
        int len;
        while ((len = zis.read(buffer)) > 0) {
          fos.write(buffer, 0, len);
        }
        fos.close();
        zis.closeEntry();
        ze = zis.getNextEntry();
      }
      zis.closeEntry();
      zis.close();
      fis.close();
      return newFile.getAbsolutePath();
    } catch (IOException ex) {
      log.info("error zipping intermediate file : " + ex.getLocalizedMessage());
      return "";
    }
  }
  
  private static final String ZIP_PREFIX = "zip";

  public static File zipFile(String filePath) {
      try {
          File file = new File(filePath);
          String zipFileName = file.getAbsolutePath().concat("." + ZIP_PREFIX);
          FileOutputStream fos = new FileOutputStream(zipFileName);
          ZipOutputStream zos = new ZipOutputStream(fos);
          zos.putNextEntry(new ZipEntry(file.getName()));
          zos.setLevel(Deflater.BEST_SPEED);
          byte data[] = new byte[100 * 1024]; // allow some bufferization
          int length;
          FileInputStream fis = new FileInputStream(file);
          while ((length = fis.read(data)) != -1) {
            zos.write(data, 0, length);
          }
          fis.close();
          zos.closeEntry();
          zos.close();
          fos.close();
          return new File(zipFileName);
      } catch (FileNotFoundException ex) {
        log.info("file does not exist : " + ex.getLocalizedMessage());
        return null;
      } catch (IOException ex) {
        log.info("error zipping intermediate file : " + ex.getLocalizedMessage());
        return null;
      }
  }

  public static void zipFile(String inputFile, String outputFile) {
      try {
          File file = new File(inputFile);
  
          FileOutputStream fos = new FileOutputStream(outputFile);
          ZipOutputStream zos = new ZipOutputStream(fos);
  
          zos.putNextEntry(new ZipEntry(file.getName()));
  
          byte[] bytes = Files.readAllBytes(Paths.get(inputFile));
          zos.write(bytes, 0, bytes.length);
          zos.closeEntry();
          zos.close();
  
      } catch (FileNotFoundException ex) {
      	log.error("The file does not exist", ex);
      } catch (IOException ex) {
      	log.error("I/O error creating zip file", ex);
      }
  }

  public static void extractTopRows(String inputFile, String outputFile, int topRows) 
  {
  	try
  	{
  		String strLine;
  		int i = 0;
  		BufferedReader br = new BufferedReader(new FileReader(inputFile));
  		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
  		bw.write(br.readLine());
  		while ((strLine = br.readLine()) != null) 
  		{
  			if (i++ < topRows) 
  			{
  			  bw.write("\n");
  			  bw.write(strLine);
  			}
  			else
  				break;
  
  		}
  		br.close();
  		bw.close();
  
  	}
  	catch (FileNotFoundException e) 
  	{
  		log.error("File doesn't exist", e);
  	}
  	catch (IOException e) 
  	{
  		log.error("Error processing " + inputFile + " or " + outputFile, e);
  	}
  }

  public static void extractPercentageOfRandomRows(String inputFileName, String outputFileName,
  		int percentage) {
  
  	int totalNmbrOfLinesInFile = 0;
  	BufferedReader br;
  
  	try
  	{
  		br = new BufferedReader(new FileReader(inputFileName));
  
  		String header = br.readLine();
  		while((br.readLine()) != null) 
  		{
  			totalNmbrOfLinesInFile++;
  		}
  		br.close();
  
  		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName));
  		bw.write(header + "\n");
  		SamplerNRandomRows mySamplerOfRandomRows = new SamplerNRandomRows();
  
  		if (totalNmbrOfLinesInFile < 1)
  		{
  			log.info("The file is empty and thus no lines to be displayed!");
  		}
  		else if (percentage < 1 || percentage > 99)
  		{
  			log.info("The required percentage of lines is expected to be between 1 and 99 !");
  		}
  		else
  		{
  
  			int nbLinesToExtract = (totalNmbrOfLinesInFile * percentage) / 100;
  			int nbLinesEffective = 1;
  
  			if (nbLinesToExtract >= 1)
  			{
  				nbLinesEffective = (int) Math.ceil(nbLinesToExtract);
  			}
  
  			List<String> myListOfRandomRows = mySamplerOfRandomRows.sampler(inputFileName, nbLinesEffective);
  			for (int index = 0; index < myListOfRandomRows.size(); index++)
  			{
  				bw.write(myListOfRandomRows.get(index) + "\n");
  			}
  		}
  
  		bw.close();
  	}
  	catch (FileNotFoundException e) 
  	{
  		log.error("File doesn't exist", e);
  	}
  	catch (IOException e)
  	{
  	  log.error("Error processing " + inputFileName + " or " + outputFileName, e);
  	}
  }

  public static void subsetOfCols(String inputFileName, String outputFileName, List<String> columnNames,
      String fieldSeparator, String fieldSurrounder) {
  
        int[] indexOfColsToExtract = new int[columnNames.size()];
  
        try 
        {
          List<String> headerList = new ArrayList<>();
  
          BufferedReader br = new BufferedReader(new FileReader(inputFileName));
          BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName));
  
          if (columnNames.size() != 0) 
            {
              String[] wordsOfHeader = br.readLine().split(fieldSeparator, -1);
              List<String> colNamesInHeader = new ArrayList<>();
  
              for (int i = 0; i < wordsOfHeader.length; i++) 
                {
                  headerList.add(wordsOfHeader[i].replaceAll("\\\\", ""));// remove the \ from the header if exist
                }
  
              boolean colsFound = false;
              int i = 0;
              for (String col : columnNames) 
                {
                  int headerIndex = 0;
                  for (String word : headerList) 
                    {
                      if (word.equals(col)) 
                        {
                          colNamesInHeader.add(word);
                          colsFound = true;
  
                          indexOfColsToExtract[i++] = headerIndex;
                          break;
                        } 
                      else 
                        {
                          indexOfColsToExtract[i] = -1;
                        }
                      headerIndex++;
                      if (headerIndex == headerList.size()) 
                        {
                          indexOfColsToExtract[i] = -1;
                          i++;
                          break;
                        }
                    }
                }
              if (colsFound) 
                {
                  Arrays.sort(indexOfColsToExtract);
                  List<String> sortedColsNamesInHeader = new ArrayList<>();
                  for (int j = 0; j < indexOfColsToExtract.length; j++) 
                    {
                      if (indexOfColsToExtract[j] != -1) 
                        {
                          sortedColsNamesInHeader.add(headerList.get(indexOfColsToExtract[j]));
                        }
                    }
  
                  // keep the same format of the header in the output file
                  for (int j = 0; j < sortedColsNamesInHeader.size() - 1; j++) 
                    {
                      bw.write(sortedColsNamesInHeader.get(j).concat(fieldSeparator));
                    }
                  bw.write(sortedColsNamesInHeader.get(sortedColsNamesInHeader.size() - 1));
                  bw.write("\n");
  
                  String colsToExtract = "";
                  String line;
                  while ((line = br.readLine()) != null) {
                    if (line.length() != 0) 
                      {
                        String regex = fieldSeparator + "(?=(?:[^\\" + fieldSurrounder + "]*\\" + fieldSurrounder + "[^\\"
                            + fieldSurrounder + "]*\\" + fieldSurrounder + ")*[^\\" + fieldSurrounder + "]*$)";
                        String[] cols = line.split(regex, -1);
  
                        for (int cpt = 0; cpt < indexOfColsToExtract.length -1; cpt++) 
                          {
                            if (indexOfColsToExtract[cpt] != -1) 
                              {
                                colsToExtract = colsToExtract + cols[indexOfColsToExtract[cpt]] + fieldSeparator;
                              }
                          }
                        bw.write(colsToExtract);
                        bw.write(cols[indexOfColsToExtract[indexOfColsToExtract.length-1]]); // to avoid having the fieldSeparator at the end of each written line
                        bw.write("\n");
                        colsToExtract = "";
  
                      }
                  }
                } 
              else 
                {
                  log.error("The columns: " + columnNames + " don't exist in  " + inputFileName);
                }
            } 
          else 
            {
              log.error("No column names were specified!");
            }
          br.close();
          bw.close();
        } catch (FileNotFoundException e) {
          log.error("The file " + inputFileName + " doesn't exist!", e);
        }
        catch (IOException e) 
        {
          log.error("Error processing " + inputFileName + " or " + outputFileName, e);
        }
  }

  public static void filterReport(String InputFileName, String OutputFileName,
      List<String> colsName, List<List<String>> colsValues, String fieldSeparator, String fieldSurrounder) 
  {
  
    if (colsName.size() == colsValues.size()) 
      {
        int[] numOfColm = new int[colsName.size()];
        try 
        {
          BufferedReader br = new BufferedReader(new FileReader(InputFileName));
  
          List<String> headerList = new ArrayList<>();
          String headerVerbatim = br.readLine();
          if (headerVerbatim == null)
            {
              log.error("The file "+InputFileName+" is empty");
              br.close();
              return;
            }
          String[] words = headerVerbatim.split(fieldSeparator, -1);
  
          for (int i = 0; i < words.length; i++) 
            {
              headerList.add(words[i].replaceAll("\\\\'", "'"));
            }
  
          log.debug("Check the existence of columns: " + colsName);
          boolean colsFound = false;
          int i = 0;
          for (String col : colsName) 
            {
              int colIndex = 0;
              for (String word : headerList) 
                {
                  if (word.equals(col)) 
                    {
                      colsFound = true;
                      numOfColm[i++] = colIndex;
                      break;
                    }
                  colsFound = false;
                  colIndex++;
                }
              if (!colsFound) 
                {
                  log.error("No filter is applied since column (" + col + ") doesn't exist in the report (" + InputFileName+")");
                  break;					
                }
            }
  
          if (colsFound) 
            {
              log.debug("Look for matching values: "+colsValues);
              BufferedWriter bw = new BufferedWriter(new FileWriter(OutputFileName));
              String line;
  
              // write header
              bw.write(headerVerbatim + "\n");
  
              while ((line = br.readLine()) != null) 
                {
                  if (line.length() != 0) {
                    String surrounder = Pattern.quote(fieldSurrounder);
                    String separator = Pattern.quote(fieldSeparator);
                    // :(?=(?:[^X]*X[^X]*X)*[^X]*$)
                    String regex = separator + "(?=(?:[^" + surrounder + "]*" + surrounder + "[^" + surrounder + "]*" + surrounder + ")*[^" + surrounder + "]*$)";
                    String[] cols = line.split(regex);
                    i = 0;
                    boolean filterInvalid = false;
                    for (List<String> listOfColsValues : colsValues) 
                      {
                        String valueToTest = cols[numOfColm[i]].replaceAll("^.|.$", "");
                        if (!listOfColsValues.contains(valueToTest)) 
                          {
                            filterInvalid = true;
                            break;
                          }
                        i++;
                      }
                    if (!filterInvalid) 
                      {
                        bw.write(line + "\n");
                      }
                  }
                }
  
              br.close();
              bw.close();
            }
        } 
        catch (FileNotFoundException e) 
        {
          log.error("The file "+InputFileName+"  doesn't exist!", e);
        }
        catch (IOException e)
          {
            log.error("Exception while processing " + InputFileName + " or " + OutputFileName, e);
          }
      } 
    else 
      {
        log.error("size of column names ("+colsName.size()+ ") != size of column values ("+colsValues+").");
      }
  }

  public static Date oneDayBefore(Date currentDate)
  {
    Calendar c = Calendar.getInstance();
    c.setTime(currentDate);
    c.add(Calendar.HOUR_OF_DAY, -24); // go back 24 hours
    Date dayBefore = c.getTime();
    return dayBefore;
  }

  
  public static Date yesterdayAtMidnight(Date currentDate)
  {
    Calendar c = Calendar.getInstance();
    c.setTime(currentDate);
    c.add(Calendar.HOUR_OF_DAY, -24); // go back 24 hours
    c.set(Calendar.HOUR_OF_DAY, 23); // now set to one second before midnight
    c.set(Calendar.MINUTE, 59);
    c.set(Calendar.SECOND, 59);
    Date yesterdayAtMidnight = c.getTime();
    return yesterdayAtMidnight;
  }

  public static Date yesterdayAtZeroHour(Date currentDate)
  {
    Calendar c = Calendar.getInstance();
    c.setTime(currentDate);
    c.add(Calendar.HOUR_OF_DAY, -24); // go back 24 hours
    c.set(Calendar.HOUR_OF_DAY, 0); // now set to one second after 0h
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 1);
    Date yesterdayAtZeroHour = c.getTime();
    return yesterdayAtZeroHour;
  }
}
