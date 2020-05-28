/*****************************************************************************
 *
 *  ReportCsvWriter.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import static com.evolving.nglm.evolution.reports.ReportUtils.d;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElementDeserializer;

/**
 * Handles phase 3 of the Report generation, reading a Kafka topic and writing a
 * csv file.
 *
 */
public class ReportCsvWriter
{

  private static final Logger log = LoggerFactory.getLogger(ReportCsvWriter.class);
  private static final int nbLoopForTrace = 10;
  private Runtime rt = Runtime.getRuntime();

  private ReportCsvFactory reportFactory;
  private String topicIn;
  private String kafkaNodeList;

  /**
   * Creates a ReportCsvWriter instance.
   * <p>
   * The number of messages fetched per poll() call from a topic is set to
   * {@link ReportUtils#DEFAULT_MAX_POLL_RECORDS_CONFIG} . This value can be
   * redefined by setting the environment variable
   * {@link ReportUtils#ENV_MAX_POLL_RECORDS}
   * 
   * @param factory
   *          The {@link ReportCsvFactory}.
   * @param kafkaNodeList
   *          The Kafka cluster node list.
   * @param topicIn
   *          The Kafka topic to read from.
   */
  public ReportCsvWriter(ReportCsvFactory factory, String kafkaNodeList, String topicIn)
  {
    this.reportFactory = factory;
    this.kafkaNodeList = kafkaNodeList;
    this.topicIn = topicIn;
  }

  private Consumer<String, ReportElement> createConsumer(String topic)
  {
    final Properties props = new Properties();

    String groupId = getGroupId() + System.currentTimeMillis();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaNodeList);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ReportElementDeserializer.class.getName());
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, getMaxPollRecords());

    log.info("Creating consumer on topic " + topic + " with group " + groupId);
    final Consumer<String, ReportElement> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topic));
    log.debug("We are now subscribed to ");
    consumer.subscription().forEach(t -> log.debug(" " + t));
    return consumer;
  }

  private String getGroupId()
  {
    String groupId = System.getenv().get(ReportUtils.ENV_GROUP_ID);
    if (groupId == null)
      groupId = ReportUtils.DEFAULT_CONSUMER_GROUPID;
    log.trace("Using " + groupId + " as groupId prefix for Kafka consumer");
    return groupId;
  }

  private int getMaxPollRecords()
  {
    int maxPollRecords = ReportUtils.DEFAULT_MAX_POLL_RECORDS_CONFIG;
    String maxPollRecordsStr = System.getenv().get(ReportUtils.ENV_MAX_POLL_RECORDS);
    if (maxPollRecordsStr != null)
      maxPollRecords = Integer.parseInt(maxPollRecordsStr);
    log.trace("Using " + maxPollRecords + " for MAX_POLL_RECORDS_CONFIG");
    return maxPollRecords;
  }

  /**
   * Produces the csv file.
   * <p>
   * All exceptions related to file system access are trapped and reported to the
   * logs. In this case, the report might be missing or incomplete.
   * 
   * @param csvfile
   *          The csv file name to produce.
   * @return a boolean value indicating if all went well. A value of false means
   *         an error has occurred and the report might be missing or incomplete.
   */
  public final boolean produceReport(String csvfile)
  {
    if (csvfile == null)
      {
        log.info("csvfile is null !");
        return false;
      }

    File file = new File(csvfile + ReportUtils.ZIP_EXTENSION);
    if (file.exists())
      {
        log.info(csvfile + " already exists, do nothing");
        return false;
      }
    FileOutputStream fos;
    try
      {
        fos = new FileOutputStream(file);
      } 
    catch (IOException ex)
      {
        log.info("Error when creating " + csvfile + " : " + ex.getLocalizedMessage());
        return false;
      }
    try
      {
        ZipOutputStream writer = new ZipOutputStream(fos);
        ZipEntry entry = new ZipEntry(new File(csvfile).getName());
        writer.putNextEntry(entry);

        final Consumer<String, ReportElement> consumer = createConsumer(topicIn);

        // final Duration delay = Duration.ofSeconds(1);
        final long delay = 10 * 1000; // 10 seconds
        final int giveUp = 3;
        int noRecordsCount = 0;
        consumer.poll(10 * 1000); // necessary for consumer to reset to beginning of partitions
        consumer.seekToBeginning(consumer.assignment());
        // consumer.poll(Duration.ofSeconds(10));
        showOffsets(consumer);
        int nbRecords = 1;
        boolean breakMainLoop = false;
        // List<String> alreadySeen = new ArrayList<>(30_000_000);
        boolean addHeader = true;
        for (int nbLoop = 0; !breakMainLoop; nbLoop++)
          {
            log.debug("Doing poll...");
            final ConsumerRecords<String, ReportElement> consumerRecords = consumer.poll(delay);
            if (consumerRecords.count() == 0)
              {
                noRecordsCount++;
                if (noRecordsCount > giveUp)
                  break;
                else
                  continue;
              }
            if (nbLoop % nbLoopForTrace == 0)
              log.debug("" + SystemTime.getCurrentTime() + " got " + d(consumerRecords.count()) + " records, total " + d(nbRecords) + " free mem = " + d(rt.freeMemory()) + "/" + d(rt.totalMemory()));

            for (ConsumerRecord<String, ReportElement> record : consumerRecords)
              {
                String key = record.key();
                nbRecords++;
                ReportElement re = record.value();
                addHeader &= reportFactory.dumpElementToCsv(key, re, writer, addHeader);
              }
            writer.flush();
            while (true)
              {
                try
                  {
                    consumer.commitSync();
                    break;
                  } catch (Exception e)
                  {
                    log.info(("Got " + e.getLocalizedMessage() + " we took too long to process batch and got kicked out of the group..."));
                    break;
                  }
              }
          }
        writer.closeEntry();
        writer.close();
        consumer.close();
      } 
    catch (IOException ex)
      {
        log.info("Error when writing to " + csvfile + " : " + ex.getLocalizedMessage());
        return false;
      }
    log.info("Finished producing " + csvfile + ReportUtils.ZIP_EXTENSION);
    return true;
  }
  
  public final boolean produceReport(String csvfile, boolean multipleFile)
  {
    if (!multipleFile)
      {
        return produceReport(csvfile);
      }
    else
      {
        if (csvfile == null)
          {
            log.info("csvfile is null !");
            return false;
          }

        File file = new File(csvfile + ReportUtils.ZIP_EXTENSION);
        if (file.exists())
          {
            log.info(csvfile + " already exists, do nothing");
            return false;
          }
        
        //
        //  consumer
        //
        
        final Consumer<String, ReportElement> consumer = createConsumer(topicIn);
        Map<String, List<Map<String, Object>>> records = new HashMap<String, List<Map<String, Object>>>();
        
        //
        //  set
        //
        
        final long delay = 10 * 1000; // 10 seconds
        final int giveUp = 3;
        int noRecordsCount = 0;
        consumer.poll(10 * 1000); // necessary for consumer to reset to beginning of partitions
        consumer.seekToBeginning(consumer.assignment());
        showOffsets(consumer);
        int nbRecords = 1;
        boolean breakMainLoop = false;
        
        //
        //  read
        //
        
        for (int nbLoop = 0; !breakMainLoop; nbLoop++)
          {
            log.debug("Doing poll...");
            final ConsumerRecords<String, ReportElement> consumerRecords = consumer.poll(delay);
            // TODO We could count the number of markers, and stop when we have seen them all
            if (consumerRecords.count() == 0)
              {
                noRecordsCount++;
                if (noRecordsCount > giveUp)
                  break;
                else
                  continue;
              }
            if (nbLoop % nbLoopForTrace == 0) log.debug("" + SystemTime.getCurrentTime() + " got " + d(consumerRecords.count()) + " records, total " + d(nbRecords) + " free mem = " + d(rt.freeMemory()) + "/" + d(rt.totalMemory()));

            for (ConsumerRecord<String, ReportElement> record : consumerRecords)
              {
                nbRecords++;
                ReportElement re = record.value();
                log.trace("read " + re);
                if (re.type != ReportElement.MARKER && re.isComplete)
                  {
                    Map<String, List<Map<String, Object>>> splittedReportElements = reportFactory.getSplittedReportElementsForFile(record.value());
                    for (String fileKey : splittedReportElements.keySet())
                    
                    if (records.containsKey(fileKey))
                      {
                        records.get(fileKey).addAll(splittedReportElements.get(fileKey));
                      }
                    else
                      {
                        List<Map<String, Object>> elements = new ArrayList<Map<String, Object>>();
                        elements.addAll(splittedReportElements.get(fileKey));
                        records.put(fileKey, elements);
                      }
                  }
              }
            while (true)
              {
                try
                  {
                    consumer.commitSync();
                    break;
                  } 
                catch (Exception e)
                  {
                    log.info(("Got " + e.getLocalizedMessage() + " we took too long to process batch and got kicked out of the group..."));
                    break;
                  }
              }
          }
        consumer.close();
        
        try
          {
            //
            //  ZIP
            //
            
            FileOutputStream fos = new FileOutputStream(file);
            ZipOutputStream writer = new ZipOutputStream(fos);
            
            for (String key : records.keySet())
              {
                //
                // data
                //

                boolean addHeader = true;
                String dataFile[] = csvfile.split("[.]");
                String dataFileName = dataFile[0] + "_" + key;
                String zipEntryName = new File(dataFileName + "." + dataFile[1]).getName();
                ZipEntry entry = new ZipEntry(zipEntryName);
                writer.putNextEntry(entry);
                for (Map<String, Object> lineMap : records.get(key))
                  {
                    reportFactory.dumpLineToCsv(lineMap, writer, addHeader);
                    addHeader = false;
                  }
              }
            writer.flush();
            writer.closeEntry();
            writer.close();
            fos.close();
          }
        catch (Exception e) 
          {
            log.error("Error in file {} ", e);
          }
      }
    log.info("Finished producing " + csvfile + ReportUtils.ZIP_EXTENSION);
    return true;
  }

  private void showOffsets(final Consumer<String, ReportElement> consumer)
  {
    log.trace("Reading at offsets :");
    consumer.assignment().forEach(p -> log.trace(" " + p.toString()));
  }
}
